use futures::{stream::BoxStream, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use stackdriver_metrics::{Client, Options};
use std::hash::{Hash, Hasher};
use vector::config::{DataType, SinkConfig, SinkContext, SinkDescription};
use vector::event::{Event, MetricValue};
use vector::sinks::gcp;
use vector::sinks::util::StreamSink;
use vector::sinks::{Healthcheck, VectorSink};

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct StackdriverConfig {
    pub project_id: String,
    pub resource: gcp::GcpTypedResource,
    pub credentials_path: Option<String>,
    #[serde(default = "default_metric_namespace_value")]
    pub default_namespace: String,
}

fn default_metric_namespace_value() -> String {
    "namespace".to_string()
}

vector::impl_generate_config_from_default!(StackdriverConfig);

inventory::submit! {
    SinkDescription::new::<StackdriverConfig>("grpc_stackdriver_metrics")
}

struct MetricSink {
    client: Client,
    project_id: String,
    resource: gcp::GcpTypedResource,
    namespace: String,
    started: chrono::DateTime<chrono::Utc>,
}

impl MetricSink {
    async fn new(
        project_id: String,
        credentials_path: Option<String>,
        resource: gcp::GcpTypedResource,
        namespace: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let options = Options::default().credentials_options(credentials_path);

        info!("gRPC Options: {:?}", options);
        let client = Client::create(options).await?;

        Ok(Self {
            client,
            project_id,
            resource,
            namespace,
            started: chrono::Utc::now(),
        })
    }
}

struct Wrap(stackdriver_metrics::TimeSeries);

impl PartialEq for Wrap {
    fn eq(&self, right: &Wrap) -> bool {
        self.0.metric.r#type == right.0.metric.r#type
    }
}

impl Eq for Wrap {}

impl Hash for Wrap {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.metric.r#type.hash(state)
    }
}

const MAX_GCP_SERIES_BATCH_SIZE: usize = 200;

#[async_trait::async_trait]
impl StreamSink for MetricSink {
    async fn run(self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        use std::time::{Duration, Instant};

        let mut buffer = std::collections::HashSet::with_capacity(200);
        let mut last_time = Instant::now();

        while let Some(event) = input.next().await {
            let metric = event.into_metric();
            let namespace = metric
                .series()
                .name
                .namespace
                .as_ref()
                .unwrap_or(&self.namespace);

            let metric_type = format!(
                "custom.googleapis.com/{}/metrics/{}",
                namespace,
                metric.series().name.name
            );
            let metric_labels = metric
                .series()
                .tags
                .clone()
                .unwrap_or_default()
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>();

            let end_time = metric.data().timestamp.unwrap_or_else(chrono::Utc::now);

            let (point_value, interval, metric_kind) = match &metric.data().value {
                &MetricValue::Counter { value } => {
                    let interval = stackdriver_metrics::Interval {
                        start_time: Some(self.started),
                        end_time,
                    };

                    (value, interval, stackdriver_metrics::MetricKind::Cumulative)
                }

                &MetricValue::Gauge { value } => {
                    let interval = stackdriver_metrics::Interval {
                        start_time: None,
                        end_time,
                    };

                    (value, interval, stackdriver_metrics::MetricKind::Gauge)
                }

                not_supported => {
                    warn!("Unsupported metric type {:?}", not_supported);
                    continue;
                }
            };

            let time_series = stackdriver_metrics::TimeSeries {
                metric: stackdriver_metrics::TypedResource {
                    r#type: metric_type,
                    labels: metric_labels,
                },

                resource: stackdriver_metrics::TypedResource {
                    r#type: self.resource.r#type.clone(),
                    labels: self.resource.labels.clone(),
                },

                metric_kind,
                value_type: stackdriver_metrics::ValueType::Int64,

                points: stackdriver_metrics::Point {
                    interval,
                    value: stackdriver_metrics::PointValue {
                        int64_value: point_value as i64,
                    },
                },
            };

            let wrapped = Wrap(time_series);
            if buffer.contains(&wrapped) {
                buffer.replace(wrapped);
            } else {
                buffer.insert(wrapped);
            }

            if buffer.len() == MAX_GCP_SERIES_BATCH_SIZE
                || last_time.elapsed() >= Duration::from_secs(10)
            {
                let values = buffer.drain().map(|w| w.0).collect();

                if let Err(e) = self
                    .client
                    .create_time_series(self.project_id.as_str(), values)
                    .await
                {
                    match e {
                        stackdriver_metrics::Error::Grpc(status) => {
                            warn!("gRPC error happened: {:?}", status);
                        }

                        stackdriver_metrics::Error::InvalidArgument(e) => {
                            error!("GCP configuration error: {}", e);
                            break;
                        }
                    }
                } else {
                    info!("Successfully pushed time series!");
                }

                last_time = Instant::now();
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "grpc_stackdriver_metrics")]
impl SinkConfig for StackdriverConfig {
    async fn build(&self, _cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let sink = MetricSink::new(
            self.project_id.clone(),
            self.credentials_path.clone(),
            self.resource.clone(),
            self.default_namespace.clone(),
        )
        .await?;
        let healthcheck = healthcheck().boxed();
        let sink = VectorSink::Stream(Box::new(sink));

        Ok((sink, healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Metric
    }

    fn sink_type(&self) -> &'static str {
        "grpc_stackdriver_metrics"
    }
}

async fn healthcheck() -> vector::Result<()> {
    Ok(())
}
