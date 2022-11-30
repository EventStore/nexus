use futures::{stream::BoxStream, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use stackdriver_metrics::{Client, Options};
use std::time::Duration;
use vector::config::{DataType, SinkConfig, SinkContext, SinkDescription};
use vector::event::{Event, MetricValue};
use vector::sinks::gcp::{self, GcpTypedResource};
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
    credential_path: Option<String>,
}

impl MetricSink {
    async fn new(
        project_id: String,
        credential_path: Option<String>,
        resource: gcp::GcpTypedResource,
        namespace: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let client = Client::new().await?;

        Ok(Self {
            client,
            project_id,
            resource,
            namespace,
            credential_path,
        })
    }
}

struct Params {
    default_namespace: String,
    resource: GcpTypedResource,
}

fn convert_event(params: &Params, event: Event) -> Option<stackdriver_metrics::TimeSeries> {
    let metric = event.into_metric();
    let namespace = metric
        .series()
        .name
        .namespace
        .as_ref()
        .unwrap_or(&params.default_namespace);

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

    let created = metric.data().timestamp.unwrap_or_else(chrono::Utc::now);

    let (point_value, metric_kind) = match &metric.data().value {
        &MetricValue::Counter { value } => (value, stackdriver_metrics::MetricKind::Cumulative),

        &MetricValue::Gauge { value } => (value, stackdriver_metrics::MetricKind::Gauge),

        not_supported => {
            warn!("Unsupported metric type {:?}", not_supported);
            return None;
        }
    };

    let value_type = match metric_kind {
        stackdriver_metrics::MetricKind::Gauge => stackdriver_metrics::ValueType::Double,
        stackdriver_metrics::MetricKind::Cumulative => stackdriver_metrics::ValueType::Int64,
    };

    let time_series = stackdriver_metrics::TimeSeries {
        metric: stackdriver_metrics::TypedResource {
            r#type: metric_type,
            labels: metric_labels,
        },

        resource: stackdriver_metrics::TypedResource {
            r#type: params.resource.r#type.clone(),
            labels: params.resource.labels.clone(),
        },

        metric_kind,
        value_type,

        points: stackdriver_metrics::Point {
            created,
            value: point_value,
        },
    };

    Some(time_series)
}

#[async_trait::async_trait]
impl StreamSink for MetricSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let options = Options::default()
            .batch_size(200)
            .period(Duration::from_secs(10))
            .credentials_options(self.credential_path.clone());

        let params = Params {
            default_namespace: self.namespace.clone(),
            resource: self.resource.clone(),
        };

        let stream =
            input.filter_map(move |event| futures::future::ready(convert_event(&params, event)));

        self.client
            .stream_time_series(self.project_id.as_str(), &options, Box::pin(stream))
            .await;
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
