use crate::config::{DataType, SinkConfig, SinkContext, SinkDescription};
use crate::event::MetricValue;
use crate::http::HttpClient;
use crate::sinks::gcp::GcpMonitoredResource;
use crate::sinks::util::http::{BatchedHttpSink, HttpSink};
use crate::sinks::util::{
    BatchConfig, BatchSettings, BoxedRawValue, JsonArrayBuffer, TowerRequestConfig,
};
use crate::sinks::{Healthcheck, VectorSink};
use crate::tls::{TlsOptions, TlsSettings};
use crate::Event;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::sink::SinkExt;
use futures::FutureExt;
use http::header::AUTHORIZATION;
use http::{HeaderValue, Uri};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct StackdriverConfig {
    pub project_id: String,
    pub resource: GcpMonitoredResource,
    pub service_account_file: Option<String>,
    #[serde(default)]
    pub request: TowerRequestConfig,
    #[serde(default)]
    pub batch: BatchConfig,
    pub tls: Option<TlsOptions>,
}

impl_generate_config_from_default!(StackdriverConfig);

inventory::submit! {
    SinkDescription::new::<StackdriverConfig>("gcp_stackdriver_metrics")
}

struct HttpEventSink {
    config: StackdriverConfig,
    started: DateTime<Utc>,
    token: gouth::Token,
}

#[async_trait::async_trait]
impl HttpSink for HttpEventSink {
    type Input = serde_json::Value;
    type Output = Vec<BoxedRawValue>;

    fn encode_event(&self, event: Event) -> Option<Self::Input> {
        let metric = event.into_metric();
        let namespace = metric.namespace.unwrap_or_else(|| "namespace".to_string());
        let metric_type = format!(
            "custom.googleapis.com/{}/metrics/{}",
            namespace, metric.name
        );
        let mut metric_labels = std::collections::HashMap::new();

        for (key, value) in metric.tags.unwrap_or_default() {
            metric_labels.insert(key, value);
        }

        let metric_kind;
        let mut interval = std::collections::HashMap::new();

        let end_time = metric.timestamp.unwrap_or_else(|| chrono::Utc::now());

        let point_value = match metric.value {
            MetricValue::Counter { value } => {
                metric_kind = "CUMULATIVE";
                interval.insert(
                    "startTime",
                    self.started.to_rfc3339_opts(SecondsFormat::Nanos, true),
                );
                interval.insert(
                    "endTime",
                    end_time.to_rfc3339_opts(SecondsFormat::Nanos, true),
                );
                value
            }
            MetricValue::Gauge { value } => {
                metric_kind = "GAUGE";
                interval.insert(
                    "endTime",
                    end_time.to_rfc3339_opts(SecondsFormat::Nanos, true),
                );
                value
            }
            not_supported => {
                warn!("Unsupported metric kind: {:?}", not_supported);
                return None;
            }
        };

        let series = serde_json::json!({
            "metric": { "type": metric_type, "labels": metric_labels },
            "resource": {
                "type": self.config.resource.tpe.clone(),
                "labels": self.config.resource.labels.clone()
            },
            "metricKind": metric_kind,
            "valueType": "INT64",
            "points": [ serde_json::json!({
                "interval": interval,
                "value": serde_json::json!({
                    "int64Value": (point_value as i64).to_string()
                })
            })]
        });

        Some(series)
    }

    async fn build_request(
        &self,
        time_series: Self::Output,
    ) -> crate::Result<hyper::Request<Vec<u8>>> {
        let time_series = serde_json::json!({ "timeSeries": time_series });

        let body = serde_json::to_vec(&time_series).unwrap();
        let uri: Uri = format!(
            "https://monitoring.googleapis.com/v3/projects/{}/timeSeries",
            self.config.project_id
        )
        .as_str()
        .parse()
        .unwrap();

        let mut request = hyper::Request::post(uri)
            .header("content-type", "application/json")
            .body(body)
            .unwrap();

        request.headers_mut().insert(
            AUTHORIZATION,
            self.token
                .header_value()
                .unwrap()
                .as_str()
                .parse::<HeaderValue>()
                .unwrap(),
        );

        Ok(request)
    }
}

lazy_static! {
    static ref REQUEST_DEFAULTS: TowerRequestConfig = TowerRequestConfig {
        rate_limit_num: Some(1000),
        rate_limit_duration_secs: Some(1),
        ..Default::default()
    };
}

#[async_trait::async_trait]
#[typetag::serde(name = "gcp_stackdriver_metrics")]
impl SinkConfig for StackdriverConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let mut token = gouth::Builder::new().scopes(&[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/monitoring",
            "https://www.googleapis.com/auth/monitoring.write",
        ]);

        if let Some(service_account_file) = self.service_account_file.clone() {
            token = token.file(service_account_file);
        }

        let token = token.build()?;
        let healthcheck = healthcheck().boxed();
        let started = chrono::Utc::now();
        let request = self.request.unwrap_with(&REQUEST_DEFAULTS);
        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(tls_settings)?;
        let batch = BatchSettings::default()
            .events(1)
            .parse_config(self.batch)?;

        let sink = HttpEventSink {
            config: self.clone(),
            started,
            token,
        };

        let sink = BatchedHttpSink::new(
            sink,
            JsonArrayBuffer::new(batch.size),
            request,
            batch.timeout,
            client,
            cx.acker(),
        )
        .sink_map_err(
            |error| error!(message = "Fatal gcp_stackdriver_metrics sink error.", %error),
        );

        Ok((VectorSink::Sink(Box::new(sink)), healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Metric
    }

    fn sink_type(&self) -> &'static str {
        "gcp_stackdriver_metrics"
    }
}

async fn healthcheck() -> crate::Result<()> {
    Ok(())
}
