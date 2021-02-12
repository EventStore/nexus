use crate::{
    config::{log_schema, DataType, GenerateConfig, TransformConfig, TransformDescription},
    event::metric::{Metric, MetricKind, MetricValue, StatisticKind},
    event::LogEvent,
    event::Value,
    internal_events::{
        LogToMetricFieldNotFound, LogToMetricParseFloatError, LogToMetricTemplateParseError,
        LogToMetricTemplateRenderError,
    },
    template::{Template, TemplateError},
    transforms::{FunctionTransform, Transform},
    Event,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::num::ParseFloatError;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct LogToMetricConfig {
    pub metrics: Vec<MetricConfig>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CounterConfig {
    field: String,
    name: Option<String>,
    namespace: Option<String>,
    #[serde(default = "default_increment_by_value")]
    increment_by_value: bool,
    tags: Option<IndexMap<String, String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GaugeConfig {
    pub field: String,
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub tags: Option<IndexMap<String, String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SetConfig {
    field: String,
    name: Option<String>,
    namespace: Option<String>,
    tags: Option<IndexMap<String, String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct HistogramConfig {
    field: String,
    name: Option<String>,
    namespace: Option<String>,
    tags: Option<IndexMap<String, String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SummaryConfig {
    field: String,
    name: Option<String>,
    namespace: Option<String>,
    tags: Option<IndexMap<String, String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetricConfig {
    Counter(CounterConfig),
    Histogram(HistogramConfig),
    Gauge(GaugeConfig),
    Set(SetConfig),
    Summary(SummaryConfig),
}

fn default_increment_by_value() -> bool {
    false
}

#[derive(Debug, Clone)]
pub struct LogToMetric {
    config: LogToMetricConfig,
}

inventory::submit! {
    TransformDescription::new::<LogToMetricConfig>("log_to_metric")
}

impl GenerateConfig for LogToMetricConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            metrics: vec![MetricConfig::Counter(CounterConfig {
                field: "field_name".to_string(),
                name: None,
                namespace: None,
                increment_by_value: false,
                tags: None,
            })],
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "log_to_metric")]
impl TransformConfig for LogToMetricConfig {
    async fn build(&self) -> crate::Result<Transform> {
        Ok(Transform::function(LogToMetric::new(self.clone())))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn output_type(&self) -> DataType {
        DataType::Metric
    }

    fn transform_type(&self) -> &'static str {
        "log_to_metric"
    }
}

impl LogToMetric {
    pub fn new(config: LogToMetricConfig) -> Self {
        LogToMetric { config }
    }
}

enum TransformError {
    FieldNotFound {
        field: String,
    },
    TemplateParseError(TemplateError),
    TemplateRenderError {
        missing_keys: Vec<String>,
    },
    ParseFloatError {
        field: String,
        error: ParseFloatError,
    },
}

fn render_template(s: &str, event: &Event) -> Result<String, TransformError> {
    let template = Template::try_from(s).map_err(TransformError::TemplateParseError)?;
    template
        .render_string(&event)
        .map_err(|missing_keys| TransformError::TemplateRenderError { missing_keys })
}

fn render_tags(
    tags: &Option<IndexMap<String, String>>,
    event: &Event,
) -> Result<Option<BTreeMap<String, String>>, TransformError> {
    Ok(match tags {
        None => None,
        Some(tags) => {
            let mut map = BTreeMap::new();
            for (name, value) in tags {
                match render_template(value, event) {
                    Ok(tag) => {
                        map.insert(name.to_string(), tag);
                    }
                    Err(TransformError::TemplateRenderError { missing_keys }) => {
                        emit!(LogToMetricTemplateRenderError { missing_keys });
                    }
                    Err(other) => return Err(other),
                }
            }
            if !map.is_empty() {
                Some(map)
            } else {
                None
            }
        }
    })
}

fn parse_field(log: &LogEvent, field: &str) -> Result<f64, TransformError> {
    let value = log
        .get(field)
        .ok_or_else(|| TransformError::FieldNotFound {
            field: field.to_string(),
        })?;
    value
        .to_string_lossy()
        .parse()
        .map_err(|error| TransformError::ParseFloatError {
            field: field.to_string(),
            error,
        })
}

fn to_metric(config: &MetricConfig, event: &Event) -> Result<Metric, TransformError> {
    let log = event.as_log();

    let timestamp = log
        .get(log_schema().timestamp_key())
        .and_then(Value::as_timestamp)
        .cloned();

    match config {
        MetricConfig::Counter(counter) => {
            let value = log
                .get(&counter.field)
                .ok_or_else(|| TransformError::FieldNotFound {
                    field: counter.field.clone(),
                })?;
            let value = if counter.increment_by_value {
                value.to_string_lossy().parse().map_err(|error| {
                    TransformError::ParseFloatError {
                        field: counter.field.clone(),
                        error,
                    }
                })?
            } else {
                1.0
            };

            let name = counter.name.as_ref().unwrap_or(&counter.field);
            let name = render_template(&name, &event)?;

            let namespace = counter.namespace.as_ref();
            let namespace = namespace
                .map(|namespace| render_template(namespace, &event))
                .transpose()?;

            let tags = render_tags(&counter.tags, &event)?;

            Ok(Metric {
                name,
                namespace,
                timestamp,
                tags,
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value },
            })
        }
        MetricConfig::Histogram(hist) => {
            let value = parse_field(&log, &hist.field)?;

            let name = hist.name.as_ref().unwrap_or(&hist.field);
            let name = render_template(&name, &event)?;

            let namespace = hist.namespace.as_ref();
            let namespace = namespace
                .map(|namespace| render_template(namespace, &event))
                .transpose()?;

            let tags = render_tags(&hist.tags, &event)?;

            Ok(Metric {
                name,
                namespace,
                timestamp,
                tags,
                kind: MetricKind::Incremental,
                value: MetricValue::Distribution {
                    values: vec![value],
                    sample_rates: vec![1],
                    statistic: StatisticKind::Histogram,
                },
            })
        }
        MetricConfig::Summary(summary) => {
            let value = parse_field(&log, &summary.field)?;

            let name = summary.name.as_ref().unwrap_or(&summary.field);
            let name = render_template(&name, &event)?;

            let namespace = summary.namespace.as_ref();
            let namespace = namespace
                .map(|namespace| render_template(namespace, &event))
                .transpose()?;

            let tags = render_tags(&summary.tags, &event)?;

            Ok(Metric {
                name,
                namespace,
                timestamp,
                tags,
                kind: MetricKind::Incremental,
                value: MetricValue::Distribution {
                    values: vec![value],
                    sample_rates: vec![1],
                    statistic: StatisticKind::Summary,
                },
            })
        }
        MetricConfig::Gauge(gauge) => {
            let value = parse_field(&log, &gauge.field)?;

            let name = gauge.name.as_ref().unwrap_or(&gauge.field);
            let name = render_template(&name, &event)?;

            let namespace = gauge.namespace.as_ref();
            let namespace = namespace
                .map(|namespace| render_template(namespace, &event))
                .transpose()?;

            let tags = render_tags(&gauge.tags, &event)?;

            Ok(Metric {
                name,
                namespace,
                timestamp,
                tags,
                kind: MetricKind::Absolute,
                value: MetricValue::Gauge { value },
            })
        }
        MetricConfig::Set(set) => {
            let value = log
                .get(&set.field)
                .ok_or_else(|| TransformError::FieldNotFound {
                    field: set.field.clone(),
                })?;
            let value = value.to_string_lossy();

            let name = set.name.as_ref().unwrap_or(&set.field);
            let name = render_template(&name, &event)?;

            let namespace = set.namespace.as_ref();
            let namespace = namespace
                .map(|namespace| render_template(namespace, &event))
                .transpose()?;

            let tags = render_tags(&set.tags, &event)?;

            Ok(Metric {
                name,
                namespace,
                timestamp,
                tags,
                kind: MetricKind::Incremental,
                value: MetricValue::Set {
                    values: std::iter::once(value).collect(),
                },
            })
        }
    }
}

impl FunctionTransform for LogToMetric {
    fn transform(&mut self, output: &mut Vec<Event>, event: Event) {
        for config in self.config.metrics.iter() {
            match to_metric(&config, &event) {
                Ok(metric) => {
                    output.push(Event::Metric(metric));
                }
                Err(TransformError::FieldNotFound { field }) => emit!(LogToMetricFieldNotFound {
                    field: field.as_ref()
                }),
                Err(TransformError::ParseFloatError { field, error }) => {
                    emit!(LogToMetricParseFloatError {
                        field: field.as_ref(),
                        error
                    })
                }
                Err(TransformError::TemplateRenderError { missing_keys }) => {
                    emit!(LogToMetricTemplateRenderError { missing_keys })
                }
                Err(TransformError::TemplateParseError(error)) => {
                    emit!(LogToMetricTemplateParseError { error })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::log_schema,
        event::metric::{Metric, MetricKind, MetricValue, StatisticKind},
        event::Event,
    };
    use chrono::{offset::TimeZone, DateTime, Utc};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<LogToMetricConfig>();
    }

    fn parse_config(s: &str) -> LogToMetricConfig {
        toml::from_str(s).unwrap()
    }

    fn ts() -> DateTime<Utc> {
        Utc.ymd(2018, 11, 14).and_hms_nano(8, 9, 10, 11)
    }

    fn create_event(key: &str, value: &str) -> Event {
        let mut log = Event::from("i am a log");
        log.as_mut_log().insert(key, value);
        log.as_mut_log().insert(log_schema().timestamp_key(), ts());
        log
    }

    #[test]
    fn count_http_status_codes() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"
            "#,
        );

        let event = create_event("status", "42");
        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "status".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value: 1.0 },
            }
        );
    }

    #[test]
    fn count_http_requests_with_tags() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "message"
            name = "http_requests_total"
            namespace = "app"
            tags = {method = "{{method}}", code = "{{code}}", missing_tag = "{{unknown}}", host = "localhost"}
            "#,
        );

        let mut event = create_event("message", "i am log");
        event.as_mut_log().insert("method", "post");
        event.as_mut_log().insert("code", "200");

        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "http_requests_total".into(),
                namespace: Some("app".into()),
                timestamp: Some(ts()),
                tags: Some(
                    vec![
                        ("method".to_owned(), "post".to_owned()),
                        ("code".to_owned(), "200".to_owned()),
                        ("host".to_owned(), "localhost".to_owned()),
                    ]
                    .into_iter()
                    .collect(),
                ),
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value: 1.0 },
            }
        );
    }

    #[test]
    fn count_exceptions() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "exception_total"
            "#,
        );

        let event = create_event("backtrace", "message");
        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "exception_total".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value: 1.0 },
            }
        );
    }

    #[test]
    fn count_exceptions_no_match() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "exception_total"
            "#,
        );

        let event = create_event("success", "42");
        let mut transform = LogToMetric::new(config);

        assert_eq!(transform.transform_one(event), None);
    }

    #[test]
    fn sum_order_amounts() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "amount"
            name = "amount_total"
            increment_by_value = true
            "#,
        );

        let event = create_event("amount", "33.99");
        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "amount_total".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value: 33.99 },
            }
        );
    }

    #[test]
    fn memory_usage_gauge() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "gauge"
            field = "memory_rss"
            name = "memory_rss_bytes"
            "#,
        );

        let event = create_event("memory_rss", "123");
        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "memory_rss_bytes".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Absolute,
                value: MetricValue::Gauge { value: 123.0 },
            }
        );
    }

    #[test]
    fn parse_failure() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"
            name = "status_total"
            increment_by_value = true
            "#,
        );

        let event = create_event("status", "not a number");
        let mut transform = LogToMetric::new(config);

        assert_eq!(transform.transform_one(event), None);
    }

    #[test]
    fn missing_field() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"
            name = "status_total"
            "#,
        );

        let event = create_event("not foo", "not a number");
        let mut transform = LogToMetric::new(config);

        assert_eq!(transform.transform_one(event), None);
    }

    #[test]
    fn multiple_metrics() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"

            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "exception_total"
            "#,
        );

        let mut event = Event::from("i am a log");
        event
            .as_mut_log()
            .insert(log_schema().timestamp_key(), ts());
        event.as_mut_log().insert("status", "42");
        event.as_mut_log().insert("backtrace", "message");

        let mut transform = LogToMetric::new(config);

        let mut output = Vec::new();
        transform.transform(&mut output, event);
        assert_eq!(2, output.len());
        assert_eq!(
            output.pop().unwrap().into_metric(),
            Metric {
                name: "exception_total".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value: 1.0 },
            }
        );
        assert_eq!(
            output.pop().unwrap().into_metric(),
            Metric {
                name: "status".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value: 1.0 },
            }
        );
    }

    #[test]
    fn multiple_metrics_with_multiple_templates() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "set"
            field = "status"
            name = "{{host}}_{{worker}}_status_set"

            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "{{service}}_exception_total"
            namespace = "{{host}}"
            "#,
        );

        let mut event = Event::from("i am a log");
        event
            .as_mut_log()
            .insert(log_schema().timestamp_key(), ts());
        event.as_mut_log().insert("status", "42");
        event.as_mut_log().insert("backtrace", "message");
        event.as_mut_log().insert("host", "local");
        event.as_mut_log().insert("worker", "abc");
        event.as_mut_log().insert("service", "xyz");

        let mut transform = LogToMetric::new(config);

        let mut output = Vec::new();
        transform.transform(&mut output, event);
        assert_eq!(2, output.len());
        assert_eq!(
            output.pop().unwrap().into_metric(),
            Metric {
                name: "xyz_exception_total".into(),
                namespace: Some("local".into()),
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Counter { value: 1.0 },
            }
        );
        assert_eq!(
            output.pop().unwrap().into_metric(),
            Metric {
                name: "local_abc_status_set".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Set {
                    values: vec!["42".into()].into_iter().collect()
                },
            }
        );
    }

    #[test]
    fn user_ip_set() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "set"
            field = "user_ip"
            name = "unique_user_ip"
            "#,
        );

        let event = create_event("user_ip", "1.2.3.4");
        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "unique_user_ip".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Set {
                    values: vec!["1.2.3.4".into()].into_iter().collect()
                },
            }
        );
    }

    #[test]
    fn response_time_histogram() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "histogram"
            field = "response_time"
            "#,
        );

        let event = create_event("response_time", "2.5");
        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "response_time".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Distribution {
                    values: vec![2.5],
                    sample_rates: vec![1],
                    statistic: StatisticKind::Histogram
                },
            }
        );
    }

    #[test]
    fn response_time_summary() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "summary"
            field = "response_time"
            "#,
        );

        let event = create_event("response_time", "2.5");
        let mut transform = LogToMetric::new(config);
        let metric = transform.transform_one(event).unwrap();

        assert_eq!(
            metric.into_metric(),
            Metric {
                name: "response_time".into(),
                namespace: None,
                timestamp: Some(ts()),
                tags: None,
                kind: MetricKind::Incremental,
                value: MetricValue::Distribution {
                    values: vec![2.5],
                    sample_rates: vec![1],
                    statistic: StatisticKind::Summary
                },
            }
        );
    }
}
