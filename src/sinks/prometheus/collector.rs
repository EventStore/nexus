use crate::{
    event::metric::{Metric, MetricValue, StatisticKind},
    prometheus::{proto, METRIC_NAME_LABEL},
    sinks::util::{encode_namespace, statistic::DistributionStatistic},
};
use indexmap::map::IndexMap;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;

pub(super) trait MetricCollector {
    type Output;

    fn new() -> Self;

    fn emit_metadata(&mut self, name: &str, fullname: &str, value: &MetricValue);

    fn emit_value(
        &mut self,
        timestamp_millis: Option<i64>,
        name: &str,
        suffix: &str,
        value: f64,
        tags: &Option<BTreeMap<String, String>>,
        extra: Option<(&str, String)>,
    );

    fn finish(self) -> Self::Output;

    fn encode_metric(
        &mut self,
        default_namespace: Option<&str>,
        buckets: &[f64],
        quantiles: &[f64],
        expired: bool,
        metric: &Metric,
    ) {
        let name = encode_namespace(
            metric.namespace.as_deref().or(default_namespace),
            '_',
            &metric.name,
        );
        let name = &name;
        let timestamp = metric.timestamp.map(|t| t.timestamp_millis());

        if metric.kind.is_absolute() {
            let tags = &metric.tags;
            self.emit_metadata(&metric.name, &name, &metric.value);

            match &metric.value {
                MetricValue::Counter { value } => {
                    self.emit_value(timestamp, &name, "", *value, tags, None);
                }
                MetricValue::Gauge { value } => {
                    self.emit_value(timestamp, &name, "", *value, tags, None);
                }
                MetricValue::Set { values } => {
                    // sets could expire
                    let value = if expired { 0 } else { values.len() };
                    self.emit_value(timestamp, &name, "", value as f64, tags, None);
                }
                MetricValue::Distribution {
                    values,
                    sample_rates,
                    statistic: StatisticKind::Histogram,
                } => {
                    // convert distributions into aggregated histograms
                    let mut counts = vec![0; buckets.len()];
                    let mut sum = 0.0;
                    let mut count = 0;
                    for (v, c) in values.iter().zip(sample_rates.iter()) {
                        buckets
                            .iter()
                            .enumerate()
                            .skip_while(|&(_, b)| b < v)
                            .for_each(|(i, _)| {
                                counts[i] += c;
                            });

                        sum += v * (*c as f64);
                        count += c;
                    }

                    for (b, c) in buckets.iter().zip(counts.iter()) {
                        self.emit_value(
                            timestamp,
                            &name,
                            "_bucket",
                            *c as f64,
                            tags,
                            Some(("le", b.to_string())),
                        );
                    }
                    self.emit_value(
                        timestamp,
                        &name,
                        "_bucket",
                        count as f64,
                        tags,
                        Some(("le", "+Inf".to_string())),
                    );
                    self.emit_value(timestamp, &name, "_sum", sum as f64, tags, None);
                    self.emit_value(timestamp, &name, "_count", count as f64, tags, None);
                }
                MetricValue::Distribution {
                    values,
                    sample_rates,
                    statistic: StatisticKind::Summary,
                } => {
                    if let Some(statistic) =
                        DistributionStatistic::new(values, sample_rates, quantiles)
                    {
                        for (q, v) in statistic.quantiles.iter() {
                            self.emit_value(
                                timestamp,
                                &name,
                                "",
                                *v,
                                tags,
                                Some(("quantile", q.to_string())),
                            );
                        }
                        self.emit_value(timestamp, &name, "_sum", statistic.sum, tags, None);
                        self.emit_value(
                            timestamp,
                            &name,
                            "_count",
                            statistic.count as f64,
                            tags,
                            None,
                        );
                        self.emit_value(timestamp, &name, "_min", statistic.min, tags, None);
                        self.emit_value(timestamp, &name, "_max", statistic.max, tags, None);
                        self.emit_value(timestamp, &name, "_avg", statistic.avg, tags, None);
                    } else {
                        self.emit_value(timestamp, &name, "_sum", 0.0, tags, None);
                        self.emit_value(timestamp, &name, "_count", 0.0, tags, None);
                    }
                }
                MetricValue::AggregatedHistogram {
                    buckets,
                    counts,
                    count,
                    sum,
                } => {
                    let mut value = 0f64;
                    for (b, c) in buckets.iter().zip(counts.iter()) {
                        // prometheus uses cumulative histogram
                        // https://prometheus.io/docs/concepts/metric_types/#histogram
                        value += *c as f64;
                        self.emit_value(
                            timestamp,
                            &name,
                            "_bucket",
                            value,
                            tags,
                            Some(("le", b.to_string())),
                        );
                    }
                    self.emit_value(
                        timestamp,
                        &name,
                        "_bucket",
                        *count as f64,
                        tags,
                        Some(("le", "+Inf".to_string())),
                    );
                    self.emit_value(timestamp, &name, "_sum", *sum, tags, None);
                    self.emit_value(timestamp, &name, "_count", *count as f64, tags, None);
                }
                MetricValue::AggregatedSummary {
                    quantiles,
                    values,
                    count,
                    sum,
                } => {
                    for (q, v) in quantiles.iter().zip(values.iter()) {
                        self.emit_value(
                            timestamp,
                            &name,
                            "",
                            *v,
                            tags,
                            Some(("quantile", q.to_string())),
                        );
                    }
                    self.emit_value(timestamp, &name, "_sum", *sum, tags, None);
                    self.emit_value(timestamp, &name, "_count", *count as f64, tags, None);
                }
            }
        }
    }
}

pub(super) struct StringCollector {
    result: String,
    processed: HashSet<String>,
}

impl MetricCollector for StringCollector {
    type Output = String;

    fn new() -> Self {
        let result = String::new();
        let processed = HashSet::new();
        Self { result, processed }
    }

    fn emit_metadata(&mut self, name: &str, fullname: &str, value: &MetricValue) {
        if !self.processed.contains(name) {
            self.encode_header(name, fullname, value);
            self.processed.insert(name.into());
        }
    }

    fn emit_value(
        &mut self,
        timestamp_millis: Option<i64>,
        name: &str,
        suffix: &str,
        value: f64,
        tags: &Option<BTreeMap<String, String>>,
        extra: Option<(&str, String)>,
    ) {
        self.result.push_str(name);
        self.result.push_str(suffix);
        self.encode_tags(tags, extra);
        let _ = match timestamp_millis {
            None => writeln!(&mut self.result, " {}", value),
            Some(timestamp) => writeln!(&mut self.result, " {} {}", value, timestamp),
        };
    }

    fn finish(self) -> String {
        self.result
    }
}

impl StringCollector {
    fn encode_tags(
        &mut self,
        tags: &Option<BTreeMap<String, String>>,
        extra: Option<(&str, String)>,
    ) {
        match (tags, extra) {
            (None, None) => Ok(()),
            (None, Some(tag)) => write!(&mut self.result, "{{{}=\"{}\"}}", tag.0, tag.1),
            (Some(tags), ref tag) => {
                let mut parts = tags
                    .iter()
                    .map(|(name, value)| format!("{}=\"{}\"", name, value))
                    .collect::<Vec<_>>();

                if let Some(tag) = tag {
                    parts.push(format!("{}=\"{}\"", tag.0, tag.1));
                }

                parts.sort();
                write!(&mut self.result, "{{{}}}", parts.join(","))
            }
        }
        .ok();
    }

    pub(super) fn encode_header(&mut self, name: &str, fullname: &str, value: &MetricValue) {
        let r#type = value.prometheus_metric_type().as_str();
        writeln!(&mut self.result, "# HELP {} {}", fullname, name).ok();
        writeln!(&mut self.result, "# TYPE {} {}", fullname, r#type).ok();
    }
}

type Labels = Vec<proto::Label>;

pub(super) struct TimeSeries {
    buffer: IndexMap<Labels, Vec<proto::Sample>>,
    metadata: IndexMap<String, proto::MetricMetadata>,
}

impl TimeSeries {
    fn make_labels(
        tags: &Option<BTreeMap<String, String>>,
        name: &str,
        suffix: &str,
        extra: Option<(&str, String)>,
    ) -> Labels {
        // Each Prometheus metric is grouped by its labels, which
        // contains all the labels from the source metric, plus the name
        // label for the actual metric name. For convenience below, an
        // optional extra tag is added.
        let mut labels = tags.clone().unwrap_or_default();
        labels.insert(METRIC_NAME_LABEL.into(), [name, suffix].join(""));
        if let Some((name, value)) = extra {
            labels.insert(name.into(), value);
        }

        // Extract the labels into a vec and sort to produce a
        // consistent key for the buffer.
        let mut labels = labels
            .into_iter()
            .map(|(name, value)| proto::Label { name, value })
            .collect::<Labels>();
        labels.sort();
        labels
    }
}

impl MetricCollector for TimeSeries {
    type Output = proto::WriteRequest;

    fn new() -> Self {
        Self {
            buffer: Default::default(),
            metadata: Default::default(),
        }
    }

    fn emit_metadata(&mut self, name: &str, fullname: &str, value: &MetricValue) {
        if !self.metadata.contains_key(name) {
            let r#type = value.prometheus_metric_type();
            let metadata = proto::MetricMetadata {
                r#type: r#type as i32,
                metric_family_name: fullname.into(),
                help: name.into(),
                unit: String::new(),
            };
            self.metadata.insert(name.into(), metadata);
        }
    }

    fn emit_value(
        &mut self,
        timestamp_millis: Option<i64>,
        name: &str,
        suffix: &str,
        value: f64,
        tags: &Option<BTreeMap<String, String>>,
        extra: Option<(&str, String)>,
    ) {
        self.buffer
            .entry(Self::make_labels(tags, name, suffix, extra))
            .or_default()
            .push(proto::Sample {
                value,
                timestamp: timestamp_millis.unwrap_or(0),
            });
    }

    fn finish(self) -> proto::WriteRequest {
        let timeseries = self
            .buffer
            .into_iter()
            .map(|(labels, samples)| proto::TimeSeries { labels, samples })
            .collect::<Vec<_>>();
        let metadata = self
            .metadata
            .into_iter()
            .map(|(_, metadata)| metadata)
            .collect();
        proto::WriteRequest {
            timeseries,
            metadata,
        }
    }
}

impl MetricValue {
    fn prometheus_metric_type(&self) -> proto::MetricType {
        use proto::MetricType;
        match self {
            MetricValue::Counter { .. } => MetricType::Counter,
            MetricValue::Gauge { .. } | MetricValue::Set { .. } => MetricType::Gauge,
            MetricValue::Distribution {
                statistic: StatisticKind::Histogram,
                ..
            } => MetricType::Histogram,
            MetricValue::Distribution {
                statistic: StatisticKind::Summary,
                ..
            } => MetricType::Summary,
            MetricValue::AggregatedHistogram { .. } => MetricType::Histogram,
            MetricValue::AggregatedSummary { .. } => MetricType::Summary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::default_summary_quantiles;
    use super::*;
    use crate::event::metric::{Metric, MetricKind, MetricValue, StatisticKind};
    use pretty_assertions::assert_eq;

    fn encode_one<T: MetricCollector>(
        default_namespace: Option<&str>,
        buckets: &[f64],
        quantiles: &[f64],
        expired: bool,
        metric: &Metric,
    ) -> T::Output {
        let mut s = T::new();
        s.encode_metric(default_namespace, buckets, quantiles, expired, metric);
        s.finish()
    }

    fn tags() -> BTreeMap<String, String> {
        vec![("code".to_owned(), "200".to_owned())]
            .into_iter()
            .collect()
    }

    macro_rules! write_request {
        ( $name:literal, $help:literal, $type:ident
          [ $(
              $suffix:literal @ $timestamp:literal = $svalue:literal
                  [ $( $label:literal => $lvalue:literal ),* ]
          ),* ]
        ) => {
            proto::WriteRequest {
                timeseries: vec![
                    $(
                        proto::TimeSeries {
                            labels: vec![
                                proto::Label {
                                    name: "__name__".into(),
                                    value: format!("{}{}", $name, $suffix),
                                },
                                $(
                                    proto::Label {
                                        name: $label.into(),
                                        value: $lvalue.into(),
                                    },
                                )*
                            ],
                            samples: vec![ proto::Sample {
                                value: $svalue,
                                timestamp: $timestamp,
                            }],
                        },
                    )*
                ],
                metadata: vec![proto::MetricMetadata {
                    r#type: proto::metric_metadata::MetricType::$type as i32,
                    metric_family_name: $name.into(),
                    help: $help.into(),
                    unit: "".into(),
                }],
            }
        };
    }

    #[test]
    fn encodes_counter_text() {
        assert_eq!(
            encode_counter::<StringCollector>(),
            r#"# HELP vector_hits hits
# TYPE vector_hits counter
vector_hits{code="200"} 10
"#
        );
    }

    #[test]
    fn encodes_counter_request() {
        assert_eq!(
            encode_counter::<TimeSeries>(),
            write_request!("vector_hits", "hits", Counter ["" @ 0 = 10.0 ["code" => "200"]])
        );
    }

    fn encode_counter<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "hits".to_owned(),
            namespace: None,
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Absolute,
            value: MetricValue::Counter { value: 10.0 },
        };
        encode_one::<T>(Some("vector"), &[], &[], false, &metric)
    }

    #[test]
    fn encodes_gauge_text() {
        assert_eq!(
            encode_gauge::<StringCollector>(),
            r#"# HELP vector_temperature temperature
# TYPE vector_temperature gauge
vector_temperature{code="200"} -1.1
"#
        );
    }

    #[test]
    fn encodes_gauge_request() {
        assert_eq!(
            encode_gauge::<TimeSeries>(),
            write_request!("vector_temperature", "temperature", Gauge ["" @ 0 = -1.1 ["code" => "200"]])
        );
    }

    fn encode_gauge<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "temperature".to_owned(),
            namespace: None,
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Absolute,
            value: MetricValue::Gauge { value: -1.1 },
        };
        encode_one::<T>(Some("vector"), &[], &[], false, &metric)
    }

    #[test]
    fn encodes_set_text() {
        assert_eq!(
            encode_set::<StringCollector>(),
            r#"# HELP vector_users users
# TYPE vector_users gauge
vector_users 1
"#
        );
    }

    #[test]
    fn encodes_set_request() {
        assert_eq!(
            encode_set::<TimeSeries>(),
            write_request!("vector_users", "users", Gauge [ "" @ 0 = 1.0 []])
        );
    }

    fn encode_set<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "users".to_owned(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Absolute,
            value: MetricValue::Set {
                values: vec!["foo".into()].into_iter().collect(),
            },
        };
        encode_one::<T>(Some("vector"), &[], &[], false, &metric)
    }

    #[test]
    fn encodes_expired_set_text() {
        assert_eq!(
            encode_expired_set::<StringCollector>(),
            r#"# HELP vector_users users
# TYPE vector_users gauge
vector_users 0
"#
        );
    }

    #[test]
    fn encodes_expired_set_request() {
        assert_eq!(
            encode_expired_set::<TimeSeries>(),
            write_request!("vector_users", "users", Gauge ["" @ 0 = 0.0 []])
        );
    }

    fn encode_expired_set<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "users".to_owned(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Absolute,
            value: MetricValue::Set {
                values: vec!["foo".into()].into_iter().collect(),
            },
        };
        encode_one::<T>(Some("vector"), &[], &[], true, &metric)
    }

    #[test]
    fn encodes_distribution_text() {
        assert_eq!(
            encode_distribution::<StringCollector>(),
            r#"# HELP vector_requests requests
# TYPE vector_requests histogram
vector_requests_bucket{le="0"} 0
vector_requests_bucket{le="2.5"} 6
vector_requests_bucket{le="5"} 8
vector_requests_bucket{le="+Inf"} 8
vector_requests_sum 15
vector_requests_count 8
"#
        );
    }

    #[test]
    fn encodes_distribution_request() {
        assert_eq!(
            encode_distribution::<TimeSeries>(),
            write_request!(
                "vector_requests", "requests", Histogram [
                        "_bucket" @ 0 = 0.0 ["le" => "0"],
                        "_bucket" @ 0 = 6.0 ["le" => "2.5"],
                        "_bucket" @ 0 = 8.0 ["le" => "5"],
                        "_bucket" @ 0 = 8.0 ["le" => "+Inf"],
                        "_sum" @ 0 = 15.0 [],
                        "_count" @ 0 = 8.0 []
                ]
            )
        );
    }

    fn encode_distribution<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "requests".to_owned(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Absolute,
            value: MetricValue::Distribution {
                values: vec![1.0, 2.0, 3.0],
                sample_rates: vec![3, 3, 2],
                statistic: StatisticKind::Histogram,
            },
        };
        encode_one::<T>(Some("vector"), &[0.0, 2.5, 5.0], &[], false, &metric)
    }

    #[test]
    fn encodes_histogram_text() {
        assert_eq!(
            encode_histogram::<StringCollector>(),
            r#"# HELP vector_requests requests
# TYPE vector_requests histogram
vector_requests_bucket{le="1"} 1
vector_requests_bucket{le="2.1"} 3
vector_requests_bucket{le="3"} 6
vector_requests_bucket{le="+Inf"} 6
vector_requests_sum 12.5
vector_requests_count 6
"#
        );
    }

    #[test]
    fn encodes_histogram_request() {
        assert_eq!(
            encode_histogram::<TimeSeries>(),
            write_request!(
                "vector_requests", "requests", Histogram [
                        "_bucket" @ 0 = 1.0 ["le" => "1"],
                        "_bucket" @ 0 = 3.0 ["le" => "2.1"],
                        "_bucket" @ 0 = 6.0 ["le" => "3"],
                        "_bucket" @ 0 = 6.0 ["le" => "+Inf"],
                        "_sum" @ 0 = 12.5 [],
                        "_count" @ 0 = 6.0 []
                    ]
            )
        );
    }

    fn encode_histogram<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "requests".to_owned(),
            namespace: None,
            timestamp: None,
            tags: None,
            kind: MetricKind::Absolute,
            value: MetricValue::AggregatedHistogram {
                buckets: vec![1.0, 2.1, 3.0],
                counts: vec![1, 2, 3],
                count: 6,
                sum: 12.5,
            },
        };
        encode_one::<T>(Some("vector"), &[], &[], false, &metric)
    }

    #[test]
    fn encodes_summary_text() {
        assert_eq!(
            encode_summary::<StringCollector>(),
            r#"# HELP ns_requests requests
# TYPE ns_requests summary
ns_requests{code="200",quantile="0.01"} 1.5
ns_requests{code="200",quantile="0.5"} 2
ns_requests{code="200",quantile="0.99"} 3
ns_requests_sum{code="200"} 12
ns_requests_count{code="200"} 6
"#
        );
    }

    #[test]
    fn encodes_summary_request() {
        assert_eq!(
            encode_summary::<TimeSeries>(),
            write_request!(
                "ns_requests", "requests", Summary [
                    "" @ 0 = 1.5 ["code" => "200", "quantile" => "0.01"],
                    "" @ 0 = 2.0 ["code" => "200", "quantile" => "0.5"],
                    "" @ 0 = 3.0 ["code" => "200", "quantile" => "0.99"],
                    "_sum" @ 0 = 12.0 ["code" => "200"],
                    "_count" @ 0 = 6.0 ["code" => "200"]
                ]
            )
        );
    }

    fn encode_summary<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "requests".to_owned(),
            namespace: None,
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Absolute,
            value: MetricValue::AggregatedSummary {
                quantiles: vec![0.01, 0.5, 0.99],
                values: vec![1.5, 2.0, 3.0],
                count: 6,
                sum: 12.0,
            },
        };
        encode_one::<T>(Some("ns"), &[], &[], false, &metric)
    }

    #[test]
    fn encodes_distribution_summary_text() {
        assert_eq!(
            encode_distribution_summary::<StringCollector>(),
            r#"# HELP ns_requests requests
# TYPE ns_requests summary
ns_requests{code="200",quantile="0.5"} 2
ns_requests{code="200",quantile="0.75"} 2
ns_requests{code="200",quantile="0.9"} 3
ns_requests{code="200",quantile="0.95"} 3
ns_requests{code="200",quantile="0.99"} 3
ns_requests_sum{code="200"} 15
ns_requests_count{code="200"} 8
ns_requests_min{code="200"} 1
ns_requests_max{code="200"} 3
ns_requests_avg{code="200"} 1.875
"#
        );
    }

    #[test]
    fn encodes_distribution_summary_request() {
        assert_eq!(
            encode_distribution_summary::<TimeSeries>(),
            write_request!(
                "ns_requests", "requests", Summary [
                    "" @ 0 = 2.0 ["code" => "200", "quantile" => "0.5"],
                    "" @ 0 = 2.0 ["code" => "200", "quantile" => "0.75"],
                    "" @ 0 = 3.0 ["code" => "200", "quantile" => "0.9"],
                    "" @ 0 = 3.0 ["code" => "200", "quantile" => "0.95"],
                    "" @ 0 = 3.0 ["code" => "200", "quantile" => "0.99"],
                    "_sum" @ 0 = 15.0 ["code" => "200"],
                    "_count" @ 0 = 8.0 ["code" => "200"],
                    "_min" @ 0 = 1.0 ["code" => "200"],
                    "_max" @ 0 = 3.0 ["code" => "200"],
                    "_avg" @ 0 = 1.875 ["code" => "200"]
                ]
            )
        );
    }

    fn encode_distribution_summary<T: MetricCollector>() -> T::Output {
        let metric = Metric {
            name: "requests".to_owned(),
            namespace: None,
            timestamp: None,
            tags: Some(tags()),
            kind: MetricKind::Absolute,
            value: MetricValue::Distribution {
                values: vec![1.0, 2.0, 3.0],
                sample_rates: vec![3, 3, 2],
                statistic: StatisticKind::Summary,
            },
        };
        encode_one::<T>(
            Some("ns"),
            &[],
            &default_summary_quantiles(),
            false,
            &metric,
        )
    }

    #[test]
    fn encodes_timestamp_text() {
        assert_eq!(
            encode_timestamp::<StringCollector>(),
            r#"# HELP temperature temperature
# TYPE temperature counter
temperature 2 1234567890123
"#
        );
    }

    #[test]
    fn encodes_timestamp_request() {
        assert_eq!(
            encode_timestamp::<TimeSeries>(),
            write_request!("temperature", "temperature", Counter ["" @ 1234567890123 = 2.0 []])
        );
    }

    fn encode_timestamp<T: MetricCollector>() -> T::Output {
        use chrono::{DateTime, NaiveDateTime, Utc};
        let metric = Metric {
            name: "temperature".to_owned(),
            namespace: None,
            timestamp: Some(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(1234567890, 123456789),
                Utc,
            )),
            tags: None,
            kind: MetricKind::Absolute,
            value: MetricValue::Counter { value: 2.0 },
        };
        encode_one::<T>(None, &[], &[], false, &metric)
    }
}
