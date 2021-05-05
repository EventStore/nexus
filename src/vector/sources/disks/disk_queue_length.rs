/// A source that scraps `proc/diskstats` to extract the disk queue length.
/// Source: https://tipstricks.itmatrix.eu/procdiskstats-line-format
use futures::{FutureExt, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;
use vector::internal_events::InternalEvent;
use vector::{
    config::{self, SourceConfig, SourceContext, SourceDescription},
    event::{Metric, MetricKind, MetricValue},
    Event,
};

struct ParsingError(Box<dyn std::error::Error + Send + 'static>);

impl InternalEvent for ParsingError {
    fn emit_logs(&self) {
        error!(message = "Parsing error.", error = ?self.0);
    }

    fn emit_metrics(&self) {
        metrics::counter!("parse_error_total", 1);
    }
}

struct DiskNotFound;

impl InternalEvent for DiskNotFound {
    fn emit_logs(&self) {
        error!("No disk matching the regex rules found");
    }

    fn emit_metrics(&self) {
        metrics::counter!("disk_not_found", 1);
    }
}

struct FileSystemError(std::io::Error);

impl InternalEvent for FileSystemError {
    fn emit_logs(&self) {
        error!(message = "Filesystem error.", error = ?self.0);
    }

    fn emit_metrics(&self) {
        metrics::counter!("filesystem_error", 1);
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct DiskQueueLengthConfig {
    #[serde(default = "default_scrape_interval_secs")]
    scrape_interval_secs: u64,

    #[serde(default)]
    regexes: Vec<String>,

    #[serde(default)]
    namespace: String,
}

pub fn default_scrape_interval_secs() -> u64 {
    3
}

inventory::submit! {
    SourceDescription::new::<DiskQueueLengthConfig>("disk_queue_length")
}

vector::impl_generate_config_from_default!(DiskQueueLengthConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "disk_queue_length")]
impl SourceConfig for DiskQueueLengthConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<vector::sources::Source> {
        let mut out = cx
            .out
            .sink_map_err(|error| error!(message = "Error sending metric.", %error));
        let mut ticks = IntervalStream::new(tokio::time::interval(Duration::from_secs(
            self.scrape_interval_secs,
        )))
        .take_until(cx.shutdown);

        // let disk_name = self.disk_name.clone();
        let mut disk_regexes = Vec::new();

        for regex in self.regexes.iter() {
            disk_regexes.push(regex::Regex::new(regex.as_str())?);
        }

        let namespace = if self.namespace.is_empty() {
            None
        } else {
            Some(self.namespace.clone())
        };

        Ok(Box::pin(
            async move {
                while ticks.next().await.is_some() {
                    match tokio::fs::read("/proc/diskstats").await {
                        Err(e) => {
                            vector::emit!(FileSystemError(e));
                            continue;
                        }

                        Ok(content) => match String::from_utf8(content) {
                            Err(e) => {
                                vector::emit!(ParsingError(Box::new(e)));
                                continue;
                            }

                            Ok(content) => {
                                let mut metric = None;

                                for line in content.lines() {
                                    let mut index = 0usize;
                                    for column in line.split_whitespace() {
                                        if index == 2 {
                                            if !disk_regexes
                                                .iter()
                                                .any(|regex| regex.is_match(column))
                                            {
                                                break;
                                            }
                                        }

                                        // Position of the disk queue length value.
                                        if index == 11 {
                                            match column.parse::<usize>() {
                                                Err(e) => {
                                                    vector::emit!(ParsingError(Box::new(e)));
                                                    break;
                                                }

                                                Ok(value) => {
                                                    let mut tags =
                                                        std::collections::BTreeMap::new();

                                                    tags.insert(
                                                        "disk".to_string(),
                                                        column.to_string(),
                                                    );

                                                    metric = Some(
                                                        Metric::new(
                                                            "disk_queue_length",
                                                            MetricKind::Absolute,
                                                            MetricValue::Gauge {
                                                                value: value as f64,
                                                            },
                                                        )
                                                        .with_namespace(namespace.clone())
                                                        .with_tags(Some(tags))
                                                        .with_timestamp(Some(chrono::Utc::now())),
                                                    );
                                                }
                                            }

                                            break;
                                        }

                                        index += 1;
                                    }

                                    if metric.is_some() {
                                        break;
                                    }
                                }

                                if let Some(metric) = metric {
                                    if out.send(Event::Metric(metric)).await.is_err() {
                                        break;
                                    }
                                } else {
                                    vector::emit!(DiskNotFound);
                                }
                            }
                        },
                    }
                }
            }
            .map(Ok)
            .boxed(),
        ))
    }

    fn output_type(&self) -> config::DataType {
        config::DataType::Metric
    }

    fn source_type(&self) -> &'static str {
        "disk_queue_length"
    }
}
