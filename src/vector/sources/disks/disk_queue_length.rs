/// A source that scraps `proc/diskstats` to extract the disk queue length.
/// Source: https://tipstricks.itmatrix.eu/procdiskstats-line-format
use futures::{FutureExt, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;
use vector::internal_events::InternalEvent;
use vector::{
    config::{self, SourceConfig, SourceContext, SourceDescription},
    event::{Event, Metric, MetricKind, MetricValue},
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

#[derive(Debug, PartialEq)]
pub struct DiskQueueLengthResult {
    pub disk: String,
    pub value: f64,
}

pub async fn get_disk_queue_length(
    file_path: impl AsRef<std::path::Path>,
    disk_regexes: &Vec<regex::Regex>,
) -> Vec<DiskQueueLengthResult> {
    let mut results = Vec::new();

    match tokio::fs::read(file_path).await {
        Err(e) => {
            vector::emit!(FileSystemError(e));
        }
        Ok(content) => match String::from_utf8(content) {
            Err(e) => {
                vector::emit!(ParsingError(Box::new(e)));
            }
            Ok(content) => {
                for line in content.lines() {
                    let mut words = line.split_whitespace();
                    if let Some(word) = words.nth(2) {
                        if disk_regexes.iter().any(|regex| regex.is_match(word)) {
                            let disk = word.to_string();
                            if let Some(word) = words.nth(8) {
                                match word.parse::<usize>() {
                                    Err(e) => {
                                        vector::emit!(ParsingError(Box::new(e)));
                                    }
                                    Ok(value) => {
                                        results.push(DiskQueueLengthResult {
                                            disk,
                                            value: value as f64,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    };
    return results;
}

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
                    let results = get_disk_queue_length("/proc/diskstats", &disk_regexes).await;
                    if results.len() < 1 {
                        vector::emit!(DiskNotFound);
                    } else {
                        let timestamp = chrono::Utc::now();
                        for r in results {
                            let mut tags = std::collections::BTreeMap::new();

                            tags.insert("disk".to_string(), r.disk.to_string());
                            let metric = Metric::new(
                                "disk_queue_length",
                                MetricKind::Absolute,
                                MetricValue::Gauge { value: r.value },
                            )
                            .with_namespace(namespace.clone())
                            .with_tags(Some(tags))
                            .with_timestamp(Some(timestamp));
                            if out.send(Event::Metric(metric)).await.is_err() {
                                break;
                            }
                        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::main]
    #[test]
    async fn test_get_disk_queue_length() {
        struct Test {
            pub diskstats: &'static str,
            pub expected_results: Vec<DiskQueueLengthResult>,
        }

        let tests = vec![
            // A typical diskstats file
            Test {
                diskstats: r#"
   1       0 loop0 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0
   1       1 loop1 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0
   1       2 loop2 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0   
   2       0 sda   1 0 4  8  9 0 13 14   2 1 2 0 0 0 0
   3       2 sda1  2 0 5 10 12 0 15  1   1 2 2 0 0 0 0
   4       3 sda2  3 0 6  7  0 0  0  0   0 1 2 0 0 0 0
     "#,
                expected_results: vec![
                    DiskQueueLengthResult {
                        disk: "sda".to_string(),
                        value: 2.0,
                    },
                    DiskQueueLengthResult {
                        disk: "sda1".to_string(),
                        value: 1.0,
                    },
                    DiskQueueLengthResult {
                        disk: "sda2".to_string(),
                        value: 0.0,
                    },
                ],
            },
            Test {
                diskstats: r#"
     "#,
                expected_results: vec![],
            },
            // This one has a float instead of an integer for sda2
            Test {
                diskstats: r#"
   1       0 loop0 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0
   1       1 loop1 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0
   1       2 loop2 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0   
   2       0 sda   1 0 4  8  9 0 13 14   2 1 2 0 0 0 0
   3       2 sda1  2 0 5 10 12 0 15  1   1 2 2 0 0 0 0
   4       3 sda2  3 0 6  7  0 0  0  0 4.5 1 2 0 0 0 0
     "#,
                expected_results: vec![
                    DiskQueueLengthResult {
                        disk: "sda".to_string(),
                        value: 2.0,
                    },
                    DiskQueueLengthResult {
                        disk: "sda1".to_string(),
                        value: 1.0,
                    },
                ],
            },
            // this one is malformed
            Test {
                diskstats: r#"
   1       0 loop0 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0
   1       1 loop1 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0
   1       2 loop2 0 0 0  0  0 0  0  0   0 0 0 0 0 0 0   
   2       0 sda   1 0 4  8 
   3       2 sda1  2 0 5 10 12 0 15  1   1 2 2 0 0 0 0
   4  
   5       3 sda2  
     "#,
                expected_results: vec![DiskQueueLengthResult {
                    disk: "sda1".to_string(),
                    value: 1.0,
                }],
            },
        ];

        for test in tests {
            let mut file = tempfile::NamedTempFile::new().expect("couldn't make temp file");
            let r = std::io::Write::write(&mut file, test.diskstats.as_bytes());
            std::io::Write::flush(&mut file).expect("flush failed");
            let count = r.unwrap();
            assert_eq!(test.diskstats.len(), count);
            let file_path = file
                .path()
                .to_str()
                .expect("temp file not a string")
                .to_string();
            let disk_regexes: Vec<regex::Regex> =
                vec![regex::Regex::new("sda").expect("failure to make simple regex")];
            let results = get_disk_queue_length(&file_path, &disk_regexes).await;
            assert_eq!(test.expected_results, results);
        }
    }
    #[tokio::main]
    #[test]
    async fn test_get_disk_queue_length_when_file_is_missing() {
        let disk_regexes: Vec<regex::Regex> =
            vec![regex::Regex::new("sda").expect("failure to make simple regex")];
        // NOTE: test will fail if the file "jdfsuhvdshfvioushdfdsj" is present
        let result = get_disk_queue_length("jdfsuhvdshfvioushdfdsj", &disk_regexes).await;
        assert_eq!(Vec::<DiskQueueLengthResult>::new(), result);
    }
}
