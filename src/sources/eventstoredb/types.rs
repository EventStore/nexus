use crate::event::{Metric, MetricKind, MetricValue};
use crate::internal_events::InternalEvent;
use hyper::client::HttpConnector;
use hyper::Body;
use hyper_openssl::HttpsConnector;
use metrics::gauge;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub proc: Proc,
    pub sys: Sys,
}

impl Stats {
    pub fn metrics(&self, namespace: Option<String>) -> Vec<Metric> {
        let mut result = Vec::new();
        let mut tags = BTreeMap::new();
        let namespace = namespace.unwrap_or_else(|| "eventstoredb".to_string());

        tags.insert("id".to_string(), self.proc.id.to_string());

        result.push(Metric {
            value: MetricValue::Gauge {
                value: self.proc.mem as f64,
            },
            name: "memory_usage".to_string(),
            kind: MetricKind::Absolute,
            tags: Some(tags.clone()),
            timestamp: None,
            namespace: Some(namespace.clone()),
        });

        result.push(Metric {
            value: MetricValue::Counter {
                value: self.proc.disk_io.read_bytes as f64,
            },
            name: "disk_io_read_bytes".to_string(),
            kind: MetricKind::Absolute,
            tags: Some(tags.clone()),
            timestamp: None,
            namespace: Some(namespace.clone()),
        });

        result.push(Metric {
            value: MetricValue::Counter {
                value: self.proc.disk_io.written_bytes as f64,
            },
            name: "disk_io_written_bytes".to_string(),
            kind: MetricKind::Absolute,
            tags: Some(tags.clone()),
            timestamp: None,
            namespace: Some(namespace.clone()),
        });

        result.push(Metric {
            value: MetricValue::Counter {
                value: self.proc.disk_io.read_ops as f64,
            },
            name: "disk_io_read_ops".to_string(),
            kind: MetricKind::Absolute,
            tags: Some(tags.clone()),
            timestamp: None,
            namespace: Some(namespace.clone()),
        });

        result.push(Metric {
            value: MetricValue::Counter {
                value: self.proc.disk_io.write_ops as f64,
            },
            name: "disk_io_write_ops".to_string(),
            kind: MetricKind::Absolute,
            tags: Some(tags.clone()),
            timestamp: None,
            namespace: Some(namespace.clone()),
        });

        result.push(Metric {
            value: MetricValue::Gauge {
                value: self.sys.free_mem as f64,
            },
            name: "free_memory".to_string(),
            kind: MetricKind::Absolute,
            tags: Some(tags.clone()),
            timestamp: None,
            namespace: Some(namespace.clone()),
        });

        if let Some(drive) = self.sys.drive.as_ref() {
            tags.insert("path".to_string(), drive.path.clone());

            result.push(Metric {
                value: MetricValue::Gauge {
                    value: drive.stats.total_bytes as f64,
                },
                name: "drive_total_bytes".to_string(),
                kind: MetricKind::Absolute,
                tags: Some(tags.clone()),
                timestamp: None,
                namespace: Some(namespace.clone()),
            });

            result.push(Metric {
                value: MetricValue::Gauge {
                    value: drive.stats.available_bytes as f64,
                },
                name: "drive_available_bytes".to_string(),
                kind: MetricKind::Absolute,
                tags: Some(tags.clone()),
                timestamp: None,
                namespace: Some(namespace.clone()),
            });

            result.push(Metric {
                value: MetricValue::Gauge {
                    value: drive.stats.used_bytes as f64,
                },
                name: "drive_used_bytes".to_string(),
                kind: MetricKind::Absolute,
                tags: Some(tags.clone()),
                timestamp: None,
                namespace: Some(namespace.clone()),
            });
        }

        result
    }
}

impl InternalEvent for Stats {
    fn emit_logs(&self) {
        debug!("EventStoreDB stats collected")
    }

    fn emit_metrics(&self) {
        gauge!("memory_usage_total", self.proc.mem as f64);
        gauge!(
            "disk_io_read_bytes_total",
            self.proc.disk_io.read_bytes as f64
        );
        gauge!(
            "disk_io_written_bytes_total",
            self.proc.disk_io.written_bytes as f64
        );
        gauge!("disk_io_read_ops_total", self.proc.disk_io.read_ops as f64);
        gauge!(
            "disk_io_write_ops_total",
            self.proc.disk_io.write_ops as f64
        );
        gauge!("free_memory_total", self.sys.free_mem as f64);

        if let Some(drive) = self.sys.drive.as_ref() {
            gauge!("drive_total_bytes", drive.stats.total_bytes as f64);
            gauge!("drive_available_bytes", drive.stats.available_bytes as f64);
            gauge!("drive_used_bytes", drive.stats.used_bytes as f64);
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Proc {
    pub id: usize,
    pub mem: usize,
    pub cpu: f64,
    pub threads_count: i64,
    pub thrown_exceptions_rate: f64,
    pub disk_io: DiskIO,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DiskIO {
    pub read_bytes: usize,
    pub written_bytes: usize,
    pub read_ops: usize,
    pub write_ops: usize,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Sys {
    pub free_mem: usize,
    pub loadavg: LoadAvg,
    pub drive: Option<Drive>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LoadAvg {
    #[serde(rename = "1m")]
    pub one_m: f64,
    #[serde(rename = "5m")]
    pub five_m: f64,
    #[serde(rename = "15m")]
    pub fifteen_m: f64,
}

#[derive(Debug)]
pub struct Drive {
    pub path: String,
    pub stats: DriveStats,
}

impl<'de> Deserialize<'de> for Drive {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(DriveVisitor)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DriveStats {
    pub available_bytes: usize,
    pub total_bytes: usize,
    pub usage: String,
    pub used_bytes: usize,
}

struct DriveVisitor;

impl<'de> Visitor<'de> for DriveVisitor {
    type Value = Drive;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "DriveStats object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, <A as MapAccess<'de>>::Error>
    where
        A: MapAccess<'de>,
    {
        if let Some(key) = map.next_key()? {
            return Ok(Drive {
                path: key,
                stats: map.next_value()?,
            });
        }

        Err(serde::de::Error::missing_field("<Drive path>"))
    }
}

#[derive(Clone)]
pub struct Client {
    pub base_url: String,
    pub inner: hyper::Client<HttpsConnector<HttpConnector>, Body>,
}

pub fn create_http_client(base_url: &str) -> Client {
    let http = hyper_openssl::HttpsConnector::new();

    Client {
        base_url: base_url.to_string(),
        inner: hyper::Client::builder().build(http.expect("Wrong openssl system configuration.")),
    }
}
