use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
use vector::event::{Metric, MetricKind, MetricValue};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub proc: Proc,
    pub sys: Sys,
    pub es: Es,
}

impl Stats {
    pub fn metrics(&self, namespace: Option<String>) -> Vec<Metric> {
        let mut result = Vec::new();
        let mut tags = BTreeMap::new();
        let now = chrono::Utc::now();
        let namespace = namespace.unwrap_or_else(|| "eventstoredb".to_string());

        tags.insert("id".to_string(), self.proc.id.to_string());

        result.push(
            Metric::new(
                "process_memory_used_bytes",
                MetricKind::Absolute,
                MetricValue::Gauge {
                    value: self.proc.mem as f64,
                },
            )
            .with_namespace(Some(namespace.clone()))
            .with_tags(Some(tags.clone()))
            .with_timestamp(Some(now)),
        );

        result.push(
            Metric::new(
                "disk_io_read_bytes",
                MetricKind::Absolute,
                MetricValue::Counter {
                    value: self.proc.disk_io.read_bytes as f64,
                },
            )
            .with_namespace(Some(namespace.clone()))
            .with_tags(Some(tags.clone()))
            .with_timestamp(Some(now)),
        );

        result.push(
            Metric::new(
                "disk_io_written_bytes",
                MetricKind::Absolute,
                MetricValue::Counter {
                    value: self.proc.disk_io.written_bytes as f64,
                },
            )
            .with_namespace(Some(namespace.clone()))
            .with_tags(Some(tags.clone()))
            .with_timestamp(Some(now)),
        );

        result.push(
            Metric::new(
                "disk_io_read_ops",
                MetricKind::Absolute,
                MetricValue::Counter {
                    value: self.proc.disk_io.read_ops as f64,
                },
            )
            .with_namespace(Some(namespace.clone()))
            .with_tags(Some(tags.clone()))
            .with_timestamp(Some(now)),
        );

        result.push(
            Metric::new(
                "disk_io_write_ops",
                MetricKind::Absolute,
                MetricValue::Counter {
                    value: self.proc.disk_io.write_ops as f64,
                },
            )
            .with_namespace(Some(namespace.clone()))
            .with_tags(Some(tags.clone()))
            .with_timestamp(Some(now)),
        );

        result.push(
            Metric::new(
                "free_memory",
                MetricKind::Absolute,
                MetricValue::Gauge {
                    value: self.sys.free_mem as f64,
                },
            )
            .with_namespace(Some(namespace.clone()))
            .with_tags(Some(tags.clone()))
            .with_timestamp(Some(now)),
        );

        for queue in self.es.queues.iter() {
            let mut queue_tags = tags.clone();
            let type_suffix = queue_name_type_suffix(queue.name.as_str());
            queue_tags.insert("name".to_string(), queue.name.clone());
            result.push(
                Metric::new(
                    format!("queue_length_{}", type_suffix),
                    MetricKind::Absolute,
                    MetricValue::Gauge {
                        value: queue.length as f64,
                    },
                )
                .with_namespace(Some(namespace.clone()))
                .with_tags(Some(queue_tags.clone()))
                .with_timestamp(Some(now)),
            );

            result.push(
                Metric::new(
                    format!("queue_avg_processing_time_{}", type_suffix),
                    MetricKind::Absolute,
                    MetricValue::Gauge {
                        value: queue.avg_processing_time,
                    },
                )
                .with_namespace(Some(namespace.clone()))
                .with_tags(Some(queue_tags.clone()))
                .with_timestamp(Some(now)),
            );
        }

        if let Some(drive) = self.sys.drive.as_ref() {
            tags.insert("path".to_string(), drive.path.clone());

            result.push(
                Metric::new(
                    "drive_total_bytes",
                    MetricKind::Absolute,
                    MetricValue::Gauge {
                        value: drive.stats.total_bytes as f64,
                    },
                )
                .with_namespace(Some(namespace.clone()))
                .with_tags(Some(tags.clone()))
                .with_timestamp(Some(now)),
            );

            result.push(
                Metric::new(
                    "drive_available_bytes",
                    MetricKind::Absolute,
                    MetricValue::Gauge {
                        value: drive.stats.available_bytes as f64,
                    },
                )
                .with_namespace(Some(namespace.clone()))
                .with_tags(Some(tags.clone()))
                .with_timestamp(Some(now)),
            );

            result.push(
                Metric::new(
                    "drive_used_bytes",
                    MetricKind::Absolute,
                    MetricValue::Gauge {
                        value: drive.stats.used_bytes as f64,
                    },
                )
                .with_namespace(Some(namespace))
                .with_tags(Some(tags))
                .with_timestamp(Some(now)),
            );
        }

        result
    }
}

fn queue_name_type_suffix(name: &str) -> String {
    name.replace(' ', "_").replace('#', "_")
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Proc {
    pub id: usize,
    pub mem: usize,
    pub cpu: f64,
    pub threads_count: i64,
    pub thrown_exceptions_rate: f64,
    pub disk_io: DiskIo,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DiskIo {
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

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Es {
    #[serde(rename = "queue", deserialize_with = "deserialize_queues")]
    pub queues: Vec<Queue>,
}

fn deserialize_queues<'de, D>(
    deserializer: D,
) -> Result<Vec<Queue>, <D as Deserializer<'de>>::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_map(QueuesVisitor)
}

struct QueuesVisitor;

impl<'de> Visitor<'de> for QueuesVisitor {
    type Value = Vec<Queue>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Queues object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, <A as MapAccess<'de>>::Error>
    where
        A: MapAccess<'de>,
    {
        let mut queues: Vec<Queue> = Vec::new();
        while map.next_key::<String>()?.is_some() {
            queues.push(map.next_value()?);
        }

        Ok(queues)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Queue {
    #[serde(rename = "queueName")]
    pub name: String,

    pub length: usize,

    pub avg_processing_time: f64,
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
