use eventstore::operations::{MemberInfo, VNodeState};
use futures::{stream, FutureExt, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio_stream::wrappers::IntervalStream;
use vector::event::{Metric, MetricKind, MetricValue};
use vector::{
    config::{self, SourceConfig, SourceContext, SourceDescription},
    event::Event,
};

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct EventStoreDbConfigNew {
    #[serde(default = "default_connection_string")]
    connection_string: String,
    #[serde(default = "default_frequency_secs")]
    frequency_secs: u64,
    default_namespace: Option<String>,
}

pub fn default_frequency_secs() -> u64 {
    2
}

pub fn default_connection_string() -> String {
    "esdb://localhost:2113".to_string()
}

inventory::submit! {
    SourceDescription::new::<EventStoreDbConfigNew>("eventstoredb_nexus_cluster_metrics")
}

vector::impl_generate_config_from_default!(EventStoreDbConfigNew);

#[async_trait::async_trait]
#[typetag::serde(name = "eventstoredb_nexus_cluster_metrics")]
impl SourceConfig for EventStoreDbConfigNew {
    async fn build(&self, cx: SourceContext) -> crate::Result<vector::sources::Source> {
        let setts = self.connection_string.parse()?;
        let client = eventstore::operations::Client::new(setts);

        source(self, client, cx)
    }

    fn output_type(&self) -> config::DataType {
        config::DataType::Metric
    }

    fn source_type(&self) -> &'static str {
        "eventstoredb_nexus_cluster_metrics"
    }
}

fn source(
    config: &EventStoreDbConfigNew,
    client: eventstore::operations::Client,
    cx: SourceContext,
) -> crate::Result<vector::sources::Source> {
    let mut out = cx
        .out
        .sink_map_err(|error| error!(message = "Error sending metric.", %error));

    let mut ticks = IntervalStream::new(tokio::time::interval(Duration::from_millis(500)))
        .take_until(cx.shutdown);

    let namespace = config
        .default_namespace
        .clone()
        .unwrap_or_else(|| "eventstoredb".to_string());

    let frequency = Duration::from_secs(config.frequency_secs);

    Ok(Box::pin(
        async move {
            let mut epoch_number = None;
            let mut leader_writer_checkpoint: Option<i64> = None;
            let mut clock = Instant::now();

            while ticks.next().await.is_some() {
                if clock.elapsed() < frequency {
                    continue;
                }

                clock = Instant::now();

                match client.read_gossip().await {
                    Err(error) => {
                        tracing::error!(target: "eventstoredb_nexus_cluster_metrics", "{}", error)
                    }
                    Ok(members) => {
                        let now = chrono::Utc::now();
                        let tags = BTreeMap::new();
                        let mut metrics = Vec::new();
                        let dead_count = members.iter().filter(|m| !m.is_alive).count();
                        let leaders = members
                            .iter()
                            .filter(|m| m.state == VNodeState::Leader)
                            .collect::<Vec<&MemberInfo>>();

                        if dead_count > 0 {
                            metrics.push(
                                Metric::new(
                                    "unresponsive_nodes",
                                    MetricKind::Incremental,
                                    MetricValue::Counter {
                                        value: dead_count as f64,
                                    },
                                )
                                .with_namespace(Some(namespace.clone()))
                                .with_tags(Some(tags.clone()))
                                .with_timestamp(Some(now)),
                            );
                        }

                        if leaders.len() != 1 {
                            metrics.push(
                                Metric::new(
                                    "leader_mismatches",
                                    MetricKind::Incremental,
                                    MetricValue::Counter { value: 1.0 },
                                )
                                .with_namespace(Some(namespace.clone()))
                                .with_tags(Some(tags.clone()))
                                .with_timestamp(Some(now)),
                            );

                            continue;
                        }

                        let current_epoch_number = leaders[0].epoch_number;
                        let current_leader_writer_checkpoint = leaders[0].writer_checkpoint;

                        if let Some(epoch_number) = epoch_number.as_mut() {
                            if current_epoch_number != *epoch_number {
                                *epoch_number = current_epoch_number;

                                metrics.push(
                                    Metric::new(
                                        "elections",
                                        MetricKind::Incremental,
                                        MetricValue::Counter { value: 1.0 },
                                    )
                                    .with_namespace(Some(namespace.clone()))
                                    .with_tags(Some(tags.clone()))
                                    .with_timestamp(Some(now)),
                                );
                            }
                        } else {
                            epoch_number = Some(current_epoch_number);
                        }

                        if let Some(leader_writer_checkpoint) = leader_writer_checkpoint.as_mut() {
                            if *leader_writer_checkpoint > current_leader_writer_checkpoint {
                                metrics.push(
                                    Metric::new(
                                        "truncations",
                                        MetricKind::Incremental,
                                        MetricValue::Counter { value: 1.0 },
                                    )
                                    .with_namespace(Some(namespace.clone()))
                                    .with_tags(Some(tags.clone()))
                                    .with_timestamp(Some(now)),
                                );

                                metrics.push(
                                    Metric::new(
                                        "leader_mismatches",
                                        MetricKind::Incremental,
                                        MetricValue::Counter { value: 1.0 },
                                    )
                                    .with_namespace(Some(namespace.clone()))
                                    .with_tags(Some(tags.clone()))
                                    .with_timestamp(Some(now)),
                                );
                            }

                            let out_of_sync_count = members
                                .iter()
                                .filter(|m| m.state == VNodeState::Follower)
                                .filter(|m| m.writer_checkpoint < *leader_writer_checkpoint)
                                .count();

                            if out_of_sync_count > 1 {
                                metrics.push(
                                    Metric::new(
                                        "out_of_sync",
                                        MetricKind::Incremental,
                                        MetricValue::Counter { value: 1.0 },
                                    )
                                    .with_namespace(Some(namespace.clone()))
                                    .with_tags(Some(tags.clone()))
                                    .with_timestamp(Some(now)),
                                );
                            }
                        }

                        leader_writer_checkpoint = Some(current_leader_writer_checkpoint);

                        if let Some(epoch_number) = epoch_number.as_ref() {
                            metrics.push(
                                Metric::new(
                                    "leader_epoch_number",
                                    MetricKind::Absolute,
                                    MetricValue::Gauge {
                                        value: *epoch_number as f64,
                                    },
                                )
                                .with_namespace(Some(namespace.clone()))
                                .with_tags(Some(tags.clone()))
                                .with_timestamp(Some(now)),
                            );
                        }

                        if let Some(writer_checkpoint) = leader_writer_checkpoint.as_ref() {
                            metrics.push(
                                Metric::new(
                                    "leader_writer_checkpoint",
                                    MetricKind::Absolute,
                                    MetricValue::Gauge {
                                        value: *writer_checkpoint as f64,
                                    },
                                )
                                .with_namespace(Some(namespace.clone()))
                                .with_tags(Some(tags.clone()))
                                .with_timestamp(Some(now)),
                            );
                        }

                        if metrics.is_empty() {
                            continue;
                        }

                        let mut metrics = stream::iter(metrics).map(Event::Metric).map(Ok);
                        if out.send_all(&mut metrics).await.is_err() {
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
