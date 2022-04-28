use eventstore::operations::{MemberInfo, VNodeState};
use futures::{stream, FutureExt, SinkExt, StreamExt};
use rand::{RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
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
    #[serde(default = "default_scrape_interval_secs")]
    scrape_interval_secs: u64,
    default_namespace: Option<String>,
}

pub fn default_scrape_interval_secs() -> u64 {
    3
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
    let mut ticks = IntervalStream::new(tokio::time::interval(Duration::from_secs(
        config.scrape_interval_secs,
    )))
    .take_until(cx.shutdown);

    let namespace = config
        .default_namespace
        .clone()
        .unwrap_or_else(|| "eventstoredb".to_string());

    Ok(Box::pin(
        async move {
            let mut epoch_number = None;
            let mut gen = rand::rngs::SmallRng::from_entropy();

            while ticks.next().await.is_some() {
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

                        if leaders.is_empty() {
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

                        if dead_count > 0 {
                            metrics.push(
                                Metric::new(
                                    "dead_nodes",
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

                        let current_epoch_number = if leaders.len() == 1 {
                            leaders[0].epoch_number
                        } else {
                            let idx = gen.next_u32() % members.len() as u32;
                            members[idx as usize].epoch_number
                        };

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
