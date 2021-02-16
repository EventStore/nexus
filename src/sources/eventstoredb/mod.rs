use self::types::{create_http_client, Stats};
use crate::{
    config::{self, GlobalOptions, SourceConfig, SourceDescription},
    shutdown::ShutdownSignal,
    Event, Pipeline,
};
use bytes::BytesMut;
use futures::{stream, FutureExt, SinkExt, StreamExt};
use hyper::body::HttpBody;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod types;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct EventStoreDBConfig {
    // Deprecated name
    #[serde(alias = "endpoint")]
    endpoint: String,
    #[serde(default = "default_scrape_interval_secs")]
    scrape_interval_secs: u64,
    namespace: Option<String>,
}

pub fn default_scrape_interval_secs() -> u64 {
    3
}

inventory::submit! {
    SourceDescription::new::<EventStoreDBConfig>("eventstoredb")
}

impl_generate_config_from_default!(EventStoreDBConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "eventstoredb")]
impl SourceConfig for EventStoreDBConfig {
    async fn build(
        &self,
        _name: &str,
        _globals: &GlobalOptions,
        shutdown: ShutdownSignal,
        out: Pipeline,
    ) -> crate::Result<super::Source> {
        Ok(eventstoredb(
            self.endpoint.clone(),
            self.scrape_interval_secs,
            shutdown,
            out,
            self.namespace.clone(),
        ))
    }

    fn output_type(&self) -> config::DataType {
        config::DataType::Metric
    }

    fn source_type(&self) -> &'static str {
        "eventstoredb"
    }
}

fn eventstoredb(
    endpoint: String,
    interval: u64,
    shutdown: ShutdownSignal,
    out: Pipeline,
    namespace: Option<String>,
) -> super::Source {
    let mut out = out.sink_map_err(|e| error!("error sending metric: {:?}", e));

    let mut ticks = tokio::time::interval(Duration::from_secs(interval)).take_until(shutdown);
    let client = create_http_client(endpoint.to_string().as_str());

    Box::pin(
        async move {
            while let Some(_) = ticks.next().await {
                let url: http::Uri = format!("{}/stats", client.base_url.as_str())
                    .parse()
                    .expect("Wrong stats url!");
                let req = hyper::Request::get(&url)
                    .header("content-type", "application/json")
                    .body(hyper::Body::empty())
                    .unwrap();

                match client.inner.request(req).await {
                    Err(e) => {
                        error!("Error when pulling stats from EventStoreDB: {:?}", e);
                        continue;
                    }

                    Ok(resp) => {
                        let mut bytes = BytesMut::new();
                        let mut body = resp.into_body();
                        let mut failed = false;

                        while let Some(content) = body.data().await {
                            match content {
                                Err(e) => {
                                    error!("Error when streaming stats from EventStoreDB: {:?}", e);
                                    failed = true;
                                    break;
                                }

                                Ok(content) => {
                                    bytes.extend(content);
                                }
                            }
                        }

                        if failed {
                            continue;
                        }

                        let bytes = bytes.freeze();
                        match serde_json::from_slice::<Stats>(bytes.as_ref()) {
                            Err(e) => {
                                error!("Error when parsing stats JSON from EventStoreDB: {:?}", e);
                            }

                            Ok(stats) => {
                                let metrics = stats.metrics(namespace.clone());
                                let mut metrics = stream::iter(metrics).map(Event::Metric).map(Ok);

                                emit!(stats);

                                if let Err(_) = out.send_all(&mut metrics).await {
                                    error!("Error sending eventstoredb metrics");
                                }
                            }
                        }
                    }
                }
            }
        }
        .map(Ok)
        .boxed(),
    )
}
