use std::collections::BTreeMap;

use vector::{
    config::{DataType, SourceConfig, SourceContext, SourceDescription},
    event::LogEvent,
    shutdown::ShutdownSignal,
    Pipeline, Value,
};

use eventstore::{Client, ClientSettings, SubEvent, SubscribeToAllOptions, SubscriptionFilter};

use futures::{SinkExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ESDBConfig {
    connection_string: String,
    #[serde(default)]
    include_links: bool,
    filter: Option<ESDBConfigFilter>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
enum ESDBConfigFilter {
    #[serde(rename = "by_category")]
    Category(String),
    #[serde(rename = "by_stream")]
    Stream(String),
    #[serde(rename = "by_streams")]
    Streams(Vec<String>),
    #[serde(rename = "by_stream_regex")]
    StreamRegex(String),
    #[serde(rename = "by_event_type")]
    EventType(String),
    #[serde(rename = "by_event_types")]
    EventTypes(Vec<String>),
    #[serde(rename = "by_event_type_regex")]
    EventTypeRegex(String),
}

#[derive(Debug, Snafu)]
pub enum ESDBConfigError {
    #[snafu(display("Unable to parse connection_string: {}", source))]
    ParseConnectionString {
        source: eventstore::ClientSettingsParseError,
    },
}

impl ESDBConfig {
    pub(self) fn validate(&self) -> Result<ClientSettings, ESDBConfigError> {
        self.connection_string
            .parse::<ClientSettings>()
            .map_err(|err| ESDBConfigError::ParseConnectionString { source: err })
    }

    pub(self) fn begin(
        self,
        shutdown: ShutdownSignal,
        out: Pipeline,
        client_settings: ClientSettings,
    ) -> vector::sources::Source {
        Box::pin(self.inner(shutdown, out, client_settings))
    }

    async fn inner(
        self,
        shutdown: ShutdownSignal,
        mut out: Pipeline,
        client_settings: ClientSettings,
    ) -> Result<(), ()> {
        let client = Client::create(client_settings).await.map_err(|_| {
            error!(message = "Failed to create client");
        })?;

        let options = self.create_subscription_options();

        let mut stream = client
            .subscribe_to_all(&options)
            .await
            .map_err(|_| {
                error!(message = "Failed to start subscription");
            })?
            .take_until(shutdown);

        while let Some(event) = stream.try_next().await.map_err(|_| {
            error!(message = "Failed to get next event from stream");
        })? {
            if let SubEvent::EventAppeared(resolved_event) = event {
                if let Some(event) = resolved_event.event {
                    let event = recorded_event_to_vector_event(event);
                    out.send(event).await.map_err(|_| {
                        error!(message = "Failed to forward events; downstream is closed.");
                    })?;
                }

                if self.include_links {
                    if let Some(link) = resolved_event.link {
                        let event = recorded_event_to_vector_event(link);
                        out.send(event).await.map_err(|_| {
                            error!(message = "Failed to forward events; downstream is closed.");
                        })?;
                    }
                }
            }
        }

        Ok(())
    }

    fn create_subscription_options(&self) -> SubscribeToAllOptions {
        let mut options = SubscribeToAllOptions::default();

        options = if self.include_links {
            options.resolve_link_tos()
        } else {
            options
        };

        let create_regex: fn(&String) -> String =
            |stream_name| format!("^{}$", regex::escape(stream_name));

        if self.filter.is_none() {
            return options;
        }

        let filter = match self.filter.as_ref().unwrap() {
            ESDBConfigFilter::Category(category) => SubscriptionFilter::on_stream_name()
                .regex(create_regex(&format!("$ce-{}", category))),

            ESDBConfigFilter::Stream(stream_name) => {
                SubscriptionFilter::on_stream_name().regex(create_regex(&stream_name))
            }
            ESDBConfigFilter::Streams(streams) => SubscriptionFilter::on_stream_name().regex(
                streams
                    .iter()
                    .map(create_regex)
                    .collect::<Vec<String>>()
                    .join("|"),
            ),
            ESDBConfigFilter::StreamRegex(regex) => {
                SubscriptionFilter::on_stream_name().regex(regex)
            }

            ESDBConfigFilter::EventType(event_type) => {
                SubscriptionFilter::on_event_type().regex(create_regex(&event_type))
            }
            ESDBConfigFilter::EventTypes(event_types) => SubscriptionFilter::on_event_type().regex(
                event_types
                    .iter()
                    .map(create_regex)
                    .collect::<Vec<String>>()
                    .join("|"),
            ),
            ESDBConfigFilter::EventTypeRegex(regex) => {
                SubscriptionFilter::on_event_type().regex(regex)
            }
        };

        options.filter(filter)
    }
}

fn data_to_value(data: bytes::Bytes) -> Value {
    if let Ok(string) = std::str::from_utf8(&data) {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(string) {
            return Value::from(json);
        }
    }

    Value::from(data)
}

fn recorded_event_to_vector_event(recorded_event: eventstore::RecordedEvent) -> vector::Event {
    let mut log = LogEvent::default();

    log.insert("stream_id", recorded_event.stream_id);
    log.insert("id", recorded_event.id.to_string());
    log.insert("revision", recorded_event.revision as i64);
    log.insert("event_type", recorded_event.event_type);
    log.insert("is_json", recorded_event.is_json);

    if recorded_event.is_json {
        log.insert("data", data_to_value(recorded_event.data));
    } else {
        log.insert("data", recorded_event.data);
    }

    log.insert(
        "custom_metadata",
        data_to_value(recorded_event.custom_metadata),
    );

    log.insert(
        "metadata",
        recorded_event
            .metadata
            .into_iter()
            .map(|(key, value)| (key, Value::from(value)))
            .collect::<BTreeMap<String, Value>>(),
    );

    let mut position = BTreeMap::new();
    position.insert(
        "commit".to_string(),
        Value::from(recorded_event.position.commit as i64),
    );
    position.insert(
        "prepare".to_string(),
        Value::from(recorded_event.position.prepare as i64),
    );
    log.insert("position", Value::Map(position));

    vector::Event::from(log)
}

inventory::submit! {
    SourceDescription::new::<ESDBConfig>("generator")
}

vector::impl_generate_config_from_default!(ESDBConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "eventstoredb")]
impl SourceConfig for ESDBConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<vector::sources::Source> {
        let client_settings = self.validate()?;
        Ok(self.clone().begin(cx.shutdown, cx.out, client_settings))
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn source_type(&self) -> &'static str {
        "generator"
    }
}
