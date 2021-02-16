use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::proto,
    sinks::util::tcp::TcpSinkConfig,
    tcp::TcpKeepaliveConfig,
    tls::TlsConfig,
    Event,
};
use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct VectorSinkConfig {
    pub address: String,
    pub keepalive: Option<TcpKeepaliveConfig>,
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Missing host in address field"))]
    MissingHost,
    #[snafu(display("Missing port in address field"))]
    MissingPort,
}

inventory::submit! {
    SinkDescription::new::<VectorSinkConfig>("vector")
}

impl GenerateConfig for VectorSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "127.0.0.1:5000".to_string(),
            keepalive: None,
            tls: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector")]
impl SinkConfig for VectorSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let sink_config =
            TcpSinkConfig::new(self.address.clone(), self.keepalive, self.tls.clone());
        sink_config.build(cx, encode_event)
    }

    fn input_type(&self) -> DataType {
        DataType::Any
    }

    fn sink_type(&self) -> &'static str {
        "vector"
    }
}

#[derive(Debug, Snafu)]
enum HealthcheckError {
    #[snafu(display("Connect error: {}", source))]
    ConnectError { source: std::io::Error },
}

fn encode_event(event: Event) -> Option<Bytes> {
    let event = proto::EventWrapper::from(event);
    let event_len = event.encoded_len();
    let full_len = event_len + 4;

    let mut out = BytesMut::with_capacity(full_len);
    out.put_u32(event_len as u32);
    event.encode(&mut out).unwrap();

    Some(out.into())
}

// #[cfg(test)]
// mod test {
//     #[test]
//     fn generate_config() {
//         crate::test_util::test_generate_config::<super::VectorSinkConfig>();
//     }
// }
