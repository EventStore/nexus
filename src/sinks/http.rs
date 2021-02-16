use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::Event,
    http::{Auth, HttpClient, MaybeAuth},
    internal_events::{HTTPEventEncoded, HTTPEventMissingMessage},
    sinks::util::{
        buffer::compression::GZIP_DEFAULT,
        encoding::{EncodingConfig, EncodingConfiguration},
        http::{BatchedHttpSink, HttpSink, RequestConfig},
        BatchConfig, BatchSettings, Buffer, Compression, Concurrency, TowerRequestConfig, UriSerde,
    },
    tls::{TlsOptions, TlsSettings},
};
use flate2::write::GzEncoder;
use futures::{future, FutureExt, SinkExt};
use http::{
    header::{self, HeaderName, HeaderValue},
    Method, Request, StatusCode, Uri,
};
use hyper::Body;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::io::Write;

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("{}: {}", source, name))]
    InvalidHeaderName {
        name: String,
        source: header::InvalidHeaderName,
    },
    #[snafu(display("{}: {}", source, value))]
    InvalidHeaderValue {
        value: String,
        source: header::InvalidHeaderValue,
    },
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct HttpSinkConfig {
    pub uri: UriSerde,
    pub method: Option<HttpMethod>,
    pub auth: Option<Auth>,
    // Deprecated, moved to request.
    pub headers: Option<IndexMap<String, String>>,
    #[serde(default)]
    pub compression: Compression,
    pub encoding: EncodingConfig<Encoding>,
    #[serde(default)]
    pub batch: BatchConfig,
    #[serde(default)]
    pub request: RequestConfig,
    pub tls: Option<TlsOptions>,
}

#[cfg(test)]
fn default_config(e: Encoding) -> HttpSinkConfig {
    HttpSinkConfig {
        uri: Default::default(),
        method: Default::default(),
        auth: Default::default(),
        headers: Default::default(),
        compression: Default::default(),
        batch: Default::default(),
        encoding: e.into(),
        request: Default::default(),
        tls: Default::default(),
    }
}

lazy_static! {
    static ref REQUEST_DEFAULTS: TowerRequestConfig = TowerRequestConfig {
        concurrency: Concurrency::Fixed(10),
        timeout_secs: Some(30),
        rate_limit_num: Some(u64::max_value()),
        ..Default::default()
    };
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone, Derivative)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
pub enum HttpMethod {
    #[derivative(Default)]
    Post,
    Put,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Text,
    Ndjson,
    Json,
}

inventory::submit! {
    SinkDescription::new::<HttpSinkConfig>("http")
}

impl GenerateConfig for HttpSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"uri = "https://10.22.212.22:9000/endpoint"
            encoding.codec = "json""#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "http")]
impl SinkConfig for HttpSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let tls = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(tls)?;

        let healthcheck = match cx.healthcheck.uri.clone() {
            Some(healthcheck_uri) => {
                healthcheck(healthcheck_uri, self.auth.clone(), client.clone()).boxed()
            }
            None => future::ok(()).boxed(),
        };

        let mut config = HttpSinkConfig {
            auth: self.auth.choose_one(&self.uri.auth)?,
            uri: self.uri.with_default_parts(),
            ..self.clone()
        };

        config.request.add_old_option(config.headers.take());
        validate_headers(&config.request.headers, &config.auth)?;

        let batch = BatchSettings::default()
            .bytes(bytesize::mib(10u64))
            .timeout(1)
            .parse_config(config.batch)?;
        let request = config.request.tower.unwrap_with(&REQUEST_DEFAULTS);

        let sink = BatchedHttpSink::new(
            config,
            Buffer::new(batch.size, Compression::None),
            request,
            batch.timeout,
            client,
            cx.acker(),
        )
        .sink_map_err(|error| error!(message = "Fatal HTTP sink error.", %error));

        let sink = super::VectorSink::Sink(Box::new(sink));

        Ok((sink, healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn sink_type(&self) -> &'static str {
        "http"
    }
}

#[async_trait::async_trait]
impl HttpSink for HttpSinkConfig {
    type Input = Vec<u8>;
    type Output = Vec<u8>;

    fn encode_event(&self, mut event: Event) -> Option<Self::Input> {
        self.encoding.apply_rules(&mut event);
        let event = event.into_log();

        let body = match &self.encoding.codec() {
            Encoding::Text => {
                if let Some(v) = event.get(crate::config::log_schema().message_key()) {
                    let mut b = v.to_string_lossy().into_bytes();
                    b.push(b'\n');
                    b
                } else {
                    emit!(HTTPEventMissingMessage);
                    return None;
                }
            }

            Encoding::Ndjson => {
                let mut b = serde_json::to_vec(&event)
                    .map_err(|error| panic!("Unable to encode into JSON: {}", error))
                    .ok()?;
                b.push(b'\n');
                b
            }

            Encoding::Json => {
                let mut b = serde_json::to_vec(&event)
                    .map_err(|error| panic!("Unable to encode into JSON: {}", error))
                    .ok()?;
                b.push(b',');
                b
            }
        };

        emit!(HTTPEventEncoded {
            byte_size: body.len(),
        });

        Some(body)
    }

    async fn build_request(&self, mut body: Self::Output) -> crate::Result<http::Request<Vec<u8>>> {
        let method = match &self.method.clone().unwrap_or(HttpMethod::Post) {
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
        };
        let uri: Uri = self.uri.uri.clone();

        let ct = match self.encoding.codec() {
            Encoding::Text => "text/plain",
            Encoding::Ndjson => "application/x-ndjson",
            Encoding::Json => {
                body.insert(0, b'[');
                body.pop(); // remove trailing comma from last record
                body.push(b']');
                "application/json"
            }
        };

        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("Content-Type", ct);

        match self.compression {
            Compression::Gzip(level) => {
                builder = builder.header("Content-Encoding", "gzip");

                let level = level.unwrap_or(GZIP_DEFAULT) as u32;
                let mut w = GzEncoder::new(Vec::new(), flate2::Compression::new(level));
                w.write_all(&body).expect("Writing to Vec can't fail");
                body = w.finish().expect("Writing to Vec can't fail");
            }
            Compression::None => {}
        }

        for (header, value) in self.request.headers.iter() {
            builder = builder.header(header.as_str(), value.as_str());
        }

        let mut request = builder.body(body).unwrap();

        if let Some(auth) = &self.auth {
            auth.apply(&mut request);
        }

        Ok(request)
    }
}

async fn healthcheck(uri: UriSerde, auth: Option<Auth>, client: HttpClient) -> crate::Result<()> {
    let auth = auth.choose_one(&uri.auth)?;
    let uri = uri.with_default_parts();
    let mut request = Request::head(&uri.uri).body(Body::empty()).unwrap();

    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        status => Err(super::HealthcheckError::UnexpectedStatus { status }.into()),
    }
}

fn validate_headers(map: &IndexMap<String, String>, auth: &Option<Auth>) -> crate::Result<()> {
    for (name, value) in map {
        if auth.is_some() && name.eq_ignore_ascii_case("Authorization") {
            return Err("Authorization header can not be used with defined auth options".into());
        }

        HeaderName::from_bytes(name.as_bytes()).with_context(|| InvalidHeaderName { name })?;
        HeaderValue::from_bytes(value.as_bytes()).with_context(|| InvalidHeaderValue { value })?;
    }

    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{
//         assert_downcast_matches,
//         config::SinkContext,
//         sinks::{
//             http::HttpSinkConfig,
//             util::{http::HttpSink, test::build_test_server},
//         },
//         test_util::{next_addr, random_lines_with_stream},
//     };
//     use bytes::buf::BufExt;
//     use flate2::read::GzDecoder;
//     use futures::{stream, StreamExt};
//     use headers::{Authorization, HeaderMapExt};
//     use hyper::Method;
//     use serde::Deserialize;
//     use std::io::{BufRead, BufReader};
//
//     #[test]
//     fn generate_config() {
//         crate::test_util::test_generate_config::<HttpSinkConfig>();
//     }
//
//     #[test]
//     fn http_encode_event_text() {
//         let encoding = EncodingConfig::from(Encoding::Text);
//         let event = Event::from("hello world");
//
//         let mut config = default_config(Encoding::Text);
//         config.encoding = encoding;
//         let bytes = config.encode_event(event).unwrap();
//
//         assert_eq!(bytes, Vec::from(&"hello world\n"[..]));
//     }
//
//     #[test]
//     fn http_encode_event_json() {
//         let encoding = EncodingConfig::from(Encoding::Ndjson);
//         let event = Event::from("hello world");
//
//         let mut config = default_config(Encoding::Json);
//         config.encoding = encoding;
//         let bytes = config.encode_event(event).unwrap();
//
//         #[derive(Deserialize, Debug)]
//         #[serde(deny_unknown_fields)]
//         struct ExpectedEvent {
//             message: String,
//             timestamp: chrono::DateTime<chrono::Utc>,
//         }
//
//         let output = serde_json::from_slice::<ExpectedEvent>(&bytes[..]).unwrap();
//
//         assert_eq!(output.message, "hello world".to_string());
//     }
//
//     #[test]
//     fn http_validates_normal_headers() {
//         let config = r#"
//         uri = "http://$IN_ADDR/frames"
//         encoding = "text"
//         [request.headers]
//         Auth = "token:thing_and-stuff"
//         X-Custom-Nonsense = "_%_{}_-_&_._`_|_~_!_#_&_$_"
//         "#;
//         let config: HttpSinkConfig = toml::from_str(&config).unwrap();
//
//         assert!(super::validate_headers(&config.request.headers, &None).is_ok());
//     }
//
//     #[test]
//     fn http_catches_bad_header_names() {
//         let config = r#"
//         uri = "http://$IN_ADDR/frames"
//         encoding = "text"
//         [request.headers]
//         "\u0001" = "bad"
//         "#;
//         let config: HttpSinkConfig = toml::from_str(&config).unwrap();
//
//         assert_downcast_matches!(
//             super::validate_headers(&config.request.headers, &None).unwrap_err(),
//             BuildError,
//             BuildError::InvalidHeaderName{..}
//         );
//     }
//
//     // TODO: Fix failure on GH Actions using macos-latest image.
//     #[cfg(not(target_os = "macos"))]
//     #[tokio::test]
//     #[should_panic(expected = "Authorization header can not be used with defined auth options")]
//     async fn http_headers_auth_conflict() {
//         let config = r#"
//         uri = "http://$IN_ADDR/"
//         encoding = "text"
//         [request.headers]
//         Authorization = "Basic base64encodedstring"
//         [auth]
//         strategy = "basic"
//         user = "user"
//         password = "password"
//         "#;
//         let config: HttpSinkConfig = toml::from_str(&config).unwrap();
//
//         let cx = SinkContext::new_test();
//
//         let _ = config.build(cx).await.unwrap();
//     }
//
//     #[tokio::test]
//     async fn http_happy_path_post() {
//         let num_lines = 1000;
//
//         let in_addr = next_addr();
//
//         let config = r#"
//         uri = "http://$IN_ADDR/frames"
//         compression = "gzip"
//         encoding = "ndjson"
//
//         [auth]
//         strategy = "basic"
//         user = "waldo"
//         password = "hunter2"
//     "#
//         .replace("$IN_ADDR", &format!("{}", in_addr));
//         let config: HttpSinkConfig = toml::from_str(&config).unwrap();
//
//         let cx = SinkContext::new_test();
//
//         let (sink, _) = config.build(cx).await.unwrap();
//         let (rx, trigger, server) = build_test_server(in_addr);
//
//         let (input_lines, events) = random_lines_with_stream(100, num_lines);
//         let pump = sink.run(events);
//
//         tokio::spawn(server);
//
//         pump.await.unwrap();
//         drop(trigger);
//
//         let output_lines = rx
//             .flat_map(|(parts, body)| {
//                 assert_eq!(Method::POST, parts.method);
//                 assert_eq!("/frames", parts.uri.path());
//                 assert_eq!(
//                     Some(Authorization::basic("waldo", "hunter2")),
//                     parts.headers.typed_get()
//                 );
//                 stream::iter(BufReader::new(GzDecoder::new(body.reader())).lines())
//             })
//             .map(Result::unwrap)
//             .map(|line| {
//                 let val: serde_json::Value = serde_json::from_str(&line).unwrap();
//                 val.get("message").unwrap().as_str().unwrap().to_owned()
//             })
//             .collect::<Vec<_>>()
//             .await;
//
//         assert_eq!(num_lines, output_lines.len());
//         assert_eq!(input_lines, output_lines);
//     }
//
//     #[tokio::test]
//     async fn http_happy_path_put() {
//         let num_lines = 1000;
//
//         let in_addr = next_addr();
//
//         let config = r#"
//         uri = "http://$IN_ADDR/frames"
//         method = "put"
//         compression = "gzip"
//         encoding = "ndjson"
//
//         [auth]
//         strategy = "basic"
//         user = "waldo"
//         password = "hunter2"
//     "#
//         .replace("$IN_ADDR", &format!("{}", in_addr));
//         let config: HttpSinkConfig = toml::from_str(&config).unwrap();
//
//         let cx = SinkContext::new_test();
//
//         let (sink, _) = config.build(cx).await.unwrap();
//         let (rx, trigger, server) = build_test_server(in_addr);
//
//         let (input_lines, events) = random_lines_with_stream(100, num_lines);
//         let pump = sink.run(events);
//
//         tokio::spawn(server);
//
//         pump.await.unwrap();
//         drop(trigger);
//
//         let output_lines = rx
//             .flat_map(|(parts, body)| {
//                 assert_eq!(Method::PUT, parts.method);
//                 assert_eq!("/frames", parts.uri.path());
//                 assert_eq!(
//                     Some(Authorization::basic("waldo", "hunter2")),
//                     parts.headers.typed_get()
//                 );
//                 stream::iter(BufReader::new(GzDecoder::new(body.reader())).lines())
//             })
//             .map(Result::unwrap)
//             .map(|line| {
//                 let val: serde_json::Value = serde_json::from_str(&line).unwrap();
//                 val.get("message").unwrap().as_str().unwrap().to_owned()
//             })
//             .collect::<Vec<_>>()
//             .await;
//
//         assert_eq!(num_lines, output_lines.len());
//         assert_eq!(input_lines, output_lines);
//     }
//
//     #[tokio::test]
//     async fn http_passes_custom_headers() {
//         let num_lines = 1000;
//
//         let in_addr = next_addr();
//
//         let config = r#"
//         uri = "http://$IN_ADDR/frames"
//         encoding = "ndjson"
//         compression = "gzip"
//         [request.headers]
//         foo = "bar"
//         baz = "quux"
//     "#
//         .replace("$IN_ADDR", &format!("{}", in_addr));
//         let config: HttpSinkConfig = toml::from_str(&config).unwrap();
//
//         let cx = SinkContext::new_test();
//
//         let (sink, _) = config.build(cx).await.unwrap();
//         let (rx, trigger, server) = build_test_server(in_addr);
//
//         let (input_lines, events) = random_lines_with_stream(100, num_lines);
//         let pump = sink.run(events);
//
//         tokio::spawn(server);
//
//         pump.await.unwrap();
//         drop(trigger);
//
//         let output_lines = rx
//             .flat_map(|(parts, body)| {
//                 assert_eq!(Method::POST, parts.method);
//                 assert_eq!("/frames", parts.uri.path());
//                 assert_eq!(
//                     Some("bar"),
//                     parts.headers.get("foo").map(|v| v.to_str().unwrap())
//                 );
//                 assert_eq!(
//                     Some("quux"),
//                     parts.headers.get("baz").map(|v| v.to_str().unwrap())
//                 );
//                 stream::iter(BufReader::new(GzDecoder::new(body.reader())).lines())
//             })
//             .map(Result::unwrap)
//             .map(|line| {
//                 let val: serde_json::Value = serde_json::from_str(&line).unwrap();
//                 val.get("message").unwrap().as_str().unwrap().to_owned()
//             })
//             .collect::<Vec<_>>()
//             .await;
//
//         assert_eq!(num_lines, output_lines.len());
//         assert_eq!(input_lines, output_lines);
//     }
//
//     #[tokio::test]
//     async fn json_compresion() {
//         let num_lines = 1000;
//
//         let in_addr = next_addr();
//
//         let config = r#"
//         uri = "http://$IN_ADDR/frames"
//         compression = "gzip"
//         encoding = "json"
//
//         [auth]
//         strategy = "basic"
//         user = "waldo"
//         password = "hunter2"
//     "#
//         .replace("$IN_ADDR", &format!("{}", in_addr));
//         let config: HttpSinkConfig = toml::from_str(&config).unwrap();
//
//         let cx = SinkContext::new_test();
//
//         let (sink, _) = config.build(cx).await.unwrap();
//         let (rx, trigger, server) = build_test_server(in_addr);
//
//         let (input_lines, events) = random_lines_with_stream(100, num_lines);
//         let pump = sink.run(events);
//
//         tokio::spawn(server);
//
//         pump.await.unwrap();
//         drop(trigger);
//
//         let output_lines = rx
//             .flat_map(|(parts, body)| {
//                 assert_eq!(Method::POST, parts.method);
//                 assert_eq!("/frames", parts.uri.path());
//                 assert_eq!(
//                     Some(Authorization::basic("waldo", "hunter2")),
//                     parts.headers.typed_get()
//                 );
//
//                 let lines: Vec<serde_json::Value> =
//                     serde_json::from_reader(GzDecoder::new(body.reader())).unwrap();
//                 stream::iter(lines)
//             })
//             .map(|line| line.get("message").unwrap().as_str().unwrap().to_owned())
//             .collect::<Vec<_>>()
//             .await;
//
//         assert_eq!(num_lines, output_lines.len());
//         assert_eq!(input_lines, output_lines);
//     }
// }
