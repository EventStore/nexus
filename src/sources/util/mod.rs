#[cfg(feature = "sources-utils-fake")]
pub mod fake;
#[cfg(feature = "sources-utils-http")]
mod http;
pub mod multiline_config;
#[cfg(all(feature = "sources-utils-tls", feature = "listenfd"))]
mod tcp;
#[cfg(all(unix, feature = "sources-socket"))]
mod unix_datagram;
#[cfg(all(unix, feature = "sources-utils-unix"))]
mod unix_stream;

#[cfg(any(feature = "sources-http", feature = "sources-heroku_logs"))]
pub(crate) use self::http::add_query_parameters;
#[cfg(feature = "sources-prometheus")]
pub(crate) use self::http::decode;
#[cfg(feature = "sources-utils-http")]
pub(crate) use self::http::{ErrorMessage, HttpSource, HttpSourceAuthConfig};
pub use multiline_config::MultilineConfig;
#[cfg(all(feature = "sources-utils-tls", feature = "listenfd"))]
pub use tcp::{SocketListenAddr, TcpSource};
#[cfg(all(unix, feature = "sources-socket",))]
pub use unix_datagram::build_unix_datagram_source;
#[cfg(all(unix, feature = "sources-utils-unix",))]
pub use unix_stream::build_unix_stream_source;
