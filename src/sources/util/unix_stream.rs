use crate::{
    async_read::VecAsyncReadExt,
    emit,
    event::Event,
    internal_events::{ConnectionOpen, OpenGauge, UnixSocketError},
    shutdown::ShutdownSignal,
    sources::Source,
    Pipeline,
};
use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};
use std::{future::ready, path::PathBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Decoder, FramedRead};
use tracing::field;
use tracing_futures::Instrument;

/// Returns a Source object corresponding to a Unix domain stream
/// socket.  Passing in different functions for build_event can allow
/// for different source-specific logic (such as decoding syslog
/// messages in the syslog source).
pub fn build_unix_stream_source<D>(
    listen_path: PathBuf,
    decoder: D,
    host_key: String,
    shutdown: ShutdownSignal,
    out: Pipeline,
    build_event: impl Fn(&str, Option<Bytes>, &str) -> Option<Event> + Clone + Send + Sync + 'static,
) -> Source
where
    D: Decoder<Item = String> + Clone + Send + 'static,
    D::Error: From<std::io::Error> + std::fmt::Debug + std::fmt::Display,
{
    let out = out.sink_map_err(|error| error!(message = "Error sending line.", %error));

    Box::pin(async move {
        let mut listener =
            UnixListener::bind(&listen_path).expect("Failed to bind to listener socket");
        info!(message = "Listening.", path = ?listen_path, r#type = "unix");

        let connection_open = OpenGauge::new();
        let mut stream = listener.incoming().take_until(shutdown.clone());
        while let Some(socket) = stream.next().await {
            let socket = match socket {
                Err(error) => {
                    error!(message = "Failed to accept socket.", %error);
                    continue;
                }
                Ok(socket) => socket,
            };

            let listen_path = listen_path.clone();
            let host_key = host_key.clone();

            let span = info_span!("connection");
            let path = if let Ok(addr) = socket.peer_addr() {
                if let Some(path) = addr.as_pathname().map(|e| e.to_owned()) {
                    span.record("peer_path", &field::debug(&path));
                    Some(path)
                } else {
                    None
                }
            } else {
                None
            };

            let build_event = build_event.clone();
            let received_from: Option<Bytes> =
                path.map(|p| p.to_string_lossy().into_owned().into());

            let stream = socket.allow_read_until(shutdown.clone().map(|_| ()));
            let mut stream = FramedRead::new(stream, decoder.clone()).filter_map(move |line| {
                ready(match line {
                    Ok(line) => build_event(&host_key, received_from.clone(), &line).map(Ok),
                    Err(error) => {
                        emit!(UnixSocketError {
                            error,
                            path: &listen_path
                        });
                        None
                    }
                })
            });

            let connection_open = connection_open.clone();
            let mut out = out.clone();
            tokio::spawn(
                async move {
                    let _open_token = connection_open.open(|count| emit!(ConnectionOpen { count }));
                    let _ = out.send_all(&mut stream).await;
                    info!("Finished sending.");

                    let socket: &UnixStream = stream.get_ref().get_ref().get_ref();
                    let _ = socket.shutdown(std::net::Shutdown::Both);
                }
                .instrument(span),
            );
        }

        Ok(())
    })
}
