use super::util::MultilineConfig;
use crate::{
    config::{log_schema, DataType, GlobalOptions, SourceConfig, SourceDescription},
    event::Event,
    internal_events::{FileEventReceived, FileOpen, FileSourceInternalEventsEmitter},
    line_agg::{self, LineAgg},
    shutdown::ShutdownSignal,
    trace::{current_span, Instrument},
    Pipeline,
};
use bytes::Bytes;
use chrono::Utc;
use file_source::{
    paths_provider::glob::{Glob, MatchOptions},
    FileServer, FingerprintStrategy, Fingerprinter,
};
use futures::{
    future::TryFutureExt,
    stream::{Stream, StreamExt},
    SinkExt,
};
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::convert::TryInto;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::spawn_blocking;

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("data_dir option required, but not given here or globally"))]
    NoDataDir,
    #[snafu(display(
        "could not create subdirectory {:?} inside of data_dir {:?}",
        subdir,
        data_dir
    ))]
    MakeSubdirectoryError {
        subdir: PathBuf,
        data_dir: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("data_dir {:?} does not exist", data_dir))]
    MissingDataDir { data_dir: PathBuf },
    #[snafu(display("data_dir {:?} is not writable", data_dir))]
    DataDirNotWritable { data_dir: PathBuf },
    #[snafu(display(
        "message_start_indicator {:?} is not a valid regex: {}",
        indicator,
        source
    ))]
    InvalidMessageStartIndicator {
        indicator: String,
        source: regex::Error,
    },
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct FileConfig {
    pub include: Vec<PathBuf>,
    pub exclude: Vec<PathBuf>,
    pub file_key: Option<String>,
    pub start_at_beginning: bool,
    pub ignore_older: Option<u64>, // secs
    #[serde(default = "default_max_line_bytes")]
    pub max_line_bytes: usize,
    pub host_key: Option<String>,
    pub data_dir: Option<PathBuf>,
    pub glob_minimum_cooldown: u64, // millis
    // Deprecated name
    #[serde(alias = "fingerprinting")]
    pub fingerprint: FingerprintConfig,
    pub message_start_indicator: Option<String>,
    pub multi_line_timeout: u64, // millis
    pub multiline: Option<MultilineConfig>,
    pub max_read_bytes: usize,
    pub oldest_first: bool,
    pub remove_after: Option<u64>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum FingerprintConfig {
    Checksum {
        // Deprecated name
        #[serde(alias = "fingerprint_bytes")]
        bytes: usize,
        ignored_header_bytes: usize,
    },
    #[serde(rename = "device_and_inode")]
    DevInode,
}

impl From<FingerprintConfig> for FingerprintStrategy {
    fn from(config: FingerprintConfig) -> FingerprintStrategy {
        match config {
            FingerprintConfig::Checksum {
                bytes,
                ignored_header_bytes,
            } => FingerprintStrategy::Checksum {
                bytes,
                ignored_header_bytes,
            },
            FingerprintConfig::DevInode => FingerprintStrategy::DevInode,
        }
    }
}

fn default_max_line_bytes() -> usize {
    bytesize::kib(100u64) as usize
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            include: vec![],
            exclude: vec![],
            file_key: Some("file".to_string()),
            start_at_beginning: false,
            ignore_older: None,
            max_line_bytes: default_max_line_bytes(),
            fingerprint: FingerprintConfig::Checksum {
                bytes: 256,
                ignored_header_bytes: 0,
            },
            host_key: None,
            data_dir: None,
            glob_minimum_cooldown: 1000, // millis
            message_start_indicator: None,
            multi_line_timeout: 1000, // millis
            multiline: None,
            max_read_bytes: 2048,
            oldest_first: false,
            remove_after: None,
        }
    }
}

inventory::submit! {
    SourceDescription::new::<FileConfig>("file")
}

impl_generate_config_from_default!(FileConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "file")]
impl SourceConfig for FileConfig {
    async fn build(
        &self,
        name: &str,
        globals: &GlobalOptions,
        shutdown: ShutdownSignal,
        out: Pipeline,
    ) -> crate::Result<super::Source> {
        // add the source name as a subdir, so that multiple sources can
        // operate within the same given data_dir (e.g. the global one)
        // without the file servers' checkpointers interfering with each
        // other
        let data_dir = globals.resolve_and_make_data_subdir(self.data_dir.as_ref(), name)?;

        // Clippy rule, because async_trait?
        #[allow(clippy::suspicious_else_formatting)]
        {
            if let Some(ref config) = self.multiline {
                let _: line_agg::Config = config.try_into()?;
            }

            if let Some(ref indicator) = self.message_start_indicator {
                Regex::new(indicator)
                    .with_context(|| InvalidMessageStartIndicator { indicator })?;
            }
        }

        Ok(file_source(self, data_dir, shutdown, out))
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn source_type(&self) -> &'static str {
        "file"
    }
}

pub fn file_source(
    config: &FileConfig,
    data_dir: PathBuf,
    shutdown: ShutdownSignal,
    mut out: Pipeline,
) -> super::Source {
    let ignore_before = config
        .ignore_older
        .map(|secs| Utc::now() - chrono::Duration::seconds(secs as i64));
    let glob_minimum_cooldown = Duration::from_millis(config.glob_minimum_cooldown);

    let paths_provider = Glob::new(&config.include, &config.exclude, MatchOptions::default())
        .expect("invalid glob patterns");

    let file_server = FileServer {
        paths_provider,
        max_read_bytes: config.max_read_bytes,
        start_at_beginning: config.start_at_beginning,
        ignore_before,
        max_line_bytes: config.max_line_bytes,
        data_dir,
        glob_minimum_cooldown,
        fingerprinter: Fingerprinter {
            strategy: config.fingerprint.clone().into(),
            max_line_length: config.max_line_bytes,
            ignore_not_found: false,
        },
        oldest_first: config.oldest_first,
        remove_after: config.remove_after.map(Duration::from_secs),
        emitter: FileSourceInternalEventsEmitter,
        handle: tokio::runtime::Handle::current(),
    };

    let file_key = config.file_key.clone();
    let host_key = config
        .host_key
        .clone()
        .unwrap_or_else(|| log_schema().host_key().to_string());
    let hostname = crate::get_hostname().ok();

    let include = config.include.clone();
    let exclude = config.exclude.clone();
    let multiline_config = config.multiline.clone();
    let message_start_indicator = config.message_start_indicator.clone();
    let multi_line_timeout = config.multi_line_timeout;

    Box::pin(async move {
        info!(message = "Starting file server.", include = ?include, exclude = ?exclude);

        // sizing here is just a guess
        let (tx, rx) = futures::channel::mpsc::channel::<Vec<(Bytes, String)>>(2);
        let rx = rx.map(futures::stream::iter).flatten();

        let messages: Box<dyn Stream<Item = (Bytes, String)> + Send + std::marker::Unpin> =
            if let Some(ref multiline_config) = multiline_config {
                wrap_with_line_agg(
                    rx,
                    multiline_config.try_into().unwrap(), // validated in build
                )
            } else if let Some(msi) = message_start_indicator {
                wrap_with_line_agg(
                    rx,
                    line_agg::Config::for_legacy(
                        Regex::new(&msi).unwrap(), // validated in build
                        multi_line_timeout,
                    ),
                )
            } else {
                Box::new(rx)
            };

        // Once file server ends this will run until it has finished processing remaining
        // logs in the queue.
        let span = current_span();
        let span2 = span.clone();
        let mut messages = messages
            .map(move |(msg, file): (Bytes, String)| {
                let _enter = span2.enter();
                create_event(msg, file, &host_key, &hostname, &file_key)
            })
            .map(Ok);
        tokio::spawn(async move { out.send_all(&mut messages).instrument(span).await });

        let span = info_span!("file_server");
        spawn_blocking(move || {
            let _enter = span.enter();
            let result = file_server.run(tx, shutdown);
            emit!(FileOpen { count: 0 });
            // Panic if we encounter any error originating from the file server.
            // We're at the `spawn_blocking` call, the panic will be caught and
            // passed to the `JoinHandle` error, similar to the usual threads.
            result.unwrap();
        })
        .map_err(|error| error!(message="File server unexpectedly stopped.", %error))
        .await
    })
}

fn wrap_with_line_agg(
    rx: impl Stream<Item = (Bytes, String)> + Send + std::marker::Unpin + 'static,
    config: line_agg::Config,
) -> Box<dyn Stream<Item = (Bytes, String)> + Send + std::marker::Unpin + 'static> {
    let logic = line_agg::Logic::new(config);
    Box::new(
        LineAgg::new(rx.map(|(line, src)| (src, line, ())), logic)
            .map(|(src, line, _context)| (line, src)),
    )
}

fn create_event(
    line: Bytes,
    file: String,
    host_key: &str,
    hostname: &Option<String>,
    file_key: &Option<String>,
) -> Event {
    emit!(FileEventReceived {
        file: &file,
        byte_size: line.len(),
    });

    let mut event = Event::from(line);

    // Add source type
    event
        .as_mut_log()
        .insert(log_schema().source_type_key(), Bytes::from("file"));

    if let Some(file_key) = &file_key {
        event.as_mut_log().insert(file_key.clone(), file);
    }

    if let Some(hostname) = &hostname {
        event.as_mut_log().insert(host_key, hostname.clone());
    }

    event
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, shutdown::ShutdownSignal, sources::file};
    use pretty_assertions::assert_eq;
    use std::{
        collections::HashSet,
        fs::{self, File},
        future::Future,
        io::{Seek, Write},
    };

    use tempfile::tempdir;
    use tokio::time::{delay_for, timeout, Duration};

    // #[test]
    // fn generate_config() {
    //     crate::test_util::test_generate_config::<FileConfig>();
    // }

    fn test_default_file_config(dir: &tempfile::TempDir) -> file::FileConfig {
        file::FileConfig {
            fingerprint: FingerprintConfig::Checksum {
                bytes: 8,
                ignored_header_bytes: 0,
            },
            data_dir: Some(dir.path().to_path_buf()),
            glob_minimum_cooldown: 0, // millis
            ..Default::default()
        }
    }

    async fn wait_with_timeout<F, R>(future: F) -> R
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        timeout(Duration::from_secs(5), future)
            .await
            .unwrap_or_else(|_| {
                panic!("Unclosed channel: may indicate file-server could not shutdown gracefully.")
            })
    }

    async fn sleep_500_millis() {
        delay_for(Duration::from_millis(500)).await;
    }

    #[test]
    fn parse_config() {
        let config: FileConfig = toml::from_str(
            r#"
        "#,
        )
        .unwrap();
        assert_eq!(config, FileConfig::default());
        assert_eq!(
            config.fingerprint,
            FingerprintConfig::Checksum {
                bytes: 256,
                ignored_header_bytes: 0,
            }
        );

        let config: FileConfig = toml::from_str(
            r#"
        [fingerprint]
        strategy = "device_and_inode"
        "#,
        )
        .unwrap();
        assert_eq!(config.fingerprint, FingerprintConfig::DevInode);

        let config: FileConfig = toml::from_str(
            r#"
        [fingerprint]
        strategy = "checksum"
        bytes = 128
        ignored_header_bytes = 512
        "#,
        )
        .unwrap();
        assert_eq!(
            config.fingerprint,
            FingerprintConfig::Checksum {
                bytes: 128,
                ignored_header_bytes: 512,
            }
        );
    }

    #[test]
    fn resolve_data_dir() {
        let global_dir = tempdir().unwrap();
        let local_dir = tempdir().unwrap();

        let mut config = Config::default();
        config.global.data_dir = global_dir.into_path().into();

        // local path given -- local should win
        let res = config
            .global
            .resolve_and_validate_data_dir(test_default_file_config(&local_dir).data_dir.as_ref())
            .unwrap();
        assert_eq!(res, local_dir.path());

        // no local path given -- global fallback should be in effect
        let res = config.global.resolve_and_validate_data_dir(None).unwrap();
        assert_eq!(res, config.global.data_dir.unwrap());
    }

    #[test]
    fn file_create_event() {
        let line = Bytes::from("hello world");
        let file = "some_file.rs".to_string();
        let host_key = "host".to_string();
        let hostname = Some("Some.Machine".to_string());
        let file_key = Some("file".to_string());

        let event = create_event(line, file, &host_key, &hostname, &file_key);
        let log = event.into_log();

        assert_eq!(log["file"], "some_file.rs".into());
        assert_eq!(log["host"], "Some.Machine".into());
        assert_eq!(log[log_schema().message_key()], "hello world".into());
        assert_eq!(log[log_schema().source_type_key()], "file".into());
    }

    #[tokio::test]
    async fn file_happy_path() {
        let n = 5;
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path1 = dir.path().join("file1");
        let path2 = dir.path().join("file2");
        let mut file1 = File::create(&path1).unwrap();
        let mut file2 = File::create(&path2).unwrap();

        sleep_500_millis().await; // The files must be observed at their original lengths before writing to them

        for i in 0..n {
            writeln!(&mut file1, "hello {}", i).unwrap();
            writeln!(&mut file2, "goodbye {}", i).unwrap();
        }

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;

        let mut hello_i = 0;
        let mut goodbye_i = 0;

        for event in received {
            let line = event.as_log()[log_schema().message_key()].to_string_lossy();
            if line.starts_with("hello") {
                assert_eq!(line, format!("hello {}", hello_i));
                assert_eq!(
                    event.as_log()["file"].to_string_lossy(),
                    path1.to_str().unwrap()
                );
                hello_i += 1;
            } else {
                assert_eq!(line, format!("goodbye {}", goodbye_i));
                assert_eq!(
                    event.as_log()["file"].to_string_lossy(),
                    path2.to_str().unwrap()
                );
                goodbye_i += 1;
            }
        }
        assert_eq!(hello_i, n);
        assert_eq!(goodbye_i, n);
    }

    #[tokio::test]
    async fn file_truncate() {
        let n = 5;
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            ..test_default_file_config(&dir)
        };
        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();

        sleep_500_millis().await; // The files must be observed at its original length before writing to it

        for i in 0..n {
            writeln!(&mut file, "pretrunc {}", i).unwrap();
        }

        sleep_500_millis().await; // The writes must be observed before truncating

        file.set_len(0).unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();

        sleep_500_millis().await; // The truncate must be observed before writing again

        for i in 0..n {
            writeln!(&mut file, "posttrunc {}", i).unwrap();
        }

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;

        let mut i = 0;
        let mut pre_trunc = true;

        for event in received {
            assert_eq!(
                event.as_log()["file"].to_string_lossy(),
                path.to_str().unwrap()
            );

            let line = event.as_log()[log_schema().message_key()].to_string_lossy();

            if pre_trunc {
                assert_eq!(line, format!("pretrunc {}", i));
            } else {
                assert_eq!(line, format!("posttrunc {}", i));
            }

            i += 1;
            if i == n {
                i = 0;
                pre_trunc = false;
            }
        }
    }

    #[tokio::test]
    async fn file_rotate() {
        let n = 5;
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            ..test_default_file_config(&dir)
        };
        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path = dir.path().join("file");
        let archive_path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();

        sleep_500_millis().await; // The files must be observed at its original length before writing to it

        for i in 0..n {
            writeln!(&mut file, "prerot {}", i).unwrap();
        }

        sleep_500_millis().await; // The writes must be observed before rotating

        fs::rename(&path, archive_path).expect("could not rename");
        let mut file = File::create(&path).unwrap();

        sleep_500_millis().await; // The rotation must be observed before writing again

        for i in 0..n {
            writeln!(&mut file, "postrot {}", i).unwrap();
        }

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;

        let mut i = 0;
        let mut pre_rot = true;

        for event in received {
            assert_eq!(
                event.as_log()["file"].to_string_lossy(),
                path.to_str().unwrap()
            );

            let line = event.as_log()[log_schema().message_key()].to_string_lossy();

            if pre_rot {
                assert_eq!(line, format!("prerot {}", i));
            } else {
                assert_eq!(line, format!("postrot {}", i));
            }

            i += 1;
            if i == n {
                i = 0;
                pre_rot = false;
            }
        }
    }

    #[tokio::test]
    async fn file_multiple_paths() {
        let n = 5;
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*.txt"), dir.path().join("a.*")],
            exclude: vec![dir.path().join("a.*.txt")],
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path1 = dir.path().join("a.txt");
        let path2 = dir.path().join("b.txt");
        let path3 = dir.path().join("a.log");
        let path4 = dir.path().join("a.ignore.txt");
        let mut file1 = File::create(&path1).unwrap();
        let mut file2 = File::create(&path2).unwrap();
        let mut file3 = File::create(&path3).unwrap();
        let mut file4 = File::create(&path4).unwrap();

        sleep_500_millis().await; // The files must be observed at their original lengths before writing to them

        for i in 0..n {
            writeln!(&mut file1, "1 {}", i).unwrap();
            writeln!(&mut file2, "2 {}", i).unwrap();
            writeln!(&mut file3, "3 {}", i).unwrap();
            writeln!(&mut file4, "4 {}", i).unwrap();
        }

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;

        let mut is = [0; 3];

        for event in received {
            let line = event.as_log()[log_schema().message_key()].to_string_lossy();
            let mut split = line.split(' ');
            let file = split.next().unwrap().parse::<usize>().unwrap();
            assert_ne!(file, 4);
            let i = split.next().unwrap().parse::<usize>().unwrap();

            assert_eq!(is[file - 1], i);
            is[file - 1] += 1;
        }

        assert_eq!(is, [n as usize; 3]);
    }

    #[tokio::test]
    async fn file_file_key() {
        // Default
        {
            let (trigger_shutdown, shutdown, shutdown_done) = ShutdownSignal::new_wired();

            let (tx, rx) = Pipeline::new_test();
            let dir = tempdir().unwrap();
            let config = file::FileConfig {
                include: vec![dir.path().join("*")],
                ..test_default_file_config(&dir)
            };

            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            let path = dir.path().join("file");
            let mut file = File::create(&path).unwrap();

            sleep_500_millis().await;

            writeln!(&mut file, "hello there").unwrap();

            sleep_500_millis().await;

            drop(trigger_shutdown);
            shutdown_done.await;

            let received = wait_with_timeout(rx.into_future()).await.0.unwrap();
            assert_eq!(
                received.as_log()["file"].to_string_lossy(),
                path.to_str().unwrap()
            );
        }

        // Custom
        {
            let (trigger_shutdown, shutdown, shutdown_done) = ShutdownSignal::new_wired();

            let (tx, rx) = Pipeline::new_test();
            let dir = tempdir().unwrap();
            let config = file::FileConfig {
                include: vec![dir.path().join("*")],
                file_key: Some("source".to_string()),
                ..test_default_file_config(&dir)
            };

            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            let path = dir.path().join("file");
            let mut file = File::create(&path).unwrap();

            sleep_500_millis().await;

            writeln!(&mut file, "hello there").unwrap();

            sleep_500_millis().await;

            drop(trigger_shutdown);
            shutdown_done.await;

            let received = wait_with_timeout(rx.into_future()).await.0.unwrap();
            assert_eq!(
                received.as_log()["source"].to_string_lossy(),
                path.to_str().unwrap()
            );
        }

        // Hidden
        {
            let (trigger_shutdown, shutdown, shutdown_done) = ShutdownSignal::new_wired();

            let (tx, rx) = Pipeline::new_test();
            let dir = tempdir().unwrap();
            let config = file::FileConfig {
                include: vec![dir.path().join("*")],
                file_key: None,
                ..test_default_file_config(&dir)
            };

            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            let path = dir.path().join("file");
            let mut file = File::create(&path).unwrap();

            sleep_500_millis().await;

            writeln!(&mut file, "hello there").unwrap();

            sleep_500_millis().await;

            drop(trigger_shutdown);
            shutdown_done.await;

            let received = wait_with_timeout(rx.into_future()).await.0.unwrap();
            assert_eq!(
                received.as_log().keys().collect::<HashSet<_>>(),
                vec![
                    log_schema().host_key().to_string(),
                    log_schema().message_key().to_string(),
                    log_schema().timestamp_key().to_string(),
                    log_schema().source_type_key().to_string()
                ]
                .into_iter()
                .collect::<HashSet<_>>()
            );
        }
    }

    #[tokio::test]
    async fn file_start_position_server_restart() {
        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            ..test_default_file_config(&dir)
        };

        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();
        writeln!(&mut file, "zeroth line").unwrap();
        sleep_500_millis().await;

        // First time server runs it picks up existing lines.
        {
            let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

            let (tx, rx) = Pipeline::new_test();
            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            sleep_500_millis().await;
            writeln!(&mut file, "first line").unwrap();
            sleep_500_millis().await;

            drop(trigger_shutdown);

            let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;
            let lines = received
                .into_iter()
                .map(|event| event.as_log()[log_schema().message_key()].to_string_lossy())
                .collect::<Vec<_>>();
            assert_eq!(lines, vec!["zeroth line", "first line"]);
        }
        // Restart server, read file from checkpoint.
        {
            let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

            let (tx, rx) = Pipeline::new_test();
            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            sleep_500_millis().await;
            writeln!(&mut file, "second line").unwrap();
            sleep_500_millis().await;

            drop(trigger_shutdown);

            let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;
            let lines = received
                .into_iter()
                .map(|event| event.as_log()[log_schema().message_key()].to_string_lossy())
                .collect::<Vec<_>>();
            assert_eq!(lines, vec!["second line"]);
        }
        // Restart server, read files from beginning.
        {
            let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

            let config = file::FileConfig {
                include: vec![dir.path().join("*")],
                start_at_beginning: true,
                ..test_default_file_config(&dir)
            };
            let (tx, rx) = Pipeline::new_test();
            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            sleep_500_millis().await;
            writeln!(&mut file, "third line").unwrap();
            sleep_500_millis().await;

            drop(trigger_shutdown);

            let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;
            let lines = received
                .into_iter()
                .map(|event| event.as_log()[log_schema().message_key()].to_string_lossy())
                .collect::<Vec<_>>();
            assert_eq!(
                lines,
                vec!["zeroth line", "first line", "second line", "third line"]
            );
        }
    }

    #[tokio::test]
    async fn file_start_position_server_restart_with_file_rotation() {
        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            ..test_default_file_config(&dir)
        };

        let path = dir.path().join("file");
        let path_for_old_file = dir.path().join("file.old");
        // Run server first time, collect some lines.
        {
            let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

            let (tx, rx) = Pipeline::new_test();
            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            let mut file = File::create(&path).unwrap();
            sleep_500_millis().await;
            writeln!(&mut file, "first line").unwrap();
            sleep_500_millis().await;

            drop(trigger_shutdown);

            let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;
            let lines = received
                .into_iter()
                .map(|event| event.as_log()[log_schema().message_key()].to_string_lossy())
                .collect::<Vec<_>>();
            assert_eq!(lines, vec!["first line"]);
        }
        // Perform 'file rotation' to archive old lines.
        fs::rename(&path, &path_for_old_file).expect("could not rename");
        // Restart the server and make sure it does not re-read the old file
        // even though it has a new name.
        {
            let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

            let (tx, rx) = Pipeline::new_test();
            let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
            tokio::spawn(source);

            let mut file = File::create(&path).unwrap();
            sleep_500_millis().await;
            writeln!(&mut file, "second line").unwrap();
            sleep_500_millis().await;

            drop(trigger_shutdown);

            let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;
            let lines = received
                .into_iter()
                .map(|event| event.as_log()[log_schema().message_key()].to_string_lossy())
                .collect::<Vec<_>>();
            assert_eq!(lines, vec!["second line"]);
        }
    }

    #[cfg(unix)] // this test uses unix-specific function `futimes` during test time
    #[tokio::test]
    async fn file_start_position_ignore_old_files() {
        use std::os::unix::io::AsRawFd;
        use std::time::{Duration, SystemTime};

        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();
        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            start_at_beginning: true,
            ignore_older: Some(5),
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let before_path = dir.path().join("before");
        let mut before_file = File::create(&before_path).unwrap();
        let after_path = dir.path().join("after");
        let mut after_file = File::create(&after_path).unwrap();

        writeln!(&mut before_file, "first line").unwrap(); // first few bytes make up unique file fingerprint
        writeln!(&mut after_file, "_first line").unwrap(); //   and therefore need to be non-identical

        {
            // Set the modified times
            let before = SystemTime::now() - Duration::from_secs(8);
            let after = SystemTime::now() - Duration::from_secs(2);

            let before_time = libc::timeval {
                tv_sec: before
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as _,
                tv_usec: 0,
            };
            let before_times = [before_time, before_time];

            let after_time = libc::timeval {
                tv_sec: after
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as _,
                tv_usec: 0,
            };
            let after_times = [after_time, after_time];

            unsafe {
                libc::futimes(before_file.as_raw_fd(), before_times.as_ptr());
                libc::futimes(after_file.as_raw_fd(), after_times.as_ptr());
            }
        }

        sleep_500_millis().await;
        writeln!(&mut before_file, "second line").unwrap();
        writeln!(&mut after_file, "_second line").unwrap();

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;
        let before_lines = received
            .iter()
            .filter(|event| event.as_log()["file"].to_string_lossy().ends_with("before"))
            .map(|event| event.as_log()[log_schema().message_key()].to_string_lossy())
            .collect::<Vec<_>>();
        let after_lines = received
            .iter()
            .filter(|event| event.as_log()["file"].to_string_lossy().ends_with("after"))
            .map(|event| event.as_log()[log_schema().message_key()].to_string_lossy())
            .collect::<Vec<_>>();
        assert_eq!(before_lines, vec!["second line"]);
        assert_eq!(after_lines, vec!["_first line", "_second line"]);
    }

    #[tokio::test]
    async fn file_max_line_bytes() {
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            max_line_bytes: 10,
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();

        sleep_500_millis().await; // The files must be observed at their original lengths before writing to them

        writeln!(&mut file, "short").unwrap();
        writeln!(&mut file, "this is too long").unwrap();
        writeln!(&mut file, "11 eleven11").unwrap();
        let super_long = std::iter::repeat("This line is super long and will take up more space that BufReader's internal buffer, just to make sure that everything works properly when multiple read calls are involved").take(10000).collect::<String>();
        writeln!(&mut file, "{}", super_long).unwrap();
        writeln!(&mut file, "exactly 10").unwrap();
        writeln!(&mut file, "it can end on a line that's too long").unwrap();

        sleep_500_millis().await;
        sleep_500_millis().await;

        writeln!(&mut file, "and then continue").unwrap();
        writeln!(&mut file, "last short").unwrap();

        sleep_500_millis().await;
        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(
            rx.map(|event| {
                event
                    .as_log()
                    .get(log_schema().message_key())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>(),
        )
        .await;

        assert_eq!(
            received,
            vec!["short".into(), "exactly 10".into(), "last short".into()]
        );
    }

    #[tokio::test]
    async fn test_multi_line_aggregation_legacy() {
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            message_start_indicator: Some("INFO".into()),
            multi_line_timeout: 25, // less than 50 in sleep()
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();

        sleep_500_millis().await; // The files must be observed at their original lengths before writing to them

        writeln!(&mut file, "leftover foo").unwrap();
        writeln!(&mut file, "INFO hello").unwrap();
        writeln!(&mut file, "INFO goodbye").unwrap();
        writeln!(&mut file, "part of goodbye").unwrap();

        sleep_500_millis().await;

        writeln!(&mut file, "INFO hi again").unwrap();
        writeln!(&mut file, "and some more").unwrap();
        writeln!(&mut file, "INFO hello").unwrap();

        sleep_500_millis().await;

        writeln!(&mut file, "too slow").unwrap();
        writeln!(&mut file, "INFO doesn't have").unwrap();
        writeln!(&mut file, "to be INFO in").unwrap();
        writeln!(&mut file, "the middle").unwrap();

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(
            rx.map(|event| {
                event
                    .as_log()
                    .get(log_schema().message_key())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>(),
        )
        .await;

        assert_eq!(
            received,
            vec![
                "leftover foo".into(),
                "INFO hello".into(),
                "INFO goodbye\npart of goodbye".into(),
                "INFO hi again\nand some more".into(),
                "INFO hello".into(),
                "too slow".into(),
                "INFO doesn't have".into(),
                "to be INFO in\nthe middle".into(),
            ]
        );
    }

    #[tokio::test]
    async fn test_multi_line_aggregation() {
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            multiline: Some(MultilineConfig {
                start_pattern: "INFO".to_owned(),
                condition_pattern: "INFO".to_owned(),
                mode: line_agg::Mode::HaltBefore,
                timeout_ms: 25, // less than 50 in sleep()
            }),
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();

        sleep_500_millis().await; // The files must be observed at their original lengths before writing to them

        writeln!(&mut file, "leftover foo").unwrap();
        writeln!(&mut file, "INFO hello").unwrap();
        writeln!(&mut file, "INFO goodbye").unwrap();
        writeln!(&mut file, "part of goodbye").unwrap();

        sleep_500_millis().await;

        writeln!(&mut file, "INFO hi again").unwrap();
        writeln!(&mut file, "and some more").unwrap();
        writeln!(&mut file, "INFO hello").unwrap();

        sleep_500_millis().await;

        writeln!(&mut file, "too slow").unwrap();
        writeln!(&mut file, "INFO doesn't have").unwrap();
        writeln!(&mut file, "to be INFO in").unwrap();
        writeln!(&mut file, "the middle").unwrap();

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(
            rx.map(|event| {
                event
                    .as_log()
                    .get(log_schema().message_key())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>(),
        )
        .await;

        assert_eq!(
            received,
            vec![
                "leftover foo".into(),
                "INFO hello".into(),
                "INFO goodbye\npart of goodbye".into(),
                "INFO hi again\nand some more".into(),
                "INFO hello".into(),
                "too slow".into(),
                "INFO doesn't have".into(),
                "to be INFO in\nthe middle".into(),
            ]
        );
    }

    #[tokio::test]
    async fn test_fair_reads() {
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            start_at_beginning: true,
            max_read_bytes: 1,
            oldest_first: false,
            ..test_default_file_config(&dir)
        };

        let older_path = dir.path().join("z_older_file");
        let mut older = File::create(&older_path).unwrap();

        sleep_500_millis().await;

        let newer_path = dir.path().join("a_newer_file");
        let mut newer = File::create(&newer_path).unwrap();

        writeln!(&mut older, "hello i am the old file").unwrap();
        writeln!(&mut older, "i have been around a while").unwrap();
        writeln!(&mut older, "you can read newer files at the same time").unwrap();

        writeln!(&mut newer, "and i am the new file").unwrap();
        writeln!(&mut newer, "this should be interleaved with the old one").unwrap();
        writeln!(&mut newer, "which is fine because we want fairness").unwrap();

        sleep_500_millis().await;

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(
            rx.map(|event| {
                event
                    .as_log()
                    .get(log_schema().message_key())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>(),
        )
        .await;

        assert_eq!(
            received,
            vec![
                "hello i am the old file".into(),
                "and i am the new file".into(),
                "i have been around a while".into(),
                "this should be interleaved with the old one".into(),
                "you can read newer files at the same time".into(),
                "which is fine because we want fairness".into(),
            ]
        );
    }

    #[tokio::test]
    async fn test_oldest_first() {
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            start_at_beginning: true,
            max_read_bytes: 1,
            oldest_first: true,
            ..test_default_file_config(&dir)
        };

        let older_path = dir.path().join("z_older_file");
        let mut older = File::create(&older_path).unwrap();

        sleep_500_millis().await;

        let newer_path = dir.path().join("a_newer_file");
        let mut newer = File::create(&newer_path).unwrap();

        writeln!(&mut older, "hello i am the old file").unwrap();
        writeln!(&mut older, "i have been around a while").unwrap();
        writeln!(&mut older, "you should definitely read all of me first").unwrap();

        writeln!(&mut newer, "i'm new").unwrap();
        writeln!(&mut newer, "hopefully you read all the old stuff first").unwrap();
        writeln!(&mut newer, "because otherwise i'm not going to make sense").unwrap();

        sleep_500_millis().await;

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(
            rx.map(|event| {
                event
                    .as_log()
                    .get(log_schema().message_key())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>(),
        )
        .await;

        assert_eq!(
            received,
            vec![
                "hello i am the old file".into(),
                "i have been around a while".into(),
                "you should definitely read all of me first".into(),
                "i'm new".into(),
                "hopefully you read all the old stuff first".into(),
                "because otherwise i'm not going to make sense".into(),
            ]
        );
    }

    #[tokio::test]
    async fn test_split_reads() {
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            start_at_beginning: true,
            max_read_bytes: 1,
            ..test_default_file_config(&dir)
        };

        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();

        writeln!(&mut file, "hello i am a normal line").unwrap();

        sleep_500_millis().await;

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        sleep_500_millis().await;

        write!(&mut file, "i am not a full line").unwrap();

        // Longer than the EOF timeout
        sleep_500_millis().await;

        writeln!(&mut file, " until now").unwrap();

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(
            rx.map(|event| {
                event
                    .as_log()
                    .get(log_schema().message_key())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>(),
        )
        .await;

        assert_eq!(
            received,
            vec![
                "hello i am a normal line".into(),
                "i am not a full line until now".into(),
            ]
        );
    }

    #[tokio::test]
    async fn test_gzipped_file() {
        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![PathBuf::from("tests/data/gzipped.log")],
            // TODO: remove this once files are fingerprinted after decompression
            //
            // Currently, this needs to be smaller than the total size of the compressed file
            // because the fingerprinter tries to read until a newline, which it's not going to see
            // in the compressed data, or this number of bytes. If it hits EOF before that, it
            // can't return a fingerprint because the value would change once more data is written.
            max_line_bytes: 100,
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        sleep_500_millis().await;

        drop(trigger_shutdown);

        let received = wait_with_timeout(
            rx.map(|event| {
                event
                    .as_log()
                    .get(log_schema().message_key())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>(),
        )
        .await;

        assert_eq!(
            received,
            vec![
                "this is a simple file".into(),
                "i have been compressed".into(),
                "in order to make me smaller".into(),
                "but you can still read me".into(),
                "hooray".into(),
            ]
        );
    }

    #[tokio::test]
    async fn remove_file() {
        let n = 5;
        let remove_after = 1;

        let (tx, rx) = Pipeline::new_test();
        let (trigger_shutdown, shutdown, _) = ShutdownSignal::new_wired();

        let dir = tempdir().unwrap();
        let config = file::FileConfig {
            include: vec![dir.path().join("*")],
            remove_after: Some(remove_after),
            glob_minimum_cooldown: 100,
            ..test_default_file_config(&dir)
        };

        let source = file::file_source(&config, config.data_dir.clone().unwrap(), shutdown, tx);
        tokio::spawn(source);

        let path = dir.path().join("file");
        let mut file = File::create(&path).unwrap();

        sleep_500_millis().await; // The files must be observed at their original lengths before writing to them

        for i in 0..n {
            writeln!(&mut file, "{}", i).unwrap();
        }
        std::mem::drop(file);

        for _ in 0..10 {
            // Wait for remove grace period to end.
            delay_for(Duration::from_secs(remove_after + 1)).await;

            if File::open(&path).is_err() {
                break;
            }
        }

        drop(trigger_shutdown);

        let received = wait_with_timeout(rx.collect::<Vec<_>>()).await;
        assert_eq!(received.len(), n);

        match File::open(&path) {
            Ok(_) => panic!("File wasn't removed"),
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::NotFound),
        }
    }
}
