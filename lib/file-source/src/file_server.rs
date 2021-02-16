use crate::{
    checkpointer::{Checkpointer, CheckpointsView},
    file_watcher::FileWatcher,
    fingerprinter::{FileFingerprint, Fingerprinter},
    FileSourceInternalEvents,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{
    executor::block_on,
    future::{select, Either, FutureExt},
    stream, Future, Sink, SinkExt,
};
use indexmap::IndexMap;
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, remove_file},
    path::PathBuf,
    sync::Arc,
    time::{self, Duration},
};
use tokio::time::delay_for;

use crate::paths_provider::PathsProvider;

/// `FileServer` is a Source which cooperatively schedules reads over files,
/// converting the lines of said files into `LogLine` structures. As
/// `FileServer` is intended to be useful across multiple operating systems with
/// POSIX filesystem semantics `FileServer` must poll for changes. That is, no
/// event notification is used by `FileServer`.
///
/// `FileServer` is configured on a path to watch. The files do _not_ need to
/// exist at startup. `FileServer` will discover new files which match
/// its path in at most 60 seconds.
pub struct FileServer<PP, E: FileSourceInternalEvents>
where
    PP: PathsProvider,
{
    pub paths_provider: PP,
    pub max_read_bytes: usize,
    pub start_at_beginning: bool,
    pub ignore_before: Option<DateTime<Utc>>,
    pub max_line_bytes: usize,
    pub data_dir: PathBuf,
    pub glob_minimum_cooldown: Duration,
    pub fingerprinter: Fingerprinter,
    pub oldest_first: bool,
    pub remove_after: Option<Duration>,
    pub emitter: E,
    pub handle: tokio::runtime::Handle,
}

/// `FileServer` as Source
///
/// The 'run' of `FileServer` performs the cooperative scheduling of reads over
/// `FileServer`'s configured files. Much care has been taking to make this
/// scheduling 'fair', meaning busy files do not drown out quiet files or vice
/// versa but there's no one perfect approach. Very fast files _will_ be lost if
/// your system aggressively rolls log files. `FileServer` will keep a file
/// handler open but should your system move so quickly that a file disappears
/// before `FileServer` is able to open it the contents will be lost. This should be a
/// rare occurrence.
///
/// Specific operating systems support evented interfaces that correct this
/// problem but your intrepid authors know of no generic solution.
impl<PP, E> FileServer<PP, E>
where
    PP: PathsProvider,
    E: FileSourceInternalEvents,
{
    pub fn run<C, S>(
        self,
        mut chans: C,
        shutdown: S,
    ) -> Result<Shutdown, <C as Sink<Vec<(Bytes, String)>>>::Error>
    where
        C: Sink<Vec<(Bytes, String)>> + Unpin,
        <C as Sink<Vec<(Bytes, String)>>>::Error: std::error::Error,
        S: Future + Unpin + Send + 'static,
        <S as Future>::Output: Clone + Send + Sync,
    {
        let mut fingerprint_buffer = Vec::new();

        let mut fp_map: IndexMap<FileFingerprint, FileWatcher> = Default::default();

        let mut backoff_cap: usize = 1;
        let mut lines = Vec::new();

        let mut checkpointer = Checkpointer::new(&self.data_dir);
        checkpointer.read_checkpoints(self.ignore_before);

        let mut known_small_files = HashSet::new();

        let mut existing_files = Vec::new();
        for path in self.paths_provider.paths().into_iter() {
            if let Some(file_id) = self.fingerprinter.get_fingerprint_or_log_error(
                &path,
                &mut fingerprint_buffer,
                &mut known_small_files,
                &self.emitter,
            ) {
                existing_files.push((path, file_id));
            }
        }

        existing_files.sort_by_key(|(path, _file_id)| {
            fs::metadata(&path)
                .and_then(|m| m.created())
                .map(DateTime::<Utc>::from)
                .unwrap_or_else(|_| Utc::now())
        });

        checkpointer.maybe_upgrade(existing_files.iter().map(|(_, id)| id).cloned());

        let checkpoints = checkpointer.view();

        let needs_checksum_upgrade = checkpoints.contains_bytes_checksums();

        for (path, file_id) in existing_files {
            if needs_checksum_upgrade {
                if let Ok(Some(old_checksum)) = self
                    .fingerprinter
                    .get_bytes_checksum(&path, &mut fingerprint_buffer)
                {
                    checkpoints.update_key(old_checksum, file_id)
                }
            }

            self.watch_new_file(
                path,
                file_id,
                &mut fp_map,
                &checkpoints,
                self.start_at_beginning,
            );
        }
        self.emitter.emit_files_open(fp_map.len());

        let mut stats = TimingStats::default();

        // Spawn the checkpoint writer task
        //
        // We have to do a lot of cloning here to convince the compiler that we aren't going to get
        // away with anything, but none of it should have any perf impact.
        let mut shutdown = shutdown.shared();
        let shutdown2 = shutdown.clone();
        let emitter = self.emitter.clone();
        let checkpointer = Arc::new(checkpointer);
        let sleep_duration = self.glob_minimum_cooldown;
        self.handle.spawn(async move {
            let mut done = false;
            loop {
                let sleep = tokio::time::delay_for(sleep_duration);
                match select(shutdown2.clone(), sleep).await {
                    Either::Left((_, _)) => done = true,
                    Either::Right((_, _)) => {}
                }

                let emitter = emitter.clone();
                let checkpointer = Arc::clone(&checkpointer);
                tokio::task::spawn_blocking(move || {
                    let start = time::Instant::now();
                    match checkpointer.write_checkpoints() {
                        Ok(count) => emitter.emit_file_checkpointed(count, start.elapsed()),
                        Err(error) => emitter.emit_file_checkpoint_write_failed(error),
                    }
                })
                .await
                .ok();

                if done {
                    break;
                }
            }
        });

        // Alright friends, how does this work?
        //
        // We want to avoid burning up users' CPUs. To do this we sleep after
        // reading lines out of files. But! We want to be responsive as well. We
        // keep track of a 'backoff_cap' to decide how long we'll wait in any
        // given loop. This cap grows each time we fail to read lines in an
        // exponential fashion to some hard-coded cap. To reduce time using glob,
        // we do not re-scan for major file changes (new files, moves, deletes),
        // or write new checkpoints, on every iteration.
        let mut next_glob_time = time::Instant::now();
        loop {
            // Glob find files to follow, but not too often.
            let now_time = time::Instant::now();
            if next_glob_time <= now_time {
                // Schedule the next glob time.
                next_glob_time = now_time.checked_add(self.glob_minimum_cooldown).unwrap();

                if stats.started_at.elapsed() > Duration::from_secs(1) {
                    stats.report();
                }

                if stats.started_at.elapsed() > Duration::from_secs(10) {
                    stats = TimingStats::default();
                }

                // Search (glob) for files to detect major file changes.
                let start = time::Instant::now();
                for (_file_id, watcher) in &mut fp_map {
                    watcher.set_file_findable(false); // assume not findable until found
                }
                for path in self.paths_provider.paths().into_iter() {
                    if let Some(file_id) = self.fingerprinter.get_fingerprint_or_log_error(
                        &path,
                        &mut fingerprint_buffer,
                        &mut known_small_files,
                        &self.emitter,
                    ) {
                        if let Some(watcher) = fp_map.get_mut(&file_id) {
                            // file fingerprint matches a watched file
                            let was_found_this_cycle = watcher.file_findable();
                            watcher.set_file_findable(true);
                            if watcher.path == path {
                                trace!(
                                    message = "Continue watching file.",
                                    path = ?path,
                                );
                            } else {
                                // matches a file with a different path
                                if !was_found_this_cycle {
                                    info!(
                                        message = "Watched file has been renamed.",
                                        path = ?path,
                                        old_path = ?watcher.path
                                    );
                                    watcher.update_path(path).ok(); // ok if this fails: might fix next cycle
                                } else {
                                    info!(
                                        message = "More than one file has the same fingerprint.",
                                        path = ?path,
                                        old_path = ?watcher.path
                                    );
                                    let (old_path, new_path) = (&watcher.path, &path);
                                    if let (Ok(old_modified_time), Ok(new_modified_time)) = (
                                        fs::metadata(&old_path).and_then(|m| m.modified()),
                                        fs::metadata(&new_path).and_then(|m| m.modified()),
                                    ) {
                                        if old_modified_time < new_modified_time {
                                            info!(
                                                message = "Switching to watch most recently modified file.",
                                                new_modified_time = ?new_modified_time,
                                                old_modified_time = ?old_modified_time,
                                            );
                                            watcher.update_path(path).ok(); // ok if this fails: might fix next cycle
                                        }
                                    }
                                }
                            }
                        } else {
                            // untracked file fingerprint
                            self.watch_new_file(path, file_id, &mut fp_map, &checkpoints, false);
                            self.emitter.emit_files_open(fp_map.len());
                        }
                    }
                }
                stats.record("discovery", start.elapsed());
            }

            // Collect lines by polling files.
            let mut global_bytes_read: usize = 0;
            let mut maxed_out_reading_single_file = false;
            for (&file_id, watcher) in &mut fp_map {
                if !watcher.should_read() {
                    continue;
                }

                let start = time::Instant::now();
                let mut bytes_read: usize = 0;
                while let Ok(Some(line)) = watcher.read_line() {
                    if line.is_empty() {
                        break;
                    }

                    let sz = line.len();
                    trace!(
                        message = "Read bytes.",
                        path = ?watcher.path,
                        bytes = ?sz
                    );
                    stats.record_bytes(sz);

                    bytes_read += sz;

                    lines.push((
                        line,
                        watcher.path.to_str().expect("not a valid path").to_owned(),
                    ));

                    if bytes_read > self.max_read_bytes {
                        maxed_out_reading_single_file = true;
                        break;
                    }
                }
                stats.record("reading", start.elapsed());

                if bytes_read > 0 {
                    global_bytes_read = global_bytes_read.saturating_add(bytes_read);
                    checkpoints.update(file_id, watcher.get_file_position());
                } else {
                    // Should the file be removed
                    if let Some(grace_period) = self.remove_after {
                        if watcher.last_read_success().elapsed() >= grace_period {
                            // Try to remove
                            match remove_file(&watcher.path) {
                                Ok(()) => {
                                    self.emitter.emit_file_deleted(&watcher.path);
                                    watcher.set_dead();
                                }
                                Err(error) => {
                                    // We will try again after some time.
                                    self.emitter.emit_file_delete_failed(&watcher.path, error);
                                }
                            }
                        }
                    }
                }

                // Do not move on to newer files if we are behind on an older file
                if self.oldest_first && maxed_out_reading_single_file {
                    break;
                }
            }

            // A FileWatcher is dead when the underlying file has disappeared.
            // If the FileWatcher is dead we don't retain it; it will be deallocated.
            fp_map.retain(|file_id, watcher| {
                if watcher.dead() {
                    self.emitter.emit_file_unwatched(&watcher.path);
                    checkpoints.set_dead(*file_id);
                    false
                } else {
                    true
                }
            });
            self.emitter.emit_files_open(fp_map.len());

            let start = time::Instant::now();
            let to_send = std::mem::take(&mut lines);
            let mut stream = stream::once(futures::future::ok(to_send));
            let result = block_on(chans.send_all(&mut stream));
            match result {
                Ok(()) => {}
                Err(error) => {
                    error!(message = "Output channel closed.", error = ?error);
                    return Err(error);
                }
            }
            stats.record("sending", start.elapsed());

            let start = time::Instant::now();
            // When no lines have been read we kick the backup_cap up by twice,
            // limited by the hard-coded cap. Else, we set the backup_cap to its
            // minimum on the assumption that next time through there will be
            // more lines to read promptly.
            if global_bytes_read == 0 {
                let lim = backoff_cap.saturating_mul(2);
                if lim > 2_048 {
                    backoff_cap = 2_048;
                } else {
                    backoff_cap = lim;
                }
            } else {
                backoff_cap = 1;
            }
            let backoff = backoff_cap.saturating_sub(global_bytes_read);

            // This works only if run inside tokio context since we are using
            // tokio's Timer. Outside of such context, this will panic on the first
            // call. Also since we are using block_on here and in the above code,
            // this should be run in its own thread. `spawn_blocking` fulfills
            // all of these requirements.
            let sleep = async move {
                if backoff > 0 {
                    delay_for(Duration::from_millis(backoff as u64)).await;
                }
            };
            futures::pin_mut!(sleep);
            match block_on(select(shutdown, sleep)) {
                Either::Left((_, _)) => return Ok(Shutdown),
                Either::Right((_, future)) => shutdown = future,
            }
            stats.record("sleeping", start.elapsed());
        }
    }

    fn watch_new_file(
        &self,
        path: PathBuf,
        file_id: FileFingerprint,
        fp_map: &mut IndexMap<FileFingerprint, FileWatcher>,
        checkpoints: &CheckpointsView,
        read_from_beginning: bool,
    ) {
        let file_position = if read_from_beginning {
            0
        } else {
            checkpoints.get(file_id).unwrap_or(0)
        };
        match FileWatcher::new(
            path.clone(),
            file_position,
            self.ignore_before,
            self.max_line_bytes,
        ) {
            Ok(mut watcher) => {
                if file_position == 0 {
                    self.emitter.emit_file_added(&path);
                } else {
                    self.emitter.emit_file_resumed(&path, file_position);
                }
                watcher.set_file_findable(true);
                fp_map.insert(file_id, watcher);
            }
            Err(error) => self.emitter.emit_file_watch_failed(&path, error),
        };
    }
}

/// A sentinel type to signal that file server was gracefully shut down.
///
/// The purpose of this type is to clarify the semantics of the result values
/// returned from the [`FileServer::run`] for both the users of the file server,
/// and the implementors.
#[derive(Debug)]
pub struct Shutdown;

struct TimingStats {
    started_at: time::Instant,
    segments: BTreeMap<&'static str, Duration>,
    events: usize,
    bytes: usize,
}

impl TimingStats {
    fn record(&mut self, key: &'static str, duration: Duration) {
        let segment = self.segments.entry(key).or_default();
        *segment += duration;
    }

    fn record_bytes(&mut self, bytes: usize) {
        self.events += 1;
        self.bytes += bytes;
    }

    fn report(&self) {
        let total = self.started_at.elapsed();
        let counted = self.segments.values().sum();
        let other = self.started_at.elapsed() - counted;
        let mut ratios = self
            .segments
            .iter()
            .map(|(k, v)| (*k, v.as_secs_f32() / total.as_secs_f32()))
            .collect::<BTreeMap<_, _>>();
        ratios.insert("other", other.as_secs_f32() / total.as_secs_f32());
        let (event_throughput, bytes_throughput) = if total.as_secs() > 0 {
            (
                self.events as u64 / total.as_secs(),
                self.bytes as u64 / total.as_secs(),
            )
        } else {
            (0, 0)
        };
        debug!(event_throughput = %scale(event_throughput), bytes_throughput = %scale(bytes_throughput), ?ratios);
    }
}

fn scale(bytes: u64) -> String {
    let units = ["", "k", "m", "g"];
    let mut bytes = bytes as f32;
    let mut i = 0;
    while bytes > 1000.0 && i <= 3 {
        bytes /= 1000.0;
        i += 1;
    }
    format!("{:.3}{}/sec", bytes, units[i])
}

impl Default for TimingStats {
    fn default() -> Self {
        Self {
            started_at: time::Instant::now(),
            segments: Default::default(),
            events: Default::default(),
            bytes: Default::default(),
        }
    }
}