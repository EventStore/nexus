use crate::FilePosition;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use flate2::bufread::MultiGzDecoder;
use std::{
    fs::{self, File},
    io::{self, BufRead, Seek},
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::metadata_ext::PortableFileExt;

/// The `FileWatcher` struct defines the polling based state machine which reads
/// from a file path, transparently updating the underlying file descriptor when
/// the file has been rolled over, as is common for logs.
///
/// The `FileWatcher` is expected to live for the lifetime of the file
/// path. `FileServer` is responsible for clearing away `FileWatchers` which no
/// longer exist.
pub struct FileWatcher {
    pub path: PathBuf,
    findable: bool,
    reader: Box<dyn BufRead>,
    file_position: FilePosition,
    devno: u64,
    inode: u64,
    is_dead: bool,
    last_read_attempt: Instant,
    last_read_success: Instant,
    max_line_bytes: usize,
    buf: BytesMut,
}

impl FileWatcher {
    /// Create a new `FileWatcher`
    ///
    /// The input path will be used by `FileWatcher` to prime its state
    /// machine. A `FileWatcher` tracks _only one_ file. This function returns
    /// None if the path does not exist or is not readable by the current process.
    pub fn new(
        path: PathBuf,
        file_position: FilePosition,
        ignore_before: Option<DateTime<Utc>>,
        max_line_bytes: usize,
    ) -> Result<FileWatcher, io::Error> {
        let f = fs::File::open(&path)?;
        let (devno, ino) = (f.portable_dev()?, f.portable_ino()?);
        let metadata = f.metadata()?;
        let mut reader = io::BufReader::new(f);

        let too_old = if let (Some(ignore_before), Ok(modified_time)) = (
            ignore_before,
            metadata.modified().map(DateTime::<Utc>::from),
        ) {
            modified_time < ignore_before
        } else {
            false
        };

        let (reader, file_position): (Box<dyn BufRead>, FilePosition) = if is_gzipped(&mut reader)?
        {
            if file_position != 0 || too_old {
                // We can't accurately seek into gzipped files without manually scanning through
                // the entire thing, so for now we simply refuse to read gzipped files for which we
                // already have a stored file position from a previous run.
                debug!(
                    message = "Not re-reading gzipped file with existing stored offset.",
                    ?path,
                    %file_position
                );
                (Box::new(null_reader()), file_position)
            } else {
                (Box::new(io::BufReader::new(MultiGzDecoder::new(reader))), 0)
            }
        } else if too_old {
            let pos = reader.seek(io::SeekFrom::End(0)).unwrap();
            (Box::new(reader), pos)
        } else {
            let pos = reader.seek(io::SeekFrom::Start(file_position)).unwrap();
            (Box::new(reader), pos)
        };

        let ts = metadata
            .modified()
            .ok()
            .and_then(|mtime| mtime.elapsed().ok())
            .and_then(|diff| Instant::now().checked_sub(diff))
            .unwrap_or_else(Instant::now);

        Ok(FileWatcher {
            path,
            findable: true,
            reader,
            file_position,
            devno,
            inode: ino,
            is_dead: false,
            last_read_attempt: ts,
            last_read_success: ts,
            max_line_bytes,
            buf: BytesMut::new(),
        })
    }

    pub fn update_path(&mut self, path: PathBuf) -> io::Result<()> {
        let file_handle = File::open(&path)?;
        if (file_handle.portable_dev()?, file_handle.portable_ino()?) != (self.devno, self.inode) {
            let mut reader = io::BufReader::new(fs::File::open(&path)?);
            let gzipped = is_gzipped(&mut reader)?;
            let new_reader: Box<dyn BufRead> = if gzipped {
                if self.file_position != 0 {
                    Box::new(null_reader())
                } else {
                    Box::new(io::BufReader::new(MultiGzDecoder::new(reader)))
                }
            } else {
                reader.seek(io::SeekFrom::Start(self.file_position))?;
                Box::new(reader)
            };
            self.reader = new_reader;
            self.devno = file_handle.portable_dev()?;
            self.inode = file_handle.portable_ino()?;
        }
        self.path = path;
        Ok(())
    }

    pub fn set_file_findable(&mut self, f: bool) {
        self.findable = f;
    }

    pub fn file_findable(&self) -> bool {
        self.findable
    }

    pub fn set_dead(&mut self) {
        self.is_dead = true;
    }

    pub fn dead(&self) -> bool {
        self.is_dead
    }

    pub fn get_file_position(&self) -> FilePosition {
        self.file_position
    }

    /// Read a single line from the underlying file
    ///
    /// This function will attempt to read a new line from its file, blocking,
    /// up to some maximum but unspecified amount of time. `read_line` will open
    /// a new file handler as needed, transparently to the caller.
    pub fn read_line(&mut self) -> io::Result<Option<Bytes>> {
        self.track_read_attempt();

        let reader = &mut self.reader;
        let file_position = &mut self.file_position;
        match read_until_with_max_size(
            reader,
            file_position,
            b'\n',
            &mut self.buf,
            self.max_line_bytes,
        ) {
            Ok(Some(_)) => {
                self.track_read_success();
                Ok(Some(self.buf.split().freeze()))
            }
            Ok(None) => {
                if !self.file_findable() {
                    self.set_dead();
                    // File has been deleted, so return what we have in the buffer, even though it
                    // didn't end with a newline. This is not a perfect signal for when we should
                    // give up waiting for a newline, but it's decent.
                    Ok(Some(self.buf.split().freeze()))
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                if let io::ErrorKind::NotFound = e.kind() {
                    self.set_dead();
                }
                Err(e)
            }
        }
    }

    fn track_read_attempt(&mut self) {
        self.last_read_attempt = Instant::now();
    }

    fn track_read_success(&mut self) {
        self.last_read_success = Instant::now();
    }

    pub fn last_read_success(&self) -> Instant {
        self.last_read_success
    }

    pub fn should_read(&self) -> bool {
        self.last_read_success.elapsed() < Duration::from_secs(10)
            || self.last_read_attempt.elapsed() > Duration::from_secs(10)
    }
}

fn is_gzipped(r: &mut io::BufReader<fs::File>) -> io::Result<bool> {
    let header_bytes = r.fill_buf()?;
    Ok(header_bytes.starts_with(&[0x1f, 0x8b]))
}

fn null_reader() -> impl BufRead {
    io::Cursor::new(Vec::new())
}

// Tweak of https://github.com/rust-lang/rust/blob/bf843eb9c2d48a80a5992a5d60858e27269f9575/src/libstd/io/mod.rs#L1471
// After more than max_size bytes are read as part of a single line, this discard the remaining bytes
// in that line, and then starts again on the next line.
fn read_until_with_max_size<R: BufRead + ?Sized>(
    r: &mut R,
    p: &mut FilePosition,
    delim: u8,
    buf: &mut BytesMut,
    max_size: usize,
) -> io::Result<Option<usize>> {
    let mut total_read = 0;
    let mut discarding = false;
    loop {
        let available = match r.fill_buf() {
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };

        let (done, used) = {
            // TODO: use memchr to make this faster
            match available.iter().position(|&b| b == delim) {
                Some(i) => {
                    if !discarding {
                        buf.extend_from_slice(&available[..i]);
                    }
                    (true, i + 1)
                }
                None => {
                    if !discarding {
                        buf.extend_from_slice(available);
                    }
                    (false, available.len())
                }
            }
        };
        r.consume(used);
        *p += used as u64; // do this at exactly same time
        total_read += used;

        if !discarding && buf.len() > max_size {
            warn!(
                message = "Found line that exceeds max_line_bytes; discarding.",
                internal_log_rate_secs = 30
            );
            discarding = true;
        }

        if done {
            if !discarding {
                return Ok(Some(total_read));
            } else {
                discarding = false;
                buf.clear();
            }
        } else if used == 0 {
            // We've hit EOF but not yet seen a newline. This can happen when unlucky timing causes
            // us to observe an incomplete write. We return None here and let the loop continue
            // next time the method is called. This is safe because the buffer is specific to this
            // FileWatcher.
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod test {
    use super::read_until_with_max_size;
    use bytes::BytesMut;
    use std::io::Cursor;

    #[test]
    fn test_read_until_with_max_size() {
        let mut buf = Cursor::new(&b"12"[..]);
        let mut pos = 0;
        let mut v = BytesMut::new();
        let p = read_until_with_max_size(&mut buf, &mut pos, b'3', &mut v, 1000).unwrap();
        assert_eq!(pos, 2);
        assert_eq!(p, None);
        assert_eq!(&*v, b"12");
        let mut buf = Cursor::new(&b"34"[..]);
        let p = read_until_with_max_size(&mut buf, &mut pos, b'3', &mut v, 1000).unwrap();
        assert_eq!(pos, 3);
        assert_eq!(p, Some(1));
        assert_eq!(&*v, b"12");

        let mut buf = Cursor::new(&b"1233"[..]);
        let mut pos = 0;
        let mut v = BytesMut::new();
        let p = read_until_with_max_size(&mut buf, &mut pos, b'3', &mut v, 1000).unwrap();
        assert_eq!(pos, 3);
        assert_eq!(p, Some(3));
        assert_eq!(&*v, b"12");
        v.truncate(0);
        let p = read_until_with_max_size(&mut buf, &mut pos, b'3', &mut v, 1000).unwrap();
        assert_eq!(pos, 4);
        assert_eq!(p, Some(1));
        assert_eq!(&*v, b"");
        v.truncate(0);
        let p = read_until_with_max_size(&mut buf, &mut pos, b'3', &mut v, 1000).unwrap();
        assert_eq!(pos, 4);
        assert_eq!(p, None);
        assert_eq!(&*v, [0; 0]);

        let mut buf = Cursor::new(&b"short\nthis is too long\nexact size\n11 eleven11\n"[..]);
        let mut pos = 0;
        let mut v = BytesMut::new();
        let p = read_until_with_max_size(&mut buf, &mut pos, b'\n', &mut v, 10).unwrap();
        assert_eq!(pos, 6);
        assert_eq!(p, Some(6));
        assert_eq!(&*v, b"short");
        v.truncate(0);
        let p = read_until_with_max_size(&mut buf, &mut pos, b'\n', &mut v, 10).unwrap();
        assert_eq!(pos, 34);
        assert_eq!(p, Some(28));
        assert_eq!(&*v, b"exact size");
        v.truncate(0);
        let p = read_until_with_max_size(&mut buf, &mut pos, b'\n', &mut v, 10).unwrap();
        assert_eq!(pos, 46);
        assert_eq!(p, None);
        assert_eq!(&*v, [0; 0]);
    }
}
