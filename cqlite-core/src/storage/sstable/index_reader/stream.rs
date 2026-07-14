//! Forward-streaming `Index.db` entry iterator for the Summary-guided BIG scan
//! path (issue #2412 §C, Stage 4).
//!
//! [`super::lazy`] gives a BIG reader a lazy open (no full parse) and a bounded
//! per-lookup interval read. A full/range scan needs the third shape: iterate
//! `Index.db` entries FORWARD from a `Summary.db`-guided start offset, one entry
//! at a time, WITHOUT materialising the whole `Vec<PartitionIndexEntry>` (the
//! ~500MB-resident structure #2385 retired at open). This iterator feeds the
//! #2361 streaming full-index walk with a disk-streamed entry source instead of
//! a resident `Vec`.
//!
//! Memory is bounded to one refill window plus one entry: consumed bytes are
//! drained after each parse, and the file is read in fixed [`REFILL_CHUNK`]
//! chunks. A partition-count-independent footprint is the whole point.
//!
//! No-heuristics (issue #28): every byte comes from the authoritative on-disk
//! `Index.db` entry framing ([`parse_big_index_entry`]); the start offset is an
//! authoritative `Summary.db` sample position. Nothing is inferred from value
//! bytes.

use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::{parse_big_index_entry, PartitionIndexEntry};
use crate::error::Result;

/// Bytes read from `Index.db` per refill. Large enough to amortise syscalls over
/// many small entries (a BIG entry is typically key + two vints ≈ tens of bytes),
/// small enough that peak buffer stays partition-count-independent.
const REFILL_CHUNK: usize = 64 * 1024;

/// A forward-only, bounded-memory reader of `Index.db` partition entries starting
/// at an authoritative `Summary.db` sample offset (issue #2412 §C).
pub(crate) struct IndexEntryStream {
    file: File,
    /// Unparsed bytes: `buf[cursor..]` is the not-yet-parsed remainder.
    buf: Vec<u8>,
    cursor: usize,
    /// The file returned 0 bytes on the last read (no more data to refill).
    eof: bool,
    /// Total whole entries yielded so far (the scale-free work accounting).
    entries_touched: usize,
    /// After the stream ends, whether unparseable bytes remained at EOF — a
    /// mid-entry-truncated `Index.db` tail (Signal A, issue #2302). The scan
    /// terminus consults this to distinguish a clean end from a cut stream.
    truncated_tail: bool,
}

impl IndexEntryStream {
    /// Open `index_path` and seek to `start_position` (an authoritative
    /// `Summary.db` sample offset; `0` for a full scan from the index start).
    pub(crate) async fn open(index_path: &Path, start_position: u64) -> Result<Self> {
        let mut file = File::open(index_path).await?;
        if start_position > 0 {
            file.seek(std::io::SeekFrom::Start(start_position)).await?;
        }
        Ok(Self {
            file,
            buf: Vec::new(),
            cursor: 0,
            eof: false,
            entries_touched: 0,
            truncated_tail: false,
        })
    }

    /// Number of whole entries yielded so far (bounded work accounting).
    #[cfg(test)]
    pub(crate) fn entries_touched(&self) -> usize {
        self.entries_touched
    }

    /// Whether the stream ended with unparseable bytes still buffered — a
    /// mid-entry-truncated `Index.db` (Signal A). Only meaningful once
    /// [`Self::next`] has returned `None`.
    pub(crate) fn truncated_tail(&self) -> bool {
        self.truncated_tail
    }

    /// Read one more [`REFILL_CHUNK`] from the file into `buf`, first dropping the
    /// already-consumed prefix so the buffer stays bounded. Returns the number of
    /// new bytes appended (0 at EOF).
    async fn refill(&mut self) -> Result<usize> {
        if self.cursor > 0 {
            self.buf.drain(0..self.cursor);
            self.cursor = 0;
        }
        let prev_len = self.buf.len();
        self.buf.resize(prev_len + REFILL_CHUNK, 0);
        let n = self.file.read(&mut self.buf[prev_len..]).await?;
        self.buf.truncate(prev_len + n);
        if n == 0 {
            self.eof = true;
        }
        Ok(n)
    }

    /// Yield the next whole `Index.db` entry, or `None` at a clean end / a
    /// truncated tail (distinguished via [`Self::truncated_tail`]).
    ///
    /// The BIG entry parser ([`parse_big_index_entry`]) is a `complete` combinator:
    /// a short buffer fails indistinguishably from corruption, so on a parse
    /// failure this refills and retries until either the entry parses or the file
    /// is exhausted. At EOF with a non-empty unparseable remainder the tail was
    /// cut mid-entry (`truncated_tail = true`).
    pub(crate) async fn next(&mut self) -> Result<Option<PartitionIndexEntry>> {
        loop {
            // Attempt a parse from the current buffer window. The borrow of
            // `slice`/`rest` ends before any mutation of `self.buf`.
            let parsed = {
                let slice = &self.buf[self.cursor..];
                if slice.is_empty() {
                    None
                } else {
                    match parse_big_index_entry(slice) {
                        Ok((rest, entry)) => Some((slice.len() - rest.len(), entry)),
                        Err(_) => None,
                    }
                }
            };
            if let Some((consumed, entry)) = parsed {
                // Forward-progress guard (mirrors the full parser's debug_assert):
                // a zero-width parse would loop forever — treat as end-of-stream.
                if consumed == 0 {
                    self.truncated_tail = self.cursor < self.buf.len();
                    return Ok(None);
                }
                self.cursor += consumed;
                self.entries_touched += 1;
                return Ok(Some(entry));
            }
            // Could not parse a whole entry from the buffered bytes.
            if self.eof {
                // Nothing more to read: a non-empty remainder is a truncated tail.
                self.truncated_tail = self.cursor < self.buf.len();
                return Ok(None);
            }
            if self.refill().await? == 0 {
                // EOF reached during refill; loop once more to classify the tail.
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode one BIG `Index.db` entry: `[key_len u16 BE][key][data_offset vint][promoted_len vint=0]`.
    fn encode_entry(key: &[u8], data_offset: u64) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(key.len() as u16).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&crate::parser::vint::encode_vuint(data_offset));
        out.extend_from_slice(&crate::parser::vint::encode_vuint(0));
        out
    }

    fn write_temp(name: &str, bytes: &[u8]) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "cqlite-2412-stream-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nb-1-big-Index.db");
        std::fs::write(&path, bytes).unwrap();
        (dir, path)
    }

    async fn drain(path: &Path, start: u64) -> (Vec<PartitionIndexEntry>, bool, usize) {
        let mut s = IndexEntryStream::open(path, start).await.unwrap();
        let mut out = Vec::new();
        while let Some(e) = s.next().await.unwrap() {
            out.push(e);
        }
        (out, s.truncated_tail(), s.entries_touched())
    }

    #[tokio::test]
    async fn streams_every_entry_in_order_from_offset_zero() {
        let mut bytes = Vec::new();
        for (k, off) in [
            (b"aaaa".as_slice(), 0u64),
            (b"bbbb", 100),
            (b"cccc", 250),
            (b"dddd", 400),
        ] {
            bytes.extend_from_slice(&encode_entry(k, off));
        }
        let (dir, path) = write_temp("order", &bytes);
        let (entries, truncated, touched) = drain(&path, 0).await;
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].key_digest.as_ref(), b"aaaa");
        assert_eq!(entries[0].data_offset, 0);
        assert_eq!(entries[3].key_digest.as_ref(), b"dddd");
        assert_eq!(entries[3].data_offset, 400);
        assert!(
            !truncated,
            "a clean stream must not report a truncated tail"
        );
        assert_eq!(touched, 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn seeking_to_a_sample_offset_starts_mid_file() {
        let first = encode_entry(b"before", 0);
        let rest = {
            let mut v = Vec::new();
            v.extend_from_slice(&encode_entry(b"target", 900));
            v.extend_from_slice(&encode_entry(b"after", 950));
            v
        };
        let mut bytes = first.clone();
        bytes.extend_from_slice(&rest);
        let (dir, path) = write_temp("seek", &bytes);
        let (entries, _truncated, _touched) = drain(&path, first.len() as u64).await;
        assert_eq!(entries.len(), 2, "seek skips the first entry");
        assert_eq!(entries[0].key_digest.as_ref(), b"target");
        assert_eq!(entries[1].key_digest.as_ref(), b"after");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn truncated_tail_is_reported() {
        let mut bytes = encode_entry(b"whole", 10);
        // A dangling key-length header with no body: a mid-entry cut (Signal A).
        bytes.extend_from_slice(&[0x00, 0x40]);
        let (dir, path) = write_temp("trunc", &bytes);
        let (entries, truncated, touched) = drain(&path, 0).await;
        assert_eq!(entries.len(), 1, "only the one whole entry is yielded");
        assert!(truncated, "the dangling tail must be flagged truncated");
        assert_eq!(touched, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn streams_across_refill_boundaries() {
        // Enough entries that the total exceeds REFILL_CHUNK, forcing multiple
        // refills — the bounded-memory streaming property under test.
        let n = 4000usize;
        let mut bytes = Vec::new();
        for i in 0..n {
            let key = format!("key{i:08}");
            bytes.extend_from_slice(&encode_entry(key.as_bytes(), (i * 10) as u64));
        }
        assert!(bytes.len() > REFILL_CHUNK, "fixture must span >1 refill");
        let (dir, path) = write_temp("refill", &bytes);
        let (entries, truncated, touched) = drain(&path, 0).await;
        assert_eq!(entries.len(), n);
        assert_eq!(entries[n - 1].data_offset, ((n - 1) * 10) as u64);
        assert!(!truncated);
        assert_eq!(touched, n);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_file_is_a_clean_empty_stream() {
        let (dir, path) = write_temp("empty", &[]);
        let (entries, truncated, touched) = drain(&path, 0).await;
        assert!(entries.is_empty());
        assert!(!truncated, "a zero-length file is not a truncated tail");
        assert_eq!(touched, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
