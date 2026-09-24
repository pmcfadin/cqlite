//! `CommitLogReader` — decode of a Cassandra 5.0 CommitLog segment into
//! mutations (issue #2389).
//!
//! Opens a segment file, parses its [descriptor](super::descriptor)
//! authoritatively (version-gated, CRC-checked), fails closed on a
//! compressed/encrypted payload (design decision D3), and exposes a **lazy**
//! iterator over decoded MUTATIONS — one sync-section/record at a time, never
//! materializing the whole mutation set up front (design decision D4 / the
//! `oom-audit` structural expectation).
//!
//! **Memory posture, stated precisely (roborev finding, review-first pass —
//! this doc previously oversold it as "streaming"):** the decode side is
//! genuinely lazy, but opening a reader reads the entire segment FILE into
//! one `Vec<u8>` up front (bounded by [`MAX_SEGMENT_BYTES`]) — it does not
//! mmap or chunk the file itself. For a Cassandra-default 32 MB segment this
//! is well inside CQLite's <128MB target; a caller inspecting many segments
//! should still bound how many `CommitLogReader`s it keeps open
//! simultaneously, since each one holds its full segment in memory.

use std::fs;
use std::path::Path;

use crate::storage::commitlog::descriptor::CommitLogDescriptor;
use crate::storage::commitlog::frame::{FrameStep, FrameWalker};
use crate::storage::commitlog::mutation::{decode_mutation, Mutation};
use crate::storage::commitlog::schema::SchemaSet;
use crate::{Error, Result};

/// Upper bound on a segment file we will read into memory (128 MiB).
///
/// Cassandra caps a segment at 32 MB by default; even a doubled configuration
/// fits comfortably. Rejecting anything larger keeps the reader within CQLite's
/// <128MB memory target and guards against a pathological/hostile file.
pub const MAX_SEGMENT_BYTES: u64 = 128 * 1024 * 1024;

/// A reader over a single Cassandra CommitLog segment.
pub struct CommitLogReader {
    bytes: Vec<u8>,
    descriptor: CommitLogDescriptor,
    schemas: SchemaSet,
}

impl CommitLogReader {
    /// Open a segment for structural decode (table id + partition key + column
    /// names). Cell/clustering values require schemas — see
    /// [`CommitLogReader::open_with_schemas`].
    ///
    /// # Errors
    /// See [`CommitLogReader::open_with_schemas`].
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_schemas(path, SchemaSet::new())
    }

    /// Open a segment with per-table schemas for full clustering/cell decode.
    ///
    /// # Errors
    /// - [`Error::CorruptCommitLogFrame`] / [`Error::UnsupportedCommitLogVersion`]
    ///   from descriptor parsing, including malformed parameters JSON or
    ///   invalid UTF-8.
    /// - [`Error::UnsupportedFormat`] if the segment exceeds
    ///   [`MAX_SEGMENT_BYTES`], or declares a compressed/encrypted payload
    ///   (fail-closed: never decoded as if plain).
    pub fn open_with_schemas<P: AsRef<Path>>(path: P, schemas: SchemaSet) -> Result<Self> {
        let path = path.as_ref();
        let meta = fs::metadata(path)?;
        if meta.len() > MAX_SEGMENT_BYTES {
            return Err(Error::UnsupportedFormat(format!(
                "CommitLog segment {} is {} bytes, exceeding the {}-byte reader limit",
                path.display(),
                meta.len(),
                MAX_SEGMENT_BYTES
            )));
        }
        let bytes = fs::read(path)?;
        let descriptor = CommitLogDescriptor::parse(&bytes)?;

        if let Some(class) = &descriptor.compression_class {
            return Err(Error::UnsupportedFormat(format!(
                "CommitLog segment uses commitlog compression ({class}); \
                 compressed segments are not supported (issue #2389 v1 is \
                 uncompressed-only) — refusing to decode compressed bytes as plain"
            )));
        }
        if descriptor.encrypted {
            return Err(Error::UnsupportedFormat(
                "CommitLog segment declares an encryption context; \
                 encrypted segments are not supported"
                    .to_string(),
            ));
        }

        Ok(Self {
            bytes,
            descriptor,
            schemas,
        })
    }

    /// The parsed segment descriptor.
    pub fn descriptor(&self) -> &CommitLogDescriptor {
        &self.descriptor
    }

    /// A lazy iterator over the segment's decoded mutations.
    ///
    /// Yields `Ok(Mutation)` per record; a genuinely corrupt record yields one
    /// `Err(...)` and then ends. After iteration, [`MutationIter::truncated`]
    /// reports whether the segment ended on a torn tail.
    pub fn mutations(&self) -> MutationIter<'_> {
        MutationIter {
            walker: FrameWalker::new(&self.bytes, self.descriptor.id, self.descriptor.header_len),
            schemas: &self.schemas,
            truncated: false,
            done: false,
        }
    }
}

/// Lazy iterator over decoded mutations (see [`CommitLogReader::mutations`]).
pub struct MutationIter<'a> {
    walker: FrameWalker<'a>,
    schemas: &'a SchemaSet,
    truncated: bool,
    done: bool,
}

impl MutationIter<'_> {
    /// Whether the segment ended on a torn/truncated tail. Meaningful only once
    /// iteration has finished.
    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

impl Iterator for MutationIter<'_> {
    type Item = Result<Mutation>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.walker.next_frame() {
            Ok(FrameStep::Record(body)) => Some(decode_mutation(body, self.schemas)),
            Ok(FrameStep::End) => {
                self.done = true;
                None
            }
            Ok(FrameStep::Truncated) => {
                self.truncated = true;
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::commitlog::descriptor::tests::build_header;
    use crate::storage::commitlog::frame::{marker_crc, RECORD_OVERHEAD, SYNC_MARKER_SIZE};

    fn write_segment(params: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        // A valid descriptor header alone is enough: open_with_schemas checks
        // compression_class/encrypted BEFORE walking any frame body, so no sync
        // marker / mutation bytes are needed to exercise the fail-closed guard.
        let bytes = build_header(7, 1_689_012_345_678_i64, params);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("CommitLog-7-1.log");
        fs::write(&path, &bytes).expect("write segment");
        (dir, path)
    }

    fn write_segment_with_one_empty_partition(
        params: &str,
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        const SEGMENT_ID: i64 = 77;

        let mut bytes = build_header(7, SEGMENT_ID, params);
        let marker_pos = bytes.len();

        // One mutation containing one empty partition update: update count,
        // 16-byte table id, empty partition key, and ITER_IS_EMPTY.
        let mut body = Vec::with_capacity(19);
        body.push(1);
        body.extend_from_slice(&[0u8; 16]);
        body.extend_from_slice(&[0, 1]);

        let segment_len = marker_pos + SYNC_MARKER_SIZE + RECORD_OVERHEAD + body.len();
        let next_marker = i32::try_from(segment_len).expect("test segment offset fits marker");
        bytes.extend_from_slice(&next_marker.to_be_bytes());
        bytes.extend_from_slice(&marker_crc(SEGMENT_ID, marker_pos).to_be_bytes());

        let size = i32::try_from(body.len()).expect("test mutation size fits record");
        let size_bytes = size.to_be_bytes();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&size_bytes);
        bytes.extend_from_slice(&size_bytes);
        bytes.extend_from_slice(&hasher.finalize().to_be_bytes());
        bytes.extend_from_slice(&body);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&size_bytes);
        hasher.update(&body);
        bytes.extend_from_slice(&hasher.finalize().to_be_bytes());
        assert_eq!(bytes.len(), segment_len, "test segment construction");

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("CommitLog-77-1.log");
        fs::write(&path, &bytes).expect("write segment");
        (dir, path)
    }

    #[test]
    fn open_fails_closed_on_cassandra_shaped_compression() {
        // Real Cassandra 5.0.2 params: compressionClass is a plain string. This
        // is the exact key shape the descriptor's compression detection must
        // recognize so open() returns the typed UnsupportedFormat error rather
        // than walking into an undecodable compressed body (PR #2797 blocker).
        let (_dir, path) = write_segment(r#"{"compressionClass":"LZ4Compressor"}"#);
        assert!(matches!(
            CommitLogReader::open(&path),
            Err(Error::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn open_fails_closed_on_cassandra_shaped_encryption() {
        // Real Cassandra 5.0.2 params: encryption is declared via encCipher /
        // encKeyAlias / encIV. open() must fail closed with UnsupportedFormat
        // (PR #2797 blocker).
        let (_dir, path) =
            write_segment(r#"{"encCipher":"AES/CBC/PKCS5Padding","encKeyAlias":"testing:1"}"#);
        assert!(matches!(
            CommitLogReader::open(&path),
            Err(Error::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn open_rejects_malformed_or_non_object_descriptor_params() {
        for params in ["not-json", "[]", "null", r#"{"compressionClass":{}}"#] {
            let (_dir, path) = write_segment(params);
            assert!(
                matches!(
                    CommitLogReader::open(&path),
                    Err(Error::CorruptCommitLogFrame(_))
                ),
                "expected descriptor parameters to fail: {params}"
            );
        }
    }

    #[test]
    fn open_succeeds_on_uncompressed_unencrypted_header() {
        // Missing or explicitly null compression metadata means uncompressed;
        // valid unset forms must not be confused with malformed params.
        for params in ["{}", r#"{"compressionClass":null}"#] {
            let (_dir, path) = write_segment(params);
            let reader = CommitLogReader::open(&path).expect("plain segment opens");
            assert!(!reader.descriptor().is_unsupported_payload());
        }
    }

    #[test]
    fn variable_descriptor_length_keeps_reader_aligned_to_sync_frames() {
        let params = [
            "{}",
            r#"{"extra":"descriptor metadata that shifts the sync marker and framed record offsets"}"#,
        ];
        let mut header_lengths = Vec::new();

        for params_json in params {
            let (_dir, path) = write_segment_with_one_empty_partition(params_json);
            let reader = CommitLogReader::open(&path).expect("valid segment opens");
            header_lengths.push(reader.descriptor().header_len);

            let mut mutations = reader.mutations();
            let mutation = mutations
                .next()
                .expect("one framed mutation")
                .expect("valid mutation body");
            assert_eq!(mutation.updates.len(), 1);
            assert!(mutation.updates_complete);
            assert_eq!(mutation.updates[0].table_id, [0; 16]);
            assert!(mutation.updates[0].partition_key.is_empty());
            assert!(mutations.next().is_none(), "section ends cleanly");
            assert!(!mutations.truncated(), "valid segment is not truncated");
        }

        assert_ne!(
            header_lengths[0], header_lengths[1],
            "the longer JSON metadata must move the first sync marker"
        );
    }
}
