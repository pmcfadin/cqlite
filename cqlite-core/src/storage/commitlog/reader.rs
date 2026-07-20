//! `CommitLogReader` — streaming decode of a Cassandra 5.0 CommitLog segment
//! into mutations (issue #2389).
//!
//! Opens a segment file, parses its [descriptor](super::descriptor)
//! authoritatively (version-gated, CRC-checked), fails closed on a
//! compressed/encrypted payload (design decision D3), and exposes a **lazy**
//! iterator over decoded mutations — one sync-section/record at a time, never
//! materializing the whole mutation set up front (design decision D4 / the
//! `oom-audit` structural expectation).

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
    ///   from descriptor parsing.
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
