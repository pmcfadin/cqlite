//! Per-record framing + sync-marker walking for a Cassandra CommitLog segment
//! (issue #2389).
//!
//! After the [descriptor header](super::descriptor), a segment body is a chain
//! of **sync-marker-delimited sections**. Each section begins with an 8-byte
//! sync marker and holds a run of length-and-CRC-framed mutation records.
//!
//! All layouts below are verified byte-for-byte against a real Cassandra 5.0.2
//! segment (the fixtures under `test-data/datasets/commitlog/`).
//!
//! ```text
//! sync marker (8 bytes, big-endian):
//!   int32 nextMarker   file offset of the *next* sync marker (= end of section)
//!   int32 crc          CRC32 over: id-low(BE i32), id-high(BE i32), markerPos(BE i32)
//!
//! record (per mutation):
//!   int32 size         serialized mutation length
//!   int32 sizeCrc      CRC32 over the 4 size bytes (BE)
//!   bytes body         `size` mutation bytes
//!   int32 bodyCrc      CRC32 continuing over size bytes ++ body bytes
//! ```
//!
//! **Truncation vs corruption.** A record or marker whose bytes are *missing*
//! (extend past end-of-file) is a torn tail — tolerated: prior records are kept
//! and the walk stops with [`FrameStep::Truncated`]. A record that is *fully
//! present* but whose CRC does not match is genuine corruption — surfaced as
//! [`Error::CorruptCommitLogFrame`]. This mirrors Cassandra's
//! `CommitLogReader` `tolerateTruncation` behavior.

use crate::{Error, Result};

/// Size of a sync marker in bytes (`int32 nextMarker` + `int32 crc`).
pub const SYNC_MARKER_SIZE: usize = 8;

/// Per-record overhead: `int32 size` + `int32 sizeCrc` + `int32 bodyCrc`.
pub const RECORD_OVERHEAD: usize = 12;

/// One step of a frame walk.
#[derive(Debug)]
pub enum FrameStep<'a> {
    /// A validated mutation record body (the serialized `Mutation` bytes).
    Record(&'a [u8]),
    /// Clean end of the written segment (padding / zeroed pre-allocation).
    End,
    /// Torn tail: a record or marker ran past end-of-file. Records already
    /// yielded before this step are valid and complete.
    Truncated,
}

/// Lazy cursor over a segment's sync sections and records.
///
/// Holds only a borrow of the segment bytes plus a couple of cursor integers —
/// it never materializes the decoded record set, satisfying the streaming
/// memory budget (design decision D4 / `oom-audit`).
pub struct FrameWalker<'a> {
    bytes: &'a [u8],
    segment_id: i64,
    /// Position of the sync marker that opens the current section.
    marker_pos: usize,
    /// Cursor within the current section (position of the next record).
    cursor: usize,
    /// End offset of the current section (the next marker's position).
    section_end: usize,
    /// Whether we are positioned inside an open section.
    in_section: bool,
    /// Set when the current section's body is torn (marker points past EOF).
    torn_section: bool,
    /// Set when the walk ended on a torn tail rather than clean padding.
    truncated_end: bool,
    /// Terminal flag once [`FrameStep::End`]/[`FrameStep::Truncated`] is reached.
    done: bool,
}

impl<'a> FrameWalker<'a> {
    /// Start a walk at the segment body (immediately after the descriptor).
    ///
    /// `body_start` is [`super::descriptor::CommitLogDescriptor::header_len`];
    /// `segment_id` is the descriptor's authoritative segment id (used to verify
    /// every sync marker's CRC).
    pub fn new(bytes: &'a [u8], segment_id: i64, body_start: usize) -> Self {
        Self {
            bytes,
            segment_id,
            marker_pos: body_start,
            cursor: 0,
            section_end: 0,
            in_section: false,
            torn_section: false,
            truncated_end: false,
            done: false,
        }
    }

    /// Advance to the next record, end, or torn tail.
    ///
    /// # Errors
    /// [`Error::CorruptCommitLogFrame`] when a fully-present record fails its
    /// size/body CRC, or a marker is internally inconsistent.
    pub fn next_frame(&mut self) -> Result<FrameStep<'a>> {
        if self.done {
            return Ok(FrameStep::End);
        }
        loop {
            if !self.in_section {
                match self.open_section()? {
                    Some(()) => {}
                    None => {
                        self.done = true;
                        return Ok(if self.truncated_end {
                            FrameStep::Truncated
                        } else {
                            FrameStep::End
                        });
                    }
                }
            }

            // Within the open section, read the next record.
            match self.read_record()? {
                RecordOutcome::Body(b) => return Ok(FrameStep::Record(b)),
                RecordOutcome::SectionDone => {
                    // Advance to the next marker.
                    self.in_section = false;
                    self.marker_pos = self.section_end;
                    continue;
                }
                RecordOutcome::CleanEnd => {
                    self.done = true;
                    return Ok(FrameStep::End);
                }
                RecordOutcome::Truncated => {
                    self.done = true;
                    return Ok(FrameStep::Truncated);
                }
            }
        }
    }

    /// Open the section at `marker_pos`. Returns `Ok(None)` on clean end or torn
    /// tail (with `truncated_end` set appropriately).
    fn open_section(&mut self) -> Result<Option<()>> {
        let pos = self.marker_pos;
        // A marker whose bytes are missing → torn tail.
        if pos + SYNC_MARKER_SIZE > self.bytes.len() {
            // Distinguish a clean end (nothing but absence) from a torn write:
            // any leftover non-marker bytes present but < 8 is a torn tail.
            self.truncated_end = pos < self.bytes.len();
            return Ok(None);
        }
        let next_marker = read_i32_be(self.bytes, pos);
        // Zeroed pre-allocation → clean end of the written region.
        if next_marker == 0 {
            self.truncated_end = false;
            return Ok(None);
        }
        // Validate the marker CRC (id-low, id-high, markerPos).
        let stored_crc = read_u32_be(self.bytes, pos + 4);
        let computed = marker_crc(self.segment_id, pos);
        if computed != stored_crc {
            // An invalid marker over an all-zero region is the normal clean end;
            // otherwise it is a torn/garbage marker (tolerate as truncation).
            let all_zero = self.bytes[pos..pos + SYNC_MARKER_SIZE]
                .iter()
                .all(|&b| b == 0);
            self.truncated_end = !all_zero;
            return Ok(None);
        }
        let next = next_marker as usize;
        // Marker is valid but points past EOF → the section body is torn.
        if next > self.bytes.len() {
            self.section_end = self.bytes.len();
            self.cursor = pos + SYNC_MARKER_SIZE;
            self.in_section = true;
            self.torn_section = true;
            return Ok(Some(()));
        }
        // A marker must point strictly forward past its own 8 bytes.
        if next < pos + SYNC_MARKER_SIZE {
            return Err(Error::CorruptCommitLogFrame(format!(
                "sync marker at offset {pos} points backward to {next}"
            )));
        }
        self.section_end = next;
        self.cursor = pos + SYNC_MARKER_SIZE;
        self.in_section = true;
        self.torn_section = false;
        Ok(Some(()))
    }

    /// Read one record from within the open section.
    fn read_record(&mut self) -> Result<RecordOutcome<'a>> {
        let cur = self.cursor;
        let len = self.bytes.len();

        // No room for a record header within the section → section finished.
        if cur >= self.section_end {
            return Ok(RecordOutcome::SectionDone);
        }
        if cur + 4 > len {
            return Ok(RecordOutcome::Truncated);
        }
        let size = read_i32_be(self.bytes, cur);
        // Zeroed padding within the (last) section → clean end of segment.
        if size <= 0 {
            return Ok(RecordOutcome::CleanEnd);
        }
        let size = size as usize;
        let end = cur + 4 + 4 + size + 4;
        // Record extends past EOF → torn tail.
        if end > len {
            return Ok(RecordOutcome::Truncated);
        }
        // Record extends past the section boundary but is within the file: the
        // marker/section framing disagrees with the record length — corrupt.
        if end > self.section_end && !self.torn_section {
            return Err(Error::CorruptCommitLogFrame(format!(
                "record at offset {cur} (len {size}) overruns section end {}",
                self.section_end
            )));
        }

        let size_crc = read_u32_be(self.bytes, cur + 4);
        let size_bytes = &self.bytes[cur..cur + 4];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(size_bytes);
        let computed_size_crc = hasher.finalize();
        if computed_size_crc != size_crc {
            return Err(Error::CorruptCommitLogFrame(format!(
                "record size CRC mismatch at offset {cur}: stored={size_crc:#010x} \
                 computed={computed_size_crc:#010x}"
            )));
        }

        let body_start = cur + 8;
        let body = &self.bytes[body_start..body_start + size];
        let body_crc = read_u32_be(self.bytes, body_start + size);
        // Continuous CRC over the size bytes followed by the body bytes.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(size_bytes);
        hasher.update(body);
        let computed_body_crc = hasher.finalize();
        if computed_body_crc != body_crc {
            return Err(Error::CorruptCommitLogFrame(format!(
                "record body CRC mismatch at offset {cur}: stored={body_crc:#010x} \
                 computed={computed_body_crc:#010x}"
            )));
        }

        self.cursor = end;
        Ok(RecordOutcome::Body(body))
    }
}

enum RecordOutcome<'a> {
    Body(&'a [u8]),
    SectionDone,
    CleanEnd,
    Truncated,
}

/// Compute a sync-marker CRC the way Cassandra's `writeSyncMarker` does:
/// CRC32 over id-low (BE i32), id-high (BE i32), markerPos (BE i32).
pub fn marker_crc(segment_id: i64, marker_pos: usize) -> u32 {
    let id = segment_id as u64;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&((id & 0xFFFF_FFFF) as u32).to_be_bytes());
    hasher.update(&((id >> 32) as u32).to_be_bytes());
    hasher.update(&(marker_pos as u32).to_be_bytes());
    hasher.finalize()
}

#[inline]
fn read_i32_be(b: &[u8], off: usize) -> i32 {
    i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
#[inline]
fn read_u32_be(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
