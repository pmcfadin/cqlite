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
//! **Truncation vs corruption.** Missing record or marker bytes, a nonzero
//! marker with a bad CRC, and a valid forward marker beyond end-of-file are
//! treated as a torn tail: prior records are kept and the walk stops with
//! [`FrameStep::Truncated`]. A fully present record with a bad CRC, or a
//! valid-CRC marker with a negative or backward next-marker offset, is
//! corruption and returns [`Error::CorruptCommitLogFrame`]. Zeroed preallocated
//! space is a clean end. This mirrors Cassandra's `CommitLogReader`
//! `tolerateTruncation` behavior without accepting structurally invalid
//! backward pointers.

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
            truncated_end: false,
            done: false,
        }
    }

    /// Advance to the next record, end, or torn tail.
    ///
    /// # Errors
    /// [`Error::CorruptCommitLogFrame`] when a fully-present record fails its
    /// size/body CRC, its framing overruns a section, or a valid-CRC sync marker
    /// has a negative or backward next-marker offset. Invalid marker CRCs and
    /// forward markers past EOF are reported as [`FrameStep::Truncated`].
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
                    // A torn section (opened past-EOF marker) sets truncated_end
                    // at open time; honor it here too, not just in open_section's
                    // own None path — otherwise hitting padding inside a torn
                    // section's remaining bytes silently downgrades Truncated to
                    // a clean End (roborev finding, review-first pass).
                    return Ok(if self.truncated_end {
                        FrameStep::Truncated
                    } else {
                        FrameStep::End
                    });
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
            // `|=`, not `=`: a prior torn section may have already set this
            // true, and this branch reaching `pos < len` false (e.g. a
            // section whose records happen to fill the buffer exactly) must
            // not silently downgrade it back to a clean end — the exact bug
            // this fix's neighboring CleanEnd-arm change was meant to close,
            // just at a second assignment site (roborev finding, review-
            // first pass).
            self.truncated_end |= pos < self.bytes.len();
            return Ok(None);
        }
        let marker = &self.bytes[pos..pos + SYNC_MARKER_SIZE];
        // Only a fully zeroed marker is clean preallocated space. Do not clear
        // truncated_end: once the walk has observed a torn tail, later padding
        // cannot turn it back into a clean segment.
        if marker.iter().all(|&byte| byte == 0) {
            return Ok(None);
        }
        let next_marker = read_i32_be(self.bytes, pos);
        // A zero next-marker value with nonzero CRC bytes is not the zeroed
        // preallocation sentinel; it must still pass CRC and pointer checks.
        // This preserves the distinction between a torn marker and a valid-CRC
        // backward pointer.
        let stored_crc = read_u32_be(self.bytes, pos + 4);
        let computed = marker_crc(self.segment_id, pos);
        if computed != stored_crc {
            // A complete, nonzero marker with a bad CRC is tolerated as a torn
            // tail. It cannot make a previously observed truncation disappear.
            self.truncated_end = true;
            return Ok(None);
        }
        // The marker offset is signed on disk. Reject negative values before
        // any conversion so they cannot wrap into a huge positive `usize` and
        // masquerade as a torn forward section.
        if next_marker < 0 {
            return Err(Error::CorruptCommitLogFrame(format!(
                "sync marker at offset {pos} has negative next-marker offset {next_marker}"
            )));
        }
        // On targets where usize is narrower than i32, an unrepresentable
        // positive offset is necessarily beyond this in-memory file and has
        // the same torn-tail meaning as any other forward offset past EOF.
        let next = match usize::try_from(next_marker) {
            Ok(next) => next,
            Err(_) => {
                self.open_torn_section(pos);
                return Ok(Some(()));
            }
        };
        // Marker is valid but points past EOF → the section body is torn. This
        // section necessarily runs to true end-of-file (nothing can follow a
        // marker that already overruns the buffer), so the walk is guaranteed
        // to terminate truncated — set that now, not just on the eventual
        // "no more marker bytes" path, so a CleanEnd/SectionDone reached while
        // still inside this torn section doesn't get reported as a clean end
        // (roborev finding, review-first pass).
        if next > self.bytes.len() {
            self.open_torn_section(pos);
            return Ok(Some(()));
        }
        // A marker must point forward past its own 8 bytes. The CRC covers
        // id/position, not next_marker; it cannot establish how a bad offset
        // arose. Cassandra 5.0.8 CommitLogSegmentReader.readSyncMarker still
        // classifies a backward offset with a matching CRC as corruption,
        // including zero at a real (positive) marker position. Preserve that
        // policy instead of guessing that the offset resulted from a torn write.
        if next < pos + SYNC_MARKER_SIZE {
            return Err(Error::CorruptCommitLogFrame(format!(
                "sync marker at offset {pos} points backward to {next}"
            )));
        }
        self.section_end = next;
        self.cursor = pos + SYNC_MARKER_SIZE;
        self.in_section = true;
        Ok(Some(()))
    }

    /// Continue reading the final section to EOF after a valid forward marker
    /// points beyond the available bytes.
    fn open_torn_section(&mut self, marker_pos: usize) {
        self.section_end = self.bytes.len();
        self.cursor = marker_pos + SYNC_MARKER_SIZE;
        self.in_section = true;
        self.truncated_end = true;
    }

    /// Read one record from within the open section.
    fn read_record(&mut self) -> Result<RecordOutcome<'a>> {
        let cur = self.cursor;
        let len = self.bytes.len();

        // No room for a record header within the section → section finished.
        if cur >= self.section_end {
            return Ok(RecordOutcome::SectionDone);
        }
        // `is_final_section` distinguishes "this section runs to true EOF" from
        // "more sections follow at self.section_end". Cassandra pads the tail of
        // EVERY non-final section up to its next sync marker — a zeroed/short
        // record header there means "done with THIS section, continue at the
        // next marker" (SectionDone), not "the whole segment ends here"
        // (CleanEnd/Truncated). Treating it as segment-end silently drops every
        // mutation in all subsequent sections (roborev finding, review-first
        // pass).
        let is_final_section = self.section_end >= len;
        if cur + 4 > self.section_end {
            return Ok(if is_final_section {
                RecordOutcome::Truncated
            } else {
                RecordOutcome::SectionDone
            });
        }
        let size = read_i32_be(self.bytes, cur);
        // Zeroed padding: true clean end of the written segment only when this
        // is the final section; otherwise it's just this section's padding.
        if size <= 0 {
            return Ok(if is_final_section {
                RecordOutcome::CleanEnd
            } else {
                RecordOutcome::SectionDone
            });
        }
        let size = size as usize;
        let end = cur + 4 + 4 + size + 4;
        // Record extends past EOF → torn tail.
        if end > len {
            return Ok(RecordOutcome::Truncated);
        }
        // Record extends past the section boundary but is within the file: the
        // marker/section framing disagrees with the record length — corrupt.
        // No `&& !self.torn_section` guard here: in a torn section
        // section_end == bytes.len() == len, so `end > self.section_end`
        // would imply `end > len`, which the EOF check above already caught
        // and returned Truncated for — this branch is unreachable with
        // torn_section true regardless, so an explicit exemption clause only
        // obscured that invariant and invited a future edit to make it live
        // and wrong (roborev finding, review-first pass).
        if end > self.section_end {
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

#[cfg(test)]
mod tests {
    use super::*;

    const SEGMENT_ID: i64 = 1234;

    fn push_marker(buf: &mut Vec<u8>, pos: usize, next_marker: i32) {
        buf.extend_from_slice(&next_marker.to_be_bytes());
        buf.extend_from_slice(&marker_crc(SEGMENT_ID, pos).to_be_bytes());
    }

    fn push_record(buf: &mut Vec<u8>, body: &[u8]) {
        let size = body.len() as i32;
        let size_bytes = size.to_be_bytes();
        let mut h = crc32fast::Hasher::new();
        h.update(&size_bytes);
        let size_crc = h.finalize();
        buf.extend_from_slice(&size_bytes);
        buf.extend_from_slice(&size_crc.to_be_bytes());
        buf.extend_from_slice(body);
        let mut h = crc32fast::Hasher::new();
        h.update(&size_bytes);
        h.update(body);
        buf.extend_from_slice(&h.finalize().to_be_bytes());
    }

    /// Regression for the roborev finding that the `SectionDone` fix (a
    /// non-final section's padding must finish THAT section, not the whole
    /// segment) had zero test coverage — every committed fixture is
    /// single-section. Builds a real two-section segment byte-for-byte per
    /// this module's own documented layout: section 1 (one record + zero
    /// padding to the next marker), section 2 (one record, then a clean
    /// natural end at true EOF).
    #[test]
    fn walks_records_across_multiple_sync_sections() {
        let mut buf = Vec::new();
        // Section 1: marker at 0, one record, 8 bytes of zero padding.
        let record1 = b"section-one-record";
        push_marker(&mut buf, 0, 0 /* placeholder, patched below */);
        push_record(&mut buf, record1);
        buf.extend_from_slice(&[0u8; 8]); // padding to the next marker
        let section2_marker_pos = buf.len();
        // Patch section 1's marker to point at section 2's marker (its own
        // section_end), now that we know the offset.
        let next1 = section2_marker_pos as i32;
        buf[0..4].copy_from_slice(&next1.to_be_bytes());
        buf[4..8].copy_from_slice(&marker_crc(SEGMENT_ID, 0).to_be_bytes());

        // Section 2: marker, one record, then true EOF (no trailing padding
        // needed — section_end == bytes.len() makes this the final section).
        let record2 = b"section-two-record";
        let record2_start = section2_marker_pos + SYNC_MARKER_SIZE;
        let final_len = record2_start + 4 + 4 + record2.len() + 4;
        push_marker(&mut buf, section2_marker_pos, final_len as i32);
        push_record(&mut buf, record2);
        assert_eq!(buf.len(), final_len, "test construction sanity check");

        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        let mut records = Vec::new();
        loop {
            match walker.next_frame().expect("no corruption in this fixture") {
                FrameStep::Record(b) => records.push(b.to_vec()),
                FrameStep::End => break,
                FrameStep::Truncated => panic!("expected a clean end, not Truncated"),
            }
        }
        assert_eq!(
            records,
            vec![record1.to_vec(), record2.to_vec()],
            "both sections' records must be yielded — a regression that treats \
             section 1's padding as segment-end would silently drop record2"
        );
    }

    /// Regression: `truncated_end` was assigned (`=`) rather than OR-ed
    /// (`|=`) at two sites in `open_section`, so it could be silently
    /// downgraded from true back to false after a torn section had already
    /// set it. Trips exactly when a torn section's one record happens to end
    /// precisely at EOF: the section finishes via `SectionDone` (not
    /// `CleanEnd`), re-enters `open_section` at `marker_pos == bytes.len()`,
    /// and that branch's `pos < len` is false — with a plain `=` this wiped
    /// truncated_end back to false and reported a clean End for a genuinely
    /// torn segment (roborev finding, review-first pass).
    #[test]
    fn torn_section_whose_record_ends_exactly_at_eof_still_reports_truncated() {
        // Marker's own CRC covers only (segment_id, marker_pos) — NOT the
        // next_marker value — so next_marker can point arbitrarily far past
        // EOF while the marker itself still validates, exactly like a real
        // segment whose section was truncated mid-write.
        let mut buf = Vec::new();
        push_marker(&mut buf, 0, 999_999); // far past EOF -> torn section
        push_record(&mut buf, b"last-record-fills-the-buffer-exactly");

        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        match walker.next_frame().expect("valid record") {
            FrameStep::Record(_) => {}
            other => panic!("expected the one record, got {other:?}"),
        }
        match walker.next_frame().expect("no corruption") {
            FrameStep::Truncated => {}
            other => panic!(
                "expected Truncated (torn section, record ends exactly at EOF), got {other:?}"
            ),
        }
    }

    #[test]
    fn negative_sync_offset_is_corruption() {
        let mut buf = Vec::new();
        push_marker(&mut buf, 0, -1);

        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        assert!(matches!(
            walker.next_frame(),
            Err(Error::CorruptCommitLogFrame(_))
        ));
    }

    #[test]
    fn valid_backward_sync_offset_remains_corruption() {
        let mut buf = Vec::new();
        // This is nonnegative but still points inside its own marker.
        push_marker(&mut buf, 0, 4);

        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        assert!(matches!(
            walker.next_frame(),
            Err(Error::CorruptCommitLogFrame(_))
        ));
    }

    #[test]
    fn forward_sync_offset_past_eof_is_a_torn_tail() {
        let mut buf = Vec::new();
        push_marker(&mut buf, 0, 999_999);

        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        assert!(matches!(walker.next_frame(), Ok(FrameStep::Truncated)));
    }

    #[test]
    fn invalid_nonzero_sync_marker_crc_is_a_torn_tail() {
        let mut buf = Vec::new();
        push_marker(&mut buf, 0, 16);
        buf[4] ^= 0x01;

        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        assert!(matches!(walker.next_frame(), Ok(FrameStep::Truncated)));
    }

    #[test]
    fn fully_zeroed_sync_marker_is_clean_end() {
        let buf = [0u8; SYNC_MARKER_SIZE];
        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        assert!(matches!(walker.next_frame(), Ok(FrameStep::End)));
    }

    #[test]
    fn zero_next_marker_with_bad_crc_is_a_torn_tail() {
        let mut buf = [0u8; SYNC_MARKER_SIZE];
        buf[4..].copy_from_slice(&1u32.to_be_bytes());
        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        assert!(matches!(walker.next_frame(), Ok(FrameStep::Truncated)));
    }

    #[test]
    fn zero_next_marker_with_valid_crc_is_backward_corruption() {
        let marker_pos = SYNC_MARKER_SIZE;
        let mut buf = vec![0u8; marker_pos + SYNC_MARKER_SIZE];
        let crc = marker_crc(SEGMENT_ID, marker_pos);
        assert_ne!(
            crc, 0,
            "fixture CRC must distinguish the marker from zeroed space"
        );
        buf[marker_pos + 4..].copy_from_slice(&crc.to_be_bytes());

        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, marker_pos);
        assert!(matches!(
            walker.next_frame(),
            Err(Error::CorruptCommitLogFrame(_))
        ));
    }

    #[test]
    fn zeroed_sync_marker_does_not_clear_prior_truncation() {
        let buf = [0u8; SYNC_MARKER_SIZE];
        let mut walker = FrameWalker::new(&buf, SEGMENT_ID, 0);
        walker.truncated_end = true;
        assert!(matches!(walker.next_frame(), Ok(FrameStep::Truncated)));
    }
}
