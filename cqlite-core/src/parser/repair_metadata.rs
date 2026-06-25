//! Repair-state metadata decode from the `Statistics.db` STATS component
//! (issue #988, epic #968).
//!
//! Apache Cassandra persists three repair-coordination fields inside the STATS
//! `MetadataType` component of `Statistics.db`:
//!
//!   * `repairedAt` (`long`) — the repair timestamp, `0` for an unrepaired
//!     SSTable. Rendered by `sstablemetadata` as `Repaired at: <n>`.
//!   * `pendingRepair` (`UUID`, nullable) — the in-flight (incremental) repair
//!     session id, `null` for an SSTable not part of a pending repair. Rendered
//!     as `Pending repair: --` when null.
//!   * `isTransient` (`boolean`) — whether the SSTable holds only transiently
//!     replicated data. Rendered as `IsTransient: false`.
//!
//! # Scope — persisted-metadata parse/report ONLY
//!
//! This module decodes and **reports** the *persisted* repair state. It does
//! **not** implement, and must not be read as implying, repair coordination,
//! incremental-repair session tracking, or **repair-aware compaction /
//! tombstone purging**. Exposing `repairedAt` here establishes nothing about
//! whether tombstones are safe to purge against a repair boundary — that is a
//! separate correctness concern that lives in the compaction layer.
//!
//! # What is decoded from real bytes
//!
//! `repairedAt` is decoded directly from the STATS component. It is reachable by
//! a fully *self-describing* forward walk over the leading STATS fields (two
//! `EstimatedHistogram`s and one `TombstoneHistogram`, every one length-prefixed
//! and free of any column-type dependency), so it is decoded for **every**
//! storage format (nb / oa / da) without needing the serialization header.
//!
//! `pendingRepair` and `isTransient` sit *after* the version-gated
//! `improvedMinMax` block (oa/da) and the variable `commitLogIntervals` set,
//! both of which require type-aware / interval-aware skipping to traverse. The
//! Cassandra 5.0 corpus contains **no** repaired / pending-repair / transient
//! fixture (every fixture is the unrepaired/null/non-transient state), and this
//! module does **not** perform that type-aware walk. It therefore reports those
//! two fields **honestly as `Unparsed`** (an explicit "not yet decoded" state)
//! rather than fabricating a concrete `null` / `false`, so a real SSTable that
//! *did* carry a pending-repair UUID or transient flag is never silently
//! misreported as absent. The strict test lane proves the *reference* state is
//! null / false across the corpus while asserting CQLite reports those fields as
//! `Unparsed`, rather than fabricating repaired bytes.

use crate::error::{Error, Result};
use crate::storage::sstable::version_gate::VersionGates;

/// Cassandra `MetadataType.STATS` ordinal (MetadataType.java).
const METADATA_TYPE_STATS: u32 = 2;

/// A STATS repair field that may either have been decoded from real bytes
/// (`Decoded`) or is honestly reported as not-yet-decoded (`Unparsed`).
///
/// This exists so the public API never fabricates a concrete value for a field
/// this module does not actually walk from the STATS bytes. A real SSTable
/// carrying a pending-repair UUID or a transient flag is reported as `Unparsed`
/// — never silently misreported as the absent/false default — until a
/// type-aware forward walk past `improvedMinMax` + `commitLogIntervals` is
/// implemented (which requires a fixture that does not exist in the corpus; see
/// module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairField<T> {
    /// The field was decoded directly from the STATS-component bytes.
    Decoded(T),
    /// The field was not decoded; its real value is unknown. Callers must NOT
    /// treat this as the absent/default state.
    Unparsed,
}

impl<T> RepairField<T> {
    /// The decoded value, or `None` when the field is `Unparsed`. Callers that
    /// need to distinguish "decoded as absent" from "not decoded" must match on
    /// the variant directly rather than using this.
    pub fn decoded(&self) -> Option<&T> {
        match self {
            RepairField::Decoded(v) => Some(v),
            RepairField::Unparsed => None,
        }
    }

    /// Whether this field was decoded from real bytes.
    pub fn is_decoded(&self) -> bool {
        matches!(self, RepairField::Decoded(_))
    }
}

/// Repair-coordination metadata persisted in the `Statistics.db` STATS
/// component. Read-side / report-only (see module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairMetadata {
    /// `repairedAt` timestamp; `0` for an unrepaired SSTable. Decoded directly
    /// from the STATS-component bytes (genuinely walked, so concrete).
    pub repaired_at: i64,

    /// `pendingRepair` session UUID (`Decoded(Some(uuid))`), an explicitly
    /// decoded null (`Decoded(None)`), or `Unparsed` when this module did not
    /// decode it from the STATS bytes.
    ///
    /// This module does NOT currently walk this field (it sits after the
    /// version-gated `improvedMinMax` block and the variable
    /// `commitLogIntervals` set), so it is reported as `Unparsed` rather than a
    /// fabricated `None`. See module docs.
    pub pending_repair: RepairField<Option<[u8; 16]>>,

    /// `isTransient` flag (`Decoded(bool)`) or `Unparsed` when not decoded.
    ///
    /// As with `pending_repair`, this module does not walk this field, so it is
    /// reported honestly as `Unparsed` rather than a fabricated `false`.
    pub is_transient: RepairField<bool>,

    /// `true` when `repaired_at` was decoded from the STATS component bytes;
    /// `false` when it could only be reported as the unrepaired default (e.g.
    /// the STATS component was not locatable). Lets callers distinguish a
    /// byte-proven value from a defaulted one.
    pub repaired_at_decoded: bool,
}

impl RepairMetadata {
    /// The canonical unrepaired state for an SSTable whose STATS component could
    /// not be located: `repairedAt` defaults to `0` (undecoded), and the two
    /// not-walked fields are reported honestly as `Unparsed`.
    pub fn unrepaired_default() -> Self {
        RepairMetadata {
            repaired_at: 0,
            pending_repair: RepairField::Unparsed,
            is_transient: RepairField::Unparsed,
            repaired_at_decoded: false,
        }
    }
}

/// Number of CRC bytes Cassandra writes after EACH metadata component. The
/// `Statistics.db` MetadataSerializer emits a 4-byte CRC32 (over the component's
/// own bytes) immediately after every component body — including the last.
/// Verified empirically against real nb/oa/da fixtures: for a component at TOC
/// offset `o` whose successor is at offset `next`, the bytes `[next-4, next)`
/// are `crc32(buf[o..next-4])`; for the final component the CRC occupies the
/// last 4 bytes of the file. The STATS component must never be decoded into this
/// CRC, so both the next-component bound and the last-component bound subtract it.
const METADATA_COMPONENT_CRC_LEN: usize = 4;

/// The located STATS component, as a `[start, end)` byte range within the
/// `Statistics.db` buffer. `end` is the *next* component's offset minus the
/// 4-byte per-component CRC that sits between STATS and that component (the
/// smallest component offset strictly greater than `start`), or — when STATS is
/// the last component — the end of the file minus the trailing 4-byte CRC. The
/// decode cursor is constructed over ONLY this slice so that a truncated STATS
/// body or an over-long internal length field fails closed instead of spilling
/// into the CRC or the following component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StatsComponentBounds {
    start: usize,
    end: usize,
}

/// Locate the byte range of the STATS (`MetadataType` ordinal 2) component from
/// the `Statistics.db` Table of Contents.
///
/// The TOC carries `(type, offset)` pairs for every metadata component. The
/// STATS component's end is derived authoritatively from those same offsets —
/// the next component begins where STATS must end — rather than from the rest of
/// the file. When STATS is the final component, its end is the start of the
/// trailing CRC. The returned range is validated `start < end <= file_len`.
///
/// Returns:
///   * `Ok(None)` ONLY when the TOC is well-formed but carries NO STATS
///     component (nothing to decode → the caller reports the unrepaired
///     default).
///   * `Ok(Some(bounds))` for a valid STATS range.
///   * `Err(Corruption)` for a malformed/truncated TOC (too short for the
///     header, component count out of range, or a TOC that overruns the
///     buffer), OR when a STATS entry IS present but its derived range is
///     invalid (offset past EOF, inverted, or zero-length) — fail closed so a
///     corrupt `Statistics.db` is never silently reported as unrepaired.
fn stats_component_bounds(input: &[u8]) -> Result<Option<StatsComponentBounds>> {
    // A malformed or truncated TOC must FAIL CLOSED rather than be silently
    // reported as "no STATS component" (which would misreport corrupt repair
    // metadata as the unrepaired default). `Ok(None)` is reserved exclusively
    // for a well-formed TOC that genuinely carries no STATS entry (below).
    if input.len() < 8 {
        return Err(Error::Corruption(format!(
            "Statistics.db too short for a metadata TOC header: {} bytes",
            input.len()
        )));
    }
    let num_components = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    if num_components == 0 || num_components > 100 {
        return Err(Error::Corruption(format!(
            "Statistics.db TOC component count out of range: {num_components}"
        )));
    }
    let toc_start = 8usize;
    let entry_size = 8usize;
    let toc_size = match (num_components as usize)
        .checked_mul(entry_size)
        .and_then(|n| n.checked_add(toc_start))
    {
        Some(n) => n,
        None => {
            return Err(Error::Corruption(format!(
                "Statistics.db TOC size overflow for {num_components} components"
            )))
        }
    };
    if input.len() < toc_size {
        return Err(Error::Corruption(format!(
            "Statistics.db TOC truncated: need {toc_size} bytes, have {}",
            input.len()
        )));
    }

    let mut stats_off: Option<usize> = None;
    let mut offsets: Vec<usize> = Vec::with_capacity(num_components as usize);
    for i in 0..num_components as usize {
        let entry = match i
            .checked_mul(entry_size)
            .and_then(|n| n.checked_add(toc_start))
        {
            Some(e) => e,
            None => {
                return Err(Error::Corruption(format!(
                    "Statistics.db TOC entry offset overflow at index {i}"
                )))
            }
        };
        let ty = u32::from_be_bytes([
            input[entry],
            input[entry + 1],
            input[entry + 2],
            input[entry + 3],
        ]);
        let off = u32::from_be_bytes([
            input[entry + 4],
            input[entry + 5],
            input[entry + 6],
            input[entry + 7],
        ]) as usize;
        offsets.push(off);
        if ty == METADATA_TYPE_STATS {
            stats_off = Some(off);
        }
    }

    // No STATS component in the TOC → nothing to decode (not an error).
    let Some(start) = stats_off else {
        return Ok(None);
    };

    // The STATS body ends 4 bytes before the *next* component begins: Cassandra
    // writes a per-component CRC32 between each component body and the next
    // component's offset (verified against real fixtures). Use the smallest
    // component offset strictly greater than `start`, minus that CRC; when STATS
    // is the last component, bound by the file end minus the trailing CRC. Both
    // cases exclude the 4-byte CRC so it is never decoded as metadata.
    let next_boundary = offsets
        .iter()
        .copied()
        .filter(|&o| o > start)
        .min()
        .unwrap_or(input.len());
    let end = next_boundary.saturating_sub(METADATA_COMPONENT_CRC_LEN);

    // A STATS entry exists but its derived range is invalid (offset past EOF,
    // inverted, or empty): this is genuine corruption — fail closed rather than
    // silently reporting the unrepaired default or building a cursor over
    // garbage.
    if start >= end || end > input.len() {
        return Err(Error::Corruption(format!(
            "STATS component range invalid: start {start}, derived end {end}, \
             Statistics.db {} bytes",
            input.len()
        )));
    }

    Ok(Some(StatsComponentBounds { start, end }))
}

/// A bounded cursor over the STATS component bytes that fails closed (explicit
/// error) on any read past the end rather than panicking.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, pos: 0 }
    }

    fn need(&self, n: usize) -> Result<usize> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| Error::Corruption("STATS cursor overflow".to_string()))?;
        if end > self.bytes.len() {
            return Err(Error::Corruption(format!(
                "STATS component truncated: need {n} bytes at offset {} but only {} remain",
                self.pos,
                self.bytes.len().saturating_sub(self.pos),
            )));
        }
        Ok(end)
    }

    fn skip(&mut self, n: usize) -> Result<()> {
        let end = self.need(n)?;
        self.pos = end;
        Ok(())
    }

    fn read_i32(&mut self) -> Result<i32> {
        let end = self.need(4)?;
        let v = i32::from_be_bytes([
            self.bytes[self.pos],
            self.bytes[self.pos + 1],
            self.bytes[self.pos + 2],
            self.bytes[self.pos + 3],
        ]);
        self.pos = end;
        Ok(v)
    }

    fn read_i64(&mut self) -> Result<i64> {
        let end = self.need(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(i64::from_be_bytes(buf))
    }
}

/// Skip an `EstimatedHistogram`: `i32 count`, then `count` × (`i64 offset`,
/// `i64 count`). Self-describing — no column-type knowledge required.
fn skip_estimated_histogram(c: &mut Cursor) -> Result<()> {
    let count = c.read_i32()?;
    if count < 0 {
        return Err(Error::Corruption(format!(
            "negative EstimatedHistogram bucket count {count}"
        )));
    }
    let bytes = (count as usize)
        .checked_mul(16)
        .ok_or_else(|| Error::Corruption("EstimatedHistogram size overflow".to_string()))?;
    c.skip(bytes)
}

/// Skip a `TombstoneHistogram`: `i32 maxBinSize`, `i32 size`, then `size` bins.
///
/// `modern` selects the entry width: the modern (`oa`/`da`)
/// `HistogramSerializer` writes `i64 point + i32 value` (12 bytes); the legacy
/// (`nb`/`mc`) serializer writes `f64 point + i64 value` (16 bytes). The choice
/// follows `TombstoneHistogram.getSerializer(version)` — the only format-gated
/// decision needed to reach `repairedAt`.
fn skip_tombstone_histogram(c: &mut Cursor, modern: bool) -> Result<()> {
    let _max_bin_size = c.read_i32()?;
    let size = c.read_i32()?;
    if size < 0 {
        return Err(Error::Corruption(format!(
            "negative TombstoneHistogram size {size}"
        )));
    }
    let entry_width = if modern { 12 } else { 16 };
    let bytes = (size as usize)
        .checked_mul(entry_width)
        .ok_or_else(|| Error::Corruption("TombstoneHistogram size overflow".to_string()))?;
    c.skip(bytes)
}

/// Whether this version uses the modern tombstone-histogram entry encoding.
///
/// `TombstoneHistogram.getSerializer` resolves to the modern `HistogramSerializer`
/// for `oa`+ (incl. BTI `da`) and the legacy serializer for older versions.
fn uses_modern_tombstone_histogram(gates: &VersionGates) -> bool {
    match gates {
        // The oa-only `hasUIntDeletionTime` gate cleanly separates the modern
        // (oa/da) histogram encoding from the legacy (nb) one.
        VersionGates::Big(g) => g.has_uint_deletion_time,
        VersionGates::Bti(_) => true,
    }
}

/// Decode the repair-state metadata from a raw `Statistics.db` buffer.
///
/// `repairedAt` is decoded from the STATS component for every format. When
/// `gates` is `None`, the legacy (nb) tombstone-histogram width is assumed
/// (nb-compatible default), matching the rest of the minimal Statistics parser.
///
/// `pendingRepair` / `isTransient` are reported honestly as `RepairField::Unparsed`
/// (this module does not walk them; see module docs); this function never
/// fabricates a repaired state.
///
/// # Errors
///
/// Returns an error only when the STATS component is present but its leading
/// (self-describing) fields are truncated/corrupt, OR overrun the STATS
/// component's authoritative end bound — strict callers can rely on this to fail
/// closed (a truncated body or an over-long internal length never spills into
/// the trailing CRC or the following component). A *missing* STATS component is
/// reported as the unrepaired default with `repaired_at_decoded = false` (the
/// buffer carried no STATS section to decode). A STATS entry that IS present but
/// whose derived byte range is invalid (offset past EOF, inverted) is treated as
/// corruption and fails closed.
pub fn parse_repair_metadata(input: &[u8], gates: Option<&VersionGates>) -> Result<RepairMetadata> {
    let Some(bounds) = stats_component_bounds(input)? else {
        return Ok(RepairMetadata::unrepaired_default());
    };

    let modern_histogram = gates.map(uses_modern_tombstone_histogram).unwrap_or(false);

    // Bound the cursor over ONLY the STATS component slice (start..end, with the
    // trailing CRC and following components excluded). Any read past this slice
    // fails closed with Error::Corruption.
    let mut c = Cursor::new(&input[bounds.start..bounds.end]);

    // 1-2. estimatedPartitionSize + estimatedCellPerPartitionCount.
    skip_estimated_histogram(&mut c)?;
    skip_estimated_histogram(&mut c)?;

    // 3. commitLogUpperBound: i64 segmentId + i32 position.
    c.skip(8 + 4)?;

    // 4. minTimestamp, maxTimestamp.
    c.skip(8 + 8)?;

    // 5. min/maxLocalDeletionTime. Both encodings (nb 2× i32, oa/da 2× u32)
    //    occupy 8 bytes total, so the width does not branch here.
    c.skip(4 + 4)?;

    // 6. minTTL, maxTTL.
    c.skip(4 + 4)?;

    // 7. compressionRatio (f64).
    c.skip(8)?;

    // 8. estimatedTombstoneDropTime (TombstoneHistogram) — the only format-gated
    //    field width on the path to repairedAt.
    skip_tombstone_histogram(&mut c, modern_histogram)?;

    // 9. sstableLevel (i32), then repairedAt (i64).
    let _sstable_level = c.read_i32()?;
    let repaired_at = c.read_i64()?;

    Ok(RepairMetadata {
        repaired_at,
        // Not walked by this module — reported honestly as not-yet-decoded
        // rather than a fabricated null / false (see module docs).
        pending_repair: RepairField::Unparsed,
        is_transient: RepairField::Unparsed,
        repaired_at_decoded: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal but valid STATS component (through `repairedAt`) plus a
    /// 4-component TOC pointing at it, so the decoder can be exercised in-memory
    /// without a fetched fixture.
    fn synthetic_statistics(modern_histogram: bool, repaired_at: i64) -> Vec<u8> {
        // --- STATS body ---
        let mut stats = Vec::new();
        let est_hist = |b: &mut Vec<u8>| {
            b.extend_from_slice(&0i32.to_be_bytes()); // bucket count 0 (self-describing)
        };
        est_hist(&mut stats); // estimatedPartitionSize
        est_hist(&mut stats); // estimatedCellPerPartitionCount
        stats.extend_from_slice(&(-1i64).to_be_bytes()); // commitLogUpperBound segmentId
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogUpperBound position
        stats.extend_from_slice(&100i64.to_be_bytes()); // minTimestamp
        stats.extend_from_slice(&200i64.to_be_bytes()); // maxTimestamp
        stats.extend_from_slice(&i32::MAX.to_be_bytes()); // minLocalDeletionTime
        stats.extend_from_slice(&i32::MAX.to_be_bytes()); // maxLocalDeletionTime
        stats.extend_from_slice(&0i32.to_be_bytes()); // minTTL
        stats.extend_from_slice(&0i32.to_be_bytes()); // maxTTL
        stats.extend_from_slice(&(-1.0f64).to_be_bytes()); // compressionRatio
                                                           // TombstoneHistogram: empty (maxBinSize=0, size=0) regardless of width
        stats.extend_from_slice(&0i32.to_be_bytes());
        stats.extend_from_slice(&0i32.to_be_bytes());
        let _ = modern_histogram; // empty histogram has identical bytes for both
        stats.extend_from_slice(&0i32.to_be_bytes()); // sstableLevel
        stats.extend_from_slice(&repaired_at.to_be_bytes()); // repairedAt

        // --- TOC: 4 components, STATS (type 2) points at the body ---
        // Layout: [u32 count][u32 marker][4 × (u32 type, u32 offset)][stats][crc]
        // The three non-STATS components are placed BEFORE the STATS body (with
        // tiny one-byte bodies) so STATS is the last component, and its end is
        // derived as `file_len - trailing CRC`. This exercises the
        // last-component bound path.
        let toc_len = 4 + 4 + 4 * 8; // count + marker + entries (no trailing acc here)
                                     // 3 one-byte placeholder bodies precede STATS.
        let comp0_off = toc_len; // HEADER-ish placeholder
        let comp1_off = comp0_off + 1;
        let comp3_off = comp1_off + 1;
        let stats_off = comp3_off + 1;
        let mut out = Vec::new();
        out.extend_from_slice(&4u32.to_be_bytes()); // num components
        out.extend_from_slice(&0u32.to_be_bytes()); // marker (unused by this decoder)
        for (ty, off) in [
            (0u32, comp0_off as u32),
            (1u32, comp1_off as u32),
            (2u32, stats_off as u32), // STATS (last by offset)
            (3u32, comp3_off as u32),
        ] {
            out.extend_from_slice(&ty.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }
        // 3 placeholder component bodies (1 byte each).
        out.extend_from_slice(&[0u8, 0u8, 0u8]);
        debug_assert_eq!(out.len(), stats_off);
        out.extend_from_slice(&stats);
        out.extend_from_slice(&0u32.to_be_bytes()); // trailing metadata CRC
        out
    }

    #[test]
    fn decodes_repaired_at_legacy_histogram() {
        let bytes = synthetic_statistics(false, 0);
        let md = parse_repair_metadata(&bytes, None).expect("decode");
        assert_eq!(md.repaired_at, 0);
        assert!(md.repaired_at_decoded);
        // pending_repair / is_transient are not walked → reported as Unparsed,
        // NOT as a fabricated null / false.
        assert_eq!(md.pending_repair, RepairField::Unparsed);
        assert_eq!(md.is_transient, RepairField::Unparsed);
        assert!(!md.pending_repair.is_decoded());
        assert!(!md.is_transient.is_decoded());
    }

    #[test]
    fn decodes_nonzero_repaired_at() {
        let bytes = synthetic_statistics(false, 1_700_000_000_000);
        let md = parse_repair_metadata(&bytes, None).expect("decode");
        assert_eq!(md.repaired_at, 1_700_000_000_000);
        assert!(md.repaired_at_decoded);
    }

    #[test]
    fn malformed_toc_fails_closed() {
        // A corrupt/truncated TOC must NOT be silently reported as the
        // unrepaired default (repaired_at=0); it must fail closed.
        // (a) non-empty file shorter than the 8-byte TOC header.
        assert!(parse_repair_metadata(&[0u8, 0, 0], None).is_err());
        // (b) component count of zero.
        let mut zero = Vec::new();
        zero.extend_from_slice(&0u32.to_be_bytes());
        zero.extend_from_slice(&0u32.to_be_bytes());
        assert!(parse_repair_metadata(&zero, None).is_err());
        // (c) absurd component count.
        let mut huge = Vec::new();
        huge.extend_from_slice(&101u32.to_be_bytes());
        huge.extend_from_slice(&0u32.to_be_bytes());
        assert!(parse_repair_metadata(&huge, None).is_err());
        // (d) well-formed count but the TOC body is truncated.
        let mut truncated = Vec::new();
        truncated.extend_from_slice(&4u32.to_be_bytes()); // claims 4 entries
        truncated.extend_from_slice(&0u32.to_be_bytes()); // marker
        truncated.extend_from_slice(&2u32.to_be_bytes()); // only a partial first entry
        assert!(parse_repair_metadata(&truncated, None).is_err());
    }

    #[test]
    fn missing_stats_component_reports_unrepaired_default() {
        // TOC with no STATS entry (only HEADER) → default, not an error.
        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_be_bytes()); // 1 component
        out.extend_from_slice(&0u32.to_be_bytes()); // marker
        out.extend_from_slice(&3u32.to_be_bytes()); // type HEADER
        out.extend_from_slice(&16u32.to_be_bytes()); // offset (irrelevant)
        let md = parse_repair_metadata(&out, None).expect("default");
        assert_eq!(md, RepairMetadata::unrepaired_default());
        assert!(!md.repaired_at_decoded);
    }

    #[test]
    fn truncated_stats_fails_closed() {
        let mut bytes = synthetic_statistics(false, 0);
        // Truncate inside the STATS body so the forward walk runs off the end.
        bytes.truncate(bytes.len() - 4);
        let err = parse_repair_metadata(&bytes, None);
        assert!(
            err.is_err(),
            "truncated STATS component must fail closed, got {err:?}"
        );
    }

    /// A STATS body whose internal length field overruns the component's end
    /// bound (but still fits within the rest of the file) must fail closed,
    /// proving the cursor is bounded by the STATS component, not by the file.
    #[test]
    fn stats_overrunning_component_bound_fails_closed() {
        // Build a buffer where STATS is NOT the last component, so there is a
        // following component AND a trailing CRC the decoder must never read.
        let mut stats = Vec::new();
        // Empty estimatedPartitionSize.
        stats.extend_from_slice(&0i32.to_be_bytes());
        // A SECOND EstimatedHistogram whose bucket count overruns the bound.
        // We will set this count after we know how many bytes remain.
        let bad_count_pos = stats.len();
        stats.extend_from_slice(&0i32.to_be_bytes()); // placeholder
                                                      // No more STATS bytes: the component ends right after this field.

        // Layout: [count][marker][3 entries][stats][NEXT component bytes][crc].
        // STATS is entry index 1; a HEADER component follows it in the file so
        // there ARE bytes after STATS that the bad count would spill into.
        let toc_len = 4 + 4 + 3 * 8;
        let stats_off = toc_len;
        let next_off = stats_off + stats.len(); // HEADER directly after STATS
        let header_bytes = [0xAAu8; 64]; // plenty of bytes after STATS

        // Now make the second histogram's bucket count large enough that
        // 16*count would run past the STATS end but still inside the file.
        let bad_count: i32 = 8; // 8 * 16 = 128 bytes, far past the 0-byte remainder
        stats[bad_count_pos..bad_count_pos + 4].copy_from_slice(&bad_count.to_be_bytes());

        let mut out = Vec::new();
        out.extend_from_slice(&3u32.to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        for (ty, off) in [
            (3u32, next_off as u32), // HEADER (follows STATS)
            (2u32, stats_off as u32),
            (0u32, (next_off + header_bytes.len()) as u32),
        ] {
            out.extend_from_slice(&ty.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }
        debug_assert_eq!(out.len(), stats_off);
        out.extend_from_slice(&stats);
        out.extend_from_slice(&header_bytes); // next component
        out.extend_from_slice(&0u32.to_be_bytes()); // trailing CRC

        let err = parse_repair_metadata(&out, None);
        assert!(
            err.is_err(),
            "a STATS body that overruns its component bound must fail closed \
             (not spill into the next component / CRC), got {err:?}"
        );
    }

    /// The component end bound is derived from the NEXT TOC offset (when STATS
    /// is not last), excluding the trailing CRC when STATS IS last.
    #[test]
    fn component_bounds_derive_end_from_next_offset() {
        // STATS-last synthetic: end == file_len - 4 (the CRC).
        let bytes = synthetic_statistics(false, 0);
        let bounds = stats_component_bounds(&bytes)
            .expect("no error")
            .expect("bounds present");
        assert_eq!(
            bounds.end,
            bytes.len() - METADATA_COMPONENT_CRC_LEN,
            "STATS-last end must exclude the trailing CRC"
        );
        assert!(bounds.start < bounds.end);
    }

    /// When STATS is NOT the last component, its end must be the next component's
    /// TOC offset MINUS the 4-byte per-component CRC32 Cassandra writes between
    /// each component body and the next component's offset — never the raw next
    /// offset (which would let the decoder read 4 CRC bytes as metadata).
    #[test]
    fn nonlast_stats_end_excludes_component_crc() {
        // Realistic layout: [count][marker][2 entries][STATS body][STATS crc][HEADER body][HEADER crc]
        let mut stats = Vec::new();
        stats.extend_from_slice(&0i32.to_be_bytes()); // estimatedPartitionSize (empty)
        stats.extend_from_slice(&0i32.to_be_bytes()); // estimatedCellPerPartitionCount (empty)
        stats.extend_from_slice(&7i64.to_be_bytes()); // a few more bytes of body

        let toc_len = 4 + 4 + 2 * 8;
        let stats_off = toc_len;
        // HEADER begins AFTER the STATS body + its 4-byte CRC.
        let header_off = stats_off + stats.len() + METADATA_COMPONENT_CRC_LEN;
        let header_body = [0xABu8; 8];

        let mut out = Vec::new();
        out.extend_from_slice(&2u32.to_be_bytes()); // 2 components
        out.extend_from_slice(&0u32.to_be_bytes()); // marker
        for (ty, off) in [(2u32, stats_off as u32), (3u32, header_off as u32)] {
            out.extend_from_slice(&ty.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }
        debug_assert_eq!(out.len(), stats_off);
        out.extend_from_slice(&stats);
        let stats_crc = crc32_ieee(&out[stats_off..stats_off + stats.len()]);
        out.extend_from_slice(&stats_crc.to_be_bytes()); // per-component CRC after STATS
        debug_assert_eq!(out.len(), header_off);
        out.extend_from_slice(&header_body);
        out.extend_from_slice(&0u32.to_be_bytes()); // HEADER's trailing CRC

        let bounds = stats_component_bounds(&out)
            .expect("no error")
            .expect("bounds present");
        assert_eq!(
            bounds.end,
            header_off - METADATA_COMPONENT_CRC_LEN,
            "non-last STATS end must exclude the inter-component CRC"
        );
        assert_eq!(
            bounds.end,
            stats_off + stats.len(),
            "non-last STATS end must equal the true STATS body end"
        );
    }

    /// Minimal CRC32 (IEEE) for the regression test's synthetic CRC bytes.
    fn crc32_ieee(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFF_FFFF;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            }
        }
        !crc
    }
}
