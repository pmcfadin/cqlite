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
//! `improvedMinMax` / `legacyMinMax` block and the variable `commitLogIntervals`
//! set, both of which require version-gated / type-aware skipping to traverse.
//!
//! # Full repair-state walk (issue #1021)
//!
//! When the caller supplies authoritative [`VersionGates`],
//! [`parse_repair_metadata`] now performs that version-gated forward walk and
//! decodes `pendingRepair` (UUID) and `isTransient` **from real bytes** in
//! addition to `repairedAt`. The walk is driven ENTIRELY by the version gates
//! (`hasLegacyMinMax` / `hasImprovedMinMax` / `hasUIntDeletionTime` /
//! `hasPendingRepair` / `hasIsTransient`) read from the parsed descriptor — no
//! heuristics, no type guessing (#28). Mirrors the exact field order of
//! cassandra-5.0.0 `StatsMetadata.StatsMetadataSerializer.serialize`, the same
//! layout the `da` STATS writer emits (`build_stats_component_da`).
//!
//! When `gates` is `None` the walk past `repairedAt` is NOT attempted (the
//! min/max block and the commit-log-interval shape are version-dependent and
//! cannot be traversed without the descriptor), so `pendingRepair` /
//! `isTransient` are reported honestly as [`RepairField::Unparsed`] rather than a
//! fabricated `null` / `false` — a real SSTable that carried a pending-repair
//! UUID or transient flag is never silently misreported as absent.

use crate::error::{Error, Result};
use crate::parser::repair_clustering::{
    resolve_clustering_value_layout, skip_covered_slice, ByteSkip,
};
use crate::storage::sstable::version_gate::VersionGates;

/// Cassandra `MetadataType.STATS` ordinal (MetadataType.java).
const METADATA_TYPE_STATS: u32 = 2;
/// Cassandra `MetadataType.HEADER` ordinal (SerializationHeader; MetadataType.java).
const METADATA_TYPE_HEADER: u32 = 3;

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
    /// Decoded from real bytes by the version-gated forward walk when
    /// authoritative [`VersionGates`] are supplied. `hasPendingRepair` is
    /// `>= na`, so the field is present and decoded for every Cassandra 5.0
    /// format CQLite reads (`nb`/`oa`/`da`). `Unparsed` is reserved for "present
    /// but not safely reachable" (an unmodeled clustering comparator) or when
    /// `gates` is `None` (the walk past `repairedAt` is not attempted). See
    /// module docs.
    pub pending_repair: RepairField<Option<[u8; 16]>>,

    /// `isTransient` flag (`Decoded(bool)`) or `Unparsed` when not decoded.
    ///
    /// Decoded from real bytes by the version-gated forward walk when
    /// authoritative [`VersionGates`] are supplied. `hasIsTransient` is `>= na`,
    /// so the field is present and decoded for every supported Cassandra 5.0
    /// format (`nb`/`oa`/`da`). `Unparsed` is reserved for "present but not
    /// safely reachable" or when `gates` is `None`.
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
/// trailing CRC.
///
/// Every Cassandra 5.0 format CQLite reads (`nb` BIG, `oa` BIG, `da` BTI) is
/// `>= na`, so `hasMetadataChecksum` is always true and
/// `MetadataSerializer.serialize` always writes a 4-byte CRC32 after each
/// component body (`MetadataSerializer.java:110-111,117`). The body bound
/// therefore always subtracts that CRC. (Pre-`na` BIG versions — `ma`–`me` — are
/// out of scope: CQLite is a Cassandra 5.0 reader and does not open them.)
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
    parse_statistics_toc(input).stats_bounds()
}

/// The `Statistics.db` Table-of-Contents parsed ONCE (issue #2148): the resolved
/// HEADER (SerializationHeader) offset and the STATS component `[start, end)`
/// bounds, both extracted in a single forward walk.
///
/// Before #2148 the metadata stack re-walked the same on-disk TOC **three times**
/// per open — once for the HEADER offset in
/// [`crate::parser::enhanced_statistics_parser`], then once each inside
/// [`read_table_counts`] and [`parse_stats_extras`]. Parsing the TOC once and
/// threading this struct to those consumers (`*_with_toc`) eliminates the two
/// redundant walks (`parser::toc_walk_metrics` drops from 3 → 1 per parse).
/// Crate-private (issue #2148 blocker): this reuse type has no external consumers
/// (only the in-crate `enhanced_statistics_parser`), and its `*_with_toc` consumers
/// slice `input` by bounds carried in the TOC. Keeping it `pub(crate)` guarantees a
/// `StatisticsToc` parsed from one buffer can never be paired with a different
/// (smaller) buffer at a public boundary, which would index out of range.
#[derive(Debug, Clone)]
pub(crate) struct StatisticsToc {
    /// Offset of the HEADER (SerializationHeader) component, or `None` when the
    /// TOC has no HEADER entry OR is too malformed to locate one. Resolved
    /// INDEPENDENTLY of the STATS bounds (issue #2148): a HEADER entry found before
    /// an invalid STATS byte-range is still returned here, so EncodingStats decodes
    /// authoritatively from it even while [`Self::stats_bounds`] fails closed. This
    /// preserves the pre-#2148 `parse_statistics_toc_for_header_offset` semantics,
    /// whose header walk never depended on any STATS-range check.
    header_offset: Option<usize>,
    /// STATS component `[start, end)` bounds. `Ok(None)` = a well-formed TOC that
    /// carries no STATS component; `Err(msg)` = a malformed/truncated TOC or an
    /// invalid STATS range (fail-closed). The corruption *message* is stored (not
    /// `Error`, which is not `Clone`) so BOTH the row-count and stats-extras
    /// consumers can each rebuild the same `Error::Corruption` from one shared
    /// parse.
    stats_bounds: std::result::Result<Option<StatsComponentBounds>, String>,
}

impl StatisticsToc {
    /// The resolved HEADER (SerializationHeader) component offset (lenient `None`).
    pub(crate) fn header_offset(&self) -> Option<usize> {
        self.header_offset
    }

    /// The STATS component bounds as a strict [`Result`], reconstructing the
    /// fail-closed `Error::Corruption` for a malformed TOC / invalid range so
    /// consumers see identical error semantics to the pre-#2148 per-consumer walk.
    fn stats_bounds(&self) -> Result<Option<StatsComponentBounds>> {
        match &self.stats_bounds {
            Ok(v) => Ok(*v),
            Err(msg) => Err(Error::Corruption(msg.clone())),
        }
    }
}

/// Parse the `Statistics.db` TOC ONCE (issue #2148), resolving both the HEADER
/// offset and the STATS component bounds in a single pass.
///
/// This is the single TOC-walk site the `parser::toc_walk_metrics` counter
/// records — consumers that accept a [`StatisticsToc`] (`*_with_toc`) add no
/// further walk. A malformed/truncated TOC yields `header_offset = None` (lenient)
/// and a fail-closed STATS-bounds error (surfaced when a consumer reads it).
pub(crate) fn parse_statistics_toc(input: &[u8]) -> StatisticsToc {
    // Count this TOC walk (issue #1658 A5 bench instrumentation — no decode effect).
    crate::parser::toc_walk_metrics::record_toc_walk();
    // The walk resolves the HEADER offset and the STATS bounds INDEPENDENTLY: a
    // corrupt STATS byte-range must never nuke a HEADER that was already located
    // (issue #2148 roborev/reviewer blocker) — a valid HEADER still decodes
    // EncodingStats authoritatively even when the STATS range fails closed.
    let (header_offset, stats_bounds) = walk_statistics_toc(input);
    StatisticsToc {
        header_offset,
        stats_bounds,
    }
}

/// Single forward walk over the `Statistics.db` TOC resolving the HEADER offset
/// and the STATS `[start, end)` bounds together. Pure (no counter side effect);
/// the public [`parse_statistics_toc`] records the walk and wraps the outcome.
///
/// The two outputs are resolved INDEPENDENTLY, exactly as the pre-#2148 separate
/// walks (`parse_statistics_toc_for_header_offset` for the header, and
/// `stats_component_bounds` for STATS) were: the returned `header_offset` is the
/// first present HEADER entry (lenient — `None` when the TOC is malformed/truncated
/// or carries no HEADER). The STATS-bounds `Result` FAILS CLOSED (`Err(msg)`) on a
/// malformed/truncated TOC or an invalid STATS range rather than being silently
/// reported as "no STATS component"; `Ok(None)` is reserved exclusively for a
/// well-formed TOC that genuinely carries no STATS entry. Crucially, a corrupt
/// STATS byte-range does NOT discard an already-resolved HEADER offset — a valid
/// HEADER still decodes EncodingStats authoritatively (issue #2148 blocker).
fn walk_statistics_toc(
    input: &[u8],
) -> (
    Option<usize>,
    std::result::Result<Option<StatsComponentBounds>, String>,
) {
    if input.len() < 8 {
        return (
            None,
            Err(format!(
                "Statistics.db too short for a metadata TOC header: {} bytes",
                input.len()
            )),
        );
    }
    let num_components = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    if num_components == 0 || num_components > 100 {
        return (
            None,
            Err(format!(
                "Statistics.db TOC component count out of range: {num_components}"
            )),
        );
    }
    let toc_start = 8usize;
    let entry_size = 8usize;
    let toc_size = match (num_components as usize)
        .checked_mul(entry_size)
        .and_then(|n| n.checked_add(toc_start))
    {
        Some(n) => n,
        None => {
            return (
                None,
                Err(format!(
                    "Statistics.db TOC size overflow for {num_components} components"
                )),
            )
        }
    };
    if input.len() < toc_size {
        return (
            None,
            Err(format!(
                "Statistics.db TOC truncated: need {toc_size} bytes, have {}",
                input.len()
            )),
        );
    }

    let mut stats_off: Option<usize> = None;
    let mut header_off: Option<usize> = None;
    let mut offsets: Vec<usize> = Vec::with_capacity(num_components as usize);
    for i in 0..num_components as usize {
        let entry = match i
            .checked_mul(entry_size)
            .and_then(|n| n.checked_add(toc_start))
        {
            Some(e) => e,
            None => {
                // TOC index arithmetic overflow: the header is not reliably
                // resolvable, so surface the corruption on BOTH channels.
                return (
                    header_off,
                    Err(format!(
                        "Statistics.db TOC entry offset overflow at index {i}"
                    )),
                );
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
            // Last-wins, matching the historical `stats_component_bounds` walk.
            stats_off = Some(off);
        } else if ty == METADATA_TYPE_HEADER && header_off.is_none() {
            // First-wins, matching the historical
            // `parse_statistics_toc_for_header_offset` early-return.
            header_off = Some(off);
        }
    }

    // No STATS component in the TOC → nothing to decode (not an error).
    let Some(start) = stats_off else {
        return (header_off, Ok(None));
    };

    // The STATS body ends 4 bytes before the *next* component begins: every
    // supported Cassandra 5.0 format (`nb`/`oa`/`da`, all `>= na`) writes a
    // per-component CRC32 between each component body and the next component's
    // offset (`MetadataSerializer.java:110-111`). Use the smallest component
    // offset strictly greater than `start`, minus that CRC; when STATS is the last
    // component, bound by the file end minus the trailing CRC. Both cases exclude
    // the 4-byte CRC so it is never decoded as metadata.
    let next_boundary = offsets
        .iter()
        .copied()
        .filter(|&o| o > start)
        .min()
        .unwrap_or(input.len());
    let end = next_boundary.saturating_sub(METADATA_COMPONENT_CRC_LEN);

    // A STATS entry exists but its derived range is invalid (offset past EOF,
    // inverted, or empty): genuine corruption — fail closed on the STATS channel
    // rather than silently reporting the unrepaired default or building a cursor
    // over garbage. The HEADER offset stays resolved and is returned regardless
    // (issue #2148 blocker: a corrupt STATS range must not nuke a valid HEADER).
    if start >= end || end > input.len() {
        return (
            header_off,
            Err(format!(
                "STATS component range invalid: start {start}, derived end {end}, \
                 Statistics.db {} bytes",
                input.len()
            )),
        );
    }

    (header_off, Ok(Some(StatsComponentBounds { start, end })))
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

    fn read_u32(&mut self) -> Result<u32> {
        let end = self.need(4)?;
        let v = u32::from_be_bytes([
            self.bytes[self.pos],
            self.bytes[self.pos + 1],
            self.bytes[self.pos + 2],
            self.bytes[self.pos + 3],
        ]);
        self.pos = end;
        Ok(v)
    }

    fn read_f64(&mut self) -> Result<f64> {
        let end = self.need(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(f64::from_be_bytes(buf))
    }

    fn read_u8(&mut self) -> Result<u8> {
        let end = self.need(1)?;
        let v = self.bytes[self.pos];
        self.pos = end;
        Ok(v)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let end = self.need(2)?;
        let v = u16::from_be_bytes([self.bytes[self.pos], self.bytes[self.pos + 1]]);
        self.pos = end;
        Ok(v)
    }

    /// Read a 16-byte raw UUID (the two `long`s Cassandra writes via
    /// `UUIDSerializer`: most-significant then least-significant bits, each BE).
    fn read_uuid(&mut self) -> Result<[u8; 16]> {
        let end = self.need(16)?;
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(buf)
    }

    /// Read an unsigned VInt (Cassandra `VIntCoding.readUnsignedVInt`):
    /// the first byte's leading 1-bits give the number of EXTRA bytes; the
    /// remaining low bits of the first byte are the high-order value bits.
    fn read_unsigned_vint(&mut self) -> Result<u64> {
        let first = self.read_u8()?;
        let extra = first.leading_ones() as usize;
        if extra == 0 {
            return Ok(first as u64);
        }
        if extra > 8 {
            return Err(Error::Corruption(format!(
                "invalid unsigned VInt: {extra} extra bytes"
            )));
        }
        let mut value: u64 = 0;
        for _ in 0..extra {
            let b = self.read_u8()?;
            value = (value << 8) | b as u64;
        }
        // The first byte contributes `8 - extra` low bits (its bits below the
        // `extra` leading 1s and the separator 0). For extra == 8 there are no
        // first-byte value bits.
        if extra < 8 {
            let mask = (1u8 << (8 - extra)) as u64;
            // bits below the separator bit
            let first_bits = first as u64 & (mask - 1);
            value |= first_bits << (8 * extra);
        }
        Ok(value)
    }

    /// Read `n` raw bytes as an owned `Vec`, bounded by the cursor (fails closed
    /// on overrun). Used for the clusteringTypes UTF-8 names.
    fn read_bytes(&mut self, n: usize) -> Result<Vec<u8>> {
        let end = self.need(n)?;
        let v = self.bytes[self.pos..end].to_vec();
        self.pos = end;
        Ok(v)
    }
}

impl ByteSkip for Cursor<'_> {
    fn read_u8(&mut self) -> Result<u8> {
        Cursor::read_u8(self)
    }
    fn read_u16(&mut self) -> Result<u16> {
        Cursor::read_u16(self)
    }
    fn read_unsigned_vint(&mut self) -> Result<u64> {
        Cursor::read_unsigned_vint(self)
    }
    fn skip(&mut self, n: usize) -> Result<()> {
        Cursor::skip(self, n)
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

/// The version-gate flags the post-`repairedAt` forward walk needs, projected
/// out of [`VersionGates`] so the walk depends only on the AUTHORITATIVE parsed
/// descriptor (no heuristics, #28). Every flag mirrors a single Cassandra
/// `BigFormat`/`BtiFormat` version gate consumed by
/// `StatsMetadata.StatsMetadataSerializer.serialize`.
#[derive(Debug, Clone, Copy)]
struct RepairWalkGates {
    /// Modern tombstone-histogram entry width (`oa`+/`da`).
    modern_histogram: bool,
    /// `hasImprovedMinMax` — emits clusteringTypes + a covered `Slice` in place
    /// of the legacy min/max clustering-value lists.
    has_improved_min_max: bool,
    /// `hasLegacyMinMax` — emits min/max clustering-value lists (each
    /// `i32 count` then `count` × vint-length value).
    has_legacy_min_max: bool,
    /// `hasPendingRepair` (`na`+/`da`) — the `pendingRepair` nullable-UUID field
    /// is present in the body.
    has_pending_repair: bool,
    /// `hasIsTransient` (`na`+/`da`) — the `isTransient` boolean is present.
    has_is_transient: bool,
}

impl RepairWalkGates {
    fn from(gates: &VersionGates) -> Self {
        match gates {
            VersionGates::Big(g) => RepairWalkGates {
                modern_histogram: g.has_uint_deletion_time,
                has_improved_min_max: g.has_improved_min_max,
                has_legacy_min_max: g.has_legacy_min_max,
                has_pending_repair: g.has_pending_repair,
                has_is_transient: g.has_is_transient,
            },
            VersionGates::Bti(g) => RepairWalkGates {
                modern_histogram: true,
                has_improved_min_max: g.has_improved_min_max,
                has_legacy_min_max: g.has_legacy_min_max,
                has_pending_repair: g.has_pending_repair,
                has_is_transient: g.has_is_transient,
            },
        }
    }
}

/// Skip the legacy min/max clustering-value lists (`hasLegacyMinMax`).
///
/// Each list is `i32 count` then `count` × a value written with
/// `ByteBufferUtil.writeWithShortLength` — an UNSIGNED 16-bit length prefix
/// (`writeShort`) then the raw bytes (cassandra-5.0.0
/// `StatsMetadata.StatsMetadataSerializer.serialize` legacy branch). Verified
/// against a real `nb` `system_schema` fixture (clustering values present).
fn skip_legacy_min_max(c: &mut Cursor) -> Result<()> {
    for _ in 0..2 {
        let count = c.read_i32()?;
        if count < 0 {
            return Err(Error::Corruption(format!(
                "negative legacy min/max clustering count {count}"
            )));
        }
        for _ in 0..count {
            let len = c.read_u16()? as usize;
            c.skip(len)?;
        }
    }
    Ok(())
}

/// Attempt to skip the improvedMinMax block (`hasImprovedMinMax`): a
/// clusteringTypes list (unsigned-VInt count, then each type a vint-length UTF-8
/// string) followed by the covered `Slice` (two `ClusteringBound`s, each
/// `byte kind` + `short size` + `size` comparator-encoded values).
///
/// The covered-clustering bound VALUES are RAW comparator-encoded (no per-value
/// length prefix), exactly as cassandra-5.0.0
/// `ClusteringPrefix.Serializer.serializeValuesWithoutSize` writes them: 32-value
/// header batches then each present value's bytes via `AbstractType.writeValue`
/// (fixed-width raw, or `writeWithVIntLength` for variable types). We skip them
/// authoritatively by resolving each clustering column's `valueLengthIfFixed()`
/// from the persisted `clusteringTypes` and mirroring
/// `ClusteringBoundOrBoundary.Serializer.skipValues` (see [`repair_clustering`]).
///
/// Returns `Ok(true)` when the whole block (types + covered Slice) was skipped, so
/// the walk may continue to `pendingRepair`/`isTransient`. Returns `Ok(false)`
/// ONLY when a clustering type is not modeled (its fixed-vs-variable nature is
/// unknown) or a bound advertises more values than the types describe — i.e. the
/// length of some value cannot be computed. In that case the caller reports the
/// trailing fields honestly as `Unparsed` rather than guessing (#28). Empty
/// bounds (`Slice.ALL`, the no-clustering shape) skip cleanly to `Ok(true)`.
fn try_skip_improved_min_max(c: &mut Cursor) -> Result<bool> {
    // clusteringTypes list → resolved per-column value layouts. The raw UTF-8
    // type names are read through the same bounded cursor.
    let layouts = {
        // Borrow the cursor mutably inside the closure by reading bytes directly;
        // read_clustering_type_layouts drives both the vint reads and the name
        // reads through `c`, but the closure also needs `c`, so we inline the
        // loop here instead of passing a borrow twice.
        let count = ByteSkip::read_unsigned_vint(c)? as usize;
        let mut v = Vec::with_capacity(count.min(64));
        for _ in 0..count {
            let len = ByteSkip::read_unsigned_vint(c)? as usize;
            let bytes = c.read_bytes(len)?;
            let name = std::str::from_utf8(&bytes).map_err(|_| {
                Error::Corruption("clusteringTypes entry is not valid UTF-8".to_string())
            })?;
            v.push(resolve_clustering_value_layout(name));
        }
        v
    };
    // coveredClustering = Slice: start bound, then end bound, each skipped per the
    // resolved layouts. A bool result distinguishes "skipped" from "unmodeled".
    skip_covered_slice(c, &layouts)
}

/// Skip the `commitLogIntervals` IntervalSet: `i32 size` then `size` × an
/// interval (`CommitLogPosition` start + end = 2 × (`i64 segmentId` +
/// `i32 position`) = 24 bytes per interval).
fn skip_commit_log_intervals(c: &mut Cursor) -> Result<()> {
    let size = c.read_i32()?;
    if size < 0 {
        return Err(Error::Corruption(format!(
            "negative commitLogIntervals size {size}"
        )));
    }
    let bytes = (size as usize)
        .checked_mul(24)
        .ok_or_else(|| Error::Corruption("commitLogIntervals size overflow".to_string()))?;
    c.skip(bytes)
}

/// Decode the repair-state metadata from a raw `Statistics.db` buffer.
///
/// `repairedAt` is decoded from the STATS component for every format. When
/// authoritative [`VersionGates`] are supplied, the full version-gated forward
/// walk continues past `repairedAt` and decodes `pendingRepair` and
/// `isTransient` from real bytes too (issue #1021). When `gates` is `None`, the
/// legacy (nb) tombstone-histogram width is assumed to reach `repairedAt`, and
/// the two later fields are reported honestly as [`RepairField::Unparsed`] (the
/// min/max block and commit-log-interval shape are version-dependent and cannot
/// be traversed without the descriptor).
///
/// # Errors
///
/// Returns an error only when the STATS component is present but its fields are
/// truncated/corrupt, OR overrun the STATS component's authoritative end bound —
/// strict callers can rely on this to fail closed (a truncated body or an
/// over-long internal length never spills into the trailing CRC or the following
/// component). A *missing* STATS component is reported as the unrepaired default
/// with `repaired_at_decoded = false`. A STATS entry that IS present but whose
/// derived byte range is invalid (offset past EOF, inverted) is treated as
/// corruption and fails closed.
pub fn parse_repair_metadata(input: &[u8], gates: Option<&VersionGates>) -> Result<RepairMetadata> {
    let walk = gates.map(RepairWalkGates::from);

    let Some(bounds) = stats_component_bounds(input)? else {
        return Ok(RepairMetadata::unrepaired_default());
    };

    let modern_histogram = walk.map(|w| w.modern_histogram).unwrap_or(false);

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

    // Without authoritative gates the post-repairedAt min/max block and the
    // commit-log-interval shape cannot be traversed; report the two later fields
    // honestly as Unparsed rather than a fabricated null / false.
    let Some(walk) = walk else {
        return Ok(RepairMetadata {
            repaired_at,
            pending_repair: RepairField::Unparsed,
            is_transient: RepairField::Unparsed,
            repaired_at_decoded: true,
        });
    };

    // 10. min/max clustering block. Cassandra serializes the improvedMinMax
    //     variant when present, otherwise the legacy variant; a version that
    //     carries neither (pre-mb) writes nothing here.
    //
    //     The improvedMinMax covered-clustering Slice carries comparator-encoded
    //     bound VALUES that this repair-only decoder does not model. When those
    //     values are present (a table with clustering data) the walk cannot
    //     safely continue, so the two later fields are reported honestly as
    //     `Unparsed` rather than mis-decoded.
    if walk.has_improved_min_max {
        if !try_skip_improved_min_max(&mut c)? {
            return Ok(RepairMetadata {
                repaired_at,
                pending_repair: RepairField::Unparsed,
                is_transient: RepairField::Unparsed,
                repaired_at_decoded: true,
            });
        }
    } else if walk.has_legacy_min_max {
        skip_legacy_min_max(&mut c)?;
    }

    // 11. hasLegacyCounterShards (bool).
    c.skip(1)?;

    // 12. totalColumnsSet (long), totalRows (long).
    c.skip(8 + 8)?;

    // 13. commitLogLowerBound (CommitLogPosition) + commitLogIntervals
    //     (IntervalSet). `hasCommitLogLowerBound` is `>= mb` and
    //     `hasCommitLogIntervals` is `>= mc` in Cassandra; both gates are below
    //     `na`, so every Cassandra 5.0 format CQLite reads (`nb`/`oa`/`da`) always
    //     carries both fields. Skip them unconditionally. (The pre-`mc` absent
    //     case is out of scope — CQLite is a Cassandra 5.0 reader.)
    c.skip(8 + 4)?; // commitLogLowerBound CommitLogPosition: i64 segmentId + i32 position
    skip_commit_log_intervals(&mut c)?;

    // 14. pendingRepair: a presence byte (writeBoolean), then a 16-byte UUID
    //     when present. `hasPendingRepair` is `version.compareTo("na") >= 0`
    //     (cassandra-5.0.0 `BigFormat`/`BtiFormat`), so it is ALWAYS true for
    //     every Cassandra 5.0 format CQLite reads (`nb`/`oa`/`da`) and the field
    //     is always present and decoded. The `else` is unreachable for supported
    //     formats (pre-`na` BIG `ma`–`me` is out of scope — CQLite is a Cassandra
    //     5.0 reader); it fails closed via `Unparsed` rather than fabricating a
    //     value, which `classify_inputs` rejects.
    let pending_repair = if walk.has_pending_repair {
        let present = c.read_u8()?;
        if present != 0 {
            RepairField::Decoded(Some(c.read_uuid()?))
        } else {
            RepairField::Decoded(None)
        }
    } else {
        RepairField::Unparsed
    };

    // 15. isTransient (bool). Same `>= na` gate as pendingRepair, so always
    //     present and decoded for supported Cassandra 5.0 formats. The `else` is
    //     unreachable for `nb`/`oa`/`da`; it fails closed (`Unparsed`).
    let is_transient = if walk.has_is_transient {
        RepairField::Decoded(c.read_u8()? != 0)
    } else {
        RepairField::Unparsed
    };

    Ok(RepairMetadata {
        repaired_at,
        pending_repair,
        is_transient,
        repaired_at_decoded: true,
    })
}

/// Authoritative per-SSTable row/partition counts decoded from the STATS
/// component of `Statistics.db` (issue #944).
///
/// Both fields come straight from cassandra-5.0.0
/// `StatsMetadata.StatsMetadataSerializer.serialize` — no heuristics (#28):
///
///   * `partition_count` is the sum of the bucket counts of the
///     `estimatedPartitionSize` `EstimatedHistogram` (the FIRST STATS field). That
///     histogram records one observation per partition, so Σ counts is exactly the
///     SSTable's partition count — the same value `SSTableReader` exposes via
///     `getEstimatedPartitionSize().count()`. It is reachable by a fully
///     self-describing walk and needs NO version gates.
///   * `total_rows` is the `totalRows` `long` written near the end of the STATS
///     body (field 12). Reaching it requires the version-gated forward walk
///     (`improvedMinMax`/`legacyMinMax` block + commit-log intervals), so it is
///     `Some` only when authoritative [`VersionGates`] are supplied AND the walk
///     could traverse the min/max block. For a clustered table whose covered-Slice
///     bound values are not modeled, `total_rows` is honestly `None` rather than a
///     guessed value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCounts {
    /// Σ `estimatedPartitionSize` histogram bucket counts = partition count.
    pub partition_count: u64,
    /// `totalRows` from the STATS body, when the gated walk could reach it.
    pub total_rows: Option<u64>,
}

/// Decode authoritative [`TableCounts`] from a raw `Statistics.db` buffer.
///
/// `partition_count` is always decoded (self-describing leading histogram).
/// `total_rows` is decoded only when `gates` is `Some` and the version-gated walk
/// to field 12 succeeds; otherwise it is `None` (never fabricated).
///
/// # Errors
///
/// Returns `Error::Corruption` when the STATS component is present but truncated
/// or structurally invalid (mirrors [`parse_repair_metadata`] — fails closed). A
/// `Statistics.db` with NO STATS component yields all-zero counts
/// (`partition_count == 0`, `total_rows == None`).
pub fn read_table_counts(input: &[u8], gates: Option<&VersionGates>) -> Result<TableCounts> {
    read_table_counts_from_bounds(input, stats_component_bounds(input)?, gates)
}

/// [`read_table_counts`] over a `Statistics.db` TOC that was already parsed once
/// (issue #2148): reuses the STATS bounds from `toc` instead of re-walking the
/// TOC, so a single metadata parse walks the TOC exactly once.
pub(crate) fn read_table_counts_with_toc(
    input: &[u8],
    toc: &StatisticsToc,
    gates: Option<&VersionGates>,
) -> Result<TableCounts> {
    read_table_counts_from_bounds(input, toc.stats_bounds()?, gates)
}

fn read_table_counts_from_bounds(
    input: &[u8],
    bounds: Option<StatsComponentBounds>,
    gates: Option<&VersionGates>,
) -> Result<TableCounts> {
    let walk = gates.map(RepairWalkGates::from);

    let Some(bounds) = bounds else {
        return Ok(TableCounts {
            partition_count: 0,
            total_rows: None,
        });
    };

    let modern_histogram = walk.map(|w| w.modern_histogram).unwrap_or(false);
    let mut c = Cursor::new(&input[bounds.start..bounds.end]);

    // 1. estimatedPartitionSize histogram: read it (don't skip) so we can sum the
    //    per-partition bucket counts. Σ counts = partition count (authoritative).
    let partition_count = sum_estimated_histogram_counts(&mut c)?;

    // 2. estimatedCellPerPartitionCount histogram — skip.
    skip_estimated_histogram(&mut c)?;

    // Without authoritative gates the path to totalRows (the gated min/max block)
    // cannot be traversed; report partition_count alone and total_rows = None.
    let Some(walk) = walk else {
        return Ok(TableCounts {
            partition_count,
            total_rows: None,
        });
    };

    // 3-8. commitLogUpperBound, timestamps, deletion times, TTLs, compressionRatio,
    //       tombstone histogram — same fixed/gated skips as the repair walk.
    c.skip(8 + 4)?; // commitLogUpperBound: i64 segmentId + i32 position
    c.skip(8 + 8)?; // minTimestamp, maxTimestamp
    c.skip(4 + 4)?; // min/maxLocalDeletionTime
    c.skip(4 + 4)?; // minTTL, maxTTL
    c.skip(8)?; // compressionRatio (f64)
    skip_tombstone_histogram(&mut c, modern_histogram)?;

    // 9. sstableLevel (i32), repairedAt (i64).
    c.skip(4 + 8)?;

    // 10. version-gated min/max clustering block. The improvedMinMax covered-Slice
    //     bound values are not always modeled; when they cannot be skipped, totalRows
    //     is unreachable and reported honestly as None.
    if walk.has_improved_min_max {
        if !try_skip_improved_min_max(&mut c)? {
            return Ok(TableCounts {
                partition_count,
                total_rows: None,
            });
        }
    } else if walk.has_legacy_min_max {
        skip_legacy_min_max(&mut c)?;
    }

    // 11. hasLegacyCounterShards (bool).
    c.skip(1)?;

    // 12. totalColumnsSet (long), then totalRows (long) — READ totalRows.
    c.skip(8)?; // totalColumnsSet
    let total_rows = c.read_i64()?;
    let total_rows = u64::try_from(total_rows).ok();

    Ok(TableCounts {
        partition_count,
        total_rows,
    })
}

/// Read an `EstimatedHistogram` and return the SUM of its bucket counts (`i64`
/// each). Layout: `i32 bucketCount`, then `bucketCount` × (`i64 offset`,
/// `i64 count`). Σ counts = number of observations (one per partition for the
/// estimatedPartitionSize histogram). Saturating add: a real SSTable never
/// overflows u64, but never wrap into a tiny count that would mislead the gate.
fn sum_estimated_histogram_counts(c: &mut Cursor) -> Result<u64> {
    let count = c.read_i32()?;
    if count < 0 {
        return Err(Error::Corruption(format!(
            "negative EstimatedHistogram bucket count {count}"
        )));
    }
    let mut sum: u64 = 0;
    for _ in 0..count {
        let _offset = c.read_i64()?;
        let bucket = c.read_i64()?;
        if bucket < 0 {
            return Err(Error::Corruption(format!(
                "negative EstimatedHistogram bucket value {bucket}"
            )));
        }
        sum = sum.saturating_add(bucket as u64);
    }
    Ok(sum)
}

/// Cassandra's canonical "no deletion" local-deletion-time sentinel,
/// normalized to `i64` for both legacy (nb) and modern (oa/da) encodings.
///
/// `LivenessInfo.NO_DELETION_TIME` is `Cell.MAX_DELETION_TIME` =
/// `Integer.MAX_VALUE` for legacy (signed `i32`) SSTables, and `0xFFFF_FFFF`
/// (the `u32` saturation value) for modern SSTables that store the field as an
/// unsigned 32-bit integer. `sstablemetadata` prints `Long.MAX_VALUE`
/// (`9223372036854775807`) for "no tombstones"; we normalize to that here so
/// the decoded `max_local_deletion_time` matches the reference tool's printed
/// integer in BOTH cases.
const NO_DELETION_TIME: i64 = i64::MAX;

/// Additional STATS-component facts decoded by [`parse_stats_extras`] that are
/// not part of the repair-coordination metadata: the SSTable's maximum local
/// deletion time and the estimated tombstone-drop-times histogram.
///
/// Decoded best-effort: a `Statistics.db` that parses today must keep parsing,
/// so callers treat an `Err` from [`parse_stats_extras`] as "leave the existing
/// placeholders" rather than propagating it.
#[derive(Debug, Clone)]
pub struct StatsExtras {
    /// SSTable `maxLocalDeletionTime`, normalized so that the version-specific
    /// "no tombstones" sentinel (`i32::MAX` for nb, `0xFFFF_FFFF` for oa/da)
    /// maps to [`NO_DELETION_TIME`] (`i64::MAX`); any real deletion time is the
    /// seconds-since-epoch value as `i64`.
    pub max_local_deletion_time: i64,
    /// `estimatedTombstoneDropTime` histogram as `(point, count)` pairs. Empty
    /// when the histogram carries no bins (the common case for SSTables with no
    /// tombstones).
    pub tombstone_drop_times: Vec<(i64, u64)>,
    /// Authoritative SSTable `maxTimestamp` (write timestamp, microseconds)
    /// decoded from STATS field 4 (issue #1729). `None` when the SSTable
    /// carries no live timestamp — Cassandra's `MetadataCollector` seeds the
    /// tracker with `Long.MIN_VALUE`, so an on-disk value of [`i64::MIN`] is the
    /// "no timestamps recorded" sentinel and is surfaced fail-closed as `None`
    /// (consumers that require a real max — e.g. the #1388 drop gate — must NOT
    /// proceed as if a max were known). Any other value is the true maximum.
    ///
    /// This intentionally replaces the historical `max_timestamp = min_timestamp`
    /// placeholder in [`crate::parser::enhanced_statistics_parser`], which
    /// silently reported the MIN write timestamp as the max.
    pub max_timestamp: Option<i64>,
    /// Authoritative SSTable `maxTTL` (seconds) decoded from STATS field 9
    /// (issue #1537). Cassandra's `MetadataCollector` seeds the TTL tracker with
    /// `Cell.NO_TTL` (`0`) and updates it only from EXPIRING cells, so a maxTTL of
    /// `0` means "no expiring cells" and is surfaced fail-closed as `None`. Any
    /// positive value is the true maximum TTL across the SSTable's expiring cells.
    /// The compaction TTL-expiry LDT floor (issue #1537,
    /// [`crate::storage::write_engine::merge::compute_expiry_ttl_ldt_floor`]) needs
    /// this authoritative maximum: the enhanced (nb) parser only decodes the
    /// EncodingStats `minTTL` baseline and honestly leaves `max_ttl = None`, so
    /// this best-effort STATS-extras walk is the only source of the true maximum.
    pub max_ttl: Option<i64>,
}

impl Default for StatsExtras {
    /// The default reported when no STATS component is locatable: "no
    /// tombstones" max-LDT ([`NO_DELETION_TIME`]), an empty histogram, and no
    /// authoritative `maxTimestamp` (fail-closed `None`).
    fn default() -> Self {
        StatsExtras {
            max_local_deletion_time: NO_DELETION_TIME,
            tombstone_drop_times: Vec::new(),
            max_timestamp: None,
            max_ttl: None,
        }
    }
}

/// Decode the SSTable `maxLocalDeletionTime` and the estimated
/// tombstone-drop-times histogram from a raw `Statistics.db` buffer.
///
/// This walks the same self-describing leading STATS fields as
/// [`parse_repair_metadata`], but reads (rather than skips) field 5
/// (min/maxLocalDeletionTime) and field 8 (the tombstone histogram).
///
/// When `gates` is `None`, the legacy (nb) encoding is assumed for BOTH the
/// max-LDT sentinel comparison and the histogram entry width (nb-compatible
/// default, matching the rest of the minimal Statistics parser and the
/// `merge.rs` caller).
///
/// # Returns
///
/// * `Ok(StatsExtras::default())` — max-LDT = [`NO_DELETION_TIME`], empty
///   histogram — when the buffer carries no STATS component (nothing to decode).
/// * `Ok(extras)` with the decoded facts otherwise.
///
/// # Errors
///
/// Returns an error when the STATS component is present but its leading
/// (self-describing) fields are truncated/corrupt or overrun the component's
/// authoritative end bound. Best-effort callers must treat this as "leave the
/// existing placeholders" and MUST NOT propagate it (a `Statistics.db` that
/// parses today must keep parsing).
pub fn parse_stats_extras(input: &[u8], gates: Option<&VersionGates>) -> Result<StatsExtras> {
    parse_stats_extras_from_bounds(input, stats_component_bounds(input)?, gates)
}

/// [`parse_stats_extras`] over a `Statistics.db` TOC that was already parsed once
/// (issue #2148): reuses the STATS bounds from `toc` instead of re-walking the
/// TOC, so a single metadata parse walks the TOC exactly once.
pub(crate) fn parse_stats_extras_with_toc(
    input: &[u8],
    toc: &StatisticsToc,
    gates: Option<&VersionGates>,
) -> Result<StatsExtras> {
    parse_stats_extras_from_bounds(input, toc.stats_bounds()?, gates)
}

fn parse_stats_extras_from_bounds(
    input: &[u8],
    bounds: Option<StatsComponentBounds>,
    gates: Option<&VersionGates>,
) -> Result<StatsExtras> {
    let Some(bounds) = bounds else {
        // No STATS component → nothing to decode. Report the canonical
        // "no tombstones" default rather than failing.
        return Ok(StatsExtras::default());
    };

    let modern = gates.map(uses_modern_tombstone_histogram).unwrap_or(false);

    // Bound the cursor over ONLY the STATS component slice (start..end, CRC and
    // following components excluded). Any read past this slice fails closed.
    let mut c = Cursor::new(&input[bounds.start..bounds.end]);

    // 1-2. estimatedPartitionSize + estimatedCellPerPartitionCount.
    skip_estimated_histogram(&mut c)?;
    skip_estimated_histogram(&mut c)?;

    // 3. commitLogUpperBound: i64 segmentId + i32 position.
    c.skip(8 + 4)?;

    // 4. minTimestamp, maxTimestamp (both i64 BE microseconds). Read (rather
    //    than skip) maxTimestamp: it is the authoritative SSTable max write
    //    timestamp (issue #1729). Both encodings (nb / oa / da) lay these out
    //    identically here, so no version gating is needed for this field.
    c.skip(8)?; // minTimestamp
    let raw_max_timestamp = c.read_i64()?;
    let max_timestamp = decode_max_timestamp(raw_max_timestamp);

    // 5. min/maxLocalDeletionTime. Width is 4 bytes each in both encodings; the
    //    signedness/sentinel differs by version (handled below).
    let _min_ldt = c.read_u32()?;
    let raw_max_ldt = c.read_u32()?;
    let max_local_deletion_time = decode_max_local_deletion_time(raw_max_ldt, modern);

    // 6. minTTL (skip), maxTTL (read). Both are `int` (4 bytes BE) in every
    //    encoding (nb / oa / da). maxTTL is the authoritative maximum TTL across
    //    the SSTable's expiring cells (issue #1537); `0` (Cassandra `Cell.NO_TTL`,
    //    the tracker seed) means "no expiring cells" and is surfaced as `None`.
    c.skip(4)?; // minTTL
    let raw_max_ttl = c.read_i32()?;
    let max_ttl = if raw_max_ttl > 0 {
        Some(i64::from(raw_max_ttl))
    } else {
        None
    };

    // 7. compressionRatio (f64).
    c.read_f64()?;

    // 8. estimatedTombstoneDropTime (TombstoneHistogram).
    let tombstone_drop_times = read_tombstone_histogram(&mut c, modern)?;

    Ok(StatsExtras {
        max_local_deletion_time,
        tombstone_drop_times,
        max_timestamp,
        max_ttl,
    })
}

/// Normalize a raw 4-byte `maxLocalDeletionTime` to a canonical `i64`.
///
/// * modern (oa/da): the field is an unsigned `u32`; the sentinel is
///   `0xFFFF_FFFF`.
/// * legacy (nb): the field is a signed `i32`; the sentinel is `i32::MAX`.
///
/// The version sentinel maps to [`NO_DELETION_TIME`] (`i64::MAX`); any real
/// deletion time (always `< 2^31` seconds-since-epoch) is returned as its
/// integer value.
/// Cassandra's `MetadataCollector` seeds its timestamp `MinMaxLongTracker` with
/// `Long.MIN_VALUE` for the max; an SSTable that recorded no live write
/// timestamp serializes that sentinel verbatim to STATS field 4. We treat it as
/// "no authoritative maxTimestamp" rather than a real (nonsensical) maximum.
pub(crate) const NO_MAX_TIMESTAMP_SENTINEL: i64 = i64::MIN;

/// Normalize a raw STATS `maxTimestamp` (i64 BE microseconds) into an
/// authoritative value, or `None` when the SSTable recorded no write timestamp.
///
/// The `Long.MIN_VALUE` sentinel maps to `None` (fail-closed: consumers that
/// require a real max must not proceed as if one were known). Every other value
/// — including a valid large or negative real timestamp — is returned verbatim.
///
/// Shared by both parsers (enhanced nb walk here, and the legacy fixed-width
/// `parse_timestamp_statistics`, issue #1653) so the sentinel→`None` mapping
/// never drifts between paths.
pub(crate) fn decode_max_timestamp(raw: i64) -> Option<i64> {
    if raw == NO_MAX_TIMESTAMP_SENTINEL {
        None
    } else {
        Some(raw)
    }
}

fn decode_max_local_deletion_time(raw: u32, modern: bool) -> i64 {
    if modern {
        if raw == u32::MAX {
            NO_DELETION_TIME
        } else {
            raw as i64
        }
    } else {
        let signed = raw as i32;
        if signed == i32::MAX {
            NO_DELETION_TIME
        } else {
            signed as i64
        }
    }
}

/// Read a `TombstoneHistogram` as `(point, count)` pairs.
///
/// Header: `i32 maxBinSize`, `i32 size` (bin count, must be `>= 0`). Each bin is
/// `f64 point + i64 value` (16 bytes) for legacy (nb), or `i64 point + i32 value`
/// (12 bytes) for modern (oa/da). The legacy `f64` point is cast to `i64`
/// (Cassandra rounds drop-times to integer bucket boundaries, so the cast is
/// exact). An empty histogram yields an empty `Vec`.
fn read_tombstone_histogram(c: &mut Cursor, modern: bool) -> Result<Vec<(i64, u64)>> {
    let _max_bin_size = c.read_i32()?;
    let size = c.read_i32()?;
    if size < 0 {
        return Err(Error::Corruption(format!(
            "negative TombstoneHistogram size {size}"
        )));
    }
    // Bound the allocation by the bytes actually present BEFORE reserving:
    // a corrupt Statistics.db can advertise a huge positive `size`, and
    // `Vec::with_capacity(size)` would otherwise attempt a massive (or aborting)
    // allocation. Each bin is `entry_width` bytes; validate that all bins fit in
    // the remaining cursor span (fail-closed via `need`) so the reserve below is
    // capped by real data, not the untrusted header.
    let entry_width = if modern { 12usize } else { 16usize };
    let required = (size as usize)
        .checked_mul(entry_width)
        .ok_or_else(|| Error::Corruption("TombstoneHistogram size overflow".to_string()))?;
    c.need(required)?;
    let mut out = Vec::with_capacity(size as usize);
    for _ in 0..size {
        if modern {
            let point = c.read_i64()?;
            let value = c.read_i32()?;
            out.push((point, value as u64));
        } else {
            let point = c.read_f64()?;
            let value = c.read_i64()?;
            out.push((point as i64, value as u64));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::version_gate::BigVersionGates;

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

    /// Build a STATS component (through `repairedAt`) wrapped in a 4-component
    /// TOC (STATS last), with caller-controlled `maxLocalDeletionTime` (raw
    /// 4-byte field, written verbatim) and tombstone-histogram bins. The bins
    /// are written using the legacy (16-byte: `f64 point + i64 value`) or
    /// modern (12-byte: `i64 point + i32 value`) layout per `modern`.
    fn synthetic_statistics_extras(modern: bool, raw_max_ldt: u32, bins: &[(i64, u64)]) -> Vec<u8> {
        // Default fixture: minTimestamp=100, maxTimestamp=200.
        synthetic_statistics_extras_ts(modern, raw_max_ldt, bins, 100, 200, 0)
    }

    /// Same as [`synthetic_statistics_extras`] but with explicit min/max write
    /// timestamps (STATS field 4), for exercising `decode_max_timestamp` (#1729),
    /// and an explicit `max_ttl` (STATS field 9, issue #1537).
    fn synthetic_statistics_extras_ts(
        modern: bool,
        raw_max_ldt: u32,
        bins: &[(i64, u64)],
        min_timestamp: i64,
        max_timestamp: i64,
        max_ttl: i32,
    ) -> Vec<u8> {
        let mut stats = Vec::new();
        // estimatedPartitionSize + estimatedCellPerPartitionCount (empty).
        stats.extend_from_slice(&0i32.to_be_bytes());
        stats.extend_from_slice(&0i32.to_be_bytes());
        // commitLogUpperBound: i64 segmentId + i32 position.
        stats.extend_from_slice(&(-1i64).to_be_bytes());
        stats.extend_from_slice(&0i32.to_be_bytes());
        // minTimestamp, maxTimestamp.
        stats.extend_from_slice(&min_timestamp.to_be_bytes());
        stats.extend_from_slice(&max_timestamp.to_be_bytes());
        // minLocalDeletionTime (any), maxLocalDeletionTime (verbatim raw u32).
        stats.extend_from_slice(&0u32.to_be_bytes());
        stats.extend_from_slice(&raw_max_ldt.to_be_bytes());
        // minTTL, maxTTL.
        stats.extend_from_slice(&0i32.to_be_bytes());
        stats.extend_from_slice(&max_ttl.to_be_bytes());
        // compressionRatio (f64).
        stats.extend_from_slice(&(-1.0f64).to_be_bytes());
        // TombstoneHistogram: maxBinSize, size, then bins.
        stats.extend_from_slice(&100i32.to_be_bytes()); // maxBinSize
        stats.extend_from_slice(&(bins.len() as i32).to_be_bytes());
        for &(point, value) in bins {
            if modern {
                stats.extend_from_slice(&point.to_be_bytes()); // i64 point
                stats.extend_from_slice(&(value as i32).to_be_bytes()); // i32 value
            } else {
                stats.extend_from_slice(&(point as f64).to_be_bytes()); // f64 point
                stats.extend_from_slice(&(value as i64).to_be_bytes()); // i64 value
            }
        }
        // sstableLevel, repairedAt.
        stats.extend_from_slice(&0i32.to_be_bytes());
        stats.extend_from_slice(&0i64.to_be_bytes());

        // TOC: 4 components, STATS (type 2) last by offset.
        let toc_len = 4 + 4 + 4 * 8;
        let comp0_off = toc_len;
        let comp1_off = comp0_off + 1;
        let comp3_off = comp1_off + 1;
        let stats_off = comp3_off + 1;
        let mut out = Vec::new();
        out.extend_from_slice(&4u32.to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        for (ty, off) in [
            (0u32, comp0_off as u32),
            (1u32, comp1_off as u32),
            (2u32, stats_off as u32),
            (3u32, comp3_off as u32),
        ] {
            out.extend_from_slice(&ty.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }
        out.extend_from_slice(&[0u8, 0u8, 0u8]);
        debug_assert_eq!(out.len(), stats_off);
        out.extend_from_slice(&stats);
        out.extend_from_slice(&0u32.to_be_bytes()); // trailing CRC
        out
    }

    fn nb_gates() -> VersionGates {
        VersionGates::Big(BigVersionGates::from_version("nb").expect("nb gates"))
    }

    fn oa_gates() -> VersionGates {
        VersionGates::Big(BigVersionGates::from_version("oa").expect("oa gates"))
    }

    fn da_gates() -> VersionGates {
        use crate::storage::sstable::version_gate::BtiVersionGates;
        VersionGates::Bti(BtiVersionGates::from_version("da").expect("da gates"))
    }

    /// Append an unsigned VInt (Cassandra `VIntCoding.writeUnsignedVInt`) so the
    /// full-walk synthetic builders match the bytes [`Cursor::read_unsigned_vint`]
    /// consumes. Values used by these tests fit in `< 128`, encoded as one byte.
    fn push_uvint(buf: &mut Vec<u8>, v: u64) {
        assert!(
            v < 0x80,
            "test uvint helper only handles single-byte values"
        );
        buf.push(v as u8);
    }

    /// Build a complete STATS body through `isTransient` for the legacy (`nb`)
    /// layout (legacy min/max clustering lists; pendingRepair UUID + transient
    /// flag carried by `na`+). Wrapped in a STATS-last 2-component TOC + CRC.
    fn synthetic_full_nb(
        repaired_at: i64,
        pending_repair: Option<[u8; 16]>,
        is_transient: bool,
    ) -> Vec<u8> {
        let mut stats = Vec::new();
        stats.extend_from_slice(&0i32.to_be_bytes()); // estimatedPartitionSize
        stats.extend_from_slice(&0i32.to_be_bytes()); // estimatedCellPerPartitionCount
        stats.extend_from_slice(&(-1i64).to_be_bytes()); // commitLogUpperBound segmentId
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogUpperBound position
        stats.extend_from_slice(&100i64.to_be_bytes()); // minTimestamp
        stats.extend_from_slice(&200i64.to_be_bytes()); // maxTimestamp
        stats.extend_from_slice(&i32::MAX.to_be_bytes()); // minLocalDeletionTime
        stats.extend_from_slice(&i32::MAX.to_be_bytes()); // maxLocalDeletionTime
        stats.extend_from_slice(&0i32.to_be_bytes()); // minTTL
        stats.extend_from_slice(&0i32.to_be_bytes()); // maxTTL
        stats.extend_from_slice(&(-1.0f64).to_be_bytes()); // compressionRatio
        stats.extend_from_slice(&0i32.to_be_bytes()); // TombstoneHistogram maxBinSize
        stats.extend_from_slice(&0i32.to_be_bytes()); // TombstoneHistogram size
        stats.extend_from_slice(&0i32.to_be_bytes()); // sstableLevel
        stats.extend_from_slice(&repaired_at.to_be_bytes()); // repairedAt
                                                             // legacy min/max clustering lists (both empty: count 0).
        stats.extend_from_slice(&0i32.to_be_bytes());
        stats.extend_from_slice(&0i32.to_be_bytes());
        stats.push(0x00); // hasLegacyCounterShards
        stats.extend_from_slice(&0u64.to_be_bytes()); // totalColumnsSet
        stats.extend_from_slice(&0u64.to_be_bytes()); // totalRows
        stats.extend_from_slice(&(-1i64).to_be_bytes()); // commitLogLowerBound segmentId
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogLowerBound position
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogIntervals size = 0
                                                      // pendingRepair: presence byte + optional UUID.
        match pending_repair {
            Some(uuid) => {
                stats.push(0x01);
                stats.extend_from_slice(&uuid);
            }
            None => stats.push(0x00),
        }
        stats.push(if is_transient { 0x01 } else { 0x00 }); // isTransient
        wrap_stats_last(stats)
    }

    /// Build a complete STATS body through `isTransient` for the `da` (BtiFormat)
    /// layout (improvedMinMax: clusteringTypes + covered Slice). One clustering
    /// type and an empty covered slice exercise the improved-min/max walk.
    fn synthetic_full_da(
        repaired_at: i64,
        pending_repair: Option<[u8; 16]>,
        is_transient: bool,
    ) -> Vec<u8> {
        let mut stats = Vec::new();
        stats.extend_from_slice(&0i32.to_be_bytes()); // estimatedPartitionSize
        stats.extend_from_slice(&0i32.to_be_bytes()); // estimatedCellPerPartitionCount
        stats.extend_from_slice(&(-1i64).to_be_bytes()); // commitLogUpperBound segmentId
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogUpperBound position
        stats.extend_from_slice(&100i64.to_be_bytes()); // minTimestamp
        stats.extend_from_slice(&200i64.to_be_bytes()); // maxTimestamp
        stats.extend_from_slice(&u32::MAX.to_be_bytes()); // minLocalDeletionTime (uint)
        stats.extend_from_slice(&u32::MAX.to_be_bytes()); // maxLocalDeletionTime (uint)
        stats.extend_from_slice(&0i32.to_be_bytes()); // minTTL
        stats.extend_from_slice(&0i32.to_be_bytes()); // maxTTL
        stats.extend_from_slice(&(-1.0f64).to_be_bytes()); // compressionRatio
        stats.extend_from_slice(&0i32.to_be_bytes()); // TombstoneHistogram maxBinSize (modern)
        stats.extend_from_slice(&0i32.to_be_bytes()); // TombstoneHistogram size (modern)
        stats.extend_from_slice(&0i32.to_be_bytes()); // sstableLevel
        stats.extend_from_slice(&repaired_at.to_be_bytes()); // repairedAt
                                                             // improvedMinMax: clusteringTypes list (1 type) + covered Slice.
        push_uvint(&mut stats, 1); // clusteringTypes count
        let ty = b"org.apache.cassandra.db.marshal.Int32Type";
        push_uvint(&mut stats, ty.len() as u64);
        stats.extend_from_slice(ty);
        // covered Slice: start bound (kind=1, size=0), end bound (kind=6, size=0).
        stats.push(1);
        stats.extend_from_slice(&0u16.to_be_bytes());
        stats.push(6);
        stats.extend_from_slice(&0u16.to_be_bytes());
        stats.push(0x00); // hasLegacyCounterShards
        stats.extend_from_slice(&0u64.to_be_bytes()); // totalColumnsSet
        stats.extend_from_slice(&0u64.to_be_bytes()); // totalRows
        stats.extend_from_slice(&(-1i64).to_be_bytes()); // commitLogLowerBound segmentId
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogLowerBound position
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogIntervals size = 0
        match pending_repair {
            Some(uuid) => {
                stats.push(0x01);
                stats.extend_from_slice(&uuid);
            }
            None => stats.push(0x00),
        }
        stats.push(if is_transient { 0x01 } else { 0x00 }); // isTransient
        wrap_stats_last(stats)
    }

    /// Wrap a STATS body in a 2-component TOC (STATS type 2 last by offset) plus a
    /// trailing 4-byte CRC, so the decoder's end bound is `file_len - CRC`.
    fn wrap_stats_last(stats: Vec<u8>) -> Vec<u8> {
        let toc_len = 4 + 4 + 2 * 8;
        let comp0_off = toc_len;
        let stats_off = comp0_off + 1;
        let mut out = Vec::new();
        out.extend_from_slice(&2u32.to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        for (ty, off) in [(0u32, comp0_off as u32), (2u32, stats_off as u32)] {
            out.extend_from_slice(&ty.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }
        out.push(0u8); // placeholder body for comp0
        debug_assert_eq!(out.len(), stats_off);
        out.extend_from_slice(&stats);
        out.extend_from_slice(&0u32.to_be_bytes()); // trailing CRC
        out
    }

    #[test]
    fn full_walk_nb_decodes_null_pending_and_transient() {
        let bytes = synthetic_full_nb(0, None, false);
        let md = parse_repair_metadata(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(md.repaired_at, 0);
        assert!(md.repaired_at_decoded);
        assert_eq!(md.pending_repair, RepairField::Decoded(None));
        assert_eq!(md.is_transient, RepairField::Decoded(false));
    }

    #[test]
    fn full_walk_nb_decodes_pending_uuid_and_transient_true() {
        let uuid = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00,
        ];
        let bytes = synthetic_full_nb(1_700_000_000_000, Some(uuid), true);
        let md = parse_repair_metadata(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(md.repaired_at, 1_700_000_000_000);
        assert_eq!(md.pending_repair, RepairField::Decoded(Some(uuid)));
        assert_eq!(md.is_transient, RepairField::Decoded(true));
    }

    #[test]
    fn full_walk_da_decodes_pending_uuid_and_transient() {
        let uuid = [0xABu8; 16];
        let bytes = synthetic_full_da(42, Some(uuid), true);
        let md = parse_repair_metadata(&bytes, Some(&da_gates())).expect("decode");
        assert_eq!(md.repaired_at, 42);
        assert_eq!(md.pending_repair, RepairField::Decoded(Some(uuid)));
        assert_eq!(md.is_transient, RepairField::Decoded(true));
    }

    #[test]
    fn full_walk_da_decodes_null_pending_non_transient() {
        let bytes = synthetic_full_da(0, None, false);
        let md = parse_repair_metadata(&bytes, Some(&da_gates())).expect("decode");
        assert_eq!(md.pending_repair, RepairField::Decoded(None));
        assert_eq!(md.is_transient, RepairField::Decoded(false));
    }

    #[test]
    fn full_walk_none_gates_reports_unparsed() {
        // Without gates the walk past repairedAt is not attempted, so the two
        // later fields are reported honestly as Unparsed (not fabricated).
        let bytes = synthetic_full_nb(7, Some([0x01u8; 16]), true);
        let md = parse_repair_metadata(&bytes, None).expect("decode repairedAt only");
        assert_eq!(md.repaired_at, 7);
        assert_eq!(md.pending_repair, RepairField::Unparsed);
        assert_eq!(md.is_transient, RepairField::Unparsed);
    }

    #[test]
    fn full_walk_truncated_pending_fails_closed() {
        // Drop the trailing CRC + isTransient + part of the UUID so the
        // pendingRepair UUID read overruns the bounded STATS slice.
        let mut bytes = synthetic_full_nb(0, Some([0x07u8; 16]), true);
        bytes.truncate(bytes.len() - (4 + 1 + 8));
        assert!(
            parse_repair_metadata(&bytes, Some(&nb_gates())).is_err(),
            "a truncated pendingRepair UUID must fail closed, not default"
        );
    }

    #[test]
    fn read_unsigned_vint_matches_encode_vuint() {
        use crate::parser::vint::encode_vuint;
        for v in [0u64, 1, 63, 127, 128, 255, 300, 16383, 16384, 1_000_000] {
            let enc = encode_vuint(v);
            let mut c = Cursor::new(&enc);
            assert_eq!(c.read_unsigned_vint().expect("decode vint"), v, "v={v}");
        }
    }

    #[test]
    fn stats_extras_real_max_ldt_nb() {
        // Real seconds-since-epoch deletion time, well under 2^31.
        let bytes = synthetic_statistics_extras(false, 1_700_000_000, &[]);
        let extras = parse_stats_extras(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(extras.max_local_deletion_time, 1_700_000_000);
        assert!(extras.tombstone_drop_times.is_empty());
        // Default fixture: maxTimestamp=200 (NOT the min=100 placeholder). #1729.
        assert_eq!(extras.max_timestamp, Some(200));
        // Default fixture: maxTTL=0 (no expiring cells) → None (#1537).
        assert_eq!(extras.max_ttl, None);
    }

    #[test]
    fn stats_extras_decodes_authoritative_max_ttl() {
        // Issue #1537: the STATS-extras walk must surface the authoritative maxTTL
        // (field 9) so the compaction TTL-expiry LDT floor can lower the output's
        // min-LDT baseline below a creation-time (`ldt - ttl`) tombstone. A positive
        // maxTTL is the true max; `0` (Cassandra `Cell.NO_TTL`) means "no expiring
        // cells" and must surface as `None`.
        let bytes =
            synthetic_statistics_extras_ts(false, i32::MAX as u32, &[], 100, 200, 10_000_000);
        let extras = parse_stats_extras(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(
            extras.max_ttl,
            Some(10_000_000),
            "authoritative maxTTL must be decoded from STATS field 9"
        );

        // maxTTL == 0 → no expiring cells → None (fail-closed).
        let none_bytes = synthetic_statistics_extras_ts(false, i32::MAX as u32, &[], 100, 200, 0);
        let none_extras = parse_stats_extras(&none_bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(
            none_extras.max_ttl, None,
            "maxTTL of 0 (no expiring cells) must surface as None"
        );
    }

    #[test]
    fn stats_extras_decodes_authoritative_max_timestamp_not_min() {
        // Acceptance criterion #3: min < X < max, parsed max must equal the
        // true max (not the min placeholder the enhanced parser used to emit).
        let min_ts = 1_000_000i64;
        let max_ts = 5_000_000i64;
        let bytes = synthetic_statistics_extras_ts(false, i32::MAX as u32, &[], min_ts, max_ts, 0);
        let extras = parse_stats_extras(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(
            extras.max_timestamp,
            Some(max_ts),
            "parsed max_timestamp must be the true max, not the min"
        );
        assert_ne!(
            extras.max_timestamp,
            Some(min_ts),
            "max_timestamp must NOT equal the min placeholder"
        );
    }

    #[test]
    fn stats_extras_max_timestamp_long_min_sentinel_is_none() {
        // Fail-closed (#1729): Cassandra seeds the max tracker with
        // Long.MIN_VALUE; an SSTable with no recorded write timestamp
        // serializes that sentinel → surfaced as None, never a bogus max.
        let bytes =
            synthetic_statistics_extras_ts(false, i32::MAX as u32, &[], i64::MAX, i64::MIN, 0);
        let extras = parse_stats_extras(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(extras.max_timestamp, None);
    }

    #[test]
    fn stats_extras_default_max_timestamp_is_none() {
        // No STATS component → fail-closed None (no authoritative max).
        let extras = StatsExtras::default();
        assert_eq!(extras.max_timestamp, None);
    }

    #[test]
    fn stats_extras_nb_sentinel_maps_to_i64_max() {
        // nb sentinel: i32::MAX → i64::MAX (NO_DELETION_TIME).
        let bytes = synthetic_statistics_extras(false, i32::MAX as u32, &[]);
        let extras = parse_stats_extras(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(extras.max_local_deletion_time, i64::MAX);
        // gates == None also defaults to legacy → same result.
        let extras_none = parse_stats_extras(&bytes, None).expect("decode");
        assert_eq!(extras_none.max_local_deletion_time, i64::MAX);
    }

    #[test]
    fn stats_extras_modern_sentinel_maps_to_i64_max() {
        // modern sentinel: 0xFFFF_FFFF → i64::MAX (NO_DELETION_TIME).
        let bytes = synthetic_statistics_extras(true, u32::MAX, &[]);
        let extras = parse_stats_extras(&bytes, Some(&oa_gates())).expect("decode");
        assert_eq!(extras.max_local_deletion_time, i64::MAX);
    }

    #[test]
    fn stats_extras_modern_real_max_ldt() {
        // A real deletion time above i32::MAX is representable as u32 on modern.
        let raw: u32 = 3_000_000_000; // > i32::MAX, < u32::MAX
        let bytes = synthetic_statistics_extras(true, raw, &[]);
        let extras = parse_stats_extras(&bytes, Some(&oa_gates())).expect("decode");
        assert_eq!(extras.max_local_deletion_time, raw as i64);
    }

    #[test]
    fn stats_extras_histogram_nb_width() {
        let bins = [(1_700_000_000i64, 3u64), (1_700_000_100i64, 7u64)];
        let bytes = synthetic_statistics_extras(false, i32::MAX as u32, &bins);
        let extras = parse_stats_extras(&bytes, Some(&nb_gates())).expect("decode");
        assert_eq!(
            extras.tombstone_drop_times,
            vec![(1_700_000_000, 3), (1_700_000_100, 7)]
        );
    }

    #[test]
    fn stats_extras_histogram_modern_width() {
        let bins = [(1_700_000_000i64, 11u64), (1_700_000_500i64, 2u64)];
        let bytes = synthetic_statistics_extras(true, u32::MAX, &bins);
        let extras = parse_stats_extras(&bytes, Some(&oa_gates())).expect("decode");
        assert_eq!(
            extras.tombstone_drop_times,
            vec![(1_700_000_000, 11), (1_700_000_500, 2)]
        );
    }

    #[test]
    fn stats_extras_empty_histogram() {
        let bytes = synthetic_statistics_extras(false, i32::MAX as u32, &[]);
        let extras = parse_stats_extras(&bytes, Some(&nb_gates())).expect("decode");
        assert!(extras.tombstone_drop_times.is_empty());
    }

    #[test]
    fn stats_extras_missing_component_returns_default() {
        // TOC with no STATS entry → default (max = i64::MAX, empty histogram),
        // not an error.
        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_be_bytes()); // 1 component
        out.extend_from_slice(&0u32.to_be_bytes()); // marker
        out.extend_from_slice(&3u32.to_be_bytes()); // type HEADER
        out.extend_from_slice(&16u32.to_be_bytes()); // offset
        let extras = parse_stats_extras(&out, None).expect("default");
        assert_eq!(extras.max_local_deletion_time, i64::MAX);
        assert!(extras.tombstone_drop_times.is_empty());
    }

    #[test]
    fn stats_extras_truncated_fails_closed() {
        // Two bins so the histogram region is large; truncate deep enough that
        // the cursor runs off the end while still reading the histogram (the
        // sstableLevel/repairedAt tail is not read by parse_stats_extras).
        let mut bytes =
            synthetic_statistics_extras(false, 1_700_000_000, &[(1i64, 1u64), (2i64, 2u64)]);
        // Drop the trailing CRC + sstableLevel + repairedAt + one full bin
        // (16 bytes legacy) + part of the next, so a histogram read overruns.
        bytes.truncate(bytes.len() - (4 + 4 + 8 + 16 + 4));
        assert!(
            parse_stats_extras(&bytes, Some(&nb_gates())).is_err(),
            "truncated STATS body must fail closed"
        );
    }

    #[test]
    fn tombstone_histogram_huge_size_fails_closed_without_allocating() {
        // A corrupt header advertising a massive bin count must fail closed via
        // the remaining-bytes precheck rather than attempting a huge (or
        // aborting) `Vec::with_capacity` (roborev High finding, #1073).
        for modern in [false, true] {
            let mut buf = Vec::new();
            buf.extend_from_slice(&100i32.to_be_bytes()); // maxBinSize
            buf.extend_from_slice(&i32::MAX.to_be_bytes()); // size = 2.1B bins, no body
            let mut c = Cursor::new(&buf);
            assert!(
                read_tombstone_histogram(&mut c, modern).is_err(),
                "huge histogram size (modern={modern}) must fail closed, not allocate"
            );
        }
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

    // ---- read_table_counts (issue #944) ----------------------------------

    /// Build an `nb`-layout STATS body with a POPULATED estimatedPartitionSize
    /// histogram (so `partition_count` is non-zero) and a known `totalRows`.
    /// `partition_buckets` are `(offset, count)` pairs; Σ counts = partition_count.
    fn synthetic_counts_nb(partition_buckets: &[(i64, i64)], total_rows: i64) -> Vec<u8> {
        let mut stats = Vec::new();
        // estimatedPartitionSize: i32 bucketCount, then (i64 offset, i64 count)*.
        stats.extend_from_slice(&(partition_buckets.len() as i32).to_be_bytes());
        for (off, cnt) in partition_buckets {
            stats.extend_from_slice(&off.to_be_bytes());
            stats.extend_from_slice(&cnt.to_be_bytes());
        }
        stats.extend_from_slice(&0i32.to_be_bytes()); // estimatedCellPerPartitionCount (empty)
        stats.extend_from_slice(&(-1i64).to_be_bytes()); // commitLogUpperBound segmentId
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogUpperBound position
        stats.extend_from_slice(&100i64.to_be_bytes()); // minTimestamp
        stats.extend_from_slice(&200i64.to_be_bytes()); // maxTimestamp
        stats.extend_from_slice(&i32::MAX.to_be_bytes()); // minLocalDeletionTime
        stats.extend_from_slice(&i32::MAX.to_be_bytes()); // maxLocalDeletionTime
        stats.extend_from_slice(&0i32.to_be_bytes()); // minTTL
        stats.extend_from_slice(&0i32.to_be_bytes()); // maxTTL
        stats.extend_from_slice(&(-1.0f64).to_be_bytes()); // compressionRatio
        stats.extend_from_slice(&0i32.to_be_bytes()); // TombstoneHistogram maxBinSize
        stats.extend_from_slice(&0i32.to_be_bytes()); // TombstoneHistogram size
        stats.extend_from_slice(&0i32.to_be_bytes()); // sstableLevel
        stats.extend_from_slice(&0i64.to_be_bytes()); // repairedAt
        stats.extend_from_slice(&0i32.to_be_bytes()); // legacy min clustering list (empty)
        stats.extend_from_slice(&0i32.to_be_bytes()); // legacy max clustering list (empty)
        stats.push(0x00); // hasLegacyCounterShards
        stats.extend_from_slice(&7u64.to_be_bytes()); // totalColumnsSet (arbitrary)
        stats.extend_from_slice(&total_rows.to_be_bytes()); // totalRows
        stats.extend_from_slice(&(-1i64).to_be_bytes()); // commitLogLowerBound segmentId
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogLowerBound position
        stats.extend_from_slice(&0i32.to_be_bytes()); // commitLogIntervals size = 0
        stats.push(0x00); // pendingRepair absent
        stats.push(0x00); // isTransient
        wrap_stats_last(stats)
    }

    #[test]
    fn read_table_counts_sums_partition_histogram_and_total_rows() {
        // 3 + 2 + 5 = 10 partitions; 2000 total rows (wide table shape).
        let buf = synthetic_counts_nb(&[(0, 3), (16, 2), (64, 5)], 2000);
        let counts = read_table_counts(&buf, Some(&nb_gates())).expect("decode");
        assert_eq!(counts.partition_count, 10, "Σ histogram bucket counts");
        assert_eq!(counts.total_rows, Some(2000), "totalRows from field 12");
    }

    #[test]
    fn read_table_counts_partition_count_without_gates_total_rows_none() {
        // Without gates, partition_count is still decoded (self-describing leading
        // histogram), but the gated walk to totalRows is not attempted.
        let buf = synthetic_counts_nb(&[(0, 4), (8, 6)], 999);
        let counts = read_table_counts(&buf, None).expect("decode");
        assert_eq!(counts.partition_count, 10);
        assert_eq!(
            counts.total_rows, None,
            "no gates → totalRows honestly absent, never guessed"
        );
    }

    /// Issue #2148 blocker (roborev + rust-reviewer): a corrupt STATS byte-range
    /// must NOT discard an already-resolved HEADER offset. The pre-#2148 header
    /// walk (`parse_statistics_toc_for_header_offset`) was independent of any
    /// STATS-range check, so a Statistics.db with a VALID HEADER entry but an
    /// INVALID STATS range still decoded EncodingStats authoritatively from the
    /// HEADER. The single-pass walk must preserve that: the STATS channel fails
    /// closed while `header_offset()` still returns the resolved HEADER — otherwise
    /// EncodingStats silently falls back to the marker search (a different result).
    #[test]
    fn invalid_stats_range_preserves_resolved_header_offset() {
        // 2-component TOC: HEADER (type 3) at a VALID body offset, STATS (type 2)
        // at an offset PAST EOF (an invalid derived range → fail-closed STATS).
        let toc_len = 4 + 4 + 2 * 8; // count + marker + 2 entries = 24
        let header_off = toc_len; // HEADER body starts right after the TOC
        let stats_off_bad = 10_000usize; // far past the buffer end → invalid range

        let mut out = Vec::new();
        out.extend_from_slice(&2u32.to_be_bytes()); // 2 components
        out.extend_from_slice(&0u32.to_be_bytes()); // marker
                                                    // HEADER first (first-wins) with a valid offset, then the bad STATS entry.
        for (ty, off) in [
            (METADATA_TYPE_HEADER, header_off as u32),
            (METADATA_TYPE_STATS, stats_off_bad as u32),
        ] {
            out.extend_from_slice(&ty.to_be_bytes());
            out.extend_from_slice(&off.to_be_bytes());
        }
        debug_assert_eq!(out.len(), header_off);
        out.extend_from_slice(&[0xABu8; 8]); // HEADER body
        out.extend_from_slice(&0u32.to_be_bytes()); // trailing CRC

        let toc = parse_statistics_toc(&out);
        assert_eq!(
            toc.header_offset(),
            Some(header_off),
            "a corrupt STATS range must NOT nuke the already-resolved HEADER offset \
             (issue #2148): EncodingStats must still decode from the HEADER, not the \
             marker fallback"
        );
        assert!(
            toc.stats_bounds().is_err(),
            "the invalid STATS range must STILL fail closed on its own channel"
        );
    }

    #[test]
    fn read_table_counts_no_stats_component_is_all_zero() {
        // A TOC with only a non-STATS component → no counts to decode.
        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_be_bytes()); // 1 component
        out.extend_from_slice(&0u32.to_be_bytes()); // checksum
        out.extend_from_slice(&0u32.to_be_bytes()); // type 0 (VALIDATION), not STATS
        out.extend_from_slice(&16u32.to_be_bytes()); // offset
        out.push(0u8); // body
        out.extend_from_slice(&0u32.to_be_bytes()); // trailing CRC
        let counts = read_table_counts(&out, Some(&nb_gates())).expect("decode");
        assert_eq!(counts.partition_count, 0);
        assert_eq!(counts.total_rows, None);
    }
}
