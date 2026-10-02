//! Corruption-location resolution for [`VerifyFinding`] (issue #4194).
//!
//! This module owns exactly one piece of logic: given a `Data.db` byte range a
//! finding is anchored to, intersect it against a boundary source's own decoded
//! partition extents to name which partitions the damage touches. It is a pure,
//! closed-interval computation — **no byte-pattern scanning of `Data.db` is ever
//! performed here** (no-heuristics mandate, issue #28; mechanically enforced by
//! `scripts/tests/test_verify_location_no_resync_scan.sh`, which runs in the
//! gate's UNSCOPED `roborev-lints` component — `tooling-tests` is diff-scoped
//! and does not declare `cqlite-core/**` (#4266), so a core-only diff would
//! skip it there).
//!
//! # Coordinate spaces (read this before touching call sites)
//!
//! `Index.db`'s `data_offset` and the BTI trie's resolved partition positions
//! both address the **decompressed (logical)** `Data.db` byte stream — Cassandra
//! opens a compressed `Data.db` through a reader that presents a virtual
//! uncompressed view, and both boundary sources were written against that view.
//! For an uncompressed table logical and physical coordinates coincide.
//! [`resolve_partitions`] therefore always operates on a *logical* damaged
//! range; callers computing a chunk's *physical* on-disk byte range (for
//! [`Location::byte_offset`]/[`Location::byte_len`], which are reported for a
//! human to go find the bytes) additionally derive the matching logical range
//! before calling in — see `verify.rs`'s call sites for the per-check
//! derivation (compressed: `chunk_index * chunk_length`; uncompressed: the
//! physical offset, unchanged).
//!
//! [`VerifyFinding`]: super::verify::VerifyFinding
//! [`Location::byte_offset`]: Location::byte_offset
//! [`Location::byte_len`]: Location::byte_len

use std::path::Path;
use std::sync::Arc;

use crate::platform::Platform;
use crate::storage::sstable::verify::{
    BtiResolvedLeaf, ComponentSet, VerifyErrorClass, VerifyFinding,
};
use crate::storage::sstable::version_gate::SsTableFormat;

/// What a [`Location`]'s physical `byte_offset`/`byte_len` actually describe.
///
/// The two readings are not interchangeable: rendering a declared range
/// identically to a damaged one told the operator that 4 bytes were damaged
/// when the real damage is the whole logical tail `partitions` enumerates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalAnchor {
    /// `byte_offset`/`byte_len` cover bytes that are PRESENT in the file and
    /// damaged — a failed chunk CRC, a decompression error. Reading that range
    /// off disk yields the damaged bytes themselves.
    DamagedExtent,
    /// `byte_offset`/`byte_len` are a location the SSTable's OWN metadata
    /// declares that the file does not satisfy: for `ChunkOffsetOutOfBounds`
    /// the declared chunk offset, plus the 4-byte TRAILING inline CRC32 that is
    /// a chunk record's minimum size, does not FIT inside `Data.db`. The test
    /// is `offset + 4 > data_len` (verify.rs), so it ALSO fires when the offset
    /// is itself readable and only the record's tail runs past EOF — hence "does
    /// not fit", never "absent"/"cannot be read", matching `format_location`.
    /// The damaged extent here is the LOGICAL range, which `partitions` names.
    DeclaredRecord,
}

/// Where a [`VerifyFinding`] anchored to a `Data.db` byte range is located, and
/// which partitions that range intersects.
///
/// [`VerifyFinding`]: super::verify::VerifyFinding
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    /// The component the byte range belongs to — always `"Data.db"` today (the
    /// only component with a chunk/offset grid a finding can anchor to).
    pub component: String,
    /// Physical (on-disk) start of the range this finding is ANCHORED to in
    /// `component`. Whether those bytes are damaged, or merely DECLARED by the
    /// SSTable's own metadata and absent from the file, is stated by
    /// [`Location::anchor`] — read it before describing this range to a human
    /// (roborev job 92 MEDIUM: these two fields previously claimed to be "the
    /// damaged range" unconditionally, which for `ChunkOffsetOutOfBounds` named
    /// 4 bytes at an offset PAST EOF beside a 900-partition damage list).
    pub byte_offset: u64,
    /// Physical (on-disk) length of the anchored range in `component`. See
    /// [`Location::byte_offset`] and [`Location::anchor`].
    pub byte_len: u64,
    /// Whether `byte_offset`/`byte_len` describe damaged bytes or a declared
    /// record location that the file does not satisfy.
    pub anchor: PhysicalAnchor,
    /// The chunk index the damage falls in, when `component` has a chunk grid
    /// (compressed `CompressionInfo.db` chunks, or the fixed-size `CRC.db`
    /// grid). `None` for a finding with no chunk grid.
    pub chunk_index: Option<usize>,
    /// The partitions whose `Data.db` extent intersects the damaged range.
    pub partitions: PartitionResolution,
}

/// The largest number of partition keys [`resolve_partitions`] will
/// materialize into `Resolved.keys` (roborev round-2 MEDIUM finding): a
/// truncation's damaged range is `[first_bad_chunk_start, data_length)` —
/// for a badly truncated file that can intersect essentially every partition
/// in the SSTable, and this module's job is precisely to enumerate them by
/// hex key, materializing one `String` (2x key length) per hit. A `Location`
/// is a per-FINDING, not per-file, structure, so nothing else in this module
/// bounds that count. `Resolved.truncated` names how many more intersected
/// but were not materialized, so the report stays affirmative about what it
/// omitted rather than either silently truncating or growing unbounded.
pub const MAX_RESOLVED_KEYS: usize = 100;

/// The outcome of resolving [`Location::partitions`] against a boundary source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionResolution {
    /// The boundary source was healthy and every intersecting partition's raw
    /// key was recoverable. `keys` is empty when no boundary entry intersects
    /// the damaged range (legitimate — e.g. the range falls entirely past the
    /// last known partition); it is capped at [`MAX_RESOLVED_KEYS`], with
    /// `truncated` naming how many additional intersecting partitions exist
    /// beyond the cap (`0` when nothing was omitted).
    Resolved { keys: Vec<KeyRef>, truncated: usize },
    /// The boundary source could not be trusted (or a needed partition key was
    /// unavailable) — the cause is named, never a guess and never a silent
    /// empty list standing in for "unknown".
    Unresolved(String),
}

/// One partition key, as recovered from the boundary source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRef {
    /// The raw partition key's bytes, lower-case hex. Always populated when
    /// the key is known.
    pub key_hex: String,
    /// Reserved for a schema-decoded rendering of the raw key (parity with
    /// `sstable-salvage`'s manifest `key`/`key_hex` split). **Never populated
    /// today** — `KeyRef::from_raw` is the only constructor and always sets
    /// this to `None`; wiring `--schema` support through to this field is a
    /// follow-up, not something already partially implemented (roborev
    /// finding, final round, #4194).
    pub rendered: Option<String>,
}

impl KeyRef {
    fn from_raw(raw: &[u8]) -> Self {
        Self {
            key_hex: hex_encode(raw),
            rendered: None,
        }
    }
}

/// A chunk/offset-anchored finding awaiting location resolution (issue #4194).
///
/// Pushed alongside its finding at the check site — where the physical byte
/// range and the LOGICAL (decompressed) damaged range are cheaply computable
/// from local context (`CompressionInfo`, the CRC.db chunk grid) — because
/// whether the boundary source (`Index.db` / `Partitions.db`) can be TRUSTED
/// is only knowable once every check has run (design.md §D2). Resolved in one
/// pass at the end of `verify_components` by [`finalize_locations`].
///
/// `pub(crate)` fields (#1116/#1135 file-size relocation): constructed at each
/// check site in `verify.rs`, resolved here, so both must cross that boundary.
pub(crate) struct PendingLocation {
    /// Index into `findings` of the finding this location belongs to.
    pub(crate) finding_index: usize,
    /// Component the byte range belongs to — always `"Data.db"` today.
    pub(crate) component: String,
    /// Physical (on-disk) start of the damaged range.
    pub(crate) byte_offset: u64,
    /// Physical (on-disk) length of the damaged range.
    pub(crate) byte_len: u64,
    /// The chunk index the damage falls in, when the component has a chunk grid.
    pub(crate) chunk_index: Option<usize>,
    /// The damaged range in `Data.db` LOGICAL (decompressed) offset space —
    /// the space `Index.db`/the BTI trie address (see this module's doc for
    /// why this differs from the physical range above).
    pub(crate) damaged_logical: (u64, u64),
    /// The boundary source's declared total LOGICAL length, bounding the last
    /// boundary entry's extent.
    pub(crate) logical_len: u64,
    /// Whether the physical range above is a damaged extent or a declared
    /// record location — carried per-finding because it is a property of the
    /// CHECK that produced it, not of the component.
    pub(crate) anchor: PhysicalAnchor,
}

/// Human-readable one-line rendering of a [`Location`] for text output
/// (`VerifyFinding`'s `Display` impl and the CLI's text renderer, issue #4194).
pub fn format_location(loc: &Location) -> String {
    let chunk = loc
        .chunk_index
        .map(|c| format!("chunk {c}, "))
        .unwrap_or_default();
    let partitions = match &loc.partitions {
        PartitionResolution::Resolved { keys, .. } if keys.is_empty() => "0 partitions".to_string(),
        PartitionResolution::Resolved { keys, truncated } => {
            // Issue #4194, roborev round-2 MEDIUM finding: `truncated` names
            // how many more intersecting partitions exist beyond the
            // `MAX_RESOLVED_KEYS` cap, so a badly truncated Data.db's report
            // stays affirmative about what it omitted rather than silently
            // showing a partial list as if it were complete.
            let more = if *truncated > 0 {
                format!(" (+{truncated} more, capped)")
            } else {
                String::new()
            };
            format!(
                "{} partition(s){}: {}",
                keys.len(),
                more,
                keys.iter()
                    .map(|k| k.key_hex.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        PartitionResolution::Unresolved(cause) => format!("partitions unresolved ({cause})"),
    };
    // The physical range is rendered according to what it IS (roborev job 92
    // MEDIUM). A `DeclaredRecord` anchor names an offset the file does not
    // reach, so it is labelled `declared` and explicitly disclosed as absent —
    // rendering it identically to a damaged extent told the operator that 4
    // bytes were damaged when the real damage was the whole logical tail the
    // partition list enumerates.
    let physical = match loc.anchor {
        PhysicalAnchor::DamagedExtent => {
            format!("offset 0x{:x} len {}", loc.byte_offset, loc.byte_len)
        }
        // "does not fit within the file", NOT "not present" (roborev job
        // 108). The out-of-bounds test is `offset + 4 > data_len`, which also
        // fires when the offset itself is INSIDE the file and only the record's
        // tail runs past the end (data_len 1000, offset 998: two of those bytes
        // are readable). Claiming the range is absent would over-state the
        // evidence in exactly the way this anchor exists to prevent.
        PhysicalAnchor::DeclaredRecord => format!(
            "declared offset 0x{:x} len {} (declared by metadata; the record does not fit \
             within the file)",
            loc.byte_offset, loc.byte_len
        ),
    };
    format!("{}: {}{} — {}", loc.component, chunk, physical, partitions)
}

/// Lower-case hex encode, one `write!` per byte rather than one `String`
/// allocation per byte (roborev round-2 LOW finding: `format!` inside the
/// loop is on the path [`MAX_RESOLVED_KEYS`] makes hot — up to 100 calls per
/// `Location`).
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{:02x}", b);
    }
    out
}

/// The cause named when the boundary source (`Index.db` / `Partitions.db`)
/// itself is the corrupt component in the same report — every OTHER location
/// in that report is poisoned by this cause (design.md §D2).
pub const BOUNDARY_SOURCE_UNREADABLE: &str = "boundary-source-unreadable";

/// A partition key was needed to resolve a location but was not recoverable
/// from the data available at resolution time (e.g. a BTI `DataOffset` leaf
/// whose raw key is only recoverable through a FULL-mode `Data.db` scan, and
/// QUICK mode never scans). Named rather than silently dropping the leaf from
/// the resolved set, which would under-report the intersecting partitions.
///
/// **The COMMON case for a `DataOffset` leaf whose finding is
/// Data.db-anchored**, not an edge case. Not because the scan runs too late
/// (it does not: `finalize_locations` runs AFTER it), but because a `Data.db`
/// corrupt enough to fail the chunk-CRC check is in practice ALSO corrupt
/// enough to fail the full row scan on the SAME bytes — the scan errors,
/// `scan_position_map` stays `None`, and a `DataOffset` leaf's raw key is
/// recoverable ONLY through that map. So a BTI table whose intersecting
/// leaves are `DataOffset` (narrow partitions) commonly reports this even
/// with a healthy boundary source, while a `RowsOffset` leaf (wide
/// partitions) resolves its key INLINE from `Rows.db` and is unaffected. See
/// `issue_4194_verify_location.rs`'s
/// `bti_compressed_chunk_crc_flip_resolves_via_rows_offset_leaves`.
pub const PARTITION_KEY_UNAVAILABLE: &str =
    "partition key unavailable for an intersecting boundary entry (requires a full-mode scan)";

/// `true` when the half-open ranges `[a.0, a.1)` and `[b.0, b.1)` overlap.
fn ranges_intersect(a: (u64, u64), b: (u64, u64)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// One boundary-source entry: `(LOGICAL Data.db position, raw key when
/// known)`. `None` marks an entry whose identity is not yet resolvable (see
/// [`PARTITION_KEY_UNAVAILABLE`]).
///
/// The key is `Arc<[u8]>`, not `Vec<u8>` (roborev round-1 MEDIUM finding): BIG's
/// `PartitionIndexEntry::raw_key`/`key_digest` are ALREADY `Arc<[u8]>` in
/// `IndexReader`'s materialized entries, so building a boundary-entry list from
/// them is an O(1) refcount bump per entry rather than an O(key_len) byte copy —
/// doubling the resident partition-index memory for a large table was the
/// exact cost this type existed to avoid.
pub type BoundaryEntry = (u64, Option<Arc<[u8]>>);

/// The cause named when the boundary source could not be OPENED at all — a
/// permission error, an I/O failure, a path that went away mid-report.
///
/// Roborev "important" finding I1 (#4194): the `Err(_) => None` arm of
/// [`finalize_locations`]'s `IndexReader::open` DISCARDED the real
/// `io::Error` and fell through to the generic [`BOUNDARY_SOURCE_UNAVAILABLE`],
/// so an operator hitting `Permission denied` on `Index.db` was told only that
/// a boundary source was "unavailable" — violating this module's own contract
/// ([`PartitionResolution::Unresolved`]) that "the cause is named".
pub const BOUNDARY_SOURCE_OPEN_FAILED: &str = "boundary source could not be opened";

/// The cause named when no boundary source was produced for this report at
/// all, with no error to attribute it to (the BTI arm's `bti_leaves == None`).
pub const BOUNDARY_SOURCE_UNAVAILABLE: &str = "boundary source unavailable for location resolution";

/// The cause named when the boundary source's own declared `Data.db`
/// positions are not STRICTLY ascending in ON-DISK PARSE ORDER — see
/// [`first_order_violation`] for why that is a corruption signal and not a
/// tolerable quirk.
pub const BOUNDARY_ENTRY_ORDER_VIOLATION: &str =
    "boundary source declares non-ascending Data.db positions";

/// The cause named when a boundary entry declares a `Data.db` position at or
/// past the declared logical length — see [`resolve_partitions`]'s bounds
/// check for the silent-drop/mis-attribution failure this refuses.
pub const BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS: &str =
    "boundary source declares a Data.db position at or past the declared logical length";

/// The boundary source's trust decision for a WHOLE report (design.md §D2:
/// fully trusted or not trusted at all, never partially).
///
/// Replaces the former `boundary_source_healthy: bool` + `Option<&[_]>` pair
/// (roborev #4194): two parameters could express four states, two of which
/// ("healthy but absent", "unhealthy but present") had to be reconciled by
/// convention at every call site, and the `None` arm could only ever render
/// ONE generic cause. A rejection now CARRIES its cause, which is what lets
/// `Index.db`'s open error, its order violation and its out-of-bounds offset
/// each reach the operator under their own name.
pub enum BoundarySource<'a> {
    /// Trusted, pre-sorted ascending by logical `data_offset`.
    Trusted(&'a [BoundaryEntry]),
    /// Not trusted — the named cause, never a silent empty list.
    Rejected(String),
}

/// The index of the first boundary entry whose declared `Data.db` position
/// does not STRICTLY exceed its predecessor's, in ON-DISK PARSE ORDER, or
/// `None` when the sequence ascends strictly.
///
/// # Why non-ascending parse order is corruption, not a quirk
///
/// `Index.db` entries are written in decorated-key order, which the format
/// guarantees is also ascending `Data.db` offset order — the definitive
/// guide states it outright: "the ordering guarantee that `Index.db` entries
/// ascend in token order and therefore in `Data.db` offset order"
/// (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`,
/// §"Token Ordering Requirement" and the sequential-windowing note). So a
/// descent in parse order is a declaration this format cannot produce.
///
/// # Why STRICT, not merely non-decreasing
///
/// Two partitions cannot share a start offset — every partition occupies at
/// least a header's worth of bytes. Two EQUAL positions give the earlier
/// entry the extent `[s, s)`, which intersects nothing, so that partition
/// would be silently dropped from every location in the report: the same
/// silent-drop shape as the out-of-bounds offset below, reached by a
/// different corruption.
///
/// This is a consistency check over AUTHORITATIVE metadata the file declares
/// about itself, not a byte-pattern guess (issue #28) — the same pattern
/// `verify.rs`'s `check_compression_info` already applies to
/// `CompressionInfo.db`'s chunk offsets, whose own rationale ("`validate()`
/// only enforces ascending order; a single corrupted offset (e.g. an MSB set)
/// is ascending yet points past EOF") transfers here verbatim.
pub fn first_order_violation(entries: &[BoundaryEntry]) -> Option<usize> {
    // An explicit index loop, not `windows(2).position(..)`: both of those are
    // primitives `test_verify_location_no_resync_scan.sh` refuses in this
    // file, and the clearer form here needs no waiver at all.
    for i in 1..entries.len() {
        if entries[i].0 <= entries[i - 1].0 {
            return Some(i);
        }
    }
    None
}

/// Intersect `damaged` (a closed-open `[start, end)` range in `Data.db`
/// LOGICAL/decompressed offset space) against `sorted_boundary_entries` — one
/// `(data_offset, raw_key)` pair per partition the boundary source (`Index.db`
/// or the BTI `Partitions.db` trie) names, **already sorted ascending by
/// `data_offset`** (a precondition, not re-sorted here — roborev round-1
/// MEDIUM finding: this function used to sort its input on EVERY call, i.e.
/// once per pending location, even though the boundary source is read once
/// per report and its natural on-disk order is already ascending offset; the
/// caller sorts once — see `verify.rs`'s `finalize_locations`). `raw_key` is
/// `None` when the entry's identity is not yet known (see
/// [`PARTITION_KEY_UNAVAILABLE`]).
///
/// `logical_len` bounds the LAST entry's extent (there is no "next entry" to
/// derive it from) — the boundary source's own declared total logical length
/// (`CompressionInfo::data_length` when compressed, the physical `Data.db`
/// length when not).
///
/// This is a closed-interval test only — no `Data.db` bytes are read or
/// scanned here (design.md §D1/§D6, issue #28).
pub fn resolve_partitions(
    damaged: (u64, u64),
    sorted_boundary_entries: &[BoundaryEntry],
    logical_len: u64,
) -> PartitionResolution {
    // The clear form, restored (roborev job 102 LOW). This was written as a
    // manual `zip(iter().skip(1))` purely to dodge the no-resync-scan guard's
    // lexical slice-pairs pattern — contorting production code to satisfy a
    // grep, over a slice of `(u64, Option<Arc<[u8]>>)` tuples that has nothing
    // to do with scanning `Data.db` bytes for a header. The guard now has a
    // named line-level opt-out, which is the right place for the exception, so
    // the code says what it means and the marker records WHY it is allowed.
    debug_assert!(
        sorted_boundary_entries
            .windows(2) // no-resync-scan-allow:windows — sortedness over boundary tuples, not a byte scan
            .all(|w| w[0].0 <= w[1].0),
        "resolve_partitions requires its input sorted ascending by data_offset"
    );

    // BOUNDS CHECK AGAINST THE DECLARED LOGICAL LENGTH — roborev blocker #2
    // (#4194). Both halves of the extent derivation below come from
    // AUTHORITATIVE metadata, but nothing had ever checked that the two agree,
    // and `Index.db`'s own parse cannot catch the disagreement: flip one bit of
    // a NON-LEADING byte of a multi-byte `position` vint and the length prefix
    // is unchanged, so every entry still parses, `is_fully_parsed()` is still
    // true, and no `IndexEntryCorrupt` finding fires. The bogus position (say
    // `+2^24`) then sorts to the END of the list, where `end` is
    // `logical_len` — BELOW its own `start` — and the `end.max(*start)` clamp
    // below turned it into the empty extent `[huge, huge)`. Consequences, both
    // silent:
    //   1. That partition intersects NOTHING, so it is never reported, never
    //      counted in `truncated`, and never surfaced as `Unresolved` — a
    //      damaged partition dropped from the report entirely.
    //   2. Removing it from the ordering WIDENS its former left neighbour's
    //      extent to the next real partition's start, so bytes belonging to
    //      partition B are attributed to partition A and A's clean key is
    //      printed as damaged with full confidence.
    // Refusing with a named cause is §D2's "a refused answer over a confident
    // wrong one". This is a consistency check over metadata the file declares
    // about itself — NOT a byte-pattern guess (issue #28); it is the same
    // check `verify.rs`'s `check_compression_info` already applies to
    // `CompressionInfo.db`'s chunk offsets, for the same stated reason.
    //
    // `>= logical_len`, not `> logical_len`: an entry starting exactly AT the
    // declared logical end has the empty extent `[len, len)`, i.e. precisely
    // the degenerate case above. Testing only `>` would leave that one value
    // silently dropped.
    //
    // O(1), not O(n): the input is sorted ascending (asserted above), so the
    // LAST entry is the maximum and the only one that can be out of bounds.
    if let Some((last_start, _)) = sorted_boundary_entries.last() {
        if *last_start >= logical_len {
            return PartitionResolution::Unresolved(format!(
                "{BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS}: declared position {last_start} \
                 (0x{last_start:x}) is not below the declared logical length {logical_len}"
            ));
        }
    }

    // Bounded DURING accumulation (`hits.len() < MAX_RESOLVED_KEYS`), not just
    // at the end: a truncation's damaged range `[first_bad_chunk_start,
    // logical_len)` can intersect essentially every partition in the file, so
    // materializing them all before capping would defeat the cap.
    //
    // `seen` dedups by raw key DURING accumulation too. A post-hoc dedup after
    // the cap would let a boundary source naming one raw key twice
    // (defensive-only; not an expected shape) consume TWO cap slots and inflate
    // `truncated` with pre-dedup entries — rendering e.g. "40 partition(s)
    // (+60 more, capped)" when only 100 DISTINCT partitions intersect.
    //
    // `seen` is itself bounded to MAX_RESOLVED_KEYS: growing it to O(distinct
    // intersecting partitions) would reintroduce exactly the O(partitions)
    // growth the cap exists to eliminate. Once `hits` is full, dedup stops — a
    // duplicate past the cap just counts as one more `truncated` entry, a minor
    // over-count traded for a hard memory bound.
    let mut hits: Vec<KeyRef> = Vec::new();
    let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    let mut unknown = false;
    let mut truncated = 0usize;
    for (i, (start, key)) in sorted_boundary_entries.iter().enumerate() {
        let end = sorted_boundary_entries
            .get(i + 1)
            .map(|(next_start, _)| *next_start)
            .unwrap_or(logical_len);
        // `end >= *start` for EVERY entry now that the bounds check above has
        // run: non-last entries take `end` from their successor, and the
        // input is sorted ascending; the last takes `logical_len`, which the
        // check proved is strictly greater. So the `.max(*start)` clamp is no
        // longer load-bearing — it was what silently converted a corrupt BIG
        // entry into an empty extent (roborev blocker #2). It is kept only as
        // a non-panicking release fallback for the BTI arm, whose leaves are
        // not yet validated this way (that is blocked on an owner decision on
        // the BTI resolution-state machinery); the assertion states the
        // invariant the BIG path now guarantees.
        debug_assert!(
            end >= *start,
            "boundary entry {i} has extent end {end} below its start {start}; the bounds \
             check in resolve_partitions should have refused this source"
        );
        let extent = (*start, end.max(*start));
        if ranges_intersect(damaged, extent) {
            match key {
                Some(k) => {
                    let raw: &[u8] = k;
                    if hits.len() < MAX_RESOLVED_KEYS {
                        if seen.insert(raw) {
                            hits.push(KeyRef::from_raw(raw));
                        }
                    } else if !seen.contains(raw) {
                        truncated += 1;
                    }
                }
                None => unknown = true,
            }
        }
    }

    if unknown {
        return PartitionResolution::Unresolved(PARTITION_KEY_UNAVAILABLE.to_string());
    }
    hits.sort_by(|a, b| a.key_hex.cmp(&b.key_hex));
    PartitionResolution::Resolved {
        keys: hits,
        truncated,
    }
}

/// Build a [`Location`] for a chunk/offset-anchored finding, fail-closed on a
/// damaged boundary source (design.md §D2): a
/// [`BoundarySource::Rejected`] always yields `Unresolved` carrying that
/// rejection's OWN cause — the boundary source is either fully trusted or not
/// trusted at all, never partially, and a refusal always names why.
#[allow(clippy::too_many_arguments)]
pub fn resolve_location(
    component: &str,
    byte_offset: u64,
    byte_len: u64,
    anchor: PhysicalAnchor,
    chunk_index: Option<usize>,
    boundary: &BoundarySource<'_>,
    damaged_logical: (u64, u64),
    logical_len: u64,
) -> Location {
    let partitions = match boundary {
        BoundarySource::Trusted(entries) => {
            resolve_partitions(damaged_logical, entries, logical_len)
        }
        BoundarySource::Rejected(cause) => PartitionResolution::Unresolved(cause.clone()),
    };
    Location {
        component: component.to_string(),
        byte_offset,
        byte_len,
        anchor,
        chunk_index,
        partitions,
    }
}

/// Resolve every [`PendingLocation`] in one pass and write the result back
/// onto its owning finding (issue #4194, design.md §D1/§D2).
///
/// The boundary source is either fully trusted for this whole report or not
/// trusted at all — never partially: `boundary_healthy` is a single decision
/// (no finding against the boundary component, and no index-structure-corrupt
/// finding class) applied uniformly to every pending location, so a damaged
/// boundary source poisons ALL of them with
/// [`BOUNDARY_SOURCE_UNREADABLE`], never just the finding that happens to be
/// nearest the damage.
///
/// `pub(crate)` (#1116 relocation): called from `verify.rs`'s
/// `verify_components` once every check has run.
pub(crate) async fn finalize_locations(
    dir: &Path,
    components: &ComponentSet,
    findings: &mut [VerifyFinding],
    pending: Vec<PendingLocation>,
    bti_leaves: Option<&[BtiResolvedLeaf]>,
    scan_position_map: Option<&std::collections::HashMap<u64, Vec<u8>>>,
    platform: Arc<Platform>,
) {
    // Distrust on EITHER signal (a union, never component-only): a finding
    // whose CLASS says the index structure is corrupt, OR any finding at all
    // against the boundary component itself. The class test alone missed
    // `MissingComponent`/`UnexpectedEof` against `Index.db`/`Partitions.db`,
    // and — the genuine wrong-answer case — a missing `Rows.db`, where
    // `check_bti_structure` still returns the `DataOffset` leaves it could
    // read: a PARTIAL boundary list that would then be presented as
    // `Resolved`. The component test cannot replace the class test either,
    // since `BtiTrieCorrupt` is raised on `Rows.db`, which spec L2 does not
    // name as a boundary source. Deliberately over-conservative where the two
    // overlap (a boundary component present on disk but unlisted in TOC.txt
    // poisons every location): §D2 prefers a refused answer to a confident
    // wrong one.
    //
    // Expressed as an `Option<String>` CAUSE rather than a bool: every
    // rejection path below (this one, a partial `IndexReader` parse, an open
    // error, an order violation) carries its own cause through to the
    // operator (roborev I1/#blocker 2, #4194).
    let boundary_components: &[&str] = match components.format {
        SsTableFormat::Big => &["Index.db"],
        SsTableFormat::Bti => &["Partitions.db", "Rows.db"],
    };
    let component_or_class_distrust = findings.iter().any(|f| {
        boundary_components.contains(&f.component.as_str())
            || match components.format {
                SsTableFormat::Big => f.class == VerifyErrorClass::IndexEntryCorrupt,
                SsTableFormat::Bti => matches!(
                    f.class,
                    VerifyErrorClass::BtiRootPointerCorrupt | VerifyErrorClass::BtiTrieCorrupt
                ),
            }
    });

    // Boundary entries: `(logical Data.db position, raw key when known)`, one
    // per partition the boundary source names — built only when the boundary
    // source is healthy (an unhealthy one is never read for this purpose,
    // matching the fail-closed contract regardless of what re-reading it
    // might yield). Keys are `Arc<[u8]>` (roborev round-1 MEDIUM finding): BIG
    // reuses `PartitionIndexEntry::raw_key`/`key_digest`'s ALREADY-`Arc`
    // storage via a refcount bump, never an `O(key_len)` byte copy that would
    // double the resident partition-index memory for a large table.
    let mut built: Result<Vec<BoundaryEntry>, String> = if component_or_class_distrust {
        Err(BOUNDARY_SOURCE_UNREADABLE.to_string())
    } else {
        match components.format {
            SsTableFormat::Big => {
                use crate::storage::sstable::index_reader::IndexReader;
                let index_path = components.path(dir, "Index.db");
                match IndexReader::open(&index_path, platform).await {
                    // `IndexReader` uses a DIFFERENT parser from
                    // `check_big_index`'s structural walk above, and per Check
                    // 4's own doc it "silently TRUNCATES the partition list on
                    // the first malformed Index.db entry" (#2302) — exposed via
                    // `is_fully_parsed()`. So no `IndexEntryCorrupt` does NOT
                    // mean `IndexReader` parsed the whole file; if the two
                    // parsers disagree, presenting a partial prefix as
                    // `Resolved` is a confident WRONG answer, exactly what the
                    // fail-closed contract (§D2) exists to
                    // prevent. Downgrade `boundary_healthy` itself (not just
                    // this arm's `None`) so every OTHER pending location in
                    // this report is poisoned too, matching "fully trusted or
                    // not at all".
                    Ok(reader) if !reader.is_fully_parsed() => {
                        Err(BOUNDARY_SOURCE_UNREADABLE.to_string())
                    }
                    Ok(reader) => {
                        let entries: Vec<BoundaryEntry> = reader
                            .get_partition_entries()
                            .iter()
                            .map(|e| {
                                let raw = e.raw_key.clone().unwrap_or_else(|| e.key_digest.clone());
                                (e.data_offset, Some(raw))
                            })
                            .collect();
                        // PARSE-ORDER VALIDATION (roborev blocker #2, #4194).
                        // Checked BEFORE the sort below, because the sort is
                        // exactly what HIDES this corruption: a bit flip in a
                        // non-leading byte of a `position` vint leaves every
                        // entry parsing cleanly, and sorting then quietly
                        // moves the bogus entry into a position where its
                        // extent collapses and its neighbour's widens. The
                        // format guarantees strict ascent in parse order
                        // (definitive guide Ch.6, cited on
                        // `first_order_violation`), so a violation is
                        // corruption and the source is refused by name.
                        match first_order_violation(&entries) {
                            Some(i) => Err(format!(
                                "{BOUNDARY_ENTRY_ORDER_VIOLATION}: Index.db entry {i} declares \
                                 Data.db position {} (0x{:x}) after entry {} declared {} \
                                 (0x{:x})",
                                entries[i].0,
                                entries[i].0,
                                i - 1,
                                entries[i - 1].0,
                                entries[i - 1].0
                            )),
                            None => Ok(entries),
                        }
                    }
                    // The REAL error, named (roborev I1, #4194): this arm used
                    // to discard it and fall through to the generic
                    // "unavailable", so `Permission denied` on `Index.db`
                    // reached the operator as no cause at all.
                    Err(e) => Err(format!(
                        "{BOUNDARY_SOURCE_OPEN_FAILED}: {}: {e}",
                        index_path.display()
                    )),
                }
            }
            SsTableFormat::Bti => match bti_leaves {
                Some(leaves) => Ok(leaves
                    .iter()
                    .map(|leaf| {
                        let key = leaf.inline_raw_key.as_deref().map(Arc::from).or_else(|| {
                            scan_position_map
                                .and_then(|m| m.get(&leaf.data_position))
                                .map(|k| Arc::from(k.as_slice()))
                        });
                        (leaf.data_position, key)
                    })
                    .collect()),
                None => Err(BOUNDARY_SOURCE_UNAVAILABLE.to_string()),
            },
        }
    };
    // `resolve_partitions` requires its input pre-sorted ascending by
    // `data_offset` (roborev round-1 MEDIUM finding — sort ONCE here rather
    // than on every pending-location call). For BIG this sort is a NO-OP by
    // the time it runs: the format guarantees strict ascent in parse order
    // (definitive guide Ch.6) and `first_order_violation` above has just
    // refused the source if it did not hold. It is retained because BTI
    // leaves come from a byte-comparable-KEY-order DFS trie walk, which is
    // NOT Data.db offset order at all, and because `resolve_partitions`'s
    // sortedness precondition must be established for BOTH arms by the same
    // statement rather than by one arm's format guarantee.
    if let Ok(entries) = built.as_mut() {
        entries.sort_by_key(|(offset, _)| *offset);
    }
    let boundary = match &built {
        Ok(entries) => BoundarySource::Trusted(entries),
        Err(cause) => BoundarySource::Rejected(cause.clone()),
    };

    for p in pending {
        let location = resolve_location(
            &p.component,
            p.byte_offset,
            p.byte_len,
            p.anchor,
            p.chunk_index,
            &boundary,
            p.damaged_logical,
            p.logical_len,
        );
        if let Some(f) = findings.get_mut(p.finding_index) {
            f.location = Some(location);
        }
    }
}

#[cfg(test)]
#[path = "verify_location_tests.rs"]
mod tests;
