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

/// What a [`Location`]'s physical `byte_offset`/`byte_len` actually describe
/// (issue #4194, roborev job 92 MEDIUM finding).
///
/// The two readings are not interchangeable, and conflating them produced
/// output that pointed an operator at bytes which do not exist: a corruption
/// LOCATOR whose physical range is a declared-but-absent offset must say so,
/// because `dd`/`xxd` at that offset returns nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalAnchor {
    /// `byte_offset`/`byte_len` cover bytes that are PRESENT in the file and
    /// damaged — a failed chunk CRC, a decompression error. Reading that range
    /// off disk yields the damaged bytes themselves.
    DamagedExtent,
    /// `byte_offset`/`byte_len` are a location the SSTable's OWN metadata
    /// declares and the file does not satisfy: for `ChunkOffsetOutOfBounds` the
    /// declared chunk offset (and the 4-byte length prefix it points at) lies
    /// beyond the end of `Data.db`, so those bytes cannot be read at all. The
    /// damaged extent in that case is the LOGICAL range, which is what
    /// `partitions` enumerates.
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
/// `pub(crate)` with `pub(crate)` fields (issue #4194 file-size relocation,
/// #1116/#1135): constructed at each check site in `verify.rs`, resolved
/// here — the type and its fields must be visible across that module
/// boundary for straight struct-literal construction.
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
/// **This is the COMMON case for a `DataOffset` leaf whose finding is
/// Data.db-anchored** (roborev round-1 MEDIUM finding), not an edge case —
/// though NOT for the reason an earlier draft of this doc claimed (roborev
/// round-3 LOW finding: location resolution happens in `finalize_locations`,
/// which runs AFTER the FULL-mode row scan, so `scan_position_map` is
/// already populated by the time any location resolves, whenever the scan
/// succeeds). The actual cause: a `Data.db` corrupt enough to fail the
/// chunk-CRC check is, in practice, ALSO corrupt enough to fail the full row
/// scan on the SAME bytes — so the scan errors, `scan_position_map` stays
/// `None`, and a `DataOffset` leaf's raw key (recoverable ONLY through that
/// map) is unavailable. A BTI table whose intersecting leaves are
/// `DataOffset` (narrow partitions) therefore commonly reports
/// `Unresolved(PARTITION_KEY_UNAVAILABLE)` for its chunk-CRC findings even
/// with a perfectly healthy boundary source; a `RowsOffset` leaf (wide
/// partitions) resolves its key INLINE from `Rows.db` and is unaffected by
/// whether the scan succeeds. See `issue_4194_verify_location.rs`'s
/// `bti_compressed_chunk_crc_flip_resolves_via_rows_offset_leaves` for the
/// positive (`RowsOffset`) case this module's tests cover.
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

    // Issue #4194, roborev round-2 MEDIUM finding: bounded DURING accumulation
    // (`hits.len() < MAX_RESOLVED_KEYS`), not just at the end — a truncation's
    // damaged range `[first_bad_chunk_start, logical_len)` can intersect
    // essentially every partition in the file, and `hits` materializing all of
    // them before ever being capped would defeat the point.
    //
    // `seen` dedups by raw key DURING accumulation (roborev round-3 LOW
    // finding): a POST-hoc `dedup_by` after the cap made `truncated` count
    // pre-dedup entries — a boundary source naming the same raw key twice
    // (defensive-only in practice; not an expected shape) would consume TWO
    // cap slots and could render e.g. "40 partition(s) (+60 more, capped)"
    // when only 100 DISTINCT partitions actually intersect. Deduping here
    // means a duplicate is recognized before it can occupy a slot OR inflate
    // `truncated`, and the final list needs no further dedup pass.
    //
    // `seen` is itself BOUNDED to MAX_RESOLVED_KEYS entries (roborev round-4
    // LOW finding): round-3's fix grew it unboundedly — O(distinct
    // intersecting partitions) — which is exactly the O(partitions) growth
    // the MAX_RESOLVED_KEYS cap (round 2) exists to eliminate for the
    // truncation case this module's own doc describes (a damaged range
    // spanning essentially the whole file). Once `hits` reaches the cap,
    // dedup stops (a duplicate found past the cap is defensive-only and
    // simply counts as one more `truncated` entry, a minor over-count
    // accepted in exchange for a hard memory bound) rather than growing
    // `seen` to match the file's full partition count.
    let mut hits: Vec<KeyRef> = Vec::new();
    let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    let mut unknown = false;
    let mut truncated = 0usize;
    for (i, (start, key)) in sorted_boundary_entries.iter().enumerate() {
        let end = sorted_boundary_entries
            .get(i + 1)
            .map(|(next_start, _)| *next_start)
            .unwrap_or(logical_len);
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
/// damaged boundary source (design.md §D2): when `boundary_source_healthy` is
/// `false`, `partitions` is always `Unresolved(BOUNDARY_SOURCE_UNREADABLE)` —
/// the boundary source is either fully trusted or not trusted at all, never
/// partially.
#[allow(clippy::too_many_arguments)]
pub fn resolve_location(
    component: &str,
    byte_offset: u64,
    byte_len: u64,
    anchor: PhysicalAnchor,
    chunk_index: Option<usize>,
    boundary_source_healthy: bool,
    damaged_logical: (u64, u64),
    boundary_entries: Option<&[BoundaryEntry]>,
    logical_len: u64,
) -> Location {
    let partitions = if !boundary_source_healthy {
        PartitionResolution::Unresolved(BOUNDARY_SOURCE_UNREADABLE.to_string())
    } else {
        match boundary_entries {
            Some(entries) => resolve_partitions(damaged_logical, entries, logical_len),
            None => PartitionResolution::Unresolved(
                "boundary source unavailable for location resolution".to_string(),
            ),
        }
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
/// `pub(crate)` (issue #4194 file-size relocation): called from
/// `verify.rs`'s `verify_components` once every check has run.
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
    // `mut`: the BIG arm below can additionally downgrade this to `false`
    // after consulting `IndexReader::is_fully_parsed()`.
    let boundary_components: &[&str] = match components.format {
        SsTableFormat::Big => &["Index.db"],
        SsTableFormat::Bti => &["Partitions.db", "Rows.db"],
    };
    let mut boundary_healthy = !findings.iter().any(|f| {
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
    let mut boundary_entries: Option<Vec<BoundaryEntry>> = if !boundary_healthy {
        None
    } else {
        match components.format {
            SsTableFormat::Big => {
                use crate::storage::sstable::index_reader::IndexReader;
                let index_path = components.path(dir, "Index.db");
                match IndexReader::open(&index_path, platform).await {
                    // Issue #4194, roborev round-1 MEDIUM finding: `IndexReader`
                    // uses a DIFFERENT parser from `check_big_index`'s structural
                    // walk above, and per Check 4's own doc it "silently
                    // TRUNCATES the partition list on the first malformed
                    // Index.db entry" (issue #2302) — exposed via
                    // `is_fully_parsed()`. `check_big_index` seeing no
                    // `IndexEntryCorrupt` does NOT mean `IndexReader` parsed the
                    // whole file; if the two parsers disagree, presenting a
                    // partial prefix as `Resolved` is a confident WRONG answer,
                    // exactly what the fail-closed contract (§D2) exists to
                    // prevent. Downgrade `boundary_healthy` itself (not just
                    // this arm's `None`) so every OTHER pending location in
                    // this report is poisoned too, matching "fully trusted or
                    // not at all".
                    Ok(reader) if !reader.is_fully_parsed() => {
                        boundary_healthy = false;
                        None
                    }
                    Ok(reader) => Some(
                        reader
                            .get_partition_entries()
                            .iter()
                            .map(|e| {
                                let raw = e.raw_key.clone().unwrap_or_else(|| e.key_digest.clone());
                                (e.data_offset, Some(raw))
                            })
                            .collect(),
                    ),
                    Err(_) => None,
                }
            }
            SsTableFormat::Bti => bti_leaves.map(|leaves| {
                leaves
                    .iter()
                    .map(|leaf| {
                        let key = leaf.inline_raw_key.as_deref().map(Arc::from).or_else(|| {
                            scan_position_map
                                .and_then(|m| m.get(&leaf.data_position))
                                .map(|k| Arc::from(k.as_slice()))
                        });
                        (leaf.data_position, key)
                    })
                    .collect()
            }),
        }
    };
    // `resolve_partitions` requires its input pre-sorted ascending by
    // `data_offset` (roborev round-1 MEDIUM finding — sort ONCE here rather
    // than on every pending-location call): BIG's on-disk parse order is
    // ascending by convention but not a documented guarantee, and BTI leaves
    // come from a byte-comparable-KEY-order DFS trie walk, which is NOT
    // Data.db offset order at all.
    if let Some(entries) = boundary_entries.as_mut() {
        entries.sort_by_key(|(offset, _)| *offset);
    }

    for p in pending {
        let location = resolve_location(
            &p.component,
            p.byte_offset,
            p.byte_len,
            p.anchor,
            p.chunk_index,
            boundary_healthy,
            p.damaged_logical,
            boundary_entries.as_deref(),
            p.logical_len,
        );
        if let Some(f) = findings.get_mut(p.finding_index) {
            f.location = Some(location);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(pairs: &[(u64, &[u8])]) -> Vec<BoundaryEntry> {
        pairs
            .iter()
            .map(|(o, k)| (*o, Some(Arc::from(*k))))
            .collect()
    }

    /// `Resolved { keys, truncated: 0 }` — the shape every case in this file
    /// expects except the dedicated cap test.
    fn resolved(keys: Vec<KeyRef>) -> PartitionResolution {
        PartitionResolution::Resolved { keys, truncated: 0 }
    }

    #[test]
    fn intersects_a_single_partition() {
        let e = entries(&[(0, b"k0"), (100, b"k1"), (200, b"k2")]);
        // Damaged range [100, 150) falls entirely inside k1's extent [100,200).
        let res = resolve_partitions((100, 150), &e, 300);
        assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
    }

    #[test]
    fn intersects_two_adjacent_partitions_when_the_range_spans_the_boundary() {
        let e = entries(&[(0, b"k0"), (100, b"k1"), (200, b"k2")]);
        // [90, 110) straddles k0's end and k1's start.
        let res = resolve_partitions((90, 110), &e, 300);
        assert_eq!(
            res,
            resolved(vec![KeyRef::from_raw(b"k0"), KeyRef::from_raw(b"k1")])
        );
    }

    #[test]
    fn touching_but_not_overlapping_is_not_an_intersection() {
        let e = entries(&[(0, b"k0"), (100, b"k1")]);
        // [100, 150) starts exactly where k0 ends — no overlap with k0.
        let res = resolve_partitions((100, 150), &e, 300);
        assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
    }

    #[test]
    fn last_partition_extent_is_bounded_by_logical_len() {
        let e = entries(&[(0, b"k0"), (100, b"k1")]);
        let res = resolve_partitions((250, 260), &e, 300);
        assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
        let res_past_end = resolve_partitions((300, 310), &e, 300);
        assert_eq!(res_past_end, resolved(vec![]));
    }

    #[test]
    fn no_intersection_resolves_to_an_empty_resolved_set() {
        let e = entries(&[(1000, b"k0")]);
        let res = resolve_partitions((0, 10), &e, 2000);
        assert_eq!(res, resolved(vec![]));
    }

    #[test]
    fn resolved_set_is_capped_and_names_the_truncated_count() {
        // MAX_RESOLVED_KEYS + 10 boundary entries, all intersecting one huge
        // damaged range — the shape a badly truncated Data.db produces.
        let mut pairs: Vec<(u64, Vec<u8>)> = Vec::new();
        for i in 0..(MAX_RESOLVED_KEYS + 10) {
            pairs.push((i as u64 * 10, format!("k{i:04}").into_bytes()));
        }
        let e: Vec<BoundaryEntry> = pairs
            .iter()
            .map(|(o, k)| (*o, Some(Arc::from(k.as_slice()))))
            .collect();
        let logical_len = (MAX_RESOLVED_KEYS as u64 + 11) * 10;
        let res = resolve_partitions((0, logical_len), &e, logical_len);
        match res {
            PartitionResolution::Resolved { keys, truncated } => {
                assert_eq!(keys.len(), MAX_RESOLVED_KEYS);
                assert_eq!(truncated, 10);
            }
            other => panic!("expected a capped Resolved set, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_raw_keys_do_not_inflate_the_truncated_count() {
        // roborev round-3 LOW finding: a boundary source naming the SAME raw
        // key at two different offsets (defensive-only in practice) must not
        // consume two cap slots or count as two omissions.
        let e: Vec<BoundaryEntry> = vec![
            (0, Some(Arc::from(b"dup".as_slice()))),
            (10, Some(Arc::from(b"dup".as_slice()))),
            (20, Some(Arc::from(b"unique".as_slice()))),
        ];
        let res = resolve_partitions((0, 30), &e, 30);
        match res {
            PartitionResolution::Resolved { keys, truncated } => {
                assert_eq!(truncated, 0, "no key exceeds MAX_RESOLVED_KEYS here");
                let mut hexes: Vec<&str> = keys.iter().map(|k| k.key_hex.as_str()).collect();
                hexes.sort();
                hexes.dedup();
                assert_eq!(
                    hexes.len(),
                    keys.len(),
                    "the resolved set must already be deduped by raw key identity: {keys:?}"
                );
            }
            other => panic!("expected Resolved, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_intersecting_key_unresolves_the_whole_finding() {
        let e: Vec<BoundaryEntry> = vec![(0u64, Some(Arc::from(b"k0".as_slice()))), (100u64, None)];
        let res = resolve_partitions((100, 150), &e, 300);
        assert_eq!(
            res,
            PartitionResolution::Unresolved(PARTITION_KEY_UNAVAILABLE.to_string())
        );
    }

    // format_location's PARTITION branches (roborev job 102 LOW, upgraded:
    // `--out text` is the DEFAULT for both `verify` and `sweep`, so these two
    // strings ARE the disclosures this change exists to make, and neither had
    // any coverage. A regression dropping the `+N more, capped` suffix would
    // have let a partial list read as complete with a green suite.
    #[test]
    fn format_location_discloses_a_capped_partition_list() {
        let loc = Location {
            component: "Data.db".to_string(),
            byte_offset: 0x28000,
            byte_len: 4,
            anchor: PhysicalAnchor::DeclaredRecord,
            chunk_index: Some(1),
            partitions: PartitionResolution::Resolved {
                keys: vec![KeyRef::from_raw(b"k0")],
                truncated: 842,
            },
        };
        let out = format_location(&loc);
        assert!(
            out.contains("(+842 more, capped)"),
            "a capped list MUST say it is incomplete: {out}"
        );
        // …and the declared-record disclosure rides along on the same line.
        assert!(
            out.contains("declared offset 0x28000") && out.contains("does not fit"),
            "capped rendering must not lose the anchor disclosure: {out}"
        );
    }

    #[test]
    fn format_location_discloses_an_unresolved_cause() {
        let loc = Location {
            component: "Data.db".to_string(),
            byte_offset: 64,
            byte_len: 16,
            anchor: PhysicalAnchor::DamagedExtent,
            chunk_index: Some(0),
            partitions: PartitionResolution::Unresolved(BOUNDARY_SOURCE_UNREADABLE.to_string()),
        };
        let out = format_location(&loc);
        assert!(
            out.contains("partitions unresolved") && out.contains(BOUNDARY_SOURCE_UNREADABLE),
            "an unresolved location MUST name its cause rather than render an empty list: {out}"
        );
        assert!(
            !out.contains("declared offset"),
            "a damaged extent must not be labelled declared: {out}"
        );
    }

    #[test]
    fn format_location_names_an_empty_resolved_set_without_claiming_partitions() {
        let loc = Location {
            component: "Data.db".to_string(),
            byte_offset: 64,
            byte_len: 16,
            anchor: PhysicalAnchor::DamagedExtent,
            chunk_index: None,
            partitions: PartitionResolution::Resolved {
                keys: vec![],
                truncated: 0,
            },
        };
        let out = format_location(&loc);
        assert!(
            out.contains("0 partitions"),
            "an empty resolved set reads as an affirmative zero, never a blank: {out}"
        );
    }

    #[test]
    fn resolve_location_fails_closed_on_a_damaged_boundary_source() {
        let e = entries(&[(0, b"k0")]);
        let loc = resolve_location(
            "Data.db",
            64,
            16,
            PhysicalAnchor::DamagedExtent,
            Some(0),
            false, // boundary source damaged
            (0, 16384),
            Some(&e),
            16384,
        );
        assert_eq!(
            loc.partitions,
            PartitionResolution::Unresolved(BOUNDARY_SOURCE_UNREADABLE.to_string())
        );
    }

    #[test]
    fn resolve_location_resolves_when_the_boundary_source_is_healthy() {
        let e = entries(&[(0, b"k0")]);
        let loc = resolve_location(
            "Data.db",
            64,
            16,
            PhysicalAnchor::DamagedExtent,
            Some(0),
            true,
            (0, 100),
            Some(&e),
            16384,
        );
        assert_eq!(loc.partitions, resolved(vec![KeyRef::from_raw(b"k0")]));
        assert_eq!(loc.component, "Data.db");
        assert_eq!(loc.byte_offset, 64);
        assert_eq!(loc.byte_len, 16);
        assert_eq!(loc.chunk_index, Some(0));
    }

    #[test]
    fn hex_encode_matches_lower_case_pairs() {
        assert_eq!(hex_encode(&[0x00, 0xab, 0xff]), "00abff");
    }
}
