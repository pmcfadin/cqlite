//! Shared model types and free helpers for the data-access read paths.
//!
//! This module holds the small, path-agnostic pieces the BTI, sequential, and
//! compaction read paths all build on: the clustering-slice model, the BTI
//! lookup-step decision, table-id matching, token-order sorting, and the
//! `scan_for_key` invocation counter. Each read-path submodule (`bti`,
//! `sequential`, `compaction`) reaches these via `super::model::*`.

use crate::types::{CellWriteMetadata, ScanRow, TableId, Value};
use crate::util::cassandra_murmur3::cassandra_murmur3_token;
#[cfg(not(feature = "tombstones"))]
use crate::Result;
use crate::RowKey;

/// A single-column clustering-key restriction pushed down to a within-partition
/// seek (Issue #954, Epic #951).
///
/// Carries the decoded clustering bound `Value`(s) for the FIRST clustering
/// column (multi-column prefixes are a documented follow-up). Each bound is one
/// clustering component; an empty `Vec` means that side is OPEN (unbounded). The
/// `_inclusive` flags distinguish `>=`/`<=` (inclusive) from `>`/`<` (exclusive)
/// — they are advisory for byte-window selection (block selection is
/// over-inclusive by block granularity; the post-scan `evaluate_leaf` backstop
/// applies the exact bound), so a slightly wider byte window never changes the
/// final result.
///
/// `ck = v` is represented as `start == end == [v]`, both inclusive.
#[derive(Debug, Clone)]
pub struct ClusteringSlice {
    /// Lower bound clustering component(s); empty = unbounded below.
    pub start: Vec<Value>,
    /// Whether the lower bound is inclusive (`>=`/`=`) vs exclusive (`>`).
    pub start_inclusive: bool,
    /// Upper bound clustering component(s); empty = unbounded above.
    pub end: Vec<Value>,
    /// Whether the upper bound is inclusive (`<=`/`=`) vs exclusive (`<`).
    pub end_inclusive: bool,
}

/// The within-partition row-body byte window selected by the BTI row index for a
/// clustering slice (Issue #954). Offsets are RELATIVE to the partition start —
/// the same domain the parser sees for `window[within..]`.
#[cfg(not(feature = "tombstones"))]
#[derive(Debug, Clone, Copy)]
pub(super) struct ClusteringRowWindow {
    /// First byte of the row body to parse (inclusive), relative to partition
    /// start. `0` decodes from the partition body start (used when statics exist).
    pub(super) body_start_rel: usize,
    /// Exclusive end of the row body to parse, relative to partition start;
    /// `usize::MAX` means "to the partition end" (clamped by the caller).
    pub(super) body_end_rel: usize,
}

/// Length of the all-`0xFF` sentinel used to represent an OPEN upper clustering
/// bound (+∞) for byte-comparable block selection (Issue #954). Any separator in
/// `Rows.db` is shorter or sorts below an all-`0xFF` run of this length, so it
/// reliably selects through the last block. 64 bytes comfortably exceeds any
/// realistic single-column clustering separator width.
#[cfg(not(feature = "tombstones"))]
pub(super) const MAX_OSS50_BOUND_SENTINEL_LEN: usize = 64;

/// `ByteSource.LT_NEXT_COMPONENT` (cassandra-5.0.8
/// `utils/bytecomparable/ByteSource.java:75`) — the byte-comparable terminator
/// `ClusteringPrefix.Kind.INCL_START_BOUND` / `EXCL_END_BOUND` emit
/// (`db/ClusteringPrefix.java:70-71`). It is smaller than `NEXT_COMPONENT`
/// (`0x40`), so a bound carrying it sorts BELOW every clustering that extends it.
#[cfg(not(feature = "tombstones"))]
const OSS50_LT_NEXT_COMPONENT: u8 = 0x20;

/// `ByteSource.GT_NEXT_COMPONENT` (`ByteSource.java:76`) — the terminator
/// `ClusteringPrefix.Kind.INCL_END_BOUND` / `EXCL_START_BOUND` emit
/// (`ClusteringPrefix.java:77-79`). It is LARGER than `NEXT_COMPONENT` (`0x40`)
/// and than the variable-length escape bytes, so a bound carrying it sorts ABOVE
/// every clustering that extends it.
#[cfg(not(feature = "tombstones"))]
const OSS50_GT_NEXT_COMPONENT: u8 = 0x60;

/// Normalize a CQL [`ClusteringSlice`] into the `(physical_lower, physical_upper)`
/// byte-comparable bounds that [`select_row_index_blocks_for_range`] consumes
/// (issue #954 High-severity correctness fix).
///
/// [`select_row_index_blocks_for_range`] selects blocks purely in **physical**
/// (on-disk, byte-comparable) order — the order `Rows.db` separators are stored
/// in. The encoder ([`encode_clustering_bound_oss50_with_order`]) already inverts
/// every byte of a DESC component (`ReversedType` / `ByteSource.invert`), so for a
/// DESC first clustering column an ASCENDING byte order corresponds to a
/// DESCENDING CQL value order. The consequence:
///
/// - **ASC** (`is_reversed[0] == false`): CQL `start` (the `>=`/`>` lower value
///   bound) IS the physical-lower bound and CQL `end` (the `<=`/`<` upper value
///   bound) IS the physical-upper bound. No swap.
/// - **DESC** (`is_reversed[0] == true`): the CQL lower-value bound encodes to the
///   physically GREATER bytes and the CQL upper-value bound to the physically
///   SMALLER bytes, so the roles SWAP. A CQL `ck >= v` (lower) selects the rows
///   with the largest values, which sit at the physical-LOW byte side through
///   `enc(v)`; a CQL `ck < v` (upper) selects the physical-HIGH side. The open
///   sentinels swap accordingly: an open CQL lower (`-∞` value) maps to physical
///   `+∞` and an open CQL upper (`+∞` value) maps to physical `-∞`.
///
/// Block selection is over-inclusive by block granularity and the post-scan
/// `evaluate_leaf` backstop re-applies the exact CQL bound by VALUE, so the
/// inclusivity of each bound does not need separate handling here — the physical
/// window only has to be a SUPERSET of the matching rows. (Swapping the roles is
/// what guarantees the superset for DESC; the previous code built
/// `[enc(lower), +∞]` for DESC, which excluded the matching low-byte rows and the
/// backstop could not recover rows that were never decoded.)
///
/// Returns:
/// - `Ok(Some((lower, upper)))` — usable physical bounds.
/// - `Ok(None)` — a bound's type is not byte-comparable-encodable here, so the
///   narrowing is unsafe and the caller must decode the whole partition.
/// - `Err(_)` — never (kept `Result` for call-site symmetry); reserved.
#[cfg(not(feature = "tombstones"))]
pub(super) fn physical_byte_bounds_for_slice(
    slice: &ClusteringSlice,
    is_reversed: &[bool],
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    use crate::storage::sstable::bti::encode_clustering_bound_oss50_with_order;

    // The physical-low sentinel: empty sorts before every separator (-∞).
    let neg_inf = Vec::<u8>::new();
    // The physical-high sentinel: an all-0xFF run sorts after every separator (+∞).
    let pos_inf = || vec![0xFFu8; MAX_OSS50_BOUND_SENTINEL_LEN];

    // Encode a closed CQL bound to its physical byte-comparable form; `None`
    // bubbles up as an un-encodable-bound fallback at the call site.
    // The safe fallback is retained (an un-encodable bound just widens to the whole
    // partition), but the encoder's error text is the ONLY diagnosis of WHICH
    // clustering type is unsupported, so log it rather than dropping it silently.
    let encode = |values: &[Value]| -> Option<Vec<u8>> {
        match encode_clustering_bound_oss50_with_order(values, is_reversed) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "OSS50 clustering bound not encodable; falling back to a full-partition \
                     scan for this slice"
                );
                None
            }
        }
    };

    // The FIRST clustering column's order decides the value↔byte direction. A
    // missing entry (no schema / fewer entries) is ascending, matching the encoder.
    let first_desc = is_reversed.first().copied().unwrap_or(false);

    // CQL lower bound (`>=`/`>` / equality lower) → its physical byte image.
    let cql_lower_bytes = if slice.start.is_empty() {
        None // open CQL lower (value -∞)
    } else {
        match encode(&slice.start) {
            Some(b) => Some(b),
            None => return Ok(None),
        }
    };
    // CQL upper bound (`<=`/`<` / equality upper) → its physical byte image.
    let cql_upper_bytes = if slice.end.is_empty() {
        None // open CQL upper (value +∞)
    } else {
        match encode(&slice.end) {
            Some(b) => Some(b),
            None => return Ok(None),
        }
    };

    let (mut phys_lower, mut phys_upper) = if first_desc {
        // DESC: CQL upper-value bound → physical-lower bytes; CQL lower-value bound
        // → physical-upper bytes. Open CQL upper → physical -∞; open CQL lower →
        // physical +∞.
        let lower = cql_upper_bytes.unwrap_or(neg_inf);
        let upper = cql_lower_bytes.unwrap_or_else(pos_inf);
        (lower, upper)
    } else {
        // ASC: CQL lower-value bound → physical-lower bytes; CQL upper-value bound
        // → physical-upper bytes (the original mapping).
        let lower = cql_lower_bytes.unwrap_or(neg_inf);
        let upper = cql_upper_bytes.unwrap_or_else(pos_inf);
        (lower, upper)
    };

    // Cassandra bound TERMINATORS (`ClusteringPrefix.Kind.asByteComparable`,
    // cassandra-5.0.8 `db/ClusteringPrefix.java:68-81`).
    //
    // A `Rows.db` separator is the byte-comparable image of a full CLUSTERING (all
    // components), while a slice bound may name only a PROPER PREFIX of the
    // clustering key — e.g. `WHERE bucket = 'bo'` on `PRIMARY KEY (pk, bucket, seq)`.
    // Cassandra distinguishes the two sides of such a prefix with a trailing marker:
    // `LT_NEXT_COMPONENT` (0x20) sorts the bound BELOW every clustering that extends
    // it, `GT_NEXT_COMPONENT` (0x60) sorts it ABOVE. Without one, a prefix bound is a
    // bare prefix and therefore always sorts BELOW its extensions — which silently
    // TRUNCATES an upper bound to the first row of the prefix's range (issue #3032:
    // `bucket = 'bo'` returned 12 of 60 rows, the first row-index block only).
    //
    // The physical-LOW side always takes `LT` and the physical-HIGH side always takes
    // `GT`. For the INCLUSIVE kinds (`=`, `>=`, `<=` — after the DESC role swap
    // above) that is byte-exactly what Cassandra emits. For the EXCLUSIVE kinds it is
    // deliberately one marker "wider" than Cassandra's, which can only ever ADD the
    // boundary prefix's own rows to the window: block selection is over-inclusive by
    // block granularity anyway and the post-scan `evaluate_leaf` backstop re-applies
    // the exact CQL bound BY VALUE, so a wider window never changes the result — while
    // a narrower one loses rows that were never decoded. Choosing by side rather than
    // by inclusivity also keeps the DESC swap correct with no second mapping.
    //
    // Appending to the open sentinels is harmless: `-∞` is the empty vector (a lone
    // 0x20 still sorts below every separator, all of which begin with the 0x40
    // `NEXT_COMPONENT` byte, and at/above the stored `ByteComparable.EMPTY` block-0
    // separator exactly as the bare empty bound did), and `+∞` is an all-0xFF run.
    phys_lower.push(OSS50_LT_NEXT_COMPONENT);
    phys_upper.push(OSS50_GT_NEXT_COMPONENT);

    Ok(Some((phys_lower, phys_upper)))
}

/// Counter of `scan_for_key` invocations, used by tests to prove the BTI
/// point-lookup path never falls through to a sequential scan (issue #831).
///
/// Incremented at the top of [`SSTableReader::scan_for_key`] and read via
/// [`SSTableReader::scan_for_key_call_count`]. The increment is a single
/// `Relaxed` atomic add on a cold path, so the runtime cost is negligible; it is
/// not gated behind `cfg(test)` because integration tests in the `tests/`
/// directory compile against the library crate without its `test` cfg.
///
/// [`SSTableReader::scan_for_key`]: crate::storage::sstable::SSTableReader
/// [`SSTableReader::scan_for_key_call_count`]: crate::storage::sstable::SSTableReader::scan_for_key_call_count
pub(crate) static SCAN_FOR_KEY_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Process-global count of *actual* chunk decompressions performed by the three
/// decompressed-chunk read sites wired to the [`DecompressedChunkCache`] (issue
/// #1567): the BIG point read (`get_cached_data`), the BTI target-chunk read
/// (`bti_decompress_and_parse_target`), and the windowed streaming scan
/// (`drain_scan_window_blocking`). Incremented exactly once per real
/// `Compression::decompress` call — i.e. ONLY on a cache miss, after the lookup
/// fails. A repeated read that hits the cache leaves this unchanged, which is the
/// TDD oracle for "the hit skipped decompression".
///
/// Mirrors [`SCAN_FOR_KEY_CALLS`]: a single `Relaxed` add, not `cfg(test)`-gated
/// (integration tests compile the lib without the `test` cfg). Read/reset via
/// [`SSTableReader::decompress_call_count`] / [`SSTableReader::reset_decompress_calls`].
///
/// [`DecompressedChunkCache`]: crate::storage::cache::DecompressedChunkCache
/// [`SSTableReader::decompress_call_count`]: crate::storage::sstable::SSTableReader::decompress_call_count
/// [`SSTableReader::reset_decompress_calls`]: crate::storage::sstable::SSTableReader::reset_decompress_calls
pub(crate) static DECOMPRESS_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Process-global count of *actual* compressed-bytes reads from the backing byte
/// source at the BIG point-read site (`get_cached_data`, issue #1567). Skipped on
/// a cache hit (the cache is consulted before the file read there), so a repeated
/// point read that hits the cache leaves this unchanged — the oracle for "zero
/// underlying reads". Read/reset via [`SSTableReader::chunk_read_call_count`] /
/// [`SSTableReader::reset_chunk_read_calls`].
///
/// [`SSTableReader::chunk_read_call_count`]: crate::storage::sstable::SSTableReader::chunk_read_call_count
/// [`SSTableReader::reset_chunk_read_calls`]: crate::storage::sstable::SSTableReader::reset_chunk_read_calls
pub(crate) static CHUNK_READ_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Compare two table IDs, handling both qualified (keyspace.table) and unqualified (table) formats.
///
/// This function allows flexible matching:
/// - "keyspace.table" matches "keyspace.table" (exact match)
/// - "table" matches "keyspace.table" (unqualified matches qualified)
/// - "keyspace.table" matches "table" (qualified matches unqualified)
/// - "table" matches "table" (exact match)
///
/// This is necessary because:
/// - Dataset mode SSTables store qualified table_ids (e.g., "test_basic.simple_table")
/// - Queries can use either qualified ("test_basic.simple_table") or unqualified ("simple_table") names
/// - Production SSTables may use unqualified table_ids
pub(in crate::storage::sstable::reader) fn table_ids_match(
    entry_table_id: &TableId,
    query_table_id: &TableId,
) -> bool {
    let entry_name = entry_table_id.name();
    let query_name = query_table_id.name();

    // Fast path: exact match
    if entry_name == query_name {
        return true;
    }

    // Extract unqualified table names for comparison
    let entry_unqualified = if let Some(dot_pos) = entry_name.rfind('.') {
        &entry_name[dot_pos + 1..]
    } else {
        entry_name
    };

    let query_unqualified = if let Some(dot_pos) = query_name.rfind('.') {
        &query_name[dot_pos + 1..]
    } else {
        query_name
    };

    // Match if unqualified names are the same
    entry_unqualified == query_unqualified
}

/// Stricter table-id match used by the BTI point-lookup guard (issue #831 review).
///
/// [`table_ids_match`] matches on the unqualified table name, so it treats
/// `ks_a.users` and `ks_b.users` as equal — fine for index lookups that are
/// already scoped to one table, but too permissive as a defensive guard against
/// a fully-qualified wrong-keyspace query. When BOTH ids are qualified
/// (`keyspace.table`), this requires exact `keyspace.table` equality; it only
/// falls back to the permissive unqualified match when one side lacks a
/// keyspace (preserving qualified-vs-unqualified flexibility).
pub(super) fn table_ids_match_strict(entry_table_id: &TableId, query_table_id: &TableId) -> bool {
    let entry_qualified = entry_table_id.name().contains('.');
    let query_qualified = query_table_id.name().contains('.');
    if entry_qualified && query_qualified {
        entry_table_id.name() == query_table_id.name()
    } else {
        table_ids_match(entry_table_id, query_table_id)
    }
}

/// Seek-path table-id consistency check (issue #1284, hardened per #1284 review).
///
/// The single-partition SEEK (`bti_decompress_and_parse_target_all`) builds
/// `entry_table_id` from THIS reader's AUTHORITATIVE serialization header
/// (`{header.keyspace}.{header.table_name}`). `query_table_id` is the query's id.
///
/// `fully_qualified_match` records HOW the manager resolved this reader for the
/// query (`resolve_reader_list`):
///   - `true`  — the query's fully-qualified `keyspace.table` key matched the
///     reader map EXACTLY (path-authoritative: this reader genuinely IS the
///     queried table), or the query was unqualified.
///   - `false` — the query was fully qualified but reached this reader via the
///     bare/unqualified NAME fallback (its keyspace did NOT match any map key).
///
/// [`table_ids_match_strict`] additionally requires the *keyspace* to match when
/// BOTH ids are fully qualified — the correct `get()` defensive guard (#831). For
/// a seek it falsely REJECTS every row when the reader's HEADER keyspace differs
/// from the query keyspace even though resolution was an exact FQ match (e.g. a
/// writer-produced header, or a header whose embedded keyspace is not the
/// path-derived one), silently zeroing the result and forcing a full-scan
/// fallback (#1284).
///
/// The relaxation (accept on a consistent TABLE NAME despite a header-keyspace
/// divergence) is therefore gated on `fully_qualified_match`:
///   - Exact FQ match (or unqualified query): the reader is authoritatively the
///     target table, so a header-keyspace divergence is benign — accept when the
///     table names agree, reject a different table name (the #831 wrong-table
///     guard survives via [`table_ids_match`]). This is #1284's actual goal.
///   - FQ query reached via the bare-name FALLBACK: the keyspace genuinely did
///     NOT match, so a relaxed name-only check could surface rows from ANOTHER
///     keyspace whose table name collides — keep STRICT keyspace matching
///     ([`table_ids_match_strict`]) and REJECT the wrong-keyspace rows.
///
/// This is no-heuristic: it relies only on the authoritative header identity, the
/// query id, and the authoritative resolution mode — never on guessing.
///
/// Used by BOTH the single-partition SEEK (`bti_collect_partition_rows`,
/// `not(tombstones)` only) and the `get()` POINT-LOOKUP decoder
/// (`bti_decompress_and_parse_target`, all feature builds, issue #1321). The
/// point-lookup caller exists in every build, so this helper is NOT gated.
pub(super) fn table_header_consistent_for_seek(
    entry_table_id: &TableId,
    query_table_id: &TableId,
    fully_qualified_match: bool,
) -> bool {
    // Only relax to the name-only consistency check for a fully-qualified query
    // when resolution was an EXACT fully-qualified match (the reader is
    // path-authoritatively the queried table). A fully-qualified query that
    // reached this reader via the bare-name fallback keeps STRICT keyspace
    // matching, so it can never return rows from a different keyspace whose table
    // name collides (#1284 review correctness fix).
    if fully_qualified_match && query_table_id.name().contains('.') {
        table_ids_match(entry_table_id, query_table_id)
    } else {
        table_ids_match_strict(entry_table_id, query_table_id)
    }
}

/// What the BTI point-lookup loop may do with a partition-parse `Err` (issue
/// #3721, roborev job 80) — the sibling of [`bti_lookup_step`] for the state that
/// decision cannot see: the parse having FAILED.
///
/// The loop's catch-all conflated three states — an `Err`, the emit closure never
/// firing, and a partition decoding to a FOREIGN key — and answered all three with
/// the straddle retry or, out of bytes, with absence. Only the first can be a
/// decode failure, and **a decode failure is not absence**: a BTI trie hit is NOT
/// followed by a scan, so `Ok(None)` tells the caller the key does not exist and
/// nothing downstream can tell that apart from a genuine miss. The comment this
/// replaced said so out loud — "could not be parsed" is not "absent".
///
/// # The two classes that cannot mean absence
///
/// A VARIANT match, never a message test (issue #28); both are `ErrorCategory::Data`:
///
/// * [`Error::ColumnDecode`] — a framed row whose column would have been dropped;
/// * [`Error::Corruption`] — the range-marker refusals reach the same parser
///   (`parse_block_emit` delegates to `parse_block_emit_windowed`, whose marker
///   arms propagate since roborev job 78), as does any other structural refusal.
///
/// Everything ELSE keeps the caller's prior behaviour, deliberately. The one that
/// matters is [`Error::Schema`]: a reader with no schema for the queried table
/// CANNOT serve the key, and that soft-miss is what lets the caller try the next
/// reader — the same distinction `super::sequential::is_parse_soft_miss` draws on
/// the BIG point path. Propagating it would hard-fail a multi-reader `get()` that
/// must fall back.
///
/// # Why this takes `more_bytes_may_arrive`
///
/// The #1572 chunk-straddle retry is why this is not a bare `matches!`. A row cut
/// by the window boundary reports `row body exhausted`, which is ITSELF an
/// `Error::ColumnDecode`, so refusing on sight would hard-fail a PRESENT key whose
/// body merely straddles a chunk. While chunks remain the failure is therefore only
/// REMEMBERED and the retry is UNCHANGED; the memory is answered where the bytes
/// run out, by [`point_read_absence_or_remembered`].
///
/// Returns `Err` only when the caller has every byte it will ever have.
pub(super) fn point_read_remember_or_bail(
    parse_result: crate::Result<()>,
    more_bytes_may_arrive: bool,
    undecodable: &mut Option<crate::Error>,
) -> crate::Result<()> {
    let Err(e) = parse_result else {
        return Ok(());
    };
    if !matches!(
        e,
        crate::Error::ColumnDecode { .. } | crate::Error::Corruption(_)
    ) {
        return Ok(());
    }
    if more_bytes_may_arrive {
        *undecodable = Some(e);
        return Ok(());
    }
    Err(e)
}

/// The verdict where a point read runs out of bytes: propagate a REMEMBERED
/// undecodable-partition failure rather than reporting the key ABSENT (issue #3721,
/// roborev job 80). With nothing remembered the caller's prior `Ok(None)` stands,
/// so a genuinely unparseable or absent tail is unchanged.
pub(super) fn point_read_absence_or_remembered<T>(
    undecodable: &mut Option<crate::Error>,
) -> crate::Result<Option<T>> {
    match undecodable.take() {
        Some(e) => Err(e),
        None => Ok(None),
    }
}

/// Per-iteration decision for the BTI chunk-targeted point-lookup loop.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum BtiLookupStep {
    /// The full partition-key prefix is buffered and matches the queried key —
    /// parse the partition.
    Parse,
    /// The header/key prefix straddles a chunk boundary and is not yet fully
    /// buffered — read the next chunk before parsing (chunk-targeted path only).
    PullNextChunk,
    /// Treat the partition as absent: either the buffered key prefix does not
    /// match, or a whole-section window is structurally too short to grow.
    Absent,
}

/// Decide what the BTI point-lookup loop should do for the current window state.
///
/// Pure so the chunk-straddle control flow is unit-testable without a real
/// multi-chunk BTI fixture (DataOffset partitions are narrow and fit in one
/// chunk, so a boundary-straddling header cannot be produced by the available
/// fixtures). Crucially, when the key prefix is not yet buffered on the
/// chunk-targeted path this returns [`BtiLookupStep::PullNextChunk`] — it must
/// NOT lead to parsing a truncated header (issue #831 review).
pub(super) fn bti_lookup_step(
    key_prefix_available: bool,
    key_matches: bool,
    chunk_targeted: bool,
) -> BtiLookupStep {
    if key_prefix_available {
        if key_matches {
            BtiLookupStep::Parse
        } else {
            BtiLookupStep::Absent
        }
    } else if chunk_targeted {
        BtiLookupStep::PullNextChunk
    } else {
        BtiLookupStep::Absent
    }
}

/// Sort a result slice in ascending Cassandra token order.
///
/// The authoritative ordering for SSTable partitions is ascending Murmur3 token, with
/// equal-token ties broken by raw key bytes (lexicographic). This matches the on-disk
/// physical order (spec §5, Appendix B §313) and the write engine's `DecoratedKey::cmp`.
///
/// Computes each key's token once to avoid O(n log n) recomputation inside the comparator.
pub(super) fn sort_by_token_order(results: &mut Vec<(RowKey, ScanRow)>) {
    // Map to (token, RowKey, ScanRow), sort, then reassemble.
    let mut tagged: Vec<(i64, RowKey, ScanRow)> = results
        .drain(..)
        .map(|(k, v)| {
            let t = cassandra_murmur3_token(k.as_bytes());
            (t, k, v)
        })
        .collect();
    tagged.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    results.extend(tagged.into_iter().map(|(_, k, v)| (k, v)));
}

/// Sort `(RowKey, ScanRow, CellMeta)` triples by Cassandra Murmur3 token order.
pub(super) fn sort_by_token_order_with_meta(
    results: &mut Vec<(
        RowKey,
        ScanRow,
        std::collections::HashMap<String, CellWriteMetadata>,
    )>,
) {
    let mut tagged: Vec<(
        i64,
        RowKey,
        ScanRow,
        std::collections::HashMap<String, CellWriteMetadata>,
    )> = results
        .drain(..)
        .map(|(k, v, m)| {
            let t = cassandra_murmur3_token(k.as_bytes());
            (t, k, v, m)
        })
        .collect();
    tagged.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    results.extend(tagged.into_iter().map(|(_, k, v, m)| (k, v, m)));
}

// Unit tests live in the sibling `model_tests.rs` (campsite rule, epic #1116):
// this module's helpers are small and path-agnostic, but their test matrix is
// several times the size of the code it covers.
#[cfg(test)]
#[path = "model_tests.rs"]
mod tests;
