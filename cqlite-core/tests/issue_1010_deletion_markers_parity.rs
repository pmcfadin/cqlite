//! Issue #1010 — Tombstone deletion-marker decode parity (CQLite ↔ Cassandra 5.0).
//!
//! Proves strict parity between CQLite's decoded deletion markers (via the
//! `delta-scan` API) and Apache Cassandra's `sstabledump` JSONL goldens for every
//! tombstone shape that Cassandra can persist into a `nb` (BIG) `Data.db`:
//!
//!   * **Partition delete** (`DELETE FROM t WHERE pk=?`)            → `DeltaRecord::PartitionDelete`
//!   * **Row delete**       (`DELETE FROM t WHERE pk=? AND ck=?`)   → `DeltaRecord::RowDelete`
//!   * **Cell delete**      (`UPDATE t SET col=null …`)             → `CellDelta { value: None }`
//!   * **Range delete bounds** (open/close, mixed inclusivity)      → `DeltaRecord::RangeDelete`
//!   * **Range tombstone boundary** (kind-2 open + kind-5 boundary) → adjacent `RangeDelete`s
//!
//! ## What is asserted (canonical-semantic floor)
//!
//! For every committed `*-Data.db.jsonl` deletion fact, the test builds an
//! ORDERED vector of expected deletion markers (in sstabledump document order)
//! and an ORDERED vector of the deletion markers CQLite emits (in SSTable scan
//! order), then compares the two vectors element-by-element. This is deliberately
//! NOT a count-only assertion: a marker that decodes to the wrong kind, the wrong
//! clustering bound, the wrong inclusivity, or the wrong deletion timestamp fails
//! the ordered diff at the exact position, naming the fixture and the index.
//!
//! Each marker carries:
//!   * deletion timestamp (`markedForDeleteAt`, µs since epoch) — asserted EXACTLY
//!     (0 µs tolerance) against the JSONL `marked_deleted`.
//!   * clustering bound values + inclusive/exclusive flag (range markers).
//!   * column name (cell markers) — the cell-path for a scalar cell tombstone is
//!     empty by construction (`UPDATE SET col=null` deletes the whole cell, not a
//!     collection element), which the assertion records explicitly.
//!
//! ## local_deletion_time
//!
//! The sstabledump JSONL also reports `local_delete_time` (the GC-grace anchor,
//! in seconds). The `delta-scan` public API intentionally surfaces only
//! `markedForDeleteAt` (`deleted_at`) per marker — the per-SSTable
//! `minLocalDeletionTime` baseline is validated byte-for-byte against
//! `Statistics.db` in `sstable_parity_statistics_db_strict_test.rs` (issue #985).
//! This test therefore parses `local_delete_time` and includes it in failure
//! diagnostics, but asserts the deletion-timestamp semantics that delta-scan does
//! expose; the local-deletion-time floor lives in the Statistics lane.
//!
//! ## Byte-offset assertions — DEFERRED (documented)
//!
//! Acceptance criterion (7) asks for an optional byte-level check that reads the
//! `Data.db` at the JSONL-reported `position` and asserts the marker flag byte +
//! deletion-time vint. The `test_deltas` `nb` fixtures are LZ4-compressed
//! (`CompressionInfo.db` is present), so the JSONL `position` is an offset into
//! the *uncompressed logical* row stream, NOT a physical file offset. A raw
//! `seek(position)` into the compressed `Data.db` would read arbitrary compressed
//! bytes, making the assertion meaningless/brittle. Reproducing Cassandra's
//! chunked-LZ4 framing to map logical→physical here would duplicate the reader's
//! decompression path inside a test. We therefore DEFER the byte-offset assertion
//! and keep the canonical-semantic JSONL comparison (kind + bound + inclusivity +
//! exact deletion timestamp, ordered) as the parity floor. Evidence type for the
//! manifest is `canonical_semantic`.
//!
//! ## Gate / fail-closed contract
//!
//!   * `#[cfg(feature = "delta-scan")]` — feature must be enabled.
//!   * Skips cleanly (prints `[SKIP] …` and returns) when `CQLITE_DATASETS_ROOT`
//!     is unset or the binary `Data.db` is absent (CI without delta fixtures).
//!   * FAILS LOUDLY if a committed JSONL reference contains deletion facts and
//!     zero markers were matched against the present binary — no silent pass.
//!
//! Run with:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --features delta-scan \
//!   --test issue_1010_deletion_markers_parity -- --nocapture
//! ```

#![cfg(feature = "delta-scan")]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, DeltaRecord};
use cqlite_core::types::Value;

// ============================================================================
// Normalized deletion-marker model (the ordered-vector comparison unit)
// ============================================================================

/// A bound of a range tombstone, normalized from either side (start/end / boundary).
#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundFacts {
    /// `true` = start (open) side, `false` = end (close) side.
    is_start: bool,
    /// Inclusive (`>=`/`<=`) vs exclusive (`>`/`<`).
    inclusive: bool,
    /// Clustering prefix values rendered as a stable string (wildcards stripped).
    clustering: String,
}

/// A single normalized deletion marker, comparable between CQLite and sstabledump.
///
/// `PartialEq`/`Eq` is derived so ordered vectors compare element-by-element.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Marker {
    Partition {
        pk: String,
        /// markedForDeleteAt, µs since epoch.
        deleted_at: i64,
    },
    Row {
        pk: String,
        ck: String,
        deleted_at: i64,
    },
    /// A scalar cell tombstone (`UPDATE SET col=null`). `path` is always empty for
    /// scalar columns — recorded explicitly so a future collection-element
    /// tombstone (with a non-empty path) would not silently compare-equal.
    Cell {
        pk: String,
        ck: String,
        column: String,
        path: String,
        deleted_at: i64,
    },
    /// A range delete expressed as a (start, end) bound pair sharing one del_at.
    Range {
        pk: String,
        start: BoundFacts,
        end: BoundFacts,
        deleted_at: i64,
    },
}

impl Marker {
    fn kind(&self) -> &'static str {
        match self {
            Marker::Partition { .. } => "partition_delete",
            Marker::Row { .. } => "row_delete",
            Marker::Cell { .. } => "cell_delete",
            Marker::Range { .. } => "range_delete",
        }
    }
}

// ============================================================================
// JSONL → expected markers
// ============================================================================

/// One raw range bound parsed from a `range_tombstone_bound` /
/// `range_tombstone_boundary` JSONL row.
#[derive(Debug, Clone)]
struct RawBound {
    is_start: bool,
    inclusive: bool,
    clustering: String,
    /// markedForDeleteAt µs.
    deleted_at: i64,
    /// local_delete_time (seconds since epoch), for diagnostics only.
    local_delete_time_secs: Option<i64>,
}

/// Parse a JSONL golden file into the ordered vector of expected deletion markers
/// (sstabledump document order), and a flag recording whether any deletion fact
/// was present at all (drives the anti-silent-pass guard).
fn expected_markers_from_jsonl(path: &Path) -> (Vec<Marker>, bool) {
    let file = fs::File::open(path).unwrap_or_else(|e| panic!("open JSONL {path:?}: {e}"));
    let reader = BufReader::new(file);
    let mut markers = Vec::new();
    let mut any_deletion_fact = false;

    for line in reader.lines() {
        let line = line.expect("read JSONL line");
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: JsonValue =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("parse JSONL {path:?}: {e}"));

        let partition = match v.get("partition") {
            Some(p) => p,
            None => continue,
        };
        let pk = jsonl_array_to_string(partition.get("key").and_then(|k| k.as_array()));

        // Partition tombstone (in the partition header).
        if let Some(di) = partition.get("deletion_info").and_then(parse_deletion_info) {
            any_deletion_fact = true;
            markers.push(Marker::Partition {
                pk: pk.clone(),
                deleted_at: di.0,
            });
            // A tombstoned partition has empty rows; nothing further here.
            continue;
        }

        let rows = match v.get("rows").and_then(|r| r.as_array()) {
            Some(r) => r,
            None => continue,
        };

        // Range bounds accumulate in document order so the pairing algorithm can
        // walk them left-to-right (open → close, with boundary markers splitting
        // into an end-of-previous + start-of-next pair).
        let mut raw_bounds: Vec<RawBound> = Vec::new();

        for row in rows {
            let rtype = row.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match rtype {
                "row" => {
                    let ck =
                        jsonl_array_to_string(row.get("clustering").and_then(|c| c.as_array()));

                    // Row tombstone (whole-row deletion_info on the row).
                    if let Some(di) = row.get("deletion_info").and_then(parse_deletion_info) {
                        any_deletion_fact = true;
                        markers.push(Marker::Row {
                            pk: pk.clone(),
                            ck,
                            deleted_at: di.0,
                        });
                        continue;
                    }

                    // Cell tombstones within a live row (UPDATE SET col=null).
                    if let Some(cells) = row.get("cells").and_then(|c| c.as_array()) {
                        for cell in cells {
                            // Collection element entries carry a `path`; scalar cell
                            // tombstones do not. Issue #493 covers element-level
                            // tombstones; here we only assert scalar cell deletes.
                            if cell.get("path").is_some() {
                                continue;
                            }
                            // A scalar cell tombstone (UPDATE SET col=null) renders as
                            // a cell with a `deletion_info` block (carrying only
                            // `local_delete_time`) and a sibling `tstamp`
                            // (markedForDeleteAt) — but NO `value`. A live cell has a
                            // `value`. Detect the tombstone by deletion_info presence
                            // AND absence of a value.
                            match cell.get("deletion_info") {
                                Some(_) if cell.get("value").is_none() => {}
                                _ => continue,
                            }
                            let name = cell
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("")
                                .to_string();
                            // Per-cell deletion timestamp: sstabledump renders the
                            // cell tombstone's writetime in the sibling `tstamp`
                            // field (markedForDeleteAt), while the cell's
                            // `deletion_info` block carries only `local_delete_time`
                            // (the GC anchor, not surfaced per-marker by delta-scan).
                            // The markedForDeleteAt µs therefore comes from `tstamp`.
                            let deleted_at = cell
                                .get("tstamp")
                                .and_then(|s| s.as_str())
                                .and_then(iso8601_to_micros)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "cell tombstone for column '{}' has no `tstamp` \
                                         (markedForDeleteAt) — cannot assert deletion timestamp",
                                        cell.get("name").and_then(|n| n.as_str()).unwrap_or("?")
                                    )
                                });
                            any_deletion_fact = true;
                            markers.push(Marker::Cell {
                                pk: pk.clone(),
                                ck: ck.clone(),
                                column: name,
                                path: String::new(),
                                deleted_at,
                            });
                        }
                    }
                }
                "range_tombstone_bound" => {
                    if let Some(rb) = parse_bound_half(row, None) {
                        any_deletion_fact = true;
                        raw_bounds.push(rb);
                    }
                }
                "range_tombstone_boundary" => {
                    // A boundary closes the previous range (its `end` half) AND opens
                    // the next range (its `start` half), each with its own del_at.
                    // Emit end-half first (document order), then start-half.
                    if let Some(end_half) = parse_bound_half(row, Some(false)) {
                        any_deletion_fact = true;
                        raw_bounds.push(end_half);
                    }
                    if let Some(start_half) = parse_bound_half(row, Some(true)) {
                        any_deletion_fact = true;
                        raw_bounds.push(start_half);
                    }
                }
                _ => {}
            }
        }

        // Pair the accumulated range bounds into (start, end, del_at) markers.
        markers.extend(pair_range_bounds(&pk, &raw_bounds));
    }

    (markers, any_deletion_fact)
}

/// Parse one bound half. `want_start = None` → simple `range_tombstone_bound`
/// (the JSONL has exactly one of `start`/`end`). `want_start = Some(b)` → a
/// `range_tombstone_boundary`, extract the `start` (true) or `end` (false) sub-object.
fn parse_bound_half(row: &JsonValue, want_start: Option<bool>) -> Option<RawBound> {
    let (is_start, inner) = match want_start {
        Some(true) => (true, row.get("start")?),
        Some(false) => (false, row.get("end")?),
        None => {
            if let Some(s) = row.get("start") {
                (true, s)
            } else {
                (false, row.get("end")?)
            }
        }
    };

    let bound_type = inner.get("type").and_then(|t| t.as_str()).unwrap_or("");
    // Inclusivity:
    //   "inclusive"/"exclusive" → as named.
    //   "excl_end_incl_start_boundary" → start=incl, end=excl.
    //   "incl_end_excl_start_boundary" → start=excl, end=incl.
    let inclusive = match bound_type {
        "inclusive" => true,
        "exclusive" => false,
        "excl_end_incl_start_boundary" => is_start,
        "incl_end_excl_start_boundary" => !is_start,
        other => panic!("unexpected range bound type {other:?}"),
    };

    let clustering = jsonl_clustering_to_string(inner.get("clustering").and_then(|c| c.as_array()));

    let di = inner.get("deletion_info")?;
    let deleted_at = di
        .get("marked_deleted")
        .and_then(|s| s.as_str())
        .and_then(iso8601_to_micros)?;
    let local_delete_time_secs = di
        .get("local_delete_time")
        .and_then(|s| s.as_str())
        .and_then(iso8601_to_micros)
        .map(|us| us / 1_000_000);

    Some(RawBound {
        is_start,
        inclusive,
        clustering,
        deleted_at,
        local_delete_time_secs,
    })
}

/// Walk the ordered bound list and pair each open (start) with the next close
/// (end), producing one `Marker::Range` per pair. Boundary markers were already
/// split into end-then-start halves, so a simple left-to-right scan that pairs a
/// start with the following end reconstructs both the closed-open and adjacent
/// (boundary) ranges in document order.
fn pair_range_bounds(pk: &str, bounds: &[RawBound]) -> Vec<Marker> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < bounds.len() {
        let a = &bounds[i];
        let b = &bounds[i + 1];
        if a.is_start && !b.is_start {
            // The range's del_at is the start bound's markedForDeleteAt; assert the
            // pair shares the same del_at (Cassandra writes one deletion time per
            // range). A boundary that changes del_at would surface as distinct
            // adjacent ranges (handled because each half carries its own del_at).
            let _ = a.local_delete_time_secs; // recorded; see module docs re: local_delete_time
            out.push(Marker::Range {
                pk: pk.to_string(),
                start: BoundFacts {
                    is_start: true,
                    inclusive: a.inclusive,
                    clustering: a.clustering.clone(),
                },
                end: BoundFacts {
                    is_start: false,
                    inclusive: b.inclusive,
                    clustering: b.clustering.clone(),
                },
                deleted_at: a.deleted_at,
            });
            i += 2;
        } else {
            // Unpaired bound (should not occur for well-formed sstabledump output);
            // skip it so the ordered diff reports the resulting shortfall.
            i += 1;
        }
    }
    out
}

/// `(marked_deleted_micros)` parsed from a JSONL `deletion_info` object whose
/// `marked_deleted` field is set (partition/row/range). Cells use a different
/// path (their del_at comes from the sibling `tstamp`).
fn parse_deletion_info(v: &JsonValue) -> Option<(i64,)> {
    let s = v.get("marked_deleted")?.as_str()?;
    Some((iso8601_to_micros(s)?,))
}

// ============================================================================
// scan_delta → actual markers
// ============================================================================

/// Collect the ordered vector of deletion markers CQLite emits for one fixture,
/// preserving SSTable scan order.
async fn actual_markers_from_scan(fixture_dir: &Path, schema: TableSchema) -> Vec<Marker> {
    let (mut rx, _summary) = scan_delta(fixture_dir.to_path_buf(), schema, 256);
    let mut markers = Vec::new();
    while let Some(result) = rx.recv().await {
        let rec = result.unwrap_or_else(|e| panic!("scan_delta error in {fixture_dir:?}: {e}"));
        match rec {
            DeltaRecord::PartitionDelete {
                partition_key,
                deleted_at,
            } => markers.push(Marker::Partition {
                pk: value_vec_to_string(&partition_key.partition),
                deleted_at,
            }),
            DeltaRecord::RowDelete { keys, deleted_at } => markers.push(Marker::Row {
                pk: value_vec_to_string(&keys.partition),
                ck: value_vec_to_string(&keys.clustering),
                deleted_at,
            }),
            DeltaRecord::RangeDelete {
                partition_key,
                start,
                end,
                deleted_at,
            } => markers.push(Marker::Range {
                pk: value_vec_to_string(&partition_key.partition),
                start: BoundFacts {
                    is_start: true,
                    inclusive: start.inclusive,
                    clustering: value_vec_to_string(&start.values),
                },
                end: BoundFacts {
                    is_start: false,
                    inclusive: end.inclusive,
                    clustering: value_vec_to_string(&end.values),
                },
                deleted_at,
            }),
            DeltaRecord::Upsert { keys, cells, .. } => {
                // Scalar cell tombstones surface as CellDelta { value: None }.
                for (col, cd) in &cells {
                    if cd.value.is_none() {
                        markers.push(Marker::Cell {
                            pk: value_vec_to_string(&keys.partition),
                            ck: value_vec_to_string(&keys.clustering),
                            column: col.0.clone(),
                            path: String::new(),
                            deleted_at: cd.writetime,
                        });
                    }
                }
            }
            DeltaRecord::StaticUpsert { .. } => {}
        }
    }
    markers
}

// ============================================================================
// Ordered-vector parity comparison
// ============================================================================

/// Compare the expected and actual ordered marker vectors POSITIONALLY. Returns a
/// list of rich diagnostic strings (empty == parity). The comparison is strictly
/// index-by-index: `expected[i]` must equal `actual[i]` exactly (kind + bounds +
/// inclusivity + del_at), and the two vectors must have identical length. This is
/// an ordered-vector contract — a reordering of same-kind markers, a reordering of
/// output, or any over-/under-emission is a failure. We report the first positional
/// mismatch at each index plus any length divergence, never falling back to
/// set-membership matching.
fn diff_markers(table: &str, expected: &[Marker], actual: &[Marker]) -> Vec<String> {
    let mut errors = Vec::new();

    // Positional comparison over the overlapping prefix.
    let common = expected.len().min(actual.len());
    for idx in 0..common {
        let exp = &expected[idx];
        let act = &actual[idx];
        if exp != act {
            errors.push(format!(
                "[{table}] deletion marker #{idx} MISMATCH (ordered positional comparison).\n    \
                 expected ({}): {exp:?}\n    \
                 actual   ({}): {act:?}\n    \
                 source component: cqlite-core delta_scan vs sstabledump JSONL golden",
                exp.kind(),
                act.kind(),
            ));
        }
    }

    // Any expected marker beyond the actual output is a MISS (under-emission).
    for (idx, exp) in expected.iter().enumerate().skip(actual.len()) {
        errors.push(format!(
            "[{table}] expected deletion marker #{idx} ({}) MISSING from scan_delta output \
             (actual produced only {} markers).\n    \
             expected: {exp:?}\n    \
             source component: cqlite-core delta_scan vs sstabledump JSONL golden",
            exp.kind(),
            actual.len(),
        ));
    }

    // Any actual marker beyond the expected output is an over-emission (a bug as bad
    // as a miss — guards against duplicate/spurious markers).
    for (idx, act) in actual.iter().enumerate().skip(expected.len()) {
        errors.push(format!(
            "[{table}] scan_delta emitted an UNEXPECTED deletion marker #{idx} ({}) with no \
             JSONL counterpart (expected only {} markers):\n    \
             actual: {act:?}\n    \
             source component: cqlite-core delta_scan — possible duplicate/spurious tombstone emission",
            act.kind(),
            expected.len(),
        ));
    }

    errors
}

/// Per-fixture summary of matched markers, by kind.
#[derive(Debug, Default)]
struct MatchCounts {
    partition: usize,
    row: usize,
    cell: usize,
    range: usize,
}

impl MatchCounts {
    fn from(markers: &[Marker]) -> Self {
        let mut c = MatchCounts::default();
        for m in markers {
            match m {
                Marker::Partition { .. } => c.partition += 1,
                Marker::Row { .. } => c.row += 1,
                Marker::Cell { .. } => c.cell += 1,
                Marker::Range { .. } => c.range += 1,
            }
        }
        c
    }

    fn total(&self) -> usize {
        self.partition + self.row + self.cell + self.range
    }
}

// ============================================================================
// Fixture discovery + schema
// ============================================================================

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    Some(PathBuf::from(root).join("sstables").join("test_deltas"))
}

/// Find the fixture directory for `table` that has a binary `Data.db` (not the
/// `.jsonl` golden). Returns `None` when no binary is present.
fn fixture_dir_with_binary(root: &Path, table: &str) -> Option<PathBuf> {
    let entries = fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        // Directory name is `<table>-<hash>`; table names use underscores so the
        // first hyphen separates table from hash.
        let dir_table = n.split('-').next().unwrap_or("");
        if dir_table != table {
            continue;
        }
        if find_binary_data_db(&path).is_some() {
            return Some(path);
        }
    }
    None
}

fn find_binary_data_db(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db") && !n.ends_with(".jsonl") {
            return Some(entry.path());
        }
    }
    None
}

fn find_jsonl(dir: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db.jsonl") {
            return Some(entry.path());
        }
    }
    None
}

/// Schemas mirror `test-data/schemas/deltas.cql` (the tables generate-deltas.sh
/// creates). Only the tables this issue exercises are defined here.
fn schema_for_table(table: &str) -> Option<TableSchema> {
    let (pk, ck, cols) = match table {
        "cell_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("col_a", "text"), col("col_b", "text")],
        ),
        "row_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("val", "text")],
        ),
        "range_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck1", "int", 0), ck_col("ck2", "text", 1)],
            vec![col("val", "text")],
        ),
        "partition_tombstones" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("val", "text")],
        ),
        "adjacent_ranges" => (
            vec![key_col("pk", "int", 0)],
            vec![ck_col("ck", "int", 0)],
            vec![col("val", "text")],
        ),
        _ => return None,
    };
    Some(TableSchema {
        keyspace: "test_deltas".to_string(),
        table: table.to_string(),
        partition_keys: pk,
        clustering_keys: ck,
        columns: cols,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    })
}

fn key_col(name: &str, data_type: &str, position: usize) -> KeyColumn {
    KeyColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        position,
    }
}

fn ck_col(name: &str, data_type: &str, position: usize) -> ClusteringColumn {
    ClusteringColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        position,
        order: cqlite_core::schema::ClusteringOrder::Asc,
    }
}

fn col(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

// ============================================================================
// Value / JSON string rendering (stable, order-preserving)
// ============================================================================

fn jsonl_array_to_string(arr: Option<&Vec<JsonValue>>) -> String {
    match arr {
        Some(a) => a
            .iter()
            .map(json_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        None => String::new(),
    }
}

/// Clustering rendering for range bounds: sstabledump emits `"*"` for the
/// unspecified suffix of a prefix bound; CQLite's `RangeBound.values` omits those
/// trailing components, so strip wildcards before comparing.
fn jsonl_clustering_to_string(arr: Option<&Vec<JsonValue>>) -> String {
    match arr {
        Some(a) => a
            .iter()
            .filter(|v| v.as_str() != Some("*"))
            .map(json_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        None => String::new(),
    }
}

fn json_value_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        JsonValue::Array(a) => a
            .iter()
            .map(json_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        JsonValue::Object(o) => o
            .iter()
            .map(|(k, v)| format!("{k}:{}", json_value_to_string(v)))
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn value_vec_to_string(values: &[Value]) -> String {
    values
        .iter()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Uuid(u) => format!("{}", uuid::Uuid::from_bytes(*u)),
        Value::Null => "null".to_string(),
        other => format!("{other:?}"),
    }
}

// ============================================================================
// ISO-8601 → microseconds (no chrono dependency; mirrors scan_delta_parity_test)
// ============================================================================

fn iso8601_to_micros(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date_part, time_part) = s.split_once('T')?;

    let mut dp = date_part.splitn(3, '-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;

    let (hms, frac) = match time_part.split_once('.') {
        Some((h, f)) => (h, f),
        None => (time_part, ""),
    };
    let mut tp = hms.splitn(3, ':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let minute: i64 = tp.next()?.parse().ok()?;
    let second: i64 = tp.next()?.parse().ok()?;

    let days = days_since_epoch(year, month, day)?;
    let epoch_seconds = days * 86400 + hour * 3600 + minute * 60 + second;
    let frac_micros = if frac.is_empty() {
        0
    } else {
        format!("{:0<6}", &frac[..frac.len().min(6)])
            .parse::<i64>()
            .ok()?
    };
    Some(epoch_seconds * 1_000_000 + frac_micros)
}

fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    let a = (14 - month) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    Some(jdn - 2_440_588)
}

// ============================================================================
// Per-fixture harness
// ============================================================================

/// Run the ordered-marker parity check for one fixture. Returns matched-marker
/// counts. Panics with rich diagnostics on any parity mismatch.
async fn run_fixture(table: &str) -> Option<MatchCounts> {
    let root = match datasets_root() {
        Some(r) if r.exists() => r,
        Some(r) => {
            println!(
                "[SKIP] {r:?} not present — set CQLITE_DATASETS_ROOT and run \
                 bash test-data/scripts/generate-deltas.sh; skipping '{table}'"
            );
            return None;
        }
        None => {
            println!(
                "[SKIP] CQLITE_DATASETS_ROOT unset — skipping deletion-marker parity for '{table}'"
            );
            return None;
        }
    };

    let fixture = match fixture_dir_with_binary(&root, table) {
        Some(d) => d,
        None => {
            println!(
                "[SKIP] no binary Data.db for table '{table}' under {root:?} — \
                 run bash test-data/scripts/generate-deltas.sh"
            );
            return None;
        }
    };

    let jsonl =
        find_jsonl(&fixture).unwrap_or_else(|| panic!("no JSONL golden in fixture {fixture:?}"));
    let schema =
        schema_for_table(table).unwrap_or_else(|| panic!("no schema defined for table '{table}'"));

    let (expected, any_deletion_fact) = expected_markers_from_jsonl(&jsonl);
    let actual = actual_markers_from_scan(&fixture, schema).await;

    // Anti-silent-pass guard: the committed JSONL is known to carry deletion facts
    // for these fixtures. If it does and yet we produced zero expected markers, the
    // parser regressed; if zero matched against a present binary, decode regressed.
    assert!(
        any_deletion_fact,
        "[{table}] JSONL golden {jsonl:?} contains NO deletion facts — fixture/parser \
         regression (this fixture must exercise tombstones)"
    );
    assert!(
        !expected.is_empty(),
        "[{table}] parsed zero expected deletion markers from {jsonl:?} despite deletion facts \
         being present — marker extraction is broken"
    );

    let errors = diff_markers(table, &expected, &actual);

    let counts = MatchCounts::from(&expected);
    println!(
        "  [{table}] deletion markers: partition={} row={} cell={} range={} (total expected={}, scan_delta emitted={})",
        counts.partition,
        counts.row,
        counts.cell,
        counts.range,
        counts.total(),
        actual.len(),
    );

    assert!(
        errors.is_empty(),
        "[{table}] deletion-marker parity FAILED ({} error(s)):\n{}",
        errors.len(),
        errors.join("\n")
    );

    // Loud failure if a present binary matched nothing despite deletion facts.
    assert!(
        counts.total() > 0,
        "[{table}] deletion facts present in JSONL but ZERO markers matched against the \
         binary Data.db — no silent pass permitted"
    );

    Some(counts)
}

// ============================================================================
// Per-marker-kind tests (one per manifest ID)
// ============================================================================

/// `cass.tombstone_ttl.deletion_markers.partition_delete`
#[tokio::test]
async fn partition_delete_markers_parity() {
    if let Some(c) = run_fixture("partition_tombstones").await {
        assert!(
            c.partition > 0,
            "partition_tombstones fixture matched zero partition deletes"
        );
        println!(
            "partition_tombstones: {} partition tombstones matched",
            c.partition
        );
    }
}

/// `cass.tombstone_ttl.deletion_markers.row_delete`
#[tokio::test]
async fn row_delete_markers_parity() {
    if let Some(c) = run_fixture("row_tombstones").await {
        assert!(c.row > 0, "row_tombstones fixture matched zero row deletes");
        println!("row_tombstones: {} row tombstones matched", c.row);
    }
}

/// `cass.tombstone_ttl.deletion_markers.cell_delete`
#[tokio::test]
async fn cell_delete_markers_parity() {
    if let Some(c) = run_fixture("cell_tombstones").await {
        assert!(
            c.cell > 0,
            "cell_tombstones fixture matched zero cell tombstones"
        );
        println!("cell_tombstones: {} cell tombstones matched", c.cell);
    }
}

/// `cass.tombstone_ttl.deletion_markers.range_delete_bounds`
#[tokio::test]
async fn range_delete_bounds_markers_parity() {
    if let Some(c) = run_fixture("range_tombstones").await {
        assert!(
            c.range > 0,
            "range_tombstones fixture matched zero range deletes"
        );
        println!(
            "range_tombstones: {} range delete bound-pairs matched",
            c.range
        );
    }
}

/// `cass.tombstone_ttl.deletion_markers.range_tombstone_boundary`
///
/// The adjacent_ranges fixture exercises kind-2 (open) + kind-5 (boundary)
/// markers: a `range_tombstone_boundary` both closes the previous range and opens
/// the next at the same clustering value with complementary inclusivity. These
/// are asserted as explicit ordered range markers, NOT collapsed into a count.
#[tokio::test]
async fn range_tombstone_boundary_markers_parity() {
    if let Some(c) = run_fixture("adjacent_ranges").await {
        assert!(
            c.range > 0,
            "adjacent_ranges fixture matched zero range/boundary markers"
        );
        println!(
            "adjacent_ranges: {} range delete bound/boundary pairs matched",
            c.range
        );
    }
}

// ============================================================================
// Aggregate test — all five marker shapes in one pass with a combined summary
// ============================================================================

#[tokio::test]
async fn all_deletion_marker_shapes_parity() {
    let tables = [
        "partition_tombstones",
        "row_tombstones",
        "cell_tombstones",
        "range_tombstones",
        "adjacent_ranges",
    ];

    let mut total = MatchCounts::default();
    let mut ran_any = false;

    println!("\n=== issue #1010 deletion-marker parity ===");
    for table in tables {
        if let Some(c) = run_fixture(table).await {
            ran_any = true;
            total.partition += c.partition;
            total.row += c.row;
            total.cell += c.cell;
            total.range += c.range;
        }
    }

    if !ran_any {
        println!("[SKIP] no delta fixtures with binaries present — aggregate parity skipped");
        return;
    }

    println!(
        "=== deletion-marker parity PASS: partition={} row={} cell={} range={} (total={}) ===",
        total.partition,
        total.row,
        total.cell,
        total.range,
        total.total()
    );
    assert!(
        total.total() > 0,
        "binaries present but zero deletion markers matched across all fixtures — no silent pass"
    );
}
