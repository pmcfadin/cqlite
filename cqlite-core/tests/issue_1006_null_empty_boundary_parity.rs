//! Issue #1006 (epic #971, Group B): Null-vs-empty & length-boundary value parity.
//!
//! Proves CQLite never confuses three semantically-distinct states and never
//! misaligns the cells that follow a length-boundary value:
//!
//!   * ABSENT — a regular column that was never written (no cell on disk).
//!   * NULL — an explicitly deleted cell (a cell tombstone, `value: None`).
//!   * EMPTY — a present, zero-length value (empty string `''`, empty blob `0x`,
//!     frozen empty collection).
//!
//! and that a boundary-length value (0, 1, 127, 128, 255, 256, 16383, 16384
//! bytes) consumes EXACTLY the right byte range so the cell that follows it
//! decodes intact (neighbour-integrity proof — see `boundary` test below).
//!
//! ## How CQLite is driven
//!
//! Through the public `delta-scan` read API (`scan_delta`), the same reader path
//! the deletion-marker parity lane (#1010) uses. Each scanned `DeltaRecord` is
//! folded into the shared canonical model (`support/canonical_jsonl.rs`) and
//! compared POSITIONALLY against the committed Apache-Cassandra `sstabledump`
//! JSONL goldens via `compare_documents`. The canonical model's
//! `CanonicalValue::{Absent, Null}` and empty-collection / empty-string variants
//! are NEVER conflated by the comparator (asserted in #1009); this lane asserts
//! the on-disk DATA carries the same three-way distinction.
//!
//! ## Why `scan_delta` is the right surface for absent/null/empty
//!
//!   * ABSENT column -> the column id is simply NOT present in the record's
//!     `cells` vector (Cassandra writes no cell), so the canonical row has no
//!     `CanonicalCell` for it — identical to how sstabledump omits it.
//!   * NULL/tombstoned cell -> `CellDelta { value: None, .. }` -> a
//!     `CanonicalCell` whose `value` is `Absent` plus a cell deletion marker,
//!     identical to sstabledump's `{ "name": ..., "deletion_info": {...} }`.
//!   * EMPTY value -> `CellDelta { value: Some(Text("")|Blob([])), .. }` -> a
//!     `CanonicalCell` with `value` = empty text / empty blob.
//!
//! ## Cell ordering
//!
//! `sstabledump` emits a row's cells sorted by (column name, cell path);
//! `scan_delta` emits them in storage order. The comparison is positional, so we
//! sort BOTH sides by (name, path) before comparing — a formatting-only
//! normalization that never hides a value/absent/null/empty difference.
//!
//! ## Manifest entries this lane gates (manifest NOT edited — reported):
//!   * cass.cql_types.boundaries.null_empty_text_blob
//!   * cass.cql_types.boundaries.absent_vs_null_regular_columns
//!   * cass.cql_types.boundaries.empty_collections
//!   * cass.cql_types.boundaries.length_prefix_edges
//!   * cass.data_db_decode.row_preamble_size_mismatch
//!     (the malformed/truncated row-preamble guard is unit-tested directly
//!     against the private row-preamble parser in
//!     `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
//!     — see the `row_preamble_*` tests there — because the preamble decoder is
//!     module-private and reader-free; this file documents that ownership.)
//!
//! Run:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --features delta-scan \
//!   --test issue_1006_null_empty_boundary_parity -- --nocapture
//! ```

#![cfg(feature = "delta-scan")]

#[path = "support/canonical_jsonl.rs"]
mod canonical_jsonl;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use canonical_jsonl::{
    compare_documents, datasets_root, find_golden_jsonl, load_golden_document_with_keys,
    render_diffs, CanonicalCell, CanonicalDocument, CanonicalPartition, CanonicalRow,
    CanonicalValue, CompareCtx, KeyKind, KeySpec, LivenessInfo,
};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};

/// Build the comparator [`KeySpec`] from a [`TableSchema`] so each KEY component
/// is canonicalized against its DECLARED CQL type (issue #971): integral key
/// columns unify sstabledump's `"1"` with CQLite's typed `1`, while a text key
/// `"5"` stays `Text` and never false-matches a numeric `5`.
fn key_spec_from_schema(schema: &TableSchema) -> KeySpec {
    KeySpec {
        partition: schema
            .partition_keys
            .iter()
            .map(|k| KeyKind::from_cql_type(&k.data_type))
            .collect(),
        clustering: schema
            .clustering_keys
            .iter()
            .map(|c| KeyKind::from_cql_type(&c.data_type))
            .collect(),
    }
}
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, CellDelta, DeltaRecord};
use cqlite_core::types::Value;

// ===========================================================================
// Manifest ids owned by this lane (reported, not edited into the manifest).
// ===========================================================================

const MID_NULL_EMPTY: &str = "cass.cql_types.boundaries.null_empty_text_blob";
const MID_ABSENT_VS_NULL: &str = "cass.cql_types.boundaries.absent_vs_null_regular_columns";
const MID_EMPTY_COLLECTIONS: &str = "cass.cql_types.boundaries.empty_collections";
const MID_LENGTH_EDGES: &str = "cass.cql_types.boundaries.length_prefix_edges";

// ===========================================================================
// Schema construction (mirrors test-data/schemas/cql-type-parity.cql, Group B)
// ===========================================================================

fn key_col(name: &str, ty: &str, pos: usize) -> KeyColumn {
    KeyColumn {
        name: name.to_string(),
        data_type: ty.to_string(),
        position: pos,
    }
}

fn ck_col(name: &str, ty: &str, pos: usize) -> ClusteringColumn {
    ClusteringColumn {
        name: name.to_string(),
        data_type: ty.to_string(),
        position: pos,
        order: ClusteringOrder::Asc,
    }
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

fn schema(table: &str, columns: Vec<Column>) -> TableSchema {
    TableSchema {
        keyspace: "test_types".to_string(),
        table: table.to_string(),
        partition_keys: vec![key_col("pk", "int", 0)],
        clustering_keys: vec![ck_col("ck", "int", 0)],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

// ===========================================================================
// Fixture discovery (binary Data.db required — skip cleanly when absent)
// ===========================================================================

/// Locate the fixture dir for `test_types/<table>-<uuid>` that has a BINARY
/// Data.db (not just the committed `.jsonl` golden). `None` when the datasets
/// root is unset or the binary is absent (worktrees ship goldens only).
fn fixture_dir_with_binary(table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks = root.join("sstables").join("test_types");
    for entry in std::fs::read_dir(&ks).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if !(n == table || n.starts_with(&format!("{table}-"))) {
            continue;
        }
        if find_binary_data_db(&path).is_some() {
            return Some(path);
        }
    }
    None
}

fn find_binary_data_db(dir: &Path) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db") && !n.ends_with(".jsonl") {
            return Some(entry.path());
        }
    }
    None
}

/// `CQLITE_REQUIRE_FIXTURES=1` turns a missing binary into a hard failure (CI
/// with the full dataset); otherwise a missing binary is a clean skip.
fn require_fixtures_strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn skip_or_fail(table: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {table} fixture unavailable: {reason}");
    }
    eprintln!("[SKIP] {table}: {reason}");
}

// ===========================================================================
// Value -> CanonicalValue (matches sstabledump JSONL rendering)
// ===========================================================================

/// Render a decoded CQLite [`Value`] into the SHARED [`CanonicalValue`] model so
/// it compares to the sstabledump golden. Blobs render as `0x<hex>` exactly as
/// sstabledump does (so an EMPTY blob is the canonical text `"0x"`, distinct
/// from an empty STRING `""`). The conversion never substitutes a default for a
/// missing value — `Value::Null` maps to `CanonicalValue::Null`, NOT `Absent`.
fn value_to_canonical(v: &Value) -> CanonicalValue {
    match v {
        Value::Null => CanonicalValue::Null,
        Value::Boolean(b) => CanonicalValue::Bool(*b),
        Value::Integer(i) => CanonicalValue::Int(*i as i128),
        Value::BigInt(i) | Value::Counter(i) => CanonicalValue::Int(*i as i128),
        Value::TinyInt(i) => CanonicalValue::Int(*i as i128),
        Value::SmallInt(i) => CanonicalValue::Int(*i as i128),
        Value::Text(s) => CanonicalValue::Text(s.clone()),
        // sstabledump renders blobs as `0x<lowercase-hex>`; empty blob => "0x".
        Value::Blob(b) => CanonicalValue::Text(format!("0x{}", hex::encode(b))),
        Value::List(xs) | Value::Set(xs) | Value::Tuple(xs) => {
            // For the boundary text/blob fixtures these never appear; collections
            // are handled by the dedicated empty_collections semantic test.
            CanonicalValue::List(xs.iter().map(value_to_canonical).collect())
        }
        Value::Map(kvs) => CanonicalValue::Map(
            kvs.iter()
                .map(|(k, val)| (value_to_canonical(k), value_to_canonical(val)))
                .collect(),
        ),
        Value::Frozen(inner) => value_to_canonical(inner),
        // Anything else is rendered debuggably; these types do not appear in the
        // Group B text/blob boundary fixtures, so a divergence here is loud.
        other => CanonicalValue::Text(format!("{other:?}")),
    }
}

// ===========================================================================
// scan_delta -> CanonicalDocument
// ===========================================================================

/// Build a [`CanonicalCell`] for a present cell or a cell tombstone, mirroring
/// sstabledump's shape:
///   * value cell      -> `value: <typed>`, no deletion.
///   * cell tombstone  -> `value: Absent`, deletion present.
fn cell_to_canonical(name: &str, cd: &CellDelta) -> CanonicalCell {
    let (value, deletion) = match &cd.value {
        Some(v) => (value_to_canonical(v), None),
        None => (
            CanonicalValue::Absent,
            Some(canonical_jsonl::CellDeletion {
                // Normalized away on both sides (see normalize_for_value_axis);
                // PRESENCE of this marker is what distinguishes NULL from EMPTY.
                marked_deleted_micros: None,
                local_delete_secs: None,
            }),
        ),
    };
    CanonicalCell {
        name: name.to_string(),
        path: Vec::new(),
        value,
        // Writetime/ttl/local-deletion-time are validated by #694 / #1011; this
        // lane gates VALUE-vs-absent-vs-null only, so those are normalized away
        // (set None) on both sides to keep the comparison focused.
        writetime_micros: None,
        ttl_secs: None,
        deletion,
    }
}

/// Scan a fixture through the public delta-scan reader and fold the SCALAR-cell
/// records into a [`CanonicalDocument`]. Only `Upsert` (regular-row) records are
/// folded; partition/range/row deletes are out of this lane's scope (#1010
/// owns those). Cells are sorted by (name, path) to match sstabledump ordering.
///
/// `value_writetime`/`deletion` detail is intentionally normalized to keep the
/// comparison on the three-way absent/null/empty + value axis. Cell tombstone
/// marked-deleted-at is NOT compared (set None on both sides below).
async fn scan_to_canonical(fixture_dir: &Path, schema: TableSchema) -> CanonicalDocument {
    let (mut rx, _summary) = scan_delta(fixture_dir.to_path_buf(), schema, 256);

    // Group rows by partition key (rendered) preserving first-seen partition order
    // and clustering order within a partition.
    let mut partitions: Vec<CanonicalPartition> = Vec::new();
    let mut pk_index: HashMap<String, usize> = HashMap::new();

    while let Some(result) = rx.recv().await {
        let rec = result.unwrap_or_else(|e| panic!("scan_delta error in {fixture_dir:?}: {e}"));
        if let DeltaRecord::Upsert {
            keys,
            liveness,
            cells,
        } = rec
        {
            let pk: Vec<CanonicalValue> = keys.partition.iter().map(value_to_canonical).collect();
            let clustering: Vec<CanonicalValue> =
                keys.clustering.iter().map(value_to_canonical).collect();

            let mut canon_cells: Vec<CanonicalCell> = cells
                .iter()
                .map(|(col_id, cd)| cell_to_canonical(&col_id.0, cd))
                .collect();
            // sstabledump emits cells sorted by (name, path); normalize order.
            canon_cells.sort_by(|a, b| {
                a.name
                    .cmp(&b.name)
                    .then(a.path_cmp_key().cmp(&b.path_cmp_key()))
            });

            let row = CanonicalRow {
                row_type: "row".to_string(),
                clustering,
                liveness: liveness.as_ref().map(|_| LivenessInfo::default()),
                deletion: None,
                cells: canon_cells,
            };

            let pk_key = render_pk(&pk);
            match pk_index.get(&pk_key) {
                Some(&idx) => partitions[idx].rows.push(row),
                None => {
                    pk_index.insert(pk_key, partitions.len());
                    partitions.push(CanonicalPartition {
                        key: pk,
                        deletion: None,
                        rows: vec![row],
                        range_bounds: Vec::new(),
                    });
                }
            }
        }
    }

    CanonicalDocument { partitions }
}

fn render_pk(pk: &[CanonicalValue]) -> String {
    pk.iter().map(render_cv).collect::<Vec<_>>().join("|")
}

fn render_cv(v: &CanonicalValue) -> String {
    match v {
        CanonicalValue::Int(i) => format!("i{i}"),
        CanonicalValue::Text(s) => format!("t{s}"),
        other => format!("{other:?}"),
    }
}

// A small extension so the sort key for `path` is stable without exposing
// internals of CanonicalValue.
trait PathCmpKey {
    fn path_cmp_key(&self) -> String;
}
impl PathCmpKey for CanonicalCell {
    fn path_cmp_key(&self) -> String {
        self.path
            .iter()
            .map(render_cv)
            .collect::<Vec<_>>()
            .join(",")
    }
}

// ===========================================================================
// Golden normalization: drop the same fields we normalize on the actual side.
// ===========================================================================

/// Normalize an EXPECTED (golden) document onto the same comparison axis as the
/// actual document: keep partition key, clustering, cell name/path, cell value
/// (typed), and the absent/null/empty distinction; null out per-cell writetime /
/// ttl / liveness detail and cell-deletion timestamps (validated by other lanes)
/// so this lane fails ONLY on a value / absent / null / empty divergence.
///
/// This is symmetric with [`scan_to_canonical`] — both sides drop identical
/// fields, so a difference can only come from VALUE or the three-way state.
fn normalize_for_value_axis(doc: &mut CanonicalDocument) {
    for p in &mut doc.partitions {
        p.deletion = None;
        for r in &mut p.rows {
            r.deletion = None;
            r.liveness = r.liveness.as_ref().map(|_| LivenessInfo::default());
            r.cells.sort_by(|a, b| {
                a.name
                    .cmp(&b.name)
                    .then(a.path_cmp_key().cmp(&b.path_cmp_key()))
            });
            for c in &mut r.cells {
                c.writetime_micros = None;
                c.ttl_secs = None;
                // Keep the PRESENCE of a cell deletion (it distinguishes NULL
                // from EMPTY) but normalize its timestamps to None on both sides.
                if let Some(d) = &mut c.deletion {
                    d.marked_deleted_micros = None;
                    d.local_delete_secs = None;
                }
            }
        }
    }
}

// ===========================================================================
// Shared comparison driver
// ===========================================================================

/// Load the golden, scan the fixture, normalize both onto the value axis, and
/// compare positionally. Panics with a precise diff on any divergence.
async fn run_parity(manifest_id: &str, table: &str, schema: TableSchema) {
    let Some(fixture_dir) = fixture_dir_with_binary(table) else {
        skip_or_fail(table, "no binary Data.db under CQLITE_DATASETS_ROOT");
        return;
    };
    let Some(golden_path) = find_golden_jsonl(&fixture_dir) else {
        skip_or_fail(table, "no -Data.db.jsonl golden in fixture dir");
        return;
    };

    let key_spec = key_spec_from_schema(&schema);
    let mut expected = load_golden_document_with_keys(&golden_path, true, &key_spec)
        .unwrap_or_else(|e| panic!("[{table}] golden load failed: {e}"));
    normalize_for_value_axis(&mut expected);

    let mut actual = scan_to_canonical(&fixture_dir, schema).await;
    normalize_for_value_axis(&mut actual);

    let ctx = CompareCtx::new(manifest_id, fixture_dir.clone());
    let diffs = compare_documents(&ctx, &expected, &actual);
    assert!(
        diffs.is_empty(),
        "[{table}] {manifest_id}: {}",
        render_diffs(&diffs)
    );
}

// ===========================================================================
// Lane 1: nb_null_empty_text_blob — absent / null / empty-string / empty-blob /
//         non-empty, with before/after neighbours.
// ===========================================================================

/// `cass.cql_types.boundaries.null_empty_text_blob`.
///
/// Rows (ck 1..4) cover, for `target_text` (text) and `target_blob` (blob):
///   ck=1 both NON-EMPTY ; ck=2 target_text ABSENT ; ck=3 target_text NULL
///   (deleted) ; ck=4 both EMPTY ('' / 0x). `before_col` / `after_col` neighbour
///   every row so a mis-consumed target value would corrupt a neighbour.
///
/// Issue #1077 (FIXED): CQLite now decodes an EMPTY-VALUE cell as the empty
/// value of the column's DECLARED type — an empty `blob` reads back as an empty
/// BLOB (`Blob([])` → golden `"0x"`), an empty text/ascii/varchar as `Text("")`.
/// The golden renders the empty blob as `"0x"` and this comparison verifies it
/// (v5_compressed_legacy.rs HAS_EMPTY_VALUE path). Empty STRING, ABSENT, and NULL
/// are all decoded distinctly (see the absent_vs_null lane below).
#[tokio::test]
async fn null_empty_text_blob_parity() {
    let s = schema(
        "nb_null_empty_text_blob",
        vec![
            col("before_col", "text"),
            col("target_text", "text"),
            col("target_blob", "blob"),
            col("after_col", "text"),
        ],
    );
    run_parity(MID_NULL_EMPTY, "nb_null_empty_text_blob", s).await;
}

/// Companion assertion that does NOT depend on the empty-blob bug: prove the
/// THREE-WAY distinction (absent / null / empty-string) and neighbour integrity
/// directly on the scanned records, independent of the comparator. This MUST
/// pass and locks in that absent != null != empty for text columns even while
/// the empty-blob type bug is open.
#[tokio::test]
async fn null_empty_text_blob_three_way_distinct() {
    let Some(fixture_dir) = fixture_dir_with_binary("nb_null_empty_text_blob") else {
        skip_or_fail("nb_null_empty_text_blob", "no binary Data.db");
        return;
    };
    let s = schema(
        "nb_null_empty_text_blob",
        vec![
            col("before_col", "text"),
            col("target_text", "text"),
            col("target_blob", "blob"),
            col("after_col", "text"),
        ],
    );
    let (mut rx, _summary) = scan_delta(fixture_dir.clone(), s, 256);
    // ck -> (target_text state, neighbour-ok)
    let mut seen: HashMap<i32, String> = HashMap::new();
    while let Some(r) = rx.recv().await {
        let rec = r.expect("scan_delta error");
        if let DeltaRecord::Upsert { keys, cells, .. } = rec {
            let ck = match keys.clustering.first() {
                Some(Value::Integer(i)) => *i,
                _ => continue,
            };
            // Neighbour integrity: before/after must decode to the exact expected
            // non-empty strings in EVERY row. If the target value mis-consumed
            // bytes, a neighbour would be wrong/missing.
            let get = |name: &str| {
                cells
                    .iter()
                    .find(|(c, _)| c.0 == name)
                    .map(|(_, d)| d.value.clone())
            };
            assert_eq!(
                get("before_col"),
                Some(Some(Value::Text("before".to_string()))),
                "ck={ck}: before_col neighbour corrupted (target value mis-consumed bytes?)"
            );
            assert_eq!(
                get("after_col"),
                Some(Some(Value::Text("after".to_string()))),
                "ck={ck}: after_col neighbour corrupted (target value mis-consumed bytes?)"
            );

            // Classify target_text's three-way state.
            let state = match get("target_text") {
                None => "ABSENT".to_string(),
                Some(None) => "NULL".to_string(),
                Some(Some(Value::Text(s))) if s.is_empty() => "EMPTY".to_string(),
                Some(Some(Value::Text(s))) => format!("NONEMPTY({})", s.len()),
                Some(Some(other)) => format!("OTHER({other:?})"),
            };
            seen.insert(ck, state);
        }
    }

    assert_eq!(
        seen.get(&1).map(String::as_str),
        Some("NONEMPTY(8)"),
        "ck=1"
    );
    assert_eq!(
        seen.get(&2).map(String::as_str),
        Some("ABSENT"),
        "ck=2: a never-written column must be ABSENT (no cell), not null/empty"
    );
    assert_eq!(
        seen.get(&3).map(String::as_str),
        Some("NULL"),
        "ck=3: a deleted cell must be NULL (tombstone), not absent/empty"
    );
    assert_eq!(
        seen.get(&4).map(String::as_str),
        Some("EMPTY"),
        "ck=4: an empty-string cell must be EMPTY, not absent/null"
    );
    // Hard proof the three states are mutually distinct.
    let s2 = seen.get(&2).cloned().unwrap_or_default();
    let s3 = seen.get(&3).cloned().unwrap_or_default();
    let s4 = seen.get(&4).cloned().unwrap_or_default();
    assert_ne!(s2, s3, "ABSENT must differ from NULL");
    assert_ne!(s3, s4, "NULL must differ from EMPTY");
    assert_ne!(s2, s4, "ABSENT must differ from EMPTY");
}

// ===========================================================================
// Lane 2: nb_absent_vs_null_regular — never-written vs deleted vs written-empty.
// ===========================================================================

/// `cass.cql_types.boundaries.absent_vs_null_regular_columns`.
///
/// Three rows: ck=1 `reg` ABSENT (never written), ck=2 `reg` NULL (deleted),
/// ck=3 `reg` EMPTY (written ''). `anchor` is the non-empty neighbour. This
/// passes today — the absent/null/empty distinction for TEXT columns is correct
/// end-to-end (and the comparator never conflates the three).
#[tokio::test]
async fn absent_vs_null_regular_parity() {
    let s = schema(
        "nb_absent_vs_null_regular",
        vec![col("anchor", "text"), col("reg", "text")],
    );
    run_parity(MID_ABSENT_VS_NULL, "nb_absent_vs_null_regular", s).await;
}

// ===========================================================================
// Lane 3: nb_empty_collections — empty multicell (stored ABSENT) vs frozen empty
//         (persists) vs non-empty. Asserted semantically (delta-scan does not
//         emit per-element cell paths — Issue #493 — so a positional cell-by-
//         cell comparator run against the sstabledump golden is out of scope).
// ===========================================================================

/// `cass.cql_types.boundaries.empty_collections`.
///
/// ck=1 = the "all empty" row, ck=2 = the "all non-empty" row. Asserts:
///   * FROZEN empty collections PERSIST as a present empty value
///     (`fl=[]`, `fs=set{}`, `fm=map{}`).
///   * MULTICELL empty collections do NOT decode as live non-empty data
///     (Cassandra stores an empty multicell collection as ABSENT / a collection
///     tombstone), so they are distinct from the frozen-empty case.
///   * Non-empty (ck=2) frozen + multicell collections carry their elements.
#[tokio::test]
async fn empty_collections_semantic() {
    let Some(fixture_dir) = fixture_dir_with_binary("nb_empty_collections") else {
        skip_or_fail("nb_empty_collections", "no binary Data.db");
        return;
    };
    let s = schema(
        "nb_empty_collections",
        vec![
            col("ml", "list<int>"),
            col("ms", "set<text>"),
            col("mm", "map<text,int>"),
            col("fl", "frozen<list<int>>"),
            col("fs", "frozen<set<text>>"),
            col("fm", "frozen<map<text,int>>"),
        ],
    );
    let (mut rx, _summary) = scan_delta(fixture_dir.clone(), s, 256);

    let mut rows: HashMap<i32, HashMap<String, Option<Value>>> = HashMap::new();
    while let Some(r) = rx.recv().await {
        let rec = r.expect("scan_delta error");
        if let DeltaRecord::Upsert { keys, cells, .. } = rec {
            let ck = match keys.clustering.first() {
                Some(Value::Integer(i)) => *i,
                _ => continue,
            };
            let entry = rows.entry(ck).or_default();
            for (cid, cd) in &cells {
                entry.insert(cid.0.clone(), cd.value.clone());
            }
        }
    }

    let r1 = rows.get(&1).expect("ck=1 (all-empty row) present");

    // FROZEN empty collections persist as present empty values.
    fn frozen_empty(v: Option<&Option<Value>>) -> bool {
        match v {
            Some(Some(Value::Frozen(inner))) => match inner.as_ref() {
                Value::List(x) => x.is_empty(),
                Value::Set(x) => x.is_empty(),
                Value::Map(x) => x.is_empty(),
                _ => false,
            },
            Some(Some(Value::List(x))) => x.is_empty(),
            Some(Some(Value::Set(x))) => x.is_empty(),
            Some(Some(Value::Map(x))) => x.is_empty(),
            _ => false,
        }
    }

    assert!(
        frozen_empty(r1.get("fl")),
        "frozen empty list `fl` must PERSIST as a present empty value, got {:?}",
        r1.get("fl")
    );
    assert!(
        frozen_empty(r1.get("fs")),
        "frozen empty set `fs` must PERSIST as a present empty value, got {:?}",
        r1.get("fs")
    );
    assert!(
        frozen_empty(r1.get("fm")),
        "frozen empty map `fm` must PERSIST as a present empty value, got {:?}",
        r1.get("fm")
    );

    // MULTICELL empty collections: Cassandra stores them as ABSENT (an empty
    // multicell collection persists only as a collection tombstone, carrying NO
    // elements). The golden shows `ml`/`ms`/`mm` at ck=1 ONLY as deletion_info
    // cells with no value. Therefore the multicell columns must NOT decode as
    // non-empty live data here — distinct from the frozen-empty case above.
    fn nonempty_collection(v: Option<&Option<Value>>) -> bool {
        match v {
            Some(Some(Value::List(x))) => !x.is_empty(),
            Some(Some(Value::Set(x))) => !x.is_empty(),
            Some(Some(Value::Map(x))) => !x.is_empty(),
            Some(Some(Value::Frozen(inner))) => match inner.as_ref() {
                Value::List(x) => !x.is_empty(),
                Value::Set(x) => !x.is_empty(),
                Value::Map(x) => !x.is_empty(),
                _ => false,
            },
            _ => false,
        }
    }
    for mc in ["ml", "ms", "mm"] {
        assert!(
            !nonempty_collection(r1.get(mc)),
            "empty MULTICELL collection `{mc}` must NOT decode as non-empty live data at ck=1 \
             (Cassandra stores empty multicell as ABSENT/tombstone), got {:?}",
            r1.get(mc)
        );
    }

    // ck=2 (non-empty) must carry elements on BOTH frozen and multicell columns —
    // proves the empty case is genuinely empty, not a decode failure.
    let r2 = rows.get(&2).expect("ck=2 (non-empty row) present");
    assert!(
        nonempty_collection(r2.get("fl")),
        "ck=2 frozen list `fl` must carry elements, got {:?}",
        r2.get("fl")
    );
    assert!(
        nonempty_collection(r2.get("ml")),
        "ck=2 multicell list `ml` must carry elements, got {:?}",
        r2.get("ml")
    );

    let _ = MID_EMPTY_COLLECTIONS; // owned manifest id (reported)
}

// ===========================================================================
// Lane 4: nb_length_prefix_edges — boundary lengths 0,1,127,128,255,256,16383,
//         16384 for text & blob, with before/after neighbours. Proves each
//         boundary value consumes the EXACT byte range via neighbour integrity.
// ===========================================================================

/// `cass.cql_types.boundaries.length_prefix_edges`.
///
/// For each of the 8 boundary rows we assert that the decoded `edge_text` /
/// `edge_blob` payload length EXACTLY matches the golden length AND that the
/// after-neighbour `after_col` decodes intact. If the boundary value's
/// length-prefix had been mis-consumed (e.g. a 1-byte VInt read where 2 were
/// needed at the 128 / 16384 boundary), the subsequent cells — including the
/// neighbour — would be corrupted. Intact neighbours therefore PROVE the
/// boundary value consumed the same byte range as the Cassandra reference.
///
/// `edge_text` lengths compare directly. `edge_blob` is also length-checked, but
/// because of the empty-blob type bug (#1006-empty-blob) the LEN-0 blob row
/// surfaces as empty TEXT; the length is still 0 so the boundary/neighbour proof
/// holds — the per-byte length math is what this lane gates.
#[tokio::test]
async fn length_prefix_edges_boundary_and_neighbor() {
    let Some(fixture_dir) = fixture_dir_with_binary("nb_length_prefix_edges") else {
        skip_or_fail("nb_length_prefix_edges", "no binary Data.db");
        return;
    };
    let s = schema(
        "nb_length_prefix_edges",
        vec![
            col("before_col", "text"),
            col("edge_text", "text"),
            col("edge_blob", "blob"),
            col("after_col", "text"),
        ],
    );

    // Expected boundary lengths per ck (from the schema/generator intent and the
    // committed golden): the 8 length edges.
    let expected_len: HashMap<i32, usize> = [
        (1, 0usize),
        (2, 1),
        (3, 127),
        (4, 128),
        (5, 255),
        (6, 256),
        (7, 16383),
        (8, 16384),
    ]
    .into_iter()
    .collect();

    let (mut rx, _summary) = scan_delta(fixture_dir.clone(), s, 256);
    let mut checked = 0usize;
    while let Some(r) = rx.recv().await {
        let rec = r.expect("scan_delta error");
        if let DeltaRecord::Upsert { keys, cells, .. } = rec {
            let ck = match keys.clustering.first() {
                Some(Value::Integer(i)) => *i,
                _ => continue,
            };
            let want = *expected_len.get(&ck).unwrap_or_else(|| {
                panic!("unexpected ck={ck} in nb_length_prefix_edges (lengths are fixed)")
            });

            let get = |name: &str| {
                cells
                    .iter()
                    .find(|(c, _)| c.0 == name)
                    .map(|(_, d)| &d.value)
            };

            // edge_text payload length must equal the boundary length exactly.
            match get("edge_text") {
                Some(Some(Value::Text(s))) => assert_eq!(
                    s.len(),
                    want,
                    "ck={ck}: edge_text decoded length {} != boundary {want} \
                     (length-prefix mis-consumed?)",
                    s.len()
                ),
                other => panic!("ck={ck}: edge_text expected text(len={want}), got {other:?}"),
            }

            // edge_blob payload length must equal the boundary length exactly.
            // (Type may be Text for the len-0 row per #1006-empty-blob; length
            // is the load-bearing assertion for the byte-range proof.)
            let blob_len = match get("edge_blob") {
                Some(Some(Value::Blob(b))) => b.len(),
                Some(Some(Value::Text(s))) => s.len(), // empty-blob bug surfaces as Text
                other => panic!("ck={ck}: edge_blob expected blob(len={want}), got {other:?}"),
            };
            assert_eq!(
                blob_len, want,
                "ck={ck}: edge_blob decoded length {blob_len} != boundary {want} \
                 (length-prefix mis-consumed?)"
            );

            // NEIGHBOUR INTEGRITY: the cell AFTER the boundary value must be intact.
            // A mis-consumed boundary length would corrupt this. before_col/after_col
            // are short non-empty strings; both must decode as the expected literals.
            match get("after_col") {
                Some(Some(Value::Text(s))) => assert!(
                    s.starts_with("after"),
                    "ck={ck}: after_col neighbour corrupted (got {s:?}); \
                     boundary edge value mis-consumed bytes"
                ),
                other => panic!("ck={ck}: after_col neighbour missing/corrupt: {other:?}"),
            }
            match get("before_col") {
                Some(Some(Value::Text(s))) => assert!(
                    s.starts_with("before"),
                    "ck={ck}: before_col neighbour corrupted (got {s:?})"
                ),
                other => panic!("ck={ck}: before_col neighbour missing/corrupt: {other:?}"),
            }

            checked += 1;
        }
    }

    assert_eq!(
        checked, 8,
        "expected all 8 boundary-length rows (0,1,127,128,255,256,16383,16384) in \
         nb_length_prefix_edges, checked {checked}"
    );
    let _ = MID_LENGTH_EDGES; // owned manifest id (reported)
}
