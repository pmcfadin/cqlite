//! Issue #1019 (epic #973 Compaction Byte Parity) — compaction paths where
//! correctness depends on schema headers and complex cell identity.
//!
//! This lane EXTENDS the differential compaction harness (issue #819/#1224) with
//! three scenario families that the simple-schema harness fixture cannot reach,
//! anchored to REAL Apache Cassandra 5.0 (`nb`) fixtures (the oracle) under
//! `test_tomb` / `test_collections`:
//!
//!  1. **AC1 — static row whose column is ABSENT from the current schema but still
//!     declared in an input SSTable header.** This is the `#850`
//!     `effective_compaction_schema` path: the schema the operator hands to
//!     `compact_sstables` does NOT mention the static column, yet the input's
//!     `Statistics.db` SerializationHeader declares it static. Compaction must
//!     re-add it from the header so the merger decodes the static cell, the writer
//!     emits the static prelude, AND the compacted output's `Statistics.db`
//!     SerializationHeader re-declares the column AS STATIC. Proven over the real
//!     `dropped_static_col` fixture (`stat_col` is static in gen-1's header).
//!
//!  2. **AC2 — dropped column fully purged by per-cell timestamp must be ABSENT
//!     from the output header without misaligning surviving columns.** This is the
//!     `#847` `for_compaction_output` header-strip path: compacting both real
//!     `dropped_regular_col` generations (gen-1 carries pre-drop `drop_col` cells,
//!     gen-2 is post-drop `keep_col` only) with `drop_col` dropped strictly after
//!     every `drop_col` cell's writetime purges every `drop_col` cell, so the
//!     compacted output's `Statistics.db` header must NOT list `drop_col`, while
//!     `keep_col` survives, stays a regular column, and its cells read back at the
//!     right clustering positions (no misalignment).
//!
//!  3. **AC3 — non-frozen collection elements reconcile by `cell_path`, not
//!     whole-column last-write-wins, and a complex deletion marker does NOT
//!     resurrect older elements.** Proven two ways: (a) per-element substrate
//!     (column, cell_path, ts, ttl, ldt, is_deleted) survives a CQLite compaction
//!     of the real `test_collections.collection_table` fixture byte-faithfully;
//!     (b) the genuine complex-deletion-marker shadowing the real Cassandra
//!     `test_deltas.collection_ops` OVERWRITE scenario (pk=2:
//!     `INSERT tags={old_a,old_b}` then `UPDATE SET tags={only_this}`) produces:
//!     Cassandra emits a per-column `tags` complex deletion marker at the
//!     overwrite timestamp `T` that shadows the older `old_a`/`old_b` elements
//!     (written at `ts < T`) while the `only_this` element (`ts > T`) survives.
//!     After a CQLite compaction the marker is re-emitted (`complex_deletions`
//!     still carries `tags` at `marked_for_delete_at = T`), the covered older
//!     elements stay ABSENT (no resurrection), and only `only_this` (above the
//!     marker) survives — exactly Cassandra's complex-deletion timestamp rule.
//!
//! ## Discipline (project doctrine, no-heuristics #28)
//!  * Decode facts (static/dropped column identity, drop time) come from the
//!    authoritative `Statistics.db` SerializationHeader + the committed golden
//!    dumps — never from a path/name heuristic.
//!  * SKIP cleanly when `CQLITE_DATASETS_ROOT` is unset or a binary `Data.db` is
//!    absent (CI without datasets); PANIC instead under `CQLITE_REQUIRE_FIXTURES=1`
//!    so a gate cannot false-pass on missing data.
//!  * A dataset-dependent assertion FAILS when the dataset is PRESENT but the
//!    expected facts are absent (never a silent 0-rows green).
//!
//! Run:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --features write-support \
//!   --test issue_1019_static_dropped_collection_compaction_parity -- --nocapture
//! ```

#![cfg(feature = "write-support")]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::cql_parser::parse_create_table;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{
    compact_sstables, CellData, ComplexDeletion, MergeStep, RowData,
};
use cqlite_core::storage::write_engine::KWayMerger;
use cqlite_core::types::Value;
use tempfile::TempDir;

// ════════════════════════════════════════════════════════════════════════════
// SECTION 0 — fixture discipline (skip-clean vs require-fixtures strict)
// ════════════════════════════════════════════════════════════════════════════

/// `true` when `CQLITE_REQUIRE_FIXTURES` is truthy: a would-be SKIP becomes a
/// PANIC so a CI gate cannot false-pass on absent data.
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn skip_or_panic(fixture: &str, reason: &str) -> bool {
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but fixture {fixture} is absent — {reason}; \
             fetch/generate it (bash test-data/scripts/fetch-datasets.sh)"
        );
    }
    eprintln!("[SKIP] {reason}");
    false
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
}

/// Find the single fixture dir under `<root>/sstables/<keyspace>` whose name
/// starts with `prefix-`.
fn find_fixture(keyspace: &str, prefix: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks = root.join("sstables").join(keyspace);
    for entry in fs::read_dir(ks).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let n = entry.file_name();
        let name = n.to_str().unwrap_or("");
        if name.starts_with(&format!("{prefix}-")) {
            return Some(path);
        }
    }
    None
}

fn gen_component(dir: &Path, gen: &str, suffix: &str) -> Option<PathBuf> {
    let p = dir.join(format!("{gen}-big-{suffix}"));
    p.is_file().then_some(p)
}

fn gen_has_data(dir: &Path, gen: &str) -> bool {
    gen_component(dir, gen, "Data.db").is_some()
}

/// Copy one generation's BINARY components (no .jsonl / .txt) into a fresh temp
/// dir so a compaction sees only that generation.
fn isolate_generation(dir: &Path, gen: &str) -> TempDir {
    let tmp = TempDir::new().expect("tempdir");
    for entry in fs::read_dir(dir).expect("read fixture dir").flatten() {
        let n = entry.file_name();
        let name = n.to_str().unwrap_or("");
        if name.starts_with(&format!("{gen}-big-"))
            && !name.ends_with(".jsonl")
            && !name.ends_with(".txt")
        {
            fs::copy(entry.path(), tmp.path().join(name)).expect("copy component");
        }
    }
    tmp
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 1 — Statistics.db SerializationHeader column reader (the oracle view)
// ════════════════════════════════════════════════════════════════════════════

/// The regular + static column NAMES CQLite recovers from a binary `Statistics.db`
/// SerializationHeader. This is the byte-derived authoritative view (no path/name
/// heuristic) used to assert AC1 (static re-declared) and AC2 (dropped stripped).
#[derive(Debug, Default, Clone)]
struct HeaderColumns {
    regular: BTreeSet<String>,
    statik: BTreeSet<String>,
}

fn decode_header_columns(stats_path: &Path) -> Option<HeaderColumns> {
    let bytes = fs::read(stats_path).ok()?;
    let (_, stats) = parse_statistics_with_fallback(&bytes, None).ok()?;
    let mut hc = HeaderColumns::default();
    for col in &stats.serialization_header_columns {
        if col.is_static {
            hc.statik.insert(col.name.clone());
        } else {
            hc.regular.insert(col.name.clone());
        }
    }
    Some(hc)
}

/// Decode the SerializationHeader of a compaction OUTPUT (`SSTableInfo::stats_path`).
fn output_header_columns(stats_path: &Path) -> HeaderColumns {
    decode_header_columns(stats_path)
        .expect("compaction output Statistics.db must decode to a SerializationHeader")
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 2 — schema builders (decode contracts; #28 authoritative metadata)
// ════════════════════════════════════════════════════════════════════════════

fn key_col(name: &str, ty: &str) -> KeyColumn {
    KeyColumn {
        name: name.to_string(),
        data_type: ty.to_string(),
        position: 0,
    }
}

fn ck_col(name: &str, ty: &str) -> ClusteringColumn {
    ClusteringColumn {
        name: name.to_string(),
        data_type: ty.to_string(),
        position: 0,
        order: ClusteringOrder::Asc,
    }
}

fn col(name: &str, ty: &str, is_static: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static,
    }
}

/// `dropped_static_col` schema. When `include_static` is false the static
/// `stat_col` is DELIBERATELY ABSENT from the schema — this is the AC1 input that
/// forces `compact_sstables` to re-add it from the input header (#850).
fn dropped_static_schema(include_static: bool) -> TableSchema {
    let mut columns = vec![col("row_col", "text", false)];
    if include_static {
        columns.push(col("stat_col", "text", true));
    }
    TableSchema {
        keyspace: "test_tomb".to_string(),
        table: "dropped_static_col".to_string(),
        partition_keys: vec![key_col("pk", "int")],
        clustering_keys: vec![ck_col("ck", "int")],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// `dropped_regular_col` schema. `drop_time_micros = Some` records `drop_col`'s
/// drop time so the merge purges its cells by per-cell writetime (#847).
fn dropped_regular_schema(drop_time_micros: Option<i64>) -> TableSchema {
    let mut dropped = HashMap::new();
    if let Some(t) = drop_time_micros {
        dropped.insert("drop_col".to_string(), t);
    }
    TableSchema {
        keyspace: "test_tomb".to_string(),
        table: "dropped_regular_col".to_string(),
        partition_keys: vec![key_col("pk", "int")],
        clustering_keys: vec![ck_col("ck", "int")],
        // `drop_col` must remain declared so its cells decode and can be purged
        // (validate_dropped_columns / #847 decode contract).
        columns: vec![
            col("keep_col", "text", false),
            col("drop_col", "text", false),
        ],
        comments: HashMap::new(),
        dropped_columns: dropped,
    }
}

/// DDL for the real `test_collections.collection_table` (matches
/// `test-data/schemas/collections.cql`).
const COLLECTION_TABLE_DDL: &str = "CREATE TABLE test_collections.collection_table (\
    id UUID PRIMARY KEY,\
    tags SET<TEXT>,\
    scores LIST<INT>,\
    properties MAP<TEXT, TEXT>,\
    numbers_set SET<INT>,\
    ordered_values LIST<TIMESTAMP>,\
    metadata_map MAP<TEXT, BIGINT>\
)";

fn collection_schema() -> TableSchema {
    let (_rest, s) = parse_create_table(COLLECTION_TABLE_DDL).expect("parse collection_table DDL");
    s
}

/// DDL for the real `test_deltas.collection_ops` overwrite fixture (matches
/// `test-data/schemas/deltas.cql`). pk=2 is the OVERWRITE scenario that emits a
/// non-frozen `tags` complex deletion marker shadowing the older elements.
const COLLECTION_OPS_DDL: &str = "CREATE TABLE test_deltas.collection_ops (\
    pk INT,\
    ck INT,\
    tags SET<TEXT>,\
    vals LIST<INT>,\
    props MAP<TEXT,TEXT>,\
    PRIMARY KEY (pk, ck)\
)";

fn collection_ops_schema() -> TableSchema {
    let (_rest, s) = parse_create_table(COLLECTION_OPS_DDL).expect("parse collection_ops DDL");
    s
}

/// Locate the `test_deltas.collection_ops` fixture generation that actually
/// carries a binary `nb-1-big-Data.db` (several digest-only generations exist).
fn collection_ops_input() -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks = root.join("sstables").join("test_deltas");
    for entry in fs::read_dir(ks).ok()?.flatten() {
        let path = entry.path();
        let n = entry.file_name();
        let name = n.to_str().unwrap_or("");
        if name.starts_with("collection_ops-") {
            let data = path.join("nb-1-big-Data.db");
            if data.is_file() {
                return Some(data);
            }
        }
    }
    None
}

/// Parse an ISO-8601 `Z` instant to epoch microseconds (drop time derivation).
fn iso8601_to_micros(s: &str) -> Option<i64> {
    // Minimal `YYYY-MM-DDThh:mm:ssZ` parser (no chrono dependency in tests).
    let bytes = s.as_bytes();
    if s.len() < 20 || bytes[4] != b'-' || bytes[10] != b'T' || !s.ends_with('Z') {
        return None;
    }
    let p = |a: usize, b: usize| s[a..b].parse::<i64>().ok();
    let (y, mo, d) = (p(0, 4)?, p(5, 7)?, p(8, 10)?);
    let (h, mi, se) = (p(11, 13)?, p(14, 16)?, p(17, 19)?);
    // Days from civil (Howard Hinnant's algorithm).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + h * 3_600 + mi * 60 + se;
    Some(secs * 1_000_000)
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Text(t) => t.clone(),
        Value::Integer(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 3 — AC1: static column absent from schema, declared in input header
// ════════════════════════════════════════════════════════════════════════════

/// **AC1 — `cqlite.issue_850.static_presence` /
/// `cass.serialization.SerializationHeaderTest.static_and_dropped_columns` (static
/// half).**
///
/// The schema handed to `compact_sstables` does NOT declare the static `stat_col`,
/// but the real `dropped_static_col` gen-1 `Statistics.db` header declares it
/// static. `effective_compaction_schema` (#850) must re-add it from the header so
/// the static cell decodes and is emitted, AND the COMPACTED OUTPUT's
/// `Statistics.db` SerializationHeader must re-declare `stat_col` AS STATIC. This
/// is the Statistics.db serialization-header behavior the AC requires.
#[test]
fn static_column_absent_from_schema_reemitted_in_output_header() {
    let Some(dir) = find_fixture("test_tomb", "dropped_static_col") else {
        skip_or_panic(
            "dropped_static_col fixture",
            "CQLITE_DATASETS_ROOT unset / dropped_static_col absent",
        );
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(
            "dropped_static_col nb-1 Data.db",
            "dropped_static_col nb-1 Data.db absent",
        );
        return;
    }

    // Fixture invariant (oracle): gen-1's header declares stat_col static. If the
    // dataset is present but this is false, FAIL loudly — never silently pass.
    let g1_stats = dir.join("nb-1-big-Statistics.db");
    let g1_header = decode_header_columns(&g1_stats)
        .expect("decode dropped_static_col gen-1 Statistics.db header");
    assert!(
        g1_header.statik.contains("stat_col"),
        "fixture invariant: dropped_static_col gen-1 header must declare stat_col static; \
         got static={:?} regular={:?}",
        g1_header.statik,
        g1_header.regular
    );

    // Compact gen-1 ALONE with a schema that OMITS the static column entirely.
    let g1_only = isolate_generation(&dir, "nb-1");
    let input = g1_only.path().join("nb-1-big-Data.db");
    assert!(input.is_file(), "isolated gen-1 Data.db must exist");

    let schema_without_static = dropped_static_schema(false);
    assert!(
        !schema_without_static.columns.iter().any(|c| c.is_static),
        "AC1 precondition: the compaction schema must NOT declare any static column"
    );

    let out_dir = g1_only.path().join("out");
    let report = block_on(compact_sstables(
        vec![input],
        &out_dir,
        &schema_without_static,
        10_190,
        None,
        None,
        true,
    ))
    .expect("compaction must succeed even though the schema omits the static column");

    // ── AC1 assertion (a): the OUTPUT Statistics.db header re-declares stat_col
    //    as static — proving effective_compaction_schema re-added it from the
    //    input header and the writer emitted the static serialization metadata.
    let out_header = output_header_columns(&report.output.stats_path);
    assert!(
        out_header.statik.contains("stat_col"),
        "AC1 (#850): the compacted output Statistics.db must re-declare stat_col AS STATIC \
         even though the compaction schema omitted it (re-added from the input header); \
         got static={:?} regular={:?}",
        out_header.statik,
        out_header.regular
    );
    assert!(
        !out_header.regular.contains("stat_col"),
        "AC1: stat_col must be re-emitted STATIC, never demoted to a regular column; \
         got regular={:?}",
        out_header.regular
    );

    // ── AC1 assertion (b): the static cell survives and is surfaced as a static
    //    row on read-back of the compacted output (Cassandra-compatible static
    //    prelude). Read with a schema that DOES declare the static so the reader
    //    surfaces it; the assertion is that the cell is present + static-kinded.
    let read_schema = dropped_static_schema(true);
    let mut merger = KWayMerger::new(vec![report.output.data_path.clone()], &read_schema)
        .expect("merger over compacted output");
    let mut saw_static_cell = false;
    loop {
        match merger.step().expect("merge step over compacted output") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in &rows {
                    if let RowData::Live { cells } = &row.row_data {
                        if cells.iter().any(|c| c.column == "stat_col") {
                            saw_static_cell = true;
                        }
                    }
                }
            }
        }
    }
    assert!(
        saw_static_cell,
        "AC1: the static stat_col cell must survive compaction and be readable from the \
         compacted output (static prelude preserved)"
    );

    eprintln!(
        "static_column_absent_from_schema_reemitted_in_output_header PASSED: \
         schema omitted stat_col; compacted output header re-declared it STATIC \
         (out static={:?}) and the static cell survived",
        out_header.statik
    );
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 4 — AC2: dropped column purged → absent from output header, no misalign
// ════════════════════════════════════════════════════════════════════════════

/// **AC2 — `cqlite.issue_847.dropped_column_filter` /
/// `cass.serialization.SerializationHeaderTest.static_and_dropped_columns` (dropped
/// half).**
///
/// Compact both real `dropped_regular_col` generations with `drop_col` dropped
/// strictly AFTER every `drop_col` cell's writetime (T_GEN1=2021-01-01 <
/// drop=2021-06-01). Every `drop_col` cell is purged, so:
///  (a) the compacted output `Statistics.db` header must NOT list `drop_col`
///      (for_compaction_output strip, #847), and
///  (b) `keep_col` survives, stays REGULAR, and reads back at the correct
///      clustering positions (gen-1 ck=1..3, gen-2 ck=4..6) — no misalignment.
#[test]
fn dropped_column_fully_purged_absent_from_output_header_no_misalign() {
    let Some(dir) = find_fixture("test_tomb", "dropped_regular_col") else {
        skip_or_panic(
            "dropped_regular_col fixture",
            "CQLITE_DATASETS_ROOT unset / dropped_regular_col absent",
        );
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(
            "dropped_regular_col nb-1 Data.db",
            "dropped_regular_col nb-1 Data.db absent",
        );
        return;
    }
    // gen-2 is mandatory for this two-generation purge scenario (carries the
    // post-drop keep_col survivors); skipping it silently degrades the test.
    if !gen_has_data(&dir, "nb-2") {
        skip_or_panic(
            "dropped_regular_col nb-2 Data.db",
            "dropped_regular_col nb-2 (post-drop) Data.db absent — both generations are \
             required for the dropped-column header-strip scenario",
        );
        return;
    }

    // Fixture invariant (oracle): gen-1 header declares drop_col, gen-2 does not.
    let g1_header =
        decode_header_columns(&dir.join("nb-1-big-Statistics.db")).expect("decode gen-1 header");
    assert!(
        g1_header.regular.contains("drop_col") && g1_header.regular.contains("keep_col"),
        "fixture invariant: gen-1 header must carry drop_col + keep_col; got {:?}",
        g1_header.regular
    );

    // Isolate both generations into one input dir (newest first not required —
    // compact_sstables sorts by token internally; we pass gen-2 then gen-1).
    let g1_only = isolate_generation(&dir, "nb-1");
    let g2_only = isolate_generation(&dir, "nb-2");
    let inputs = vec![
        g2_only.path().join("nb-2-big-Data.db"),
        g1_only.path().join("nb-1-big-Data.db"),
    ];

    let drop_time_micros = iso8601_to_micros("2021-06-01T00:00:00Z").expect("drop time parse");
    let schema = dropped_regular_schema(Some(drop_time_micros));

    let out_dir = g1_only.path().join("out");
    let report = block_on(compact_sstables(
        inputs, &out_dir, &schema, 10_191, None, None,
        // purge_safe=true: the full set is compacted, so the dropped-cell predicate
        // is applied and the column can be stripped from the output header.
        true,
    ))
    .expect("compaction must succeed");

    // ── AC2 assertion (a): drop_col is ABSENT from the output header; keep_col
    //    remains REGULAR (and not accidentally promoted to static).
    let out_header = output_header_columns(&report.output.stats_path);
    assert!(
        !out_header.regular.contains("drop_col") && !out_header.statik.contains("drop_col"),
        "AC2 (#847): drop_col was fully purged (all cells pre-drop) and MUST be absent from \
         the compacted output Statistics.db header; got regular={:?} static={:?}",
        out_header.regular,
        out_header.statik
    );
    assert!(
        out_header.regular.contains("keep_col"),
        "AC2: keep_col must survive as a regular column in the output header; got regular={:?}",
        out_header.regular
    );

    // ── AC2 assertion (b): keep_col survivors read back at the right clustering
    //    positions — proving the dropped-column strip did not misalign columns.
    let read_schema = dropped_regular_schema(None);
    let mut merger = KWayMerger::new(vec![report.output.data_path.clone()], &read_schema)
        .expect("merger over compacted output");
    let mut keep_survivors: Vec<(String, String)> = Vec::new();
    let mut drop_survivors: Vec<(String, String)> = Vec::new();
    loop {
        match merger.step().expect("merge step over compacted output") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in &rows {
                    let ck = row
                        .clustering_key
                        .as_ref()
                        .and_then(|c| c.columns.first().map(|(_, v)| value_to_string(v)))
                        .unwrap_or_default();
                    if let RowData::Live { cells } = &row.row_data {
                        for c in cells {
                            if c.column == "keep_col" {
                                keep_survivors.push((ck.clone(), value_to_string(&c.value)));
                            } else if c.column == "drop_col" {
                                drop_survivors.push((ck.clone(), value_to_string(&c.value)));
                            }
                        }
                    }
                }
            }
        }
    }
    let _ = &g2_only; // keep gen-2 binaries alive until the read-back completes.

    assert!(
        drop_survivors.is_empty(),
        "AC2: every drop_col cell was pre-drop and MUST be purged; survived: {drop_survivors:?}"
    );
    keep_survivors.sort();
    let mut expected = vec![
        ("1".to_string(), "keep_a_1".to_string()),
        ("2".to_string(), "keep_a_2".to_string()),
        ("3".to_string(), "keep_a_3".to_string()),
        ("4".to_string(), "keep_b_4".to_string()),
        ("5".to_string(), "keep_b_5".to_string()),
        ("6".to_string(), "keep_b_6".to_string()),
    ];
    expected.sort();
    assert_eq!(
        keep_survivors, expected,
        "AC2: keep_col survivors must read back at the right clustering positions with the \
         right values after the dropped-column header strip (no misalignment)"
    );

    eprintln!(
        "dropped_column_fully_purged_absent_from_output_header_no_misalign PASSED: \
         output header={:?} (drop_col stripped); {} keep_col survivors aligned",
        out_header.regular,
        keep_survivors.len()
    );
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 5 — AC3: non-frozen collection per-element reconcile + no resurrection
// ════════════════════════════════════════════════════════════════════════════

/// One per-element complex cell observed via the compaction read path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ElementFacts {
    partition_key: Vec<u8>,
    column: String,
    cell_path: Vec<u8>,
    timestamp: i64,
    ttl: Option<u32>,
    local_deletion_time: Option<i32>,
    is_deleted: bool,
}

/// Walk inputs through the compaction `KWayMerger` and collect every per-element
/// complex cell (those carrying a `cell_path`).
fn per_element_facts(inputs: Vec<PathBuf>, schema: &TableSchema) -> Vec<ElementFacts> {
    let mut merger = KWayMerger::new(inputs, schema).expect("KWayMerger::new");
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in &rows {
                    if let RowData::Live { cells } = &entry.row_data {
                        for c in cells {
                            collect_element(&entry.key.key, c, &mut out);
                        }
                    }
                }
            }
        }
    }
    out.sort();
    out
}

fn collect_element(pk: &[u8], c: &CellData, out: &mut Vec<ElementFacts>) {
    if let Some(path) = &c.cell_path {
        out.push(ElementFacts {
            partition_key: pk.to_vec(),
            column: c.column.clone(),
            cell_path: path.clone(),
            timestamp: c.timestamp,
            ttl: c.ttl,
            local_deletion_time: c.local_deletion_time,
            is_deleted: c.is_deleted,
        });
    }
}

fn collection_table_input() -> Option<PathBuf> {
    let dir = find_fixture("test_collections", "collection_table")?;
    let data = dir.join("nb-1-big-Data.db");
    data.is_file().then_some(data)
}

/// **AC3 (a) — `cqlite.issue_899.per_element_collection_compaction` /
/// `cass.compaction.GcCompactionTest.static_and_complex_columns_survive_gc`
/// (complex half).**
///
/// Per-element collection metadata (column + cell_path + per-element ts/ttl/ldt +
/// is_deleted) reconciles by `cell_path` and survives a CQLite compaction of the
/// REAL `collection_table` SSTable byte-faithfully — i.e. elements are NOT
/// collapsed into a single whole-column last-write-wins value at the row
/// timestamp. The fixture must carry at least one multi-element collection so the
/// equality is not vacuous.
#[test]
fn collection_per_element_metadata_reconciles_by_cell_path_survives_compaction() {
    let Some(input) = collection_table_input() else {
        skip_or_panic(
            "collection_table fixture",
            "CQLITE_DATASETS_ROOT unset / collection_table nb-1 Data.db absent",
        );
        return;
    };
    let schema = collection_schema();

    let input_facts = per_element_facts(vec![input.clone()], &schema);
    assert!(
        !input_facts.is_empty(),
        "fixture invariant: collection_table must surface per-element complex cells; \
         empty means the reader is not emitting per-element substrate"
    );

    // Prove multi-element granularity exists (else equality is vacuous).
    let mut per_col: BTreeMap<(Vec<u8>, String), usize> = BTreeMap::new();
    for f in &input_facts {
        *per_col
            .entry((f.partition_key.clone(), f.column.clone()))
            .or_default() += 1;
    }
    let max_elems = per_col.values().copied().max().unwrap_or(0);
    assert!(
        max_elems >= 2,
        "fixture invariant: need a multi-element collection to prove per-element survival \
         (max elements per (pk,column) = {max_elems})"
    );

    let out_dir = TempDir::new().expect("out dir");
    let report = block_on(compact_sstables(
        vec![input.clone()],
        out_dir.path(),
        &schema,
        10_192,
        None,
        None,
        true,
    ))
    .expect("collection compaction must succeed");

    let output_facts = per_element_facts(vec![report.output.data_path.clone()], &schema);
    assert_eq!(
        output_facts, input_facts,
        "AC3 (#899): per-element collection metadata must reconcile by cell_path and survive \
         compaction byte-faithfully (cell_path + per-element ts/ttl/ldt/is_deleted), NOT be \
         collapsed to a whole-column last-write-wins value"
    );

    let distinct_ts: BTreeSet<i64> = output_facts.iter().map(|f| f.timestamp).collect();
    eprintln!(
        "collection_per_element_metadata_reconciles_by_cell_path_survives_compaction PASSED: \
         {} per-element cells survived identically; {} distinct per-element ts; max {} elements \
         in one (pk,column)",
        output_facts.len(),
        distinct_ts.len(),
        max_elems
    );
}

/// The `tags`-column complex-deletion marker + surviving `tags` elements observed
/// for the OVERWRITE partition (pk=2) after a compaction.
#[derive(Debug, Default)]
struct OverwriteView {
    /// The `tags` complex deletion marker's `marked_for_delete_at` (µs), if any.
    tags_marker: Option<i64>,
    /// Surviving `tags` element `(utf8 cell_path, timestamp, is_deleted)`.
    tags_elements: Vec<(String, i64, bool)>,
}

/// Walk a single compaction output and capture the `tags` complex-deletion marker
/// and surviving `tags` elements for the partition whose `int` pk == `want_pk`.
fn overwrite_view(data: &Path, schema: &TableSchema, want_pk: i32) -> OverwriteView {
    let mut merger = KWayMerger::new(vec![data.to_path_buf()], schema).expect("KWayMerger::new");
    let want_key = want_pk.to_be_bytes().to_vec();
    let mut view = OverwriteView::default();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { key, rows } => {
                if key.key != want_key {
                    continue;
                }
                for entry in &rows {
                    if let Some(cd) = entry
                        .complex_deletions
                        .iter()
                        .find(|c: &&ComplexDeletion| c.column == "tags")
                    {
                        view.tags_marker = Some(cd.marked_for_delete_at);
                    }
                    if let RowData::Live { cells } = &entry.row_data {
                        for c in cells {
                            if c.column != "tags" {
                                continue;
                            }
                            if let Some(path) = &c.cell_path {
                                let p = String::from_utf8_lossy(path).into_owned();
                                view.tags_elements.push((p, c.timestamp, c.is_deleted));
                            }
                        }
                    }
                }
            }
        }
    }
    view.tags_elements.sort();
    view
}

/// **AC3 (b) — a non-frozen-collection COMPLEX DELETION MARKER follows Cassandra's
/// timestamp rule and does NOT resurrect older covered elements
/// (`cqlite.issue_899.collection_complex_deletion_marker` /
/// `cass.compaction.GcCompactionTest` complex-deletion half).**
///
/// Driven by the genuine Cassandra `test_deltas.collection_ops` OVERWRITE
/// partition (pk=2): `INSERT tags={'old_a','old_b'}` at `T_ins` then
/// `UPDATE SET tags={'only_this'}` at `T > T_ins`. Cassandra writes a per-column
/// `tags` COMPLEX DELETION MARKER at `marked_for_delete_at = T` that shadows every
/// `tags` element with `ts < T` (the `old_a`/`old_b` elements), while the
/// `only_this` element (`ts > T`) survives. The fixture already encodes the marker
/// (the oracle); this test pins that a CQLite COMPACTION preserves it:
///
///   * the marker is RE-EMITTED — the compacted output still carries a `tags`
///     `ComplexDeletion` at the same `marked_for_delete_at = T`;
///   * `only_this` (`ts > T`) SURVIVES as the sole `tags` element;
///   * NO element with `ts <= T` is resurrected (the covered older elements stay
///     absent and `only_this` is the only `tags` cell_path present).
///
/// Compaction is idempotent here, so we also re-compact the output and assert the
/// view is unchanged — the marker keeps shadowing across a second pass (no
/// resurrection on repeated compaction).
#[test]
fn collection_complex_deletion_marker_does_not_resurrect_older_elements() {
    let Some(input) = collection_ops_input() else {
        skip_or_panic(
            "collection_ops fixture",
            "CQLITE_DATASETS_ROOT unset / test_deltas.collection_ops nb-1 Data.db absent",
        );
        return;
    };
    let schema = collection_ops_schema();
    const PK: i32 = 2; // the OVERWRITE partition

    // Fixture invariant (oracle): pk=2's INPUT already carries the `tags` complex
    // deletion marker, the surviving `only_this` element above it, and NO older
    // covered element. If the dataset is present but this is false, FAIL loudly.
    let input_view = overwrite_view(&input, &schema, PK);
    let input_marker = input_view.tags_marker.unwrap_or_else(|| {
        panic!(
            "fixture invariant: collection_ops pk={PK} INPUT must carry a `tags` complex \
             deletion marker (the overwrite); got tags_elements={:?}",
            input_view.tags_elements
        )
    });
    assert_eq!(
        input_view.tags_elements,
        vec![("only_this".to_string(), input_marker + 1, false)],
        "fixture invariant: collection_ops pk={PK} input must expose ONLY the `only_this` \
         element written 1µs ABOVE the marker (Cassandra's overwrite); the older old_a/old_b \
         elements (ts < marker) must already be shadowed (absent)"
    );

    // Compact pk=2 (the whole fixture) through CQLite.
    let out_dir = TempDir::new().expect("out dir");
    let report = block_on(compact_sstables(
        vec![input.clone()],
        out_dir.path(),
        &schema,
        10_193,
        None,
        None,
        true,
    ))
    .expect("collection_ops compaction must succeed");

    // ── AC3(b) assertion (1): the `tags` complex deletion marker is RE-EMITTED at
    //    the SAME timestamp — the compaction did not drop the marker.
    let out_view = overwrite_view(&report.output.data_path, &schema, PK);
    assert_eq!(
        out_view.tags_marker,
        Some(input_marker),
        "AC3(b) (#899): the compacted output for pk={PK} must RE-EMIT the `tags` complex \
         deletion marker at marked_for_delete_at={input_marker} (Cassandra overwrite rule); \
         dropping it would let older covered elements resurrect"
    );

    // ── AC3(b) assertion (2): ONLY the element written ABOVE the marker survives;
    //    NO element with ts <= marker is resurrected, and `only_this` is the sole
    //    surviving `tags` cell_path.
    assert_eq!(
        out_view.tags_elements,
        vec![("only_this".to_string(), input_marker + 1, false)],
        "AC3(b) no-resurrection: after compaction the ONLY surviving `tags` element must be \
         `only_this` (ts={} > marker={input_marker}); the older old_a/old_b elements \
         (ts < marker) must NOT be resurrected",
        input_marker + 1
    );
    for (path, ts, _) in &out_view.tags_elements {
        assert!(
            *ts > input_marker,
            "AC3(b) no-resurrection: surviving `tags` element {path:?} has ts={ts} which is \
             NOT strictly above the complex deletion marker {input_marker} — a shadowed \
             (ts <= marker) element was resurrected"
        );
    }

    // ── AC3(b) assertion (3): idempotence — re-compacting the output keeps the
    //    marker shadowing; the view is identical (no resurrection on a 2nd pass).
    let out2_dir = TempDir::new().expect("out2 dir");
    let report2 = block_on(compact_sstables(
        vec![report.output.data_path.clone()],
        out2_dir.path(),
        &schema,
        10_194,
        None,
        None,
        true,
    ))
    .expect("re-compaction must succeed");
    let out2_view = overwrite_view(&report2.output.data_path, &schema, PK);
    assert_eq!(
        out2_view.tags_marker,
        Some(input_marker),
        "AC3(b): the `tags` complex deletion marker must survive a SECOND compaction pass"
    );
    assert_eq!(
        out2_view.tags_elements, out_view.tags_elements,
        "AC3(b) no-resurrection: a second compaction pass must keep exactly the same surviving \
         `tags` element set (idempotent marker shadowing, no resurrection)"
    );

    eprintln!(
        "collection_complex_deletion_marker_does_not_resurrect_older_elements PASSED: \
         pk={PK} `tags` complex deletion marker (marked_for_delete_at={input_marker}) re-emitted \
         through compaction; only `only_this` (ts={}) survived; older shadowed elements stayed \
         absent across two compaction passes (no resurrection)",
        input_marker + 1
    );
}
