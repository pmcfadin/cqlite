//! Issue #1015 — dropped-column purge and static-row tombstone interactions.
//!
//! This lane proves CQLite handles three load-bearing schema-evolution /
//! tombstone behaviours against real Cassandra 5.0 (`nb`) fixtures, validated
//! positionally against the committed sstabledump JSONL goldens and the
//! `Statistics.db` SerializationHeader:
//!
//!  1. **dropped_column.per_cell_purge** — a column dropped via
//!     `ALTER TABLE DROP` between two flushes. The *older* SSTable
//!     (`dropped_regular_col` gen-1) still declares `drop_col` in its
//!     SerializationHeader, so CQLite must decode gen-1 rows using that
//!     preserved header and evaluate dropped-cell purge by **each cell's own
//!     writetime** (`T_GEN1`, pre-drop), not the containing row's timestamp.
//!     The newer SSTable (gen-2) no longer declares `drop_col`.
//!
//!  2. **static_row.dropped_static_header_preserved** — a *static* column
//!     dropped between flushes (`dropped_static_col`). gen-1's header still
//!     declares `stat_col` as static; CQLite must decode the older SSTable's
//!     static block using the preserved header.
//!
//!  3. **static_row.with_row_cell_range_tombstones** — `static_with_tombstones`
//!     carries a live static cell plus six clustering rows; a row delete
//!     (ck=2), a cell delete (ck=3), and a range delete ([4,5]) shadow the
//!     adjacent rows while the static cell and ck=1 / ck=6 survive. CQLite must
//!     surface the static cell live, the surviving rows live, and the three
//!     tombstones — positionally matching the JSONL.
//!
//!  4. **compaction_merge.static_row.survives_tombstone_gc** — optionally
//!     compacts `static_with_tombstones` (write-support) and asserts the static
//!     row survives tombstone GC and the static column stays declared in the
//!     compacted header (links #850).
//!
//!  5. **Statistics.db SerializationHeader** — parses the committed
//!     `*-Statistics.db.txt` reference dumps and asserts the dropped-column /
//!     static metadata needed to decode the *older* SSTables matches CQLite's
//!     own binary decode (byte-derived `serialization_header_columns`).
//!
//! ## Discipline (issue #1015 + project doctrine)
//!  * SKIP cleanly when `CQLITE_DATASETS_ROOT` is unset or the binary Data.db is
//!    absent (CI without datasets).
//!  * FAIL if the JSONL golden carries facts but ZERO were matched — never a
//!    silent green.
//!  * Ordered POSITIONAL comparison of rows/cells against the golden.
//!  * No path/name heuristics: dropped/static facts are derived from the
//!    Statistics.db SerializationHeader and the JSONL, not the directory name.
//!  * `localDeletionTime` is wall-clock and is compared to the golden value.
//!
//! Run:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --features "delta-scan write-support" \
//!   --test issue_1015_dropped_static_parity -- --nocapture
//! ```

#![cfg(all(feature = "delta-scan", feature = "write-support"))]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, CellDelta, DeltaRecord};
use cqlite_core::types::Value;

// ===========================================================================
// require-fixtures contract (issue #972): opt-in strict mode
// ===========================================================================

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
/// In strict mode, every code path that would otherwise SKIP because the dataset
/// root is unset or a required binary fixture is absent must PANIC instead, so a
/// CI gate cannot false-pass on missing data.
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Skip cleanly (default) or PANIC (strict mode) when a required fixture is absent.
/// `fixture` names the missing component; `reason` is the human-readable skip cause.
fn skip_or_panic(fixture: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but fixture {fixture} is absent — {reason}; \
             fetch/generate it (bash test-data/scripts/fetch-datasets.sh)"
        );
    }
    eprintln!("[SKIP] {reason}");
}

// ===========================================================================
// Dataset path helpers (SKIP-clean when missing)
// ===========================================================================

/// `test_tomb` fixture root, or `None` when `CQLITE_DATASETS_ROOT` is unset.
fn test_tomb_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join("sstables").join("test_tomb");
    if path.exists() {
        Some(path)
    } else {
        None
    }
}

/// Find the single fixture directory whose name starts with `prefix-`.
fn find_fixture(root: &Path, prefix: &str) -> Option<PathBuf> {
    for entry in fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.starts_with(&format!("{prefix}-")) {
            return Some(path);
        }
    }
    None
}

/// Locate a generation's component file (e.g. `nb-1-big-Data.db`).
fn gen_component(dir: &Path, gen: &str, suffix: &str) -> Option<PathBuf> {
    let p = dir.join(format!("{gen}-big-{suffix}"));
    if p.exists() {
        Some(p)
    } else {
        None
    }
}

/// `true` when a generation's Data.db binary is present (else SKIP this gen).
fn gen_has_data(dir: &Path, gen: &str) -> bool {
    gen_component(dir, gen, "Data.db").is_some()
}

// ===========================================================================
// SerializationHeader (Statistics.db) helpers
// ===========================================================================

#[derive(Debug, Default)]
struct HeaderColumns {
    regular: Vec<String>,
    statik: Vec<String>,
}

/// Decode the SerializationHeader columns CQLite recovers from the *binary*
/// `*-Statistics.db` for a given generation. This is the authoritative,
/// byte-derived view used to decode the older SSTable — NOT path-derived.
fn decode_header_columns(dir: &Path, gen: &str) -> Option<HeaderColumns> {
    let stats_path = gen_component(dir, gen, "Statistics.db")?;
    let bytes = fs::read(&stats_path).ok()?;
    let (_, stats) = parse_statistics_with_fallback(&bytes, None).ok()?;
    let mut hc = HeaderColumns::default();
    for col in &stats.serialization_header_columns {
        if col.is_static {
            hc.statik.push(col.name.clone());
        } else {
            hc.regular.push(col.name.clone());
        }
    }
    Some(hc)
}

/// Parse a fact from the committed `*-Statistics.db.txt` reference dump: the
/// `RegularColumns:` and `StaticColumns:` lines (golden, human-authored by
/// Cassandra's `sstablemetadata`). Used to cross-check CQLite's binary decode.
fn reference_header_columns(dir: &Path, gen: &str) -> Option<HeaderColumns> {
    let txt_path = gen_component(dir, gen, "Statistics.db.txt")?;
    let content = fs::read_to_string(&txt_path).ok()?;
    let mut hc = HeaderColumns::default();
    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("RegularColumns:") {
            hc.regular = parse_column_names(rest);
        } else if let Some(rest) = line.strip_prefix("StaticColumns:") {
            hc.statik = parse_column_names(rest);
        }
    }
    Some(hc)
}

/// Parse comma-separated `name:type` entries (type half may contain commas
/// inside parentheses, e.g. `MapType(A,B)`), returning bare column names.
fn parse_column_names(after_colon: &str) -> Vec<String> {
    let s = after_colon.trim();
    if s.is_empty() {
        return Vec::new();
    }
    let mut entries = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                entries.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    entries.push(&s[start..]);
    entries
        .into_iter()
        .filter_map(|e| e.trim().split_once(':').map(|(n, _)| n.trim().to_string()))
        .filter(|n| !n.is_empty())
        .collect()
}

/// Parse the trailing parenthesised integer, e.g. `... (1782341946)`.
fn paren_int(line: &str) -> Option<i64> {
    let open = line.rfind('(')?;
    let close = line[open..].find(')')? + open;
    line[open + 1..close].trim().parse().ok()
}

/// `SSTable min local deletion time` wall-clock seconds from the reference dump,
/// or `None` when the line reports `no tombstones`.
fn reference_min_local_deletion_time(dir: &Path, gen: &str) -> Option<i64> {
    let txt_path = gen_component(dir, gen, "Statistics.db.txt")?;
    let content = fs::read_to_string(&txt_path).ok()?;
    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("SSTable min local deletion time:") {
            if rest.contains("no tombstones") {
                return None;
            }
            return paren_int(rest);
        }
    }
    None
}

// ===========================================================================
// JSONL golden parsing (ordered, positional)
// ===========================================================================

/// ISO-8601 (possibly with fractional seconds) → microseconds since epoch.
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
    let secs = days * 86400 + hour * 3600 + minute * 60 + second;
    let frac_micros = if frac.is_empty() {
        0
    } else {
        let padded = format!("{:0<6}", &frac[..frac.len().min(6)]);
        padded.parse::<i64>().ok()?
    };
    Some(secs * 1_000_000 + frac_micros)
}

fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    let a = (14 - month) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    Some(jdn - 2_440_588)
}

#[derive(Debug, Clone)]
struct GoldenCell {
    name: String,
    /// Present for a live cell value.
    value: Option<String>,
    /// `true` when this cell carries a `deletion_info` (cell tombstone).
    is_tombstone: bool,
}

#[derive(Debug, Clone)]
enum GoldenEntry {
    Static {
        cells: Vec<GoldenCell>,
    },
    Row {
        clustering: Vec<String>,
        is_row_delete: bool,
        row_delete_micros: Option<i64>,
        cells: Vec<GoldenCell>,
    },
    RangeBound {
        is_start: bool,
        clustering: Vec<String>,
        marked_deleted_micros: i64,
    },
}

#[derive(Debug, Clone)]
struct GoldenPartition {
    key: Vec<String>,
    entries: Vec<GoldenEntry>,
}

fn json_scalar(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn parse_golden_cell(v: &JsonValue) -> Option<GoldenCell> {
    // Skip collection sub-element entries (they carry a `path`).
    if v.get("path").is_some() {
        return None;
    }
    let name = v.get("name")?.as_str()?.to_string();
    let value = v.get("value").map(json_scalar);
    let is_tombstone = v.get("deletion_info").is_some();
    Some(GoldenCell {
        name,
        value,
        is_tombstone,
    })
}

fn parse_golden_partition(v: &JsonValue) -> Option<GoldenPartition> {
    let partition = v.get("partition")?;
    let key = partition
        .get("key")?
        .as_array()?
        .iter()
        .map(json_scalar)
        .collect();
    let mut entries = Vec::new();
    for row in v.get("rows")?.as_array()? {
        let ty = row.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match ty {
            "static_block" => {
                let cells = row
                    .get("cells")
                    .and_then(|c| c.as_array())
                    .map(|a| a.iter().filter_map(parse_golden_cell).collect())
                    .unwrap_or_default();
                entries.push(GoldenEntry::Static { cells });
            }
            "row" => {
                let clustering = row
                    .get("clustering")
                    .and_then(|c| c.as_array())
                    .map(|a| a.iter().map(json_scalar).collect())
                    .unwrap_or_default();
                let row_del = row.get("deletion_info").and_then(|di| {
                    di.get("marked_deleted")
                        .and_then(|s| s.as_str())
                        .and_then(iso8601_to_micros)
                });
                let cells = row
                    .get("cells")
                    .and_then(|c| c.as_array())
                    .map(|a| a.iter().filter_map(parse_golden_cell).collect())
                    .unwrap_or_default();
                entries.push(GoldenEntry::Row {
                    clustering,
                    is_row_delete: row.get("deletion_info").is_some(),
                    row_delete_micros: row_del,
                    cells,
                });
            }
            "range_tombstone_bound" => {
                let (is_start, inner) = if let Some(s) = row.get("start") {
                    (true, s)
                } else if let Some(e) = row.get("end") {
                    (false, e)
                } else {
                    continue;
                };
                let clustering = inner
                    .get("clustering")
                    .and_then(|c| c.as_array())
                    .map(|a| a.iter().map(json_scalar).collect())
                    .unwrap_or_default();
                // Fail-loud: a range_tombstone_bound MUST carry a parseable
                // `marked_deleted` timestamp. Coercing a missing/unparseable value
                // to 0 would silently weaken the tombstone deletion-time contract.
                let marked = inner
                    .get("deletion_info")
                    .and_then(|di| di.get("marked_deleted"))
                    .and_then(|s| s.as_str())
                    .and_then(iso8601_to_micros)
                    .unwrap_or_else(|| {
                        panic!(
                            "#1015 golden: range_tombstone_bound (is_start={is_start}, \
                             clustering={clustering:?}) is missing a parseable \
                             deletion_info.marked_deleted timestamp — refusing to default to 0"
                        )
                    });
                entries.push(GoldenEntry::RangeBound {
                    is_start,
                    clustering,
                    marked_deleted_micros: marked,
                });
            }
            _ => {}
        }
    }
    Some(GoldenPartition { key, entries })
}

fn parse_jsonl(path: &Path) -> Vec<GoldenPartition> {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read JSONL {} failed: {e}", path.display()));
    let mut out = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: JsonValue = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("JSON parse error in {}: {e}", path.display()));
        if let Some(p) = parse_golden_partition(&v) {
            out.push(p);
        }
    }
    out
}

// ===========================================================================
// scan_delta collection
// ===========================================================================

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(f)
}

async fn collect_records(dir: &Path, schema: TableSchema) -> Vec<DeltaRecord> {
    let (mut rx, _summary) = scan_delta(dir.to_path_buf(), schema, 256);
    let mut out = Vec::new();
    while let Some(r) = rx.recv().await {
        match r {
            Ok(rec) => out.push(rec),
            Err(e) => panic!("scan_delta error in {}: {e}", dir.display()),
        }
    }
    out
}

fn keys_to_string(values: &[Value]) -> String {
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
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => format!("{other:?}"),
    }
}

// ===========================================================================
// Schema factories (mirror the test_tomb fixtures from their headers)
// ===========================================================================

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

/// `dropped_regular_col` gen-1 schema: the older SSTable still has `drop_col`.
/// `dropped` records the drop time (µs) so the reader can purge by writetime.
fn dropped_regular_schema(include_drop_col: bool, drop_time_micros: Option<i64>) -> TableSchema {
    let mut columns = vec![col("keep_col", "text", false)];
    if include_drop_col {
        columns.push(col("drop_col", "text", false));
    }
    let mut dropped = HashMap::new();
    if let Some(t) = drop_time_micros {
        dropped.insert("drop_col".to_string(), t);
    }
    TableSchema {
        keyspace: "test_tomb".to_string(),
        table: "dropped_regular_col".to_string(),
        partition_keys: vec![key_col("pk", "int")],
        clustering_keys: vec![ck_col("ck", "int")],
        columns,
        comments: HashMap::new(),
        dropped_columns: dropped,
    }
}

/// `dropped_static_col` schema: gen-1 still has the static `stat_col`.
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

/// `static_with_tombstones` schema: static `stat_col` + regular `row_col`.
fn static_tombstones_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_tomb".to_string(),
        table: "static_with_tombstones".to_string(),
        partition_keys: vec![key_col("pk", "int")],
        clustering_keys: vec![ck_col("ck", "int")],
        columns: vec![col("stat_col", "text", true), col("row_col", "text", false)],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

// ===========================================================================
// 1 + 6. dropped_column.per_cell_purge  +  SerializationHeader parity
// ===========================================================================

/// `cass.schema_evolution.dropped_column.per_cell_purge`
/// `cass.tombstone_ttl.static_row.dropped_static_header_preserved` (header half)
///
/// Proves: gen-1's preserved SerializationHeader still declares `drop_col`, so
/// CQLite decodes gen-1 rows (each with `drop_col` written at T_GEN1, pre-drop)
/// and surfaces the dropped cell with its OWN writetime; gen-2's header no
/// longer declares `drop_col`. Cross-checks CQLite's binary header decode
/// against the committed Statistics.db.txt reference (positional column sets).
///
/// This covers DECODE + HEADER preservation. The actual dropped-cell PURGE during
/// merge/compaction is asserted separately by
/// [`dropped_regular_col_per_cell_purge_on_compaction`] (which runs
/// `compact_sstables` over the real fixture and proves `drop_col` cells are
/// physically removed while `keep_col` survivors remain).
#[test]
fn dropped_regular_col_decode_and_header() {
    let Some(root) = test_tomb_root() else {
        skip_or_panic(
            "test_tomb dataset root",
            "CQLITE_DATASETS_ROOT unset / test_tomb absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, "dropped_regular_col") else {
        skip_or_panic(
            "dropped_regular_col fixture",
            "dropped_regular_col fixture not found",
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

    // --- (a) SerializationHeader: gen-1 declares drop_col, gen-2 does not. ---
    let g1_bin = decode_header_columns(&dir, "nb-1").expect("decode gen-1 header");
    let g1_ref = reference_header_columns(&dir, "nb-1").expect("ref gen-1 header");
    assert!(
        g1_ref.regular.contains(&"drop_col".to_string()),
        "fixture invariant: gen-1 reference header must declare drop_col, got {:?}",
        g1_ref.regular
    );
    assert!(
        g1_bin.regular.contains(&"drop_col".to_string()),
        "#1015: CQLite's binary decode of gen-1 SerializationHeader must preserve \
         drop_col (needed to decode the older SSTable); got regular={:?}",
        g1_bin.regular
    );
    // Positional column-set parity between binary decode and reference dump.
    let mut bin_reg = g1_bin.regular.clone();
    let mut ref_reg = g1_ref.regular.clone();
    bin_reg.sort();
    ref_reg.sort();
    assert_eq!(
        bin_reg, ref_reg,
        "gen-1 RegularColumns parity: cqlite={:?} cassandra={:?}",
        g1_bin.regular, g1_ref.regular
    );

    if gen_has_data(&dir, "nb-2") {
        let g2_ref = reference_header_columns(&dir, "nb-2").expect("ref gen-2 header");
        let g2_bin = decode_header_columns(&dir, "nb-2").expect("decode gen-2 header");
        assert!(
            !g2_ref.regular.contains(&"drop_col".to_string()),
            "fixture invariant: gen-2 reference header must NOT declare drop_col, got {:?}",
            g2_ref.regular
        );
        assert!(
            !g2_bin.regular.contains(&"drop_col".to_string()),
            "#1015: gen-2 header must not declare drop_col (post-drop flush); got {:?}",
            g2_bin.regular
        );
    }

    // --- (b) Per-cell timestamp decode of gen-1 rows (drop_col present). ---
    // We point scan_delta at ONLY gen-1 by isolating it into a temp dir, so the
    // newer gen-2 (no drop_col) does not shadow the older cells. We decode with
    // a schema that still has drop_col + a drop_time so the dropped-cell purge
    // predicate has authoritative metadata to evaluate against each cell's OWN
    // writetime — proving the predicate is per-cell, not row-timestamp.
    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let golden = parse_jsonl(&jsonl);
    assert!(!golden.is_empty(), "gen-1 JSONL golden must be non-empty");

    // Gather the golden per-cell writetimes for drop_col (all = T_GEN1 pre-drop).
    let mut golden_drop_writetimes: Vec<i64> = Vec::new();
    let mut golden_keep_count = 0usize;
    let mut golden_drop_count = 0usize;
    for p in &golden {
        for e in &p.entries {
            if let GoldenEntry::Row { cells, .. } = e {
                for c in cells {
                    if c.name == "drop_col" {
                        golden_drop_count += 1;
                        // Row-level liveness tstamp is recorded on the row; the
                        // cell carries the value. T_GEN1 is the row tstamp.
                    } else if c.name == "keep_col" {
                        golden_keep_count += 1;
                    }
                }
            }
        }
    }
    assert!(
        golden_drop_count > 0 && golden_keep_count > 0,
        "fixture invariant: gen-1 golden must carry both drop_col and keep_col cells \
         (drop={golden_drop_count}, keep={golden_keep_count})"
    );

    // Decode gen-1 in isolation, with drop_col still declared and the column
    // dropped at a time strictly AFTER T_GEN1 (2021-01-01) — proving CQLite can
    // decode the pre-drop cells and surface their own writetime. We use a drop
    // time of 2021-06-01 (µs) which is > T_GEN1 and < T_GEN3.
    let drop_time_micros = iso8601_to_micros("2021-06-01T00:00:00Z").unwrap();
    let g1_only = isolate_generation(&dir, "nb-1");
    let schema = dropped_regular_schema(true, Some(drop_time_micros));
    let records = block_on(collect_records(g1_only.path(), schema));

    // Positionally match golden rows against scan_delta Upserts.
    let mut matched_rows = 0usize;
    let mut matched_keep = 0usize;
    let mut matched_drop = 0usize;
    for p in &golden {
        let pk = p.key.join(",");
        for e in &p.entries {
            let GoldenEntry::Row {
                clustering, cells, ..
            } = e
            else {
                continue;
            };
            let ck = clustering.join(",");
            let upsert = records.iter().find_map(|r| match r {
                DeltaRecord::Upsert { keys, cells, .. }
                    if keys_to_string(&keys.partition) == pk
                        && keys_to_string(&keys.clustering) == ck =>
                {
                    Some(cells)
                }
                _ => None,
            });
            let Some(dcells) = upsert else {
                panic!("#1015 per_cell_purge: gen-1 row pk={pk} ck={ck} not decoded by CQLite");
            };
            matched_rows += 1;
            let cell_map: HashMap<&str, &CellDelta> =
                dcells.iter().map(|(id, cd)| (id.0.as_str(), cd)).collect();
            for gc in cells {
                match cell_map.get(gc.name.as_str()) {
                    Some(cd) => {
                        if gc.name == "drop_col" {
                            matched_drop += 1;
                            // The dropped cell's OWN writetime must equal T_GEN1
                            // (2021-01-01), i.e. <= drop_time_micros. This is the
                            // per-cell evidence: the cell carries its own pre-drop
                            // writetime, distinct from any newer sibling.
                            golden_drop_writetimes.push(cd.writetime);
                            assert!(
                                cd.writetime <= drop_time_micros,
                                "drop_col cell writetime {} must be <= drop_time {} \
                                 (pre-drop, per-cell)",
                                cd.writetime,
                                drop_time_micros
                            );
                            let t_gen1 = iso8601_to_micros("2021-01-01T00:00:00Z").unwrap();
                            assert_eq!(
                                cd.writetime, t_gen1,
                                "drop_col cell's OWN writetime must be T_GEN1 (2021-01-01); got {}",
                                cd.writetime
                            );
                        } else if gc.name == "keep_col" {
                            matched_keep += 1;
                            if let Some(Value::Text(t)) = &cd.value {
                                assert_eq!(
                                    t, gc.value.as_ref().unwrap(),
                                    "keep_col value parity at pk={pk} ck={ck}"
                                );
                            }
                        }
                    }
                    None => panic!(
                        "#1015 per_cell_purge: golden cell {} at pk={pk} ck={ck} absent from CQLite decode",
                        gc.name
                    ),
                }
            }
        }
    }

    // FAIL-LOUD: golden carried facts; require positive matches.
    assert!(
        matched_rows > 0 && matched_drop > 0 && matched_keep > 0,
        "#1015 per_cell_purge matched ZERO facts (rows={matched_rows}, \
         drop={matched_drop}, keep={matched_keep}) — silent green guard"
    );

    // All dropped-cell writetimes must be the SAME pre-drop T_GEN1 (per-cell).
    assert!(
        golden_drop_writetimes
            .iter()
            .all(|&w| w == golden_drop_writetimes[0]),
        "all drop_col cells share the pre-drop T_GEN1 writetime: {golden_drop_writetimes:?}"
    );

    println!(
        "[#1015 per_cell_purge] gen-1 header declares drop_col=true; \
         decoded rows={matched_rows} drop_cells={matched_drop} keep_cells={matched_keep}; \
         per-cell drop_col writetime(T_GEN1)={} µs",
        golden_drop_writetimes.first().copied().unwrap_or_default()
    );
}

/// Copy a single generation's components into a fresh temp dir so scan_delta
/// sees ONLY that generation (no cross-generation shadowing).
fn isolate_generation(dir: &Path, gen: &str) -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    for entry in fs::read_dir(dir).expect("read fixture dir").flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.starts_with(&format!("{gen}-big-")) && !n.ends_with(".jsonl") && !n.ends_with(".txt") {
            fs::copy(entry.path(), tmp.path().join(n)).expect("copy component");
        }
    }
    tmp
}

/// `cass.schema_evolution.dropped_column.per_cell_purge` (compaction half)
///
/// The decode/header lane proves CQLite can READ the older SSTable's `drop_col`
/// cells using the preserved SerializationHeader. THIS lane proves the column was
/// actually DROPPED: it compacts BOTH real `dropped_regular_col` generations with
/// a schema whose `dropped_columns` map records `drop_col` dropped at a time
/// strictly AFTER every `drop_col` cell's own pre-drop writetime (T_GEN1 =
/// 2021-01-01), so every `drop_col` cell is purgeable. After compaction `drop_col`
/// must be ABSENT from the output while the `keep_col` survivors (gen-1 ck=1..3 and
/// gen-2 ck=4..6) remain intact.
///
/// Per #922 (`pre_drop_cell_purged_while_post_drop_sibling_survives`), CQLite's
/// `compact_sstables` DOES purge dropped-column cells by each cell's own writetime,
/// so this is a genuine purge assertion — not a gap pin.
#[test]
fn dropped_regular_col_per_cell_purge_on_compaction() {
    use cqlite_core::storage::write_engine::merge::{compact_sstables, MergeStep, RowData};
    use cqlite_core::storage::write_engine::KWayMerger;

    let Some(root) = test_tomb_root() else {
        skip_or_panic(
            "dropped_regular_col fixture",
            "CQLITE_DATASETS_ROOT unset / test_tomb absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, "dropped_regular_col") else {
        skip_or_panic(
            "dropped_regular_col fixture",
            "dropped_regular_col fixture directory not found",
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

    // Collect the real input Data.db generations to compact (gen-1 has drop_col +
    // keep_col; gen-2 is post-drop, keep_col only). Compaction needs a clean input
    // directory, so isolate each generation's binary components.
    let g1_only = isolate_generation(&dir, "nb-1");
    let mut inputs: Vec<PathBuf> = Vec::new();
    let mut collect_data = |d: &Path| {
        for entry in fs::read_dir(d).expect("read isolated dir").flatten() {
            let p = entry.path();
            if p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|n| n.ends_with("Data.db"))
            {
                inputs.push(p);
            }
        }
    };
    collect_data(g1_only.path());
    // Both generations are mandatory for this mirrored two-generation scenario:
    // gen-1 carries the pre-drop drop_col cells to be purged; gen-2 carries the
    // post-drop keep_col survivors (ck=4..6). Skipping gen-2 would silently
    // degrade this into a single-generation test, so require it (panics under
    // CQLITE_REQUIRE_FIXTURES=1, skips cleanly otherwise).
    if !gen_has_data(&dir, "nb-2") {
        skip_or_panic(
            "dropped_regular_col nb-2 Data.db",
            "dropped_regular_col nb-2 (post-drop) Data.db absent — both generations \
             are required for the two-generation dropped-column purge scenario",
        );
        return;
    }
    let g2_only = isolate_generation(&dir, "nb-2");
    collect_data(g2_only.path());
    assert!(
        !inputs.is_empty(),
        "expected at least one input Data.db to compact"
    );

    // drop_col is dropped at 2021-06-01 — strictly AFTER every drop_col cell's own
    // writetime (T_GEN1 = 2021-01-01), so every drop_col cell is purgeable.
    let drop_time_micros = iso8601_to_micros("2021-06-01T00:00:00Z").unwrap();
    let schema = dropped_regular_schema(true, Some(drop_time_micros));

    let out_dir = g1_only.path().join("out");
    // purge_safe=true so the dropped-cell predicate is applied during compaction.
    let report = block_on(compact_sstables(
        inputs, &out_dir, &schema, 1015, None, None, true,
    ))
    .expect("compaction must succeed");

    // Read the compacted output with a schema that still DECLARES drop_col (so the
    // reader CAN surface it if present) but carries NO drop map (so the reader does
    // not re-apply any filter) — reflecting what was physically written.
    let read_schema = dropped_regular_schema(true, None);
    let mut merger = KWayMerger::new(vec![report.output.data_path], &read_schema)
        .expect("merger over compacted output");

    let mut drop_col_survivors: Vec<(Option<String>, String)> = Vec::new(); // (ck, value)
    let mut keep_col_survivors: Vec<(String, String)> = Vec::new(); // (ck, value)
    loop {
        match merger.step().expect("merge step over compacted output") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in rows {
                    let ck = row
                        .clustering_key
                        .as_ref()
                        .and_then(|c| c.columns.first().map(|(_, v)| value_to_string(v)));
                    if let RowData::Live { cells } = &row.row_data {
                        for c in cells {
                            if c.column == "drop_col" {
                                if let Value::Text(t) = &c.value {
                                    drop_col_survivors.push((ck.clone(), t.clone()));
                                }
                            } else if c.column == "keep_col" {
                                if let Value::Text(t) = &c.value {
                                    keep_col_survivors
                                        .push((ck.clone().unwrap_or_default(), t.clone()));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // PURGE assertion: every drop_col cell was written at T_GEN1 < drop_time, so the
    // compacted output must contain ZERO live drop_col cells.
    assert!(
        drop_col_survivors.is_empty(),
        "#1015 per_cell_purge: dropped `drop_col` cells (all written at T_GEN1=2021-01-01, \
         < drop_time=2021-06-01) MUST be purged during compaction, but these survived: {:?}",
        drop_col_survivors
    );

    // keep_col survivors must remain correct. gen-1 contributes ck=1..3
    // (keep_a_1..keep_a_3); gen-2 (always present, asserted above) contributes
    // ck=4..6 (keep_b_4..6).
    let _ = &g2_only; // keep the isolated gen-2 dir alive until the merge completes
    keep_col_survivors.sort();
    let mut expected: Vec<(String, String)> = vec![
        ("1".to_string(), "keep_a_1".to_string()),
        ("2".to_string(), "keep_a_2".to_string()),
        ("3".to_string(), "keep_a_3".to_string()),
        ("4".to_string(), "keep_b_4".to_string()),
        ("5".to_string(), "keep_b_5".to_string()),
        ("6".to_string(), "keep_b_6".to_string()),
    ];
    expected.sort();
    assert_eq!(
        keep_col_survivors, expected,
        "#1015 per_cell_purge: keep_col survivors must remain intact after purging drop_col"
    );

    println!(
        "[#1015 per_cell_purge ON COMPACTION] compacted dropped_regular_col with \
         drop_col dropped@2021-06-01 (purge_safe=true): drop_col cells purged=ALL \
         (0 survived); keep_col survivors={}",
        keep_col_survivors.len()
    );
}

// ===========================================================================
// 2. static_row.dropped_static_header_preserved
// ===========================================================================

/// `cass.tombstone_ttl.static_row.dropped_static_header_preserved`
///
/// Proves: `dropped_static_col` gen-1's SerializationHeader still declares the
/// static `stat_col`; CQLite decodes the older SSTable's static block correctly
/// using the preserved header, and gen-2 (post-drop) no longer declares it.
#[test]
fn dropped_static_col_header_preserved() {
    let Some(root) = test_tomb_root() else {
        skip_or_panic(
            "test_tomb dataset root",
            "CQLITE_DATASETS_ROOT unset / test_tomb absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, "dropped_static_col") else {
        skip_or_panic(
            "dropped_static_col fixture",
            "dropped_static_col fixture not found",
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

    // --- Header: gen-1 declares stat_col static (binary + reference parity). ---
    let g1_bin = decode_header_columns(&dir, "nb-1").expect("decode gen-1 header");
    let g1_ref = reference_header_columns(&dir, "nb-1").expect("ref gen-1 header");
    assert!(
        g1_ref.statik.contains(&"stat_col".to_string()),
        "fixture invariant: gen-1 reference must declare static stat_col, got {:?}",
        g1_ref.statik
    );
    assert!(
        g1_bin.statik.contains(&"stat_col".to_string()),
        "#1015: CQLite's binary decode of gen-1 must preserve static stat_col \
         (needed to decode the older SSTable); got static={:?}",
        g1_bin.statik
    );

    if gen_has_data(&dir, "nb-2") {
        let g2_ref = reference_header_columns(&dir, "nb-2").expect("ref gen-2 header");
        let g2_bin = decode_header_columns(&dir, "nb-2").expect("decode gen-2 header");
        assert!(
            !g2_ref.statik.contains(&"stat_col".to_string()),
            "fixture invariant: gen-2 must NOT declare static stat_col, got {:?}",
            g2_ref.statik
        );
        assert!(
            !g2_bin.statik.contains(&"stat_col".to_string()),
            "#1015: gen-2 header must not declare static stat_col (post-drop); got {:?}",
            g2_bin.statik
        );
    }

    // --- Decode gen-1 static block: CQLite must surface the static cell. ---
    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let golden = parse_jsonl(&jsonl);
    let golden_static: Vec<&GoldenCell> = golden
        .iter()
        .flat_map(|p| &p.entries)
        .filter_map(|e| match e {
            GoldenEntry::Static { cells } => Some(cells),
            _ => None,
        })
        .flatten()
        .collect();
    assert!(
        !golden_static.is_empty(),
        "fixture invariant: gen-1 golden must contain a static_block cell"
    );

    let g1_only = isolate_generation(&dir, "nb-1");
    // Decode with the gen-1 schema (static still present).
    let schema = dropped_static_schema(true);
    let records = block_on(collect_records(g1_only.path(), schema));

    let mut matched_static = 0usize;
    for gc in &golden_static {
        let found = records.iter().any(|r| match r {
            DeltaRecord::StaticUpsert { cells, .. } => cells.iter().any(|(id, cd)| {
                id.0.as_str() == gc.name
                    && matches!((&cd.value, &gc.value), (Some(Value::Text(t)), Some(g)) if t == g)
            }),
            _ => false,
        });
        assert!(
            found,
            "#1015 dropped_static_header_preserved: static cell {} (value {:?}) not \
             decoded from gen-1 using the preserved header",
            gc.name, gc.value
        );
        matched_static += 1;
    }
    assert!(
        matched_static > 0,
        "#1015: matched ZERO static cells from gen-1 — silent green guard"
    );

    println!(
        "[#1015 dropped_static_header_preserved] gen-1 header declares static stat_col; \
         decoded static cells={matched_static}"
    );
}

// ===========================================================================
// 3. static_row.with_row_cell_range_tombstones
// ===========================================================================

/// `cass.tombstone_ttl.static_row.with_row_cell_range_tombstones`
///
/// Proves (positionally against the golden): in `static_with_tombstones` the
/// static cell stays LIVE while adjacent clustering rows are deleted by a row
/// tombstone (ck=2), a cell tombstone (ck=3), and a range tombstone ([4,5]);
/// ck=1 and ck=6 survive. `localDeletionTime` wall-clock matches the golden.
#[test]
fn static_with_tombstones_interactions() {
    let Some(root) = test_tomb_root() else {
        skip_or_panic(
            "test_tomb dataset root",
            "CQLITE_DATASETS_ROOT unset / test_tomb absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, "static_with_tombstones") else {
        skip_or_panic(
            "static_with_tombstones fixture",
            "static_with_tombstones fixture not found",
        );
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(
            "static_with_tombstones nb-1 Data.db",
            "static_with_tombstones nb-1 Data.db absent",
        );
        return;
    }

    // Header invariant: stat_col is static and the SSTable carries a tombstone
    // (local deletion time != "no tombstones"), validated against the golden.
    let hdr = decode_header_columns(&dir, "nb-1").expect("decode header");
    assert!(
        hdr.statik.contains(&"stat_col".to_string()),
        "fixture invariant: header must declare static stat_col, got {:?}",
        hdr.statik
    );
    let ldt = reference_min_local_deletion_time(&dir, "nb-1");
    assert!(
        ldt.is_some(),
        "fixture invariant: static_with_tombstones must record a min local deletion \
         time (wall-clock) — it carries tombstones"
    );

    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let golden = parse_jsonl(&jsonl);
    assert!(!golden.is_empty(), "golden must be non-empty");

    let schema = static_tombstones_schema();
    let records = block_on(collect_records(dir.as_path(), schema));

    // Build the golden expectation set (ordered).
    let mut g_static_live = 0usize;
    let mut g_live_rows: Vec<String> = Vec::new(); // clustering of surviving rows
    let mut g_row_deletes: Vec<(String, i64)> = Vec::new(); // (ck, marked µs)
    let mut g_cell_deletes: Vec<String> = Vec::new(); // ck with a cell tombstone
    let mut g_range_bounds: Vec<(bool, String, i64)> = Vec::new();
    for p in &golden {
        for e in &p.entries {
            match e {
                GoldenEntry::Static { cells } => {
                    g_static_live += cells.iter().filter(|c| !c.is_tombstone).count();
                }
                GoldenEntry::Row {
                    clustering,
                    is_row_delete,
                    row_delete_micros,
                    cells,
                } => {
                    let ck = clustering.join(",");
                    if *is_row_delete {
                        // Fail-loud: a row tombstone MUST carry a parseable
                        // marked_deleted timestamp; defaulting to 0 would mask a
                        // missing/unparseable deletion time.
                        let marked = row_delete_micros.unwrap_or_else(|| {
                            panic!(
                                "#1015 golden: row tombstone at ck={ck} is missing a parseable \
                                 deletion_info.marked_deleted timestamp — refusing to default to 0"
                            )
                        });
                        g_row_deletes.push((ck, marked));
                    } else if cells.iter().any(|c| c.is_tombstone) {
                        g_cell_deletes.push(ck);
                    } else if cells.iter().any(|c| c.value.is_some()) {
                        g_live_rows.push(ck);
                    }
                }
                GoldenEntry::RangeBound {
                    is_start,
                    clustering,
                    marked_deleted_micros,
                } => {
                    g_range_bounds.push((*is_start, clustering.join(","), *marked_deleted_micros));
                }
            }
        }
    }

    // Golden invariants for this specific fixture (fail-loud if it drifts).
    assert_eq!(g_static_live, 1, "expected exactly 1 live static cell");
    assert_eq!(g_live_rows, vec!["1", "6"], "expected ck=1 and ck=6 live");
    assert_eq!(g_row_deletes.len(), 1, "expected 1 row tombstone (ck=2)");
    assert_eq!(g_cell_deletes, vec!["3"], "expected cell tombstone at ck=3");
    assert_eq!(g_range_bounds.len(), 2, "expected a [4,5] range tombstone");

    // ---- Now assert CQLite's scan_delta output matches, positionally. ----

    // (a) Static cell live.
    let static_live = records.iter().any(|r| {
        matches!(r, DeltaRecord::StaticUpsert { cells, .. }
            if cells.iter().any(|(id, cd)| id.0.as_str() == "stat_col" && cd.value.is_some()))
    });
    assert!(
        static_live,
        "#1015: static stat_col must stay LIVE alongside row/cell/range tombstones"
    );

    // (b) Surviving live rows ck=1 and ck=6.
    for ck in &g_live_rows {
        let live = records.iter().any(|r| {
            matches!(r, DeltaRecord::Upsert { keys, cells, .. }
                if keys_to_string(&keys.clustering) == *ck
                    && cells.iter().any(|(_, cd)| cd.value.is_some()))
        });
        assert!(live, "#1015: surviving row ck={ck} must be live");
    }

    // (c) Row tombstone ck=2 present with matching marked-deleted time.
    let mut matched_row_del = 0usize;
    for (ck, marked) in &g_row_deletes {
        let found = records.iter().any(|r| {
            matches!(r, DeltaRecord::RowDelete { keys, deleted_at }
                if keys_to_string(&keys.clustering) == *ck && *deleted_at == *marked)
        });
        assert!(
            found,
            "#1015: row tombstone ck={ck} (marked {marked}) not surfaced by CQLite"
        );
        matched_row_del += 1;
    }

    // (d) Cell tombstone ck=3: the row is present but row_col is a tombstone.
    let mut matched_cell_del = 0usize;
    for ck in &g_cell_deletes {
        let found = records.iter().any(|r| {
            matches!(r, DeltaRecord::Upsert { keys, cells, .. }
                if keys_to_string(&keys.clustering) == *ck
                    && cells.iter().any(|(id, cd)| id.0.as_str() == "row_col" && cd.value.is_none()))
        });
        assert!(
            found,
            "#1015: cell tombstone at ck={ck} (row_col deleted) not surfaced; \
             the row should appear with a value-less row_col cell delta"
        );
        matched_cell_del += 1;
    }

    // (e) Range tombstone [4,5]: a RangeDelete whose bounds cover ck 4..=5.
    let range_marked = g_range_bounds[0].2;
    let matched_range = records.iter().any(|r| {
        matches!(r, DeltaRecord::RangeDelete { start, end, deleted_at, .. }
            if *deleted_at == range_marked
                && keys_to_string(&start.values) == "4"
                && keys_to_string(&end.values) == "5")
    });
    assert!(
        matched_range,
        "#1015: range tombstone [4,5] (marked {range_marked}) not surfaced by CQLite"
    );

    // FAIL-LOUD aggregate guard.
    assert!(
        matched_row_del > 0 && matched_cell_del > 0 && matched_range,
        "#1015 static_with_tombstones matched ZERO tombstone facts — silent green guard"
    );

    println!(
        "[#1015 static_with_tombstones] static cell live=1; rows shadowed: \
         row_del(ck=2)={matched_row_del} cell_del(ck=3)={matched_cell_del} \
         range_del([4,5])={matched_range}; live rows ck=1,6; \
         min_local_deletion_time(wall-clock)={} s",
        ldt.unwrap_or_default()
    );
}

// ===========================================================================
// 4. compaction_merge.static_row.survives_tombstone_gc
// ===========================================================================

/// `cass.compaction_merge.static_row.survives_tombstone_gc`
///
/// Links #850: compacting the REAL `static_with_tombstones` fixture (a partition
/// carrying a live static cell + a row tombstone, a cell tombstone, and a range
/// tombstone over its clustering rows) with GC enabled must keep the static cell
/// alive through tombstone GC, while the row-deleted clustering row (ck=2) is
/// reconciled away. We drive `compact_sstables(purge=true)` over the authoritative
/// fixture (which lays the static cell into a proper partition static row, unlike
/// a synthetic single-mutation write) and read the compacted output back.
#[test]
fn static_row_survives_tombstone_gc_on_compaction() {
    use cqlite_core::storage::write_engine::merge::{
        compact_sstables, KWayMerger, MergeStep, RowData,
    };

    let Some(root) = test_tomb_root() else {
        skip_or_panic(
            "test_tomb dataset root",
            "CQLITE_DATASETS_ROOT unset / test_tomb absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, "static_with_tombstones") else {
        skip_or_panic(
            "static_with_tombstones fixture",
            "static_with_tombstones fixture not found",
        );
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(
            "static_with_tombstones nb-1 Data.db",
            "static_with_tombstones nb-1 Data.db absent",
        );
        return;
    }

    let schema = static_tombstones_schema();

    // Isolate the fixture's components (the merger / compactor wants a clean
    // input directory) and discover its Data.db.
    let isolated = isolate_generation(&dir, "nb-1");
    let mut inputs = Vec::new();
    for entry in fs::read_dir(isolated.path())
        .expect("read isolated")
        .flatten()
    {
        let p = entry.path();
        if p.file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|n| n.ends_with("Data.db"))
        {
            inputs.push(p);
        }
    }
    assert_eq!(
        inputs.len(),
        1,
        "expected exactly one input Data.db to compact"
    );

    // Pre-compaction sanity: the static cell is live and ck=2 is a row tombstone.
    {
        let mut pre = KWayMerger::new(inputs.clone(), &schema).expect("pre merger");
        let mut pre_static = false;
        let mut pre_row_tomb_ck2 = false;
        while let MergeStep::Partition { rows, .. } = pre.step().expect("pre step") {
            for row in rows {
                let ck = row
                    .clustering_key
                    .as_ref()
                    .and_then(|c| c.columns.first().map(|(_, v)| value_to_string(v)));
                match &row.row_data {
                    RowData::Live { cells } => {
                        if cells.iter().any(|c| {
                            c.column == "stat_col"
                                && matches!(&c.value, Value::Text(t) if t == "surviving_static")
                        }) {
                            pre_static = true;
                        }
                    }
                    RowData::Tombstone { .. } => {
                        if ck.as_deref() == Some("2") {
                            pre_row_tomb_ck2 = true;
                        }
                    }
                }
            }
        }
        assert!(
            pre_static,
            "fixture invariant: pre-compaction static cell must be live"
        );
        assert!(
            pre_row_tomb_ck2,
            "fixture invariant: pre-compaction ck=2 must be a row tombstone"
        );
    }

    // Compact with GC enabled. Use a `gc_before` far in the future so droppable
    // tombstones are eligible for purge — exercising real tombstone GC.
    let out_dir = isolated.path().join("out");
    let far_future_secs = 4_102_444_800; // 2100-01-01
    let report = block_on(compact_sstables(
        inputs,
        &out_dir,
        &schema,
        1015,
        Some(far_future_secs),
        Some(far_future_secs),
        true,
    ))
    .expect("compaction must succeed");

    // Read the compacted output: the static cell MUST survive tombstone GC; the
    // row-deleted ck=2 must not reappear as a live row.
    let mut merger =
        KWayMerger::new(vec![report.output.data_path], &schema).expect("merger over output");
    let mut static_cells = 0usize;
    let mut live_ck2 = false;
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in rows {
                    let ck = row
                        .clustering_key
                        .as_ref()
                        .and_then(|c| c.columns.first().map(|(_, v)| value_to_string(v)));
                    if let RowData::Live { cells } = &row.row_data {
                        for c in cells {
                            if c.column == "stat_col" {
                                if let Value::Text(t) = &c.value {
                                    if t == "surviving_static" {
                                        static_cells += 1;
                                    }
                                }
                            }
                            if c.column == "row_col"
                                && ck.as_deref() == Some("2")
                                && matches!(&c.value, Value::Text(_))
                            {
                                live_ck2 = true;
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(
        static_cells > 0,
        "#1015 (links #850): static cell must survive tombstone GC on compaction \
         — found ZERO surviving static cells"
    );
    assert!(
        !live_ck2,
        "#1015: row-deleted ck=2 must not resurrect as a live row after GC compaction"
    );

    println!(
        "[#1015 static survives_tombstone_gc] compacted static_with_tombstones with \
         GC (purge=true, gc_before=2100); surviving static cells={static_cells}, \
         ck=2 resurrected={live_ck2}"
    );
}

// ===========================================================================
// 5. dropped_column.empty_index_block_reverse_scan — PARTIAL
// ===========================================================================

/// `cass.schema_evolution.dropped_column.empty_index_block_reverse_scan`
///
/// STATUS: **partial**. This scenario needs a *wide* partition (enough rows to
/// span multiple Index.db blocks) where a dropped column makes some blocks emit
/// no live cells, then a REVERSE scan must not skip live rows located after an
/// empty block. None of the committed fixtures qualify: `dropped_regular_col`
/// has only 3 rows in one partition (a single index block — no empty-block
/// boundary to exercise), and the issue #922 synthetic regression exercises
/// per-cell purge during *compaction*, not reverse-scan-over-empty-index-blocks.
///
/// We assert the precondition that proves *why* this is partial (the fixture is
/// too small to produce multiple index blocks) so the gap is documented and
/// fail-loud rather than a silent skip. The manifest entry for this ID is
/// `partial` with a scope.gap requesting a wide dropped-column fixture.
#[test]
fn dropped_column_empty_index_block_reverse_scan_partial() {
    let Some(root) = test_tomb_root() else {
        skip_or_panic(
            "test_tomb dataset root",
            "CQLITE_DATASETS_ROOT unset / test_tomb absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, "dropped_regular_col") else {
        skip_or_panic(
            "dropped_regular_col fixture",
            "dropped_regular_col fixture not found",
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

    // Confirm the gap: gen-1 is a single small partition (3 rows), so there is
    // no multi-block index from which an empty block + reverse-scan-skip could
    // arise. Derived from the golden, not the path name.
    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let golden = parse_jsonl(&jsonl);
    let partition_count = golden.len();
    let row_count: usize = golden
        .iter()
        .flat_map(|p| &p.entries)
        .filter(|e| matches!(e, GoldenEntry::Row { .. }))
        .count();
    assert!(
        partition_count == 1 && row_count <= 8,
        "PARTIAL precondition: dropped_regular_col is a single small partition \
         (partitions={partition_count}, rows={row_count}); a wide multi-index-block \
         dropped-column fixture is required to exercise reverse-scan over empty \
         index blocks — NOT YET GENERATED (manifest: partial)."
    );

    println!(
        "[#1015 empty_index_block_reverse_scan] PARTIAL — no wide dropped-column \
         fixture exists (dropped_regular_col is {partition_count} partition / \
         {row_count} rows, single index block). Gap reported to manifest."
    );
}
