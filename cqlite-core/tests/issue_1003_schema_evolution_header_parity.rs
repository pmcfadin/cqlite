//! Issue #1003 (epic #971) — schema-evolution SerializationHeader parity.
//!
//! Proves CQLite deserializes `nb`-format SSTables using the **on-disk** column
//! types recorded in each generation's Cassandra `SerializationHeader`
//! (Statistics.db), NOT by blindly applying the *current* schema. The six
//! `test_types/se_*` fixtures model the schema-evolution surface that Cassandra
//! 5.0.2 actually produces (ALTER ... TYPE is forbidden, so "altered" is modelled
//! as an ADD-column header divergence — the sanctioned fallback documented in
//! `test-data/schemas/cql-type-parity.cql`, Group A):
//!
//!   * `se_no_schema_change` — control: gen-1 and gen-2 headers are identical.
//!   * `se_altered_column_type` — gen-2 ADDs `added_col bigint`; gen-1 header lacks it.
//!   * `se_dropped_column_same_type` — `dropme text` dropped between flushes; gen-1
//!     header STILL declares it, gen-2 does not.
//!   * `se_altered_then_dropped_column` — ADD `evolve_col` (gen-2), DROP (gen-3);
//!     gen-1 {base_col}, gen-2 {base_col,evolve_col}, gen-3 {base_col}.
//!   * `se_static_regular_kind_mismatch` — `stat_col` recorded STATIC beside regular `row_col`.
//!   * `se_frozen_multicell_collection` — `fl frozen<list<text>>` vs `ml list<text>`:
//!     the header records the frozen-vs-multicell flag difference.
//!
//! ## What this lane asserts (acceptance criteria, #1003)
//!
//!  1. **On-disk header decode.** For EVERY generation, CQLite's *binary* decode
//!     of `Statistics.db` (`serialization_header_columns`: name + CQL type +
//!     static kind) matches the committed Cassandra `*-Statistics.db.txt`
//!     reference dump, positionally by name and exactly by type/kind. This is the
//!     authoritative "recorded on-disk type" — derived from the header bytes, NOT
//!     from the fixture path or column name.
//!
//!  2. **Per-generation divergence.** gen-1's header is explicitly asserted to
//!     DIFFER from gen-2 (and gen-3) exactly as Cassandra recorded — the dropped /
//!     added / static / frozen column facts are checked per generation, never
//!     collapsed.
//!
//!  3. **Decoded-row parity (comparator).** CQLite reads each generation through
//!     the public `scan_delta` reader path, the decode is rendered to
//!     sstabledump-shaped JSONL, and the shared `canonical_jsonl` comparator
//!     compares it POSITIONALLY against the committed `*-Data.db.jsonl` golden for
//!     the scalar-column scenarios (control / added / dropped / static). Affected
//!     live columns (`added_col`, `evolve_col`), dropped columns (`dropme` in the
//!     pre-drop generation), and skipped columns (the bigint `added_col` absent
//!     from gen-1) are all proven to decode by the recorded on-disk type.
//!
//!  4. **Frozen-vs-multicell.** The frozen flag difference is asserted from the
//!     header (`frozen<list<text>>` vs `list<text>`) and the frozen scalar `fl`
//!     value is checked directly. The multicell `ml` element-level decode is NOT
//!     run through the document comparator (see the note on that test).
//!
//! ## Discipline
//!  * SKIP cleanly when `CQLITE_DATASETS_ROOT` is unset or a generation's binary
//!    Data.db / Statistics.db is absent (CI without datasets); PANIC instead when
//!    `CQLITE_REQUIRE_FIXTURES=1`.
//!  * A missing / empty / placeholder golden FAILS (the comparator errors loud and
//!    we surface it — never a silent skip).
//!  * Comparison is positional. Cell order within a row is normalized to column
//!    name order to match sstabledump's emission order — the only formatting
//!    normalization applied.
//!  * Failure messages carry the manifest scenario id, table, component path,
//!    column name, expected on-disk type, current schema type, and the byte/value
//!    diff.
//!
//! Manifest entries gated (reported, not edited here):
//!   * cass.schema_evolution.serialization_header.no_schema_change
//!   * cass.schema_evolution.serialization_header.altered_column_type
//!   * cass.schema_evolution.serialization_header.dropped_column_same_type
//!   * cass.schema_evolution.serialization_header.altered_then_dropped_column
//!   * cass.schema_evolution.serialization_header.static_regular_kind_mismatch
//!   * cass.schema_evolution.serialization_header.frozen_multicell_collection_mismatch
//!
//! Run:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --features delta-scan \
//!   --test issue_1003_schema_evolution_header_parity -- --nocapture
//! ```

#![cfg(feature = "delta-scan")]

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, DeltaRecord};
use cqlite_core::types::Value;

#[path = "support/canonical_jsonl.rs"]
mod canonical_jsonl;

use canonical_jsonl::{
    compare_documents, load_golden_document_with_keys, parse_document_str_with_keys, render_diffs,
    CompareCtx, KeyKind, KeySpec,
};

/// Build the comparator [`KeySpec`] from a [`TableSchema`]: ordered partition +
/// clustering key CQL types, so each KEY component is canonicalized against its
/// DECLARED type rather than guessed from the JSON shape (issue #971).
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

// ===========================================================================
// require-fixtures contract (epic #971): opt-in strict mode
// ===========================================================================

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Skip cleanly (default) or PANIC (strict mode) when a required fixture is absent.
fn skip_or_panic(fixture: &str, reason: &str) -> bool {
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but fixture {fixture} is absent — {reason}; \
             fetch/generate it (bash test-data/scripts/fetch-datasets.sh)"
        );
    }
    eprintln!("[SKIP] {reason}");
    true
}

// ===========================================================================
// Fixture discovery (test_types / se_* corpus)
// ===========================================================================

fn test_types_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join("sstables").join("test_types");
    path.exists().then_some(path)
}

/// The single fixture directory whose name starts with `prefix-<uuid>`.
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

fn gen_component(dir: &Path, gen: &str, suffix: &str) -> PathBuf {
    dir.join(format!("{gen}-big-{suffix}"))
}

fn gen_has_data(dir: &Path, gen: &str) -> bool {
    gen_component(dir, gen, "Data.db").exists()
}

// ===========================================================================
// SerializationHeader: binary decode + reference-dump cross-check
// ===========================================================================

/// One column's authoritative on-disk facts, derived from the header bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeaderCol {
    name: String,
    /// CQL type as CQLite recovers it from the marshal class
    /// (`text`, `bigint`, `list<text>`, `frozen<list<text>>`, ...).
    cql_type: String,
    is_static: bool,
}

/// CQLite's byte-derived view of a generation's SerializationHeader regular +
/// static columns, ordered as they appear in the header (NOT path/name-derived).
fn decode_header(dir: &Path, gen: &str) -> Option<Vec<HeaderCol>> {
    let bytes = fs::read(gen_component(dir, gen, "Statistics.db")).ok()?;
    let (_, stats) = parse_statistics_with_fallback(&bytes, None).ok()?;
    Some(
        stats
            .serialization_header_columns
            .iter()
            .map(|c| HeaderCol {
                name: c.name.clone(),
                cql_type: c.column_type.clone(),
                is_static: c.is_static,
            })
            .collect(),
    )
}

/// One column from the committed `*-Statistics.db.txt` reference dump: name,
/// Cassandra marshal type, and static kind (from which line it was parsed).
#[derive(Debug, Clone)]
struct RefCol {
    name: String,
    /// The fully-qualified marshal type Cassandra recorded
    /// (`org.apache.cassandra.db.marshal.UTF8Type`, ...).
    marshal_type: String,
    is_static: bool,
}

/// Parse `RegularColumns:` / `StaticColumns:` from the reference dump, preserving
/// order. The type half may contain commas inside parentheses
/// (`ListType(UTF8Type)`, `FrozenType(ListType(...))`), so split at top level.
fn reference_header(dir: &Path, gen: &str) -> Option<Vec<RefCol>> {
    let content = fs::read_to_string(gen_component(dir, gen, "Statistics.db.txt")).ok()?;
    let mut out = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("StaticColumns:") {
            out.extend(parse_ref_entries(rest, true));
        } else if let Some(rest) = line.strip_prefix("RegularColumns:") {
            out.extend(parse_ref_entries(rest, false));
        }
    }
    Some(out)
}

fn parse_ref_entries(after_colon: &str, is_static: bool) -> Vec<RefCol> {
    let s = after_colon.trim();
    if s.is_empty() {
        return Vec::new();
    }
    let bytes = s.as_bytes();
    let mut entries = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
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
        .filter_map(|e| {
            let e = e.trim();
            let colon = e.find(':')?;
            let name = e[..colon].trim().to_string();
            let marshal = e[colon + 1..].trim().to_string();
            if name.is_empty() {
                None
            } else {
                Some(RefCol {
                    name,
                    marshal_type: marshal,
                    is_static,
                })
            }
        })
        .collect()
}

/// Convert a Cassandra marshal type (as printed in Statistics.db.txt) to the same
/// CQL string CQLite's header decoder produces, so the binary decode and the
/// reference dump can be compared on equal terms. Mirrors
/// `convert_marshal_type_to_cql` for the types exercised by the se_* corpus.
fn marshal_to_cql(marshal: &str) -> String {
    fn strip_ns(t: &str) -> &str {
        t.rsplit('.').next().unwrap_or(t)
    }
    let m = marshal.trim();
    if let Some(inner) = strip_marshal_wrapper(m, "FrozenType") {
        return format!("frozen<{}>", marshal_to_cql(inner));
    }
    if let Some(inner) = strip_marshal_wrapper(m, "ListType") {
        return format!("list<{}>", marshal_to_cql(inner));
    }
    if let Some(inner) = strip_marshal_wrapper(m, "SetType") {
        return format!("set<{}>", marshal_to_cql(inner));
    }
    match strip_ns(m) {
        "UTF8Type" | "AsciiType" => "text".to_string(),
        "LongType" => "bigint".to_string(),
        "Int32Type" => "int".to_string(),
        "BooleanType" => "boolean".to_string(),
        other => other.to_string(),
    }
}

/// If `m` is `[ns.]Wrapper(inner)`, return `inner` (the single type argument).
fn strip_marshal_wrapper<'a>(m: &'a str, wrapper: &str) -> Option<&'a str> {
    let with_open = format!("{wrapper}(");
    let idx = m.find(&with_open)?;
    // Ensure the wrapper is the outermost token (only a namespace before it).
    let prefix = &m[..idx];
    if !(prefix.is_empty() || prefix.ends_with('.')) {
        return None;
    }
    let inner_start = idx + with_open.len();
    let rest = &m[inner_start..];
    let close = rest.rfind(')')?;
    Some(rest[..close].trim())
}

/// Assert CQLite's binary header decode matches the Cassandra reference dump for
/// one generation: same columns, same order (regular columns), same CQL type,
/// same static kind. Returns the decoded columns for cross-generation checks.
fn assert_header_parity(manifest_id: &str, table: &str, dir: &Path, gen: &str) -> Vec<HeaderCol> {
    let bin = decode_header(dir, gen).unwrap_or_else(|| {
        panic!(
            "[{manifest_id}] table={table} component={}-big-Statistics.db: CQLite failed to \
             binary-decode the SerializationHeader",
            gen
        )
    });
    let reference = reference_header(dir, gen).unwrap_or_else(|| {
        panic!(
            "[{manifest_id}] table={table} component={}-big-Statistics.db.txt: reference dump missing",
            gen
        )
    });

    // POSITIONAL header parity. Both the Cassandra reference dump
    // (StaticColumns: line, then RegularColumns: line) and CQLite's binary
    // decode (static columns first, then regular columns — see
    // enhanced_statistics_parser::parse_serialization_header) order columns the
    // same way: statics first in on-disk order, then regulars in on-disk order.
    // We therefore compare the two as ORDERED VECTORS, checking position, name,
    // converted CQL type, and static kind at each index. A name-keyed map/set
    // (the previous approach) would silently accept a WRONG-ORDER header; the
    // index-by-index walk below fails loud on a reorder, a missing column, or an
    // over-emitted column at the exact position.
    let expected_seq: Vec<(String, String, bool)> = reference
        .iter()
        .map(|rc| (rc.name.clone(), marshal_to_cql(&rc.marshal_type), rc.is_static))
        .collect();
    let actual_seq: Vec<(String, String, bool)> = bin
        .iter()
        .map(|bc| (bc.name.clone(), bc.cql_type.clone(), bc.is_static))
        .collect();

    let common = expected_seq.len().min(actual_seq.len());
    for i in 0..common {
        let (en, et, es) = &expected_seq[i];
        let (an, at, as_) = &actual_seq[i];
        assert_eq!(
            an, en,
            "[{manifest_id}] table={table} component={gen}-big-Statistics.db position #{i}: \
             column-NAME/ORDER mismatch — cassandra={en:?}, cqlite={an:?} \
             (full expected order={:?}, actual order={:?})",
            expected_seq, actual_seq
        );
        assert_eq!(
            at, et,
            "[{manifest_id}] table={table} component={gen}-big-Statistics.db position #{i} \
             column={en}: on-disk type mismatch — cassandra => {et}, cqlite => {at}"
        );
        assert_eq!(
            as_, es,
            "[{manifest_id}] table={table} component={gen}-big-Statistics.db position #{i} \
             column={en}: static-kind mismatch — cassandra static={es}, cqlite static={as_}"
        );
    }

    // Length divergence: a column missing from / over-emitted by the binary
    // decode at the tail, reported at the exact position.
    if expected_seq.len() != actual_seq.len() {
        if let Some((en, et, es)) = expected_seq.get(common) {
            panic!(
                "[{manifest_id}] table={table} component={gen}-big-Statistics.db: column at \
                 position #{common} ({en}, type={et}, static={es}) MISSING from CQLite binary \
                 header decode (cassandra has {} columns, cqlite decoded {})",
                expected_seq.len(),
                actual_seq.len()
            );
        }
        if let Some((an, at, as_)) = actual_seq.get(common) {
            panic!(
                "[{manifest_id}] table={table} component={gen}-big-Statistics.db: CQLite binary \
                 decode emitted UNEXPECTED column at position #{common} ({an}, type={at}, \
                 static={as_}) not present in the Cassandra reference header (cassandra has {} \
                 columns, cqlite decoded {})",
                expected_seq.len(),
                actual_seq.len()
            );
        }
    }

    bin
}

/// Sorted (name) regular-column name set for cross-generation divergence checks.
fn regular_names(cols: &[HeaderCol]) -> Vec<String> {
    let mut v: Vec<String> = cols
        .iter()
        .filter(|c| !c.is_static)
        .map(|c| c.name.clone())
        .collect();
    v.sort();
    v
}

// ===========================================================================
// scan_delta -> sstabledump-shaped JSONL (scalar lane)
// ===========================================================================

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(f)
}

/// Copy ONLY one generation's binary components into a fresh temp dir so
/// scan_delta sees that generation alone (no cross-generation shadowing).
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

/// Render a primary-key / scalar value as the JSON token sstabledump emits.
/// Partition/clustering keys appear as JSON in the golden (`"key":["1"]`,
/// `"clustering":[1]`); for the int keys exercised here both forms canonicalize
/// to the same `Int`, so emit a bare number.
fn value_to_json(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Counter(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Text(s) => serde_json::to_string(s).unwrap_or_else(|_| format!("{s:?}")),
        Value::Null => "null".to_string(),
        Value::Frozen(inner) => value_to_json(inner),
        Value::List(items) | Value::Set(items) => {
            let parts: Vec<String> = items.iter().map(value_to_json).collect();
            format!("[{}]", parts.join(","))
        }
        // Scalar lane only feeds the comparator scalar/int/text/bigint values;
        // anything else is rendered structurally and will surface as a diff if it
        // ever reaches the comparator unexpectedly (no silent coercion).
        other => format!("{other:?}"),
    }
}

/// Microseconds-since-epoch -> the `YYYY-MM-DDTHH:MM:SSZ` form sstabledump emits
/// for whole-second writetimes (the se_* fixtures all use whole-second T_GENn).
/// Fractional micros are appended only when present, matching sstabledump.
fn micros_to_iso(micros: i64) -> String {
    let total_secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let (y, mo, d, h, mi, s) = civil_from_secs(total_secs);
    if frac == 0 {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    } else {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{frac:06}Z")
    }
}

/// Civil date/time from epoch seconds (UTC). Howard Hinnant's algorithm.
fn civil_from_secs(secs: i64) -> (i64, i64, i64, i64, i64, i64) {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let h = rem / 3600;
    let mi = (rem % 3600) / 60;
    let s = rem % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d, h, mi, s)
}

/// Build a sstabledump-shaped JSONL string for a scalar-column table from
/// scan_delta records. Cells within a row are emitted in column-NAME order to
/// match sstabledump's emission (the only normalization applied). Static blocks
/// are emitted as `static_block`. `excluded_cols` are columns not rendered into
/// the JSONL (used to drop non-scalar/multicell columns from the scalar lane).
///
/// Rows are grouped into a single partition per partition key, in first-seen
/// order; rows are sorted by clustering value (the fixtures use a single int ck).
fn records_to_jsonl(records: &[DeltaRecord], excluded_cols: &[&str]) -> String {
    use std::collections::BTreeMap;

    // partition-key-json -> (static cells, BTreeMap<ck_sort_key, row>)
    struct Part {
        key_json: Vec<String>,
        /// (name, value, per-cell writetime micros) — static cells carry a
        /// per-cell `tstamp` in sstabledump's static_block.
        statics: Vec<(String, Value, i64)>,
        rows: BTreeMap<i64, RowAcc>,
    }
    struct RowAcc {
        clustering_json: Vec<String>,
        liveness_micros: Option<i64>,
        cells: Vec<(String, Option<Value>)>,
    }

    let mut parts: Vec<Part> = Vec::new();
    let mut idx_of: BTreeMap<String, usize> = BTreeMap::new();

    let pk_key = |pk: &[Value]| {
        pk.iter()
            .map(value_to_json)
            .collect::<Vec<_>>()
            .join("\u{1}")
    };
    let included = |name: &str| !excluded_cols.contains(&name);

    for rec in records {
        match rec {
            DeltaRecord::Upsert {
                keys,
                liveness,
                cells,
            } => {
                let pk = pk_key(&keys.partition);
                let pidx = *idx_of.entry(pk.clone()).or_insert_with(|| {
                    parts.push(Part {
                        key_json: keys.partition.iter().map(value_to_json).collect(),
                        statics: Vec::new(),
                        rows: BTreeMap::new(),
                    });
                    parts.len() - 1
                });
                let ck_sort = match keys.clustering.first() {
                    Some(Value::Integer(i)) => *i as i64,
                    Some(Value::BigInt(i)) => *i,
                    _ => 0,
                };
                let row = parts[pidx].rows.entry(ck_sort).or_insert_with(|| RowAcc {
                    clustering_json: keys.clustering.iter().map(value_to_json).collect(),
                    liveness_micros: liveness.as_ref().map(|l| l.writetime),
                    cells: Vec::new(),
                });
                if row.liveness_micros.is_none() {
                    row.liveness_micros = liveness.as_ref().map(|l| l.writetime);
                }
                for (id, cd) in cells {
                    if included(id.0.as_str()) {
                        row.cells.push((id.0.clone(), cd.value.clone()));
                    }
                }
            }
            DeltaRecord::StaticUpsert {
                partition_key,
                cells,
            } => {
                let pk = pk_key(&partition_key.partition);
                let pidx = *idx_of.entry(pk.clone()).or_insert_with(|| {
                    parts.push(Part {
                        key_json: partition_key.partition.iter().map(value_to_json).collect(),
                        statics: Vec::new(),
                        rows: BTreeMap::new(),
                    });
                    parts.len() - 1
                });
                for (id, cd) in cells {
                    if included(id.0.as_str()) {
                        if let Some(v) = &cd.value {
                            parts[pidx]
                                .statics
                                .push((id.0.clone(), v.clone(), cd.writetime));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let mut out = String::new();
    for part in &parts {
        let mut rows_json: Vec<String> = Vec::new();

        // static_block first (sstabledump emits the static block ahead of rows).
        if !part.statics.is_empty() {
            let mut statics = part.statics.clone();
            statics.sort_by(|a, b| a.0.cmp(&b.0));
            let cells: Vec<String> = statics
                .iter()
                .map(|(n, v, wt)| {
                    format!(
                        "{{\"name\":\"{n}\",\"value\":{},\"tstamp\":\"{}\"}}",
                        value_to_json(v),
                        micros_to_iso(*wt)
                    )
                })
                .collect();
            rows_json.push(format!(
                "{{\"type\":\"static_block\",\"cells\":[{}]}}",
                cells.join(",")
            ));
        }

        for row in part.rows.values() {
            let mut cells = row.cells.clone();
            // Cell order normalization: sstabledump emits cells in column order;
            // the se_* scalar columns are alphabetical, so sort by name.
            cells.sort_by(|a, b| a.0.cmp(&b.0));
            let cell_json: Vec<String> = cells
                .iter()
                .map(|(n, v)| match v {
                    Some(val) => format!("{{\"name\":\"{n}\",\"value\":{}}}", value_to_json(val)),
                    None => format!("{{\"name\":\"{n}\"}}"),
                })
                .collect();
            let mut row_obj = String::new();
            let _ = write!(
                row_obj,
                "{{\"type\":\"row\",\"clustering\":[{}]",
                row.clustering_json.join(",")
            );
            if let Some(micros) = row.liveness_micros {
                let _ = write!(
                    row_obj,
                    ",\"liveness_info\":{{\"tstamp\":\"{}\"}}",
                    micros_to_iso(micros)
                );
            }
            let _ = write!(row_obj, ",\"cells\":[{}]}}", cell_json.join(","));
            rows_json.push(row_obj);
        }

        let _ = writeln!(
            out,
            "{{\"partition\":{{\"key\":[{}]}},\"rows\":[{}]}}",
            part.key_json.join(","),
            rows_json.join(",")
        );
    }
    out
}

// ===========================================================================
// Schema factories (CURRENT schema — intentionally the latest column set, to
// prove decode uses the recorded on-disk header, not this schema).
// ===========================================================================

fn kc(n: &str, t: &str) -> KeyColumn {
    KeyColumn {
        name: n.to_string(),
        data_type: t.to_string(),
        position: 0,
    }
}
fn ck(n: &str, t: &str) -> ClusteringColumn {
    ClusteringColumn {
        name: n.to_string(),
        data_type: t.to_string(),
        position: 0,
        order: ClusteringOrder::Asc,
    }
}
fn col(n: &str, t: &str, s: bool) -> Column {
    Column {
        name: n.to_string(),
        data_type: t.to_string(),
        nullable: true,
        default: None,
        is_static: s,
    }
}

fn schema(table: &str, columns: Vec<Column>, clustering: Vec<ClusteringColumn>) -> TableSchema {
    TableSchema {
        keyspace: "test_types".to_string(),
        table: table.to_string(),
        partition_keys: vec![kc("pk", "int")],
        clustering_keys: clustering,
        columns,
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// Drive scan_delta over one isolated generation, render to JSONL, and compare
/// positionally against the committed golden via the shared comparator.
fn assert_decode_parity(
    manifest_id: &str,
    table: &str,
    dir: &Path,
    gen: &str,
    schema: TableSchema,
    excluded_cols: &[&str],
) {
    let golden_path = gen_component(dir, gen, "Data.db.jsonl");

    // Build the schema-aware KEY spec (ordered partition + clustering CQL types)
    // BEFORE the schema is moved into the scan, so both the golden and the
    // CQLite-rendered actual canonicalize each key component against its declared
    // type. This makes sstabledump's string-rendered numeric keys (`"1"`) unify
    // with CQLite's typed `1` ONLY for integral key columns (issue #971): a text
    // key `"5"` would stay `Text` and never false-match a numeric `5`.
    let key_spec = key_spec_from_schema(&schema);

    // FAIL-LOUD on a missing / empty / placeholder golden.
    let golden = load_golden_document_with_keys(&golden_path, true, &key_spec).unwrap_or_else(|e| {
        panic!("[{manifest_id}] table={table} gen={gen}: golden load failed: {e}")
    });

    let isolated = isolate_generation(dir, gen);
    let records = block_on(collect_records(isolated.path(), schema));
    let jsonl = records_to_jsonl(&records, excluded_cols);
    let synthetic_path = golden_path.with_extension("cqlite.synthetic");
    let actual = parse_document_str_with_keys(&jsonl, &synthetic_path, true, &key_spec).unwrap_or_else(|e| {
        panic!(
            "[{manifest_id}] table={table} gen={gen}: CQLite-rendered JSONL failed to parse: {e}\n\
             ---- rendered ----\n{jsonl}"
        )
    });

    let ctx = CompareCtx::new(manifest_id.to_string(), golden_path.clone());
    let diffs = compare_documents(&ctx, &golden, &actual);
    assert!(
        diffs.is_empty(),
        "[{manifest_id}] table={table} gen={gen} component={}-big-Data.db: decoded rows diverge \
         from the Cassandra sstabledump golden:\n{}",
        gen,
        render_diffs(&diffs)
    );
}

// ===========================================================================
// 1. no_schema_change (control)
// ===========================================================================

/// `cass.schema_evolution.serialization_header.no_schema_change`
///
/// Control: both generations' SerializationHeaders declare the IDENTICAL column
/// set `{v: text}`. Proves the header decode is stable and decode-parity holds
/// for an unevolved table.
#[test]
fn no_schema_change_control() {
    const MID: &str = "cass.schema_evolution.serialization_header.no_schema_change";
    const TABLE: &str = "se_no_schema_change";

    let Some(root) = test_types_root() else {
        skip_or_panic(
            "test_types root",
            "CQLITE_DATASETS_ROOT unset / test_types absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, TABLE) else {
        skip_or_panic(TABLE, &format!("{TABLE} fixture not found"));
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(TABLE, &format!("{TABLE} nb-1 Data.db absent"));
        return;
    }

    let g1 = assert_header_parity(MID, TABLE, &dir, "nb-1");
    let g1_reg = regular_names(&g1);
    assert_eq!(
        g1_reg,
        vec!["v".to_string()],
        "[{MID}] gen-1 regular columns must be {{v}}, got {g1_reg:?}"
    );

    let cur_schema = || schema(TABLE, vec![col("v", "text", false)], vec![ck("ck", "int")]);
    assert_decode_parity(MID, TABLE, &dir, "nb-1", cur_schema(), &[]);

    if gen_has_data(&dir, "nb-2") {
        let g2 = assert_header_parity(MID, TABLE, &dir, "nb-2");
        let g2_reg = regular_names(&g2);
        // CONTROL: per-generation headers are IDENTICAL.
        assert_eq!(
            g1_reg, g2_reg,
            "[{MID}] control: gen-1 and gen-2 headers must be identical, got g1={g1_reg:?} g2={g2_reg:?}"
        );
        assert_decode_parity(MID, TABLE, &dir, "nb-2", cur_schema(), &[]);
    }

    println!("[{MID}] OK — control header stable {{v}} across generations; decode-parity matched.");
}

// ===========================================================================
// 2. altered_column_type (ADD bigint in gen-2)
// ===========================================================================

/// `cass.schema_evolution.serialization_header.altered_column_type`
///
/// gen-1 header declares only `{orig_col: text}`; gen-2 ADDs `{added_col: bigint}`.
/// Proves CQLite (a) decodes gen-1 WITHOUT inventing `added_col` even though the
/// CURRENT schema has it, and (b) decodes gen-2's `added_col` as the recorded
/// on-disk `bigint`. Per-generation header divergence is asserted explicitly.
#[test]
fn altered_column_type_added_bigint() {
    const MID: &str = "cass.schema_evolution.serialization_header.altered_column_type";
    const TABLE: &str = "se_altered_column_type";

    let Some(root) = test_types_root() else {
        skip_or_panic(
            "test_types root",
            "CQLITE_DATASETS_ROOT unset / test_types absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, TABLE) else {
        skip_or_panic(TABLE, &format!("{TABLE} fixture not found"));
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(TABLE, &format!("{TABLE} nb-1 Data.db absent"));
        return;
    }

    // CURRENT schema = the evolved (gen-2) column set, on purpose.
    let cur_schema = || {
        schema(
            TABLE,
            vec![
                col("orig_col", "text", false),
                col("added_col", "bigint", false),
            ],
            vec![ck("ck", "int")],
        )
    };

    let g1 = assert_header_parity(MID, TABLE, &dir, "nb-1");
    let g1_reg = regular_names(&g1);
    assert_eq!(
        g1_reg,
        vec!["orig_col".to_string()],
        "[{MID}] gen-1 header must declare ONLY orig_col (no added_col), got {g1_reg:?}"
    );
    // gen-1 has no added_col in the header → decode must NOT surface it even
    // though the current schema declares it.
    assert_decode_parity(MID, TABLE, &dir, "nb-1", cur_schema(), &[]);

    if gen_has_data(&dir, "nb-2") {
        let g2 = assert_header_parity(MID, TABLE, &dir, "nb-2");
        let g2_reg = regular_names(&g2);
        assert_eq!(
            g2_reg,
            vec!["added_col".to_string(), "orig_col".to_string()],
            "[{MID}] gen-2 header must declare {{added_col,orig_col}}, got {g2_reg:?}"
        );
        // Per-generation DIVERGENCE: gen-2 has added_col, gen-1 does not.
        assert_ne!(
            g1_reg, g2_reg,
            "[{MID}] gen-1 and gen-2 headers must DIFFER (added_col)"
        );
        // The recorded on-disk type of added_col is bigint in gen-2.
        let added = g2
            .iter()
            .find(|c| c.name == "added_col")
            .expect("added_col present in gen-2 header");
        assert_eq!(
            added.cql_type, "bigint",
            "[{MID}] gen-2 added_col on-disk type must be bigint, got {}",
            added.cql_type
        );
        assert_decode_parity(MID, TABLE, &dir, "nb-2", cur_schema(), &[]);
    }

    println!(
        "[{MID}] OK — gen-1 {{orig_col}} vs gen-2 {{added_col:bigint,orig_col}}; \
         decode used recorded header per generation."
    );
}

// ===========================================================================
// 3. dropped_column_same_type (DROP text between flushes)
// ===========================================================================

/// `cass.schema_evolution.serialization_header.dropped_column_same_type`
///
/// gen-1 header STILL declares `dropme: text` (Cassandra preserves dropped columns
/// in pre-drop SSTable headers); gen-2 does NOT. Proves CQLite decodes the pre-drop
/// generation's `dropme` cells using the preserved on-disk header, and that the
/// dropped column is correctly ABSENT (skipped) from gen-2.
#[test]
fn dropped_column_same_type() {
    const MID: &str = "cass.schema_evolution.serialization_header.dropped_column_same_type";
    const TABLE: &str = "se_dropped_column_same_type";

    let Some(root) = test_types_root() else {
        skip_or_panic(
            "test_types root",
            "CQLITE_DATASETS_ROOT unset / test_types absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, TABLE) else {
        skip_or_panic(TABLE, &format!("{TABLE} fixture not found"));
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(TABLE, &format!("{TABLE} nb-1 Data.db absent"));
        return;
    }

    // CURRENT schema = post-drop (no dropme), to prove the pre-drop generation is
    // decoded from its OWN header rather than this schema. We still declare dropme
    // in the gen-1 read schema (a reader must be able to surface a preserved
    // column); the decode-parity check below renders dropme from the header.
    let g1 = assert_header_parity(MID, TABLE, &dir, "nb-1");
    let g1_reg = regular_names(&g1);
    assert_eq!(
        g1_reg,
        vec!["dropme".to_string(), "keepme".to_string()],
        "[{MID}] gen-1 header must STILL declare dropped column dropme, got {g1_reg:?}"
    );
    let dropme = g1
        .iter()
        .find(|c| c.name == "dropme")
        .expect("dropme present in gen-1 header");
    assert_eq!(
        dropme.cql_type, "text",
        "[{MID}] gen-1 dropme on-disk type must be text, got {}",
        dropme.cql_type
    );

    // Decode gen-1 with a schema that still declares dropme (the reader can only
    // surface a preserved column when the schema knows it). The decode must match
    // the golden, which carries both dropme and keepme cells.
    let g1_schema = schema(
        TABLE,
        vec![col("keepme", "text", false), col("dropme", "text", false)],
        vec![ck("ck", "int")],
    );
    assert_decode_parity(MID, TABLE, &dir, "nb-1", g1_schema, &[]);

    if gen_has_data(&dir, "nb-2") {
        let g2 = assert_header_parity(MID, TABLE, &dir, "nb-2");
        let g2_reg = regular_names(&g2);
        assert_eq!(
            g2_reg,
            vec!["keepme".to_string()],
            "[{MID}] gen-2 header must NOT declare dropme (post-drop), got {g2_reg:?}"
        );
        assert_ne!(
            g1_reg, g2_reg,
            "[{MID}] gen-1 (with dropme) and gen-2 (without) headers must DIFFER"
        );
        let g2_schema = schema(
            TABLE,
            vec![col("keepme", "text", false)],
            vec![ck("ck", "int")],
        );
        assert_decode_parity(MID, TABLE, &dir, "nb-2", g2_schema, &[]);
    }

    println!(
        "[{MID}] OK — gen-1 preserves dropped dropme:text in header; gen-2 omits it; \
         decode matched golden per generation."
    );
}

// ===========================================================================
// 4. altered_then_dropped_column (ADD g2, DROP g3 — three generations)
// ===========================================================================

/// `cass.schema_evolution.serialization_header.altered_then_dropped_column`
///
/// Three generations:
///   * gen-1 header: `{base_col: text}`
///   * gen-2 header: `{base_col, evolve_col: text}`  (evolve_col ADDed)
///   * gen-3 header: `{base_col}`                    (evolve_col DROPped)
///
/// Proves CQLite tracks the recorded column set PER generation across an
/// add-then-drop lifecycle: evolve_col live in gen-2, absent in gen-1 and gen-3.
#[test]
fn altered_then_dropped_column() {
    const MID: &str = "cass.schema_evolution.serialization_header.altered_then_dropped_column";
    const TABLE: &str = "se_altered_then_dropped_column";

    let Some(root) = test_types_root() else {
        skip_or_panic(
            "test_types root",
            "CQLITE_DATASETS_ROOT unset / test_types absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, TABLE) else {
        skip_or_panic(TABLE, &format!("{TABLE} fixture not found"));
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(TABLE, &format!("{TABLE} nb-1 Data.db absent"));
        return;
    }

    let g1 = assert_header_parity(MID, TABLE, &dir, "nb-1");
    let g1_reg = regular_names(&g1);
    assert_eq!(
        g1_reg,
        vec!["base_col".to_string()],
        "[{MID}] gen-1 header must be {{base_col}}, got {g1_reg:?}"
    );
    let base_schema = || {
        schema(
            TABLE,
            vec![col("base_col", "text", false)],
            vec![ck("ck", "int")],
        )
    };
    let evolved_schema = || {
        schema(
            TABLE,
            vec![
                col("base_col", "text", false),
                col("evolve_col", "text", false),
            ],
            vec![ck("ck", "int")],
        )
    };
    // gen-1: evolve_col not yet added → must not be surfaced even with the
    // evolved (gen-2) schema applied.
    assert_decode_parity(MID, TABLE, &dir, "nb-1", evolved_schema(), &[]);

    if gen_has_data(&dir, "nb-2") {
        let g2 = assert_header_parity(MID, TABLE, &dir, "nb-2");
        let g2_reg = regular_names(&g2);
        assert_eq!(
            g2_reg,
            vec!["base_col".to_string(), "evolve_col".to_string()],
            "[{MID}] gen-2 header must be {{base_col,evolve_col}}, got {g2_reg:?}"
        );
        assert_ne!(
            g1_reg, g2_reg,
            "[{MID}] gen-1 {{base_col}} and gen-2 {{base_col,evolve_col}} headers must DIFFER"
        );
        let evolve = g2
            .iter()
            .find(|c| c.name == "evolve_col")
            .expect("evolve_col in gen-2 header");
        assert_eq!(
            evolve.cql_type, "text",
            "[{MID}] gen-2 evolve_col must be text"
        );
        assert_decode_parity(MID, TABLE, &dir, "nb-2", evolved_schema(), &[]);
    }

    if gen_has_data(&dir, "nb-3") {
        let g3 = assert_header_parity(MID, TABLE, &dir, "nb-3");
        let g3_reg = regular_names(&g3);
        assert_eq!(
            g3_reg,
            vec!["base_col".to_string()],
            "[{MID}] gen-3 header must be {{base_col}} (evolve_col dropped), got {g3_reg:?}"
        );
        // gen-3 collapses back to {base_col}; identical to gen-1's regular set.
        assert_eq!(
            g1_reg, g3_reg,
            "[{MID}] gen-3 regular set must collapse back to gen-1's {{base_col}}"
        );
        assert_decode_parity(MID, TABLE, &dir, "nb-3", base_schema(), &[]);
    }

    println!(
        "[{MID}] OK — base_col / +evolve_col / -evolve_col tracked per generation \
         (gen-1 -> gen-2 -> gen-3); decode matched golden."
    );
}

// ===========================================================================
// 5. static_regular_kind_mismatch
// ===========================================================================

/// `cass.schema_evolution.serialization_header.static_regular_kind_mismatch`
///
/// `stat_col` is recorded with the STATIC kind beside regular `row_col`. Proves
/// CQLite recovers the static-vs-regular KIND from the header (not from the
/// current schema) and decodes the static block + clustering rows accordingly.
#[test]
fn static_regular_kind_mismatch() {
    const MID: &str = "cass.schema_evolution.serialization_header.static_regular_kind_mismatch";
    const TABLE: &str = "se_static_regular_kind_mismatch";

    let Some(root) = test_types_root() else {
        skip_or_panic(
            "test_types root",
            "CQLITE_DATASETS_ROOT unset / test_types absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, TABLE) else {
        skip_or_panic(TABLE, &format!("{TABLE} fixture not found"));
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(TABLE, &format!("{TABLE} nb-1 Data.db absent"));
        return;
    }

    let g1 = assert_header_parity(MID, TABLE, &dir, "nb-1");
    // KIND parity: stat_col static, row_col regular — derived from header bytes.
    let stat = g1
        .iter()
        .find(|c| c.name == "stat_col")
        .expect("stat_col in header");
    let row = g1
        .iter()
        .find(|c| c.name == "row_col")
        .expect("row_col in header");
    assert!(
        stat.is_static,
        "[{MID}] header must record stat_col with the STATIC kind"
    );
    assert!(
        !row.is_static,
        "[{MID}] header must record row_col as a REGULAR column"
    );

    let cur_schema = schema(
        TABLE,
        vec![col("stat_col", "text", true), col("row_col", "text", false)],
        vec![ck("ck", "int")],
    );
    assert_decode_parity(MID, TABLE, &dir, "nb-1", cur_schema, &[]);

    println!("[{MID}] OK — static stat_col + regular row_col kind recovered from header; decode matched golden.");
}

// ===========================================================================
// 6. frozen_multicell_collection_mismatch
// ===========================================================================

/// `cass.schema_evolution.serialization_header.frozen_multicell_collection_mismatch`
///
/// `fl frozen<list<text>>` vs `ml list<text>` (multicell). The frozen flag is
/// recorded in the header as a `FrozenType(...)` wrapper on `fl` only. Proves
/// CQLite recovers the frozen-vs-multicell distinction from the header and decodes
/// the frozen scalar `fl` value correctly.
///
/// SCOPE: the multicell `ml` element-level decode is NOT run through the document
/// comparator. The golden renders `ml` as a multicell collection: a
/// complex-deletion shell carrying a WALL-CLOCK `local_delete_time` (regeneration
/// timestamp, e.g. 2026-06-25) plus one cell PER element keyed by a freshly
/// generated cell-path UUID. Both are non-deterministic across fixture
/// regenerations, and `scan_delta` (by design, issue #493) collapses a multicell
/// list into a single `List` value without per-element paths/writetimes — so a
/// positional document comparison of `ml` would compare CQLite's collapsed list
/// against Cassandra's per-UUID element cells and could not pass faithfully. The
/// FROZEN flag and the frozen value are the deterministic parity surface here and
/// are asserted; multicell element-path/wall-clock fidelity is tracked by #493.
#[test]
fn frozen_multicell_collection_mismatch() {
    const MID: &str =
        "cass.schema_evolution.serialization_header.frozen_multicell_collection_mismatch";
    const TABLE: &str = "se_frozen_multicell_collection_mismatch";

    let Some(root) = test_types_root() else {
        skip_or_panic(
            "test_types root",
            "CQLITE_DATASETS_ROOT unset / test_types absent",
        );
        return;
    };
    let Some(dir) = find_fixture(&root, TABLE) else {
        skip_or_panic(TABLE, &format!("{TABLE} fixture not found"));
        return;
    };
    if !gen_has_data(&dir, "nb-1") {
        skip_or_panic(TABLE, &format!("{TABLE} nb-1 Data.db absent"));
        return;
    }

    let g1 = assert_header_parity(MID, TABLE, &dir, "nb-1");
    let fl = g1.iter().find(|c| c.name == "fl").expect("fl in header");
    let ml = g1.iter().find(|c| c.name == "ml").expect("ml in header");
    // FROZEN flag parity: the header wraps fl in FrozenType, ml stays multicell.
    assert_eq!(
        fl.cql_type, "frozen<list<text>>",
        "[{MID}] header must record fl as frozen<list<text>>, got {}",
        fl.cql_type
    );
    assert_eq!(
        ml.cql_type, "list<text>",
        "[{MID}] header must record ml as a multicell list<text>, got {}",
        ml.cql_type
    );
    assert_ne!(
        fl.cql_type, ml.cql_type,
        "[{MID}] frozen-vs-multicell flag must DISTINGUISH fl from ml in the header"
    );

    // Decode-parity for the frozen scalar `fl` ONLY (ml excluded — see SCOPE).
    // The golden's `fl` cell is a single frozen list value: ck=1 -> [a,b,c],
    // ck=2 -> [x]. scan_delta surfaces fl as Frozen(List([...])); render and
    // compare against the golden with ml dropped from BOTH sides.
    let cur_schema = schema(
        TABLE,
        vec![
            col("ml", "list<text>", false),
            col("fl", "frozen<list<text>>", false),
        ],
        vec![ck("ck", "int")],
    );
    let isolated = isolate_generation(&dir, "nb-1");
    let records = block_on(collect_records(isolated.path(), cur_schema));
    let mut matched = 0usize;
    for rec in &records {
        if let DeltaRecord::Upsert { keys, cells, .. } = rec {
            let ck_val = match keys.clustering.first() {
                Some(Value::Integer(i)) => *i,
                _ => continue,
            };
            let fl_cell = cells.iter().find(|(id, _)| id.0 == "fl");
            let Some((_, cd)) = fl_cell else { continue };
            let list = match &cd.value {
                Some(Value::Frozen(inner)) => match inner.as_ref() {
                    Value::List(items) => items.clone(),
                    other => panic!("[{MID}] fl frozen inner must be List, got {other:?}"),
                },
                Some(Value::List(items)) => items.clone(),
                other => panic!("[{MID}] fl must decode to a (frozen) list, got {other:?}"),
            };
            let texts: Vec<String> = list
                .iter()
                .map(|v| match v {
                    Value::Text(s) => s.clone(),
                    other => panic!("[{MID}] fl element must be text, got {other:?}"),
                })
                .collect();
            let expected: Vec<&str> = match ck_val {
                1 => vec!["a", "b", "c"],
                2 => vec!["x"],
                other => panic!("[{MID}] unexpected clustering value {other}"),
            };
            assert_eq!(
                texts, expected,
                "[{MID}] fl frozen<list<text>> value at ck={ck_val}: cqlite={texts:?} golden={expected:?}"
            );
            matched += 1;
        }
    }
    assert_eq!(
        matched, 2,
        "[{MID}] expected to decode the frozen fl value for ck=1 and ck=2, matched {matched} \
         — silent-green guard"
    );

    println!(
        "[{MID}] OK — header distinguishes fl frozen<list<text>> from ml list<text>; \
         frozen fl value matched golden for ck=1,2 (multicell ml element-level decode out of \
         scope here — tracked by #493)."
    );
}
