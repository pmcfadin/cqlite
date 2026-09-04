//! Issue #1007 (epic #971) — UDT / tuple / frozen / nested complex-value parity.
//!
//! Proves CQLite decodes SCHEMA-PROVIDED complex values — UDTs, tuples, frozen
//! values, and nested collections — STRUCTURALLY (by index/field/key order),
//! using the SUPPLIED schema + the on-disk SerializationHeader type metadata,
//! NOT by inferring types from bytes (issue #28 no-heuristics mandate).
//!
//! ## What is read and how schema is supplied
//!
//! Each `test_types/cx_*` fixture is read with the `delta-scan` API
//! ([`scan_delta`]). The supplied [`TableSchema`] is parsed from the canonical
//! DDL committed at `test-data/schemas/cql-type-parity.cql` (Group C), so the
//! test feeds CQLite the *same* CREATE TABLE the fixtures were generated from —
//! the column identity + CQL type comes from the supplied schema, while the
//! reader's complex-value decode is driven by the on-disk header's marshal-format
//! type metadata. The reader's decode loop iterates the on-disk
//! SerializationHeader column order and intersects it with the supplied schema
//! (`row_decoder.rs` ~L4079), so a column present on disk but absent
//! from the supplied schema (a DROPPED column) is excluded from the supplied
//! schema's column set — exactly the legacy/evolved surface #1007 targets.
//!
//! ## Structural comparison (positional, not by presence)
//!
//! Each decoded [`Value`] is lowered into the shared
//! [`canonical_jsonl::CanonicalValue`] model and compared against the
//! `*-Data.db.jsonl` golden's typed value with the shared, positional comparator.
//! Field order (UDT), tuple INDEX order, map key order, set order, list order,
//! and null-vs-empty distinctions are all compared element-by-element; a
//! reordering or a null/empty conflation FAILS at the exact nested path
//! (`col.field`, `tuple[2]`, `map[key]`, …).
//!
//! ## Gate
//!
//!   * `#[cfg(feature = "delta-scan")]`.
//!   * Skips cleanly when `CQLITE_DATASETS_ROOT` is unset or the binary `Data.db`
//!     is absent; FAILS LOUD when a present binary decodes to a value that
//!     diverges from the golden.
//!
//! Run with:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --features delta-scan \
//!   --test issue_1007_complex_type_parity -- --nocapture
//! ```

#![cfg(feature = "delta-scan")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, DeltaRecord};
use cqlite_core::types::{UdtValue, Value};

#[path = "support/canonical_jsonl.rs"]
mod canonical_jsonl;

use canonical_jsonl::{load_golden_document, CanonicalValue, NormalizedFloat};

// ===========================================================================
// Value -> CanonicalValue lowering (structural, order-preserving)
// ===========================================================================

/// Lower a CQLite-decoded [`Value`] into the shared [`CanonicalValue`] model so
/// it can be compared positionally against the sstabledump golden's typed value.
///
/// Order is preserved EXACTLY: list/set/map/tuple element order and UDT field
/// order survive 1:1 (no sorting, no dedup). `Value::Null` and a UDT field whose
/// value is `None` both lower to `CanonicalValue::Null` (matching sstabledump's
/// explicit JSON `null`), while an empty string lowers to `Text("")` — so the
/// null-vs-empty distinction is preserved.
fn lower_value(v: &Value) -> CanonicalValue {
    match v {
        Value::Null => CanonicalValue::Null,
        Value::Boolean(b) => CanonicalValue::Bool(*b),
        Value::Integer(i) => CanonicalValue::Int(*i as i128),
        Value::BigInt(i) => CanonicalValue::Int(*i as i128),
        Value::Counter(i) => CanonicalValue::Int(*i as i128),
        Value::SmallInt(i) => CanonicalValue::Int(*i as i128),
        Value::TinyInt(i) => CanonicalValue::Int(*i as i128),
        Value::Float(f) => CanonicalValue::Float(NormalizedFloat(*f)),
        Value::Float32(f) => CanonicalValue::Float(NormalizedFloat(*f as f64)),
        Value::Text(s) => CanonicalValue::Text(String::from_utf8_lossy(s).into_owned()),
        Value::Timestamp(micros) => CanonicalValue::Timestamp {
            micros: *micros,
            raw: micros.to_string(),
        },
        Value::List(xs) => CanonicalValue::List(xs.iter().map(lower_value).collect()),
        // sstabledump renders BOTH lists and sets as JSON arrays — the comparator
        // therefore parses a golden set as `CanonicalValue::List`. Lower a decoded
        // `Value::Set` to `List` too so the comparison is apples-to-apples; ORDER is
        // still asserted positionally (Cassandra persists sets sorted, sstabledump
        // and the reader both emit them in that persisted order).
        Value::Set(xs) => CanonicalValue::List(xs.iter().map(lower_value).collect()),
        Value::Map(kvs) => CanonicalValue::Map(
            kvs.iter()
                .map(|(k, val)| (lower_value(k), lower_value(val)))
                .collect(),
        ),
        Value::Tuple(xs) => {
            // sstabledump renders a tuple as a JSON array; the comparator parses
            // that into CanonicalValue::List. Mirror that so a tuple compares to
            // the golden array positionally (index order is load-bearing).
            CanonicalValue::List(xs.iter().map(lower_value).collect())
        }
        Value::Udt(udt) => lower_udt(udt),
        Value::Frozen(inner) => lower_value(inner),
        Value::Blob(bytes) => CanonicalValue::Text(format!("0x{}", hex::encode(bytes))),
        Value::Uuid(b) => CanonicalValue::Text(uuid::Uuid::from_bytes(*b).to_string()),
        other => CanonicalValue::Text(format!("{other:?}")),
    }
}

/// Lower a UDT into `CanonicalValue::Tuple` (an ORDERED list of (field, value)),
/// matching how the comparator decodes the golden's JSON object for a UDT. Field
/// order is preserved exactly; a `None` field value becomes `Null` (distinct
/// from an empty-string field).
fn lower_udt(udt: &UdtValue) -> CanonicalValue {
    CanonicalValue::Tuple(
        udt.fields
            .iter()
            .map(|f| {
                let cv = match &f.value {
                    Some(v) => lower_value(v),
                    None => CanonicalValue::Null,
                };
                (f.name.clone(), cv)
            })
            .collect(),
    )
}

// ===========================================================================
// Golden expected values: (pk, ck) -> { column -> CanonicalValue }
// ===========================================================================

/// One expected row's column values, keyed by clustering key string. The golden
/// is a single partition (pk=1) for every cx_* fixture; we key by ck.
type ExpectedRows = BTreeMap<String, BTreeMap<String, CanonicalValue>>;

/// Parse the golden JSONL into expected per-(ck, column) canonical values.
///
/// For multicell columns sstabledump emits one cell per element/field with a
/// `path`; the value the *reader* produces is the WHOLE collapsed collection /
/// UDT (v1 delta-scan collapses non-frozen collections + multicell UDTs into a
/// single typed [`Value`], see `CellDelta` docs). So for the multicell columns we
/// reassemble the golden's per-path cells into the same collapsed shape (an
/// ordered list for a multicell list, an ordered UDT for a multicell UDT) so the
/// structural comparison is apples-to-apples and still positional.
fn expected_from_golden(path: &Path, multicell_cols: &[(&str, MulticellKind)]) -> ExpectedRows {
    let doc =
        load_golden_document(path, true).unwrap_or_else(|e| panic!("load golden {path:?}: {e}"));
    let mut out: ExpectedRows = BTreeMap::new();

    for part in &doc.partitions {
        for row in &part.rows {
            let ck = render_canonical_key(&row.clustering);
            let entry = out.entry(ck).or_default();

            // Group cells by column name preserving document (path) order.
            let mut grouped: BTreeMap<String, Vec<&canonical_jsonl::CanonicalCell>> =
                BTreeMap::new();
            // Preserve first-seen order of columns via a side vec.
            let mut col_order: Vec<String> = Vec::new();
            for cell in &row.cells {
                if !grouped.contains_key(&cell.name) {
                    col_order.push(cell.name.clone());
                }
                grouped.entry(cell.name.clone()).or_default().push(cell);
            }

            for col in &col_order {
                let cells = &grouped[col];
                if let Some((_, kind)) = multicell_cols.iter().find(|(name, _)| name == col) {
                    let assembled = assemble_multicell(*kind, cells);
                    entry.insert(col.clone(), assembled);
                } else {
                    // Scalar / frozen single-cell column: exactly one value cell
                    // (path-less) carries the whole value.
                    let value_cell = cells
                        .iter()
                        .find(|c| !matches!(c.value, CanonicalValue::Absent))
                        .unwrap_or_else(|| {
                            panic!(
                                "golden {path:?} column '{col}' (ck={ck}) has no value cell",
                                ck = render_canonical_key(&row.clustering)
                            )
                        });
                    entry.insert(col.clone(), value_cell.value.clone());
                }
            }
        }
    }

    out
}

/// How a multicell column's per-path golden cells are reassembled into the
/// collapsed shape the reader emits. sstabledump encodes each multicell column as
/// one cell per element/field with a `path`; the leading path-less cell is the
/// complex-deletion shell (ignored — delta-scan surfaces it via `replaced`).
#[derive(Clone, Copy)]
enum MulticellKind {
    /// multicell UDT: path = field NAME, value = field value → ordered UDT.
    Udt,
    /// multicell list: path = element id, value = element → ordered List.
    List,
    /// multicell map: path = key, value = value → ordered Map (key order).
    Map,
    /// multicell set whose ELEMENT lives in the cell-path (value is empty); the
    /// path is the serialized frozen-element bytes. Each path-hex is decoded into
    /// a frozen `map<text,int>` and assembled (set→ordered List).
    SetFrozenMapInPath,
}

fn assemble_multicell(
    kind: MulticellKind,
    cells: &[&canonical_jsonl::CanonicalCell],
) -> CanonicalValue {
    let elem_cells: Vec<&canonical_jsonl::CanonicalCell> = match kind {
        MulticellKind::SetFrozenMapInPath => cells
            .iter()
            .copied()
            .filter(|c| !c.path.is_empty())
            .collect(),
        _ => cells
            .iter()
            .copied()
            .filter(|c| !c.path.is_empty() && !matches!(c.value, CanonicalValue::Absent))
            .collect(),
    };

    match kind {
        MulticellKind::Udt => CanonicalValue::Tuple(
            elem_cells
                .iter()
                .map(|c| {
                    let field = match c.path.first() {
                        Some(CanonicalValue::Text(s)) => s.clone(),
                        other => format!("{other:?}"),
                    };
                    (field, c.value.clone())
                })
                .collect(),
        ),
        MulticellKind::List => {
            CanonicalValue::List(elem_cells.iter().map(|c| c.value.clone()).collect())
        }
        MulticellKind::Map => CanonicalValue::Map(
            elem_cells
                .iter()
                .map(|c| {
                    (
                        c.path.first().cloned().unwrap_or(CanonicalValue::Null),
                        c.value.clone(),
                    )
                })
                .collect(),
        ),
        MulticellKind::SetFrozenMapInPath => CanonicalValue::List(
            elem_cells
                .iter()
                .map(|c| match c.path.first() {
                    Some(CanonicalValue::Text(hexstr)) => decode_frozen_map_text_int(hexstr),
                    other => panic!("set element path is not hex text: {other:?}"),
                })
                .collect(),
        ),
    }
}

/// Decode the hex-encoded serialized bytes of a frozen `map<text,int>` (the
/// sstabledump cell-PATH for a `set<frozen<map<text,int>>>` element) into a
/// canonical Map, preserving key order. Format: `count(i32) [klen(i32) k vlen(i32) v]…`.
fn decode_frozen_map_text_int(hexstr: &str) -> CanonicalValue {
    let bytes =
        hex::decode(hexstr).unwrap_or_else(|e| panic!("bad set-element hex {hexstr:?}: {e}"));
    let mut i = 0usize;
    let rd_i32 = |b: &[u8], i: &mut usize| -> i32 {
        let v = i32::from_be_bytes([b[*i], b[*i + 1], b[*i + 2], b[*i + 3]]);
        *i += 4;
        v
    };
    let count = rd_i32(&bytes, &mut i);
    let mut entries = Vec::new();
    for _ in 0..count {
        let klen = rd_i32(&bytes, &mut i) as usize;
        let k = String::from_utf8_lossy(&bytes[i..i + klen]).to_string();
        i += klen;
        let vlen = rd_i32(&bytes, &mut i) as usize;
        let v = rd_i32(&bytes[i..i + vlen], &mut 0) as i128;
        i += vlen;
        entries.push((CanonicalValue::Text(k), CanonicalValue::Int(v)));
    }
    CanonicalValue::Map(entries)
}

/// Render a clustering key (vec of canonical values) into a stable string key.
fn render_canonical_key(ck: &[CanonicalValue]) -> String {
    ck.iter()
        .map(render_canonical)
        .collect::<Vec<_>>()
        .join("|")
}

fn render_canonical(v: &CanonicalValue) -> String {
    match v {
        CanonicalValue::Int(i) => i.to_string(),
        CanonicalValue::Text(s) => s.clone(),
        CanonicalValue::Bool(b) => b.to_string(),
        CanonicalValue::Null => "null".to_string(),
        other => format!("{other:?}"),
    }
}

// ===========================================================================
// Actual values from scan_delta: (ck) -> { column -> CanonicalValue }
// ===========================================================================

type ActualRows = BTreeMap<String, BTreeMap<String, CanonicalValue>>;

async fn actual_from_scan(fixture_dir: &Path, schema: TableSchema) -> ActualRows {
    let (mut rx, _summary) = scan_delta(fixture_dir.to_path_buf(), schema, 256);
    let mut out: ActualRows = BTreeMap::new();
    while let Some(result) = rx.recv().await {
        let rec = result.unwrap_or_else(|e| panic!("scan_delta error in {fixture_dir:?}: {e}"));
        if let DeltaRecord::Upsert { keys, cells, .. } = rec {
            let ck = keys
                .clustering
                .iter()
                .map(value_key)
                .collect::<Vec<_>>()
                .join("|");
            let entry = out.entry(ck).or_default();
            for (col, cd) in &cells {
                if let Some(v) = &cd.value {
                    entry.insert(col.0.clone(), lower_value(v));
                }
            }
        }
    }
    out
}

fn value_key(v: &Value) -> String {
    match v {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Text(s) => String::from_utf8_lossy(s).into_owned(),
        Value::Boolean(b) => b.to_string(),
        other => format!("{other:?}"),
    }
}

// ===========================================================================
// Schema loading (faithful supplied schema from the committed DDL)
// ===========================================================================

fn schema_file() -> PathBuf {
    // The committed DDL lives in the repo at test-data/schemas; resolve relative
    // to the crate manifest dir so it works regardless of CWD.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data")
        .join("schemas")
        .join("cql-type-parity.cql")
}

/// Extract a single `CREATE TABLE <table> (...)` statement body from the DDL file
/// and parse it into a [`TableSchema`]. Keyspace is forced to `test_types` (the
/// DDL uses `USE test_types;` rather than qualified names).
fn load_table_schema(table: &str) -> TableSchema {
    let ddl = std::fs::read_to_string(schema_file())
        .unwrap_or_else(|e| panic!("read schema {:?}: {e}", schema_file()));
    let stmt = extract_create_table(&ddl, table)
        .unwrap_or_else(|| panic!("CREATE TABLE {table} not found in DDL"));
    let (_, mut schema) = cqlite_core::schema::cql_parser::parse_create_table(&stmt)
        .unwrap_or_else(|e| panic!("parse CREATE TABLE {table}: {e:?}"));
    schema.keyspace = "test_types".to_string();
    schema.table = table.to_string();
    schema
}

/// Pull the `CREATE TABLE [IF NOT EXISTS] <table> ( … );` statement out of the
/// multi-statement DDL, normalizing away the leading `IF NOT EXISTS` so the
/// parser's table-name match is unambiguous.
fn extract_create_table(ddl: &str, table: &str) -> Option<String> {
    let needle = format!("create table if not exists {table} ");
    let lower = ddl.to_lowercase();
    let start = lower.find(&needle).or_else(|| {
        let alt = format!("create table {table} ");
        lower.find(&alt)
    })?;
    // From the matched start, find the terminating `);` (the table body's close).
    let rest = &ddl[start..];
    let semi = rest.find(';')?;
    let stmt = &rest[..=semi];
    // Strip "IF NOT EXISTS" so parse_create_table's name match is clean.
    let stmt = stmt.replacen("IF NOT EXISTS ", "", 1);
    // Strip line comments (`-- …` to end of line); the DDL annotates columns with
    // trailing `--` comments that the CQL parser does not accept inline.
    let cleaned = stmt
        .lines()
        .map(|line| match line.find("--") {
            Some(idx) => &line[..idx],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n");
    Some(cleaned)
}

// ===========================================================================
// Fixture discovery
// ===========================================================================

fn datasets_root() -> Option<PathBuf> {
    let p = PathBuf::from(std::env::var("CQLITE_DATASETS_ROOT").ok()?);
    p.exists().then_some(p)
}

/// Find the cx_* fixture directory (with a binary Data.db) for `table`,
/// optionally a specific generation file prefix (e.g. `nb-1-big`).
fn fixture_dir(table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks = root.join("sstables").join("test_types");
    for entry in std::fs::read_dir(&ks).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.starts_with(&format!("{table}-")) {
            return Some(path);
        }
    }
    None
}

fn binary_data_db_present(dir: &Path, prefix: &str) -> bool {
    dir.join(format!("{prefix}-Data.db")).exists()
}

/// `CQLITE_REQUIRE_FIXTURES=1` makes a missing fixture a HARD failure, so a skipped
/// comparison can never be mistaken for a passing `mirrored` parity run.
///
/// TWO lanes set it, and naming only the first is what #3725 was filed about: the
/// `parity-regen-matrix.yml` cql-type leg regenerates the corpus and sets it, but
/// that workflow is EXEMPT from `required`, so it executed these 6 cases without
/// gating any merge. Since #3725 the local gate's `feature-iso-delta-scan`
/// component also exports it on the FULL gate, and that component IS the
/// merge-gating executor for this file.
///
/// Without it the test skips cleanly (fresh checkout, binaries absent).
fn require_fixtures_strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Skip when fixtures are absent — unless strict mode is on, in which case fail
/// loud so the scenario cannot false-pass as compared.
fn skip_or_fail(table: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {table} fixture unavailable: {reason}");
    }
    println!("[SKIP] {table}: {reason}");
}

/// The committed JSONL goldens live in the repo's `test-data` tree (the binary
/// `Data.db` files are gitignored and supplied separately via
/// `CQLITE_DATASETS_ROOT`). Resolve the golden for `table`'s `prefix` generation
/// from the in-repo `test-data` relative to the crate manifest.
fn golden_for_gen(table: &str, prefix: &str) -> Option<PathBuf> {
    let ks = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data")
        .join("datasets")
        .join("sstables")
        .join("test_types");
    for entry in std::fs::read_dir(&ks).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.starts_with(&format!("{table}-")) {
            let p = path.join(format!("{prefix}-Data.db.jsonl"));
            if p.exists() {
                return Some(p);
            }
        }
    }
    None
}

/// Run a single-generation parity check. `multicell_cols` lists columns the
/// golden emits as per-path multicell cells (so they are reassembled into the
/// collapsed shape the reader emits). Returns the number of rows compared.
async fn run_fixture(
    table: &str,
    schema: TableSchema,
    multicell_cols: &[(&str, MulticellKind)],
) -> Option<usize> {
    let dir = match fixture_dir(table) {
        Some(d) => d,
        None => {
            skip_or_fail(table, "no fixture dir under CQLITE_DATASETS_ROOT");
            return None;
        }
    };
    if !binary_data_db_present(&dir, "nb-1-big") {
        skip_or_fail(table, "binary nb-1-big-Data.db absent");
        return None;
    }
    let golden = golden_for_gen(table, "nb-1-big")
        .unwrap_or_else(|| panic!("{table}: golden nb-1-big-Data.db.jsonl missing"));

    let expected = expected_from_golden(&golden, multicell_cols);
    let actual = actual_from_scan(&dir, schema).await;

    compare_rows(table, &expected, &actual);
    Some(expected.len())
}

/// Bidirectional structural comparison. First the ROW-KEY sets must match
/// exactly (a row over-emitted or dropped by CQLite FAILS), then per shared row
/// the COLUMN-KEY sets must match exactly (a stale/over-emitted or dropped
/// column FAILS), and only then are values compared. Field/index/key order is
/// already baked into the canonical model's positional `PartialEq`, so a
/// reorder/null-vs-empty conflation FAILS here with the exact nested path.
///
/// Iterating only the expected side (the previous behavior) silently ignored
/// extra rows/columns emitted by CQLite, letting an over-emission regression
/// false-pass; the set checks below close that hole.
fn compare_rows(table: &str, expected: &ExpectedRows, actual: &ActualRows) {
    let mut errors: Vec<String> = Vec::new();

    // Row-key set parity: report rows missing from / unexpected in the decode.
    for ck in actual.keys() {
        if !expected.contains_key(ck) {
            errors.push(format!(
                "[{table}] row ck={ck} UNEXPECTED in CQLite decode (over-emission)"
            ));
        }
    }

    for (ck, exp_cols) in expected {
        let act_cols = match actual.get(ck) {
            Some(c) => c,
            None => {
                errors.push(format!("[{table}] row ck={ck} MISSING from CQLite decode"));
                continue;
            }
        };

        // Column-key set parity within this row: report columns CQLite emitted
        // that the golden does not contain (stale/dropped-column over-emission).
        for col in act_cols.keys() {
            if !exp_cols.contains_key(col) {
                errors.push(format!(
                    "[{table}] ck={ck} column '{col}' UNEXPECTED in CQLite decode \
                     (over-emission)\n    actual (CQLite): {}",
                    render_full(&act_cols[col]),
                ));
            }
        }

        for (col, exp_val) in exp_cols {
            match act_cols.get(col) {
                Some(act_val) if act_val == exp_val => {}
                Some(act_val) => errors.push(format!(
                    "[{table}] ck={ck} column '{col}' STRUCTURAL MISMATCH\n    \
                     expected (Cassandra): {}\n    actual   (CQLite):    {}",
                    render_full(exp_val),
                    render_full(act_val),
                )),
                None => errors.push(format!(
                    "[{table}] ck={ck} column '{col}' MISSING from CQLite decode\n    \
                     expected (Cassandra): {}",
                    render_full(exp_val),
                )),
            }
        }
    }

    assert!(
        errors.is_empty(),
        "[{table}] complex-type parity FAILED ({} error(s)):\n{}",
        errors.len(),
        errors.join("\n")
    );
    assert!(
        !expected.is_empty(),
        "[{table}] golden produced zero expected rows — fixture/golden regression"
    );
}

/// Full nested rendering for diagnostics (so a tuple[2] / col.field divergence is
/// legible at the exact path).
fn render_full(v: &CanonicalValue) -> String {
    match v {
        CanonicalValue::Absent => "<absent>".to_string(),
        CanonicalValue::Null => "null".to_string(),
        CanonicalValue::Bool(b) => b.to_string(),
        CanonicalValue::Int(i) => i.to_string(),
        CanonicalValue::Float(f) => f.0.to_string(),
        CanonicalValue::Text(s) => format!("{s:?}"),
        CanonicalValue::Timestamp { micros, raw } => format!("ts({micros}={raw})"),
        CanonicalValue::List(xs) => format!(
            "[{}]",
            xs.iter()
                .enumerate()
                .map(|(i, x)| format!("[{i}]={}", render_full(x)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CanonicalValue::Set(xs) => format!(
            "set{{{}}}",
            xs.iter().map(render_full).collect::<Vec<_>>().join(", ")
        ),
        CanonicalValue::Map(kvs) => format!(
            "map{{{}}}",
            kvs.iter()
                .map(|(k, v)| format!("[{}]={}", render_full(k), render_full(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CanonicalValue::Tuple(fs) => format!(
            "{{{}}}",
            fs.iter()
                .map(|(k, v)| format!("{k}={}", render_full(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

// ===========================================================================
// Tests — one per manifest id
// ===========================================================================

/// `cass.cql_types.complex.tuple_field_order`
#[tokio::test]
async fn tuple_field_order_parity() {
    let schema = load_table_schema("cx_tuple_field_order");
    if let Some(n) = run_fixture("cx_tuple_field_order", schema, &[]).await {
        println!("cx_tuple_field_order: {n} rows compared (tuple index order incl. null middle)");
    }
}

/// `cass.cql_types.complex.udt_field_order_null_empty`
///
/// Regression coverage for issue #1080 (now FIXED): a `frozen<person_type>`
/// column decodes to a STRUCTURED UDT on the schema-driven read path (previously
/// it was dropped/blobbed). Exercises UDT field order and null-vs-empty fields.
#[tokio::test]
async fn udt_field_order_null_empty_parity() {
    let schema = load_table_schema("cx_udt_field_order_null_empty");
    if let Some(n) = run_fixture("cx_udt_field_order_null_empty", schema, &[]).await {
        println!("cx_udt_field_order_null_empty: {n} rows compared (UDT field order, null vs '')");
    }
}

/// `cass.cql_types.complex.frozen_udt_value`
///
/// Regression coverage for issue #1080 (now FIXED): a nested
/// `frozen<employee_type>` (which contains a `frozen<address_type>`) decodes to
/// the structured nested UDT (previously a raw blob hex string).
#[tokio::test]
async fn frozen_udt_value_parity() {
    let schema = load_table_schema("cx_frozen_udt_value");
    if let Some(n) = run_fixture("cx_frozen_udt_value", schema, &[]).await {
        println!("cx_frozen_udt_value: {n} rows compared (nested frozen UDT)");
    }
}

/// `cass.cql_types.complex.nested_frozen_collections`
///
/// Outer collections are all NON-frozen (multicell): `m_list_vals` is a
/// `map<text, frozen<list<int>>>` (per-path = map key), `l_set_vals` a
/// `list<frozen<set<text>>>` (per-path = element id), and `s_map_vals` a
/// `set<frozen<map<text,int>>>` (the frozen-map element lives in the cell PATH).
/// CQLite decodes all three into the correct typed nested `Value`s; the test
/// reassembles the golden's per-path encoding into the same collapsed shape and
/// asserts map key order, list order, and the inner frozen-collection contents
/// positionally.
#[tokio::test]
async fn nested_frozen_collections_parity() {
    let schema = load_table_schema("cx_nested_frozen_collections");
    let multicell = [
        ("m_list_vals", MulticellKind::Map),
        ("l_set_vals", MulticellKind::List),
        ("s_map_vals", MulticellKind::SetFrozenMapInPath),
    ];
    if let Some(n) = run_fixture("cx_nested_frozen_collections", schema, &multicell).await {
        println!(
            "cx_nested_frozen_collections: {n} rows compared (collection-of-frozen-collection)"
        );
    }
}

/// `cass.cql_types.complex.multicell_udt_collection_paths`
///
/// Both multicell columns decode structurally: the `ml list<text>` column as an
/// ordered list via cell-paths, and the `mp person_type` (non-frozen, top-level
/// UDT) reassembled from its per-field cells into a structured UDT. Fixed by
/// issue #1081: the complex-ness decision and the multicell-UDT decode now use
/// the AUTHORITATIVE on-disk SerializationHeader marshal type
/// (`ColumnInfo.column_type`, which carries `UserType(...)`) instead of the
/// supplied schema's bare CQL short form (`person_type`), which previously
/// misrouted the column to the scalar path and yielded a raw blob (`0x08020000`).
#[tokio::test]
async fn multicell_udt_collection_paths_parity() {
    let schema = load_table_schema("cx_multicell_udt_collection_paths");
    let multicell = [("mp", MulticellKind::Udt), ("ml", MulticellKind::List)];
    if let Some(n) = run_fixture("cx_multicell_udt_collection_paths", schema, &multicell).await {
        println!(
            "cx_multicell_udt_collection_paths: {n} rows compared (multicell UDT + list paths)"
        );
    }
}

/// `cass.cql_types.complex.legacy_dropped_tuple_udt_fields`
///
/// Two-part assertion:
///   1. Reading gen-1 with the GEN-1 schema (all 3 columns declared) decodes the
///      tuple + frozen-UDT + survivor columns, matching the gen-1 golden.
///   2. Reading gen-1 with the EVOLVED schema (only `survivor` declared) must
///      SKIP the dropped `drop_tuple` / `drop_udt` columns using the on-disk
///      SerializationHeader column-set/type metadata, yet still decode `survivor`
///      correctly (its bytes follow the dropped columns' bytes on disk).
///
/// Regression coverage for issue #1080 (now FIXED): previously the gen-1
/// `drop_udt frozen<person_type>` column failed to decode, and that frozen-UDT
/// decode error also dropped the trailing `survivor` column (Err→break blast
/// radius). Now part 1 decodes tuple + frozen-UDT + survivor, and part 2 exercises
/// the dropped-column-skip (bytes consumed via on-disk header metadata, not emitted).
#[tokio::test]
async fn legacy_dropped_tuple_udt_fields_parity() {
    let table = "cx_legacy_dropped_tuple_udt";
    let dir = match fixture_dir(table) {
        Some(d) => d,
        None => {
            skip_or_fail(table, "no fixture dir under CQLITE_DATASETS_ROOT");
            return;
        }
    };
    if !binary_data_db_present(&dir, "nb-1-big") {
        skip_or_fail(table, "binary nb-1-big-Data.db absent");
        return;
    }

    // --- Part 1: gen-1 with full schema (all 3 columns) ---
    let gen1_golden = golden_for_gen(table, "nb-1-big")
        .unwrap_or_else(|| panic!("{table}: nb-1-big golden missing"));
    let full_schema = load_table_schema(table);
    let expected_full = expected_from_golden(&gen1_golden, &[]);
    let actual_full = actual_from_scan(&dir, full_schema).await;
    compare_rows(
        &format!("{table}#gen1-full-schema"),
        &expected_full,
        &actual_full,
    );
    println!(
        "{table}: gen-1 full-schema decoded {} rows (tuple + frozen UDT + survivor)",
        expected_full.len()
    );

    // --- Part 2: gen-1 with EVOLVED schema (drop_tuple/drop_udt removed) ---
    // The dropped columns must be skipped via on-disk header metadata; survivor
    // must still decode correctly. Expected = survivor-only projection of gen-1.
    let evolved_schema = evolved_schema_survivor_only(table);
    let mut expected_survivor: ExpectedRows = BTreeMap::new();
    for (ck, cols) in &expected_full {
        if let Some(v) = cols.get("survivor") {
            let mut m = BTreeMap::new();
            m.insert("survivor".to_string(), v.clone());
            expected_survivor.insert(ck.clone(), m);
        }
    }
    let actual_evolved = actual_from_scan(&dir, evolved_schema).await;

    // Survivor must match AND the dropped columns must NOT be emitted.
    let mut errors: Vec<String> = Vec::new();
    for (ck, cols) in &expected_survivor {
        let act = actual_evolved.get(ck);
        match act.and_then(|c| c.get("survivor")) {
            Some(v) if v == &cols["survivor"] => {}
            Some(v) => errors.push(format!(
                "[{table}#evolved] ck={ck} survivor mismatch after dropping complex cols: \
                 expected {} actual {}",
                render_full(&cols["survivor"]),
                render_full(v),
            )),
            None => errors.push(format!(
                "[{table}#evolved] ck={ck} survivor MISSING — dropped-column byte skip likely \
                 misaligned the remaining column",
            )),
        }
        if let Some(c) = act {
            if c.contains_key("drop_tuple") || c.contains_key("drop_udt") {
                errors.push(format!(
                    "[{table}#evolved] ck={ck} a DROPPED column ({:?}) leaked into the decode \
                     despite being absent from the supplied (evolved) schema",
                    c.keys().collect::<Vec<_>>()
                ));
            }
        }
    }
    assert!(
        errors.is_empty(),
        "[{table}] legacy dropped-column parity FAILED ({} error(s)):\n{}",
        errors.len(),
        errors.join("\n")
    );
    println!(
        "{table}: gen-1 evolved-schema skipped dropped tuple/UDT, survivor intact ({} rows)",
        expected_survivor.len()
    );
}

/// Build the evolved schema for `cx_legacy_dropped_tuple_udt`: same PK/CK, only
/// the `survivor text` regular column remains (drop_tuple / drop_udt removed),
/// mirroring the post-DROP schema state Cassandra would have after the two flushes.
fn evolved_schema_survivor_only(table: &str) -> TableSchema {
    let mut schema = load_table_schema(table);
    schema.columns.retain(|c| c.name == "survivor");
    schema
}
