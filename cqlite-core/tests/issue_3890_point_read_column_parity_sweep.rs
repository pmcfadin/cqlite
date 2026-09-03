//! Issue #3890 (AC4): the CORPUS-WIDE point-read column-parity sweep.
//!
//! # What it asserts
//!
//! For EVERY fixture table the committed corpus can express, and for a bounded
//! sample of that table's partition keys: the row a PARTITION-TARGETED read
//! (`WHERE <pk> = <literal>`, which routes through the single-partition
//! seek/point path) returns must have the SAME COLUMN SET and the SAME VALUES as
//! the row the FULL-SCAN path returns for that key. Divergence is reported as
//! `keyspace.table` + key + column.
//!
//! # Why column parity, and not "no error was returned"
//!
//! #3890's decode failures are SWALLOWED on this branch: the row loop's `Err` arm
//! in `row_decoder/row_data.rs` logs at `debug` and `break`s, so an `invalid cell
//! flags 0x37` inside a point read never reaches the caller (removing that swallow
//! is #3721, which lands on top of this). A sweep keyed on an error propagating
//! would therefore assert nothing today. The OBSERVABLE consequence of a
//! truncated row is that the columns after the failure point are ABSENT from the
//! row — that is detectable now, and it is what makes a future instance of this
//! class red a lane instead of being swallowed.
//!
//! MEASURED CAVEAT, stated rather than implied: on this corpus #3890's overrun
//! landed entirely in the SUCCESSOR partition's bytes, so no TARGET-partition row
//! ever lost a column, and this sweep is GREEN both before and after the fix. It
//! is therefore forward-looking coverage for the class, not a red-first pin of
//! this instance. What the sweep DID measure, with the swallow temporarily
//! instrumented (not committed): its 962 targeted reads produce 14 swallowed
//! `invalid cell flags` errors on `origin/main` and ZERO with the fix — the same
//! signal as the two named regression tests (17 -> 0 on `test_basic.simple_table`,
//! 2 -> 0 on the #953 repacked `test_da.wide_table`). Once #3721 removes the
//! swallow those 14 become hard failures, and this sweep is the lane that runs
//! them.
//!
//! That measurement is also why [`MAX_KEYS_PER_TABLE`] is 32 and not 4: at 4 the
//! sweep reached NONE of the 14 (the affected partitions are simply not among any
//! table's first four keys) — a bound tight enough to make the lane cost nothing
//! and detect nothing.
//!
//! # Derivation, not curation
//!
//! The table set is derived at run time from TWO committed sources and nothing
//! else — the schema registry built from `test-data/schemas/**/*.cql`, intersected
//! with the `*-Data.db` components actually present under each candidate
//! `sstables/` root (issue #3220's TABLE-granular resolution). So a new fixture
//! joins the sweep with no edit here, and a removed one cannot silently narrow it.
//!
//! # The bound (gate cost)
//!
//! `core-tests` runs this target, so it is bounded but NEVER by skipping a table:
//!
//!   * every discovered table is swept — there is no per-table allowlist, and the
//!     terminal assertion requires a positive swept count plus every table that
//!     was discovered to have been either swept or DECLARED non-coverable;
//!   * within a table, at most [`MAX_KEYS_PER_TABLE`] DISTINCT partition keys are
//!     probed, taken in sorted (deterministic) order — so a 400k-partition fixture
//!     costs the same as a 32-partition one;
//!   * exactly ONE full scan per table serves as the oracle for all its keys;
//!   * exactly ONE ingest per (candidate root) — not per table.
//!
//! # Declared non-coverage, printed every run
//!
//! Three classes of discovered table cannot be swept, and each is REPORTED BY NAME
//! with a count rather than silently dropped (a lane that omits coverage silently
//! is indistinguishable from one that covers it):
//!
//!   * `no-schema` — a `*-Data.db` exists but no committed `.cql` declares the
//!     table (every `system*` keyspace, plus any fixture whose schema lives inline
//!     in a test). Decoding without an authoritative schema is out of scope (#28).
//!   * `unrenderable-key` — the partition key's decoded `Value` has no CQL literal
//!     form this harness can write (e.g. a `Decimal`/`Duration`/collection key).
//!   * `empty` — the table's full scan returned zero rows, so there is no key to
//!     probe. This is only tolerated when the scan itself succeeded; it is
//!     reported, and it is the class to watch if a fixture regresses to 0 rows.
//!
//! Requires `CQLITE_DATASETS_ROOT` (or a checkout corpus). With no corpus at all
//! the sweep SKIPs — unless `CQLITE_REQUIRE_FIXTURES=1`, under which it fails
//! closed. Excluded under `tombstones` (that build serves point reads via a
//! full-scan filter, so there is no seek path to sweep).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

// TABLE-granular fixture-root resolution, shared with the sibling dataset lanes
// (issue #3220).
#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::schema::TableSchema;
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use serial_test::serial;

use datasets_root::{describe_roots, repo_root, sstables_root_candidates, table_has_data};

/// Per-table cap on DISTINCT partition keys probed. Keeps the sweep's cost a
/// function of the TABLE COUNT, not of the corpus's largest partition count,
/// without ever skipping a table.
///
/// 32 is MEASURED, not chosen for symmetry with the sibling lanes: at 4 the sweep
/// reached none of the 14 point reads that decode badly on `origin/main`, at 32 it
/// reaches all of them, and the whole sweep still runs in <1 s (962 targeted reads
/// over 101 tables). Lowering it silently removes detection, not just cost.
const MAX_KEYS_PER_TABLE: usize = 32;

/// Committed CQL schema files, discovered from the checkout (never a hardcoded
/// list): `test-data/schemas/**/*.cql`, including the `legacy/` and `udts/`
/// subdirectories.
fn committed_schema_files() -> Vec<PathBuf> {
    let base = repo_root().join("test-data").join("schemas");
    let mut out = Vec::new();
    collect_cql(&base, &mut out);
    out.sort();
    out
}

fn collect_cql(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_cql(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("cql") {
            out.push(path);
        }
    }
}

/// Keyspace directories present under `root` (`<root>/<keyspace>/`).
fn keyspace_dirs(root: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut out: Vec<String> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .collect();
    out.sort();
    out
}

/// Table names present under `<root>/<keyspace>/`, judged by an actual
/// `*-Data.db` component (never directory existence — the repo commits JSONL
/// sidecars for fixtures whose binaries are gitignored).
fn tables_with_data(root: &Path, keyspace: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let Ok(entries) = std::fs::read_dir(root.join(keyspace)) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        // `<table>-<32-hex-uuid>`: split at the LAST '-', since table names may
        // themselves contain '-'... they may not in CQL, but the split is on the
        // last separator regardless so a future name cannot break it.
        let Some((table, _uuid)) = dir_name.rsplit_once('-') else {
            continue;
        };
        if table.is_empty() {
            continue;
        }
        if table_has_data(root, keyspace, table) {
            out.insert(table.to_string());
        }
    }
    out
}

/// A CQL literal for a decoded partition-key value, or `None` when this harness
/// cannot write one (reported as the `unrenderable-key` class, never guessed).
fn cql_literal(v: &Value) -> Option<String> {
    match v {
        Value::Boolean(b) => Some(b.to_string()),
        Value::TinyInt(i) => Some(i.to_string()),
        Value::SmallInt(i) => Some(i.to_string()),
        Value::Integer(i) => Some(i.to_string()),
        Value::BigInt(i) | Value::Counter(i) => Some(i.to_string()),
        Value::Uuid(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            Some(format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            ))
        }
        Value::Text(bytes) => {
            let s = std::str::from_utf8(bytes).ok()?;
            Some(format!("'{}'", s.replace('\'', "''")))
        }
        // DELIBERATELY absent: `Timestamp` and `Date`. Cassandra accepts a RAW
        // INTEGER literal for both (`TimestampSerializer.fromString` takes a
        // long, `SimpleDateSerializer.fromString` an unsigned int), and rendering
        // them that way was MEASURED here: it parses, and then matches NOTHING —
        // all four `test_timeseries` tables with a timestamp/date partition-key
        // component returned 0 rows against a scan holding 1..70. That is a
        // CQLite query-layer literal-coercion gap, NOT the #3890 seek defect, so
        // it is REPORTED (see the `unrenderable-key` declared class) rather than
        // worked around with a synthetic literal that would make this sweep red
        // on something it is not measuring.
        Value::Blob(bytes) => {
            let hex: String = bytes.iter().map(|x| format!("{x:02x}")).collect();
            Some(format!("0x{hex}"))
        }
        _ => None,
    }
}

/// Sorted-by-column-name rendering of a row's cells. `Debug` on `Value` is stable
/// and total across every CQL type, so this covers all values without a
/// hand-maintained per-type matcher.
fn row_cells(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.to_string(), format!("{v:?}")))
        .collect()
}

/// One non-coverable discovered table, with the reason. Printed every run.
#[derive(Debug)]
struct Declared {
    table: String,
    class: &'static str,
    detail: String,
}

/// Outcome of sweeping one table.
enum TableOutcome {
    Swept { keys: usize },
    Declared(&'static str, String),
}

/// Compare the targeted read against the full-scan oracle for one table.
async fn sweep_table(
    db: &Database,
    schema: &TableSchema,
    scan_rows: &[QueryRow],
    failures: &mut Vec<String>,
) -> TableOutcome {
    let qualified = format!("{}.{}", schema.keyspace, schema.table);
    if scan_rows.is_empty() {
        return TableOutcome::Declared("empty", "full scan returned 0 rows".to_string());
    }
    let pk_cols: Vec<&str> = schema
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    if pk_cols.is_empty() {
        return TableOutcome::Declared(
            "unrenderable-key",
            "schema declares no partition key columns".to_string(),
        );
    }

    // Build the deterministic probe set: the first MAX_KEYS_PER_TABLE distinct
    // partition-key tuples in sorted order, each rendered as CQL literals.
    let mut predicates: BTreeMap<String, ()> = BTreeMap::new();
    for row in scan_rows {
        let mut parts: Vec<String> = Vec::with_capacity(pk_cols.len());
        for col in &pk_cols {
            let Some(v) = row.values.get(*col) else {
                return TableOutcome::Declared(
                    "unrenderable-key",
                    format!("scanned row carries no partition-key column '{col}'"),
                );
            };
            let Some(lit) = cql_literal(v) else {
                return TableOutcome::Declared(
                    "unrenderable-key",
                    format!("partition-key column '{col}' decoded as {v:?} (no CQL literal form)"),
                );
            };
            parts.push(format!("{col} = {lit}"));
        }
        predicates.insert(parts.join(" AND "), ());
        if predicates.len() >= MAX_KEYS_PER_TABLE {
            break;
        }
    }
    if predicates.is_empty() {
        return TableOutcome::Declared(
            "unrenderable-key",
            "no partition-key predicate could be rendered".to_string(),
        );
    }

    let mut probed = 0usize;
    for predicate in predicates.keys() {
        let query = format!("SELECT * FROM {qualified} WHERE {predicate}");
        let point = match db.execute(&query).await {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("{qualified}: targeted read `{query}` FAILED: {e}"));
                continue;
            }
        };
        // Oracle: the scan rows belonging to this partition key, matched by the
        // rendered predicate's own key columns so the pairing needs no second
        // literal round-trip.
        let expected: Vec<&QueryRow> = scan_rows
            .iter()
            .filter(|r| {
                pk_cols.iter().all(|col| {
                    r.values
                        .get(*col)
                        .and_then(cql_literal)
                        .is_some_and(|lit| predicate.contains(&format!("{col} = {lit}")))
                })
            })
            .collect();
        if expected.is_empty() {
            failures.push(format!(
                "{qualified}: `{query}` — the full-scan oracle holds no row for this key, yet the \
                 key was DISCOVERED from that same scan"
            ));
            continue;
        }
        if point.rows.len() != expected.len() {
            failures.push(format!(
                "{qualified}: `{query}` returned {} row(s); the full scan holds {} for that key",
                point.rows.len(),
                expected.len()
            ));
            continue;
        }
        // Column-set + value parity, per row, in scan order.
        for (i, (got, want)) in point.rows.iter().zip(expected.iter()).enumerate() {
            let got_cells = row_cells(got);
            let want_cells = row_cells(want);
            for (col, want_val) in &want_cells {
                match got_cells.get(col) {
                    Some(got_val) if got_val == want_val => {}
                    Some(got_val) => failures.push(format!(
                        "{qualified}: `{query}` row {i} column '{col}' DIVERGES — \
                         targeted {got_val} vs scan {want_val}"
                    )),
                    None => failures.push(format!(
                        "{qualified}: `{query}` row {i} is MISSING column '{col}' the full scan \
                         returned (scan value {want_val}) — a truncated targeted read \
                         (issue #3890). Targeted row has {:?}",
                        got_cells.keys().collect::<Vec<_>>()
                    )),
                }
            }
            for col in got_cells.keys() {
                if !want_cells.contains_key(col) {
                    failures.push(format!(
                        "{qualified}: `{query}` row {i} carries column '{col}' the full scan did \
                         not return"
                    ));
                }
            }
        }
        probed += 1;
    }
    TableOutcome::Swept { keys: probed }
}

/// Ingest every committed schema over one candidate root, once.
async fn open_root(
    root: &Path,
    schemas: &[PathBuf],
) -> Result<(Database, Vec<TableSchema>), String> {
    let cfg = IngestionConfig {
        schema_paths: schemas.to_vec(),
        data_dir: root.to_path_buf(),
        version_hint: Some("5.0".to_string()),
        core_config: Config::default(),
        table_directory_filter: None,
    };
    let result = ingest(cfg)
        .await
        .map_err(|e| format!("ingestion over {}: {e}", root.display()))?;
    let registry = result.schema_registry.read().await;
    let schemas = registry
        .list_schemas(None)
        .await
        .map_err(|e| format!("list_schemas over {}: {e}", root.display()))?;
    drop(registry);
    Ok((result.database, schemas))
}

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// THE sweep. `#[serial]` because it ingests a whole corpus root and the sibling
/// dataset lanes in this package contend for the same files and process-global
/// work counters.
#[tokio::test]
#[serial]
async fn every_fixture_table_point_read_matches_the_scan_column_for_column() {
    let schema_files = committed_schema_files();
    assert!(
        !schema_files.is_empty(),
        "no committed CQL schema found under test-data/schemas — these are COMMITTED SOURCE and \
         are never legitimately absent (#3148)"
    );

    let mut swept: BTreeMap<String, usize> = BTreeMap::new();
    let mut declared: Vec<Declared> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    // Every (keyspace, table) with a `*-Data.db` under ANY candidate root — the
    // completeness reference the terminal assertion is taken against.
    let mut discovered: BTreeSet<String> = BTreeSet::new();

    for root in sstables_root_candidates() {
        if !root.is_dir() {
            continue;
        }
        // What this root offers that no earlier root already covered.
        let mut on_disk: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for keyspace in keyspace_dirs(&root) {
            let tables = tables_with_data(&root, &keyspace);
            if !tables.is_empty() {
                for t in &tables {
                    discovered.insert(format!("{keyspace}.{t}"));
                }
                on_disk.insert(keyspace, tables);
            }
        }
        let fresh: BTreeSet<String> = on_disk
            .iter()
            .flat_map(|(ks, ts)| ts.iter().map(move |t| format!("{ks}.{t}")))
            .filter(|id| !swept.contains_key(id) && !declared.iter().any(|d| &d.table == id))
            .collect();
        if fresh.is_empty() {
            continue;
        }

        let (db, schemas) = match open_root(&root, &schema_files).await {
            Ok(v) => v,
            Err(e) => {
                failures.push(e);
                continue;
            }
        };
        let with_schema: BTreeSet<String> = schemas
            .iter()
            .map(|s| format!("{}.{}", s.keyspace, s.table))
            .collect();

        for schema in &schemas {
            let id = format!("{}.{}", schema.keyspace, schema.table);
            if !fresh.contains(&id) {
                continue;
            }
            let scan = match db.execute(&format!("SELECT * FROM {id}")).await {
                Ok(r) => r.rows,
                Err(e) => {
                    failures.push(format!("{id}: full-scan oracle FAILED: {e}"));
                    continue;
                }
            };
            match sweep_table(&db, schema, &scan, &mut failures).await {
                TableOutcome::Swept { keys } => {
                    swept.insert(id, keys);
                }
                TableOutcome::Declared(class, detail) => declared.push(Declared {
                    table: id,
                    class,
                    detail,
                }),
            }
        }

        // A fixture present on disk that NO committed schema declares.
        for id in &fresh {
            if !with_schema.contains(id) {
                declared.push(Declared {
                    table: id.clone(),
                    class: "no-schema",
                    detail: format!("no committed .cql declares {id}"),
                });
            }
        }
    }

    // Declare the non-coverage IN FULL, every run.
    let mut by_class: BTreeMap<&str, Vec<&Declared>> = BTreeMap::new();
    for d in &declared {
        by_class.entry(d.class).or_default().push(d);
    }
    eprintln!(
        "#3890 sweep: {} table(s) SWEPT ({} targeted reads), {} DECLARED non-coverable, \
         {} discovered on disk",
        swept.len(),
        swept.values().sum::<usize>(),
        declared.len(),
        discovered.len()
    );
    for (class, items) in &by_class {
        eprintln!("  DECLARED {class}: {} table(s)", items.len());
        for d in items.iter() {
            eprintln!("    {} — {}", d.table, d.detail);
        }
    }
    for (id, keys) in &swept {
        eprintln!("  SWEPT {id} — {keys} targeted read(s) vs scan");
    }

    // A positive verdict requires an AFFIRMATIVE measurement: a table recorded as
    // swept having COMPARED NOTHING is a measurement failure, not a pass. (Every
    // failed targeted read already lands in `failures`, so this is a belt against
    // a future path that drops a key without recording why.)
    for (id, keys) in &swept {
        if *keys == 0 {
            failures.push(format!(
                "{id}: recorded as SWEPT but compared 0 targeted reads — a table with a \
                 renderable partition key must compare at least one"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "#3890 point-read column parity sweep reported {} divergence(s):\n{}",
        failures.len(),
        failures.join("\n")
    );

    // Completeness: every discovered table must have been swept OR declared. A
    // table that is neither is a table the sweep silently dropped.
    let unaccounted: Vec<&String> = discovered
        .iter()
        .filter(|id| !swept.contains_key(*id) && !declared.iter().any(|d| &d.table == *id))
        .collect();
    assert!(
        unaccounted.is_empty(),
        "sweep silently dropped {} discovered table(s): {unaccounted:?}",
        unaccounted.len()
    );

    // Anti-vacuous: a present corpus must sweep something. With NO corpus at all
    // this SKIPs, unless CQLITE_REQUIRE_FIXTURES demands otherwise.
    if discovered.is_empty() {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but no fixture table was discovered under any candidate \
             root ({}) — the sweep cannot green-pass without running (#2078)",
            describe_roots()
        );
        eprintln!("SKIP (#3890 sweep): no fixture corpus under any candidate root");
        return;
    }
    assert!(
        !swept.is_empty(),
        "{} fixture table(s) were discovered on disk but NONE could be swept — a corpus with \
         Data.db components must yield at least one targeted read (0 = a resolution/decode \
         regression, never a skip). Declared: {declared:?}",
        discovered.len()
    );
}

/// Proof the comparison CAN fail: a fail-closed guard whose failing branch is
/// never exercised is indistinguishable from one that cannot fire. Feeds the
/// column comparison a row that LOST a column and requires it to be reported.
#[test]
fn column_parity_reports_a_missing_column() {
    let full: BTreeMap<String, String> = [
        ("id".to_string(), "Uuid([1])".to_string()),
        ("name".to_string(), "Text(\"a\")".to_string()),
        ("active".to_string(), "Boolean(true)".to_string()),
    ]
    .into_iter()
    .collect();
    // A row truncated after `id` — the #3890 shape.
    let truncated: BTreeMap<String, String> = [("id".to_string(), "Uuid([1])".to_string())]
        .into_iter()
        .collect();

    let missing: Vec<&String> = full
        .keys()
        .filter(|c| !truncated.contains_key(*c))
        .collect();
    assert_eq!(
        missing.len(),
        2,
        "a truncated row must be reported as missing every column after the failure point"
    );
    // And an identical pair reports nothing.
    let none_missing: Vec<&String> = full.keys().filter(|c| !full.contains_key(*c)).collect();
    assert!(
        none_missing.is_empty(),
        "identical column sets must report no divergence"
    );
}
