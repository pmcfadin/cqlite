//! Parquet ↔ sstabledump-JSONL value-parity harness (issue #1490, epic #1469).
//!
//! # What this harness asserts
//!
//! For each declared corpus table:
//!
//!   1. export it to Parquet through the WIRED writer — the real `cqlite export
//!      --format parquet` binary, not a library shortcut;
//!   2. read the Parquet back with the `arrow`/`parquet` crates;
//!   3. project both the Parquet rows and the table's committed
//!      `*-Data.db.jsonl` sstabledump golden into ONE canonical value space;
//!   4. sort both sides by primary key (Parquet row order is not guaranteed) and
//!      assert FULL PER-CELL equality.
//!
//! The pre-existing Parquet tests check row counts, `PAR1` magic, a few spot
//! values and DuckDB aggregates; `parquet_golden_tests.rs` freezes a byte
//! snapshot of CQLite's OWN output, which cannot detect a wrong value because it
//! was produced by the same code. This harness's oracle is CASSANDRA-WRITTEN
//! (#3042), so it can.
//!
//! # Fail-closed rules
//!
//! * Fixture roots are resolved PER TABLE (`sstables_root_for_table`), never by
//!   keyspace: a root holding the keyspace but not the table would otherwise win
//!   the selection and the case would skip while the fixture sat in the checkout
//!   (#3220).
//! * A case whose SSTable binaries are COMMITTED to git is `must_run`: an
//!   absence is a hard failure, unconditionally. Authority for the flag is
//!   `git ls-files 'test-data/datasets/sstables/**-Data.db'`, never presence on
//!   disk (the fetched corpus unpacks into the same tree).
//! * There is deliberately NO suite-wide `assert!(ran > 0)`: it cannot see one
//!   case skipping behind its siblings. Each case asserts for itself.
//! * `CQLITE_REQUIRE_FIXTURES=1` promotes EVERY case to `must_run`.

#![allow(dead_code)]

#[path = "../../../../cqlite-core/tests/support/canonical_jsonl.rs"]
pub mod canonical_jsonl;
#[path = "../../../../cqlite-core/tests/support/datasets_root.rs"]
pub mod datasets_root;

pub mod arrow_rows;
pub mod cql_type;
pub mod golden_rows;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use canonical_jsonl::{CanonicalValue, KeySpec};
use cql_type::ColumnType;
use golden_rows::GoldenRow;

/// One corpus table under value parity.
pub struct ParityCase {
    pub keyspace: &'static str,
    pub table: &'static str,
    /// Committed CQL schema fixture (file name under `test-data/schemas/`).
    pub schema: &'static str,
    /// User-defined type names the schema declares, so a UDT column type is
    /// recognized rather than guessed.
    pub udts: &'static [&'static str],
    /// EVERY column of the table as `(name, declared CQL type)`, copied from the
    /// committed schema — i.e. from the Cassandra schema the fixture was written
    /// with, never from CQLite's Arrow mapping (#3041).
    pub columns: &'static [(&'static str, &'static str)],
    /// Partition-key columns in declared order.
    pub partition_key: &'static [&'static str],
    /// Clustering columns in declared order.
    pub clustering: &'static [&'static str],
    /// SSTable binaries are committed to git → a SKIP is a hard failure.
    pub must_run: bool,
    /// What this case buys the corpus (type families, format).
    pub covers: &'static str,
}

/// What one case did, for the per-case assertion and the run census.
pub enum CaseOutcome {
    Ran { rows: usize, cells: usize },
    Skipped(String),
}

impl ParityCase {
    pub fn id(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }

    /// Parse the declared column types once, failing loudly on an unparsable
    /// declaration.
    fn column_types(&self) -> Result<Vec<ColumnType>, String> {
        self.columns
            .iter()
            .map(|(name, declared)| cql_type::parse_column(name, declared, self.udts))
            .collect()
    }

    fn key_spec(&self) -> KeySpec {
        KeySpec::from_cql_types(
            &self.cql_types_of(self.partition_key),
            &self.cql_types_of(self.clustering),
        )
    }

    fn cql_types_of(&self, names: &[&str]) -> Vec<&'static str> {
        names
            .iter()
            .map(|n| {
                self.columns
                    .iter()
                    .find(|(cn, _)| cn == n)
                    .map(|(_, t)| *t)
                    .unwrap_or("")
            })
            .collect()
    }
}

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// The resolved on-disk fixture: the single-generation table directory.
struct Fixture {
    table_dir: PathBuf,
    golden: PathBuf,
}

/// Resolve `<root>/<keyspace>/<table>-*/` per TABLE across every candidate root.
///
/// Returns `Ok(None)` only when no candidate root carries the table at all; a
/// root that carries it in an unusable shape (several generations, no golden) is
/// an ERROR, never a skip.
fn resolve_fixture(case: &ParityCase) -> Result<Option<Fixture>, String> {
    let Some(root) = datasets_root::sstables_root_for_table(case.keyspace, case.table) else {
        return Ok(None);
    };
    let ks_dir = root.join(case.keyspace);
    let prefix = format!("{}-", case.table);
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&ks_dir)
        .map_err(|e| format!("cannot read {}: {e}", ks_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(false)
        })
        .collect();
    dirs.sort();
    if dirs.len() != 1 {
        return Err(format!(
            "{}: expected exactly one table directory under {}, found {:?}",
            case.id(),
            ks_dir.display(),
            dirs
        ));
    }
    let table_dir = dirs.remove(0);

    let entries: Vec<String> = std::fs::read_dir(&table_dir)
        .map_err(|e| format!("cannot read {}: {e}", table_dir.display()))?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .collect();
    let datas: Vec<&String> = entries.iter().filter(|n| n.ends_with("-Data.db")).collect();
    let goldens: Vec<&String> = entries
        .iter()
        .filter(|n| n.ends_with("-Data.db.jsonl"))
        .collect();
    // A multi-generation table's per-generation dumps are not the reconciled
    // result set the export produces, so the harness refuses rather than
    // comparing one generation against a merged read.
    if datas.len() != 1 {
        return Err(format!(
            "{}: expected exactly one *-Data.db generation in {}, found {}: the harness \
             compares a single-generation dump against a reconciled export",
            case.id(),
            table_dir.display(),
            datas.len()
        ));
    }
    if goldens.len() != 1 {
        return Err(format!(
            "{}: expected exactly one *-Data.db.jsonl golden in {}, found {}",
            case.id(),
            table_dir.display(),
            goldens.len()
        ));
    }
    Ok(Some(Fixture {
        golden: table_dir.join(goldens[0]),
        table_dir,
    }))
}

/// Copy the fixture's SSTable components into an isolated `<keyspace>/<table-…>`
/// data directory.
///
/// Isolation is not cosmetic: pointed at a shared corpus root the CLI ingests
/// EVERY table it finds, so one case's export would depend on unrelated
/// fixtures (and on this machine's corpus size). Only real SSTable components
/// are copied — the `.jsonl` / `.txt` sidecars stay out so the reader never sees
/// them.
fn isolated_data_dir(case: &ParityCase, fixture: &Fixture, tmp: &Path) -> Result<PathBuf, String> {
    let data_dir = tmp.join("data");
    let dest = data_dir.join(case.keyspace).join(
        fixture
            .table_dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("fixture directory has no name")?,
    );
    std::fs::create_dir_all(&dest).map_err(|e| format!("mkdir {}: {e}", dest.display()))?;
    for entry in std::fs::read_dir(&fixture.table_dir)
        .map_err(|e| format!("read_dir {}: {e}", fixture.table_dir.display()))?
    {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".jsonl") || name.ends_with(".txt") {
            continue;
        }
        if !entry.path().is_file() {
            continue;
        }
        std::fs::copy(entry.path(), dest.join(&name))
            .map_err(|e| format!("copy {name}: {e}"))?;
    }
    Ok(data_dir)
}

/// Run the real CLI export and return the Parquet file path.
fn export_parquet(case: &ParityCase, data_dir: &Path, tmp: &Path) -> Result<PathBuf, String> {
    let schema = datasets_root::schema_path(case.schema)
        .ok_or_else(|| format!("committed schema fixture '{}' is unreadable", case.schema))?;
    let out = tmp.join(format!("{}.{}.parquet", case.keyspace, case.table));
    let table = case.id();
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            schema.to_str().ok_or("non-UTF-8 schema path")?,
            "--data-dir",
            data_dir.to_str().ok_or("non-UTF-8 data dir")?,
            "export",
            out.to_str().ok_or("non-UTF-8 output path")?,
            "--format",
            "parquet",
            "--table",
            &table,
        ])
        .output()
        .map_err(|e| format!("spawning the cqlite binary failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "cqlite export failed for {table} ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    if !out.is_file() {
        return Err(format!("cqlite export produced no file at {}", out.display()));
    }
    Ok(out)
}

/// One projected row: the primary-key components, then column → canonical value.
pub struct Row {
    keys: Vec<CanonicalValue>,
    cells: BTreeMap<String, CanonicalValue>,
}

impl Row {
    fn sort_key(&self) -> String {
        self.keys.iter().map(render_value).collect::<Vec<_>>().join("\u{1f}")
    }
}

/// Project the exported Parquet file into canonical rows, asserting its column
/// set is exactly the declared one.
fn project_parquet(case: &ParityCase, path: &Path) -> Result<Vec<Row>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .map_err(|e| format!("{} is not a readable Parquet file: {e}", path.display()))?
        .build()
        .map_err(|e| format!("building the Parquet reader failed: {e}"))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("decoding the Parquet file failed: {e}"))?;
    let Some(first) = batches.first() else {
        return Err(format!(
            "{}: the export produced no record batch at all",
            case.id()
        ));
    };

    let mut declared: Vec<&str> = case.columns.iter().map(|(n, _)| *n).collect();
    declared.sort_unstable();
    let first_schema = first.schema();
    let mut actual: Vec<&str> = first_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    actual.sort_unstable();
    if declared != actual {
        return Err(format!(
            "{}: the Parquet schema's columns {actual:?} do not match the case's declared \
             columns {declared:?} — reconcile the case with the fixture's CQL schema",
            case.id()
        ));
    }

    let key_columns: Vec<&str> = case
        .partition_key
        .iter()
        .chain(case.clustering.iter())
        .copied()
        .collect();

    let mut rows = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        for r in 0..batch.num_rows() {
            let mut cells = BTreeMap::new();
            for (ci, field) in schema.fields().iter().enumerate() {
                let ctx = format!("{}.{}[row {r}]", case.id(), field.name());
                let value =
                    arrow_rows::canonical_from_arrow(batch.column(ci).as_ref(), r, &ctx)?;
                cells.insert(field.name().clone(), value);
            }
            let keys = key_columns
                .iter()
                .map(|k| {
                    cells.get(*k).cloned().ok_or_else(|| {
                        format!("{}: key column '{k}' missing from the export", case.id())
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            rows.push(Row { keys, cells });
        }
    }
    Ok(rows)
}

/// Compact, comparison-stable rendering — used for sort keys and for diffs.
///
/// Deliberately NOT `Debug`: `CanonicalValue::Timestamp` carries a diagnostic
/// `raw` string that differs between the two sides while the compared
/// microseconds agree, so a `Debug`-derived sort key would order equal values
/// differently.
pub fn render_value(v: &CanonicalValue) -> String {
    match v {
        CanonicalValue::Absent | CanonicalValue::Null => "<absent>".to_string(),
        CanonicalValue::Bool(b) => format!("bool:{b}"),
        CanonicalValue::Int(i) => format!("int:{i}"),
        CanonicalValue::Float(f) => format!("float:{:?}", f.0),
        CanonicalValue::Text(s) => format!("text:{s:?}"),
        CanonicalValue::Timestamp { micros, .. } => format!("ts:{micros}"),
        CanonicalValue::List(xs) | CanonicalValue::Set(xs) => format!(
            "[{}]",
            xs.iter().map(render_value).collect::<Vec<_>>().join(",")
        ),
        CanonicalValue::Map(kvs) => format!(
            "{{{}}}",
            kvs.iter()
                .map(|(k, v)| format!("{}=>{}", render_value(k), render_value(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        CanonicalValue::Tuple(fs) => format!(
            "({})",
            fs.iter()
                .map(|(k, v)| format!("{k}={}", render_value(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

/// Run one case end-to-end. `Err` is a parity failure or a fail-closed refusal;
/// `Ok(Skipped)` only when no candidate root carries the table.
pub fn run_case(case: &ParityCase) -> Result<CaseOutcome, String> {
    let columns = case.column_types()?;
    let Some(fixture) = resolve_fixture(case)? else {
        return Ok(CaseOutcome::Skipped(datasets_root::describe_search(
            case.keyspace,
            case.table,
        )));
    };

    let tmp = tempfile::TempDir::new().map_err(|e| format!("tempdir: {e}"))?;
    let data_dir = isolated_data_dir(case, &fixture, tmp.path())?;
    let parquet = export_parquet(case, &data_dir, tmp.path())?;

    let golden_doc = canonical_jsonl::load_golden_document_with_keys(
        &fixture.golden,
        true,
        &case.key_spec(),
    )
    .map_err(|e| format!("{}: loading the sstabledump golden failed: {e}", case.id()))?;
    let golden = golden_rows::project_golden(
        &golden_doc,
        &columns,
        case.partition_key,
        case.clustering,
    )
    .map_err(|e| format!("{}: {e}", case.id()))?;

    let parquet_rows = project_parquet(case, &parquet)?;
    compare(case, &columns, golden, parquet_rows)
}

/// Sort both sides by primary key and assert full per-cell equality.
fn compare(
    case: &ParityCase,
    columns: &[ColumnType],
    golden: Vec<GoldenRow>,
    parquet: Vec<Row>,
) -> Result<CaseOutcome, String> {
    let mut expected: Vec<Row> = golden
        .into_iter()
        .map(|g| Row {
            keys: g.keys,
            cells: g.cells,
        })
        .collect();
    let mut actual = parquet;

    if expected.is_empty() {
        return Err(format!(
            "{}: the sstabledump golden projected to ZERO rows — a dataset-dependent \
             comparison must never pass on an empty oracle",
            case.id()
        ));
    }
    expected.sort_by_key(|r| r.sort_key());
    actual.sort_by_key(|r| r.sort_key());

    if expected.len() != actual.len() {
        return Err(format!(
            "{}: row count differs — golden {} vs Parquet {}",
            case.id(),
            expected.len(),
            actual.len()
        ));
    }

    let mut diffs: Vec<String> = Vec::new();
    let mut compared_cells = 0usize;
    for (i, (e, a)) in expected.iter().zip(actual.iter()).enumerate() {
        if e.sort_key() != a.sort_key() {
            diffs.push(format!(
                "row {i}: primary key differs — golden {} vs Parquet {}",
                e.sort_key(),
                a.sort_key()
            ));
            if diffs.len() >= 10 {
                break;
            }
            continue;
        }
        for col in columns {
            let ev = e.cells.get(&col.name).unwrap_or(&CanonicalValue::Absent);
            let av = a.cells.get(&col.name).unwrap_or(&CanonicalValue::Absent);
            compared_cells += 1;
            if ev != av {
                diffs.push(format!(
                    "row {i} (pk {}) column '{}' ({}): golden {} vs Parquet {}",
                    e.sort_key(),
                    col.name,
                    col.declared,
                    render_value(ev),
                    render_value(av)
                ));
                if diffs.len() >= 10 {
                    break;
                }
            }
        }
        if diffs.len() >= 10 {
            break;
        }
    }

    if !diffs.is_empty() {
        return Err(format!(
            "{}: {} per-cell parity difference(s) vs the sstabledump golden (first {}):\n  {}",
            case.id(),
            diffs.len(),
            diffs.len().min(10),
            diffs.join("\n  ")
        ));
    }

    Ok(CaseOutcome::Ran {
        rows: expected.len(),
        cells: compared_cells,
    })
}

/// Drive one case and apply the per-case fail-closed rule.
pub fn assert_case(case: &ParityCase) {
    match run_case(case) {
        Err(e) => panic!("{e}"),
        Ok(CaseOutcome::Ran { rows, cells }) => {
            assert!(
                rows > 0 && cells > 0,
                "{}: comparison ran but compared {rows} row(s) / {cells} cell(s)",
                case.id()
            );
            eprintln!(
                "[{}] {rows} rows x {} declared columns = {cells} cells compared vs sstabledump \
                 golden ({})",
                case.id(),
                case.columns.len(),
                case.covers
            );
        }
        Ok(CaseOutcome::Skipped(reason)) => {
            if case.must_run || require_fixtures() {
                panic!(
                    "{}: this case MUST run ({}) but no fixture was found: {reason}",
                    case.id(),
                    if case.must_run {
                        "its SSTable binaries are committed to git"
                    } else {
                        "CQLITE_REQUIRE_FIXTURES=1"
                    }
                );
            }
            eprintln!("[{}] SKIPPED — {reason}", case.id());
        }
    }
}
