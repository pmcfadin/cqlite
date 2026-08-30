//! Parquet ↔ sstabledump-JSONL value-parity harness (issue #1490, epic #1469).
//!
//! # What this harness asserts
//!
//! For each declared corpus table:
//!
//!   1. export it to Parquet through the WIRED writer — the real `cqlite export
//!      --format parquet` binary, not a library shortcut;
//!   2. read the Parquet back with the `arrow`/`parquet` crates;
//!   3. validate the Parquet schema — column SET and every field's Arrow TYPE —
//!      against the case's independently declared CQL types (`arrow_expect`),
//!      BEFORE any value is compared: canonicalization is width-blind, so a
//!      wrong CQL→Arrow mapping would otherwise round-trip its values unchanged
//!      and pass;
//!   4. project both the Parquet rows and the table's committed
//!      `*-Data.db.jsonl` sstabledump golden into ONE canonical value space;
//!   5. sort both sides by primary key (Parquet row order is not guaranteed) and
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
//! * A recorded divergence is always PRECISE and SELF-RETIRING, never a skip:
//!   [`KnownGap`] (whole case) and [`KnownTypeGap`] (one column's Arrow type)
//!   both fail when the divergence stops reproducing, and both refuse to absorb
//!   a different failure. Both compare STRUCTURED failure data by EQUALITY, not
//!   a rendered message by containment — see `failure.rs` for the two weaker
//!   designs that leaked.

#![allow(dead_code)]

#[path = "../../../../cqlite-core/tests/support/canonical_jsonl.rs"]
pub mod canonical_jsonl;
#[path = "../../../../cqlite-core/tests/support/datasets_root.rs"]
pub mod datasets_root;

pub mod arrow_expect;
pub mod arrow_rows;
pub mod cql_type;
pub mod failure;
pub mod golden_rows;
pub mod spelling;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use canonical_jsonl::{CanonicalValue, KeySpec};
use cql_type::ColumnType;
use failure::{Failure, Failures, Stage};
use golden_rows::GoldenRow;

// `ExpectedFailure` is used only by the test binaries that DECLARE a
// `known_gap`, and each test binary includes this module by `#[path]` — so in a
// binary that declares none the re-export is genuinely unused. Allowed rather
// than dropped: the type belongs to the case-declaration surface beside
// `KnownGap`, and making callers reach into `failure::` for one half of the same
// record would be worse.
#[allow(unused_imports)]
pub use failure::{ExpectedFailure, KnownGap, KnownTypeGap};

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
    /// A DOCUMENTED, ISSUE-TRACKED divergence this case currently exhibits.
    ///
    /// The harness then asserts the divergence STILL EXISTS: the day the export
    /// bug is fixed this test FAILS and demands the case be promoted to a full
    /// parity case. A known gap can never quietly become an unnoticed pass, and
    /// it cannot be used to silence a NEW divergence either — the recorded
    /// failure set has to be EXACTLY the one that shows up, extras included.
    pub known_gap: Option<KnownGap>,
    /// DOCUMENTED, ISSUE-TRACKED Arrow TYPE gaps, one per column.
    ///
    /// A whole-case [`KnownGap`] defers the table's entire value comparison,
    /// which is far too blunt for a single column's wrong Arrow type: the other
    /// columns' values are still worth comparing, and so is this column's VALUE
    /// (a wrong type often renders the right value). So a type gap is recorded
    /// per COLUMN, and everything else about the case keeps running.
    pub known_type_gaps: &'static [KnownTypeGap],
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
        std::fs::copy(entry.path(), dest.join(&name)).map_err(|e| format!("copy {name}: {e}"))?;
    }
    Ok(data_dir)
}

/// Run the real CLI export and return the Parquet file path.
fn export_parquet(case: &ParityCase, data_dir: &Path, tmp: &Path) -> Result<PathBuf, Failures> {
    let schema = datasets_root::schema_path(case.schema).ok_or_else(|| {
        Failures::refusal(format!(
            "committed schema fixture '{}' is unreadable",
            case.schema
        ))
    })?;
    let out = tmp.join(format!("{}.{}.parquet", case.keyspace, case.table));
    let table = case.id();
    let utf8 = |p: Option<&str>, what: &str| -> Result<String, Failures> {
        p.map(str::to_string)
            .ok_or_else(|| Failures::refusal(format!("non-UTF-8 {what}")))
    };
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            &utf8(schema.to_str(), "schema path")?,
            "--data-dir",
            &utf8(data_dir.to_str(), "data dir")?,
            "export",
            &utf8(out.to_str(), "output path")?,
            "--format",
            "parquet",
            "--table",
            &table,
        ])
        .output()
        .map_err(|e| Failures::refusal(format!("spawning the cqlite binary failed: {e}")))?;
    if !output.status.success() {
        // A STRUCTURED failure, not prose: this is the one thing a whole-case
        // `known_gap` can record about an aborting export, and the record is
        // compared against the exact observed failure set (see `failure.rs`).
        return Err(Failures::one(Failure::ExportAborted {
            table: table.clone(),
            stderr: format!(
                "({}): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ),
        }));
    }
    if !out.is_file() {
        return Err(Failures::refusal(format!(
            "cqlite export produced no file at {}",
            out.display()
        )));
    }
    Ok(out)
}

/// One projected row: the primary-key components, then column → canonical value.
pub struct Row {
    keys: Vec<CanonicalValue>,
    cells: BTreeMap<String, CanonicalValue>,
}

impl Row {
    /// TEST-SUPPORT ONLY: overwrite one cell, so a self-test can prove the
    /// comparison notices. It cannot weaken a real run: nothing in the harness
    /// calls it, and a `Row` is rebuilt from the Parquet file on every case.
    pub fn overwrite_cell(&mut self, column: &str, value: CanonicalValue) {
        self.cells.insert(column.to_string(), value);
    }

    /// TEST-SUPPORT ONLY: overwrite one primary-key component (position in
    /// `partition_key ++ clustering` order).
    pub fn overwrite_key(&mut self, index: usize, value: CanonicalValue) {
        self.keys[index] = value;
    }

    pub fn cell(&self, column: &str) -> Option<&CanonicalValue> {
        self.cells.get(column)
    }

    fn sort_key(&self) -> String {
        self.keys
            .iter()
            .map(render_value)
            .collect::<Vec<_>>()
            .join("\u{1f}")
    }
}

/// What the Arrow TYPE stage determined — reported as data, so the stages after
/// it can decide what they can still do rather than being cancelled wholesale.
#[derive(Default)]
struct TypeCheck {
    /// Every type failure, plus any bookkeeping refusal.
    failures: Vec<Failure>,
    /// Columns whose exported Arrow type DIVERGED. Their VALUES cannot be
    /// meaningfully compared (a wrong type renders a different value), so the
    /// value comparison is recorded UNRUNNABLE for exactly these columns and
    /// RUNS for every other one.
    blocked_columns: Vec<String>,
    /// A harness bookkeeping refusal (a gap naming an undeclared column, an
    /// unclassifiable Arrow type): the type stage did not answer at all, so the
    /// value comparison cannot be scoped and is blocked whole.
    blocks_all_values: bool,
}

impl TypeCheck {
    /// Record an `ArrowType` failure AND the column it blocks, in one place, so
    /// the two can never drift apart.
    fn blocked_column(&mut self, failure: Failure) {
        if let Failure::ArrowType { mismatch, .. } = &failure {
            self.blocked_columns.push(mismatch.column.clone());
        }
        self.failures.push(failure);
    }
}

/// The TYPE half of the schema check: every field's Arrow type against the
/// case's independently declared CQL type.
///
/// Deliberately reports EVERY mismatching column rather than the first — a wrong
/// mapping usually affects a family of columns, and one message naming all of
/// them is what makes the diagnosis possible.
///
/// A column carrying a recorded [`KnownTypeGap`] whose `actual` EQUALS the
/// exported type is excused (loudly, on stderr); any other mismatch on that
/// column still fails, and a gap that no longer reproduces fails too.
fn validate_arrow_types(
    case: &ParityCase,
    columns: &[ColumnType],
    schema: &arrow::datatypes::Schema,
) -> TypeCheck {
    let mut check = TypeCheck::default();
    for gap in case.known_type_gaps {
        if !columns.iter().any(|c| c.name == gap.column) {
            check.failures.push(Failure::Refusal(format!(
                "a KnownTypeGap is recorded for column '{}', which the case does not \
                 declare — a gap must name a real column or it can never retire",
                gap.column
            )));
            check.blocks_all_values = true;
            return check;
        }
    }

    let mut excused: Vec<&'static str> = Vec::new();
    for field in schema.fields() {
        let Some(col) = columns.iter().find(|c| c.name == *field.name()) else {
            // Unreachable: the column-set equality above already ran.
            check.failures.push(Failure::Refusal(format!(
                "Parquet column '{}' has no declared CQL type",
                field.name()
            )));
            check.blocks_all_values = true;
            return check;
        };
        let gap = case
            .known_type_gaps
            .iter()
            .find(|g| g.column == col.name.as_str());
        match arrow_expect::validate_field(col, field.data_type()) {
            Ok(()) => {
                if let Some(gap) = gap {
                    check.failures.push(Failure::Refusal(format!(
                        "column '{}' records the KNOWN type gap {} ({}) but its exported Arrow \
                         type is now CORRECT — delete the KnownTypeGap so the column is covered \
                         for real, and close {}",
                        gap.column, gap.issue, gap.what, gap.issue
                    )));
                }
            }
            Err(Ok(mismatch)) => match gap {
                // Equality, not a substring: a DIFFERENT wrong type on this
                // column is a different defect and must not be absorbed.
                Some(gap) if gap.actual == mismatch.actual => excused.push(gap.issue),
                // The `note` is diagnostic decoration and is deliberately NOT
                // part of the failure's signature: the DEFECT is the mismatch,
                // and a whole-case `known_gap` must be able to record it whether
                // or not a per-column gap also names the column.
                Some(gap) => check.blocked_column(Failure::ArrowType {
                    note: Some(format!(
                        "the recorded type gap {} expected the export to produce {:?}, so this \
                         is a DIFFERENT type defect, which the gap must never hide",
                        gap.issue, gap.actual
                    )),
                    mismatch,
                }),
                None => check.blocked_column(Failure::ArrowType {
                    mismatch,
                    note: None,
                }),
            },
            Err(Err(refusal)) => {
                check.failures.push(Failure::Refusal(refusal));
                check.blocks_all_values = true;
            }
        }
    }

    for issue in excused {
        eprintln!(
            "[{}] KNOWN TYPE GAP {issue} still present — that column's TYPE is deferred, its \
             values are still compared",
            case.id()
        );
    }
    check
}

/// Read the exported Parquet file back: its record batches, after asserting its
/// column SET is exactly the declared one.
///
/// The TYPE check and the row projection are SEPARATE stages on purpose — see
/// [`run_stages`]: a wrong Arrow type on one column must not cancel the other
/// columns' value comparison.
fn read_parquet(case: &ParityCase, path: &Path) -> Result<Vec<RecordBatch>, Failures> {
    let bytes = std::fs::read(path)
        .map_err(|e| Failures::refusal(format!("read {}: {e}", path.display())))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .map_err(|e| {
            Failures::refusal(format!(
                "{} is not a readable Parquet file: {e}",
                path.display()
            ))
        })?
        .build()
        .map_err(|e| Failures::refusal(format!("building the Parquet reader failed: {e}")))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Failures::refusal(format!("decoding the Parquet file failed: {e}")))?;
    let Some(first) = batches.first() else {
        return Err(Failures::refusal(
            "the export produced no record batch at all",
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
        return Err(Failures::refusal(format!(
            "the Parquet schema's columns {actual:?} do not match the case's declared \
             columns {declared:?} — reconcile the case with the fixture's CQL schema"
        )));
    }

    Ok(batches)
}

/// Project already-read record batches into canonical rows.
fn project_rows(case: &ParityCase, batches: &[RecordBatch]) -> Result<Vec<Row>, Failures> {
    let key_columns: Vec<&str> = case
        .partition_key
        .iter()
        .chain(case.clustering.iter())
        .copied()
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for r in 0..batch.num_rows() {
            let mut cells = BTreeMap::new();
            for (ci, field) in schema.fields().iter().enumerate() {
                let ctx = format!("{}.{}[row {r}]", case.id(), field.name());
                let value = arrow_rows::canonical_from_arrow(batch.column(ci).as_ref(), r, &ctx)
                    .map_err(Failures::refusal)?;
                cells.insert(field.name().clone(), value);
            }
            let keys = key_columns
                .iter()
                .map(|k| {
                    cells.get(*k).cloned().ok_or_else(|| {
                        Failures::refusal(format!("key column '{k}' missing from the export"))
                    })
                })
                .collect::<Result<Vec<_>, Failures>>()?;
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

/// The INDEPENDENTLY determined outcome of every stage of one case.
///
/// # Why an aggregate, and not a `?`-chain
///
/// Round-3 roborev finding: the pipeline stopped at the first failing stage, so
/// the "exact failure set" a [`KnownGap`] is compared against was not the exact
/// set of what went wrong — it was the set of what went wrong BEFORE the first
/// abort. Two concrete holes:
///
///   * an expected export abort ran before the golden was ever loaded, so a
///     MALFORMED or physical-dump-INELIGIBLE golden (#1742) was never noticed;
///   * one column's Arrow type mismatch cancelled the whole value comparison, so
///     a VALUE regression on any OTHER column rode along invisibly — while the
///     per-column [`KnownTypeGap`] doc promised the opposite.
///
/// So each stage's inputs are established as independently as they really are:
/// the golden stage depends on NOTHING the export does and always runs, and a
/// type mismatch blocks the value comparison for ITS column only.
///
/// A stage that genuinely cannot proceed is recorded as
/// [`Failure::Unrunnable`], never omitted: a stage that passed and a stage that
/// never ran must not be indistinguishable, and a gap that wants to defer one
/// has to say so by name.
///
/// Public, with [`stage_case`] and [`finish_case`], only so the aggregate's own
/// exclusivity can be tested on REAL export output: a self-test stages a case
/// whose type gap defers one column, PERTURBS a cell of an UNAFFECTED column,
/// and requires the aggregate to report it. Nothing in the harness mutates a
/// staged case.
pub struct Stages {
    columns: Vec<ColumnType>,
    /// `None` when the golden stage failed (its failure is in `failures`).
    golden: Option<Vec<GoldenRow>>,
    /// `None` when the export or the parquet read failed.
    parquet: Option<Vec<Row>>,
    /// The stage that blocked `golden`/`parquet`, for the `Unrunnable` record.
    blocked_by: Option<Stage>,
    /// Columns whose values cannot be compared because their TYPE diverged.
    type_blocked_columns: Vec<String>,
    /// A type-stage refusal blocks the value comparison whole.
    type_blocks_all_values: bool,
    /// Everything that went wrong, across ALL stages.
    failures: Vec<Failure>,
}

/// Everything one case needs to compare, before comparing it.
///
/// Exposed as its own step so the harness's OWN sensitivity can be tested: a
/// self-test perturbs one cell / drops one row / rewrites one key and requires
/// [`compare`] to REPORT it. A comparison harness that has never been shown to
/// fail is not evidence of anything (`harness_detects_*` in
/// `issue_1490_parquet_jsonl_parity.rs`).
pub struct Prepared {
    pub columns: Vec<ColumnType>,
    pub golden: Vec<GoldenRow>,
    pub parquet: Vec<Row>,
}

/// Run every stage UP TO the value comparison, aggregating their failures.
///
/// `Ok(None)` only when no candidate root carries the table. `Err` is reserved
/// for the SETUP refusals that make every later stage meaningless (an unparsable
/// column declaration, an unusable fixture directory, no temp dir): with no
/// declared types and no fixture there is nothing to aggregate.
fn run_stages(case: &ParityCase) -> Result<Option<Stages>, Failures> {
    let columns = case.column_types().map_err(Failures::refusal)?;
    let Some(fixture) = resolve_fixture(case).map_err(Failures::refusal)? else {
        return Ok(None);
    };
    let tmp = tempfile::TempDir::new().map_err(|e| Failures::refusal(format!("tempdir: {e}")))?;
    let data_dir = isolated_data_dir(case, &fixture, tmp.path()).map_err(Failures::refusal)?;

    let mut stages = Stages {
        columns,
        golden: None,
        parquet: None,
        blocked_by: None,
        type_blocked_columns: Vec::new(),
        type_blocks_all_values: false,
        failures: Vec::new(),
    };

    // Stage GOLDEN — FIRST and unconditionally, because it depends on nothing
    // the export does. Running it before the export is what makes it impossible
    // for a recorded export abort to suppress it.
    match load_golden(case, &fixture, &stages.columns) {
        Ok(golden) => stages.golden = Some(golden),
        Err(f) => {
            stages.failures.extend(f.into_items());
            stages.blocked_by = Some(Stage::Golden);
        }
    }

    // Stage EXPORT → Stage PARQUET-READ → Stage ARROW-TYPES, each recording an
    // `Unrunnable` for what it prevented.
    match export_parquet(case, &data_dir, tmp.path()) {
        Err(f) => {
            stages.failures.extend(f.into_items());
            stages.unrunnable(Stage::ParquetRead, Stage::Export);
            stages.unrunnable(Stage::ArrowTypes, Stage::Export);
            stages.blocked_by = Some(Stage::Export);
        }
        Ok(parquet_path) => match read_parquet(case, &parquet_path) {
            Err(f) => {
                stages.failures.extend(f.into_items());
                stages.unrunnable(Stage::ArrowTypes, Stage::ParquetRead);
                stages.blocked_by = Some(Stage::ParquetRead);
            }
            Ok(batches) => {
                // The batches are read, so the TYPE stage and the ROW
                // PROJECTION are both runnable and both run — neither cancels
                // the other.
                let schema = batches
                    .first()
                    .map(|b| b.schema())
                    .expect("read_parquet refuses an empty batch list");
                let check = validate_arrow_types(case, &stages.columns, &schema);
                stages.failures.extend(check.failures);
                stages.type_blocked_columns = check.blocked_columns;
                stages.type_blocks_all_values = check.blocks_all_values;

                match project_rows(case, &batches) {
                    Ok(rows) => stages.parquet = Some(rows),
                    Err(f) => {
                        stages.failures.extend(f.into_items());
                        stages.blocked_by = Some(Stage::ParquetRead);
                    }
                }
            }
        },
    }
    Ok(Some(stages))
}

impl Stages {
    /// TEST-SUPPORT ONLY: overwrite one cell of the exported side, so a
    /// self-test can prove the AGGREGATE notices a value regression in a column
    /// that a recorded type gap does not cover. It cannot weaken a real run:
    /// nothing in the harness calls it, and the rows are rebuilt from the
    /// Parquet file on every case.
    pub fn overwrite_parquet_cell(&mut self, row: usize, column: &str, value: CanonicalValue) {
        let rows = self
            .parquet
            .as_mut()
            .expect("overwrite_parquet_cell needs a case whose export was read back");
        rows[row].overwrite_cell(column, value);
    }

    /// Record that `stage` could not run because `blocked_by` failed.
    fn unrunnable(&mut self, stage: Stage, blocked_by: Stage) {
        self.failures.push(Failure::Unrunnable {
            stage,
            column: None,
            blocked_by,
        });
    }

    /// The columns the value comparison can still cover, and the `Unrunnable`
    /// records for the ones it cannot.
    fn comparable_columns(&mut self) -> Vec<ColumnType> {
        if self.type_blocks_all_values {
            self.unrunnable(Stage::ValueComparison, Stage::ArrowTypes);
            return Vec::new();
        }
        let blocked = std::mem::take(&mut self.type_blocked_columns);
        for column in &blocked {
            self.failures.push(Failure::Unrunnable {
                stage: Stage::ValueComparison,
                column: Some(column.clone()),
                blocked_by: Stage::ArrowTypes,
            });
        }
        self.columns
            .iter()
            .filter(|c| !blocked.contains(&c.name))
            .cloned()
            .collect()
    }
}

/// Stage GOLDEN: load the committed sstabledump dump and project it, including
/// its physical-dump ELIGIBILITY refusals (#1742).
fn load_golden(
    case: &ParityCase,
    fixture: &Fixture,
    columns: &[ColumnType],
) -> Result<Vec<GoldenRow>, Failures> {
    let golden_doc =
        canonical_jsonl::load_golden_document_with_keys(&fixture.golden, true, &case.key_spec())
            .map_err(|e| {
                Failures::refusal(format!("loading the sstabledump golden failed: {e}"))
            })?;
    golden_rows::project_golden(&golden_doc, columns, case.partition_key, case.clustering)
        .map_err(Failures::refusal)
}

/// Export, read back and project both sides. `Ok(None)` only when no candidate
/// root carries the table.
///
/// Reports the AGGREGATE of every stage before the value comparison, so an
/// export abort never suppresses the golden's own validation.
pub fn prepare(case: &ParityCase) -> Result<Option<Prepared>, Failures> {
    let Some(stages) = run_stages(case).map_err(|f| f.for_case(&case.id()))? else {
        return Ok(None);
    };
    if !stages.failures.is_empty() {
        return Err(Failures::many(stages.failures).for_case(&case.id()));
    }
    let (Some(golden), Some(parquet)) = (stages.golden, stages.parquet) else {
        // Unreachable: a missing side always records a failure above.
        return Err(
            Failures::refusal("a stage produced neither rows nor a failure").for_case(&case.id()),
        );
    };
    Ok(Some(Prepared {
        columns: stages.columns,
        golden,
        parquet,
    }))
}

/// Run one case end-to-end. `Err` carries EVERY failure the case produced, from
/// every stage that could be determined independently; `Ok(Skipped)` only when
/// no candidate root carries the table.
pub fn run_case(case: &ParityCase) -> Result<CaseOutcome, Failures> {
    let Some(stages) = stage_case(case)? else {
        return Ok(CaseOutcome::Skipped(datasets_root::describe_search(
            case.keyspace,
            case.table,
        )));
    };
    finish_case(case, stages)
}

/// Every stage BEFORE the value comparison, as an aggregate. `Ok(None)` only
/// when no candidate root carries the table.
///
/// Public for the aggregate's self-tests (see [`Stages`]).
pub fn stage_case(case: &ParityCase) -> Result<Option<Stages>, Failures> {
    run_stages(case).map_err(|f| f.for_case(&case.id()))
}

/// Stage VALUE-COMPARISON, then the aggregate verdict over EVERY stage.
///
/// Public for the aggregate's self-tests (see [`Stages`]).
pub fn finish_case(case: &ParityCase, mut stages: Stages) -> Result<CaseOutcome, Failures> {
    // Stage VALUE-COMPARISON — over the columns whose type did not diverge, and
    // only if both sides exist. Anything it cannot cover is recorded, by name.
    let comparable = stages.comparable_columns();
    let mut outcome = None;
    match (stages.golden.take(), stages.parquet.take()) {
        (Some(golden), Some(parquet)) if !comparable.is_empty() => {
            match compare(case, &comparable, golden, parquet) {
                Ok(o) => outcome = Some(o),
                Err(f) => stages.failures.extend(f.into_items()),
            }
        }
        (Some(_), Some(_)) => {
            // Every column's type diverged: already recorded per column.
        }
        _ => {
            let blocked_by = stages.blocked_by.unwrap_or(Stage::Golden);
            stages.unrunnable(Stage::ValueComparison, blocked_by);
        }
    }

    if !stages.failures.is_empty() {
        return Err(Failures::many(stages.failures).for_case(&case.id()));
    }
    outcome.ok_or_else(|| {
        // Unreachable: a comparison that did not happen always records why.
        Failures::refusal("the value comparison neither ran nor recorded why").for_case(&case.id())
    })
}

/// Sort both sides by primary key and assert full per-cell equality.
pub fn compare(
    case: &ParityCase,
    columns: &[ColumnType],
    golden: Vec<GoldenRow>,
    parquet: Vec<Row>,
) -> Result<CaseOutcome, Failures> {
    compare_inner(case, columns, golden, parquet).map_err(|f| f.for_case(&case.id()))
}

fn compare_inner(
    case: &ParityCase,
    columns: &[ColumnType],
    golden: Vec<GoldenRow>,
    parquet: Vec<Row>,
) -> Result<CaseOutcome, Failures> {
    let mut expected: Vec<Row> = golden
        .into_iter()
        .map(|g| Row {
            keys: g.keys,
            cells: g.cells,
        })
        .collect();
    let mut actual = parquet;

    if expected.is_empty() {
        return Err(Failures::refusal(
            "the sstabledump golden projected to ZERO rows — a dataset-dependent \
             comparison must never pass on an empty oracle",
        ));
    }
    expected.sort_by_key(|r| r.sort_key());
    actual.sort_by_key(|r| r.sort_key());

    if expected.len() != actual.len() {
        return Err(Failures::one(Failure::Value(format!(
            "row count differs — golden {} vs Parquet {}",
            expected.len(),
            actual.len()
        ))));
    }

    let mut diffs: Vec<Failure> = Vec::new();
    let mut compared_cells = 0usize;
    for (i, (e, a)) in expected.iter().zip(actual.iter()).enumerate() {
        if e.sort_key() != a.sort_key() {
            diffs.push(Failure::Value(format!(
                "row {i}: primary key differs — golden {} vs Parquet {}",
                e.sort_key(),
                a.sort_key()
            )));
            if diffs.len() >= 10 {
                break;
            }
            continue;
        }
        for col in columns {
            let ctx = format!("{} row {i} column '{}'", case.id(), col.name);
            // Spelling normalization applies to BOTH sides through the same
            // function, so it can only erase a difference in HOW a value is
            // written, never a difference in the VALUE (see spelling.rs).
            //
            // It says nothing about the value's TYPE, and neither does this
            // comparison: canonicalization folds every integer width into one
            // `Int` and both float widths into one `Float`, so a wrong CQL→Arrow
            // mapping compares equal here. Type fidelity is asserted separately,
            // per field, by `arrow_expect::validate_field` in
            // `project_parquet` — before any value reaches this loop.
            let ev = spelling::normalize_spelling(
                e.cells
                    .get(&col.name)
                    .cloned()
                    .unwrap_or(CanonicalValue::Absent),
                &col.spec,
                &ctx,
            )
            .map_err(Failures::refusal)?;
            let av = spelling::normalize_spelling(
                a.cells
                    .get(&col.name)
                    .cloned()
                    .unwrap_or(CanonicalValue::Absent),
                &col.spec,
                &ctx,
            )
            .map_err(Failures::refusal)?;
            compared_cells += 1;
            if ev != av {
                diffs.push(Failure::Value(format!(
                    "row {i} (pk {}) column '{}' ({}): golden {} vs Parquet {}",
                    e.sort_key(),
                    col.name,
                    col.declared,
                    render_value(&ev),
                    render_value(&av)
                )));
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
        return Err(Failures::many(diffs));
    }

    Ok(CaseOutcome::Ran {
        rows: expected.len(),
        cells: compared_cells,
    })
}

/// Drive one case and apply the per-case fail-closed rule.
pub fn assert_case(case: &ParityCase) {
    let outcome = run_case(case);
    if let Some(gap) = &case.known_gap {
        match outcome {
            Err(failures) => {
                // SET EQUALITY on structured failure data, never substring
                // containment of a rendered message: containment proves the
                // recorded failure is PRESENT but says nothing about whether
                // anything ELSE is, so an unrecorded regression aggregated into
                // the same message rode along excused (see `failure.rs`).
                if let Some(problem) = gap.mismatch(&case.id(), failures.items()) {
                    panic!(
                        "{}: this case records a KNOWN export gap ({} — {}) whose recorded \
                         failure set is {:?}, but {problem}\n\nOBSERVED: {failures}",
                        case.id(),
                        gap.issue,
                        gap.what,
                        gap.expect,
                    );
                }
                eprintln!(
                    "[{}] KNOWN GAP {} still present ({}) — parity comparison deferred",
                    case.id(),
                    gap.issue,
                    gap.what
                );
            }
            Ok(CaseOutcome::Ran { rows, cells }) => panic!(
                "{}: the KNOWN export gap {} ({}) NO LONGER reproduces — {rows} rows / \
                 {cells} cells now compare equal. Delete the `known_gap` from this case so \
                 the table is covered for real, and close {}.",
                case.id(),
                gap.issue,
                gap.what,
                gap.issue
            ),
            Ok(CaseOutcome::Skipped(reason)) => {
                if case.must_run || require_fixtures() {
                    panic!(
                        "{}: this case MUST run but no fixture was found: {reason}",
                        case.id()
                    );
                }
                eprintln!("[{}] SKIPPED — {reason}", case.id());
            }
        }
        return;
    }
    match outcome {
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
