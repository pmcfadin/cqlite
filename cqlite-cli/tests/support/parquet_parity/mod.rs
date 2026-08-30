//! Parquet ↔ sstabledump-JSONL value-parity harness (issue #1490, epic #1469).
//!
//! # What this harness asserts
//!
//! For each declared corpus table, five STAGES (see [`Stages`]), each
//! determined as INDEPENDENTLY as it really is and aggregated — never a
//! `?`-chain that stops at the first failure:
//!
//!   1. project the table's committed `*-Data.db.jsonl` sstabledump golden into
//!      the canonical value space, including its physical-dump ELIGIBILITY
//!      (#1742). Depends on NOTHING the export does, so it runs FIRST and
//!      unconditionally;
//!   2. export the table to Parquet through the WIRED writer — the real `cqlite
//!      export --format parquet` binary, not a library shortcut;
//!   3. read the Parquet back with the `arrow`/`parquet` crates and check its
//!      column SET;
//!   4. validate every field's Arrow TYPE against the case's independently
//!      declared CQL types (`arrow_expect`): canonicalization is width-blind, so
//!      a wrong CQL→Arrow mapping would otherwise round-trip its values
//!      unchanged and pass. A mismatch defers the VALUES of ITS column only;
//!   5. sort both sides by primary key (Parquet row order is not guaranteed) and
//!      assert FULL PER-CELL equality over every column stage 4 did not defer.
//!
//! A stage that genuinely cannot run is recorded as [`Failure::Unrunnable`],
//! never omitted — a stage that passed and a stage that never ran must not be
//! indistinguishable, or an earlier failure silently shrinks the "exact failure
//! set" a [`KnownGap`] is compared against.
//!
//! # The comparison outcome is THREE-valued
//!
//! `equal` / `unequal` / **unsupported-representation** (issue #1490 round 4,
//! `unsupported.rs`). A positive verdict requires an AFFIRMATIVE MEASUREMENT, so
//! a stage that could not measure something must be distinguishable from one
//! that passed: a harness that compares a CQL tuple it cannot represent, or
//! accepts any Arrow `Struct` for a UDT without checking the field widths, is
//! emitting a pass it never measured — the vacuous-pass shape, inside the thing
//! whose whole job is to detect wrongness.
//!
//! A refusal is recorded as [`Failure::UnsupportedRepresentation`], names the
//! column and the representation, removes that column from the compared cells
//! (so the census number shrinks, truthfully), and FAILS the case: every
//! declared column is declared COVERAGE. It is NOT a [`KnownGap`] — a gap means
//! "a recorded product defect still reproduces", a refusal means "this harness
//! cannot represent this shape" — and it cannot be recorded into one.
//!
//! The pre-existing Parquet tests check row counts, `PAR1` magic, a few spot
//! values and DuckDB aggregates; `parquet_golden_tests.rs` freezes a byte
//! snapshot of CQLite's OWN output, which cannot detect a wrong value because it
//! was produced by the same code. This harness's oracle is CASSANDRA-WRITTEN
//! (#3042), so it can.
//!
//! # Fail-closed rules
//!
//! * The case's own DECLARATION (columns, types, key definitions) is VALIDATED
//!   against the committed `test-data/schemas/*.cql` before any stage runs
//!   (`schema_fixture`). The declaration is the ground truth stages 4 and 5 are
//!   derived from, so an unverified one that drifted to match a wrong export
//!   mapping would make both of them pass. Only the harness's own
//!   deliberate-misdeclaration controls opt out, by naming a reason
//!   ([`SchemaCheck::Synthetic`]), which is announced on every run.
//! * Fixture roots are resolved PER TABLE, and FALLIBLY
//!   (`fixture_root::first_candidate_root_with_table`), never by
//!   keyspace: a root holding the keyspace but not the table would otherwise win
//!   the selection and the case would skip while the fixture sat in the checkout
//!   (#3220).
//! * The golden is DERIVED from the selected `*-Data.db`, never resolved beside
//!   it: a `nb-1-big-Data.db.jsonl` left next to a regenerated
//!   `nb-2-big-Data.db` would compare one generation's data against another
//!   generation's dump — a false failure or a false PASS, reported as neither.
//!   A non-corresponding pair is a named refusal ([`fixture_in_table_dir`]).
//! * A case whose SSTable binaries are COMMITTED to git is `must_run`: an
//!   absence is a hard failure, unconditionally. Authority for the flag is
//!   `git ls-files 'test-data/datasets/sstables/**-Data.db'`, never presence on
//!   disk (the fetched corpus unpacks into the same tree).
//! * There is deliberately NO suite-wide `assert!(ran > 0)`: it cannot see one
//!   case skipping behind its siblings. Each case asserts for itself.
//! * `CQLITE_REQUIRE_FIXTURES=1` promotes EVERY case to `must_run`.
//! * A recorded divergence is EXCLUSIVE across the whole AGGREGATE: an expected
//!   export abort cannot suppress the golden's validation, and a deferred column
//!   TYPE cannot suppress any other column's VALUES.
//! * A representation the harness cannot compare is REFUSED by name, never
//!   compared anyway and never counted as equal (`unsupported.rs`).
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
pub mod cases;
pub mod cql_type;
pub mod decimal;
pub mod declared;
pub mod failure;
pub mod fixture_root;
pub mod golden_rows;
pub mod golden_schema;
pub mod golden_text;
pub mod schema_fixture;
pub mod spelling;
pub mod unsupported;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use canonical_jsonl::{CanonicalValue, KeySpec};
use cql_type::ColumnType;
use failure::{Failure, Failures, Stage};
// Fixture DISCOVERY and STAGING live in `fixture_root` (round 19); the whole
// fail-closed resolution path — candidate search, generation census,
// golden/generation correspondence, isolated staging — is one file there, and
// test targets reach it as `parquet_parity::fixture_root::…`.
use fixture_root::{isolated_data_dir, resolve_fixture, Fixture};
use golden_rows::GoldenRow;

// `ExpectedFailure` is used only by the test binaries that DECLARE a
// `known_gap`, and each test binary includes this module by `#[path]` — so in a
// binary that declares none the re-export is genuinely unused. Allowed rather
// than dropped: the type belongs to the case-declaration surface beside
// `KnownGap`, and making callers reach into `failure::` for one half of the same
// record would be worse.
#[allow(unused_imports)]
pub use failure::{ExpectedFailure, KnownGap, KnownTypeGap};
pub use schema_fixture::SchemaCheck;

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
    ///
    /// A hand-copied declaration is the harness's GROUND TRUTH, so it is
    /// VERIFIED against that schema on every run — see [`schema_check`] and
    /// `schema_fixture`.
    ///
    /// [`schema_check`]: ParityCase::schema_check
    pub columns: &'static [(&'static str, &'static str)],
    /// Partition-key columns in declared order.
    pub partition_key: &'static [&'static str],
    /// Clustering columns in declared order.
    pub clustering: &'static [&'static str],
    /// Whether the three declarations above (columns, types, key definitions)
    /// are validated against the committed `test-data/schemas/<schema>` — the
    /// default for every REAL case, and opted out of, visibly and with a stated
    /// reason, only by the harness's own deliberate-misdeclaration controls.
    pub schema_check: SchemaCheck,
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

    pub fn key_spec(&self) -> KeySpec {
        KeySpec::from_cql_types(
            &self.cql_types_of(self.partition_key),
            &self.cql_types_of(self.clustering),
        )
    }

    /// The declared CQL types of `names`, in order.
    ///
    /// A name not present in `columns` PANICS rather than yielding `""`. The
    /// `""` this replaces went to `KeyKind::from_cql_type`, which classifies an
    /// unrecognised type name as `Other` — so a mistyped partition/clustering
    /// name silently disabled numeric-key unification for that component instead
    /// of naming the case-definition error. "I could not find the declaration"
    /// and "the declaration is not integral" are DIFFERENT states, and this is
    /// the harness's own case table, so the first one is always a bug here.
    pub fn cql_types_of(&self, names: &[&str]) -> Vec<&'static str> {
        names
            .iter()
            .map(|n| {
                self.columns
                    .iter()
                    .find(|(cn, _)| cn == n)
                    .map(|(_, t)| *t)
                    .unwrap_or_else(|| {
                        panic!(
                            "{}.{}: key component '{n}' is not among the declared columns {:?} \
                             — a key name with no declaration cannot be given a KeySpec kind, \
                             and defaulting it to a non-integral one would silently change how \
                             the golden's key components are canonicalized",
                            self.keyspace,
                            self.table,
                            self.columns.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
                        )
                    })
            })
            .collect()
    }
}

/// Is `CQLITE_REQUIRE_FIXTURES` set to a value that makes an absent fixture
/// fatal?
///
/// STRICT on purpose. The `.unwrap_or(false)` this replaces collapsed two
/// genuinely UNKNOWN states onto "not required" — the permissive answer, which
/// turns the whole fail-closed rule off:
///
///   * `VarError::NotUnicode` — the operator DID set the variable and the
///     harness cannot read what to. Unset is an affirmative "not required";
///     "set to something I cannot decode" is not.
///   * an unrecognised value — `CQLITE_REQUIRE_FIXTURES=yes` (or `=Y`, or a
///     value with a stray trailing space from a CI expression) read as "not
///     required", so an operator who asked for the strict mode silently got the
///     lenient one and every absent fixture SKIPPED under a green run.
///
/// Unset and empty are affirmatively NOT required (an empty value is how a shell
/// spells "unset"). The truthy/falsey sets are explicit; anything else PANICS
/// naming the value, because a knob whose meaning the harness cannot determine
/// must not silently pick the weaker mode.
fn require_fixtures() -> bool {
    interpret_require_fixtures(std::env::var("CQLITE_REQUIRE_FIXTURES"))
}

/// [`require_fixtures`] as a PURE function of the env read.
///
/// Separated so the strictness is provable without mutating
/// `CQLITE_REQUIRE_FIXTURES`, which is process-global while libtest runs tests
/// on many threads — a test that set it would change the fail-closed mode of
/// every case running concurrently.
pub fn interpret_require_fixtures(read: Result<String, std::env::VarError>) -> bool {
    match read {
        Ok(raw) => {
            let v = raw.trim();
            if v.is_empty()
                || v == "0"
                || v.eq_ignore_ascii_case("false")
                || v.eq_ignore_ascii_case("no")
            {
                false
            } else if v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes") {
                true
            } else {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES={raw:?} is not a value this harness recognises \
                     (1/true/yes, 0/false/no, or unset). It is REFUSED rather than read as \
                     \"not required\": an unparsed value used to turn the fail-closed \
                     missing-fixture rule OFF, which is the vacuous-pass mode this knob exists \
                     to prevent."
                )
            }
        }
        Err(std::env::VarError::NotPresent) => false,
        Err(std::env::VarError::NotUnicode(raw)) => panic!(
            "CQLITE_REQUIRE_FIXTURES is SET to a value that is not UTF-8 ({raw:?}); the harness \
             REFUSES it rather than read an undecodable value as \"not required\", which would \
             silently disable the fail-closed missing-fixture rule the operator asked for"
        ),
    }
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
    // `is_file()` is two-valued here TOO, but its permissive answer points the
    // safe way: a path it could not stat answers `false` and REFUSES. Left as
    // `is_file()` deliberately — the fallible form would report the stat error
    // instead, which is no more actionable than "produced no file".
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
    /// Columns this row deliberately did NOT decode, because the Arrow TYPE
    /// stage had already blocked them (issue #1490 round 7, finding 1). Recorded
    /// rather than left implicit: a skipped column that somehow reached the
    /// value comparison must be a loud refusal, never an `Absent` that could
    /// compare EQUAL to a golden absence.
    undecoded: BTreeSet<String>,
}

impl Row {
    /// TEST-SUPPORT ONLY: overwrite one cell, so a self-test can prove the
    /// comparison notices. It cannot weaken a real run: nothing in the harness
    /// calls it, and a `Row` is rebuilt from the Parquet file on every case.
    pub fn overwrite_cell(&mut self, column: &str, value: CanonicalValue) {
        // Placing a value makes the column decoded BY DEFINITION, so the
        // `undecoded` record has to go with it — otherwise a self-test that
        // perturbs a blocked column would hit the bookkeeping refusal instead of
        // the value difference it means to demonstrate.
        self.undecoded.remove(column);
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

    /// Was this column deliberately NOT decoded (its Arrow type was already
    /// blocked by the TYPE stage)? Reported so the isolation guarantee is
    /// assertable rather than merely claimed.
    pub fn is_undecoded(&self, column: &str) -> bool {
        self.undecoded.contains(column)
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
    /// Columns whose Arrow type the stage could not MEASURE — today a UDT as a
    /// `Struct`. Blocked before the projection too: see `project_rows`, round 12.
    refused_columns: Vec<(String, unsupported::Unsupported)>,
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

    /// Same as `blocked_column`, for an UNMEASURABLE column's refusal.
    fn refused_column(&mut self, column: String, refused: unsupported::Unsupported) {
        self.failures.push(Failure::UnsupportedRepresentation {
            stage: Stage::ArrowTypes,
            column: column.clone(),
            refused,
        });
        self.refused_columns.push((column, refused));
    }

    /// Every column the projection must NOT decode: type DIVERGED or UNMEASURABLE.
    fn value_blocked_names(&self) -> Vec<String> {
        let mut names = self.blocked_columns.clone();
        names.extend(self.refused_columns.iter().map(|(c, _)| c.clone()));
        names
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
            arrow_expect::FieldVerdict::Valid => {
                if let Some(gap) = gap {
                    check.failures.push(Failure::Refusal(format!(
                        "column '{}' records the KNOWN type gap {} ({}) but its exported Arrow \
                         type is now CORRECT — delete the KnownTypeGap so the column is covered \
                         for real, and close {}",
                        gap.column, gap.issue, gap.what, gap.issue
                    )));
                }
            }
            // The THIRD outcome (issue #1490 round 4): the harness cannot verify
            // this column's Arrow type, so it says so instead of passing it.
            //
            // A recorded `KnownTypeGap` on such a column is deliberately NEITHER
            // excused NOR retired here: a gap records the type the export
            // currently produces, and nothing measured that. The refusal fails
            // the case on its own, which is the fail-closed answer.
            //
            // It DOES block the column's VALUES (round 12). The refusal is as
            // wide as what it refuses; what was measured wrong is how wide that
            // is — the missing declaration is a UDT's FIELD types, which the
            // value decode needs too, so its refusal used to abort the ROW
            // PROJECTION and cancel every sibling (`project_rows`). Blocking is
            // also truthful on its own terms: canonicalization is width-blind,
            // so values "compared" over unverified field types are a pass the
            // harness never measured.
            arrow_expect::FieldVerdict::Unmeasurable(refused) => {
                check.refused_column(col.name.clone(), refused);
            }
            arrow_expect::FieldVerdict::Mismatch(mismatch) => match gap {
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
            arrow_expect::FieldVerdict::Refusal(refusal) => {
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

/// Which columns the value comparison cannot cover, as the row projection needs
/// to know it: a column whose Arrow TYPE diverged — or whose Arrow type could
/// not be MEASURED at all — is not going to be compared, so decoding it can only
/// manufacture a failure.
struct ValueBlocks<'a> {
    /// Columns whose exported Arrow type DIVERGED or was UNMEASURABLE
    /// (`TypeCheck::value_blocked_names`).
    columns: &'a [String],
    /// The type stage did not answer at all, so NO column's values are compared.
    all: bool,
}

/// Project already-read record batches into canonical rows.
///
/// # Per-column isolation, and where it used to leak (issue #1490 round 7)
///
/// A column whose Arrow type diverged is removed from the compared set
/// ([`Stages::comparable_columns`]) — that is the harness's promise that a
/// deferred column's SIBLINGS still compare. This function used to decode EVERY
/// column anyway, and `arrow_rows` deliberately has no decoder for a type it
/// never declared valid (`UInt32`, `LargeList`, …), so a divergence INTO such a
/// type failed the projection WHOLESALE and took every unaffected column's value
/// comparison down with it — defeating exactly the isolation the type stage went
/// to the trouble of computing.
///
/// So a blocked NON-KEY column is not decoded at all; it is recorded in the
/// row's `undecoded` set and its per-column deferral is reported (once, by
/// `comparable_columns`).
///
/// # The second route into the same leak (issue #1490 round 12)
///
/// A column can also be undecodable because the DECLARATION does not reach into
/// it: a UDT is declared by NAME only, so a UDT field carries
/// `DeclaredType::Unavailable` and an ambiguous representation inside the Struct
/// (a scale-zero `Decimal128`) is refused. Its TYPE claim is already
/// `unsupported-representation` for that same missing declaration, so the type
/// stage blocks it here too (`TypeCheck::value_blocked_names`) rather than let
/// its refusal abort the projection and cancel its siblings.
///
/// A blocked KEY column IS still decoded, because the
/// primary key is what ALIGNS the two sides' rows: without it no column can be
/// compared, and that — and only that — blocks the comparison whole, with a
/// message saying so.
fn project_rows(
    case: &ParityCase,
    batches: &[RecordBatch],
    columns: &[ColumnType],
    blocks: &ValueBlocks<'_>,
) -> Result<Vec<Row>, Failures> {
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
            let mut undecoded = BTreeSet::new();
            for (ci, field) in schema.fields().iter().enumerate() {
                let name = field.name();
                let is_key = key_columns.contains(&name.as_str());
                if !is_key && (blocks.all || blocks.columns.iter().any(|c| c == name)) {
                    undecoded.insert(name.clone());
                    continue;
                }
                // Every Arrow cell is canonicalized through the ONE
                // declared-type-guided entry point, carrying the column's
                // declared CQL type (`declared.rs`).
                let Some(col) = columns.iter().find(|c| c.name == *name) else {
                    // Unreachable: `read_parquet` asserted column-set equality.
                    return Err(Failures::refusal(format!(
                        "Parquet column '{name}' has no declared CQL type, so it cannot be \
                         canonicalized from its declared type"
                    )));
                };
                let ctx = format!("{}.{name}[row {r}]", case.id());
                let value = declared::canonicalize_arrow(
                    batch.column(ci).as_ref(),
                    r,
                    &declared::Declared::cell(&col.spec, ctx),
                )
                .map_err(|e| {
                    if is_key {
                        Failures::refusal(format!(
                            "{e}\n  — this is a PRIMARY-KEY column, so the two sides' rows \
                             cannot be aligned and NO column's values can be compared; every \
                             other column's deferral is recorded separately"
                        ))
                    } else {
                        Failures::refusal(e)
                    }
                })?;
                cells.insert(name.clone(), value);
            }
            let keys = key_columns
                .iter()
                .map(|k| {
                    cells.get(*k).cloned().ok_or_else(|| {
                        Failures::refusal(format!("key column '{k}' missing from the export"))
                    })
                })
                .collect::<Result<Vec<_>, Failures>>()?;
            rows.push(Row {
                keys,
                cells,
                undecoded,
            });
        }
    }
    Ok(rows)
}

/// TEST-SUPPORT ONLY: drive the row projection over SYNTHETIC record batches
/// with an explicit blocked-column set.
///
/// The per-column isolation guarantee (finding 1, issue #1490 round 7) is about
/// what happens when a blocked column's exported Arrow type is one the decoder
/// does not handle (`UInt32`, `LargeList`, …). A real export cannot be made to
/// produce such a type — that is the point of the type check — so the only way
/// to demonstrate the property is to hand the projection a batch that carries
/// one. It cannot weaken a real run: `run_stages` builds its own `ValueBlocks`
/// from the TYPE stage's answer, and nothing in the harness calls this.
pub fn project_rows_for_test(
    case: &ParityCase,
    batches: &[RecordBatch],
    columns: &[ColumnType],
    blocked_columns: &[String],
    blocks_all_values: bool,
) -> Result<Vec<Row>, Failures> {
    project_rows(
        case,
        batches,
        columns,
        &ValueBlocks {
            columns: blocked_columns,
            all: blocks_all_values,
        },
    )
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
    /// Columns whose values the TYPE stage's own refusal blocks (round 12) —
    /// reported as `UnsupportedRepresentation`, never as compared-and-equal.
    type_refused_columns: Vec<(String, unsupported::Unsupported)>,
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
    // Stage ZERO — the case's own DECLARATION against the committed CQL schema.
    // It runs before everything because the declaration is what every later
    // stage's expectation is derived from: an unverified declaration that drifted
    // to match a wrong export mapping makes the Arrow TYPE check and the VALUE
    // comparison BOTH pass (issue #1490 round 6, `schema_fixture`).
    match case.schema_check {
        SchemaCheck::Committed => {
            schema_fixture::validate_declaration(case).map_err(Failures::refusal)?;
        }
        // Announced on EVERY run, never a silent exemption: an opt-out that
        // nobody can see is indistinguishable from a check that stopped working.
        SchemaCheck::Synthetic { why } => eprintln!(
            "[{}] SCHEMA DECLARATION CHECK OPTED OUT — synthetic control: {why}",
            case.id()
        ),
    }
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
        type_refused_columns: Vec::new(),
        type_blocks_all_values: false,
        failures: Vec::new(),
    };

    // Stage GOLDEN — FIRST and unconditionally, because it depends on nothing
    // the export does. Running it before the export is what makes it impossible
    // for a recorded export abort to suppress it.
    match golden_rows::load_golden(case, &fixture, &stages.columns) {
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
                let (check, rows) = types_and_projection(case, &batches, &stages.columns);
                stages.failures.extend(check.failures);
                stages.type_blocked_columns = check.blocked_columns;
                stages.type_refused_columns = check.refused_columns;
                stages.type_blocks_all_values = check.blocks_all_values;
                match rows {
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

/// Stage ARROW-TYPES and the ROW PROJECTION, wired the ONE way: what the type
/// stage found divergent or could not measure is what the projection is told not
/// to decode (`TypeCheck::value_blocked_names`). One function because the
/// COUPLING is the property: the leak rounds 7 and 12 both found was the
/// projection decoding a column the type stage had already given up on.
fn types_and_projection(
    case: &ParityCase,
    batches: &[RecordBatch],
    columns: &[ColumnType],
) -> (TypeCheck, Result<Vec<Row>, Failures>) {
    // The batch list is non-empty by construction on both paths: `read_parquet`
    // refuses an empty one, and the test wrapper documents the same requirement.
    let schema = batches
        .first()
        .map(|b| b.schema())
        .expect("read_parquet refuses an empty batch list");
    let check = validate_arrow_types(case, columns, &schema);
    let blocked = check.value_blocked_names();
    let rows = project_rows(
        case,
        batches,
        columns,
        &ValueBlocks {
            columns: &blocked,
            all: check.blocks_all_values,
        },
    );
    (check, rows)
}

/// TEST-SUPPORT ONLY: the TYPE stage and the ROW PROJECTION over SYNTHETIC
/// batches, through the SAME [`types_and_projection`] a real run uses — so the
/// round-12 control asserts the WIRING (the type stage's own refusal is what
/// blocks the projection), which a hand-assembled blocked list could not. No real
/// export can supply the input: a UDT reaching the type stage as an Arrow
/// `Struct` is what #3556 prevents. Nothing in the harness calls it; like a real
/// run it requires a NON-EMPTY batch list.
pub fn types_and_projection_for_test(
    case: &ParityCase,
    batches: &[RecordBatch],
    columns: &[ColumnType],
) -> (Vec<Failure>, Result<Vec<Row>, Failures>) {
    let (check, rows) = types_and_projection(case, batches, columns);
    (check.failures, rows)
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

    /// The columns the value comparison can still cover, plus the record for
    /// every column it cannot cover and WHY — `Unrunnable` when another stage
    /// blocked it, `UnsupportedRepresentation` when the harness itself refuses
    /// the declared representation.
    ///
    /// The two are kept apart because they mean different things: an
    /// `Unrunnable` column is comparable in principle and was prevented, a
    /// REFUSED column is one the harness cannot compare at all (issue #1490
    /// round 4). Both remove the column from the compared set, so its cells are
    /// NOT counted in the census — the smaller number is the true one.
    ///
    /// A column is refused for its VALUES either because its DECLARED type is a
    /// representation the harness cannot compare (`unsupported.rs`) or because
    /// the TYPE stage could not measure it and the missing declaration is the one
    /// the value decode needs (a UDT's field types — round 12). Either way it is
    /// REFUSED by name, never silently "compared and equal".
    fn comparable_columns(&mut self) -> Vec<ColumnType> {
        // Refusals FIRST, and unconditionally: a refusal is a property of the
        // DECLARED type, so it holds whether or not the export produced
        // anything. Recording it even when a later stage was also blocked is
        // deliberate — the case declares a column the harness cannot compare,
        // and that stays true.
        let mut refused: Vec<(String, unsupported::Unsupported)> = self
            .columns
            .iter()
            .filter_map(|c| {
                unsupported::refused_value_representation(&c.spec).map(|u| (c.name.clone(), u))
            })
            .collect();
        // The TYPE stage's OWN refusals join them (round 12): the declaration it
        // lacked — a UDT's FIELD types — is the one the value decode needs, so
        // such a column is refused for its VALUES too and the projection already
        // skipped it. Recorded at VALUE-COMPARISON as well as at ARROW-TYPES:
        // "the type claim is unmeasurable" and "the values were not compared
        // either" are two different facts.
        for (column, representation) in std::mem::take(&mut self.type_refused_columns) {
            if !refused.iter().any(|(c, _)| *c == column) {
                refused.push((column, representation));
            }
        }
        for (column, refused) in &refused {
            self.failures.push(Failure::UnsupportedRepresentation {
                stage: Stage::ValueComparison,
                column: column.clone(),
                refused: *refused,
            });
        }
        let is_refused = |name: &String| refused.iter().any(|(c, _)| c == name);

        if self.type_blocks_all_values {
            self.unrunnable(Stage::ValueComparison, Stage::ArrowTypes);
            return Vec::new();
        }
        let blocked = std::mem::take(&mut self.type_blocked_columns);
        for column in &blocked {
            // A refused column already carries the stronger record. Emitting
            // both would report one column's values as twice-uncovered, which
            // would break the multiset equality a KnownGap is compared against
            // for no gain.
            if is_refused(column) {
                continue;
            }
            self.failures.push(Failure::Unrunnable {
                stage: Stage::ValueComparison,
                column: Some(column.clone()),
                blocked_by: Stage::ArrowTypes,
            });
        }
        self.columns
            .iter()
            .filter(|c| !blocked.contains(&c.name) && !is_refused(&c.name))
            .cloned()
            .collect()
    }
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
            // The golden side decodes every declared column; a blocked column is
            // dropped from the COMPARED set, not from this projection.
            undecoded: BTreeSet::new(),
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
            // A column the projection deliberately did not decode must never be
            // compared: its cell would be `Absent`, which could compare EQUAL to
            // a golden absence and report coverage that never happened. If the
            // compared set and the blocked set ever disagree, that is a harness
            // bookkeeping defect and it says so.
            if a.undecoded.contains(&col.name) {
                return Err(Failures::refusal(format!(
                    "{ctx}: the value comparison was asked to cover a column the row \
                     projection did not decode (its Arrow type was blocked) — the compared \
                     set and the blocked set disagree"
                )));
            }
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
            // `unwrap_or(Absent)` on BOTH sides is a DOMAIN value, not a
            // collapsed error: a Cassandra row legitimately omits a cell, and
            // `Absent` is how this harness spells that. Nothing fallible is
            // being defaulted — the maps are already-parsed rows — and the
            // default is applied symmetrically, so it cannot erase a difference.
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
