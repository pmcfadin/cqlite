//! PRECONDITIONS are GAP-INDEPENDENT — issue #1490 (AD1), epic #1469, round 19.
//!
//! # The rule these tests exist to pin
//!
//! > An expected-failure marker must suppress ONLY the assertion it names, never
//! > a validity PRECONDITION of the comparison.
//!
//! A [`KnownGap`] says "this named comparison is expected to fail because of a
//! recorded product defect". It must never also mean "and therefore skip the
//! checks that establish the comparison was meaningful at all".
//!
//! # Why a whole suite and not one regression test
//!
//! Round 19's finding — `load_golden` accepting a nonempty JSONL document whose
//! partitions all carry empty `rows`, because the zero-row check sat later, in
//! `compare_inner`, behind an expected export abort — is the THIRD appearance of
//! ONE family in this harness:
//!
//!   * **round 12**: the pipeline `?`-chained, so the "exact failure set" a gap
//!     is compared against stopped at the first failing stage. Fixed with five
//!     aggregated stages — which closed two members of the family and left this
//!     one.
//!   * **round 13**: physical-dump eligibility was decided from what a LENIENT
//!     parser managed to parse, so a present-but-invalid field read as an
//!     absence. Fixed by deciding it from the golden TEXT.
//!   * **round 19**: the zero-row check sat after the gap short-circuit.
//!
//! So the fix was a separated CLASS (`failure::Precondition`), and the coverage
//! has to be a class too: EVERY precondition gets a case proving a `KnownGap`
//! does not suppress it, because "unconditional" is exactly the property that
//! decays silently — a later refactor that re-entangles ONE check would
//! otherwise be caught by nothing.
//!
//! # How each case is built, and why it needs no fetched corpus
//!
//! Every case drives the REAL pipeline (`stage_case_in_roots` + `finish_case`)
//! over a SCRATCH candidate root the test builds: a stub `*-Data.db` plus a
//! golden written for the case. The export genuinely runs and genuinely ABORTS
//! (the case names a committed schema that does not declare its table), and the
//! recorded [`KnownGap`] records EXACTLY that abort and every stage it blocks —
//! i.e. a gap that, before this fix, excused the whole case. Each case then
//! requires the precondition to surface anyway.
//!
//! The abort is what makes these tests faithful to the finding: it is the
//! short-circuit that the precondition must survive. A control at the bottom
//! shows the same gap DOES excuse the case when every precondition holds, so
//! these are not passing merely because a `KnownGap` never excuses anything.

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use std::path::{Path, PathBuf};

use parquet_parity::failure::{Failure, Precondition, Stage};
use parquet_parity::{ExpectedFailure, KnownGap, ParityCase, SchemaCheck};

// ---------------------------------------------------------------------------
// The scratch corpus, and the gap that would have excused everything
// ---------------------------------------------------------------------------

/// A stub `*-Data.db`. Its BYTES never matter: every case here names a committed
/// schema that does not declare its table, so `cqlite export` aborts before any
/// SSTable content is read — which is precisely the short-circuit the
/// preconditions have to survive.
const STUB_DATA: &[u8] = b"not-a-real-sstable";

/// One valid, ELIGIBLE, NON-EMPTY sstabledump line for the scratch table.
const GOOD_GOLDEN: &str = r#"{"partition":{"key":["7"]},"rows":[{"type":"row","liveness_info":{"tstamp":"2024-01-01T00:00:00.000001Z"},"cells":[{"name":"v","value":"hello"}]}]}"#;

/// The round-19 finding's own oracle: a NONEMPTY JSONL document, structurally
/// valid and physical-dump eligible, every partition of which carries an EMPTY
/// `rows` array. It parses, it validates, and it witnesses NOTHING.
const EMPTY_ROWS_GOLDEN: &str = concat!(
    r#"{"partition":{"key":["7"]},"rows":[]}"#,
    "\n",
    r#"{"partition":{"key":["8"]},"rows":[]}"#,
    "\n",
);

/// Build `<root>/<keyspace>/<table>-fixture/` holding the named files.
fn scratch_root(keyspace: &str, table: &str, files: &[(&str, &[u8])]) -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let dir = tmp
        .path()
        .join(keyspace)
        .join(format!("{table}-0123456789abcdef0123456789abcdef"));
    std::fs::create_dir_all(&dir).expect("mkdir scratch table dir");
    for (name, bytes) in files {
        std::fs::write(dir.join(name), bytes).expect("write scratch file");
    }
    tmp
}

/// The ordinary scratch fixture: one Data generation and its CORRESPONDING
/// golden, whose text the caller chooses.
fn scratch_root_with_golden(keyspace: &str, table: &str, golden: &str) -> tempfile::TempDir {
    scratch_root(
        keyspace,
        table,
        &[
            ("nb-1-big-Data.db", STUB_DATA),
            ("nb-1-big-Data.db.jsonl", golden.as_bytes()),
        ],
    )
}

/// The EXACT failure set an aborting export produces — what a `KnownGap`
/// legitimately records for a case whose export cannot run, and therefore the
/// gap that must NOT also excuse a precondition.
const ABORTING_EXPORT: &[ExpectedFailure] = &[
    ExpectedFailure::ExportAborted {
        detail: "Could not determine column names for export",
    },
    ExpectedFailure::Unrunnable {
        stage: Stage::ParquetRead,
        column: None,
        blocked_by: Stage::Export,
    },
    ExpectedFailure::Unrunnable {
        stage: Stage::ArrowTypes,
        column: None,
        blocked_by: Stage::Export,
    },
    ExpectedFailure::Unrunnable {
        stage: Stage::ValueComparison,
        column: None,
        blocked_by: Stage::Export,
    },
];

const EXPORT_ABORT_GAP: KnownGap = KnownGap {
    issue: "#0000",
    expect: ABORTING_EXPORT,
    what: "NEGATIVE CONTROL: records the export abort and every stage it blocks, and NOTHING \
           about the oracle — the shape that used to excuse a whole case",
};

/// A case over the scratch keyspace/table, naming a committed schema that does
/// NOT declare it (so the real export aborts) and recording the abort as a gap.
const fn scratch_case(known_gap: Option<KnownGap>) -> ParityCase {
    ParityCase {
        keyspace: "test_precondition",
        table: "scratch_table",
        // Deliberately a committed schema that does not declare this table.
        schema: "basic-types.cql",
        udts: &[],
        columns: &[("id", "int"), ("v", "text")],
        partition_key: &["id"],
        clustering: &[],
        schema_check: SchemaCheck::Synthetic {
            why: "the scratch table is not in any committed schema — that is what makes the \
                   real `cqlite export` abort, which is the short-circuit these controls must \
                   survive",
        },
        must_run: true,
        covers: "NEGATIVE CONTROL: a PRECONDITION must survive a recorded export-abort gap",
        known_gap,
        known_type_gaps: &[],
    }
}

// ---------------------------------------------------------------------------
// The harness: run a case over scratch roots and report what the gap said
// ---------------------------------------------------------------------------

/// Drive the real pipeline over `roots` and return every failure it produced.
///
/// Two shapes, both real: PRECONDITIONS 1 and 2 (the DECLARATION and the
/// FIXTURE) end staging outright, because with no verified declared types and no
/// resolved fixture there is nothing for the later stages to be aggregated FROM
/// — the export is never even spawned. PRECONDITIONS 3-6 are aggregated
/// alongside the export abort, which is the short-circuit round 19 was about.
/// The property under test is the same either way: the recorded gap must not
/// excuse the case, and must name the precondition.
fn failures_of(case: &ParityCase, roots: &[PathBuf]) -> Vec<Failure> {
    match parquet_parity::stage_case_in_roots(case, roots) {
        Err(failures) => failures.into_items(),
        Ok(None) => panic!("the scratch root holds the table, so it must never SKIP"),
        Ok(Some(stages)) => match parquet_parity::finish_case(case, stages) {
            Err(failures) => failures.into_items(),
            Ok(_) => panic!("the scratch export aborts, so the case must fail"),
        },
    }
}

/// The `KnownGap` verdict for `case` over `roots`, plus the failure set it saw.
fn gap_verdict(case: &ParityCase, roots: &[PathBuf]) -> (Option<String>, Vec<Failure>) {
    let failures = failures_of(case, roots);
    let gap = case
        .known_gap
        .as_ref()
        .expect("these controls all record a gap");
    (gap.mismatch(&case.id(), &failures), failures)
}

/// Assert that `check` was reported as an unmet PRECONDITION and that the
/// recorded gap REFUSED to excuse the case because of it.
fn assert_precondition_survives_the_gap(
    case: &ParityCase,
    root: &Path,
    check: Precondition,
    detail: &str,
) {
    let (problem, failures) = gap_verdict(case, &[root.to_path_buf()]);
    let observed: Vec<&Failure> = failures.iter().filter(|f| f.is_precondition()).collect();
    assert!(
        observed
            .iter()
            .any(|f| matches!(f, Failure::Precondition { check: c, .. } if *c == check)),
        "the {} precondition must be REPORTED, not skipped. Observed: {:?}",
        check.name(),
        failures.iter().map(Failure::signature).collect::<Vec<_>>()
    );
    let problem = problem.unwrap_or_else(|| {
        panic!(
            "the recorded KnownGap EXCUSED a case whose {} precondition did not hold — a gap \
             may suppress only the ASSERTION it names",
            check.name()
        )
    });
    assert!(
        problem.contains("excuse a validity PRECONDITION")
            && problem.contains(check.name())
            && problem.contains(detail),
        "the refusal must name the PRECONDITION and what did not hold ({}, {detail:?}): \
         {problem}",
        check.name()
    );
}

// ---------------------------------------------------------------------------
// THE FINDING (round 19): an EMPTY oracle behind an expected export abort
// ---------------------------------------------------------------------------

/// A nonempty JSONL golden whose partitions ALL carry empty `rows` arrays, on a
/// case whose export ABORTS behind a recorded `KnownGap`, must FAIL.
///
/// This is roborev round 19's finding verbatim. The zero-row check used to live
/// in `compare_inner`, so the aborting export meant it never ran: the observed
/// failure set was EXACTLY the recorded one and the case was fully excused,
/// while the oracle it claimed to defer a comparison against contained nothing
/// at all.
#[test]
fn an_empty_oracle_is_not_excused_by_a_gap_recording_an_aborting_export() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root_with_golden(CASE.keyspace, CASE.table, EMPTY_ROWS_GOLDEN);

    // First, the trap: the WHOLE recorded set really does reproduce, so a
    // mechanism that only compared the recorded failures would have excused it.
    let failures = failures_of(&CASE, &[tmp.path().to_path_buf()]);
    let signatures: Vec<String> = failures.iter().map(Failure::signature).collect();
    for expected in ["export-aborted", "unrunnable[value-comparison]"] {
        assert!(
            signatures.iter().any(|s| s.starts_with(expected)),
            "the recorded export-abort set must genuinely reproduce, or this control is not \
             the finding: {signatures:?}"
        );
    }

    assert_precondition_survives_the_gap(
        &CASE,
        tmp.path(),
        Precondition::GoldenNonEmpty,
        "projected to ZERO rows",
    );
}

/// The same empty oracle with NO gap recorded must fail too — so the refusal is
/// a property of the oracle and not of the gap bookkeeping.
#[test]
fn an_empty_oracle_fails_a_case_that_records_no_gap_at_all() {
    const CASE: ParityCase = scratch_case(None);
    let tmp = scratch_root_with_golden(CASE.keyspace, CASE.table, EMPTY_ROWS_GOLDEN);
    let failures = failures_of(&CASE, &[tmp.path().to_path_buf()]);
    assert!(
        failures.iter().any(|f| matches!(
            f,
            Failure::Precondition {
                check: Precondition::GoldenNonEmpty,
                ..
            }
        )),
        "{:?}",
        failures.iter().map(Failure::signature).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// ONE CASE PER PRECONDITION — "unconditional" is what decays silently
// ---------------------------------------------------------------------------

/// PRECONDITION 1 — the case's DECLARATION matches the committed CQL schema.
///
/// Uses `SchemaCheck::Committed` on a real committed table and mis-declares one
/// column's type. An unverified declaration is what stages 4 and 5 derive their
/// expectations from, so a gap excusing this would defer a comparison whose
/// ground truth is wrong.
#[test]
fn a_gap_does_not_suppress_the_declaration_precondition() {
    const CASE: ParityCase = ParityCase {
        keyspace: "test_precondition",
        table: "scratch_table",
        schema: "basic-types.cql",
        udts: &[],
        columns: &[("id", "int"), ("v", "text")],
        partition_key: &["id"],
        clustering: &[],
        // The point of this control: the declaration check is DEMANDED, and the
        // committed schema does not declare `test_precondition.scratch_table`.
        schema_check: SchemaCheck::Committed,
        must_run: true,
        covers: "NEGATIVE CONTROL: a drifted declaration behind a recorded gap",
        known_gap: Some(EXPORT_ABORT_GAP),
        known_type_gaps: &[],
    };
    let tmp = scratch_root_with_golden(CASE.keyspace, CASE.table, GOOD_GOLDEN);
    assert_precondition_survives_the_gap(
        &CASE,
        tmp.path(),
        Precondition::Declaration,
        "scratch_table",
    );
}

/// PRECONDITION 2 — a single-generation fixture and the golden that CORRESPONDS
/// to it.
///
/// The scratch directory holds `nb-2-big-Data.db` beside a `nb-1-big` golden:
/// one generation's data against another generation's dump, which can yield a
/// false failure OR a false pass. A gap must never defer that question.
#[test]
fn a_gap_does_not_suppress_the_fixture_correspondence_precondition() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root(
        CASE.keyspace,
        CASE.table,
        &[
            ("nb-2-big-Data.db", STUB_DATA),
            ("nb-1-big-Data.db.jsonl", GOOD_GOLDEN.as_bytes()),
        ],
    );
    assert_precondition_survives_the_gap(
        &CASE,
        tmp.path(),
        Precondition::Fixture,
        "belongs to a DIFFERENT generation",
    );
}

/// PRECONDITION 3 — the golden exists, is readable and PARSES.
#[test]
fn a_gap_does_not_suppress_the_golden_parses_precondition() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root_with_golden(CASE.keyspace, CASE.table, "{not json at all");
    assert_precondition_survives_the_gap(
        &CASE,
        tmp.path(),
        Precondition::GoldenReadable,
        "sstabledump golden",
    );
}

/// PRECONDITION 4 — the golden's sstabledump STRUCTURE holds, field by field.
///
/// A BARE NUMERIC partition-key component. `sstabledump` writes every component
/// as a JSON string (`serializePartitionKey`), and this is not a cosmetic shape
/// detail: a bare number canonicalizes straight to an `Int` and compares EQUAL
/// to a correct export — a FALSE PASS. It is the right subject here because it
/// survives the earlier lexeme-preserving pass and is refused by the TOTAL
/// structural validation and by nothing else.
#[test]
fn a_gap_does_not_suppress_the_golden_structure_precondition() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root_with_golden(
        CASE.keyspace,
        CASE.table,
        &GOOD_GOLDEN.replace(r#""key":["7"]"#, r#""key":[7]"#),
    );
    assert_precondition_survives_the_gap(
        &CASE,
        tmp.path(),
        Precondition::GoldenStructure,
        "not usable as a physical-dump oracle",
    );
}

/// PRECONDITION 5 — the golden is physical-dump ELIGIBLE (#1742) and PROJECTS.
///
/// A per-cell TOMBSTONE on the scalar column `v`. Deliberately a construct the
/// TEXT validation cannot judge — a `deletion_info` on a cell is the ordinary
/// marker a whole-collection INSERT writes, so its legality depends on the
/// DECLARED type, which only the projection knows. It is therefore the shape
/// that isolates this precondition from PRECONDITION 4: eligibility constructs
/// visible in the text (a row deletion, a TTL) are refused by the structural
/// validation first, and the two must not be tested through the same door.
#[test]
fn a_gap_does_not_suppress_the_golden_eligibility_precondition() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root_with_golden(
        CASE.keyspace,
        CASE.table,
        r#"{"partition":{"key":["7"]},"rows":[{"type":"row","liveness_info":{"tstamp":"2024-01-01T00:00:00.000001Z"},"cells":[{"name":"v","deletion_info":{"local_delete_time":"2024-01-01T00:00:00Z"}}]}]}"#,
    );
    assert_precondition_survives_the_gap(
        &CASE,
        tmp.path(),
        Precondition::GoldenEligible,
        "cell tombstone on scalar column 'v'",
    );
}

/// PRECONDITION 6 — the golden projects to AT LEAST ONE ROW.
///
/// The finding again, asserted through the same per-precondition harness as its
/// five siblings so a refactor that re-entangles ANY of them reds one suite.
#[test]
fn a_gap_does_not_suppress_the_golden_non_emptiness_precondition() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root_with_golden(CASE.keyspace, CASE.table, EMPTY_ROWS_GOLDEN);
    assert_precondition_survives_the_gap(
        &CASE,
        tmp.path(),
        Precondition::GoldenNonEmpty,
        "projected to ZERO rows",
    );
}

// ---------------------------------------------------------------------------
// The CONTROL: the very same gap DOES excuse the case when every precondition
// holds
// ---------------------------------------------------------------------------

/// Without a control, every test above would pass against a `KnownGap` mechanism
/// that had simply stopped excusing anything — which is a different bug with the
/// same green.
///
/// Same scratch case, same recorded gap, a VALID/ELIGIBLE/NON-EMPTY golden: the
/// gap must be satisfied exactly, so the assertion it names really is deferred.
#[test]
fn the_same_gap_still_excuses_the_case_when_every_precondition_holds() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root_with_golden(CASE.keyspace, CASE.table, GOOD_GOLDEN);
    let (problem, failures) = gap_verdict(&CASE, &[tmp.path().to_path_buf()]);
    assert!(
        failures.iter().all(|f| !f.is_precondition()),
        "this control must hold every precondition: {:?}",
        failures.iter().map(Failure::signature).collect::<Vec<_>>()
    );
    assert_eq!(
        problem,
        None,
        "the recorded set is exactly what happened, so the ASSERTION the gap names must still \
         be deferred: {:?}",
        failures.iter().map(Failure::signature).collect::<Vec<_>>()
    );
}

/// LAYER 3 — `assert_case`, the entry point every real parity case goes
/// through, refuses BEFORE it reads `case.known_gap` at all.
///
/// Layers 1 and 2 already make the gap unable to excuse a precondition, so this
/// one is about the DIAGNOSTIC: without it the panic reports a vacuous
/// comparison as an "unrecorded extra", which reads like a bookkeeping slip
/// rather than "the oracle was empty". Asserted through the real entry point,
/// because that is the one an ordinary case calls.
#[test]
fn assert_case_names_the_precondition_before_it_reads_the_recorded_gap() {
    const CASE: ParityCase = scratch_case(Some(EXPORT_ABORT_GAP));
    let tmp = scratch_root_with_golden(CASE.keyspace, CASE.table, EMPTY_ROWS_GOLDEN);
    let root = tmp.path().to_path_buf();

    let panicked = std::panic::catch_unwind(move || {
        parquet_parity::assert_case_in_roots(&CASE, &[root]);
    })
    .expect_err("assert_case MUST panic on a case whose oracle is empty");
    let message = panicked
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    assert!(
        message.contains("validity PRECONDITION(s) of the comparison did not hold")
            && message.contains(Precondition::GoldenNonEmpty.name())
            && message.contains("projected to ZERO rows")
            && message.contains("A KnownGap can never excuse one"),
        "the panic must name the PRECONDITION, not report it as an unrecorded extra: {message}"
    );
}

/// The census: this suite covers EVERY precondition, and covers nothing that is
/// not one.
///
/// "Unconditional" is exactly the property that decays silently, so the
/// obligation is derived from `Precondition::ALL` rather than from a list kept
/// here — a variant added there with no test reds this immediately, and a test
/// naming something not in the census reds it too.
#[test]
fn every_precondition_has_a_case_proving_a_gap_does_not_suppress_it() {
    /// The precondition each `a_gap_does_not_suppress_*` case above drives,
    /// paired with that test's name so a reader can find it.
    const COVERED: &[(Precondition, &str)] = &[
        (
            Precondition::Declaration,
            "a_gap_does_not_suppress_the_declaration_precondition",
        ),
        (
            Precondition::Fixture,
            "a_gap_does_not_suppress_the_fixture_correspondence_precondition",
        ),
        (
            Precondition::GoldenReadable,
            "a_gap_does_not_suppress_the_golden_parses_precondition",
        ),
        (
            Precondition::GoldenStructure,
            "a_gap_does_not_suppress_the_golden_structure_precondition",
        ),
        (
            Precondition::GoldenEligible,
            "a_gap_does_not_suppress_the_golden_eligibility_precondition",
        ),
        (
            Precondition::GoldenNonEmpty,
            "a_gap_does_not_suppress_the_golden_non_emptiness_precondition",
        ),
    ];

    for check in Precondition::ALL {
        assert!(
            COVERED.iter().any(|(c, _)| c == check),
            "PRECONDITION '{}' has no case proving a KnownGap does not suppress it. Add one to \
             this suite — a precondition nobody exercises is one a refactor can re-entangle \
             with the comparison path, which is exactly how rounds 12, 13 and 19 happened.",
            check.name()
        );
    }
    for (check, test_name) in COVERED {
        assert!(
            Precondition::ALL.contains(check),
            "'{test_name}' claims to cover '{}', which is not in Precondition::ALL",
            check.name()
        );
    }
    assert_eq!(
        COVERED.len(),
        Precondition::ALL.len(),
        "the coverage table and the census must be the same size (no duplicate claims)"
    );
}

/// A `KnownGap` cannot be made to name a precondition, because no
/// `ExpectedFailure` variant can produce one's signature — layer (1) of the two
/// that keep preconditions gap-independent.
///
/// Asserted on DATA rather than by reading the enum: `ExpectedFailure` has three
/// variants and this exercises every one of them, so adding a fourth that could
/// carry a precondition would need this test changed on purpose.
#[test]
fn no_expected_failure_can_carry_a_precondition_signature() {
    let precondition = Failure::Precondition {
        check: Precondition::GoldenNonEmpty,
        why: "the sstabledump golden projected to ZERO rows".to_string(),
    };
    let gap = KnownGap {
        issue: "#0000",
        expect: &[
            ExpectedFailure::ExportAborted {
                detail: "the sstabledump golden projected to ZERO rows",
            },
            ExpectedFailure::ArrowType {
                column: "v",
                expected: "utf8",
                actual: "utf8",
            },
            ExpectedFailure::Unrunnable {
                stage: Stage::ValueComparison,
                column: Some("v"),
                blocked_by: Stage::Golden,
            },
        ],
        what: "NEGATIVE CONTROL: every ExpectedFailure variant, trying to name a precondition",
    };
    assert!(
        gap.mismatch("test_precondition.scratch_table", &[precondition])
            .is_some(),
        "no combination of ExpectedFailure variants may excuse a Failure::Precondition"
    );
}
