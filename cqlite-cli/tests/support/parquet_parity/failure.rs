//! STRUCTURED failure data for the Parquet↔JSONL parity harness (issue #1490).
//!
//! # Why the harness does not report failures as prose
//!
//! A recorded [`KnownGap`] is only defensible over a plain skip because it is
//! PRECISE and SELF-RETIRING: it names one divergence, fails when that
//! divergence stops reproducing, and refuses to absorb anything else.
//!
//! The first two attempts at that third property both failed, in opposite
//! directions, and both because the gap was matched against a RENDERED MESSAGE:
//!
//!   1. A single substring (`"column 'lp'"`) matched too LOOSELY — any unrelated
//!      failure mentioning the same column satisfied it.
//!   2. A CONJUNCTION of precise substrings fixed that, and then matched
//!      NON-EXCLUSIVELY: it proves the recorded failure IS present but says
//!      nothing about whether anything ELSE is. The harness aggregates every
//!      Arrow type mismatch into ONE message, so a simultaneous unrecorded
//!      regression on a different column (an `ma` mismatch appearing after the
//!      recorded `lp` one) rode along inside the same string and was excused.
//!
//! # A refusal is not a failure to record
//!
//! [`Failure::UnsupportedRepresentation`] is the harness declining to compare a
//! representation it cannot represent (see `unsupported.rs`). It has NO
//! [`ExpectedFailure`] counterpart, on purpose: a gap records a product defect
//! that still reproduces, and absorbing a harness refusal into one would turn
//! "unmeasured" into "known and fine" — the exact conversion this module exists
//! to prevent.
//!
//! # A PRECONDITION is not an ASSERTION, and a gap can only ever excuse an
//! ASSERTION
//!
//! An expected-failure marker must suppress ONLY the assertion it names, never a
//! validity PRECONDITION of the comparison. A [`KnownGap`] says "this named
//! comparison is expected to fail because of a recorded product defect"; it must
//! never also mean "and therefore skip the checks that establish the comparison
//! was meaningful at all".
//!
//! [`Failure::Precondition`] is that class, and it is structurally
//! unsuppressible for TWO independent reasons:
//!
//!   1. there is no [`ExpectedFailure`] variant that can carry one, so no gap
//!      can even NAME it — the multiset equality therefore always reports it as
//!      an unrecorded extra; and
//!   2. [`KnownGap::mismatch`] refuses up front when ANY observed failure is a
//!      precondition, before the recorded set is consulted at all.
//!
//! This is the THIRD appearance of one family (see [`Precondition`] for the
//! roll), so the two layers are deliberate redundancy rather than duplication:
//! (1) alone would be defeated the day somebody adds an `ExpectedFailure`
//! counterpart "for symmetry", and (2) alone would be defeated by a second
//! consumer that matches gaps without going through `mismatch`.
//!
//! Containment cannot express the property actually wanted, which is EXACTNESS:
//! the observed failure set must EQUAL the recorded one. So the harness reports
//! a typed [`Failure`] per thing that went wrong, both the assert path and the
//! known-gap path consume that list, and [`KnownGap::mismatch`] compares the
//! observed MULTISET of failure signatures to the recorded one by equality. An
//! unrecorded failure accompanying a known gap is an EXTRA element, so the sets
//! differ and the case FAILS.

#![allow(dead_code)]

use super::arrow_expect::TypeMismatch;
use super::unsupported::Unsupported;

/// The stages one case runs, each with an INDEPENDENTLY determined outcome.
///
/// Named as data so a stage that COULD NOT RUN is recordable
/// ([`Failure::Unrunnable`]) rather than silently missing from the failure set.
/// Round-3 roborev finding: the pipeline bailed at the first failing stage, so
/// an expected export abort suppressed the golden's own validation and a type
/// mismatch on one column suppressed every OTHER column's values — the "exact
/// failure set" a [`KnownGap`] is compared against was therefore not the exact
/// set of what went wrong, and a gap could hide both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Load and project the committed sstabledump golden, including its
    /// physical-dump ELIGIBILITY (#1742). Depends on NOTHING the export does.
    Golden,
    /// Run the real `cqlite export --format parquet`.
    Export,
    /// Read the exported Parquet back and check its column SET.
    ParquetRead,
    /// Validate every field's Arrow TYPE against the declared CQL type.
    ArrowTypes,
    /// Sort both sides by primary key and compare per cell.
    ValueComparison,
}

impl Stage {
    /// The stage's stable identifier — part of an [`Failure::Unrunnable`]
    /// signature, so it is a TOKEN and not prose.
    pub fn name(self) -> &'static str {
        match self {
            Stage::Golden => "golden-projection",
            Stage::Export => "export",
            Stage::ParquetRead => "parquet-read",
            Stage::ArrowTypes => "arrow-types",
            Stage::ValueComparison => "value-comparison",
        }
    }
}

/// A validity PRECONDITION of the whole comparison — a check that establishes
/// the comparison is MEANINGFUL, as opposed to an ASSERTION about what it found.
///
/// # Why this exists as its own class (issue #1490, rounds 12/13/19)
///
/// Three review rounds found the same family — a check that could be skipped
/// because something EARLIER in the pipeline had already been excused:
///
///   * **round 12**: the pipeline `?`-chained, so the "exact failure set" a
///     [`KnownGap`] is compared against stopped at the first failing stage. Fixed
///     by aggregating five independently-determined stages.
///   * **round 13**: physical-dump ELIGIBILITY was decided from what a LENIENT
///     parser managed to parse, so a present-but-invalid `ttl`/`deletion_info`
///     read as an absence. Fixed by deciding it from the golden TEXT.
///   * **round 19**: the ZERO-ROW check sat AFTER the gap short-circuit, in
///     `compare_inner`, so a golden whose partitions all carried empty `rows`
///     passed on a case whose export aborted behind a recorded gap. Fixed by
///     making it — and every other precondition — run at the point the oracle is
///     LOADED, unconditionally.
///
/// Round 12's aggregation closed two of the three and left this one, which is
/// why the fix is a CLASS and not a moved line: preconditions are separated from
/// assertions across the whole case pipeline, and are gap-independent BY
/// CONSTRUCTION (see the module header for the two layers that enforce it).
///
/// Each variant is a stable TOKEN, so a precondition failure's signature is
/// derived from data rather than scraped out of a sentence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precondition {
    /// The case's declared columns, types and key definitions agree with the
    /// committed CQL schema — the ground truth every later expectation is
    /// derived from (`schema_fixture`).
    Declaration,
    /// A fixture was resolved: exactly one Data generation, the golden that
    /// CORRESPONDS to it, and an isolated staging directory the export can read
    /// (`fixture_root`).
    Fixture,
    /// The golden exists, is readable, and PARSES.
    GoldenReadable,
    /// The golden's sstabledump STRUCTURE holds — the total field-by-field
    /// validation (`golden_schema`).
    GoldenStructure,
    /// The golden is physical-dump ELIGIBLE (#1742) and projects: no TTL, no row
    /// or partition deletion, no range tombstone, no static block, and every
    /// position canonicalizes under its declared type.
    GoldenEligible,
    /// The golden projects to AT LEAST ONE ROW. An empty oracle cannot witness
    /// anything, so a comparison against it is vacuous however it turns out.
    GoldenNonEmpty,
}

impl Precondition {
    /// The precondition's stable identifier — part of the failure signature, so
    /// it is a TOKEN and not prose.
    pub fn name(self) -> &'static str {
        match self {
            Precondition::Declaration => "declaration-matches-committed-schema",
            Precondition::Fixture => "fixture-resolved-and-staged",
            Precondition::GoldenReadable => "golden-readable-and-parses",
            Precondition::GoldenStructure => "golden-structure-valid",
            Precondition::GoldenEligible => "golden-physical-dump-eligible",
            Precondition::GoldenNonEmpty => "golden-projects-at-least-one-row",
        }
    }

    /// The property it establishes, for the diagnostic.
    pub fn what(self) -> &'static str {
        match self {
            Precondition::Declaration => {
                "the case's declared columns and keys match the committed CQL schema"
            }
            Precondition::Fixture => {
                "a single-generation fixture and its CORRESPONDING golden were resolved and staged"
            }
            Precondition::GoldenReadable => "the sstabledump golden exists, is readable and parses",
            Precondition::GoldenStructure => "the sstabledump golden's structure holds",
            Precondition::GoldenEligible => {
                "the sstabledump golden is eligible for physical-dump parity (#1742)"
            }
            Precondition::GoldenNonEmpty => "the sstabledump golden projects to at least one row",
        }
    }
}

/// ONE structured thing that went wrong in a case.
///
/// Every variant carries its identifying facts as FIELDS, so
/// [`Failure::signature`] can be derived from them rather than scraped back out
/// of a rendered sentence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// The `cqlite export` invocation itself aborted.
    ///
    /// At most ONE of these can occur per case — the export runs once — which is
    /// what lets [`ExpectedFailure::ExportAborted`] pin the CLI's stderr by
    /// substring without reopening the non-exclusivity hole.
    ExportAborted {
        /// `<keyspace>.<table>`, as passed to `--table`.
        table: String,
        /// The CLI's stderr, verbatim.
        stderr: String,
    },
    /// One column's exported Arrow type is not its declared CQL type's.
    ArrowType {
        mismatch: TypeMismatch,
        /// Diagnostic decoration (e.g. "a recorded gap expected a different
        /// type"). Deliberately NOT part of the signature: it describes the
        /// harness's bookkeeping, not the defect's identity.
        note: Option<String>,
    },
    /// A VALUE divergence vs the sstabledump golden: a per-cell difference, a
    /// row-count difference or a primary-key difference.
    ///
    /// Deliberately NOT expressible as an [`ExpectedFailure`]: the per-cell diff
    /// list is TRUNCATED at 10 entries, so an observed value-failure multiset is
    /// not the complete one and an "equality" against it would be a lie. A value
    /// divergence therefore always fails a known-gap case as an unrecorded
    /// extra — which is the fail-closed answer.
    Value(String),
    /// A stage that COULD NOT RUN, because a stage it depends on failed.
    ///
    /// Recorded rather than omitted: a stage that PASSED and a stage that never
    /// ran must never be indistinguishable. Omitting it is what let an expected
    /// export abort hide an ineligible golden, and a recorded type mismatch on
    /// one column hide a VALUE regression on the others — in both cases the
    /// "exact failure set" silently shrank to the part the gap already knew
    /// about. Being a [`Failure`], an unrunnable stage now has to be RECORDED by
    /// any [`KnownGap`] that wants to defer the case, so the deferral states
    /// exactly how much it defers.
    Unrunnable {
        stage: Stage,
        /// The single column it could not run FOR, when the block is per column
        /// (a wrong Arrow type on one column blocks only that column's values).
        column: Option<String>,
        /// The stage whose failure blocked it.
        blocked_by: Stage,
    },
    /// A REPRESENTATION the harness declines to compare or to validate: the
    /// THIRD outcome beside `equal` and `unequal` (see `unsupported.rs`).
    ///
    /// Distinct from every other variant on purpose:
    ///
    /// * not `ArrowType`/`Value` — nothing DIVERGED; the harness declined to
    ///   answer, and a decline must never be counted as a difference either;
    /// * not `Unrunnable` — that stage was blocked by ANOTHER stage's failure,
    ///   whereas this stage ran and refused this one column;
    /// * not `Refusal` — that is harness BOOKKEEPING (an unparsable declaration,
    ///   a mis-recorded gap), while this is a named, enumerated representation
    ///   with a retirement path.
    ///
    /// And, like `Value`, deliberately NOT expressible as an
    /// [`ExpectedFailure`]: a [`KnownGap`] records a PRODUCT defect that still
    /// reproduces, so letting it absorb a HARNESS refusal would silently convert
    /// "we cannot measure this" into "we know about this and it is fine". An
    /// observed refusal is therefore always an unrecorded extra, and it always
    /// fails the case.
    UnsupportedRepresentation {
        /// The stage that refused — the ARROW-TYPE stage for an unverifiable
        /// type claim, the VALUE-COMPARISON stage for an uncomparable value
        /// representation.
        stage: Stage,
        /// The declared column it refused for. Always per column: a refusal is a
        /// property of one declared type, never of the whole case.
        column: String,
        refused: Unsupported,
    },
    /// A validity PRECONDITION of the comparison did not hold (see
    /// [`Precondition`]).
    ///
    /// NOT expressible as an [`ExpectedFailure`], and refused up front by
    /// [`KnownGap::mismatch`]: an expected-failure marker may suppress only the
    /// ASSERTION it names, never a precondition of the comparison being
    /// meaningful. A gap that could excuse "the oracle was empty" or "the golden
    /// is ineligible" would be recording, as a known product defect, the fact
    /// that nothing was measured.
    Precondition {
        check: Precondition,
        /// What did not hold, verbatim from the site that measured it.
        why: String,
    },
    /// The harness REFUSES to answer: an unparsable declaration, a fixture that
    /// is not eligible for physical-dump parity, an unreadable file, a
    /// mis-recorded gap. Never recordable as a known gap either.
    ///
    /// Distinct from [`Failure::Precondition`]: a refusal is harness
    /// BOOKKEEPING about a record or a representation, while a precondition is a
    /// property of the ORACLE and of the case's declaration that the comparison
    /// is built on. Both are unrecordable, but only the precondition class is
    /// enumerated, hoisted to the point the oracle is LOADED, and asserted as a
    /// class.
    Refusal(String),
}

impl Failure {
    /// The failure's IDENTITY, for multiset comparison against a recorded gap.
    ///
    /// Two failures share a signature exactly when they are the same defect on
    /// the same subject. Free-text detail that varies run to run (a temp path,
    /// an exit status, a diagnostic note) is excluded on purpose; detail that
    /// identifies the defect (the column, both Arrow types) is included.
    pub fn signature(&self) -> String {
        match self {
            Failure::ExportAborted { table, .. } => format!("export-aborted[{table}]"),
            Failure::ArrowType { mismatch, .. } => format!(
                "arrow-type[{}] expected={} actual={}",
                mismatch.column, mismatch.expected, mismatch.actual
            ),
            Failure::Value(diff) => format!("value-difference[{diff}]"),
            Failure::Unrunnable {
                stage,
                column,
                blocked_by,
            } => unrunnable_signature(*stage, column.as_deref(), *blocked_by),
            // The `why` is NOT in the signature: the identity of a refusal is
            // WHICH representation was refused on WHICH column, by WHICH stage.
            Failure::UnsupportedRepresentation {
                stage,
                column,
                refused,
            } => format!(
                "unsupported-representation[{}:column '{column}'] representation={}",
                stage.name(),
                refused.representation
            ),
            // The `why` IS part of the signature: two preconditions of the same
            // class on the same case are two different things that did not hold.
            // It can never collide with a recorded expectation — no
            // `ExpectedFailure` produces a `precondition[...]` signature.
            Failure::Precondition { check, why } => {
                format!("precondition[{}] {why}", check.name())
            }
            Failure::Refusal(reason) => format!("refusal[{reason}]"),
        }
    }

    /// Is this a validity PRECONDITION of the comparison, rather than an
    /// ASSERTION about what the comparison found?
    ///
    /// The single predicate every consumer asks, so "a gap may excuse an
    /// assertion and never a precondition" is enforced in ONE place.
    pub fn is_precondition(&self) -> bool {
        matches!(self, Failure::Precondition { .. })
    }

    /// Human-readable rendering, for the panic message.
    pub fn render(&self) -> String {
        match self {
            Failure::ExportAborted { table, stderr } => {
                format!("cqlite export failed for {table}: {stderr}")
            }
            Failure::ArrowType { mismatch, note } => match note {
                Some(note) => format!("{mismatch} — {note}"),
                None => mismatch.to_string(),
            },
            Failure::Value(diff) => diff.clone(),
            Failure::Unrunnable {
                stage,
                column,
                blocked_by,
            } => {
                let subject = match column {
                    Some(column) => format!(" for column '{column}'"),
                    None => String::new(),
                };
                format!(
                    "the {} stage COULD NOT RUN{subject} because the {} stage failed — recorded \
                     rather than omitted, so a stage that never ran is never mistaken for one \
                     that passed",
                    stage.name(),
                    blocked_by.name()
                )
            }
            Failure::UnsupportedRepresentation {
                stage,
                column,
                refused,
            } => format!(
                "UNSUPPORTED REPRESENTATION: the {} stage REFUSES column '{column}' \
                 (representation '{}') — {}. A representation the harness cannot compare is \
                 NOT a pass: this case DECLARES the column, so it claims coverage the harness \
                 cannot deliver. Teach the harness the representation or drop the column; it \
                 cannot be recorded as a known gap (issue #1490).",
                stage.name(),
                refused.representation,
                refused.why
            ),
            Failure::Precondition { check, why } => format!(
                "PRECONDITION NOT MET ({}): {why}. This is a validity PRECONDITION of the \
                 comparison — it establishes that {} — so no KnownGap can excuse it: a gap \
                 records a product defect the comparison FOUND, and there was no meaningful \
                 comparison to find one in (issue #1490 round 19).",
                check.name(),
                check.what()
            ),
            Failure::Refusal(reason) => reason.clone(),
        }
    }
}

/// The shared signature of an unrunnable stage — derived once, so the observed
/// [`Failure`] and the recorded [`ExpectedFailure`] can never drift apart.
fn unrunnable_signature(stage: Stage, column: Option<&str>, blocked_by: Stage) -> String {
    match column {
        Some(column) => format!(
            "unrunnable[{}:column '{column}'] blocked-by={}",
            stage.name(),
            blocked_by.name()
        ),
        None => format!(
            "unrunnable[{}] blocked-by={}",
            stage.name(),
            blocked_by.name()
        ),
    }
}

/// Every failure one case produced, in discovery order.
///
/// Never empty: a `Failures` is only constructed on a failing path.
#[derive(Debug, Clone)]
pub struct Failures {
    case: Option<String>,
    items: Vec<Failure>,
}

impl Failures {
    /// A single harness REFUSAL.
    pub fn refusal(reason: impl Into<String>) -> Self {
        Failures::one(Failure::Refusal(reason.into()))
    }

    /// A single failing PRECONDITION (see [`Precondition`]).
    pub fn precondition(check: Precondition, why: impl Into<String>) -> Self {
        Failures::one(Failure::Precondition {
            check,
            why: why.into(),
        })
    }

    /// A single failure of any kind.
    pub fn one(item: Failure) -> Self {
        Failures {
            case: None,
            items: vec![item],
        }
    }

    /// Several failures the harness deliberately reported together (every
    /// mismatching column, not just the first — a wrong mapping usually affects
    /// a family of columns and one report naming all of them is what makes the
    /// diagnosis possible).
    pub fn many(items: Vec<Failure>) -> Self {
        Failures { case: None, items }
    }

    /// Attach the case id for the diagnostic, if it is not already attached.
    pub fn for_case(mut self, case: &str) -> Self {
        if self.case.is_none() {
            self.case = Some(case.to_string());
        }
        self
    }

    pub fn items(&self) -> &[Failure] {
        &self.items
    }

    /// Every observed PRECONDITION failure. Consulted BEFORE any known-gap
    /// bookkeeping, by every consumer.
    pub fn preconditions(&self) -> Vec<&Failure> {
        self.items.iter().filter(|f| f.is_precondition()).collect()
    }

    /// Consume the list, so one stage's failures can be folded into the case's
    /// AGGREGATE instead of ending it.
    pub fn into_items(self) -> Vec<Failure> {
        self.items
    }
}

impl From<String> for Failures {
    fn from(reason: String) -> Self {
        Failures::refusal(reason)
    }
}

impl From<Failure> for Failures {
    fn from(item: Failure) -> Self {
        Failures::one(item)
    }
}

impl std::fmt::Display for Failures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = match &self.case {
            Some(case) => format!("{case}: "),
            None => String::new(),
        };
        match self.items.len() {
            1 => write!(f, "{prefix}{}", self.items[0].render()),
            n => {
                write!(
                    f,
                    "{prefix}{n} failure(s) — the harness reports EVERY one, so a recorded \
                     known_gap can be compared against the exact set:",
                )?;
                for item in &self.items {
                    write!(f, "\n  - {}", item.render())?;
                }
                Ok(())
            }
        }
    }
}

/// ONE failure a [`KnownGap`] records, as structured data.
///
/// Compared by EQUALITY on the same signature [`Failure::signature`] derives, so
/// a DIFFERENT defect on the same subject is never absorbed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedFailure {
    /// The `cqlite export` invocation aborted, with `detail` appearing in its
    /// stderr.
    ///
    /// `detail` is the ONE substring comparison left in the mechanism, and it is
    /// sound only because of the surrounding exactness: the CLI's stderr also
    /// carries a temp-directory path and an exit status, neither of which is a
    /// stable fact about the defect, so the converter error text is the only
    /// part that can be pinned — and the failure's IDENTITY ("this case's export
    /// aborted") is unique, because an export runs at most once, so this variant
    /// can never be joined by a second unrecorded abort hiding inside the same
    /// string. The multiset equality still governs whether anything ELSE
    /// happened.
    ExportAborted { detail: &'static str },
    /// Column `column` exported Arrow type `actual` where `expected` was
    /// expected — all three in `arrow_expect`'s rendering vocabulary
    /// (`ArrowShape::describe` / `render_arrow`), compared by EQUALITY.
    ArrowType {
        column: &'static str,
        expected: &'static str,
        actual: &'static str,
    },
    /// A stage of the pipeline COULD NOT RUN because `blocked_by` failed.
    ///
    /// A gap that defers a case has to say how MUCH it defers: recording an
    /// export abort no longer implicitly excuses the parquet read, the type
    /// check and the value comparison — each of those has to be recorded as
    /// unrunnable, by name.
    Unrunnable {
        stage: Stage,
        /// The one column it could not run for, or `None` for the whole stage.
        column: Option<&'static str>,
        blocked_by: Stage,
    },
}

impl ExpectedFailure {
    fn signature(&self, case_id: &str) -> String {
        match self {
            ExpectedFailure::ExportAborted { .. } => format!("export-aborted[{case_id}]"),
            ExpectedFailure::ArrowType {
                column,
                expected,
                actual,
            } => format!("arrow-type[{column}] expected={expected} actual={actual}"),
            ExpectedFailure::Unrunnable {
                stage,
                column,
                blocked_by,
            } => unrunnable_signature(*stage, *column, *blocked_by),
        }
    }
}

/// A recorded, issue-tracked export gap.
///
/// # The recorded set is EXACT, not a lower bound
///
/// `expect` is the COMPLETE list of failures the case currently exhibits. The
/// harness compares it to what actually happened by MULTISET EQUALITY:
///
/// * a recorded failure that stopped happening → the gap is retiring, FAIL;
/// * a failure that is NOT recorded → an unrelated regression the gap must never
///   hide, FAIL;
/// * no failures at all → the gap is fixed, FAIL (delete the record).
///
/// Only an exact match is excused. See the module header for the two weaker
/// designs this replaced and why each leaked.
pub struct KnownGap {
    /// The GitHub issue tracking the fix, e.g. `"#3556"`.
    pub issue: &'static str,
    /// The EXACT set of failures the case exhibits today.
    pub expect: &'static [ExpectedFailure],
    /// Why the gap exists, in one line.
    pub what: &'static str,
}

impl KnownGap {
    /// `None` when the observed failures are EXACTLY the recorded ones;
    /// otherwise the reason they are not, ready to panic with.
    ///
    /// A gap may excuse only ASSERTIONS. Any observed [`Failure::Precondition`]
    /// makes this return `Some` unconditionally — see [`precondition_banner`],
    /// and [`Precondition`] for the three rounds that made it a class.
    pub fn mismatch(&self, case_id: &str, observed: &[Failure]) -> Option<String> {
        // FIRST, before the recorded set is read at all: a precondition of the
        // comparison did not hold, so there was no meaningful comparison for a
        // gap to be about. This is layer (2) of the two the module header names;
        // layer (1) is that no `ExpectedFailure` can produce a `precondition[…]`
        // signature, which is why the set difference below ALSO always reports
        // one as an unrecorded extra. Both are kept on purpose.
        let banner = precondition_banner(observed);

        if self.expect.is_empty() {
            return Some(format!(
                "{}the recorded KnownGap lists NO expected failures, so it could match \
                 anything — record the exact failure set it exhibits",
                banner.clone().unwrap_or_default()
            ));
        }

        let mut want: Vec<String> = self.expect.iter().map(|e| e.signature(case_id)).collect();
        let mut got: Vec<String> = observed.iter().map(Failure::signature).collect();
        want.sort();
        got.sort();
        if want != got {
            let missing = multiset_difference(&want, &got);
            let extra = multiset_difference(&got, &want);
            let mut why = banner.clone().unwrap_or_default();
            why.push_str(
                "the observed failure set is not the recorded one (the comparison is set \
                 EQUALITY, so an unrecorded failure riding alongside a known gap is a FAILURE)",
            );
            if !missing.is_empty() {
                why.push_str(&format!(
                    "\n  RECORDED BUT NOT OBSERVED (is the gap retiring?): {missing:?}"
                ));
            }
            if !extra.is_empty() {
                why.push_str(&format!(
                    "\n  OBSERVED BUT NOT RECORDED (a DIFFERENT failure the gap must never \
                     hide): {extra:?}"
                ));
            }
            return Some(why);
        }

        // The signatures agree; now the one substring the mechanism keeps (see
        // `ExpectedFailure::ExportAborted`).
        for expected in self.expect {
            if let ExpectedFailure::ExportAborted { detail } = expected {
                let stderr = observed.iter().find_map(|f| match f {
                    Failure::ExportAborted { stderr, .. } => Some(stderr.as_str()),
                    _ => None,
                });
                match stderr {
                    Some(stderr) if stderr.contains(detail) => {}
                    Some(stderr) => {
                        return Some(format!(
                            "the export aborted, as recorded, but its stderr does not carry the \
                             recorded detail {detail:?} — so this is a DIFFERENT abort:\n{stderr}"
                        ))
                    }
                    // Unreachable: the signature equality above already matched
                    // an `export-aborted` entry.
                    None => {
                        return Some(
                            "an export-abort signature matched but no ExportAborted failure was \
                             observed"
                                .to_string(),
                        )
                    }
                }
            }
        }

        // Unreachable while layer (1) holds — a `precondition[…]` signature can
        // never equal any `ExpectedFailure`'s, so the set equality above must
        // already have failed. Kept because the whole point of this class is
        // that it must not depend on layer (1) alone.
        if let Some(banner) = banner {
            return Some(format!(
                "{banner}(the recorded set matched, which means an `ExpectedFailure` \
                 counterpart for a PRECONDITION has been introduced — delete it: a gap must \
                 never be able to name one)"
            ));
        }
        None
    }
}

/// The refusal banner for any observed PRECONDITION failure, or `None` when
/// there are none.
///
/// Every precondition is named, because "unconditional" is exactly the property
/// that decays silently and a reader has to be able to see WHICH check was about
/// to be skipped.
fn precondition_banner(observed: &[Failure]) -> Option<String> {
    let failed: Vec<&Failure> = observed.iter().filter(|f| f.is_precondition()).collect();
    if failed.is_empty() {
        return None;
    }
    let mut why = String::from(
        "a KnownGap can NEVER excuse a validity PRECONDITION of the comparison — it may \
         suppress only the ASSERTION it names. The following precondition(s) did not hold, so \
         there was no meaningful comparison for this gap to be about:",
    );
    for f in failed {
        why.push_str(&format!("\n  PRECONDITION NOT MET: {}", f.render()));
    }
    why.push('\n');
    why
        .push_str("Fix the precondition (or the fixture) — never record it as a known gap.\n\n");
    Some(why)
}

/// `a` minus `b`, counting duplicates (both inputs SORTED).
fn multiset_difference(a: &[String], b: &[String]) -> Vec<String> {
    let mut remaining: Vec<&String> = b.iter().collect();
    let mut out = Vec::new();
    for item in a {
        match remaining.iter().position(|r| *r == item) {
            Some(i) => {
                remaining.remove(i);
            }
            None => out.push(item.clone()),
        }
    }
    out
}

/// A recorded, issue-tracked Arrow TYPE gap for ONE column.
///
/// Same discipline as [`KnownGap`], applied to the type check:
///
/// * **Precise.** `actual` is compared to the exported type by EQUALITY (in the
///   compact `arrow_expect::render_arrow` vocabulary), so a DIFFERENT wrong type
///   on the same column is still a failure. Nothing is matched by substring.
/// * **Self-retiring.** If the column's type becomes correct, the case FAILS and
///   demands the record be deleted — a fixed gap can never go unnoticed.
/// * **Narrow.** It excuses this ONE column's type and nothing else: the column's
///   values are still compared per cell, and every other column's type is still
///   asserted.
pub struct KnownTypeGap {
    /// The column whose exported Arrow type is currently wrong.
    pub column: &'static str,
    /// The GitHub issue tracking the fix, e.g. `"#3563"`.
    pub issue: &'static str,
    /// The type the export currently produces, as `render_arrow` renders it
    /// (e.g. `"utf8"`). Compared by equality.
    pub actual: &'static str,
    /// Why the gap exists, in one line.
    pub what: &'static str,
}
