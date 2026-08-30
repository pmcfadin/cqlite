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
//! Containment cannot express the property actually wanted, which is EXACTNESS:
//! the observed failure set must EQUAL the recorded one. So the harness reports
//! a typed [`Failure`] per thing that went wrong, both the assert path and the
//! known-gap path consume that list, and [`KnownGap::mismatch`] compares the
//! observed MULTISET of failure signatures to the recorded one by equality. An
//! unrecorded failure accompanying a known gap is an EXTRA element, so the sets
//! differ and the case FAILS.

#![allow(dead_code)]

use super::arrow_expect::TypeMismatch;

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
    /// The harness REFUSES to answer: an unparsable declaration, a fixture that
    /// is not eligible for physical-dump parity, an unreadable file, a
    /// mis-recorded gap. Never recordable as a known gap either.
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
            Failure::Refusal(reason) => format!("refusal[{reason}]"),
        }
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
            Failure::Refusal(reason) => reason.clone(),
        }
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
    pub fn mismatch(&self, case_id: &str, observed: &[Failure]) -> Option<String> {
        if self.expect.is_empty() {
            return Some(
                "the recorded KnownGap lists NO expected failures, so it could match \
                 anything — record the exact failure set it exhibits"
                    .to_string(),
            );
        }

        let mut want: Vec<String> = self.expect.iter().map(|e| e.signature(case_id)).collect();
        let mut got: Vec<String> = observed.iter().map(Failure::signature).collect();
        want.sort();
        got.sort();
        if want != got {
            let missing = multiset_difference(&want, &got);
            let extra = multiset_difference(&got, &want);
            let mut why = String::from(
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
        None
    }
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
