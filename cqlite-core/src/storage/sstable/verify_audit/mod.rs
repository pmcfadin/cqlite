//! `cqlite verify --mode audit`: cross-component consistency audit (issue #4195).
//!
//! Where `Quick`/`Full` ask "is each component well-formed?", the audit asks
//! "do the components AGREE with each other?" and reports a CLOSED set of eight
//! named invariants as [`AuditVerdict`]: `Pass`, `Fail` (a matching
//! `VerifyFinding` is also pushed) or `Skip(<cause>)`.
//!
//! # Truthfulness rules (the point of the module)
//!
//! * A `Pass` is only ever produced by a check that RAN TO COMPLETION and
//!   AGREED. Absence of a finding is never read as agreement.
//! * An unreadable or absent input is `Skip(<cause>)` naming why, never `Pass`
//!   and never a silent zero. The component's own corruption is reported by the
//!   finding the structural check already pushed.
//! * Every check reads decoded, authoritative structures only. There is NO
//!   byte-pattern search over `Data.db` (A11; enforced by
//!   `scripts/tests/test_verify_audit_no_resync_scan.sh`).

mod bloom_bti;
mod compression_stats;
mod data_view;
mod index_checks;

#[doc(hidden)]
pub use data_view::take_header_bytes_requested;

use crate::storage::sstable::verify::{
    BtiResolvedLeaf, CompressionState, VerifyErrorClass, VerifyFinding,
};
use crate::storage::sstable::version_gate::SsTableFormat;
use std::path::PathBuf;

/// Cause string shared with the #4194 location vocabulary.
pub const BOUNDARY_SOURCE_UNREADABLE: &str = "boundary source unreadable";

/// Stable invariant ids, in report order. Never renumbered.
pub const INVARIANT_IDS: [&str; 8] = [
    "index-data-identity",
    "summary-index-correlation",
    "bloom-no-false-negative",
    "bti-trie-bounds",
    "toc-exact-listing",
    "digest-matches-data",
    "compression-coverage",
    "statistics-vs-scan",
];

/// Outcome of one audited invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditVerdict {
    /// The check ran to completion and the components agree.
    Pass,
    /// The components disagree; a matching finding is in `VerifyReport.findings`.
    Fail,
    /// Not measured, with the cause (never a pass).
    Skip(String),
}

/// One row of the checklist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    /// Stable invariant id (see [`INVARIANT_IDS`]).
    pub invariant: &'static str,
    /// Verdict for this invariant.
    pub verdict: AuditVerdict,
}

/// The closed, always-eight-row audit checklist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditSummary {
    /// Exactly one row per named invariant, in [`INVARIANT_IDS`] order.
    pub rows: [AuditRow; 8],
    /// Whether `--deep` ran (gates the `statistics-vs-scan` row).
    pub deep: bool,
}

impl AuditSummary {
    /// Number of rows that were actually MEASURED (`Pass` or `Fail`).
    pub fn measured(&self) -> usize {
        self.rows
            .iter()
            .filter(|r| !matches!(r.verdict, AuditVerdict::Skip(_)))
            .count()
    }

    /// `true` when any row failed.
    pub fn any_fail(&self) -> bool {
        self.rows.iter().any(|r| r.verdict == AuditVerdict::Fail)
    }

    /// `true` when nothing was measured: a vacuous audit is NOT a clean bill of
    /// health (affirmative-zero doctrine).
    pub fn vacuous(&self) -> bool {
        self.measured() == 0
    }

    /// Verdict of the row named `id`.
    pub fn verdict(&self, id: &str) -> Option<&AuditVerdict> {
        self.rows
            .iter()
            .filter(|r| r.invariant == id)
            .map(|r| &r.verdict)
            .next()
    }
}

/// What `verify_components` hands the audit. Built there because
/// `ComponentSet`'s fields are private to `verify.rs`.
pub(crate) struct AuditInputs<'a> {
    pub format: SsTableFormat,
    pub data_path: PathBuf,
    pub index_path: PathBuf,
    pub summary_path: PathBuf,
    pub filter_path: PathBuf,
    pub digest_path: PathBuf,
    pub deep: bool,
    pub compression: &'a CompressionState,
    pub bti_leaves: Option<&'a [BtiResolvedLeaf]>,
    /// `findings.len()` after the TOC check / digest check / BTI-or-BIG index
    /// structure check: the slices between them attribute findings to checks.
    /// Components `TOC.txt` lists (empty when there is no TOC).
    pub toc_components: &'a [String],
    pub toc_findings: std::ops::Range<usize>,
    pub digest_findings: std::ops::Range<usize>,
    pub structure_findings: std::ops::Range<usize>,
    /// `--deep` only: partitions observed by a real scan, or why it failed.
    pub observed_partitions: Option<Result<usize, String>>,
    /// `--deep`: the BTI trie-vs-scan identity cross-check failed.
    pub bti_identity_failed: bool,
    pub statistics_path: PathBuf,
    pub platform: std::sync::Arc<crate::platform::Platform>,
}

fn skip(cause: impl Into<String>) -> AuditVerdict {
    AuditVerdict::Skip(cause.into())
}

fn has_class(
    findings: &[VerifyFinding],
    range: &std::ops::Range<usize>,
    classes: &[VerifyErrorClass],
) -> bool {
    findings
        .get(range.clone())
        .is_some_and(|s| s.iter().any(|f| classes.contains(&f.class)))
}

/// Run the audit, pushing a finding for every `Fail`.
pub(crate) async fn run(input: AuditInputs<'_>, findings: &mut Vec<VerifyFinding>) -> AuditSummary {
    let big = input.format == SsTableFormat::Big;
    let not_big = || skip("not applicable: BTI format");
    let not_bti = || skip("not applicable: BIG format");

    // ---- BIG: index walk shared by invariants 1 and 2 and the BIG bloom probe
    let structure_bad = has_class(
        findings,
        &input.structure_findings,
        &[
            VerifyErrorClass::IndexEntryCorrupt,
            VerifyErrorClass::MissingComponent,
            VerifyErrorClass::BtiRootPointerCorrupt,
            VerifyErrorClass::BtiTrieCorrupt,
            VerifyErrorClass::UnexpectedEof,
        ],
    );
    let walk = if big && !structure_bad {
        Some(index_checks::walk_index(&input.index_path))
    } else {
        None
    };

    let mut view = data_view::DataView::open(&input.data_path, input.compression);

    let (identity, summary_corr) = if big {
        match &walk {
            Some(index_checks::IndexWalk::Entries(entries)) => {
                let id = index_checks::check_index_data_identity(entries, &mut view, findings);
                let sc = index_checks::check_summary_index_correlation(
                    &input.summary_path,
                    entries,
                    &input.platform,
                    findings,
                )
                .await;
                (id, sc)
            }
            Some(index_checks::IndexWalk::Unreadable) | None => (
                skip(BOUNDARY_SOURCE_UNREADABLE),
                skip(BOUNDARY_SOURCE_UNREADABLE),
            ),
        }
    } else {
        (not_big(), not_big())
    };

    // ---- invariant 3: bloom, both formats
    let bloom = if big {
        match &walk {
            Some(index_checks::IndexWalk::Entries(entries)) => {
                bloom_bti::check_bloom_big(&input.filter_path, entries, findings)
            }
            _ => skip(BOUNDARY_SOURCE_UNREADABLE),
        }
    } else {
        bloom_bti::check_bloom_bti(
            &input.filter_path,
            input.bti_leaves.filter(|_| !structure_bad),
            &mut view,
            findings,
        )
    };

    // ---- invariant 4: BTI trie
    let bti = if big {
        not_bti()
    } else {
        let v = bloom_bti::check_bti_bounds(
            input.bti_leaves.filter(|_| !structure_bad),
            &mut view,
            findings,
        );
        if input.bti_identity_failed {
            AuditVerdict::Fail
        } else {
            v
        }
    };

    // ---- invariants 5 and 6: reused verbatim from the structural checks
    let toc = if has_class(
        findings,
        &input.toc_findings,
        &[VerifyErrorClass::MissingComponent],
    ) {
        AuditVerdict::Fail
    } else if input
        .toc_components
        .iter()
        .all(|c| c == "Data.db" || c == "TOC.txt")
    {
        // A listing naming nothing but `Data.db` (and the TOC itself) agrees with the directory
        // trivially; no cross-component listing was checked, so it is unmeasured.
        skip("TOC.txt lists only Data.db: no cross-component listing to check")
    } else {
        AuditVerdict::Pass
    };
    let digest = if has_class(
        findings,
        &input.digest_findings,
        &[
            VerifyErrorClass::DigestMismatch,
            VerifyErrorClass::MissingComponent,
        ],
    ) {
        AuditVerdict::Fail
    } else if !input.digest_path.exists() {
        skip("no Digest.crc32")
    } else {
        AuditVerdict::Pass
    };

    // ---- invariants 7 and 8
    let compression = compression_stats::check_compression_coverage(
        input.compression,
        &input.data_path,
        findings,
    );
    let statistics = compression_stats::check_statistics_vs_scan(
        &input.statistics_path,
        input.deep,
        input.observed_partitions.as_ref(),
        &input.platform,
        findings,
    )
    .await;

    let verdicts = [
        identity,
        summary_corr,
        bloom,
        bti,
        toc,
        digest,
        compression,
        statistics,
    ];
    let mut it = verdicts.into_iter();
    let rows = INVARIANT_IDS.map(|id| AuditRow {
        invariant: id,
        verdict: it
            .next()
            .unwrap_or_else(|| skip("internal: missing verdict")),
    });
    AuditSummary {
        rows,
        deep: input.deep,
    }
}
