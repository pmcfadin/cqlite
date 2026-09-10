//! Manifest/exit-code decision logic for `cqlite salvage` (issue #4196) —
//! the imperfect-run predicate, table-dir-level finding attachment, the
//! post-write-failure exit-code fallback, and the JSON/text manifest
//! rendering. Split out of `salvage.rs` (round 15, campsite rule / epic
//! #1116) when that file crossed the ~800-line source threshold — a PURE
//! MOVE, no behavior changed by the split.

use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::salvage::{ComponentFinding, SalvageReport};

use crate::cli_types::{SalvageArgs, SalvageOutFormatArg};

use super::discovery::SkippedInput;

/// Does this ONE generation's report make the overall run imperfect (exit 3
/// rather than 0)? Factored out of `execute_salvage_command` (roborev, issue
/// #4196, round-13 Medium finding) so the exit-code predicate is unit-testable
/// directly against constructed `SalvageReport` values, without a real
/// `salvage_sstable` fixture for each case.
///
/// A report is imperfect when it refused outright, when it names a genuine
/// `Loss`, OR when `recovered > written` — a partition that decoded but
/// reconciled to nothing to write (`recover.rs`'s `Ok(None)` arm) is not a
/// `Loss` and not a `refused` report on its own, but it IS a partial silent
/// drop when it happens alongside other partitions that DID write: without
/// this third arm such a run read as "every partition recovered" and exited
/// 0, even though one partition's content silently never reached the output.
pub(super) fn report_is_imperfect(report: &SalvageReport) -> bool {
    report.refused.is_some()
        || !report.losses.is_empty()
        || report.partitions.recovered > report.partitions.written
}

/// Attach table-dir-level facts (a discovery skip, or an in-loop hard error
/// from a SIBLING generation) to the FIRST report — factored out of
/// `execute_salvage_command` (roborev, issue #4196, round-14 Medium finding)
/// so the attachment logic is unit-testable directly against constructed
/// `SalvageReport`/`SkippedInput` values, without a real fixture that can
/// force a genuine hard I/O error out of `salvage_sstable`.
///
/// Both classes are facts about the TABLE DIR as a whole, not any one
/// generation (round-13 Low finding's reasoning), so both attach to
/// `reports[0]` only — never replicated across every report. A no-op when
/// `reports` is empty (the caller is responsible for that case: with no
/// report to attach to, `execute_salvage_command` falls back to
/// `exit_after_partial_failure` instead of calling this at all).
pub(super) fn record_table_dir_level_findings(
    reports: &mut [SalvageReport],
    skipped: &[SkippedInput],
    hard_errors: &[(PathBuf, String)],
) {
    let Some(first) = reports.first_mut() else {
        return;
    };
    for s in skipped {
        first.component_findings.push(ComponentFinding {
            class: "SkippedInputGeneration".to_string(),
            component: s.path.display().to_string(),
            detail: format!(
                "a published *-Data.db under the same table dir was never salvaged: {}",
                s.reason
            ),
        });
    }
    for (path, reason) in hard_errors {
        first.component_findings.push(ComponentFinding {
            class: "SalvageGenerationFailed".to_string(),
            component: path.display().to_string(),
            detail: format!(
                "salvage hard-errored for this generation (not a classified refusal — the \
                 input itself, or the output writer, failed): {reason}"
            ),
        });
    }
}

/// A post-write failure step — a LATER generation's `salvage_sstable` error,
/// or a failed `--manifest` write — MUST NOT report exit `1` ("nothing
/// written") when an EARLIER generation already wrote a real `Data.db`, OR
/// when an earlier generation produced a manifest-worthy REFUSAL (its own
/// legitimate exit-2 outcome on its own — design D3 — which still deserves a
/// manifest, unlike a genuine "nothing gathered at all" usage error). R7/D3
/// reserve exit `1` for that LAST case only. `reports` gathered so far (from
/// the generations that DID complete before the failure) is rendered/written
/// best-effort before exiting, so the operator still gets a manifest for
/// them (roborev, issue #4196, batched finding a + round-4 Low finding 6,
/// which caught the `reports` non-empty but ALL-refused case originally
/// falling through to the empty-`reports` exit-1 branch).
pub(super) fn exit_after_partial_failure(
    reports: &[SalvageReport],
    args: &SalvageArgs,
    is_table_dir: bool,
) -> ! {
    // roborev, issue #4196 (round-5 Medium finding 2): `reports` only
    // reflects generations that returned `Ok(...)` — a generation that
    // hard-errors INSIDE `salvage_sstable` (`write_partition`/`finish`,
    // scoped out of round 4's boundary-monotonicity fix) can still have
    // written REAL, partial `Data.db` bytes before failing, invisible to
    // `reports` entirely (`SSTableWriter` opens `Data.db` lazily on the
    // FIRST `write_partition`, so a failure at partition k>0 leaves a
    // partial generation on disk). Probing `--out` directly is the only way
    // to know whether something exists there before choosing between
    // "nothing written" (1/2, which DOCUMENT a clean `--out`) and "something
    // written" (3) — trusting `reports` alone here would let a script that
    // trusts the 1/2 promise treat a dirty `--out` as untouched, and a
    // SUBSEQUENT run into the same `--out` then fails with "not empty".
    let out_has_data_db = out_dir_has_data_db(&args.out);
    if reports.is_empty() {
        if out_has_data_db {
            std::process::exit(3);
        }
        // Genuinely nothing gathered and nothing on disk — no manifest
        // exists to write.
        std::process::exit(1);
    }
    // Best-effort: the caller already reported the failure that brought us
    // here to stderr; a second failure writing/rendering what WAS gathered
    // is not separately fatal — `reports` being non-empty already means
    // there is something worth a manifest, either way below.
    let _ = write_manifest_file(reports, args, is_table_dir);
    render_console(reports, args, is_table_dir);
    let any_output_written = out_has_data_db || reports.iter().any(|r| r.refused.is_none());
    if any_output_written {
        std::process::exit(3);
    }
    // Every gathered report refused AND nothing on disk — mirrors the
    // terminal `all_refused` arm's own exit 2, reached here instead because
    // a LATER step hard-failed before that arm ran.
    std::process::exit(2);
}

/// `true` iff any `*-Data.db` exists anywhere under `out` (roborev, issue
/// #4196, round-5 Medium finding 2) — the only way [`exit_after_partial_failure`]
/// can see a partial generation `salvage_sstable` wrote before hard-erroring,
/// which never reaches `reports` (no `Ok(report)` was ever returned for it).
fn out_dir_has_data_db(out: &Path) -> bool {
    let Ok(rd) = std::fs::read_dir(out) else {
        return false;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if out_dir_has_data_db(&path) {
                return true;
            }
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

/// Render the manifest as JSON per the INPUT KIND, never the generation
/// count: `is_table_dir` -> always a JSON array (one entry per generation,
/// even when there is only one); a single `Data.db` input -> always a bare
/// object (design D5's shape).
fn manifest_json(reports: &[SalvageReport], is_table_dir: bool) -> anyhow::Result<String> {
    if is_table_dir {
        Ok(serde_json::to_string_pretty(reports)?)
    } else {
        // roborev, issue #4196, round-6 Low finding: both callers (including
        // the failure path in `exit_after_partial_failure`) can in principle
        // reach this with an empty `reports` — an unguarded `reports[0]`
        // would panic in the error path, where a panic is worst. Refuse
        // explicitly instead.
        let report = reports
            .first()
            .ok_or_else(|| anyhow::anyhow!("no salvage report to render (reports is empty)"))?;
        Ok(serde_json::to_string_pretty(report)?)
    }
}

/// Write the JSON manifest (design D5) to `--manifest`, when given.
///
/// A failure here is a HARD error (roborev, issue #4196): D5/R8 make the
/// manifest THE contract, so a run that reports 0/3 while silently failing
/// to write it — most reachably a refusal, where `--out` is never created
/// and a naive `--manifest <out>/salvage.json` invocation (as documented in
/// `--help` and `dev-cookbook.md`) then has no parent directory — must not
/// be reported as if the manifest existed. The parent directory is created
/// first so the documented "manifest lives under --out" pattern works even
/// when `--out` itself was never populated.
pub(super) fn write_manifest_file(
    reports: &[SalvageReport],
    args: &SalvageArgs,
    is_table_dir: bool,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let Some(path) = &args.manifest else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    let json = manifest_json(reports, is_table_dir).context("failed to serialize manifest")?;
    std::fs::write(path, json).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Console rendering (independent of `--manifest`): the JSON manifest to
/// stdout under `--out-format json`, else a text rendering to stderr.
pub(super) fn render_console(reports: &[SalvageReport], args: &SalvageArgs, is_table_dir: bool) {
    match args.out_format {
        SalvageOutFormatArg::Json => match manifest_json(reports, is_table_dir) {
            Ok(text) => println!("{text}"),
            Err(e) => eprintln!("cqlite salvage: failed to serialize manifest: {e}"),
        },
        SalvageOutFormatArg::Text => {
            for report in reports {
                eprint!("{}", report.render_text());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        out_dir_has_data_db, record_table_dir_level_findings, report_is_imperfect, SkippedInput,
    };
    use cqlite_core::storage::write_engine::salvage::{
        PartitionTotals, Refusal, RefusalReason, SalvageReport,
    };
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// A minimal, otherwise-clean report — every test below overrides just
    /// the field(s) under test, so a change to `SalvageReport`'s shape
    /// cannot silently make an unrelated field the reason a case passes.
    fn clean_report() -> SalvageReport {
        SalvageReport {
            input: "in".to_string(),
            output: "out".to_string(),
            format: "nb".to_string(),
            compressed_input: false,
            boundary_source: "index".to_string(),
            generation: 1,
            partitions: PartitionTotals {
                total: 1,
                recovered: 1,
                lost: 0,
                written: 1,
            },
            losses: Vec::new(),
            component_findings: Vec::new(),
            attempted: true,
            refused: None,
            now: "2026-01-01T00:00:00Z".to_string(),
            cqlite_version: "test".to_string(),
        }
    }

    #[test]
    fn clean_report_is_not_imperfect() {
        assert!(!report_is_imperfect(&clean_report()));
    }

    #[test]
    fn refused_report_is_imperfect() {
        let mut report = clean_report();
        report.refused = Some(Refusal {
            reason: RefusalReason::NothingDecodable,
            remedy: "inspect the losses above".to_string(),
        });
        assert!(report_is_imperfect(&report));
    }

    /// roborev, issue #4196, round-13 Medium finding: `recovered > written`
    /// with an otherwise-clean, non-refused report (a partition reconciled
    /// to nothing to write alongside others that DID write) must still be
    /// treated as imperfect — this is the exact case that previously exited
    /// 0 while silently dropping a partition's content.
    #[test]
    fn recovered_exceeding_written_is_imperfect_even_when_not_refused_and_no_losses() {
        let mut report = clean_report();
        report.partitions = PartitionTotals {
            total: 2,
            recovered: 2,
            lost: 0,
            written: 1,
        };
        assert!(report.refused.is_none());
        assert!(report.losses.is_empty());
        assert!(report_is_imperfect(&report));
    }

    #[test]
    fn recovered_equal_to_written_with_no_losses_is_not_imperfect() {
        let mut report = clean_report();
        report.partitions = PartitionTotals {
            total: 3,
            recovered: 3,
            lost: 0,
            written: 3,
        };
        assert!(!report_is_imperfect(&report));
    }

    /// roborev, issue #4196, round-14 Medium finding:
    /// `record_table_dir_level_findings` attaches BOTH a discovery skip and
    /// an in-loop hard error to the FIRST report only — never replicated
    /// across every report (round-13 Low finding's reasoning applies to
    /// both classes identically).
    #[test]
    fn record_table_dir_level_findings_attaches_to_first_report_only() {
        let mut reports = vec![clean_report(), clean_report(), clean_report()];
        let skipped = vec![SkippedInput {
            path: PathBuf::from("/data/nb-abc-big-Data.db"),
            reason: "unparseable generation number".to_string(),
        }];
        let hard_errors = vec![(
            PathBuf::from("/data/nb-2-big-Data.db"),
            "disk full".to_string(),
        )];
        record_table_dir_level_findings(&mut reports, &skipped, &hard_errors);

        assert_eq!(reports[0].component_findings.len(), 2);
        assert!(reports[0]
            .component_findings
            .iter()
            .any(|f| f.class == "SkippedInputGeneration"
                && f.component.contains("nb-abc-big-Data.db")));
        assert!(reports[0]
            .component_findings
            .iter()
            .any(|f| f.class == "SalvageGenerationFailed"
                && f.component.contains("nb-2-big-Data.db")
                && f.detail.contains("disk full")));
        assert!(
            reports[1].component_findings.is_empty(),
            "the SECOND report must carry no findings — the fact belongs to the table dir, \
             not this specific generation"
        );
        assert!(reports[2].component_findings.is_empty());
    }

    /// Calling with an empty `reports` slice is a documented no-op (the
    /// caller is responsible for routing that case to
    /// `exit_after_partial_failure` instead) — must not panic.
    #[test]
    fn record_table_dir_level_findings_on_empty_reports_is_a_no_op() {
        let mut reports: Vec<SalvageReport> = Vec::new();
        let hard_errors = vec![(PathBuf::from("/data/nb-1-big-Data.db"), "boom".to_string())];
        record_table_dir_level_findings(&mut reports, &[], &hard_errors);
        assert!(reports.is_empty());
    }

    /// No skips and no hard errors: the first report is left untouched.
    #[test]
    fn record_table_dir_level_findings_with_nothing_to_record_is_a_no_op() {
        let mut reports = vec![clean_report()];
        record_table_dir_level_findings(&mut reports, &[], &[]);
        assert!(reports[0].component_findings.is_empty());
    }

    /// Roborev, issue #4196 (round-5 Medium finding 2): `out_dir_has_data_db`
    /// is the probe `exit_after_partial_failure` relies on to see a partial
    /// generation `salvage_sstable` wrote before hard-erroring (invisible to
    /// `reports`, which only reflects `Ok(...)` returns) — unit-tested
    /// directly since reproducing that exact hard-error trigger end-to-end
    /// via the CLI needs a genuine I/O failure mid-write.
    #[test]
    fn empty_dir_has_no_data_db() {
        let temp = TempDir::new().expect("tempdir");
        assert!(!out_dir_has_data_db(temp.path()));
    }

    #[test]
    fn nonexistent_dir_has_no_data_db() {
        let temp = TempDir::new().expect("tempdir");
        assert!(!out_dir_has_data_db(&temp.path().join("does-not-exist")));
    }

    #[test]
    fn dir_with_only_non_data_files_has_no_data_db() {
        let temp = TempDir::new().expect("tempdir");
        std::fs::write(temp.path().join("nb-1-big-Statistics.db"), b"x").unwrap();
        std::fs::write(temp.path().join("nb-1-big-TOC.txt"), b"x").unwrap();
        assert!(!out_dir_has_data_db(temp.path()));
    }

    #[test]
    fn direct_data_db_is_found() {
        let temp = TempDir::new().expect("tempdir");
        std::fs::write(temp.path().join("nb-1-big-Data.db"), b"x").unwrap();
        assert!(out_dir_has_data_db(temp.path()));
    }

    /// The realistic case: `--out`'s writer-nested `<out>/<keyspace>/<table>/`
    /// layout — the probe must recurse.
    #[test]
    fn nested_data_db_is_found() {
        let temp = TempDir::new().expect("tempdir");
        let nested = temp.path().join("ks").join("tbl");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("nb-1-big-Data.db"), b"x").unwrap();
        assert!(out_dir_has_data_db(temp.path()));
    }
}
