//! Manifest/exit-code decision logic for `cqlite salvage` (issue #4196) —
//! the imperfect-run predicate, table-dir-level finding attachment, the
//! post-write-failure exit-code fallback, and the JSON/text manifest
//! rendering. Split out of `salvage.rs` (round 15, campsite rule / epic
//! #1116) when that file crossed the ~800-line source threshold — a PURE
//! MOVE, no behavior changed by the split.
//!
//! `--manifest` PATH SAFETY (will the operator's chosen path destroy bytes they
//! did not name for overwriting?) lives in the sibling [`super::manifest_path`],
//! split out the same way in round 22 when the guard added there took THIS file
//! past the same threshold, over the containment mechanism shared with `--out` in
//! [`super::write_guard`] (round 23). This file's only stake in it is the
//! RE-CHECK immediately before the truncating `File::create` — see
//! [`write_manifest_file`].

use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::salvage::{ComponentFinding, SalvageReport};

use crate::cli_types::{SalvageArgs, SalvageOutFormatArg};

use super::discovery::SkippedInput;
use super::write_guard::{WriteGuard, MANIFEST_REMEDY};

/// Does this ONE generation's report make the overall run imperfect (exit 3
/// rather than 0)? Factored out of `execute_salvage_command` (roborev, issue
/// #4196, round-13 Medium finding) so the exit-code predicate is unit-testable
/// directly against constructed `SalvageReport` values, without a real
/// `salvage_sstable` fixture for each case.
///
/// A report is imperfect when it refused outright, when it names a genuine
/// `Loss`, when `recovered > written` — a partition that decoded but
/// reconciled to nothing to write (`recover.rs`'s `Ok(None)` arm) is not a
/// `Loss` and not a `refused` report on its own, but it IS a partial silent
/// drop when it happens alongside other partitions that DID write: without
/// this third arm such a run read as "every partition recovered" and exited
/// 0, even though one partition's content silently never reached the output —
/// OR when it carries a VERIFICATION-GAP finding
/// ([`is_verification_gap_class`]).
pub(super) fn report_is_imperfect(report: &SalvageReport) -> bool {
    report.refused.is_some()
        || !report.losses.is_empty()
        || report.partitions.recovered > report.partitions.written
        || report
            .component_findings
            .iter()
            .any(|f| is_verification_gap_class(&f.class))
}

/// The `component_findings` classes that mean **a verification did not run** —
/// so a report carrying one is IMPERFECT (exit 3), never a clean exit 0.
///
/// Roborev, issue #4196, round-22 Medium finding: `component_findings`
/// influenced NOTHING about the exit code. An uncompressed input with no
/// `CRC.db` (`chunks.rs`'s `uncompressed_chunk_preflight` early return) emits
/// `ChunkCrcUnavailable` and returns `chunk_size: 0, data_length: 0`, which
/// disables chunk-CRC loss detection ENTIRELY — so the run reported
/// `losses: 0 RECOGNISED` and exited 0, byte-for-byte indistinguishable from a
/// fully CRC-verified clean run. Same for the short-`CRC.db` tail. design.md
/// D3 claims "an unmeasured run cannot read as clean": true of `render_text`,
/// false of `$?`, which is what every script actually branches on. The design
/// already escalates the analogous skipped-generation anomaly from 0 to 3, so
/// this was internally inconsistent too.
///
/// # What is DELIBERATELY not in this set, and why
///
/// The predicate is "verification DID NOT RUN", not "something is imperfect
/// about the input" — the excluded classes are excluded on that distinction,
/// each for a stated reason:
///
/// * `UncompressedChunkCrcMismatch` / `ChunkDecompressionError` — verification
///   RAN and found a real failure. Its consequence for the OUTPUT is already
///   carried by the `Loss` entries for every partition the bad chunk
///   intersects, which `report_is_imperfect` covers directly; a bad chunk that
///   no partition intersects genuinely leaves "every partition recovered"
///   true and measured.
/// * `UnprovenByteParity` — a PERMANENT, input-independent property of the
///   zero-clustering-column table SHAPE (issue #4217), true of every run
///   including a perfect one. Folding it in would make exit 3 the normal
///   outcome for such a table and drain the code of its meaning.
/// * `SkippedInputGeneration` / `SalvageGenerationFailed` — table-dir-level
///   facts that `execute_salvage_command` already forces to exit 3 directly
///   from `discovery.skipped` / `hard_errors`, before ever consulting a
///   report's findings.
///
/// `UnverifiedEmptyDecode` IS in the set even though it is redundant today
/// (its `Ok(None)` arm always makes `recovered > written` as well): the
/// property "a decode that could not be cross-checked is not a clean run"
/// should not depend on that counter coincidence continuing to hold.
fn is_verification_gap_class(class: &str) -> bool {
    // `ChunkCrcUnavailable` covers BOTH chunk-CRC gaps `chunks.rs` names: a
    // wholly absent `CRC.db`, and a `CRC.db` whose entries stop short of what
    // `Data.db` needs (the unverified-tail finding).
    //
    // `UnpublishedInputGeneration` (round-22 Low finding, see
    // `record_unpublished_input_findings`): without the `-TOC.txt` publication
    // barrier salvage cannot know the input generation was ever COMPLETELY
    // written, so the run's premise is unverified — and a table-DIRECTORY input
    // already exits 3 for exactly that condition, so this keeps `$?` the same
    // for the same file whichever way it is named.
    matches!(
        class,
        "ChunkCrcUnavailable" | "UnverifiedEmptyDecode" | "UnpublishedInputGeneration"
    )
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

/// Record an EXPLICITLY-named single-file input that carries no `-TOC.txt`
/// publication barrier (roborev, issue #4196, round-22 Low finding).
///
/// The barrier is enforced for a table-DIRECTORY input (a barrier-less
/// generation there is a named `SkippedInput`, never salvaged) and OVERRIDDEN for
/// an explicit file path — salvaging an unpublished, partially-flushed
/// generation is a legitimate recovery scenario. But it must not be silent: the
/// absence lands in the manifest as an `UnpublishedInputGeneration` finding,
/// which `is_verification_gap_class` also makes an imperfect (exit 3) outcome, so
/// the exit code no longer depends on WHICH WAY the same file was named. See
/// `discover_salvage_inputs`'s doc for the full reasoning.
///
/// Attached to the FIRST report, like every other input-level fact; a no-op when
/// `reports` is empty or nothing was recorded.
pub(super) fn record_unpublished_input_findings(
    reports: &mut [SalvageReport],
    barrier_absent: &[PathBuf],
) {
    let Some(first) = reports.first_mut() else {
        return;
    };
    for path in barrier_absent {
        first.component_findings.push(ComponentFinding {
            class: "UnpublishedInputGeneration".to_string(),
            component: path.display().to_string(),
            detail:
                "this generation has no sibling -TOC.txt (the publication barrier), so salvage \
                 cannot know it was ever COMPLETELY written — it was salvaged anyway because it \
                 was named EXPLICITLY as a file (a table-directory input skips such a generation \
                 instead), and the recovered output may therefore come from an unpublished, \
                 partially-flushed generation"
                    .to_string(),
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
    guard: &WriteGuard,
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
        // roborev, issue #4196, round-15 Medium finding 5 (an Opus
        // whole-module audit): reaching THIS branch (`reports.is_empty()`)
        // is ONLY possible via `execute_salvage_command`'s "every
        // generation hard-errored inside `salvage_sstable`" path — the
        // caller's OWN loop pushes every generation into either `reports`
        // (`Ok(...)`) or `hard_errors` (`Err(...)`), never neither, so an
        // empty `reports` here provably means every one hard-errored, i.e.
        // a genuine `write_partition`/`finish()` I/O failure (disk full,
        // EACCES), not a classified boundary/component refusal (THOSE
        // always return `Ok(report)` with `report.refused` set — see this
        // whole module's design D3 philosophy — and so are never absent
        // from `reports`). `out_has_data_db == true` therefore means the
        // writer got partway through before the I/O error struck. This IS
        // exactly the case `execute_salvage_command`'s own doc comment
        // already names for exit `1` ("a post-write failure struck before
        // ANY report — refused or otherwise — had been gathered") — NOT
        // exit `3`, which the CLI's `--help`/long_about documents as
        // "check the manifest", false here: no `SalvageReport` was ever
        // constructed for the failed generation(s) (the error propagated
        // OUT of `salvage_sstable` before it could build one), so there is
        // nothing to write INTO a manifest — `write_manifest_file`/
        // `render_console` are not called below for exactly this reason
        // (a table-dir input COULD still write a technically-valid empty
        // `[]`, but a single-file input has no bare object to construct at
        // all, and printing `[]` while a real I/O failure occurred would
        // itself read as "nothing was ever attempted", which is false).
        eprintln!(
            "cqlite salvage: no manifest was produced — every generation hard-errored before a \
             report could be built (see the error(s) above); inspect {} directly for whatever \
             partial output was written",
            args.out.display()
        );
        std::process::exit(1);
    }
    // Best-effort: the caller already reported the failure that brought us
    // here to stderr; a second failure writing/rendering what WAS gathered
    // is not separately fatal — `reports` being non-empty already means
    // there is something worth a manifest, either way below.
    let _ = write_manifest_file(reports, args, is_table_dir, guard);
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
///
/// Writes DIRECTLY to `writer` via `serde_json::to_writer_pretty` (roborev,
/// issue #4196, round-15 Medium finding 4 — an Opus whole-module audit)
/// rather than building one intermediate `String` via `to_string_pretty`
/// first: with `losses` now capped per report (`recover::MAX_RESIDENT_LOSSES`),
/// a table dir's `reports: Vec<SalvageReport>` stays `O(generations)` (real
/// files under the input directory — not adversarially inflatable the way
/// a corrupt Index.db's partition count is), but the FORMER shape still
/// duplicated that whole, already-bounded structure a second time as one
/// resident pretty-printed `String` before either writing it or printing
/// it — streaming removes that second copy entirely for both the
/// `--manifest` file and the `--out-format json` console path.
fn write_manifest_json<W: std::io::Write>(
    writer: W,
    reports: &[SalvageReport],
    is_table_dir: bool,
) -> anyhow::Result<()> {
    if is_table_dir {
        serde_json::to_writer_pretty(writer, reports)?;
    } else {
        // roborev, issue #4196, round-6 Low finding: both callers (including
        // the failure path in `exit_after_partial_failure`) can in principle
        // reach this with an empty `reports` — an unguarded `reports[0]`
        // would panic in the error path, where a panic is worst. Refuse
        // explicitly instead.
        let report = reports
            .first()
            .ok_or_else(|| anyhow::anyhow!("no salvage report to render (reports is empty)"))?;
        serde_json::to_writer_pretty(writer, report)?;
    }
    Ok(())
}

/// Write the JSON manifest (design D5) to `--manifest`, when given.
///
/// The path is validated by [`super::manifest_path::validate_manifest_path`] up
/// front, before any salvage work — a `--manifest` inside the input directory
/// never reaches this function (spec R5.1: `File::create` truncates).
///
/// It is RE-CHECKED here, immediately before `File::create`, against the SAME
/// [`WriteGuard`] (roborev, issue #4196, round-23 finding F1 — HIGH, REPRODUCED
/// against the compiled binary by an independent Cassandra-format expert
/// review). The up-front check alone cannot close this: at validation time a
/// path under the run's own output directory DOES NOT EXIST YET, and a path that
/// does not exist yet is indistinguishable from one that never will — until the
/// run itself creates it. Between the two checks the salvage loop has created
/// `<--out>/<keyspace>/<table>/` and written the recovered `Data.db` into it, so
/// this is the first moment the collision is observable as a real file. Without
/// it, a perfect guard still loses to WHEN it ran: the manifest overwrote the
/// recovered `Data.db` with 607 bytes of JSON and the run reported
/// `recovered=100 lost=0`, exit 0.
///
/// A refusal here is an ordinary `Err`, which the caller already treats as a
/// manifest-write failure (exit 3 once real output exists, per
/// [`exit_after_partial_failure`]) — the recovered generation is intact and the
/// operator is told which path was refused; only the manifest is missing.
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
    guard: &WriteGuard,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let Some(path) = &args.manifest else {
        return Ok(());
    };
    // Round-23 F1: the last decision before the truncating `File::create`, not
    // just the first one at start-up — see this function's doc.
    if let Err(collision) = guard.assert_disjoint("--manifest", path, MANIFEST_REMEDY) {
        anyhow::bail!(collision);
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    let file = std::fs::File::create(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    write_manifest_json(std::io::BufWriter::new(file), reports, is_table_dir)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Console rendering (independent of `--manifest`): the JSON manifest to
/// stdout under `--out-format json`, else a text rendering to stderr.
pub(super) fn render_console(reports: &[SalvageReport], args: &SalvageArgs, is_table_dir: bool) {
    match args.out_format {
        SalvageOutFormatArg::Json => {
            let stdout = std::io::stdout();
            if let Err(e) = write_manifest_json(stdout.lock(), reports, is_table_dir) {
                eprintln!("cqlite salvage: failed to serialize manifest: {e}");
                return;
            }
            println!();
        }
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
        out_dir_has_data_db, record_table_dir_level_findings, record_unpublished_input_findings,
        report_is_imperfect, SkippedInput,
    };
    use cqlite_core::storage::write_engine::salvage::{
        ComponentFinding, PartitionTotals, Refusal, RefusalReason, SalvageReport,
    };
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// One `component_findings` entry of `class`, with placeholder
    /// component/detail — the predicate under test reads the CLASS only.
    fn finding(class: &str) -> ComponentFinding {
        ComponentFinding {
            class: class.to_string(),
            component: "CRC.db".to_string(),
            detail: "detail".to_string(),
        }
    }

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
            losses_truncated: 0,
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

    /// roborev, issue #4196, round-22 Medium finding: a report whose
    /// chunk-CRC verification NEVER RAN (no `CRC.db` for an uncompressed
    /// input, or a `CRC.db` shorter than `Data.db` needs) must be imperfect —
    /// otherwise `losses: 0 RECOGNISED` + exit 0 is indistinguishable from a
    /// fully CRC-verified clean run, and the affirmative-zero doctrine holds
    /// only for the rendered text and not for `$?`.
    #[test]
    fn chunk_crc_unavailable_finding_makes_an_otherwise_clean_report_imperfect() {
        let mut report = clean_report();
        report.component_findings = vec![finding("ChunkCrcUnavailable")];
        assert!(report.refused.is_none());
        assert!(report.losses.is_empty());
        assert_eq!(report.partitions.recovered, report.partitions.written);
        assert!(
            report_is_imperfect(&report),
            "a run whose chunk-CRC validation did not run at all must not read as clean"
        );
    }

    #[test]
    fn unverified_empty_decode_finding_makes_a_report_imperfect() {
        let mut report = clean_report();
        report.component_findings = vec![finding("UnverifiedEmptyDecode")];
        assert!(report_is_imperfect(&report));
    }

    /// Roborev, issue #4196, round-22 Low finding: an explicitly-named
    /// `Data.db` with no `-TOC.txt` publication barrier is salvaged, recorded —
    /// and imperfect, so `$?` matches what the same file gets through its table
    /// DIRECTORY (which skips it and exits 3).
    #[test]
    fn record_unpublished_input_findings_attaches_and_makes_the_report_imperfect() {
        let mut reports = vec![clean_report(), clean_report()];
        let unpublished = PathBuf::from("/data/t/nb-3-big-Data.db");
        record_unpublished_input_findings(&mut reports, std::slice::from_ref(&unpublished));

        assert_eq!(reports[0].component_findings.len(), 1);
        let f = &reports[0].component_findings[0];
        assert_eq!(f.class, "UnpublishedInputGeneration");
        assert!(
            f.component.contains("nb-3-big-Data.db"),
            "the finding must NAME the generation; got {f:?}"
        );
        assert!(
            f.detail.contains("-TOC.txt"),
            "the detail must name the missing component; got {f:?}"
        );
        assert!(
            report_is_imperfect(&reports[0]),
            "an unverifiable publication barrier must not read as a clean run"
        );
        assert!(
            reports[1].component_findings.is_empty(),
            "an input-level fact attaches to the FIRST report only"
        );
    }

    #[test]
    fn record_unpublished_input_findings_with_nothing_to_record_is_a_no_op() {
        let mut reports = vec![clean_report()];
        record_unpublished_input_findings(&mut reports, &[]);
        assert!(reports[0].component_findings.is_empty());
        assert!(!report_is_imperfect(&reports[0]));
        // And an empty `reports` must not panic.
        let mut empty: Vec<SalvageReport> = Vec::new();
        record_unpublished_input_findings(&mut empty, &[PathBuf::from("/data/t/nb-1-big-Data.db")]);
        assert!(empty.is_empty());
    }

    /// The EXCLUSIONS, pinned: `UnprovenByteParity` states a permanent,
    /// input-independent caveat about the zero-clustering-column table shape
    /// (#4217) that is true of every run including a perfect one, and
    /// `UncompressedChunkCrcMismatch` means verification RAN and found
    /// something — its output consequence already reaches the exit code
    /// through the `Loss` entries for every partition the bad chunk
    /// intersects. Folding either in would make exit 3 the normal outcome for
    /// a whole class of healthy tables and drain the code of meaning.
    #[test]
    fn non_verification_gap_findings_alone_do_not_make_a_report_imperfect() {
        for class in ["UnprovenByteParity", "UncompressedChunkCrcMismatch"] {
            let mut report = clean_report();
            report.component_findings = vec![finding(class)];
            assert!(
                !report_is_imperfect(&report),
                "{class} is deliberately NOT a verification-gap class — see \
                 `is_verification_gap_class`'s doc for why"
            );
        }
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
