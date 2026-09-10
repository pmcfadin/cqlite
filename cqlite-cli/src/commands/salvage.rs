//! `cqlite salvage` command (issue #4196, epic #4192) — recover every
//! completely-decodable partition of a damaged SSTable into a fresh
//! generation, from the authoritative boundary source. See
//! `openspec/changes/sstable-salvage/{proposal,design}.md`.
//!
//! Wired like `read_commitlog.rs`: operates directly on files, needs no
//! `Database`/ingestion, and is dispatched by `main.rs` BEFORE database
//! initialization (mirroring `verify`'s short-circuit).

use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::salvage::{
    salvage_sstable, ComponentFinding, SalvageOptions, SalvageReport,
};

use crate::cli_types::{SalvageArgs, SalvageOutFormatArg};

/// Execute the `salvage` command.
///
/// Design D3's WHOLE exit-code space (0/1/2/3) is owned end-to-end by DIRECT
/// [`std::process::exit`] calls (mirroring `commands::verify::execute_verify_command`'s
/// established pattern) — this function NEVER returns an `Err` for `main.rs`'s
/// `?`/classify-error path to route: 2/3 are SUCCESSFUL outcomes with a
/// non-zero code, not error conditions, and (roborev, issue #4196) routing a
/// genuine USAGE error through `?` would have sent it through
/// `error::classify_error`'s `CliExitCode` enum instead — which has NO
/// variant equal to `1` (`Success=0, InvalidCliArgs=2, SchemaError=3,
/// DataDirError=4, QueryExecutionError=5, WriteError=6`), so an unreadable
/// `--schema` file would have exited `3` (salvage's OWN "output written with
/// losses" code) with no output and no manifest ever having existed. Every
/// fallible step is therefore matched explicitly here, never `?`-propagated:
/// * `0` — every partition, across every generation, recovered, AND every
///   published generation was actually attempted (a discovery-level SKIP —
///   an unparseable generation number, see `discover_salvage_inputs` — rules
///   this out even when every ATTEMPTED generation was itself perfect;
///   roborev, issue #4196, round-4 Medium finding 5).
/// * `3` — SOME `Data.db` was written (at least one generation produced
///   output), but not every partition of every generation was recovered —
///   genuine losses, another generation refused outright, or a published
///   generation was skipped at discovery. Check the manifest for which
///   generations wrote output: a table-dir input salvages each generation
///   SEPARATELY (D1), so "one generation refused" must not read as "nothing
///   was produced" when a sibling succeeded. Also reached by a POST-WRITE
///   failure (a later generation's `salvage_sstable` hard error, or a failed
///   `--manifest` write) once ANY earlier generation already wrote real
///   output (roborev, issue #4196, batched finding a) — see
///   [`exit_after_partial_failure`]: exit `1` there would misreport "nothing
///   written" when `--out` in fact holds a complete generation set.
/// * `2` — EVERY generation refused: no `Data.db` was written anywhere under
///   `--out`. Also reached by a post-write failure once every GATHERED
///   report (before the failure) was itself a refusal — its manifest is
///   still written (round-4 Low finding 6): distinct from `1`, which is
///   reserved for when NOTHING was gathered at all.
/// * `1` — usage error: the cause is printed to stderr and the process
///   exits directly from the failing step, OR a post-write failure struck
///   before ANY report — refused or otherwise — had been gathered.
pub async fn execute_salvage_command(schema_path: Option<&Path>, args: &SalvageArgs) {
    let Some(schema_path) = schema_path else {
        eprintln!("cqlite salvage: --schema is required (the global --schema flag)");
        std::process::exit(1);
    };

    // roborev, issue #4196, round-8 Low finding: `if let Ok(...)` silently
    // proceeded whenever `--out` existed but could not be READ at all
    // (permissions, or a non-directory file at that path) — the guard
    // exists specifically to fail closed before writing into something it
    // should not, so degrading to permissive on a read failure defeats it;
    // the subsequent writer error then surfaces as an opaque post-write
    // failure instead of this intended, named exit-1 usage error.
    match std::fs::read_dir(&args.out) {
        Ok(mut rd) => {
            if rd.next().is_some() {
                eprintln!("cqlite salvage: --out {} is not empty", args.out.display());
                std::process::exit(1);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Does not exist yet — the writer creates it. Proceed.
        }
        Err(e) => {
            eprintln!(
                "cqlite salvage: --out {} could not be read: {e}",
                args.out.display()
            );
            std::process::exit(1);
        }
    }

    // roborev, issue #4196, round-12 High finding: `load_compaction_table_schema`
    // (shared with `compact`) returns the FIRST `CREATE TABLE` in the
    // `--schema` file unconditionally — for a multi-table schema file whose
    // target table is not first (e.g. `tombstone-parity.cql`'s
    // `resurrection_gc_positive`, the 5th of 9 tables), salvage silently
    // decoded and re-encoded with the WRONG column set and reported a
    // confidently "clean" manifest, the worst failure shape for a
    // data-recovery tool. `load_compaction_table_schema_for_table` (round-13
    // Medium finding: consolidated back into `write.rs` — the ORIGINAL
    // round-12 fix duplicated the whole function locally, WITHOUT its JSON-
    // schema-file fallback branch, regressing `--schema x.json salvage`)
    // derives the target table name from the INPUT's own directory name
    // (Cassandra's `<table>-<32-hex-id>` convention) and selects the
    // MATCHING `CREATE TABLE` statement from the file, failing closed when
    // none (or more than one) matches.
    // `--table` (when named) always wins over derivation — an explicit
    // operator choice, and the only route for an input directory NOT laid
    // out in Cassandra's own `<table>-<id>` convention (common for staged/
    // synthetic test fixtures, which name directories after the SCENARIO
    // rather than the real table).
    let target_table = match &args.table {
        Some(t) => t.clone(),
        None => match table_name_from_input(&args.input) {
            Some(t) => t,
            None => {
                eprintln!(
                    "cqlite salvage: could not derive a table name from {} (expected a table \
                     directory, or a Data.db file directly under one) — name it explicitly with \
                     --table",
                    args.input.display()
                );
                std::process::exit(1);
            }
        },
    };
    let schema = match crate::commands::write::load_compaction_table_schema_for_table(
        schema_path,
        Some(&target_table),
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "cqlite salvage: failed to resolve a schema for table '{target_table}' from \
                     {}: {e:#}",
                schema_path.display()
            );
            std::process::exit(1);
        }
    };

    // The manifest shape follows the INPUT KIND (design D5: "a table-dir
    // input writes a JSON array, one entry per generation"), never the
    // COUNT of generations actually found (roborev, issue #4196) — a
    // table-dir holding exactly one generation must still emit an array, or
    // a consumer's `jq '.[] | .losses'` breaks on precisely the common
    // single-generation case.
    let is_table_dir = args.input.is_dir();
    let discovery = match discover_salvage_inputs(&args.input) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("cqlite salvage: {e:#}");
            std::process::exit(1);
        }
    };
    // roborev, issue #4196 (batched finding h; round-5 Medium extended it to
    // a missing TOC.txt): a `*-Data.db` discovery declines to attempt — an
    // unparseable generation number, or a missing publication barrier — is a
    // NAMED, SKIPPED entry, never silently dropped. Named here,
    // unconditionally, even when other generations exist to salvage.
    for skipped in &discovery.skipped {
        eprintln!(
            "cqlite salvage: skipping {} — {}; other generations are still salvaged",
            skipped.path.display(),
            skipped.reason
        );
    }
    let generations = discovery.generations;
    if generations.is_empty() {
        if discovery.skipped.is_empty() {
            eprintln!(
                "cqlite salvage: no published *-Data.db found under {} (a Data.db needs a \
                 sibling TOC.txt)",
                args.input.display()
            );
        } else {
            eprintln!(
                "cqlite salvage: no salvageable generation under {} — every *-Data.db found was \
                 skipped (see the skip messages above)",
                args.input.display()
            );
        }
        std::process::exit(1);
    }

    // roborev, issue #4196 (round-4 Medium, finding 5; moved earlier in
    // round-6 per Low finding — a discovery-level skip must be VISIBLE in
    // the manifest, but the findings were previously appended to `reports`
    // AFTER the salvage loop, so a mid-loop hard error that reaches
    // `exit_after_partial_failure` wrote the manifest from reports that
    // never got this finding at all): a discovery-level skip (an
    // unparseable generation, see `discover_salvage_inputs`'s doc) must be
    // VISIBLE in the manifest — a consumer reading only the JSON must be
    // able to see that a published generation was never attempted, not just
    // an operator reading stderr. Recorded via the EXISTING
    // `component_findings` vehicle (design D5 already declares its shape
    // generic: `{class, component, detail}`), on EVERY report as it is
    // gathered — it is a table-dir-level fact, not one specific
    // generation's. Attached to the FIRST report only (roborev, issue
    // #4196, round-13 Low finding): a table dir with G generations and S
    // skips previously replicated each skip finding onto EVERY report,
    // producing G × S identical entries across the manifest array for a
    // fact that is true of the table dir as a whole, not any one
    // generation. `generations` is non-empty here (the empty case exits
    // above), so index 0 always exists.
    let mut reports = Vec::with_capacity(generations.len());
    for (idx, input) in generations.iter().enumerate() {
        match salvage_sstable(input, &args.out, &schema, SalvageOptions::default()).await {
            Ok(mut report) => {
                if idx == 0 {
                    for skipped in &discovery.skipped {
                        report.component_findings.push(ComponentFinding {
                            class: "SkippedInputGeneration".to_string(),
                            component: skipped.path.display().to_string(),
                            detail: format!(
                                "a published *-Data.db under the same table dir was never \
                                 salvaged: {}",
                                skipped.reason
                            ),
                        });
                    }
                }
                reports.push(report);
            }
            Err(e) => {
                eprintln!(
                    "cqlite salvage: salvage failed for {}: {e:#}",
                    input.display()
                );
                exit_after_partial_failure(&reports, args, is_table_dir);
            }
        }
    }

    if let Err(e) = write_manifest_file(&reports, args, is_table_dir) {
        eprintln!(
            "cqlite salvage: failed to write manifest to {}: {e:#}",
            args.manifest
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        );
        exit_after_partial_failure(&reports, args, is_table_dir);
    }
    render_console(&reports, args, is_table_dir);

    // roborev, issue #4196: exit 2 means "no Data.db anywhere", never "some
    // generation refused" — a table-dir input salvages each generation
    // separately (D1), so one refused generation must not discard a
    // sibling's real output from a script branching on the exit code.
    let all_refused = !reports.is_empty() && reports.iter().all(|r| r.refused.is_some());
    // roborev, issue #4196 (round-4 Medium, finding 5): a run where every
    // ATTEMPTED generation recovered cleanly but a SIBLING was never
    // attempted (an unparseable-generation skip) is NOT "every partition,
    // across every generation, recovered" — the documented exit-0 contract
    // above `execute_salvage_command` — so it must NOT read as exit 0.
    let any_imperfect = reports.iter().any(report_is_imperfect) || !discovery.skipped.is_empty();
    if all_refused {
        std::process::exit(2);
    }
    if any_imperfect {
        std::process::exit(3);
    }
}

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
fn report_is_imperfect(report: &SalvageReport) -> bool {
    report.refused.is_some()
        || !report.losses.is_empty()
        || report.partitions.recovered > report.partitions.written
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
fn exit_after_partial_failure(
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

/// Derive the target table's DIRECTORY NAME from `input` — either `input`
/// itself (a table dir) or its PARENT (a single `Data.db` file) — stripping
/// a trailing `-<32-hex-id>` suffix when present, matching Cassandra's own
/// `<table>-<tableId>` directory convention. Mirrors
/// `cqlite_core::storage::sstable::snapshot_path::extract_table_name`'s
/// algorithm exactly (that module is crate-private to `cqlite-core`, so
/// reimplemented locally rather than widening its visibility for one
/// caller) — roborev, issue #4196, round-12 High finding.
fn table_name_from_input(input: &Path) -> Option<String> {
    let dir_name = if input.is_dir() {
        input.file_name()
    } else {
        input.parent().and_then(|p| p.file_name())
    }?
    .to_str()?;
    match dir_name.rsplit_once('-') {
        Some((table_name, id)) if is_table_id_suffix(id) => Some(table_name.to_string()),
        _ => Some(dir_name.to_string()),
    }
}

/// `true` iff `id` is EXACTLY 32 lowercase hex chars — a Cassandra table-id
/// suffix (same predicate as `snapshot_path::is_table_id_suffix`).
fn is_table_id_suffix(id: &str) -> bool {
    id.len() == 32
        && id
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, 'a'..='f'))
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

/// A `*-Data.db` (with a publishing `*-TOC.txt` sibling) that
/// [`discover_salvage_inputs`] declined to include because its generation
/// number could not be parsed — named rather than silently defaulted
/// (roborev, issue #4196, batched finding h).
struct SkippedInput {
    path: PathBuf,
    reason: String,
}

/// [`discover_salvage_inputs`]'s result: the generations it WILL salvage
/// (oldest first), separate from the ones it named and skipped.
struct SalvageDiscovery {
    generations: Vec<PathBuf>,
    skipped: Vec<SkippedInput>,
}

/// `args.input` is a single `Data.db` file, or a table directory whose
/// generations (BIG `nb-*-big-Data.db` AND BTI `da-*-bti-Data.db`, each with
/// a sibling `TOC.txt` publication barrier) are salvaged separately, oldest
/// generation first for deterministic output.
///
/// A file whose generation number cannot be parsed out of its name is NEVER
/// folded into the sortable set at sort key `0` (roborev, issue #4196,
/// batched finding h): that risked sorting a malformed entry AHEAD of every
/// real generation and, since `salvage_sstable` hard-erroring on it used to
/// abort the WHOLE run (fixed alongside this — see
/// `exit_after_partial_failure`), a single badly-named file could silently
/// prevent every sibling generation from ever being attempted. Such an entry
/// is instead named in [`SalvageDiscovery::skipped`] and excluded from
/// `generations`; the caller still salvages every OTHER generation.
fn discover_salvage_inputs(input: &Path) -> anyhow::Result<SalvageDiscovery> {
    use anyhow::Context;

    if input.is_file() {
        return Ok(SalvageDiscovery {
            generations: vec![input.to_path_buf()],
            skipped: Vec::new(),
        });
    }
    if !input.is_dir() {
        return Err(anyhow::anyhow!(
            "{} is neither a file nor a directory",
            input.display()
        ));
    }

    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    let mut skipped: Vec<SkippedInput> = Vec::new();
    for entry in
        std::fs::read_dir(input).with_context(|| format!("failed to read {}", input.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let family = if name.ends_with("-big-Data.db") {
            "-big"
        } else if name.ends_with("-bti-Data.db") {
            "-bti"
        } else {
            continue;
        };
        // roborev, issue #4196, round-11 Low finding: `trim_end_matches`
        // strips REPEATED trailing occurrences of the pattern — a file
        // literally named `...-Data.db-Data.db` (or any stem that itself
        // ends in `-Data.db`) would have BOTH stripped, yielding a `base`
        // that no longer names the real component prefix, so the `-TOC.txt`
        // probe below then looks for the wrong sibling. `strip_suffix`
        // removes the suffix EXACTLY ONCE, which is what `family`'s match
        // above already established this loop needs.
        let Some(base) = name.strip_suffix("-Data.db") else {
            continue;
        };
        let toc = path.with_file_name(format!("{base}-TOC.txt"));
        if !toc.exists() {
            // roborev, issue #4196 (round-5 Medium): the SAME visibility gap
            // finding 5 fixed for an unparseable generation number, left
            // open for a missing publication barrier — a very plausible
            // damage mode for this tool's own target audience. Named and
            // skipped via the SAME `SkippedInput` vehicle rather than
            // silently `continue`d, so it is recorded in the manifest and
            // forces an imperfect (exit 3) outcome instead of vanishing.
            skipped.push(SkippedInput {
                path,
                reason: format!("no sibling {base}-TOC.txt (unpublished generation)"),
            });
            continue;
        }
        match base
            .strip_suffix(family)
            .and_then(|s| s.rsplit_once('-'))
            .and_then(|(_, g)| g.parse::<u64>().ok())
        {
            Some(generation) => found.push((generation, path)),
            None => skipped.push(SkippedInput {
                path,
                reason: format!("'{name}' does not end in <family>-<integer>-Data.db"),
            }),
        }
    }
    found.sort_by_key(|(g, _)| *g);
    Ok(SalvageDiscovery {
        generations: found.into_iter().map(|(_, p)| p).collect(),
        skipped,
    })
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
fn write_manifest_file(
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
fn render_console(reports: &[SalvageReport], args: &SalvageArgs, is_table_dir: bool) {
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
    use super::{out_dir_has_data_db, report_is_imperfect};
    use cqlite_core::storage::write_engine::salvage::{
        PartitionTotals, Refusal, RefusalReason, SalvageReport,
    };
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
