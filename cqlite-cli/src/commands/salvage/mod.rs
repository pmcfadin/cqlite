//! `cqlite salvage` command (issue #4196, epic #4192) — recover every
//! completely-decodable partition of a damaged SSTable into a fresh
//! generation, from the authoritative boundary source. See
//! `openspec/changes/sstable-salvage/{proposal,design}.md`.
//!
//! Wired like `read_commitlog.rs`: operates directly on files, needs no
//! `Database`/ingestion, and is dispatched by `main.rs` BEFORE database
//! initialization (mirroring `verify`'s short-circuit).
//!
//! Split into submodules (round 15, campsite rule / epic #1116) when this
//! file crossed the ~800-line source threshold — a PURE MOVE, no behavior
//! changed by the split: [`discovery`] resolves `args.input` into the
//! generations to salvage and derives a target table name; [`report`]
//! decides the exit code, attaches table-dir-level findings, and renders
//! the JSON/text manifest.

use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::salvage::{salvage_sstable, SalvageOptions};

use crate::cli_types::SalvageArgs;

mod discovery;
mod report;

use discovery::{discover_salvage_inputs, table_name_from_input};
use report::{
    exit_after_partial_failure, record_table_dir_level_findings, render_console,
    report_is_imperfect, write_manifest_file,
};

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
///   [`report::exit_after_partial_failure`]: exit `1` there would misreport
///   "nothing written" when `--out` in fact holds a complete generation set.
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
    // roborev, issue #4196, round-14 Medium finding: a hard `Err` from ONE
    // generation's `salvage_sstable` used to exit IMMEDIATELY
    // (`exit_after_partial_failure`, `-> !`), so every REMAINING generation
    // in the table dir was never even attempted — unlike a discovery-level
    // skip (named, printed, and recorded above), those generations appeared
    // NOWHERE in the manifest at all, directly contradicting the skip
    // path's own "other generations are still salvaged" promise and this
    // tool's "every partition accounted for" contract. Record the failure
    // and CONTINUE instead; every generation is now attempted regardless of
    // an earlier one's hard error, and the failure set is folded into the
    // SAME `exit_after_partial_failure` used below ONLY when literally
    // EVERY generation hard-errored (`reports` stays empty) — that specific
    // sub-case's "probe `--out`, decide 1 vs already-existing-output" logic
    // is unchanged, just reached after the loop instead of on the first
    // failure.
    let mut hard_errors: Vec<(PathBuf, String)> = Vec::new();
    let mut reports = Vec::with_capacity(generations.len());
    for input in &generations {
        match salvage_sstable(input, &args.out, &schema, SalvageOptions::default()).await {
            Ok(report) => reports.push(report),
            Err(e) => {
                eprintln!(
                    "cqlite salvage: salvage failed for {}: {e:#} — other generations are still \
                     attempted",
                    input.display()
                );
                hard_errors.push((input.clone(), format!("{e:#}")));
            }
        }
    }

    // Both discovery-level skips AND in-loop hard errors are table-dir-level
    // facts, not one specific generation's — both attach to the FIRST
    // report only (round-13 Low finding's reasoning applies identically to
    // the new hard-error class). When EVERY generation hard-errored,
    // `reports` is empty and there is nothing to attach to; that case falls
    // through to `exit_after_partial_failure` below unchanged.
    if reports.is_empty() {
        if !hard_errors.is_empty() {
            // Every generation hard-errored: no report exists anywhere to
            // attach a finding to. Mirrors the pre-round-14 immediate-exit
            // behavior exactly (probe `--out` directly; exit 3 if something
            // was still written despite every generation reporting
            // failure, else exit 1 — genuinely nothing gathered).
            exit_after_partial_failure(&reports, args, is_table_dir);
        }
    } else {
        record_table_dir_level_findings(&mut reports, &discovery.skipped, &hard_errors);
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
    // Gated on `hard_errors.is_empty()` too (round-14 Medium finding): a
    // hard-errored generation might have left a PARTIAL `Data.db` on disk
    // before failing (`exit_after_partial_failure`'s own doc names this —
    // `SSTableWriter` opens `Data.db` lazily on the first `write_partition`
    // and a later failure can still leave bytes behind), which `reports`
    // cannot see at all since that generation produced no `SalvageReport`;
    // claiming "no Data.db anywhere" without probing would be a guess, so
    // fall through to `any_imperfect` (exit 3) instead whenever a hard
    // error occurred, regardless of what the SUCCESSFUL reports say.
    let all_refused = hard_errors.is_empty()
        && !reports.is_empty()
        && reports.iter().all(|r| r.refused.is_some());
    // roborev, issue #4196 (round-4 Medium, finding 5): a run where every
    // ATTEMPTED generation recovered cleanly but a SIBLING was never
    // attempted (an unparseable-generation skip) is NOT "every partition,
    // across every generation, recovered" — the documented exit-0 contract
    // above `execute_salvage_command` — so it must NOT read as exit 0.
    // `!hard_errors.is_empty()` (round-14 Medium finding) extends this the
    // same way: a hard-errored sibling generation is every bit as much an
    // imperfect run as a refused or lossy one.
    let any_imperfect = reports.iter().any(report_is_imperfect)
        || !discovery.skipped.is_empty()
        || !hard_errors.is_empty();
    if all_refused {
        std::process::exit(2);
    }
    if any_imperfect {
        std::process::exit(3);
    }
}
