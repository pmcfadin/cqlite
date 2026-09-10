//! `cqlite salvage` command (issue #4196, epic #4192) — recover every
//! completely-decodable partition of a damaged SSTable into a fresh
//! generation, from the authoritative boundary source. See
//! `openspec/changes/sstable-salvage/{proposal,design}.md`.
//!
//! Wired like `read_commitlog.rs`: operates directly on files, needs no
//! `Database`/ingestion, and is dispatched by `main.rs` BEFORE database
//! initialization (mirroring `verify`'s short-circuit).

use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::salvage::{salvage_sstable, SalvageOptions, SalvageReport};

use crate::cli_types::{SalvageArgs, SalvageOutFormatArg};
use crate::commands::write::load_compaction_table_schema;

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
/// * `0` — every partition, across every generation, recovered.
/// * `3` — SOME `Data.db` was written (at least one generation produced
///   output), but not every partition of every generation was recovered —
///   either genuine losses, or another generation refused outright. Check
///   the manifest for which generations wrote output: a table-dir input
///   salvages each generation SEPARATELY (D1), so "one generation refused"
///   must not read as "nothing was produced" when a sibling succeeded. Also
///   reached by a POST-WRITE failure (a later generation's `salvage_sstable`
///   hard error, or a failed `--manifest` write) once ANY earlier generation
///   already wrote real output (roborev, issue #4196, batched finding a) —
///   see [`exit_after_partial_failure`]: exit `1` there would misreport
///   "nothing written" when `--out` in fact holds a complete generation set.
/// * `2` — EVERY generation refused: no `Data.db` was written anywhere
///   under `--out`.
/// * `1` — usage error: the cause is printed to stderr and the process
///   exits directly from the failing step, OR a post-write failure struck
///   before any generation had written real output.
pub async fn execute_salvage_command(schema_path: Option<&Path>, args: &SalvageArgs) {
    let Some(schema_path) = schema_path else {
        eprintln!("cqlite salvage: --schema is required (the global --schema flag)");
        std::process::exit(1);
    };

    if let Ok(mut rd) = std::fs::read_dir(&args.out) {
        if rd.next().is_some() {
            eprintln!("cqlite salvage: --out {} is not empty", args.out.display());
            std::process::exit(1);
        }
    }

    let schema = match load_compaction_table_schema(schema_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "cqlite salvage: failed to load schema from {}: {e:#}",
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
    // roborev, issue #4196 (batched finding h): a filename whose generation
    // number could not be parsed is a NAMED, SKIPPED entry — never silently
    // folded into sort key `0` (which risked sorting it AHEAD of every real
    // generation and, on a hard failure there, aborting the whole run before
    // any sibling ran at all). Named here, unconditionally, even when other
    // generations exist to salvage.
    for skipped in &discovery.skipped {
        eprintln!(
            "cqlite salvage: skipping {} — could not parse a generation number from its \
             filename ({}); other generations are still salvaged",
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
                "cqlite salvage: no salvageable generation under {} — every published *-Data.db \
                 found had an unparseable generation number (see the skip messages above)",
                args.input.display()
            );
        }
        std::process::exit(1);
    }

    let mut reports = Vec::with_capacity(generations.len());
    for input in &generations {
        match salvage_sstable(input, &args.out, &schema, SalvageOptions::default()).await {
            Ok(report) => reports.push(report),
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
    let any_imperfect = reports
        .iter()
        .any(|r| r.refused.is_some() || !r.losses.is_empty());
    if all_refused {
        std::process::exit(2);
    }
    if any_imperfect {
        std::process::exit(3);
    }
}

/// A post-write failure step — a LATER generation's `salvage_sstable` error,
/// or a failed `--manifest` write — MUST NOT report exit `1` ("nothing
/// written") when an EARLIER generation already wrote a real `Data.db`.
/// R7/D3 reserve exit `1` for a genuine usage error where nothing was
/// produced; once any generation actually wrote output, this run's outcome
/// is "imperfect", not "nothing happened" — exit `3` (roborev, issue #4196,
/// batched finding a). `reports` gathered so far (from the generations that
/// DID complete before the failure) is rendered/written best-effort before
/// exiting, so the operator still gets a manifest for those.
fn exit_after_partial_failure(
    reports: &[SalvageReport],
    args: &SalvageArgs,
    is_table_dir: bool,
) -> ! {
    let any_output_written = reports.iter().any(|r| r.refused.is_none());
    if any_output_written {
        // Best-effort: the caller already reported the failure that brought
        // us here to stderr; a second failure writing/rendering what WAS
        // gathered is not separately fatal — exit 3 either way, since real
        // output already exists on disk.
        let _ = write_manifest_file(reports, args, is_table_dir);
        render_console(reports, args, is_table_dir);
        std::process::exit(3);
    }
    std::process::exit(1);
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
        let base = name.trim_end_matches("-Data.db");
        let toc = path.with_file_name(format!("{base}-TOC.txt"));
        if !toc.exists() {
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
fn manifest_json(reports: &[SalvageReport], is_table_dir: bool) -> serde_json::Result<String> {
    if is_table_dir {
        serde_json::to_string_pretty(reports)
    } else {
        serde_json::to_string_pretty(&reports[0])
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
