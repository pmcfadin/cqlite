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
///   must not read as "nothing was produced" when a sibling succeeded.
/// * `2` — EVERY generation refused: no `Data.db` was written anywhere
///   under `--out`.
/// * `1` — usage error: the cause is printed to stderr and the process
///   exits directly from the failing step.
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
    let generations = match discover_salvage_inputs(&args.input) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("cqlite salvage: {e:#}");
            std::process::exit(1);
        }
    };
    if generations.is_empty() {
        eprintln!(
            "cqlite salvage: no published *-Data.db found under {} (a Data.db needs a sibling \
             TOC.txt)",
            args.input.display()
        );
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
                std::process::exit(1);
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
        std::process::exit(1);
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

/// `args.input` is a single `Data.db` file, or a table directory whose
/// generations (BIG `nb-*-big-Data.db` AND BTI `da-*-bti-Data.db`, each with
/// a sibling `TOC.txt` publication barrier) are salvaged separately, oldest
/// generation first for deterministic output.
fn discover_salvage_inputs(input: &Path) -> anyhow::Result<Vec<PathBuf>> {
    use anyhow::Context;

    if input.is_file() {
        return Ok(vec![input.to_path_buf()]);
    }
    if !input.is_dir() {
        return Err(anyhow::anyhow!(
            "{} is neither a file nor a directory",
            input.display()
        ));
    }

    let mut found: Vec<(u64, PathBuf)> = Vec::new();
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
        let generation = base
            .strip_suffix(family)
            .and_then(|s| s.rsplit_once('-'))
            .and_then(|(_, g)| g.parse::<u64>().ok())
            .unwrap_or(0);
        found.push((generation, path));
    }
    found.sort_by_key(|(g, _)| *g);
    Ok(found.into_iter().map(|(_, p)| p).collect())
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
