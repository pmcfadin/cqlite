//! `cqlite rebuild` command (issue #4197, epic #4192) — regenerate requested
//! derived SSTable components from a healthy, UNCHANGED `Data.db`. See
//! `openspec/changes/sstable-rebuild/{proposal,design}.md`.
//!
//! Wired like `salvage.rs`/`read_commitlog.rs`: operates directly on files,
//! needs no `Database`/ingestion, and is dispatched by `main.rs` BEFORE
//! database initialization.

use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::rebuild::{
    rebuild_components, Component, RebuildOptions, RebuildReport,
};

use crate::cli_types::{RebuildArgs, RebuildOutFormatArg};

/// `true` iff `id` is EXACTLY 32 lowercase hex chars — a Cassandra table-id
/// suffix (mirrors `commands::salvage::discovery`'s own predicate, kept as a
/// separate small copy rather than widening that module's visibility for
/// one caller — see its own doc for why this whole shape exists).
fn is_table_id_suffix(id: &str) -> bool {
    id.len() == 32
        && id
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, 'a'..='f'))
}

/// Derive a target table name from `input`'s own directory name (Cassandra's
/// `<table>-<32-hex-id>` convention). Unlike `salvage`'s equivalent, this
/// does NOT walk up past a `snapshots/<tag>` layer — rebuild's committed
/// test corpus never exercises that layout, and doing so would duplicate
/// `commands::salvage::discovery`'s already-hardened logic for a case this
/// command does not need; `--table` covers any input this simpler
/// derivation cannot resolve.
fn table_name_from_input(input: &Path) -> Option<String> {
    let dir_name = if input.is_dir() {
        input.file_name()?.to_str()?
    } else {
        input.parent()?.file_name()?.to_str()?
    };
    match dir_name.rsplit_once('-') {
        Some((table_name, id)) if !table_name.is_empty() && is_table_id_suffix(id) => {
            Some(table_name.to_string())
        }
        _ => Some(dir_name.to_string()),
    }
}

/// Resolve `path` to an absolute, symlink-resolved form even when `path`
/// itself does not exist yet (`--out` is created lazily by the rebuild run) —
/// canonicalizes the nearest EXISTING ancestor and reattaches the
/// not-yet-created suffix components verbatim. Used by the `--out`
/// containment guard, which must compare real filesystem identity (symlinks
/// resolved, `..` applied), not raw argument strings.
fn resolve_existing_prefix(path: &Path) -> std::io::Result<PathBuf> {
    let mut current = path.to_path_buf();
    let mut suffix: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match current.canonicalize() {
            Ok(mut resolved) => {
                for part in suffix.into_iter().rev() {
                    resolved.push(part);
                }
                return Ok(resolved);
            }
            Err(e) => {
                let Some(name) = current.file_name().map(std::ffi::OsStr::to_os_string) else {
                    return Err(e);
                };
                suffix.push(name);
                let Some(parent) = current.parent().map(Path::to_path_buf) else {
                    return Err(e);
                };
                current = parent;
            }
        }
    }
}

/// Resolve `input` into the `Data.db` generation(s) to rebuild, oldest first.
/// A single `Data.db` file is returned as-is; a directory is scanned for
/// `*-big-Data.db` / `*-bti-Data.db` siblings (rebuild does not require a
/// `TOC.txt` publication barrier the way `salvage` does — regenerating a
/// missing `TOC.txt` is itself one of this command's jobs).
fn discover_generations(input: &Path) -> anyhow::Result<Vec<PathBuf>> {
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
    for entry in std::fs::read_dir(input)? {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let is_data = name.ends_with("-big-Data.db") || name.ends_with("-bti-Data.db");
        if !is_data {
            continue;
        }
        let generation = name
            .split('-')
            .nth(1)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        found.push((generation, path));
    }
    found.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    Ok(found.into_iter().map(|(_, p)| p).collect())
}

/// Render + (optionally) write the manifest for the WHOLE run. Mirrors
/// `salvage`'s documented shape: a single-`Data.db` input writes ONE
/// manifest object; a table-dir input (even one holding a single
/// generation) writes a JSON array, one entry per generation — so a
/// consumer's `jq '.[] | ...'` never breaks on the common single-generation
/// case (roborev precedent from #4196, carried over here deliberately).
fn render_and_write_reports(
    reports: &[RebuildReport],
    is_table_dir: bool,
    args: &RebuildArgs,
) -> anyhow::Result<()> {
    for report in reports {
        eprintln!("{}", report.render_text());
    }
    let json = if is_table_dir {
        serde_json::to_string_pretty(reports)?
    } else {
        serde_json::to_string_pretty(
            reports
                .first()
                .ok_or_else(|| anyhow::anyhow!("no report generated"))?,
        )?
    };
    if matches!(args.out_format, RebuildOutFormatArg::Json) {
        println!("{json}");
    }
    if let Some(manifest_path) = &args.manifest {
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(manifest_path, json)?;
    }
    Ok(())
}

/// Execute the `rebuild` command. Owns its own 0/1/2 exit-code space (design
/// D3) via direct `std::process::exit` calls, mirroring `salvage`'s
/// established pattern — never returns an `Err` for `main.rs` to route.
pub async fn execute_rebuild_command(schema_path: Option<&Path>, args: &RebuildArgs) {
    if args.in_place {
        eprintln!(
            "cqlite rebuild: --in-place requires verify --mode audit (issue #4195), not yet \
             available — use --out instead"
        );
        std::process::exit(1);
    }
    let Some(out) = &args.out else {
        eprintln!("cqlite rebuild: --out is required (unless --in-place, which refuses today)");
        std::process::exit(1);
    };

    // `--out` must never resolve INSIDE the input tree (mirrors `salvage`'s
    // own containment guard, roborev issue #4196 round-23 finding F5):
    // `copy_untouched_components` copies every untouched component
    // byte-for-byte, so `--out` pointed at (or inside) the input directory
    // would silently overwrite a derived component IN PLACE — exactly the
    // protocol `--in-place` gates behind `verify --mode audit` (#4195),
    // bypassed here by naming the same directory as `--out` instead. Checked
    // BEFORE anything else is attempted — nothing has been read or written
    // yet, so the refusal has nothing to undo.
    let input_root = if args.input.is_dir() {
        args.input.clone()
    } else {
        args.input
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    };
    let resolved_input = match resolve_existing_prefix(&input_root) {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "cqlite rebuild: cannot resolve input path {}: {e}",
                input_root.display()
            );
            std::process::exit(1);
        }
    };
    let resolved_out = match resolve_existing_prefix(out) {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "cqlite rebuild: cannot resolve --out path {}: {e}",
                out.display()
            );
            std::process::exit(1);
        }
    };
    if resolved_out.starts_with(&resolved_input) {
        eprintln!(
            "cqlite rebuild: --out {} resolves inside the input tree {} — rebuild must not \
             modify a byte of its input; point --out at a sibling directory or another volume",
            out.display(),
            input_root.display()
        );
        std::process::exit(1);
    }

    let Some(schema_path) = schema_path else {
        eprintln!("cqlite rebuild: --schema is required (the global --schema flag)");
        std::process::exit(1);
    };

    let components = match Component::parse_list(&args.components) {
        Ok(c) if !c.is_empty() => c,
        Ok(_) => {
            eprintln!("cqlite rebuild: --components must name at least one component");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("cqlite rebuild: {e}");
            std::process::exit(1);
        }
    };

    let target_table = match &args.table {
        Some(t) => t.clone(),
        None => match table_name_from_input(&args.input) {
            Some(t) => t,
            None => {
                eprintln!(
                    "cqlite rebuild: could not derive a table name from {} — name it explicitly \
                     with --table",
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
                "cqlite rebuild: failed to resolve a schema for table '{target_table}' from {}: \
                 {e:#}",
                schema_path.display()
            );
            std::process::exit(1);
        }
    };

    let generations = match discover_generations(&args.input) {
        Ok(g) if !g.is_empty() => g,
        Ok(_) => {
            eprintln!(
                "cqlite rebuild: no *-Data.db found under {}",
                args.input.display()
            );
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("cqlite rebuild: {e:#}");
            std::process::exit(1);
        }
    };

    let mut reports = Vec::with_capacity(generations.len());
    let mut any_refused = false;
    for data_db_path in &generations {
        let out_dir = if generations.len() > 1 {
            out.join(
                data_db_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default()
                    .trim_end_matches("-Data.db")
                    .to_string(),
            )
        } else {
            out.clone()
        };
        let options = RebuildOptions {
            out_dir,
            statistics_recovery_source: None,
        };
        match rebuild_components(data_db_path, &schema, &components, &options).await {
            Ok(report) => {
                if report.refused.is_some() {
                    any_refused = true;
                }
                reports.push(report);
            }
            Err(e) => {
                eprintln!("cqlite rebuild: {e}");
                std::process::exit(1);
            }
        }
    }

    let is_table_dir = args.input.is_dir();
    if let Err(e) = render_and_write_reports(&reports, is_table_dir, args) {
        eprintln!("cqlite rebuild: failed to write report: {e:#}");
        std::process::exit(1);
    }

    if any_refused {
        std::process::exit(2);
    }
    std::process::exit(0);
}
