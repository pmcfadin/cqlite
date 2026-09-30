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
        // `None`, NOT the bare directory name (issue #4197 F4). A directory
        // that does not follow Cassandra's `<table>-<32-hex-id>` convention
        // carries no table name to derive — returning its name anyway
        // produced a BOGUS target (`data_db_bit_flip`) that then failed
        // schema resolution with a confusing message, and made the caller's
        // dedicated "name it explicitly with --table" diagnostic
        // unreachable dead code.
        _ => None,
    }
}

/// Resolve `path` to an absolute, symlink-resolved form even when `path`
/// itself does not exist yet (`--out` is created lazily by the rebuild run) —
/// canonicalizes the nearest EXISTING ancestor and reattaches the
/// not-yet-created suffix components verbatim. Used by the `--out`
/// containment guard, which must compare real filesystem identity (symlinks
/// resolved, `..` applied), not raw argument strings.
fn resolve_existing_prefix(path: &Path) -> std::io::Result<PathBuf> {
    // roborev finding (Medium): a bare relative single-component path (e.g.
    // `rebuilt`, as opposed to `./rebuilt`) has `parent() == Some("")` —
    // walking up from `""` fails to canonicalize AND has no `file_name()`,
    // so the loop below returned `Err(NotFound)` for a perfectly valid
    // path. Anchor a relative `path` onto the current directory FIRST, so
    // every ancestor walked is a non-empty, real filesystem path.
    let mut current = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
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
    // The text rendering is ALWAYS written, to stderr, as the human trace of
    // the run; `--out-format` selects only the machine-readable (stdout)
    // rendering. Deliberately not gated on the format (issue #4197 F5): a
    // `--out-format json | jq .` pipe is unaffected either way, and
    // suppressing the human trace would remove the only console record of a
    // refusal for a caller that asked for JSON. `RebuildOutFormatArg`'s own
    // doc comments state this contract.
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
    // Spec R7.3: a non-empty `--out` is a usage error — rebuild never
    // overwrites same-named component files from an unrelated generation
    // silently.
    if let Ok(mut rd) = std::fs::read_dir(out) {
        if rd.next().is_some() {
            eprintln!("cqlite rebuild: --out {} is not empty", out.display());
            std::process::exit(1);
        }
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
    // roborev finding (Medium): generations are rebuilt sequentially and
    // each one's output was committed to disk before the next was
    // attempted — a LATER generation refusing left EARLIER ones' output
    // sitting under `--out`, contradicting this command's own documented
    // exit-2 contract ("nothing written") and design D3. Track every
    // generation's own output directory and, on the FIRST refusal, stop
    // attempting further generations and remove every already-written
    // one — `--out` ends up holding nothing whenever the run's overall
    // exit is 2, matching a single-`Data.db` input's behavior exactly.
    let mut written_out_dirs: Vec<PathBuf> = Vec::new();
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
            out_dir: out_dir.clone(),
            statistics_recovery_source: None,
        };
        match rebuild_components(data_db_path, &schema, &components, &options).await {
            Ok(report) => {
                let refused = report.refused.is_some();
                reports.push(report);
                if refused {
                    any_refused = true;
                    // roborev finding N7 (review round 2): a swallowed
                    // `remove_dir_all` error used to leave `mark_rolled_back`
                    // asserting the directory WAS removed regardless — the
                    // inverse of the bug this rollback exists to fix. Warn
                    // rather than silently continue; the manifest entry is
                    // still marked `rolled_back` below either way (a
                    // rollback was ATTEMPTED, so the manifest must never
                    // claim `regenerated` under a path we just tried to
                    // delete, whether or not the deletion fully succeeded).
                    for written in &written_out_dirs {
                        if let Err(e) = std::fs::remove_dir_all(written) {
                            eprintln!(
                                "cqlite rebuild: failed to roll back {} after a later \
                                 generation's refusal: {e} — remove it manually before \
                                 re-running",
                                written.display()
                            );
                        }
                    }
                    // roborev finding (Medium, issue #4197 F2): the manifest
                    // is rendered from `reports` REGARDLESS of the rollback
                    // above, so every entry whose output directory was just
                    // removed must say so — otherwise it advertises
                    // `regenerated: [...]` under an `output` path that no
                    // longer exists, and spec R9's consumer contract (a
                    // non-refused entry names a real, surviving path) is
                    // false. The removed generations are exactly the reports
                    // pushed BEFORE this refusing one: each successful
                    // iteration pushes its report and then records its
                    // out_dir, so `written_out_dirs.len()` is the count of
                    // leading successful reports.
                    let rolled_back = written_out_dirs.len();
                    for report in reports.iter_mut().take(rolled_back) {
                        report.mark_rolled_back();
                    }
                    break;
                }
                written_out_dirs.push(out_dir);
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
