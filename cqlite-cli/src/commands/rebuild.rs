//! `cqlite rebuild` command (issue #4197, epic #4192) — regenerate requested
//! derived SSTable components from a healthy, UNCHANGED `Data.db`. See
//! `openspec/changes/sstable-rebuild/{proposal,design}.md`.
//!
//! Wired like `salvage.rs`/`read_commitlog.rs`: operates directly on files,
//! needs no `Database`/ingestion, and is dispatched by `main.rs` BEFORE
//! database initialization.

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::generation::Generation;
use cqlite_core::storage::write_engine::rebuild::{
    rebuild_components, rebuild_generation, Component, RebuildOptions, RebuildReport,
};

use crate::cli_types::{RebuildArgs, RebuildOutFormatArg};
use crate::commands::generation_listing::{data_path, list_live, print_notes};

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
    // A bare relative `Data.db` (`parent()` is `""`) lives in the current directory.
    let cwd;
    let dir_name = if input.is_dir() {
        input.file_name()?.to_str()?
    } else {
        let parent = match input.parent()? {
            p if p.as_os_str().is_empty() => {
                cwd = std::env::current_dir().ok()?;
                cwd.as_path()
            }
            p => p,
        };
        parent.file_name()?.to_str()?
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

/// One unit of the run's plan.
enum Planned {
    /// A single `Data.db` named explicitly: `rebuild_components` resolves it through the module.
    File(PathBuf),
    /// A generation of the table directory's Live set (issue #4626, GD4).
    Listed(Generation),
}

impl Planned {
    fn data_path(&self) -> PathBuf {
        match self {
            Planned::File(p) => p.clone(),
            Planned::Listed(g) => data_path(g),
        }
    }
}

/// What the run will do, oldest generation first.
struct Plan {
    items: Vec<Planned>,
    /// Orphans (`Data.db` missing or zero-length): named in the manifest, never rebuilt.
    orphans: Vec<Generation>,
    /// `*-Data.db` files the listing ignores (not an SSTable component name): they refuse the run
    /// by name, never silently dropped (G13).
    unrecognised: Vec<PathBuf>,
}

/// Resolve `input` into the generation(s) to rebuild. A single `Data.db` file is returned as-is;
/// a directory is listed through the generation module with Scope Live, `TocMissing` accepted
/// (rebuild does not require a `TOC.txt` publication barrier the way a reader does: regenerating
/// a missing `TOC.txt` is itself one of this command's jobs). The module never recurses, so
/// `snapshots/` and `backups/` are never entered.
fn plan_generations(input: &Path) -> anyhow::Result<Plan> {
    if input.is_file() {
        return Ok(Plan {
            items: vec![Planned::File(input.to_path_buf())],
            orphans: Vec::new(),
            unrecognised: Vec::new(),
        });
    }
    if !input.is_dir() {
        return Err(anyhow::anyhow!(
            "{} is neither a file nor a directory",
            input.display()
        ));
    }
    let listing = list_live(input)?;
    print_notes("rebuild", &listing.notes);
    Ok(Plan {
        items: listing.live.into_iter().map(Planned::Listed).collect(),
        orphans: listing.orphans,
        // Other ignored files (`.gitignore`, `notes.txt`) are not SSTable-like and stay ignored.
        unrecognised: listing
            .ignored
            .into_iter()
            .filter(|p| {
                p.is_file()
                    && p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.ends_with("-Data.db"))
            })
            .collect(),
    })
}

/// The manifest entries for Orphans: named with their condition, not refused, not rebuilt, and
/// carrying no `output` (nothing was written for them). Appended AFTER every generation's entry, so
/// the leading-entries rollback bookkeeping of the run is unaffected.
fn orphan_reports(orphans: &[Generation], components: &[Component]) -> Vec<RebuildReport> {
    orphans
        .iter()
        .map(|g| {
            RebuildReport::for_unopened_generation(
                g,
                &data_path(g),
                Path::new(""),
                components,
                None,
            )
        })
        .collect()
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
        // Atomic (temp + fsync + rename + dir fsync, the staged_publish order); a failure names
        // the path and cause and fails the run (issues #4528, #4549).
        cqlite_core::storage::write_engine::rebuild::staged_publish::write_file_atomic(
            manifest_path,
            json.as_bytes(),
        )
        .map_err(|e| {
            anyhow::anyhow!("failed to write manifest {}: {e}", manifest_path.display())
        })?;
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
            // Issue #4529 (AC2): an interrupted run leaves a recognisably-named staging directory;
            // name it so the operator knows the contents are unpublished and incomplete.
            match cqlite_core::storage::write_engine::rebuild::staged_publish::find_leftover(out) {
                Some(left) => eprintln!(
                    "cqlite rebuild: --out {} is not empty: it holds a staging directory from an \
                     interrupted run ({}), whose files are unpublished and incomplete — remove it \
                     (or the whole directory) and re-run",
                    out.display(),
                    left.display()
                ),
                None => eprintln!("cqlite rebuild: --out {} is not empty", out.display()),
            }
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

    let Plan {
        items: generations,
        orphans,
        unrecognised,
    } = match plan_generations(&args.input) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("cqlite rebuild: {e:#}");
            std::process::exit(1);
        }
    };
    // GD4 / R7.4 / G13: a below-floor generation, or a `*-Data.db` the listing does not
    // recognise, refuses the whole run (R7: exit 2 = REFUSED, nothing written; there is no partial
    // code), decided from the listing BEFORE anything is built, so `--out` is never created for this refusal. The
    // other live generations are reported rolled back (nothing regenerated), the same shape R9
    // gives a rolled-back entry.
    if !unrecognised.is_empty()
        || generations
            .iter()
            .any(|p| matches!(p, Planned::Listed(g) if g.below_floor().is_some()))
    {
        let mut reports = Vec::new();
        for planned in &generations {
            let Planned::Listed(g) = planned else {
                continue;
            };
            let out_dir = if generations.len() > 1 {
                out.join(g.base_name())
            } else {
                out.clone()
            };
            let refusal = RebuildReport::unsupported_version_refusal(g);
            let refused = refusal.is_some();
            let mut report = RebuildReport::for_unopened_generation(
                g,
                &data_path(g),
                &out_dir,
                &components,
                refusal,
            );
            if !refused {
                report.mark_rolled_back();
            }
            reports.push(report);
        }
        for path in &unrecognised {
            eprintln!(
                "cqlite rebuild: refusing the run: {} is not an SSTable component name",
                path.display()
            );
            reports.push(RebuildReport::for_unrecognised_file(path, &components));
        }
        reports.extend(orphan_reports(&orphans, &components));
        if let Err(e) = render_and_write_reports(&reports, true, args) {
            eprintln!("cqlite rebuild: failed to write report: {e:#}");
            std::process::exit(1);
        }
        std::process::exit(2);
    }

    if generations.is_empty() {
        // Nothing live to rebuild. Orphans are still named in the manifest (GD4). The exit is 1:
        // R7 allows only 0 (every requested component regenerated), 2 (a generation REFUSED) and
        // 1 (a usage error, cause on stderr), with no partial-success code (design D3); nothing was
        // rebuilt and nothing was refused, so this is the usage error "the input has no live
        // generation", exactly as an empty directory already is.
        if !orphans.is_empty() {
            let reports = orphan_reports(&orphans, &components);
            if let Err(e) = render_and_write_reports(&reports, true, args) {
                eprintln!("cqlite rebuild: failed to write report: {e:#}");
            }
        }
        eprintln!(
            "cqlite rebuild: no live *-Data.db found under {}{}",
            args.input.display(),
            if orphans.is_empty() {
                ""
            } else {
                " (only Orphans, reported in the manifest and not rebuilt)"
            }
        );
        std::process::exit(1);
    }

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
    for planned in &generations {
        let data_db_path = planned.data_path();
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
        let outcome = match planned {
            Planned::File(path) => rebuild_components(path, &schema, &components, &options).await,
            Planned::Listed(g) => rebuild_generation(g, &schema, &components, &options).await,
        };
        match outcome {
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
                // roborev job 130 (Medium): this arm used to just print and exit,
                // leaving every EARLIER generation's fully-written output under
                // `--out` and writing no manifest at all. That breaks the same
                // "a refused run leaves --out empty" contract the `refused` branch
                // above honors, and spec R7.3's non-empty-`--out` refusal then
                // blocks a re-run with no hint about the orphan. Roll back exactly
                // like a refusal: remove every prior generation's out_dir, mark
                // those reports `rolled_back`, and remove this generation's own
                // out_dir too (rebuild_components may have `create_dir_all`'d it
                // before hitting this error).
                for written in &written_out_dirs {
                    if let Err(rm_err) = std::fs::remove_dir_all(written) {
                        eprintln!(
                            "cqlite rebuild: failed to roll back {} after a later \
                             generation's error: {rm_err} — remove it manually before \
                             re-running",
                            written.display()
                        );
                    }
                }
                if let Err(rm_err) = std::fs::remove_dir_all(&out_dir) {
                    // ENOENT is expected whenever rebuild_components failed before
                    // ever creating out_dir; anything else is worth naming.
                    if rm_err.kind() != std::io::ErrorKind::NotFound {
                        eprintln!(
                            "cqlite rebuild: failed to remove {} after this generation's \
                             error: {rm_err} — remove it manually before re-running",
                            out_dir.display()
                        );
                    }
                }
                let rolled_back = written_out_dirs.len();
                for report in reports.iter_mut().take(rolled_back) {
                    report.mark_rolled_back();
                }
                let is_table_dir = args.input.is_dir();
                reports.extend(orphan_reports(&orphans, &components));
                // A single-file input that fails writes nothing (R7.3 usage error): there is no
                // report to render, and "no report generated" would only be noise.
                if !reports.is_empty() {
                    if let Err(write_err) = render_and_write_reports(&reports, is_table_dir, args) {
                        eprintln!("cqlite rebuild: failed to write report: {write_err:#}");
                    }
                }
                eprintln!("cqlite rebuild: {e}");
                std::process::exit(1);
            }
        }
    }

    let is_table_dir = args.input.is_dir();
    reports.extend(orphan_reports(&orphans, &components));
    if let Err(e) = render_and_write_reports(&reports, is_table_dir, args) {
        eprintln!("cqlite rebuild: failed to write report: {e:#}");
        std::process::exit(1);
    }

    if any_refused {
        std::process::exit(2);
    }
    std::process::exit(0);
}
