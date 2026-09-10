//! Input discovery for `cqlite salvage` (issue #4196) — resolving `args.input`
//! (a single `Data.db` file, or a table directory) into the ordered set of
//! generations to salvage, and deriving a target table name from an input's
//! own directory naming. Split out of `salvage.rs` (round 15, campsite rule
//! / epic #1116) when that file crossed the ~800-line source threshold — a
//! PURE MOVE, no behavior changed by the split.

use std::path::{Path, PathBuf};

/// A `*-Data.db` (with a publishing `*-TOC.txt` sibling) that
/// [`discover_salvage_inputs`] declined to include because its generation
/// number could not be parsed — named rather than silently defaulted
/// (roborev, issue #4196, batched finding h).
pub(super) struct SkippedInput {
    pub(super) path: PathBuf,
    pub(super) reason: String,
}

/// [`discover_salvage_inputs`]'s result: the generations it WILL salvage
/// (oldest first), separate from the ones it named and skipped.
pub(super) struct SalvageDiscovery {
    pub(super) generations: Vec<PathBuf>,
    pub(super) skipped: Vec<SkippedInput>,
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
pub(super) fn discover_salvage_inputs(input: &Path) -> anyhow::Result<SalvageDiscovery> {
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

/// Derive the target table's DIRECTORY NAME from `input` — either `input`
/// itself (a table dir) or its PARENT (a single `Data.db` file) — stripping
/// a trailing `-<32-hex-id>` suffix when present, matching Cassandra's own
/// `<table>-<tableId>` directory convention. Mirrors
/// `cqlite_core::storage::sstable::snapshot_path::extract_table_name`'s
/// algorithm exactly (that module is crate-private to `cqlite-core`, so
/// reimplemented locally rather than widening its visibility for one
/// caller) — roborev, issue #4196, round-12 High finding.
pub(super) fn table_name_from_input(input: &Path) -> Option<String> {
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
