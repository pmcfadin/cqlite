//! Keyspace-qualified table-directory resolution and live-generation resolution for
//! `cqlite explain` (#4193) and `cqlite diff` (#4201), split out of `explain.rs` for the
//! file-size ratchet.
//!
//! Which generations are live is the generation module's answer (issue #4626, GD5/GD6): Scope
//! Live over ONE directory, with a committed transaction hiding its obsolete inputs and an
//! uncommitted one its unfinished outputs, a `TocMissing` generation accepted and shown. The module
//! follows Cassandra's `Directories.SSTableLister`
//! (`git show cassandra-5.0.8:src/java/org/apache/cassandra/db/Directories.java`): `<cf dir>/snapshots/<tag>/`,
//! `<cf dir>/backups/` and secondary-index subdirectories (`.<index>`) are never part of the live
//! set, so they are never read.

use std::path::{Path, PathBuf};

use super::generation_listing::{data_path, display_name, list_live, metadata_refusal};

/// Directory names that never hold live generations of the enclosing table.
fn is_non_live_subdir(name: &str) -> bool {
    name == "snapshots" || name == "backups" || name.starts_with('.')
}

/// The live generations of one table directory, ready for a verb to use.
pub(crate) struct ResolvedGenerations {
    /// `*-Data.db` of every live generation, newest first (the order the merge expects: run
    /// index 0 is the newest).
    pub input_paths: Vec<PathBuf>,
    /// Every live generation in the module's id order (sequence ids first, then UUID ids), as the
    /// first line shows it: `nb-1-big`, or `nb-1-big (toc-missing)`.
    pub listed_names: Vec<String>,
    /// Facts the listing carries that are not generations (orphans, directory notes), for stderr.
    pub listing: cqlite_core::storage::sstable::generation::Listing,
}

/// Resolve `dir` through the generation module. `Err` is a read failure (exit 2) naming the
/// path: an unlistable or inconsistent directory, a below-floor generation, or one whose
/// `Statistics.db` cannot be trusted (R8). An empty live set is `Ok` with empty paths; the caller
/// states that in its own words.
pub(crate) fn resolve_generations(dir: &Path) -> Result<ResolvedGenerations, String> {
    let listing = list_live(dir).map_err(|error| error.to_string())?;
    let refusals: Vec<String> = listing.live.iter().filter_map(metadata_refusal).collect();
    if !refusals.is_empty() {
        return Err(format!(
            "generation(s) in {} cannot be read: {}",
            dir.display(),
            refusals.join("; ")
        ));
    }
    Ok(ResolvedGenerations {
        // The comparator's order, newest first: `SSTableIdFactory.COMPARATOR` puts every UUID id
        // after every sequence id, so a UUID generation is the "newest". The merge depends on this
        // order only as the tie-break: run index 0 wins an exact-timestamp tie, so the order
        // matters for ties and for nothing else.
        input_paths: listing.live.iter().rev().map(data_path).collect(),
        listed_names: listing.live.iter().map(display_name).collect(),
        listing,
    })
}

fn dir_name_matches_table(name: &str, table_name: &str) -> bool {
    name.eq_ignore_ascii_case(table_name)
        || name
            .strip_prefix(table_name)
            .map(|suffix| suffix.starts_with('-'))
            .unwrap_or(false)
}

/// Whether `dir` holds a live generation. A directory whose transaction state is inconsistent
/// still counts: it is a table directory, and the verb that lists it refuses it by name (exit 2).
fn has_live_generations(dir: &Path) -> Result<bool, String> {
    use cqlite_core::storage::sstable::generation::GenerationError;
    match list_live(dir) {
        Ok(listing) => Ok(!listing.live.is_empty()),
        Err(
            GenerationError::TxnInconsistent { .. }
            | GenerationError::TxnLogUnreadable { .. }
            | GenerationError::TxnLogName { .. },
        ) => Ok(true),
        Err(error) => Err(format!("could not inspect {}: {error}", dir.display())),
    }
}

/// Whether `root` is itself `<table-dir>/snapshots/<tag>` of a table named `table_name`: such a
/// directory is a table directory in its own right, its generations its own Live set (GD6.1).
fn is_snapshot_dir_of(root: &Path, table_name: &str) -> bool {
    let snapshots = root.parent();
    let table_dir = snapshots.and_then(Path::parent);
    snapshots
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        == Some("snapshots")
        && table_dir
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .is_some_and(|n| dir_name_matches_table(n, table_name))
}

/// Resolve the one table directory for `keyspace.table_name` at or below `root`.
///
/// `root` itself is accepted when it is named like the table and holds live generations (an
/// explicit table-directory path identifies itself). Below `root`, a candidate must be named
/// `<table>-<id>` AND sit directly under a directory named `keyspace` (Cassandra's
/// `<data>/<keyspace>/<table>-<id>` layout), so a same-named table of another keyspace is never
/// picked. More than one match is a named error, never a silent pick.
pub(crate) fn resolve_table_dir(
    root: &Path,
    keyspace: &str,
    table_name: &str,
) -> Result<PathBuf, String> {
    let root_matches = root
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| dir_name_matches_table(name, table_name));
    if (root_matches || is_snapshot_dir_of(root, table_name)) && has_live_generations(root)? {
        return Ok(root.to_path_buf());
    }

    let mut candidates = Vec::new();
    collect_table_dirs(root, keyspace, table_name, 8, &mut candidates)?;
    candidates.sort();
    candidates.dedup();
    match candidates.as_slice() {
        [] => Err(format!(
            "table '{keyspace}.{table_name}' has no published SSTable directory below {}",
            root.display()
        )),
        [only] => Ok(only.clone()),
        many => Err(format!(
            "table '{keyspace}.{table_name}' resolves to multiple SSTable directories: {}",
            many.iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn collect_table_dirs(
    dir: &Path,
    keyspace: &str,
    table_name: &str,
    depth: usize,
    out: &mut Vec<PathBuf>,
) -> Result<(), String> {
    if depth == 0 {
        return Ok(());
    }
    let entries = std::fs::read_dir(dir)
        .map_err(|error| format!("could not read data directory {}: {error}", dir.display()))?;
    let dir_is_keyspace = dir
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case(keyspace));
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !path.is_dir() || is_non_live_subdir(name) {
            continue;
        }
        if dir_name_matches_table(name, table_name) {
            if dir_is_keyspace && has_live_generations(&path)? {
                out.push(path);
            }
            continue;
        }
        collect_table_dirs(&path, keyspace, table_name, depth - 1, out)?;
    }
    Ok(())
}
