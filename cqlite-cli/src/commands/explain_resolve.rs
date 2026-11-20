//! Keyspace-qualified table-directory resolution and live-generation discovery for
//! `cqlite explain` (#4193), split out of `explain.rs` for the file-size ratchet.
//!
//! Live generations follow Cassandra's `Directories.SSTableLister`
//! (`git show cassandra-5.0.8:src/java/org/apache/cassandra/db/Directories.java`): live sstables
//! are the files directly in the table directory (`LifecycleTransaction.getFiles(location, ..)`
//! is a flat listing). `<cf dir>/snapshots/<tag>/` (`SNAPSHOT_SUBDIR`), `<cf dir>/backups/`
//! (`BACKUPS_SUBDIR`, only listed with `includeBackups`) and secondary-index subdirectories
//! (`.<index>`, `isSecondaryIndexFolder`) are never part of the live set, so they are never read.

use std::path::{Path, PathBuf};

/// Directory names that never hold live generations of the enclosing table.
fn is_non_live_subdir(name: &str) -> bool {
    name == "snapshots" || name == "backups" || name.starts_with('.')
}

/// The `*-Data.db` of every published live generation directly inside `dir`, newest first.
///
/// A generation is `<version>-<generation>-<format>-Data.db` with a sibling `-TOC.txt` (the
/// publication barrier). Order is generation descending (non-numeric ids sort as 0), then file
/// name descending, so equal-generation ties are deterministic regardless of `read_dir` order.
pub(crate) fn discover_live_generations(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut found: Vec<(u64, String, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let mut parts = name.splitn(4, '-');
        let (Some(version), Some(generation), Some(format), Some("Data.db")) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let toc = dir.join(format!("{version}-{generation}-{format}-TOC.txt"));
        if !toc.is_file() {
            continue;
        }
        let order = generation.parse::<u64>().unwrap_or(0);
        found.push((order, name.clone(), entry.path()));
    }
    found.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    Ok(found.into_iter().map(|(_, _, path)| path).collect())
}

fn dir_name_matches_table(name: &str, table_name: &str) -> bool {
    name.eq_ignore_ascii_case(table_name)
        || name
            .strip_prefix(table_name)
            .map(|suffix| suffix.starts_with('-'))
            .unwrap_or(false)
}

fn has_live_generations(dir: &Path) -> Result<bool, String> {
    discover_live_generations(dir)
        .map(|generations| !generations.is_empty())
        .map_err(|error| format!("could not inspect {}: {error}", dir.display()))
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
    if root_matches && has_live_generations(root)? {
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
