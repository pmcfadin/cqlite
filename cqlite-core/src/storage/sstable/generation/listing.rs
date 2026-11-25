//! Listing one table's generations for a [`Scope`] (requirements G1 to G4, G8, G10, G13).
//!
//! Mirrors `Directories.SSTableLister` over `LogAwareFileLister` (`cassandra-5.0.8`
//! `db/Directories.java:980-1140`, `db/lifecycle/LogAwareFileLister.java:88-120`):
//!
//! - one directory per scope, never descending into a subdirectory (`:110` drops directories);
//! - non-log files are listed first, transaction logs second (`:88-95`);
//! - a file whose name is not an SSTable component is ignored, not an error (`Directories.java:1128-1130`);
//! - a log is never returned as a component (`:1121-1122`).
//!
//! The result accounts for every file (G13): listed generation, Orphan, hidden by a named log,
//! transaction log, or ignored.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use super::error::{GenerationError, GenerationResult};
use super::id::GenerationId;
use super::model::{Component, Condition, DirectoryListing, Generation, Listing, Scope, TocPolicy};
use super::name::{check_floor, parse_component_name};
use super::txn_classify::{classify, hidden_files, match_log_name};
use crate::storage::sstable::version_gate::SsTableFormat;

const BACKUPS_SUBDIR: &str = "backups";
const SNAPSHOT_SUBDIR: &str = "snapshots";
const DATA_COMPONENT: &str = "Data.db";
const TOC_COMPONENT: &str = "TOC.txt";

/// List the generations of the table directory `table_dir` for `scope`.
///
/// This is the module's whole listing surface. A caller chooses a [`Scope`] and whether a
/// generation without `TOC.txt` is acceptable ([`TocPolicy`]), and nothing else (G10): there is no
/// predicate, format allow-list or version allow-list, so nothing is filtered on a verb's behalf.
/// The `Live` scope requires `table_dir` to exist; `Backups` and `Snapshot(tag)` list nothing when
/// their directory is absent (`Directories.getBackupsDirectoryIfExists`).
pub fn list_generations(
    table_dir: &Path,
    scope: &Scope,
    toc: TocPolicy,
) -> GenerationResult<Listing> {
    let mut listing = Listing::default();
    for (dir, must_exist) in scope_directories(table_dir, scope)? {
        let one = list_directory(&dir, must_exist)?;
        listing.live.extend(one.live);
        listing.orphans.extend(one.orphans);
        listing.hidden.extend(one.hidden);
        listing.txn_logs.extend(one.txn_logs);
        listing.ignored.extend(one.ignored);
        listing.notes.extend(one.notes);
        listing.hidden_ids.extend(one.hidden_ids);
    }
    sort_generations(&mut listing.live);
    sort_generations(&mut listing.orphans);

    if toc == TocPolicy::Refuse {
        let missing: Vec<String> = listing
            .live
            .iter()
            .filter(|g| g.has_condition(Condition::TocMissing))
            .map(Generation::base_name)
            .collect();
        if !missing.is_empty() {
            return Err(GenerationError::TocMissingRefused {
                generations: missing,
            });
        }
    }
    Ok(listing)
}

fn sort_generations(generations: &mut [Generation]) {
    // `SSTableIdFactory.COMPARATOR` first (`Directories.sortedList`); the rest only makes the
    // order deterministic where two descriptors share an id (BIG and BTI share one id space).
    generations.sort_by(|a, b| {
        a.id.cmp(&b.id)
            .then_with(|| a.format.as_str().cmp(b.format.as_str()))
            .then_with(|| a.version.cmp(&b.version))
            .then_with(|| a.directory.cmp(&b.directory))
    });
}

fn scope_directories(table_dir: &Path, scope: &Scope) -> GenerationResult<Vec<(PathBuf, bool)>> {
    Ok(match scope {
        Scope::Live => vec![(table_dir.to_path_buf(), true)],
        Scope::Backups => vec![(table_dir.join(BACKUPS_SUBDIR), false)],
        Scope::LiveAndBackups => vec![
            (table_dir.to_path_buf(), true),
            (table_dir.join(BACKUPS_SUBDIR), false),
        ],
        Scope::Snapshot(tag) => {
            if tag.is_empty() || tag == "." || tag == ".." || tag.contains(['/', '\\', '\0']) {
                return Err(GenerationError::InvalidSnapshotTag { tag: tag.clone() });
            }
            vec![(table_dir.join(SNAPSHOT_SUBDIR).join(tag), false)]
        }
    })
}

fn io_error(path: &Path, source: std::io::Error) -> GenerationError {
    GenerationError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Names of the non-directory entries of `dir`, and the entries whose names are not UTF-8 (which
/// can never be an SSTable component).
fn read_entries(dir: &Path) -> GenerationResult<(Vec<String>, Vec<PathBuf>)> {
    let mut names = Vec::new();
    let mut non_utf8 = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| io_error(dir, e))? {
        let entry = entry.map_err(|e| io_error(dir, e))?;
        let path = entry.path();
        // `isDirectory` follows symlinks; a broken link is a file.
        if path.is_dir() {
            continue;
        }
        match entry.file_name().into_string() {
            Ok(name) => names.push(name),
            Err(_) => non_utf8.push(path),
        }
    }
    names.sort();
    Ok((names, non_utf8))
}

/// List one directory.
fn list_directory(dir: &Path, must_exist: bool) -> GenerationResult<DirectoryListing> {
    let mut out = DirectoryListing {
        directory: dir.to_path_buf(),
        ..DirectoryListing::default()
    };
    if !dir.is_dir() {
        if must_exist {
            return Err(io_error(
                dir,
                std::io::Error::new(std::io::ErrorKind::NotFound, "directory does not exist"),
            ));
        }
        return Ok(out);
    }

    // Pass 1: every file that is not a transaction log.
    let (names, non_utf8) = read_entries(dir)?;
    let files: BTreeSet<String> = names
        .iter()
        .filter(|n| match_log_name(n).is_none())
        .cloned()
        .collect();
    out.ignored.extend(non_utf8);

    // Pass 2, AFTER pass 1: the transaction logs (`LogAwareFileLister.java:88-95`).
    let (names_again, _) = read_entries(dir)?;
    let logs: Vec<String> = names_again
        .into_iter()
        .filter(|n| match_log_name(n).is_some())
        .collect();
    out.txn_logs = logs.iter().map(|l| dir.join(l)).collect();

    let classification = classify(dir, &files, &logs)?;
    out.hidden = hidden_files(dir, &classification);
    out.notes = classification.notes.clone();

    // Group the visible component files by descriptor.
    type Key = (GenerationId, &'static str, String);
    let mut groups: BTreeMap<Key, Vec<Component>> = BTreeMap::new();
    for file in &files {
        let parsed = parse_component_name(file);
        if classification.hidden.contains_key(file) {
            if let Some(p) = parsed {
                out.hidden_ids.push(p.id);
            }
            continue;
        }
        match parsed {
            None => out.ignored.push(dir.join(file)),
            Some(p) => groups
                .entry((p.id, p.format.as_str(), p.version))
                .or_default()
                .push(Component {
                    name: p.component,
                    path: dir.join(file),
                }),
        }
    }

    for ((id, format_name, version), mut components) in groups {
        let Some(format) = SsTableFormat::parse(format_name) else {
            continue; // unreachable: the key was built from a parsed format
        };
        components.sort_by(|a, b| a.name.cmp(&b.name));
        let generation = build_generation(dir, version, id, format, components);
        if generation.is_live() {
            out.live.push(generation);
        } else {
            out.orphans.push(generation);
        }
    }
    out.ignored.sort();
    Ok(out)
}

/// Decide a generation's listing-time conditions (G3, G4, G9).
fn build_generation(
    dir: &Path,
    version: String,
    id: GenerationId,
    format: SsTableFormat,
    components: Vec<Component>,
) -> Generation {
    let mut conditions = BTreeSet::new();
    // G9: from the descriptor alone, before any component is touched.
    let below_floor = check_floor(&version, format).err();
    if below_floor.is_some() {
        conditions.insert(Condition::UnsupportedVersion);
    }
    // G4: a missing or zero-length Data.db is Cassandra's own "all components are orphaned"
    // (`SortedTableScrubber.java:165-180`).
    match components.iter().find(|c| c.name == DATA_COMPONENT) {
        None => {
            conditions.insert(Condition::Orphan);
        }
        Some(data) => match std::fs::metadata(&data.path) {
            Ok(m) if m.len() == 0 => {
                conditions.insert(Condition::Orphan);
            }
            Ok(_) => {}
            Err(_) => {
                conditions.insert(Condition::Unreadable);
            }
        },
    }
    // G3: kept, not excluded (`TOCComponent.java:123-145`).
    if !conditions.contains(&Condition::Orphan)
        && !components.iter().any(|c| c.name == TOC_COMPONENT)
    {
        conditions.insert(Condition::TocMissing);
    }
    Generation {
        directory: dir.to_path_buf(),
        version,
        id,
        format,
        components,
        conditions: conditions.into_iter().collect(),
        below_floor,
        metadata: Arc::new(OnceLock::new()),
    }
}
