//! The on-disk `*-Data.db` WALK — the one discovery both the constructors and
//! [`SSTableManager::refresh_tables`] run (split out of `sstable/mod.rs` and
//! `sstable/refresh.rs` per the campsite rule, epic #1116).
//!
//! Two entry points, one responsibility:
//!
//! * [`SSTableManager::find_data_files`] — the recursive base-path walk, used by
//!   `SSTableManager::new`'s `load_existing_sstables` and by the refresh's
//!   `DiscoverySource::BasePath` arm;
//! * [`SSTableManager::discover_data_file_paths`] — the refresh's re-discovery,
//!   which dispatches on the manager's recorded [`DiscoverySource`] so a refresh
//!   re-runs EXACTLY the discovery the manager was built with (issue #1749).
//!
//! # An unreadable directory means the walk was INCOMPLETE (issue #4159)
//!
//! Both walks used to answer a `read_dir` / `file_type` failure by pretending the
//! directory was empty (`Err(_) => return Ok(results)`, `Err(_) => continue`,
//! `.unwrap_or(false)`). An unreadable directory then contributed no `*-Data.db`
//! paths, so its SSTables were silently absent from the reader map and the scan
//! surfaces reported that absence as an EMPTY SUCCESS.
//!
//! The first fix for that made every such failure ABORT the walk — which is the
//! opposite error, and a worse one in practice. `SSTableManager::new` would then
//! fail entirely because of one inaccessible directory, and essentially every ext4
//! data volume carries a root-owned `lost+found` at mode 0700: on the most common
//! real deployment layout NO table was readable, where before every readable table
//! opened. It also contradicted this fix's own governing principle (see
//! [`refusal`](super::refusal)) — "one corrupt file must not render an unrelated
//! table unreadable" — by aborting on the per-DIRECTORY axis while recording on the
//! per-FILE one.
//!
//! So the walk RECORDS the fact instead of choosing between two wrong answers. A
//! [`DirWalk`] carries what was found AND every directory that could not be read,
//! with the original [`std::io::Error`]'s kind and message intact, which gives the
//! reader four distinguishable outcomes rather than two:
//!
//! | discovered? | walk complete? | answer |
//! |---|---|---|
//! | yes | — | `Ok(rows)` (or `Err` if a generation REFUSED — see [`refusal`](super::refusal)) |
//! | no  | yes | `Ok(empty)` — genuinely absent |
//! | no  | no  | `Err` — absence is not knowable while part of the tree is unreadable |
//!
//! The last row is the point: `lost+found` stays harmless for the case that
//! matters, because a table you actually query is discovered and opens fine. Only a
//! query for an *apparently* absent table errors while the tree is partly
//! unreadable, which is the correct fail-closed answer.
//!
//! An incomplete walk is deliberately NOT recorded as an unattributed refusal: that
//! key bears on every table (see [`refusal`](super::refusal)'s module doc), so a
//! stock `lost+found` would refuse all reads. It is a distinct fact with a distinct
//! error, [`Error::IncompleteDiscovery`](crate::Error::IncompleteDiscovery).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::MAX_SSTABLE_SCAN_DEPTH;
use super::{is_apple_double_sidecar, refresh::DiscoverySource, SSTableManager};
use crate::platform::Platform;
use crate::{Error, Result};

/// One directory (or entry) the walk could not read, with the refusal itself.
///
/// The [`Error`] is retained behind an [`Arc`] rather than rendered to a message,
/// for the same reason
/// [`RefusedSSTable`](super::refusal::RefusedSSTable) does: a caller can walk the
/// `source` chain for the authoritative cause, and its [`std::io::ErrorKind`] is
/// what distinguishes `PermissionDenied` from `NotFound`.
#[derive(Debug, Clone)]
pub(crate) struct UnreadableDir {
    path: PathBuf,
    cause: Arc<Error>,
}

impl UnreadableDir {
    pub(crate) fn new(path: PathBuf, cause: Error) -> Self {
        Self {
            path,
            cause: Arc::new(cause),
        }
    }

    /// The directory (or entry) that could not be read.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// A shared handle on the ORIGINAL failure.
    pub(crate) fn cause(&self) -> Arc<Error> {
        Arc::clone(&self.cause)
    }
}

/// What one `*-Data.db` walk found, AND whether it saw all of it.
///
/// `unreadable` empty ⇔ the walk had a complete view of the tree, which is the
/// only condition under which "not discovered" may be reported as "absent".
#[derive(Debug, Default, Clone)]
pub(crate) struct DirWalk {
    /// Every `*-Data.db` path the walk reached.
    pub(crate) data_files: Vec<PathBuf>,
    /// Every directory or entry the walk could NOT read.
    pub(crate) unreadable: Vec<UnreadableDir>,
}

impl DirWalk {
    /// Did this walk have a complete view of `path`?
    ///
    /// `false` when `path` lies inside a subtree the walk could not read, in which
    /// case the walk's SILENCE about `path` carries no information — it is neither
    /// evidence that `path` exists nor that it does not. Used by `refresh_tables`
    /// so an unreadable directory can never be mistaken for "these generations were
    /// deleted", which would drop live readers.
    pub(crate) fn view_was_complete_for(&self, path: &Path) -> bool {
        !self.unreadable.iter().any(|d| path.starts_with(&d.path))
    }

    /// Canonicalize every recorded unreadable path with `canon`, so
    /// [`view_was_complete_for`](Self::view_was_complete_for) can be asked with the
    /// canonical reader paths the refresh diff uses.
    pub(crate) fn canonicalized_unreadable(&self, canon: impl Fn(&Path) -> PathBuf) -> Self {
        Self {
            data_files: self.data_files.clone(),
            unreadable: self
                .unreadable
                .iter()
                .map(|d| UnreadableDir {
                    path: canon(&d.path),
                    cause: Arc::clone(&d.cause),
                })
                .collect(),
        }
    }
}

/// Everything currently making "this table was not discovered" an unreliable
/// statement, from BOTH sources — kept apart because they are refreshed
/// differently.
#[derive(Debug, Default, Clone)]
pub(crate) struct IncompleteDiscovery {
    /// Recorded by this manager's OWN walk. A refresh re-walks the same tree, so it
    /// replaces this wholesale: a directory that became readable stops counting,
    /// and one that just became unreadable starts.
    from_walk: Vec<UnreadableDir>,
    /// Reported by an EXTERNAL discovery (`DiscoveryService`) that handed this
    /// manager its table directories.
    ///
    /// A refresh with [`DiscoverySource::TableDirs`] re-walks only the directories
    /// it was GIVEN, so it can never re-observe — and therefore must never clear —
    /// a gap in the enumeration ABOVE them. An unreadable KEYSPACE directory means
    /// table directories that were never handed over at all, and a refresh has no
    /// way to learn they exist. Wiping these on refresh would silently restore the
    /// swallow.
    external: Vec<UnreadableDir>,
}

impl IncompleteDiscovery {
    /// Every gap, from both sources.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &UnreadableDir> {
        self.from_walk.iter().chain(self.external.iter())
    }

    /// How many directories could not be read.
    pub(crate) fn len(&self) -> usize {
        self.from_walk.len() + self.external.len()
    }

    /// Add gaps found by this manager's own walk.
    pub(crate) fn extend_from_walk(&mut self, dirs: impl IntoIterator<Item = UnreadableDir>) {
        self.from_walk.extend(dirs);
    }

    /// Replace this manager's own walk gaps (a refresh re-observed the tree),
    /// leaving externally-reported ones untouched.
    pub(crate) fn replace_from_walk(&mut self, dirs: Vec<UnreadableDir>) {
        self.from_walk = dirs;
    }

    /// Record a gap an EXTERNAL discovery reported.
    pub(crate) fn note_external(&mut self, dir: UnreadableDir) {
        self.external.push(dir);
    }
}

/// Wrap `e` for `path`, PRESERVING its [`std::io::ErrorKind`].
///
/// Shared with the manager's own table-directory walk
/// (`load_from_table_directories`) so both build the recorded cause identically
/// and cannot drift.
///
/// `io::Error::other` would flatten every kind to `Other`, making
/// `PermissionDenied` (a `lost+found`, an operator permissions mistake) and
/// `NotFound` (a directory removed mid-walk) indistinguishable to any caller that
/// matches on the kind — which is precisely the distinction an operator needs here.
pub(crate) fn unreadable_dir_error(path: &Path, what: &str, e: Error) -> Error {
    let io = to_io(e);
    Error::Io(std::io::Error::new(
        io.kind(),
        format!("Failed to {what} {}: {io}", path.display()),
    ))
}

impl SSTableManager {
    /// Recursively find all *-Data.db files up to `max_depth` levels deep,
    /// recording (never swallowing, never aborting on) any directory it could not
    /// read. See the [module docs](self) for why both of those are wrong.
    pub(super) fn find_data_files<'a>(
        platform: &'a Platform,
        dir: &'a Path,
        max_depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<DirWalk>> + Send + 'a>> {
        let dir = dir.to_path_buf();
        Box::pin(async move {
            let mut walk = DirWalk::default();

            // An unreadable directory is NOT an empty one — but it is also not a
            // reason to abandon the rest of the tree. Record and continue; the
            // caller refuses only a query whose answer this gap could change.
            let mut dir_entries = match platform.fs().read_dir(&dir).await {
                Ok(entries) => entries,
                Err(e) => {
                    let cause = unreadable_dir_error(&dir, "read SSTable directory", e);
                    tracing::warn!(
                        "SSTable discovery could not read {}: {cause}. Discovery is \
                         INCOMPLETE; queries for tables not otherwise discovered will \
                         fail closed.",
                        dir.display()
                    );
                    walk.unreadable.push(UnreadableDir::new(dir, cause));
                    return Ok(walk);
                }
            };

            loop {
                // An entry can fail MID-ITERATION (a concurrent unlink, an I/O
                // fault). The remainder of THIS directory is then unknown, so the
                // directory is recorded as unreadable and iteration stops — the
                // entries already collected stay, they are real.
                let entry = match dir_entries.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(e) => {
                        let cause = unreadable_dir_error(&dir, "read an entry of", Error::Io(e));
                        tracing::warn!("SSTable discovery: {cause}");
                        walk.unreadable.push(UnreadableDir::new(dir.clone(), cause));
                        break;
                    }
                };
                let path = entry.path();
                let Some(filename) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                // Skip macOS AppleDouble sidecars via is_apple_double_sidecar().
                // See Issue #481.
                if filename.ends_with("-Data.db") && !is_apple_double_sidecar(filename) {
                    walk.data_files.push(path);
                    continue;
                }
                if max_depth == 0 {
                    continue;
                }
                // A `stat` failure means this entry's subtree is unknown: it may be
                // a directory full of SSTables. `.unwrap_or(false)` silently pruned
                // it; recording keeps the gap visible without losing the siblings.
                let is_dir = match entry.file_type().await {
                    Ok(ft) => ft.is_dir(),
                    Err(e) => {
                        let cause = unreadable_dir_error(
                            &path,
                            "determine the file type of (while scanning for SSTables)",
                            Error::Io(e),
                        );
                        tracing::warn!("SSTable discovery: {cause}");
                        walk.unreadable.push(UnreadableDir::new(path, cause));
                        continue;
                    }
                };
                if is_dir {
                    let sub = Self::find_data_files(platform, &path, max_depth - 1).await?;
                    walk.data_files.extend(sub.data_files);
                    walk.unreadable.extend(sub.unreadable);
                }
            }

            Ok(walk)
        })
    }

    /// List the current on-disk `Data.db` paths using the manager's recorded
    /// [`DiscoverySource`] — the same discovery the manager was built with.
    pub(super) async fn discover_data_file_paths(&self) -> Result<DirWalk> {
        match &self.discovery_source {
            DiscoverySource::BasePath => {
                if !self.platform.fs().exists(&self.base_path).await? {
                    // A base path that does not exist is a COMPLETE view of an
                    // empty tree, not an unreadable one.
                    return Ok(DirWalk::default());
                }
                SSTableManager::find_data_files(
                    &self.platform,
                    &self.base_path,
                    MAX_SSTABLE_SCAN_DEPTH,
                )
                .await
            }
            DiscoverySource::TableDirs(dirs) => {
                let mut walk = DirWalk::default();
                for dir in dirs {
                    if !self.platform.fs().exists(dir).await? {
                        continue;
                    }
                    let mut entries = match self.platform.fs().read_dir(dir).await {
                        Ok(entries) => entries,
                        Err(e) => {
                            let cause =
                                unreadable_dir_error(dir, "read discovered table directory", e);
                            tracing::warn!("SSTable discovery: {cause}");
                            walk.unreadable.push(UnreadableDir::new(dir.clone(), cause));
                            continue;
                        }
                    };
                    loop {
                        let entry = match entries.next_entry().await {
                            Ok(Some(entry)) => entry,
                            Ok(None) => break,
                            Err(e) => {
                                let cause =
                                    unreadable_dir_error(dir, "read an entry of", Error::Io(e));
                                tracing::warn!("SSTable discovery: {cause}");
                                walk.unreadable.push(UnreadableDir::new(dir.clone(), cause));
                                break;
                            }
                        };
                        let path = entry.path();
                        if let Some(fname) = path.file_name().and_then(|n| n.to_str()) {
                            if fname.ends_with("-Data.db") && !is_apple_double_sidecar(fname) {
                                walk.data_files.push(path);
                            }
                        }
                    }
                }
                Ok(walk)
            }
        }
    }
}

/// Recover the underlying [`std::io::Error`] from a platform-layer [`Error`], so
/// the recorded cause keeps its [`std::io::ErrorKind`].
///
/// The platform filesystem returns `crate::Error`; when that is already
/// `Error::Io` the original kind is right there. Anything else is not an I/O error
/// at all and is preserved as its own message under `ErrorKind::Other`, which is
/// honest — there is no kind to recover.
fn to_io(e: Error) -> std::io::Error {
    match e {
        Error::Io(io) => io,
        other => std::io::Error::other(other.to_string()),
    }
}
