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
    /// The canonicalized spelling of `path`, when it differs and could be resolved.
    ///
    /// Both spellings are kept because the two sides of a prefix test can resolve
    /// DIFFERENTLY, and the mismatch is not hypothetical: under a symlinked base
    /// path, `canonicalize` on the unreadable DIRECTORY succeeds (its parent is
    /// readable) while `canonicalize` on a held reader's FILE inside that directory
    /// FAILS with EACCES and falls back to the raw path. Comparing one form only
    /// then answers "the walk saw this path" for a path it demonstrably could not
    /// see, and the reader is removed. See
    /// [`view_was_complete_for`](DirWalk::view_was_complete_for).
    canonical: Option<PathBuf>,
    cause: Arc<Error>,
}

impl UnreadableDir {
    pub(crate) fn new(path: PathBuf, cause: Error) -> Self {
        Self {
            path,
            canonical: None,
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
    /// Tested against BOTH recorded spellings, because `path` may itself be in
    /// either form: `canonicalize` on a file inside a 0-mode directory fails and
    /// falls back to the raw path, while the directory above it canonicalizes fine.
    /// Matching one spelling only reports a COMPLETE view of a path the walk could
    /// not see — and in `refresh_tables` that removes a live reader.
    pub(crate) fn view_was_complete_for(&self, path: &Path) -> bool {
        !self.unreadable.iter().any(|d| {
            path.starts_with(&d.path)
                || d.canonical
                    .as_ref()
                    .is_some_and(|canonical| path.starts_with(canonical))
        })
    }

    /// Attach each unreadable path's canonical spelling with `canon`, so
    /// [`view_was_complete_for`](Self::view_was_complete_for) can be asked with the
    /// canonical reader paths the refresh diff uses. The RAW spelling is retained
    /// alongside rather than replaced — see [`UnreadableDir::canonical`].
    pub(crate) fn with_canonical_unreadable(&self, canon: impl Fn(&Path) -> PathBuf) -> Self {
        Self {
            data_files: self.data_files.clone(),
            unreadable: self
                .unreadable
                .iter()
                .map(|d| {
                    let canonical = canon(&d.path);
                    UnreadableDir {
                        path: d.path.clone(),
                        canonical: (canonical != d.path).then_some(canonical),
                        cause: Arc::clone(&d.cause),
                    }
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

/// Is `path` — an entry whose NAME already matched `*-Data.db` — a candidate the
/// read path may hand to `SSTableReader::open`?
///
/// The name pattern alone is not a type. `true` iff `lstat` says the entry is a
/// REGULAR FILE; a DIRECTORY named `da-9-bti-Data.db` and a SYMLINK named
/// `da-8-bti-Data.db` are both rejected. Every candidate-enumeration site in the
/// read path calls this, so the three of them cannot drift:
/// [`find_data_files`](SSTableManager::find_data_files),
/// [`discover_data_file_paths`](SSTableManager::discover_data_file_paths) and
/// `manager_open`'s `load_from_table_directories`.
///
/// # Why this exists (issue #4159 follow-on)
///
/// #4159 made a generation that cannot be READ refuse rather than silently
/// contribute nothing. With enumeration accepting anything matching the NAME, a
/// stray entry that is not an SSTable at all then made an entire HEALTHY table
/// return [`Error::UnreadableSSTable`](crate::Error::UnreadableSSTable) — a false
/// refusal on data that reads perfectly. On the pre-#4159 binary the same two
/// entries (planted by `scripts/tests/test_bti_perf_scan.sh`) only inflated the
/// reported GENERATION COUNT: `rows_scanned` stayed 468 and the exit code stayed 0.
/// #4159 turned that miscount into total loss of the table, which is strictly
/// worse.
///
/// # Why symlinks are NOT followed here, while the directory walk DOES follow them
///
/// This deliberately contradicts the policy in
/// [`discovery::scan_gap::entry_is_dir`](crate::discovery::scan_gap), and the
/// difference is not an oversight — the two answer questions about DIFFERENT OBJECT
/// KINDS:
///
/// * A symlinked keyspace or table **DIRECTORY** is a normal Cassandra layout
///   (`data_file_directories` spread across mounts), so skipping one silently loses
///   real data. `entry_is_dir` therefore uses `std::fs::metadata` and FOLLOWS the
///   link.
/// * Cassandra symlinks DIRECTORIES, never individual component files. A symlink
///   named `da-8-bti-Data.db` pointing at `da-2-bti-Data.db` is a **phantom
///   duplicate generation**: the same bytes under a second generation number, with
///   no `da-8-*-Statistics.db` companion. Following it either duplicates rows or
///   refuses on the missing companion. Neither is right, so this predicate uses
///   `symlink_metadata` and does NOT follow the link.
///
/// # A rejected entry is NOT a refusal and NOT a discovery gap
///
/// Recording either would refuse the table again and defeat the fix. A directory
/// named `X-Data.db` holds no SSTable rows, so skipping it omits nothing — unlike
/// an unreadable DIRECTORY, whose contents are unknown, and unlike a refused
/// generation, whose rows are real. This is a rejection of the CANDIDATE, before
/// any claim about readable data exists to be lost.
///
/// # An UNOBSERVABLE entry stays a candidate; a VANISHED one does not
///
/// `lstat` failing is not evidence that the entry is not an SSTable — under a table
/// directory that lost `x` it fails for every child, including real generations.
/// Answering `false` there would reinstate exactly the swallow #4159 removed, so
/// only a POSITIVE observation rejects. An entry we cannot look at stays a
/// candidate and the open path renders the authoritative verdict: it either opens
/// or is RECORDED AS A REFUSAL, which is the loud answer.
///
/// `NotFound` is the one failure that IS a positive observation — of absence. The
/// entry was listed by `read_dir` and is gone by the time it is stat'd, which on a
/// live data directory means a compaction unlinked it mid-walk. There are no rows
/// there to omit, and treating it as a candidate would make an ordinary compaction
/// race refuse the whole table — the very defect this predicate removes. Both
/// sibling probes in this module already read `NotFound` as genuine absence.
/// A DANGLING symlink is NOT this case: `lstat` does not follow the link, so it
/// succeeds and the entry is rejected as non-regular above.
///
/// # Residual (issue #4168)
///
/// If a generation's OTHER components exist but its `Data.db` is a directory or a
/// symlink, that generation is now skipped silently. That is an INCOMPLETE
/// GENERATION — a different concern, about component sets rather than about
/// candidate types — and it is not handled here.
///
/// `tokio::fs` is used directly rather than [`Platform`]'s filesystem for the same
/// reason the sibling type probe in `find_data_files` does: the platform layer
/// exposes no `symlink_metadata`, and adding one for a single caller would put the
/// FOLLOWING and NON-FOLLOWING probes behind indistinguishable names.
pub(crate) async fn is_data_db_candidate(path: &Path) -> bool {
    match tokio::fs::symlink_metadata(path).await {
        Ok(md) => md.is_file(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
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
                    // The NAME is not the type: an entry that is not a regular file
                    // is not an SSTable, and must not be opened (nor refused) as
                    // one. See `is_data_db_candidate`.
                    if is_data_db_candidate(&path).await {
                        walk.data_files.push(path);
                    } else {
                        tracing::debug!(
                            "SSTable discovery: skipping {} — it matches `*-Data.db` but \
                             is not a regular file",
                            path.display()
                        );
                    }
                    continue;
                }
                if max_depth == 0 {
                    continue;
                }
                // A `stat` failure means this entry's subtree is unknown: it may be
                // a directory full of SSTables. `.unwrap_or(false)` silently pruned
                // it; recording keeps the gap visible without losing the siblings.
                //
                // `tokio::fs::metadata` FOLLOWS symlinks; `DirEntry::file_type` does
                // not, and reports the LINK. Using `file_type` here silently skipped
                // a symlinked keyspace or table directory — a normal Cassandra layout
                // when data is spread across mounts — with no gap recorded at all,
                // i.e. the swallow this module removes, reintroduced one level down.
                // A DANGLING symlink is genuine absence (`NotFound`: nothing is at
                // the target) and is skipped without a gap; every other failure is
                // recorded.
                let is_dir = match tokio::fs::metadata(&path).await {
                    Ok(md) => md.is_dir(),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
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
                // `try_exists`, not `exists`: the latter is `metadata().is_ok()`, so
                // an unstattable base path would read as ABSENT and return a
                // COMPLETE empty walk — claiming the whole tree is knowably empty
                // when it could not be looked at.
                match self.platform.fs().try_exists(&self.base_path).await {
                    // A base path that does not exist is a COMPLETE view of an
                    // empty tree, not an unreadable one.
                    Ok(false) => return Ok(DirWalk::default()),
                    Ok(true) => {}
                    Err(e) => {
                        let cause =
                            unreadable_dir_error(&self.base_path, "stat SSTable base path", e);
                        tracing::warn!("SSTable discovery: {cause}");
                        let mut walk = DirWalk::default();
                        walk.unreadable
                            .push(UnreadableDir::new(self.base_path.clone(), cause));
                        return Ok(walk);
                    }
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
                    // Same reason as the base-path arm: an unstattable table
                    // directory must not `continue` as though it were absent, which
                    // would drop every generation under it from the refresh's
                    // discovered set — and the refresh reads "not discovered" as
                    // "removed from disk".
                    match self.platform.fs().try_exists(dir).await {
                        Ok(false) => continue,
                        Ok(true) => {}
                        Err(e) => {
                            let cause =
                                unreadable_dir_error(dir, "stat discovered table directory", e);
                            tracing::warn!("SSTable discovery: {cause}");
                            walk.unreadable.push(UnreadableDir::new(dir.clone(), cause));
                            continue;
                        }
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
                            // Same candidate test as `find_data_files`, via the same
                            // predicate: a refresh that re-admitted a directory or a
                            // symlink named `*-Data.db` would re-introduce the false
                            // refusal one generation later.
                            if fname.ends_with("-Data.db")
                                && !is_apple_double_sidecar(fname)
                                && is_data_db_candidate(&path).await
                            {
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

#[cfg(test)]
mod candidate_tests {
    use super::is_data_db_candidate;

    /// The whole truth table of [`is_data_db_candidate`], on one directory.
    ///
    /// Stated as one test over one staging so the ACCEPT and the REJECT legs cannot
    /// drift apart: a predicate that accepted nothing would satisfy every reject
    /// leg on its own.
    #[tokio::test]
    async fn only_a_regular_file_is_a_candidate() {
        let dir = tempfile::TempDir::new().expect("TempDir");
        let dir = dir.path();

        let regular = dir.join("nb-1-big-Data.db");
        std::fs::write(&regular, b"not a real SSTable, and not read here").expect("write");
        assert!(
            is_data_db_candidate(&regular).await,
            "a REGULAR FILE is a candidate — its CONTENT is the open path's business, \
             not this predicate's"
        );

        let directory = dir.join("nb-2-big-Data.db");
        std::fs::create_dir(&directory).expect("mkdir");
        assert!(
            !is_data_db_candidate(&directory).await,
            "a DIRECTORY named `*-Data.db` holds no SSTable rows"
        );

        // Missing: `read_dir` listed it and it is gone by the time it is stat'd —
        // an ordinary compaction race. Genuine absence, so not a candidate.
        assert!(
            !is_data_db_candidate(&dir.join("nb-3-big-Data.db")).await,
            "an entry that no longer exists is genuine absence, not a candidate"
        );
    }

    /// Symlinks are NOT followed here — see the predicate's docs for why this
    /// differs from the directory walk, which follows them deliberately.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_is_never_a_candidate_even_when_its_target_is_one() {
        let dir = tempfile::TempDir::new().expect("TempDir");
        let dir = dir.path();

        let target = dir.join("nb-1-big-Data.db");
        std::fs::write(&target, b"the real generation").expect("write");
        let link = dir.join("nb-8-big-Data.db");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        assert!(
            is_data_db_candidate(&target).await,
            "control: the TARGET is a candidate, so the rejection below is about the \
             LINK and not about the bytes"
        );
        assert!(
            !is_data_db_candidate(&link).await,
            "a symlink onto a sibling generation is a PHANTOM DUPLICATE GENERATION"
        );

        let dangling = dir.join("nb-9-big-Data.db");
        std::os::unix::fs::symlink(dir.join("no-such-target"), &dangling).expect("symlink");
        assert!(
            !is_data_db_candidate(&dangling).await,
            "a DANGLING symlink must be rejected as a symlink (lstat SUCCEEDS on it), \
             never mistaken for the NotFound branch"
        );
    }
}
