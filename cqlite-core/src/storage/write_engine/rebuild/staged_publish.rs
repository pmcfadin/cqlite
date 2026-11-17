//! Crash-safe publication of an SSTable generation written by `cqlite salvage` / `cqlite rebuild`
//! (issue #4529, epic #4192).
//!
//! # The defect this closes
//!
//! Cassandra 5.0.8 discovers SSTables by FILE NAME, not by `TOC.txt`:
//! `Directories.SSTableLister.getFilter` hands every file in a table directory to
//! `SSTable.tryComponentFromFilename` -> `Descriptor.fromFileWithComponent` and groups the ones that
//! parse by `Descriptor`, with no TOC requirement; `TOCComponent.loadOrCreate` then REBUILDS a missing
//! TOC from whatever component files are present. A verb that writes its components one by one under
//! their final names therefore leaves, after a crash, a final-named PARTIAL set that Cassandra
//! discovers (CQLite's own "TOC.txt last" barrier only protects CQLite readers).
//!
//! # The remedy
//!
//! 1. Every component is written under a STAGING directory next to the final location,
//!    `<root>/.cqlite-publish-<tag>.tmp/` (dot-prefixed, `.tmp`-suffixed, so it is recognisable and
//!    is never a keyspace/table directory). Cassandra's lister does not descend into subdirectories
//!    (`LogAwareFileLister` lists one directory with `Files.newDirectoryStream`), so nothing staged
//!    is discoverable.
//! 2. When the whole set is written, each staged file is fsynced, then renamed into place, and the
//!    destination directories are fsynced.
//!
//! # Publish order (and why)
//!
//! `Data.db` is renamed LAST, `TOC.txt` second to last, everything else first. At 5.0.8
//! `SSTableReaderLoadingBuilder.build` begins with
//! `checkArgument(components.contains(Components.DATA), "Data component is missing for sstable %s")`,
//! so a final-named set that lacks `Data.db` is REFUSED by Cassandra, never opened — whereas a set
//! that already had `Data.db` could be opened with whichever other components happened to be
//! present. Renames cannot be made collectively atomic, so a crash inside the rename window leaves a
//! final-named set WITHOUT `Data.db` (refused, not opened); the window never leaves an openable
//! partial set. CQLite's own barrier agrees: `TOC.txt` publishes before `Data.db`, so a CQLite
//! reader sees "TOC but no Data.db" = unpublished.
//!
//! # Leftovers
//!
//! A crash before publication leaves only the staging directory. [`StagedPublish::begin`] REFUSES
//! to run while one exists, naming it (a re-run must not interleave with an interrupted one's files);
//! the operator removes it (or the whole `--out`, which both verbs already require to be empty).

use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

/// Prefix of a staging directory name: dot-prefixed so it is hidden and can never parse as an
/// SSTable component or a keyspace/table directory.
pub const STAGE_PREFIX: &str = ".cqlite-publish-";
/// Suffix of a staging directory name (Cassandra's own `Descriptor.TMP_EXT` convention).
pub const STAGE_SUFFIX: &str = ".tmp";

/// `true` when `name` is a staging directory name this module creates.
pub fn is_stage_name(name: &str) -> bool {
    name.starts_with(STAGE_PREFIX) && name.ends_with(STAGE_SUFFIX)
}

/// The first staging directory (an interrupted run's leftover) under `root` or any directory
/// below it, if any. Searched recursively because `cqlite rebuild` on a multi-generation table
/// directory stages each generation under its own `<out>/<generation>/` subdirectory.
pub fn find_leftover(root: &Path) -> Option<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(root)
        .ok()?
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .map(|e| e.path())
        .collect();
    dirs.sort();
    for d in &dirs {
        if d.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(is_stage_name)
        {
            return Some(d.clone());
        }
    }
    dirs.iter().find_map(|d| find_leftover(d))
}

/// A generation being written under a staging directory, awaiting [`publish`](Self::publish).
#[derive(Debug)]
pub struct StagedPublish {
    root: PathBuf,
    stage: PathBuf,
    /// `root` did not exist before [`begin`](StagedPublish::begin): an unpublished run removes it
    /// again (when empty), so a refused run leaves `--out` exactly as it found it.
    created_root: bool,
    /// The stage was removed by a completed publish: `Drop` has nothing left to clean (and must
    /// not issue further, un-checked removals after the run's last checked step).
    published: bool,
    /// The nearest ancestor of `root` that existed before [`begin`](StagedPublish::begin) created it
    /// (`Some` only when `root` was created): the top of the directory chain that must be fsynced.
    anchor: Option<PathBuf>,
}

/// A filesystem operation of the publish window, reported to a test observer.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Rename(PathBuf),
    SyncDir(PathBuf),
}

fn sync_all_dirs(dirs: &[PathBuf], obs: &mut dyn FnMut(&Op)) -> Result<()> {
    for d in dirs {
        obs(&Op::SyncDir(d.clone()));
        fsync_dir(d)?;
    }
    Ok(())
}

impl StagedPublish {
    /// Create the staging directory `<root>/.cqlite-publish-<tag>.tmp/` (and `root` itself if
    /// needed). Fails closed, naming the cause, when ANY staging directory already exists under
    /// `root`: an interrupted earlier run left it.
    pub fn begin(root: &Path, tag: &str) -> Result<Self> {
        let created_root = !root.exists();
        let anchor = if created_root {
            root.ancestors()
                .skip(1)
                .find(|a| a.exists())
                .map(Path::to_path_buf)
        } else {
            None
        };
        std::fs::create_dir_all(root)?;
        if let Some(left) = find_leftover(root) {
            return Err(Error::InvalidInput(format!(
                "{} holds a staging directory from an interrupted run ({}): remove it (or the whole \
                 output directory) before re-running — its files are unpublished and incomplete",
                root.display(),
                left.display()
            )));
        }
        let stage = root.join(format!("{STAGE_PREFIX}{tag}{STAGE_SUFFIX}"));
        std::fs::create_dir(&stage)?;
        Ok(Self {
            root: root.to_path_buf(),
            stage,
            created_root,
            published: false,
            anchor,
        })
    }

    /// The directory the writers must write into (the final layout, relative to this).
    pub fn dir(&self) -> &Path {
        &self.stage
    }

    /// Fsync every staged file, refuse (before ANY rename) if a destination already exists, rename
    /// everything except `Data.db` (TOC.txt last of those), fsync the destination directories (the
    /// barrier), rename `Data.db`, fsync the directories again, then remove the staging directory.
    ///
    /// The barrier matters under power loss: without it a filesystem may persist the `Data.db` rename
    /// before the others, leaving a final-named set that HAS `Data.db` but lacks components.
    pub fn publish(mut self) -> Result<()> {
        self.publish_observed(&mut |_| {})
    }

    fn publish_observed(&mut self, obs: &mut dyn FnMut(&Op)) -> Result<()> {
        let mut staged: Vec<(PathBuf, PathBuf)> = Vec::new(); // (staged file, relative path)
        collect_files(&self.stage, &self.stage, &mut staged)?;
        if staged.is_empty() {
            return Err(Error::Storage(format!(
                "staged publish of {}: nothing was staged",
                self.stage.display()
            )));
        }
        // Publish order: everything else, then TOC.txt, then Data.db (see the module doc).
        staged.sort_by(|a, b| rank(&a.1).cmp(&rank(&b.1)).then_with(|| a.1.cmp(&b.1)));
        // Pre-flight: a collision refuses with nothing renamed (the stage is removed by `Drop`).
        for (_, rel) in &staged {
            let dest = self.root.join(rel);
            if dest.symlink_metadata().is_ok() {
                return Err(Error::Storage(format!(
                    "refusing to publish over an existing {}",
                    dest.display()
                )));
            }
        }
        for (file, _) in &staged {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(file)?
                .sync_all()
                .map_err(|e| {
                    Error::Storage(format!("failed to fsync staged {}: {e}", file.display()))
                })?;
        }
        // Create the destination directories; every directory from a destination up to the sync
        // floor is fsynced so the new entries (`out/ks`, `out/ks/t`, a created `out`) are durable.
        let floor: &Path = self.anchor.as_deref().unwrap_or(&self.root);
        let mut sync_dirs: Vec<PathBuf> = Vec::new();
        for (_, rel) in &staged {
            if let Some(parent) = self.root.join(rel).parent() {
                std::fs::create_dir_all(parent)?;
                for anc in parent.ancestors() {
                    if !anc.starts_with(floor) {
                        break;
                    }
                    if !sync_dirs.iter().any(|d| d == anc) {
                        sync_dirs.push(anc.to_path_buf());
                    }
                }
            }
        }
        let data_start = staged.iter().position(|(_, r)| rank(r) == 2);
        let split = data_start.unwrap_or(staged.len());
        for (renamed, (file, rel)) in staged.iter().enumerate() {
            if renamed == split {
                // Barrier: every non-Data.db rename is durable before Data.db can appear.
                sync_all_dirs(&sync_dirs, obs)?;
            }
            test_abort_before_rename(renamed);
            let dest = self.root.join(rel);
            obs(&Op::Rename(dest.clone()));
            std::fs::rename(file, &dest).map_err(|e| {
                Error::Storage(format!(
                    "failed to publish {} -> {}: {e}",
                    file.display(),
                    dest.display()
                ))
            })?;
        }
        test_abort_before_rename(staged.len());
        sync_all_dirs(&sync_dirs, obs)?;
        remove_stage_tree(&self.stage)?;
        self.published = true;
        obs(&Op::SyncDir(self.root.clone()));
        fsync_dir(&self.root)
    }

    /// Abandon the generation: remove the staging directory (best effort).
    pub fn discard(self) {
        // `Drop` does the removal.
    }
}

/// A `StagedPublish` that is dropped without being published (an early `return`, a `?`, a refused
/// run) removes its staging directory, so only a hard kill (SIGKILL, power loss) can leave one.
impl Drop for StagedPublish {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let _ = std::fs::remove_dir_all(&self.stage);
        if self.created_root {
            let _ = std::fs::remove_dir(&self.root); // only if empty (i.e. nothing was published)
        }
    }
}

/// Debug-build-only crash seam for the #4529 tests: `CQLITE_TEST_PUBLISH_ABORT=<n>` aborts the
/// process (no unwinding, no `Drop`: a faithful stand-in for SIGKILL) just before the `n`-th rename
/// of the publish window (`n == 0`: after everything is staged and fsynced, before any rename;
/// `n == <file count>`: after the last rename). Compiled out of release builds.
#[cfg(debug_assertions)]
fn test_abort_before_rename(renamed: usize) {
    if let Ok(v) = std::env::var("CQLITE_TEST_PUBLISH_ABORT") {
        if v.parse::<usize>() == Ok(renamed) {
            std::process::abort();
        }
    }
}
#[cfg(not(debug_assertions))]
fn test_abort_before_rename(_renamed: usize) {}

fn rank(rel: &Path) -> u8 {
    let name = rel.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if name.ends_with("-Data.db") {
        2
    } else if name.ends_with("-TOC.txt") {
        1
    } else {
        0
    }
}

fn collect_files(base: &Path, dir: &Path, out: &mut Vec<(PathBuf, PathBuf)>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_files(base, &path, out)?;
        } else {
            let rel = path
                .strip_prefix(base)
                .map_err(|e| Error::Storage(format!("staged path outside its stage: {e}")))?
                .to_path_buf();
            out.push((path, rel));
        }
    }
    Ok(())
}

/// Remove the (now empty of files) staging tree bottom-up.
fn remove_stage_tree(dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            remove_stage_tree(&path)?;
        }
    }
    std::fs::remove_dir(dir)?;
    Ok(())
}

fn fsync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| {
                Error::Storage(format!("failed to fsync directory {}: {e}", dir.display()))
            })?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage_set(stage: &Path, base: &str, comps: &[&str]) {
        std::fs::create_dir_all(stage.join("ks").join("t")).unwrap();
        for c in comps {
            std::fs::write(stage.join("ks").join("t").join(format!("{base}-{c}")), c).unwrap();
        }
    }

    #[test]
    fn publish_moves_everything_and_leaves_no_stage() {
        let tmp = tempfile::tempdir().unwrap();
        let sp = StagedPublish::begin(tmp.path(), "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db", "Index.db", "TOC.txt"]);
        sp.publish().unwrap();
        let dir = tmp.path().join("ks").join("t");
        for c in ["Data.db", "Index.db", "TOC.txt"] {
            assert!(dir.join(format!("nb-1-big-{c}")).is_file(), "{c}");
        }
        assert!(find_leftover(tmp.path()).is_none(), "staging dir removed");
    }

    #[test]
    fn data_db_is_renamed_last_and_toc_second_to_last() {
        let mut v = vec![
            PathBuf::from("ks/t/nb-1-big-Data.db"),
            PathBuf::from("ks/t/nb-1-big-TOC.txt"),
            PathBuf::from("ks/t/nb-1-big-Index.db"),
            PathBuf::from("ks/t/nb-1-big-Statistics.db"),
        ];
        v.sort_by(|a, b| rank(a).cmp(&rank(b)).then_with(|| a.cmp(b)));
        assert!(v[2].ends_with("nb-1-big-TOC.txt") && v[3].ends_with("nb-1-big-Data.db"));
    }

    #[test]
    fn begin_refuses_while_a_leftover_stage_exists_naming_it() {
        let tmp = tempfile::tempdir().unwrap();
        let _crashed = StagedPublish::begin(tmp.path(), "1").unwrap(); // never published = a crash
        let err = StagedPublish::begin(tmp.path(), "2")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(".cqlite-publish-1.tmp") && err.contains("interrupted run"),
            "{err}"
        );
    }

    #[test]
    fn publish_refuses_to_overwrite_an_existing_final_file() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("ks/t")).unwrap();
        std::fs::write(tmp.path().join("ks/t/nb-1-big-Data.db"), b"old").unwrap();
        let sp = StagedPublish::begin(tmp.path(), "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db"]);
        let err = sp.publish().unwrap_err().to_string();
        assert!(err.contains("refusing to publish over"), "{err}");
        assert_eq!(
            std::fs::read(tmp.path().join("ks/t/nb-1-big-Data.db")).unwrap(),
            b"old"
        );
    }

    #[test]
    fn stage_names_never_parse_as_a_cassandra_component_file() {
        // Cassandra requires EXACTLY four '-'-separated tokens whose first matches [a-z]+
        // (`Descriptor.filenameTokens` / `Version.validate`): a leading '.' fails the version token.
        let name = format!("{STAGE_PREFIX}1{STAGE_SUFFIX}");
        assert!(is_stage_name(&name));
        let tokens: Vec<&str> = name.split('-').collect();
        assert!(tokens.len() != 4 || !tokens[0].chars().all(|c| c.is_ascii_lowercase()));
    }

    /// Run `publish_observed` on `sp`, returning the result and the recorded operation sequence.
    fn observe(sp: &mut StagedPublish) -> (Result<()>, Vec<Op>) {
        let mut ops = Vec::new();
        let r = sp.publish_observed(&mut |o| ops.push(o.clone()));
        (r, ops)
    }

    #[test]
    fn dir_fsync_barrier_precedes_data_db_rename_and_follows_it() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("out");
        let mut sp = StagedPublish::begin(&root, "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db", "Index.db", "TOC.txt"]);
        let (r, ops) = observe(&mut sp);
        r.unwrap();
        let data = ops
            .iter()
            .position(|o| matches!(o, Op::Rename(p) if p.ends_with("nb-1-big-Data.db")))
            .unwrap();
        let toc = ops
            .iter()
            .position(|o| matches!(o, Op::Rename(p) if p.ends_with("nb-1-big-TOC.txt")))
            .unwrap();
        let table = root.join("ks").join("t");
        let sync_before = ops[toc..data]
            .iter()
            .any(|o| *o == Op::SyncDir(table.clone()));
        assert!(
            sync_before,
            "dir fsync between TOC.txt and Data.db: {ops:?}"
        );
        assert_eq!(
            ops[data + 1],
            Op::SyncDir(table.clone()),
            "dir fsync right after Data.db: {ops:?}"
        );
        // Intermediate and created directories are durable too (out/ks, out, and out's parent).
        for d in [root.join("ks"), root.clone(), tmp.path().to_path_buf()] {
            assert!(
                ops.contains(&Op::SyncDir(d.clone())),
                "{} unsynced: {ops:?}",
                d.display()
            );
        }
        // No rename follows Data.db.
        assert!(!ops[data + 1..].iter().any(|o| matches!(o, Op::Rename(_))));
    }

    #[test]
    fn collision_is_found_before_any_rename_and_leaves_out_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let table = tmp.path().join("ks/t");
        std::fs::create_dir_all(&table).unwrap();
        // The collision is the LAST file in publish order (Data.db): a per-rename check would
        // already have renamed Index.db and TOC.txt.
        std::fs::write(table.join("nb-1-big-Data.db"), b"old").unwrap();
        let mut sp = StagedPublish::begin(tmp.path(), "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db", "Index.db", "TOC.txt"]);
        let (r, ops) = observe(&mut sp);
        let err = r.unwrap_err().to_string();
        assert!(
            err.contains("refusing to publish over") && err.contains("nb-1-big-Data.db"),
            "{err}"
        );
        assert!(
            ops.is_empty(),
            "no rename or fsync before the refusal: {ops:?}"
        );
        let stage = sp.dir().to_path_buf();
        drop(sp);
        assert!(!stage.exists(), "stage removed");
        assert!(!table.join("nb-1-big-Index.db").exists());
        assert!(!table.join("nb-1-big-TOC.txt").exists());
        assert_eq!(
            std::fs::read(table.join("nb-1-big-Data.db")).unwrap(),
            b"old"
        );
        let mut names: Vec<_> = std::fs::read_dir(&table)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        names.sort();
        assert_eq!(names, vec![std::ffi::OsString::from("nb-1-big-Data.db")]);
    }

    #[test]
    fn drop_after_a_completed_publish_touches_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("out"); // created by the run, so Drop would try remove_dir(root)
        let mut sp = StagedPublish::begin(&root, "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db", "TOC.txt"]);
        let (r, _) = observe(&mut sp);
        r.unwrap();
        assert!(sp.published);
        // After the last checked step something else appears where the stage / root were: an
        // unrelated tree at the stage path, and an unrelated file in the root.
        let stage = sp.dir().to_path_buf();
        std::fs::create_dir(&stage).unwrap();
        std::fs::write(stage.join("unrelated"), b"keep").unwrap();
        std::fs::write(root.join("other.txt"), b"keep").unwrap();
        drop(sp);
        assert_eq!(std::fs::read(stage.join("unrelated")).unwrap(), b"keep");
        assert_eq!(std::fs::read(root.join("other.txt")).unwrap(), b"keep");
        assert!(root.join("ks/t/nb-1-big-Data.db").is_file());
    }
}
