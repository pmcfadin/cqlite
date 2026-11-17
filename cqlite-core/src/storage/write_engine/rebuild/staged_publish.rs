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
//! The same holds for a failed rename or directory fsync once publication has STARTED: the stage is
//! retained (never removed by `Drop`) so the final-named partial set is identifiable.

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
    /// The rename window was entered (the first rename may have happened): a failure from here on
    /// RETAINS the stage so the final-named partial set stays recognisable as an interrupted run's
    /// (`begin` then refuses, naming it). Before this, a failure removes the stage and leaves
    /// `--out` as found.
    publication_started: bool,
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

fn sync_all_dirs(dirs: &[PathBuf], obs: &mut dyn FnMut(&Op) -> Result<()>) -> Result<()> {
    for d in dirs {
        obs(&Op::SyncDir(d.clone()))?;
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
            existing_anchor(root)
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
            publication_started: false,
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
        self.publish_observed(&mut |_| Ok(()))
    }

    fn publish_observed(&mut self, obs: &mut dyn FnMut(&Op) -> Result<()>) -> Result<()> {
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
            // Opened read-only on purpose: fsync(2) (and F_FULLFSYNC on macOS) is valid on an
            // O_RDONLY descriptor, so a staged file that is read-only (mode 0o444) still publishes.
            std::fs::File::open(file)?.sync_all().map_err(|e| {
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
                for anc in dir_chain(parent, floor) {
                    if !sync_dirs.contains(&anc) {
                        sync_dirs.push(anc);
                    }
                }
            }
        }
        let data_start = staged.iter().position(|(_, r)| rank(r) == 2);
        let split = data_start.unwrap_or(staged.len());
        // From here a failure may leave final-named files: keep the stage as the leftover marker.
        self.publication_started = true;
        // The stage marker must be durable BEFORE any final name can be: fsync the stage directory
        // (its staged subtree entries), then its parent (the stage's own entry), then every newly
        // created ancestor up to the anchor. Otherwise a power loss could persist destination
        // renames while the `.cqlite-publish-*.tmp` entry is lost, leaving an unidentifiable
        // final-named partial set (#4529, AC2).
        let mut stage_durable = vec![self.stage.clone()];
        stage_durable.extend(dir_chain(&self.root, floor));
        sync_all_dirs(&stage_durable, obs)?;
        for (renamed, (file, rel)) in staged.iter().enumerate() {
            if renamed == split {
                // Barrier: every non-Data.db rename is durable before Data.db can appear.
                sync_all_dirs(&sync_dirs, obs)?;
            }
            test_abort_before_rename(renamed);
            let dest = self.root.join(rel);
            obs(&Op::Rename(dest.clone()))?;
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
        obs(&Op::SyncDir(self.root.clone()))?;
        fsync_dir(&self.root)
    }

    /// Abandon the generation: remove the staging directory (best effort).
    pub fn discard(self) {
        // `Drop` does the removal.
    }
}

/// A `StagedPublish` dropped without being published BEFORE the rename window (an early `return`, a
/// `?`, a refused run) removes its staging directory. A failure INSIDE the window, or a hard kill
/// (SIGKILL, power loss), leaves it.
impl Drop for StagedPublish {
    fn drop(&mut self) {
        // Published: nothing to clean. Publication started: a partial final-named set may exist,
        // so the stage (and `root`) stay as the recognisable leftover (#4529, AC2).
        if self.published || self.publication_started {
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

/// `p`, or `.` when `p` is the empty path (the parent of a single-component relative path such as
/// `out`): the empty path names no directory, so it can be neither tested for existence nor fsynced.
fn normalize_dir(p: &Path) -> PathBuf {
    if p.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        p.to_path_buf()
    }
}

/// The nearest ancestor of `root` that exists (the top of the chain `begin` creates), with an empty
/// parent normalised to `.`.
fn existing_anchor(root: &Path) -> Option<PathBuf> {
    root.ancestors()
        .skip(1)
        .map(normalize_dir)
        .find(|a| a.exists())
}

/// `dir` and each of its ancestors up to and including `floor` (an empty ancestor is `.`).
fn dir_chain(dir: &Path, floor: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for anc in dir.ancestors() {
        let anc = normalize_dir(anc);
        let at_floor = anc == floor;
        out.push(anc);
        if at_floor {
            break;
        }
    }
    out
}

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

    #[cfg(unix)]
    #[test]
    fn read_only_staged_files_publish_and_stay_read_only() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let sp = StagedPublish::begin(tmp.path(), "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db", "TOC.txt"]);
        for c in ["Data.db", "TOC.txt"] {
            let f = sp.dir().join("ks").join("t").join(format!("nb-1-big-{c}"));
            std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o444)).unwrap();
        }
        sp.publish().unwrap();
        for c in ["Data.db", "TOC.txt"] {
            let f = tmp
                .path()
                .join("ks")
                .join("t")
                .join(format!("nb-1-big-{c}"));
            let mode = std::fs::metadata(&f).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o444, "{c} must stay read-only");
        }
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
        let r = sp.publish_observed(&mut |o| {
            ops.push(o.clone());
            Ok(())
        });
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
    fn stage_and_created_chain_are_fsynced_before_the_first_rename() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("a").join("out"); // `a` and `out` are both newly created
        let mut sp = StagedPublish::begin(&root, "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db", "Index.db", "TOC.txt"]);
        let stage = sp.dir().to_path_buf();
        let (r, ops) = observe(&mut sp);
        r.unwrap();
        let first = ops.iter().position(|o| matches!(o, Op::Rename(_))).unwrap();
        let pos = |d: &Path| ops.iter().position(|o| *o == Op::SyncDir(d.to_path_buf()));
        let a = tmp.path().join("a");
        for d in [&stage, &root, &a, &tmp.path().to_path_buf()] {
            assert!(
                pos(d).is_some_and(|i| i < first),
                "{} not fsynced before the first rename: {ops:?}",
                d.display()
            );
        }
        // Stage first, then its parent (the stage's own entry), then the chain above it.
        assert!(pos(&stage) < pos(&root) && pos(&root) < pos(&a));
    }

    #[test]
    fn single_component_relative_out_anchors_at_dot_and_is_fsynced() {
        // Pure helpers: no process-cwd change. `out` has the empty path as its parent.
        let out = Path::new("out");
        assert_eq!(existing_anchor(out), Some(PathBuf::from(".")));
        assert_eq!(
            existing_anchor(Path::new("./out")),
            Some(PathBuf::from("."))
        );
        let floor = Path::new(".");
        assert_eq!(
            dir_chain(Path::new("out/ks/t"), floor),
            vec![
                PathBuf::from("out/ks/t"),
                PathBuf::from("out/ks"),
                PathBuf::from("out"),
                PathBuf::from("."),
            ]
        );
        assert_eq!(
            dir_chain(out, floor),
            vec![PathBuf::from("out"), floor.to_path_buf()]
        );
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

    /// Fail the `n`-th observed op matching `pick`, then drop and check the leftover is identifiable.
    fn failed_mid_window(pick: impl Fn(&Op) -> bool, nth: usize) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("out");
        let mut sp = StagedPublish::begin(&root, "1").unwrap();
        stage_set(
            sp.dir(),
            "nb-1-big",
            &[
                "Data.db",
                "Index.db",
                "Statistics.db",
                "Summary.db",
                "TOC.txt",
            ],
        );
        let mut seen = 0;
        let r = sp.publish_observed(&mut |o| {
            if pick(o) {
                seen += 1;
                if seen == nth {
                    return Err(Error::Storage("injected fault".into()));
                }
            }
            Ok(())
        });
        assert!(r.unwrap_err().to_string().contains("injected fault"));
        let stage = sp.dir().to_path_buf();
        drop(sp);
        assert!(stage.is_dir(), "stage retained after a mid-window failure");
        assert!(is_stage_name(stage.file_name().unwrap().to_str().unwrap()));
        assert!(
            !root.join("ks/t/nb-1-big-Data.db").exists(),
            "no final Data.db"
        );
        assert_eq!(find_leftover(&root), Some(stage));
        let err = StagedPublish::begin(&root, "2").unwrap_err().to_string();
        assert!(
            err.contains(".cqlite-publish-1.tmp") && err.contains("interrupted run"),
            "{err}"
        );
    }

    #[test]
    fn failure_on_a_mid_window_rename_retains_the_stage() {
        failed_mid_window(|o| matches!(o, Op::Rename(_)), 3);
    }

    #[test]
    fn failure_on_the_pre_data_db_directory_fsync_retains_the_stage() {
        // First fsync of the destination table directory = the pre-Data.db barrier (the earlier
        // stage/chain fsyncs precede the first rename).
        failed_mid_window(|o| matches!(o, Op::SyncDir(p) if p.ends_with("ks/t")), 1);
    }

    #[test]
    fn failure_before_any_rename_removes_the_stage_and_leaves_out_as_found() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("out");
        let mut sp = StagedPublish::begin(&root, "1").unwrap();
        stage_set(sp.dir(), "nb-1-big", &["Data.db", "TOC.txt"]);
        std::fs::create_dir_all(root.join("ks/t")).unwrap();
        std::fs::write(root.join("ks/t/nb-1-big-Data.db"), b"old").unwrap();
        assert!(sp.publish_observed(&mut |_| Ok(())).is_err());
        let stage = sp.dir().to_path_buf();
        drop(sp);
        assert!(!stage.exists());
        assert!(find_leftover(&root).is_none());
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
