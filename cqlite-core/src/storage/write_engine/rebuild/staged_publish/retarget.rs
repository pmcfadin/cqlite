//! Staging inside a table directory and re-allocation after a lost race for a generation id
//! (#4623, #4626). A child module of `staged_publish` so it can reach its private state.

use super::{existing_anchor, is_stage_name, StagedPublish, STAGE_PREFIX, STAGE_SUFFIX};
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};

/// What [`StagedPublish::begin_in`] does when a stage from an earlier run is already there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeftoverPolicy {
    /// Refuse, naming the leftover (a re-run must not interleave with an interrupted one).
    Refuse,
    /// Ignore it: another live writer's stage is indistinguishable from a crashed one's, so a
    /// concurrent writer (an engine flush) must not refuse on it. Callers use a unique tag.
    Ignore,
}

/// A stage directly inside `dir` (non-recursive), if any.
fn find_leftover_in(dir: &Path) -> Option<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter(|e| e.file_name().to_str().is_some_and(is_stage_name))
        .map(|e| e.path())
        .collect();
    found.sort();
    found.into_iter().next()
}

impl StagedPublish {
    /// Like [`begin`](StagedPublish::begin), but the stage lives in `stage_parent` (a directory
    /// below `root`, normally the table directory) instead of `root`, and the leftover check looks
    /// only there, never across the whole of `root`. Files staged under
    /// [`dir`](StagedPublish::dir) are published to `root.join(<path relative to the stage>)`.
    pub fn begin_in(
        root: &Path,
        stage_parent: &Path,
        tag: &str,
        policy: LeftoverPolicy,
    ) -> Result<Self> {
        let created_root = !root.exists();
        let anchor = if created_root {
            existing_anchor(root)
        } else {
            None
        };
        std::fs::create_dir_all(stage_parent)?;
        let stage = stage_parent.join(format!("{STAGE_PREFIX}{tag}{STAGE_SUFFIX}"));
        let mut this = Self {
            root: root.to_path_buf(),
            stage: stage.clone(),
            created_root,
            published: false,
            publication_started: false,
            anchor,
            stage_parent: stage_parent.to_path_buf(),
            renumber: None,
            collided: None,
            toc_last: true,
        };
        // Disarmed (`Drop` removes nothing) until `create_dir` succeeds: a refusal, or an existing
        // stage with the same tag, must never delete a directory this call did not create.
        this.published = true;
        if policy == LeftoverPolicy::Refuse {
            if let Some(left) = find_leftover_in(stage_parent) {
                return Err(Error::InvalidInput(format!(
                    "{} holds a staging directory from an interrupted run ({}): remove it before \
                     re-running; its files are unpublished and incomplete",
                    stage_parent.display(),
                    left.display()
                )));
            }
        }
        std::fs::create_dir(&stage)?;
        this.published = false; // armed: the stage is ours
        Ok(this)
    }

    /// The final path of the staged file whose stage-relative path is `rel`, with the id renumbered
    /// when a previous attempt lost a race for it.
    pub(super) fn dest_of(&self, rel: &Path) -> PathBuf {
        let Some((from, to)) = self.renumber else {
            return self.root.join(rel);
        };
        let renamed = rel
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| super::super::no_clobber::retarget_name(n, from, to));
        match (rel.parent(), renamed) {
            (Some(parent), Some(name)) => self.root.join(parent).join(name),
            _ => self.root.join(rel),
        }
    }

    /// Publish the stage, re-allocating the id when a final name already exists.
    ///
    /// Staged names carry id `from`. On a collision everything this attempt linked is removed
    /// (never anything else), `next_id(taken)` chooses a fresh id, and the set is published again
    /// under it, at most `attempts` times in all; then a named error. Returns the id published.
    pub fn publish_retargeting(
        mut self,
        from: u64,
        attempts: u32,
        next_id: &mut dyn FnMut(u64) -> Result<u64>,
    ) -> Result<u64> {
        let attempts = attempts.max(1);
        let mut id = from;
        let mut attempt = 1;
        loop {
            let err = match self.publish_observed(&mut |_| Ok(())) {
                Ok(()) => return Ok(id),
                Err(e) => e,
            };
            let Some(taken) = self.collided.take() else {
                return Err(err);
            };
            if attempt >= attempts {
                return Err(Error::Storage(format!(
                    "could not publish a generation without overwriting an existing one: \
                     {attempts} attempt(s), the last id tried was {id} and {} already exists",
                    taken.display()
                )));
            }
            id = next_id(id)?;
            self.renumber = Some((from, id));
            attempt += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPS: [&str; 3] = ["Index.db", "TOC.txt", "Data.db"];

    fn stage_set(stage: &StagedPublish, id: u64, body: &str) {
        let d = stage.dir().join("ks").join("t");
        std::fs::create_dir_all(&d).unwrap();
        for c in COMPS {
            std::fs::write(d.join(format!("nb-{id}-big-{c}")), format!("{body}-{c}")).unwrap();
        }
    }

    fn table(root: &Path) -> PathBuf {
        let t = root.join("ks").join("t");
        std::fs::create_dir_all(&t).unwrap();
        t
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn a_lost_race_republishes_under_the_next_id_and_keeps_the_other_writers_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let t = table(tmp.path());
        let stage = StagedPublish::begin_in(tmp.path(), &t, "a", LeftoverPolicy::Refuse).unwrap();
        stage_set(&stage, 3, "mine");
        // Another writer published nb-3 meanwhile (every file, so the preflight sees it).
        for c in COMPS {
            std::fs::write(t.join(format!("nb-3-big-{c}")), format!("theirs-{c}")).unwrap();
        }
        let id = stage
            .publish_retargeting(3, 8, &mut |taken| Ok(taken + 1))
            .unwrap();
        assert_eq!(id, 4);
        for c in COMPS {
            assert_eq!(
                std::fs::read_to_string(t.join(format!("nb-3-big-{c}"))).unwrap(),
                format!("theirs-{c}")
            );
            assert_eq!(
                std::fs::read_to_string(t.join(format!("nb-4-big-{c}"))).unwrap(),
                format!("mine-{c}")
            );
        }
        assert_eq!(names(&t).len(), 6, "no stage left: {:?}", names(&t));
    }

    /// The race is lost on `Data.db`, the LAST file, AFTER the others were linked (the observer
    /// plants the other writer's `Data.db` just before that link): only the names this run linked
    /// are rolled back and the other writer's `Data.db` survives.
    #[test]
    fn a_collision_after_partial_linking_removes_only_our_own_files() {
        let tmp = tempfile::tempdir().unwrap();
        let t = table(tmp.path());
        let mut stage =
            StagedPublish::begin_in(tmp.path(), &t, "b", LeftoverPolicy::Refuse).unwrap();
        stage_set(&stage, 3, "mine");
        let theirs = t.join("nb-3-big-Data.db");
        let planted = theirs.clone();
        let err = stage
            .publish_observed(&mut |op| {
                if matches!(op, super::super::Op::Rename(p) if p.ends_with("nb-3-big-Data.db")) {
                    std::fs::write(&planted, "theirs").unwrap();
                }
                Ok(())
            })
            .unwrap_err()
            .to_string();
        assert!(err.contains("nb-3-big-Data.db"), "{err}");
        assert!(stage.collided.is_some());
        let finals: Vec<String> = names(&t)
            .into_iter()
            .filter(|n| !is_stage_name(n))
            .collect();
        assert_eq!(
            finals,
            vec!["nb-3-big-Data.db".to_string()],
            "ours were removed"
        );
        assert_eq!(std::fs::read_to_string(&theirs).unwrap(), "theirs");
    }

    /// m2: the leftover check looks only in the stage's own directory, never across the root.
    #[test]
    fn leftover_refusal_is_scoped_to_the_table_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let t = table(tmp.path());
        let other = tmp.path().join("ks2").join("t2");
        std::fs::create_dir_all(other.join(".cqlite-publish-old.tmp")).unwrap();
        // A leftover in ANOTHER table does not refuse this one.
        let ok = StagedPublish::begin_in(tmp.path(), &t, "c", LeftoverPolicy::Refuse).unwrap();
        ok.discard();
        // A leftover in the target table directory does, naming it.
        std::fs::create_dir_all(t.join(".cqlite-publish-crashed.tmp")).unwrap();
        let err = StagedPublish::begin_in(tmp.path(), &t, "d", LeftoverPolicy::Refuse)
            .unwrap_err()
            .to_string();
        assert!(err.contains(".cqlite-publish-crashed.tmp"), "{err}");
        // A concurrent writer's policy ignores it.
        StagedPublish::begin_in(tmp.path(), &t, "e", LeftoverPolicy::Ignore)
            .unwrap()
            .discard();
    }

    /// Round 2, High: a NEW SSTable (`begin_in`) publishes `TOC.txt` LAST, after the other
    /// components are linked and the directory is fsynced. A fault injected immediately before
    /// the TOC link leaves no `TOC.txt`, so no generation is ever published-looking.
    #[test]
    fn toc_is_linked_last_and_a_fault_before_it_publishes_nothing() {
        use crate::storage::sstable::generation::{list_generations, Scope, TocPolicy};
        let tmp = tempfile::tempdir().unwrap();
        let t = table(tmp.path());
        let mut stage =
            StagedPublish::begin_in(tmp.path(), &t, "f", LeftoverPolicy::Refuse).unwrap();
        stage_set(&stage, 3, "mine");
        let mut ops = Vec::new();
        let err = stage
            .publish_observed(&mut |op| {
                ops.push(format!("{op:?}"));
                if matches!(op, super::super::Op::Rename(p) if p.ends_with("nb-3-big-TOC.txt")) {
                    return Err(Error::Storage("injected fault before the TOC link".into()));
                }
                Ok(())
            })
            .unwrap_err()
            .to_string();
        assert!(err.contains("injected"), "{err}");
        assert!(!t.join("nb-3-big-TOC.txt").exists(), "TOC must not exist");
        // Everything else was linked before the TOC, and a directory fsync preceded it.
        let toc_at = ops.iter().position(|o| o.contains("TOC.txt")).unwrap();
        assert!(ops[..toc_at].iter().any(|o| o.contains("Data.db")));
        assert!(ops[toc_at - 1].starts_with("SyncDir"), "{ops:?}");
        // No reader sees a TOC without its Data.db; nothing carries a TOC at all.
        let listing = list_generations(&t, &Scope::Live, TocPolicy::Accept).unwrap();
        for g in listing.live.iter().chain(listing.orphans.iter()) {
            assert!(
                g.component_path("TOC.txt").is_none(),
                "{} has a TOC",
                g.base_name()
            );
        }
    }

    /// Round 2, Low: refusing because a stage with the same tag already exists must not delete
    /// that pre-existing stage (the failed `begin_in` must not own its cleanup).
    #[test]
    fn a_preexisting_same_tag_stage_survives_the_refusal() {
        let tmp = tempfile::tempdir().unwrap();
        let t = table(tmp.path());
        let theirs = t.join(".cqlite-publish-same.tmp");
        std::fs::create_dir_all(&theirs).unwrap();
        std::fs::write(theirs.join("keep.txt"), "other writer").unwrap();
        for policy in [LeftoverPolicy::Refuse, LeftoverPolicy::Ignore] {
            assert!(StagedPublish::begin_in(tmp.path(), &t, "same", policy).is_err());
            assert_eq!(
                std::fs::read_to_string(theirs.join("keep.txt")).unwrap(),
                "other writer",
                "{policy:?}"
            );
        }
    }
}
