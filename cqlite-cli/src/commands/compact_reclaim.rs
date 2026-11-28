//! #4666: a compaction removes its fully-expired inputs only once the output is durable.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cqlite_core::storage::write_engine::merge::CompactReport;

/// The first ancestor of `output` (itself included) that already exists, recorded BEFORE the
/// compaction creates `<output>/<keyspace>/<table>` (and any missing levels of `output`).
pub(crate) fn existing_anchor(output: &Path) -> PathBuf {
    let anchor = output
        .ancestors()
        .find(|a| a.as_os_str().is_empty() || a.exists())
        .unwrap_or_else(|| Path::new("."));
    if anchor.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        anchor.to_path_buf()
    }
}

/// Every directory whose entry must be durable for `table_dir` to survive a power loss: the table
/// dir and each ancestor up to and including `anchor` (the first directory that existed before the
/// compaction). Bottom-up.
fn durability_dirs(table_dir: &Path, anchor: &Path) -> Vec<PathBuf> {
    let anchor_norm = |p: &Path| {
        if p.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            p.to_path_buf()
        }
    };
    let mut dirs: Vec<PathBuf> = Vec::new();
    for d in table_dir.ancestors() {
        dirs.push(anchor_norm(d));
        if anchor_norm(d) == anchor_norm(anchor) {
            return dirs;
        }
    }
    // `table_dir` is not under `anchor` (should not happen): sync the anchor regardless.
    dirs.push(anchor_norm(anchor));
    dirs
}

fn sync_dirs(dirs: &[PathBuf], fsync: &mut dyn FnMut(&Path) -> Result<()>) -> Result<()> {
    for d in dirs {
        fsync(d).with_context(|| format!("cannot fsync compaction output dir {}", d.display()))?;
    }
    Ok(())
}

/// The output (TOC.txt last, every file fsynced by `SSTableWriter::finish`) is durable only once
/// every directory entry on its path is too. Only then may a fully-expired input be removed.
/// `fsync_dir` is platform-aware (a documented no-op off unix).
pub(crate) fn reclaim_after_durable(report: &CompactReport, anchor: &Path) -> Result<()> {
    if let Some(table_dir) = report.output.data_path.parent() {
        let dirs = durability_dirs(table_dir, anchor);
        sync_dirs(&dirs, &mut |d| {
            cqlite_core::storage::write_engine::rebuild::fsync_dir(d).map_err(Into::into)
        })?;
    }
    abort_before_reclaim_seam();
    report.reclaim_dropped_inputs();
    Ok(())
}

/// Debug-build-only crash seam for the #4666 tests: `CQLITE_TEST_COMPACT_ABORT_BEFORE_RECLAIM=1`
/// aborts the process (no unwinding: a stand-in for SIGKILL) after the output is durably written
/// and before any input is removed. Compiled out of release builds.
#[cfg(debug_assertions)]
fn abort_before_reclaim_seam() {
    if std::env::var("CQLITE_TEST_COMPACT_ABORT_BEFORE_RECLAIM").as_deref() == Ok("1") {
        std::process::abort();
    }
}
#[cfg(not(debug_assertions))]
fn abort_before_reclaim_seam() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_new_output_syncs_every_created_level_and_the_existing_anchor() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("new/a/out");
        let anchor = existing_anchor(&out);
        assert_eq!(anchor, tmp.path());
        let table = out.join("ks/tbl");
        let dirs = durability_dirs(&table, &anchor);
        let want: Vec<PathBuf> = [
            table.clone(),
            out.join("ks"),
            out.clone(),
            tmp.path().join("new/a"),
            tmp.path().join("new"),
            tmp.path().to_path_buf(),
        ]
        .into();
        assert_eq!(dirs, want);
    }

    #[test]
    fn existing_output_anchors_at_itself() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(existing_anchor(tmp.path()), tmp.path());
        let dirs = durability_dirs(&tmp.path().join("ks/tbl"), tmp.path());
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[2], tmp.path());
    }

    #[test]
    fn relative_output_syncs_dot_not_empty_path() {
        assert_eq!(existing_anchor(Path::new("zz-no-such-out")), Path::new("."));
        let dirs = durability_dirs(Path::new("zz-no-such-out/ks/tbl"), Path::new("."));
        assert_eq!(dirs.last(), Some(&PathBuf::from(".")));
        assert!(dirs.iter().all(|d| !d.as_os_str().is_empty()));
    }

    #[test]
    fn recorder_sees_every_level_and_stops_on_first_error() {
        let dirs = durability_dirs(Path::new("/w/out/ks/tbl"), Path::new("/w/out"));
        let mut seen = Vec::new();
        sync_dirs(&dirs, &mut |d| {
            seen.push(d.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, dirs);
        let mut n = 0;
        let r = sync_dirs(&dirs, &mut |_| {
            n += 1;
            Err(anyhow::anyhow!("boom"))
        });
        assert!(r.is_err());
        assert_eq!(n, 1);
    }
}
