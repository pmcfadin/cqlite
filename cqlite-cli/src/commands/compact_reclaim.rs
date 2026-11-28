//! #4666: a compaction removes its fully-expired inputs only once the output is durable.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use cqlite_core::storage::write_engine::merge::CompactReport;

/// Every directory whose entry must be durable for `table_dir` to survive a power loss: the table
/// dir, each ancestor up to and including `output_root` (compact creates `<out>/<ks>/<table>`),
/// and `output_root`'s parent (in case `output_root` itself was just created). Bottom-up.
fn durability_dirs(table_dir: &Path, output_root: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for d in table_dir.ancestors() {
        dirs.push(d.to_path_buf());
        if d == output_root {
            break;
        }
    }
    if !dirs.iter().any(|d| d == output_root) {
        // `table_dir` is not under `output_root` (should not happen): sync the root regardless.
        dirs.push(output_root.to_path_buf());
    }
    if let Some(parent) = output_root.parent() {
        dirs.push(parent.to_path_buf());
    }
    for d in &mut dirs {
        if d.as_os_str().is_empty() {
            *d = PathBuf::from(".");
        }
    }
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
pub(crate) fn reclaim_after_durable(report: &CompactReport, output_root: &Path) -> Result<()> {
    if let Some(table_dir) = report.output.data_path.parent() {
        let dirs = durability_dirs(table_dir, output_root);
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
    fn syncs_table_keyspace_output_and_output_parent_bottom_up() {
        let dirs = durability_dirs(Path::new("/w/out/ks/tbl"), Path::new("/w/out"));
        let want: Vec<PathBuf> = ["/w/out/ks/tbl", "/w/out/ks", "/w/out", "/w"]
            .iter()
            .map(PathBuf::from)
            .collect();
        assert_eq!(dirs, want);
    }

    #[test]
    fn relative_output_root_syncs_dot_not_empty_path() {
        let dirs = durability_dirs(Path::new("out/ks/tbl"), Path::new("out"));
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
