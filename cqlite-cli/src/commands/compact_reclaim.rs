//! #4666: a compaction removes its fully-expired inputs only once the output is durable.

use anyhow::{Context, Result};
use cqlite_core::storage::write_engine::merge::CompactReport;

/// The output (TOC.txt last, every file fsynced by `SSTableWriter::finish`) is durable only once its
/// directory entries are too. Only then may a fully-expired input be removed.
pub(crate) fn reclaim_after_durable(report: &CompactReport) -> Result<()> {
    if let Some(dir) = report.output.data_path.parent() {
        std::fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .with_context(|| format!("cannot fsync compaction output dir {}", dir.display()))?;
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
