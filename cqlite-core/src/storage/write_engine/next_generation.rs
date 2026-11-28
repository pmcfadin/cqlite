//! The write engine's next SSTable id (issue #4626, requirements G6 and G7).
//!
//! Delegates to the generation module: the maximum sequence id over the table directory's Live set
//! and `backups/`, across BIG and BTI and every version letter, plus one. The directory is
//! `<data_dir>/<keyspace>/<table>`, which is where `SSTableWriter` publishes a flush.

use std::path::Path;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::sstable::generation::{next_generation_id, GenerationId, MAX_SEQUENCE_ID};
use crate::storage::sstable::writer::SSTableInfo;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::rebuild::no_clobber::{retarget_name, MAX_PUBLISH_ATTEMPTS};
use super::rebuild::staged_publish::{LeftoverPolicy, StagedPublish};

static STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// `.compaction-tmp-<gen>-<pid>-<n>`: the startup sweep's prefix, made unique so two engines
/// sharing a data directory never share a staging root.
pub(super) fn unique_tmp_name(generation: u64) -> String {
    format!(
        ".compaction-tmp-{generation}-{}-{}",
        std::process::id(),
        STAGE_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// The stage a flush writes into: `<table_dir>/.cqlite-publish-flush-<pid>-<n>.tmp`. The tag is
/// unique per call and leftovers are ignored, so concurrent writers never refuse on each other.
pub(super) fn begin_stage(data_dir: &Path, table_dir: &Path) -> Result<StagedPublish> {
    let tag = format!(
        "flush-{}-{}",
        std::process::id(),
        STAGE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    StagedPublish::begin_in(data_dir, table_dir, &tag, LeftoverPolicy::Ignore)
}

/// Publish a staged generation written under id `from` WITHOUT replacing any existing file. If
/// another writer took the id meanwhile, re-allocate (bounded) and publish under the fresh one.
/// Returns the id published.
pub(super) fn publish_staged(stage: StagedPublish, table_dir: &Path, from: u64) -> Result<u64> {
    stage.publish_retargeting(from, MAX_PUBLISH_ATTEMPTS, &mut |taken| {
        reallocate(table_dir, taken)
    })
}

/// A fresh id above `taken` after another writer used it, inside the Java int range (G7).
pub(super) fn reallocate(table_dir: &Path, taken: u64) -> Result<u64> {
    let next = match next_generation_id(table_dir) {
        Ok(GenerationId::Sequence(n)) => u64::from(n),
        Ok(other) => {
            return Err(Error::Storage(format!(
                "next generation id for {} is not a sequence id: {other}",
                table_dir.display()
            )))
        }
        Err(e) => {
            return Err(Error::Storage(format!(
                "cannot re-allocate a generation in {}: {e}",
                table_dir.display()
            )))
        }
    };
    let next = next.max(taken.saturating_add(1));
    ensure_flushable(next)?;
    Ok(next)
}

fn relocated(p: &Path, table_dir: &Path, from: u64, to: u64) -> PathBuf {
    match p.file_name().and_then(|n| n.to_str()) {
        Some(n) => table_dir.join(retarget_name(n, from, to)),
        None => p.to_path_buf(),
    }
}

/// `info` (paths under a stage) with every path moved to its published name in `table_dir`.
pub(super) fn relocate_info(
    info: SSTableInfo,
    table_dir: &Path,
    from: u64,
    to: u64,
) -> SSTableInfo {
    let r = |p: &PathBuf| relocated(p, table_dir, from, to);
    SSTableInfo {
        data_path: r(&info.data_path),
        index_path: info.index_path.as_ref().map(r),
        filter_path: info.filter_path.as_ref().map(r),
        summary_path: info.summary_path.as_ref().map(r),
        stats_path: r(&info.stats_path),
        compression_info_path: info.compression_info_path.as_ref().map(r),
        partitions_path: info.partitions_path.as_ref().map(r),
        rows_path: info.rows_path.as_ref().map(r),
        toc_path: r(&info.toc_path),
        digest_path: r(&info.digest_path),
        crc_path: info.crc_path.as_ref().map(r),
        ..info
    }
}

/// The id the first flush of an engine over `data_dir` uses (1 when the table has no directory yet).
pub(super) fn next_flush_generation(data_dir: &Path, schema: &TableSchema) -> Result<u64> {
    let table_dir = data_dir.join(&schema.keyspace).join(&schema.table);
    if !table_dir.is_dir() {
        return Ok(1);
    }
    match next_generation_id(&table_dir) {
        Ok(GenerationId::Sequence(n)) => Ok(u64::from(n)),
        Ok(other) => Err(Error::Storage(format!(
            "next generation id for {} is not a sequence id: {other}",
            table_dir.display()
        ))),
        Err(e) => Err(Error::Storage(format!(
            "cannot allocate the next SSTable generation in {}: {e}",
            table_dir.display()
        ))),
    }
}

/// Refuse a flush whose id would leave the Java `int` range (G7).
pub(super) fn ensure_flushable(generation: u64) -> Result<()> {
    if generation > u64::from(MAX_SEQUENCE_ID) {
        return Err(Error::Storage(format!(
            "no sequence id is left: generation {generation} exceeds the Java int limit 2147483647"
        )));
    }
    Ok(())
}
