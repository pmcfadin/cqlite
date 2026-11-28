//! No-clobber publication of an engine compaction's output (#4623, #4626), split out of
//! `compaction.rs` for the file-size ratchet.
//!
//! The output components sit under a private tmp root named for the id they were written with.
//! Each is LINKED (never renamed over) into the table directory: a final name that already exists
//! means another writer took the id, so the files this attempt linked are removed (only those) and
//! the whole set is published again under a re-allocated id, a bounded number of times.
//! Order and durability barriers are unchanged from the former rename publish (#1959): non-TOC
//! components, a directory fsync of the leaf-to-data-root chain, `TOC.txt` last, a leaf fsync.

use std::path::{Path, PathBuf};

#[cfg(test)]
use super::compaction::{
    FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC, FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC,
};
use super::rebuild::no_clobber::{
    is_collision, link_no_clobber, retarget_name, MAX_PUBLISH_ATTEMPTS,
};
use crate::error::{Error, Result};
use crate::storage::sstable::writer::SSTableInfo;

enum Attempt {
    Published,
    /// A final name already existed; everything this attempt linked is gone again.
    Collided(PathBuf),
}

/// Publish `tmp_info`'s components (written under id `generation`) into `sstable_dir`; returns the
/// id they were published under. On error nothing this call linked remains; the caller removes
/// `tmp_dir`.
pub(super) fn publish_compaction_output(
    sstable_dir: &Path,
    data_root: &Path,
    tmp_dir: &Path,
    tmp_info: &SSTableInfo,
    generation: u64,
) -> Result<u64> {
    std::fs::create_dir_all(sstable_dir).map_err(|e| {
        Error::Storage(format!(
            "Failed to create SSTable directory {:?}: {}",
            sstable_dir, e
        ))
    })?;
    let mut id = generation;
    for attempt in 1..=MAX_PUBLISH_ATTEMPTS {
        match try_publish(sstable_dir, data_root, tmp_dir, tmp_info, generation, id)? {
            Attempt::Published => return Ok(id),
            Attempt::Collided(taken) => {
                if attempt == MAX_PUBLISH_ATTEMPTS {
                    return Err(Error::Storage(format!(
                        "could not publish the compaction output without overwriting an existing \
                         generation: {MAX_PUBLISH_ATTEMPTS} attempts, the last id tried was {id} \
                         and {} already exists",
                        taken.display()
                    )));
                }
                id = super::next_generation::reallocate(sstable_dir, id)?;
            }
        }
    }
    Err(Error::Storage(
        "compaction publish made no attempt".to_string(),
    ))
}

fn try_publish(
    sstable_dir: &Path,
    data_root: &Path,
    tmp_dir: &Path,
    info: &SSTableInfo,
    from: u64,
    to: u64,
) -> Result<Attempt> {
    // Non-TOC components first. Index/Summary are BIG-only (BTI reports None, #908); Filter.db is
    // absent for a disabled bloom filter (#852); CRC.db for uncompressed BIG (#1197).
    let mut non_toc: Vec<&PathBuf> = vec![&info.data_path];
    non_toc.extend(info.index_path.as_ref());
    non_toc.extend(info.filter_path.as_ref());
    non_toc.extend(info.summary_path.as_ref());
    non_toc.push(&info.stats_path);
    non_toc.push(&info.digest_path);
    non_toc.extend(info.crc_path.as_ref());
    non_toc.extend(info.partitions_path.as_ref());
    non_toc.extend(info.compression_info_path.as_ref());

    let dst_of = |src: &PathBuf| -> Result<PathBuf> {
        let name = src
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Error::Storage("Component path has no filename".to_string()))?;
        Ok(sstable_dir.join(retarget_name(name, from, to)))
    };

    let mut linked: Vec<PathBuf> = Vec::new();
    // Remove ONLY what this attempt linked.
    let rollback = |linked: &[PathBuf]| {
        for dst in linked {
            let _ = std::fs::remove_file(dst);
        }
    };
    let link = |src: &PathBuf, linked: &mut Vec<PathBuf>| -> Result<Option<PathBuf>> {
        let dst = dst_of(src)?;
        match link_no_clobber(src, &dst) {
            Ok(()) => {
                linked.push(dst);
                Ok(None)
            }
            Err(e) if is_collision(&e) => Ok(Some(dst)),
            Err(e) => Err(Error::Storage(format!(
                "Publishing {:?} to {:?} failed (rolling back, inputs intact): {}",
                src, dst, e
            ))),
        }
    };

    for src in &non_toc {
        match link(src, &mut linked) {
            Ok(None) => {}
            Ok(Some(taken)) => {
                rollback(&linked);
                return Ok(Attempt::Collided(taken));
            }
            Err(e) => {
                rollback(&linked);
                return Err(e);
            }
        }
    }

    // Directory durability barrier BEFORE the TOC (#1959): every non-TOC dirent is durable first.
    #[cfg(test)]
    if FAIL_COMPACTION_DIR_FSYNC_BEFORE_TOC.with(|f| f.get()) {
        rollback(&linked);
        return Err(Error::Storage(
            "injected directory fsync fault before TOC rename (test seam, issue #1959)".to_string(),
        ));
    }
    for dir in super::durability::dirs_to_sync(sstable_dir, data_root) {
        if let Err(e) = super::durability::sync_directory(&dir) {
            rollback(&linked);
            return Err(e);
        }
    }

    // Publish by linking TOC.txt last, then fsync the LEAF again so the TOC dirent is durable
    // before the inputs (the only other durable copy) are deleted (#1959).
    match link(&info.toc_path, &mut linked) {
        Ok(None) => {}
        Ok(Some(taken)) => {
            rollback(&linked);
            return Ok(Attempt::Collided(taken));
        }
        Err(e) => {
            rollback(&linked);
            return Err(e);
        }
    }
    #[cfg(test)]
    if FAIL_COMPACTION_DIR_FSYNC_AFTER_TOC.with(|f| f.get()) {
        rollback(&linked);
        return Err(Error::Storage(
            "injected directory fsync fault after TOC rename (test seam, issue #1959)".to_string(),
        ));
    }
    if let Err(e) = super::durability::sync_directory(sstable_dir) {
        rollback(&linked);
        return Err(e);
    }
    let _ = tmp_dir; // removed by the caller after the inputs are reclaimed
    Ok(Attempt::Published)
}
