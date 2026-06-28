//! BTI (`da`) write-path state and helpers for [`SSTableWriter`].
//!
//! The BTI format (issue #766 / #908 / #910) replaces the legacy BIG
//! `Index.db`/`Summary.db` partition index with a `Partitions.db` trie and a
//! within-partition `Rows.db` row-index trie. Because a wide partition's
//! `Rows.db` `RowsOffset` is only known after `Rows.db` is serialized in
//! [`SSTableWriter::finish`], each partition's trie payload is deferred:
//! [`PendingBtiPartition`] records the raw key, the `Data.db` offset, and — for
//! wide partitions — the OSS50 row-index blocks. The two helpers here own the
//! BTI-specific portions of the write path so the BIG path stays isolated:
//!
//! - [`SSTableWriter::queue_bti_partition`] is called per partition from
//!   `write_partition` to defer the trie payload.
//! - [`SSTableWriter::write_bti_components`] is called from `finish` to
//!   serialize `Rows.db` + `Partitions.db`.
//!
//! All bytes are byte-for-byte identical to the prior inline implementation;
//! this module only relocates the BTI write path out of `mod.rs`.

use super::{partitions_writer, PromotedIndexBlock, SSTableWriter};
use crate::error::{Error, Result};
use crate::storage::write_engine::mutation::{DecoratedKey, PartitionTombstone};
use std::path::PathBuf;

/// A deferred BTI partition payload (issue #910).
///
/// Narrow partitions (`row_index` is `None`) get a direct `Data.db` offset in
/// the partition trie; wide partitions get a `Rows.db` `TrieIndexEntry` and a
/// positive `RowsOffset` once `Rows.db` is serialized.
#[derive(Debug)]
pub(super) struct PendingBtiPartition {
    /// Raw on-disk partition-key bytes.
    pub(super) raw_key: Vec<u8>,
    /// Partition's absolute `Data.db` start offset.
    pub(super) data_offset: u64,
    /// `Some` for a wide partition: its row-index blocks + partition deletion.
    /// `None` for a narrow partition (direct `Data.db` offset).
    pub(super) row_index: Option<PendingRowIndex>,
}

/// The row-index payload of a wide BTI partition, queued for `Rows.db`.
#[derive(Debug)]
pub(super) struct PendingRowIndex {
    /// Per-block OSS50 separators + within-partition offsets.
    pub(super) blocks: Vec<partitions_writer::RowIndexBlock>,
    /// Partition-level deletion `(local_deletion_time, marked_for_delete_at)`,
    /// or `None` for LIVE.
    pub(super) partition_deletion: Option<(i32, i64)>,
}

impl SSTableWriter {
    /// Defer this partition's `Partitions.db` trie payload (BTI, issue #766 / #910).
    ///
    /// Called per partition from `write_partition` only when the writer is in
    /// the BTI format (`self.bti_pending` is `Some`); a no-op for BIG. The
    /// payload is a direct `Data.db` offset for a NARROW partition (< 2
    /// column-index blocks) or a `Rows.db` `RowsOffset` for a WIDE partition
    /// (>= 2 blocks). The `RowsOffset` is only known after `Rows.db` is
    /// serialized in `finish()`, so we record the raw key, the `Data.db`
    /// offset, and — for wide partitions — the OSS50 row-index separators here,
    /// and finalize both tries at `finish()`. The wide gate (>= 2 blocks)
    /// mirrors `RowIndexEntry.create()` /
    /// `IndexWriter::add_partition_with_promoted` and guide ch.17.
    pub(super) fn queue_bti_partition(
        &mut self,
        key: &DecoratedKey,
        data_offset: u64,
        promoted_blocks: &[PromotedIndexBlock],
        partition_tombstone: Option<&PartitionTombstone>,
    ) {
        let Some(ref mut pending) = self.bti_pending else {
            return;
        };
        let row_index = if promoted_blocks.len() >= 2 {
            // Build OSS50-separator row-index blocks. A block lacking an
            // OSS50 separator (marker-led, or no clustering key) cannot be
            // placed in the trie; if ANY block lacks one we fall back to a
            // direct Data.db offset for the whole partition rather than emit
            // an unreadable separator (no-heuristics: never guess bytes).
            let mut blocks = Vec::with_capacity(promoted_blocks.len());
            let mut all_have_sep = true;
            for b in promoted_blocks {
                match &b.oss50_separator {
                    Some(sep) if !sep.is_empty() => {
                        blocks.push(partitions_writer::RowIndexBlock {
                            separator_key: sep.clone(),
                            block_offset: b.offset,
                            open_marker: None,
                        });
                    }
                    _ => {
                        all_have_sep = false;
                        break;
                    }
                }
            }
            // Separators must be strictly ascending and unique for the trie.
            let strictly_ascending = blocks
                .windows(2)
                .all(|w| w[0].separator_key < w[1].separator_key);
            if all_have_sep && strictly_ascending && !blocks.is_empty() {
                let partition_deletion =
                    partition_tombstone.map(|pt| (pt.local_deletion_time, pt.deletion_time));
                Some(PendingRowIndex {
                    blocks,
                    partition_deletion,
                })
            } else {
                None
            }
        } else {
            None
        };
        pending.push(PendingBtiPartition {
            raw_key: key.key.clone(),
            data_offset,
            row_index,
        });
    }

    /// Serialize the BTI `Rows.db` + `Partitions.db` components (issue #766 /
    /// #908 / #910).
    ///
    /// Returns `(partitions_path, rows_path)`; both `None` when this is a BIG
    /// writer (no deferred BTI payloads). For BIG nothing is written, keeping
    /// the default path byte-for-byte unchanged.
    ///
    /// Order matters: `Rows.db` is serialized FIRST so each wide partition's
    /// `TrieIndexEntry` offset (`RowsOffset`) is known, then the partition trie
    /// leaves store either that positive `RowsOffset` (wide) or the negative
    /// direct `Data.db` offset (narrow). Cassandra always emits a `Rows.db`
    /// component for a BTI SSTable, even a 0-byte one when no partition is wide
    /// (verified against the real `simple_table`/`collection_table`/`ttl_table`
    /// `da-2-bti-Rows.db` fixtures, all 0 bytes yet listed in the TOC).
    ///
    /// Finding 2 (roborev #908): an EMPTY BTI SSTable (no partitions) cannot
    /// produce a readable `Partitions.db` — a zero-byte trie has no 8-byte root
    /// footer, so the BTI reader rejects it, and a `da` SSTable that omits
    /// `Partitions.db` is unreadable. Rather than publish an unreadable artifact
    /// we REFUSE to finish an empty BTI SSTable with a clear error. (A narrow
    /// non-empty table still publishes a valid trie + a 0-byte `Rows.db`,
    /// matching Cassandra.)
    ///
    /// Takes the deferred state by value (`bti_pending` / `partitions_trie`)
    /// rather than borrowing `&mut self`, so `finish` can call it after
    /// `summary_writer.finish()` has partially moved `self`.
    pub(super) async fn write_bti_components(
        bti_pending: Option<Vec<PendingBtiPartition>>,
        partitions_trie: Option<partitions_writer::PartitionsTrieWriter>,
        cpath: impl Fn(&str) -> PathBuf,
    ) -> Result<(Option<PathBuf>, Option<PathBuf>)> {
        let Some(pending) = bti_pending else {
            return Ok((None, None));
        };

        if pending.is_empty() {
            return Err(Error::InvalidInput(
                "cannot publish an empty BTI SSTable: a `da` SSTable requires a readable \
                 Partitions.db trie (with an 8-byte root footer), which has no valid \
                 zero-partition form. Write at least one partition, or use the BIG format \
                 for empty SSTables."
                    .to_string(),
            ));
        }

        // 1. Serialize Rows.db from the wide partitions, recovering each
        //    wide partition's RowsOffset (in pending-order of wide entries).
        let mut rows_writer = partitions_writer::RowsTrieWriter::new();
        for p in &pending {
            if let Some(ri) = &p.row_index {
                rows_writer.add_partition_row_index(
                    &p.raw_key,
                    p.data_offset,
                    ri.blocks.clone(),
                    ri.partition_deletion,
                );
            }
        }
        let (rows_bytes, rows_offsets) = rows_writer.finish()?;

        // 2. Build the partition trie: wide partitions get their positive
        //    RowsOffset, narrow partitions keep the negative DataOffset.
        let mut trie = partitions_trie.unwrap_or_default();
        let mut wide_idx = 0usize;
        for p in &pending {
            if p.row_index.is_some() {
                let rows_offset = rows_offsets[wide_idx];
                wide_idx += 1;
                trie.add_partition_with_payload(
                    &p.raw_key,
                    partitions_writer::PartitionPayload::RowsOffset(rows_offset),
                );
            } else {
                trie.add_partition_with_payload(
                    &p.raw_key,
                    partitions_writer::PartitionPayload::DataOffset(p.data_offset),
                );
            }
        }
        let partitions_bytes = trie.finish()?;

        // Partitions.db must be non-empty here (pending is non-empty).
        let part_path = cpath("Partitions.db");
        tokio::fs::write(&part_path, partitions_bytes).await?;

        // Rows.db is ALWAYS emitted for BTI (possibly 0 bytes).
        let rows_path = cpath("Rows.db");
        tokio::fs::write(&rows_path, rows_bytes).await?;

        Ok((Some(part_path), Some(rows_path)))
    }
}
