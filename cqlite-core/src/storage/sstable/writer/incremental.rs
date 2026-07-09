//! `SSTableWriter`-level incremental partition-write wiring (issue #1668,
//! stage 5c-iv part 2).
//!
//! Wraps `DataWriter::begin_partition_incremental` (stage 5c-iv part 1) with
//! the SAME bookkeeping `SSTableWriter::write_partition` performs around it
//! (token-order validation, key-range/baseline stats, and — once the
//! `DataWriter`-level session finishes — Index.db/Filter.db/Summary.db/BTI
//! registration and partition-count/observability counters), split into two
//! calls so the CALLER (a streaming merge consumer) can drive the
//! `DataWriter`-level session directly in between, feeding one cluster group
//! at a time instead of assembling a whole `Vec<Mutation>` first:
//!
//! ```text
//! let mut session = output_writer.begin_partition_incremental(&key, tombstone, &rts)?;
//! if schema_has_static { session.feed_static_row(&ops, ts, schema)?; }
//! for mutation in cluster_groups { session.feed_row(&mutation, schema)?; }
//! let (offset, blocks, emit) = session.finish(schema)?;
//! output_writer.complete_partition_incremental(&key, tombstone, offset, &blocks, emit)?;
//! ```
//!
//! `begin_partition_incremental` returns `data_writer::IncrementalPartitionWriter<'w, 'r>`
//! DIRECTLY — its lifetime `'w` is tied to the WHOLE `&'w mut SSTableWriter`
//! borrow (Rust cannot narrow a returned borrow to "just one field" across a
//! function boundary), so the caller cannot use `output_writer` for anything
//! else while the session is alive. This is not a real constraint: the
//! caller only ever calls `session.feed_row`/`finish` during that window,
//! never anything else on `output_writer` — and once `finish()` consumes the
//! session, NLL proves the borrow has ended, freeing `output_writer` for the
//! `complete_partition_incremental` call. Splitting `finish()`'s Data.db-only
//! result (offset/blocks/emit — plain owned values, no borrow) out from the
//! remaining bookkeeping is exactly what makes this composable.

use super::data_writer::IncrementalPartitionWriter;
use super::stats_fold;
use super::{PromotedIndexBlock, SSTableWriter, StatisticsMetadata};
use crate::error::{Error, Result};
use crate::storage::sstable::writer::data_writer::PartitionEmitCounts;
use crate::storage::write_engine::mutation::{DecoratedKey, PartitionTombstone, RangeTombstone};

impl SSTableWriter {
    /// Begin an incremental partition write (issue #1668, stage 5c-iv part
    /// 2). Performs the SAME pre-work `write_partition` does before handing
    /// off to `DataWriter` (token-order validation, key-range tracking), then
    /// delegates to `DataWriter::begin_partition_incremental`.
    ///
    /// # Precondition: baselines must be pre-seeded
    ///
    /// Unlike `write_partition` (which has the whole partition's
    /// `Vec<Mutation>` in hand and can fold its stats BEFORE pushing a
    /// baseline to `DataWriter`), this entry point streams mutations in one
    /// at a time and cannot know a partition's minimum LDT/timestamp before
    /// it starts emitting bytes for that partition — buffering ahead to find
    /// out would defeat the entire point of this streaming path (bounding
    /// peak memory to `max_row_width × k`, issue #1668). So this method
    /// requires `self.baselines_locked` (i.e. the caller already called
    /// `pre_seed_encoding_baselines` with the true minimum computed over ALL
    /// inputs) and asserts it rather than silently risking an LDT-delta
    /// underflow panic deep inside `DataWriter`. This is not a real
    /// constraint on the one production caller: `compact_sstables_with_registry`
    /// always pre-seeds baselines before calling `KWayMerger::merge`. A
    /// caller that has not pre-seeded (e.g. a unit test constructing a bare
    /// `SSTableWriter::new`) must call `pre_seed_encoding_baselines` first —
    /// exactly as production does — or use `write_partition` instead.
    ///
    /// The caller must NOT use `self` again until the returned session is
    /// consumed via `IncrementalPartitionWriter::finish` and this writer's
    /// `complete_partition_incremental` is called with its result — see the
    /// module doc.
    pub(crate) fn begin_partition_incremental<'w, 'r>(
        &'w mut self,
        key: &DecoratedKey,
        partition_tombstone: Option<&PartitionTombstone>,
        range_tombstones: &'r [RangeTombstone],
    ) -> Result<IncrementalPartitionWriter<'w, 'r>> {
        debug_assert!(
            self.baselines_locked,
            "begin_partition_incremental requires pre-seeded encoding baselines \
             (call pre_seed_encoding_baselines before streaming partitions) — \
             see the precondition doc on this method, issue #1668"
        );
        if let Some(last_token) = self.last_token {
            if key.token <= last_token {
                return Err(Error::InvalidInput(format!(
                    "Partitions must be written in token order: got token {} after {}",
                    key.token, last_token
                )));
            }
        }
        self.last_token = Some(key.token);
        self.stats.update_key_range(&key.key);

        // No baseline push here (unlike `write_partition`): `baselines_locked`
        // is required (see doc above), so `write_partition`'s
        // `if !self.baselines_locked { update_stats_from_metadata }` gate
        // would always be a no-op anyway.

        self.data_writer.begin_partition_incremental(
            key,
            partition_tombstone,
            range_tombstones,
            &self.schema,
        )
    }

    /// Finalize the SSTableWriter-level bookkeeping for a partition written
    /// via the incremental entry point (issue #1668, stage 5c-iv part 2).
    /// Call this AFTER `IncrementalPartitionWriter::finish` has already
    /// returned its `(offset, blocks, emit)` — this method never touches
    /// `DataWriter` again, only the OTHER `SSTableWriter` fields (Index.db,
    /// Filter.db, Summary.db, BTI, partition-count/observability), mirroring
    /// `write_partition`'s tail EXACTLY (moved, not duplicated in spirit —
    /// see `write_partition`'s own comments for the rationale behind each
    /// step).
    ///
    /// `partition_stats` is the caller's per-partition fold (every mutation
    /// of this partition folded through `stats_fold::fold_mutation_stats` as
    /// it streamed by — the caller cannot fold directly into
    /// `self.stats` while the `IncrementalPartitionWriter` session holds an
    /// exclusive borrow of `self`). Merged in here, once the session's borrow
    /// has ended, via `stats_fold::merge_stats_fold` so the FINAL
    /// `StatisticsMetadata` (max values / tombstone histogram / live-LDT
    /// sentinel / partition-level-deletion flag) matches `write_partition`'s
    /// byte-for-byte, even though `baselines_locked` already made the
    /// DataWriter-baseline half of the old push a no-op.
    pub(crate) fn complete_partition_incremental(
        &mut self,
        key: &DecoratedKey,
        partition_tombstone: Option<&PartitionTombstone>,
        data_offset: u64,
        promoted_blocks: &[PromotedIndexBlock],
        emit_counts: PartitionEmitCounts,
        partition_stats: &StatisticsMetadata,
    ) -> Result<()> {
        stats_fold::merge_stats_fold(&mut self.stats, partition_stats);
        self.stats.row_count += emit_counts.rows;
        self.stats.column_count += emit_counts.columns;

        let partition_serialized_size = self.data_writer.position().saturating_sub(data_offset);
        self.stats
            .record_partition(partition_serialized_size, emit_counts.columns);

        let entry_info =
            self.index_writer
                .add_partition_with_promoted(key, data_offset, promoted_blocks)?;

        if let Some(ref mut filter) = self.filter_writer {
            filter.add_key(key);
        }

        self.queue_bti_partition(key, data_offset, promoted_blocks, partition_tombstone);

        self.summary_writer.note_partition(key);
        if self.summary_sample_counter % self.summary_sample_interval == 0 {
            self.summary_writer
                .add_entry(key, entry_info.index_offset)?;
        }
        self.summary_sample_counter += 1;
        self.partition_count += 1;
        self.stats.increment_partition_count();

        crate::observability::add_counter(crate::observability::catalog::WRITE_PARTITIONS, 1, &[]);

        Ok(())
    }
}
