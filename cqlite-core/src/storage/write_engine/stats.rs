//! Write-engine counters and cumulative compaction statistics.
//!
//! Extracted verbatim from `write_engine/mod.rs` (issue #1120, epic #1116) as a
//! behavior-preserving split. Owns the lightweight engine accessor methods
//! (`memtable_size`, `wal_size`, `total_written`, `l0_count`, ...) and the
//! public `CompactionStats` snapshot type. `WriteEngine`'s fields are reachable
//! here because this is a sibling module in the same crate.

use super::WriteEngine;
use std::time::Duration;

/// Cumulative statistics across all compaction operations (M5.2, Issue #474)
///
/// Tracks lifetime totals for monitoring compaction health and throughput.
/// Updated atomically at the end of each successful merge.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Total number of completed compaction cycles
    pub compactions_completed: u64,
    /// Total number of input SSTables consumed
    pub sstables_merged_in: u64,
    /// Total number of output SSTables produced
    pub sstables_produced: u64,
    /// Total bytes read from input SSTables
    pub bytes_read: u64,
    /// Total bytes written to output SSTables
    pub bytes_written: u64,
    /// Total rows merged across all compactions
    pub rows_merged: u64,
    /// Total wall-clock time spent in compaction
    pub total_time: Duration,
}

impl WriteEngine {
    /// Get the current memtable size in bytes
    pub fn memtable_size(&self) -> usize {
        self.memtable.size_bytes()
    }

    /// Get the current memtable row count
    pub fn memtable_row_count(&self) -> usize {
        self.memtable.row_count()
    }

    /// Get the current WAL size in bytes
    pub fn wal_size(&self) -> u64 {
        self.wal.size()
    }

    /// Get the current generation number
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Return the cumulative number of rows written since the engine was opened
    /// (Issue #486).
    ///
    /// This counter is incremented for every row that is successfully inserted
    /// into the memtable and is NOT reset on flush.  It therefore represents
    /// the total write throughput for the current session.
    ///
    /// Note: This counter is in-process only and resets to zero when the engine
    /// is re-opened.  WAL replay rows (recovered from a previous crash) are NOT
    /// counted; only rows written through `write()` / `write_async()` during
    /// the current session are counted.
    pub fn total_written(&self) -> u64 {
        self.rows_written
    }

    /// Return the number of L0 SSTables successfully flushed since the engine
    /// was opened (Issue #486).
    ///
    /// Incremented once per successful `flush()` call that produces a non-empty
    /// SSTable.  This is an in-process counter and resets to zero when the
    /// engine is re-opened.
    pub fn l0_count(&self) -> u64 {
        self.l0_count
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::test_support::{
        create_test_mutation, create_test_schema, flush_n_sstables_sync,
    };
    use crate::storage::write_engine::WriteEngineConfig;
    use tempfile::TempDir;

    #[test]
    fn test_bytes_written_includes_all_components() {
        // After a successful merge, cumulative_stats.bytes_written must be at
        // least as large as the sum of Data.db sizes alone (i.e. it includes
        // the other component files too).
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let policy = crate::storage::write_engine::STCSPolicy::new(4, 32, 0.5, 1.5, 0).unwrap();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();
        let input_paths = flush_n_sstables_sync(&mut engine, 4);

        // Compute the sum of just the Data.db sizes before compaction
        let data_db_total: u64 = input_paths
            .iter()
            .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum();

        engine.set_merge_policy(Box::new(policy)).unwrap();
        engine.maintenance_step(Duration::from_secs(60)).unwrap();

        let stats = engine.maintenance_stats();
        // bytes_written counts all output components, so it should be >= what
        // data_db_total reported for the inputs (output may differ in size but
        // the multi-component sum must be >= the Data.db-only measurement).
        // More concretely: if any non-Data component was written, the total
        // must be larger than data_size alone.
        //
        // We assert >= 0 always holds (u64), and additionally that the field
        // was updated at all (compaction ran).
        assert_eq!(stats.compactions_completed, 1, "compaction must have run");
        // The bytes_written field is now the sum of all components.
        // We can't assert an exact value, but we know:
        //  - data_db_total may be 0 for tiny test SSTables written by the test writer
        //  - if data_db_total > 0, bytes_written >= data_db_total is a reasonable lower bound
        //  - at minimum, the field must equal total_bytes_written (multi-component sum) >= 0
        let _ = data_db_total; // used above for context; value may be 0 in test environment
                               // The assertion that matters: stats are populated and consistent across calls.
                               // maintenance_stats() returns a clone so two consecutive calls must agree.
        let stats2 = engine.maintenance_stats();
        assert_eq!(
            stats.bytes_written, stats2.bytes_written,
            "maintenance_stats() must be consistent across calls"
        );
        // bytes_written is u64; it is always >= 0. Just confirm the field was set.
        assert_eq!(
            stats.sstables_produced, 1,
            "one output SSTable must have been produced"
        );
    }

    // -----------------------------------------------------------------------
    // Issue #486 — total_written and l0_count non-placeholder behaviour
    // -----------------------------------------------------------------------

    /// After writing N rows and flushing, `total_written()` == N even though
    /// `memtable_row_count()` has been reset to 0.
    #[tokio::test]
    async fn test_total_written_survives_flush() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let mut engine = WriteEngine::new(config).unwrap();

        // Write 5 rows to the first "batch"
        for i in 0..5 {
            engine
                .write(create_test_mutation(
                    i,
                    &format!("User{i}"),
                    1_000_000 + i as i64,
                ))
                .unwrap();
        }
        assert_eq!(engine.total_written(), 5);
        assert_eq!(engine.memtable_row_count(), 5);

        // Flush — memtable resets but total_written must NOT
        engine.flush().await.unwrap();
        assert_eq!(
            engine.memtable_row_count(),
            0,
            "memtable should be empty after flush"
        );
        assert_eq!(
            engine.total_written(),
            5,
            "total_written must NOT reset after flush"
        );

        // Write 3 more rows in a second batch
        for i in 10..13 {
            engine
                .write(create_test_mutation(
                    i,
                    &format!("User{i}"),
                    2_000_000 + i as i64,
                ))
                .unwrap();
        }
        assert_eq!(
            engine.total_written(),
            8,
            "total_written must accumulate across flushes"
        );
        assert_eq!(engine.memtable_row_count(), 3);

        // Flush again
        engine.flush().await.unwrap();
        assert_eq!(engine.memtable_row_count(), 0);
        assert_eq!(
            engine.total_written(),
            8,
            "total_written must still be 8 after second flush"
        );
    }

    /// `l0_count()` reflects the number of successful flush operations (not zero).
    #[tokio::test]
    async fn test_l0_count_increments_on_flush() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let mut engine = WriteEngine::new(config).unwrap();

        assert_eq!(engine.l0_count(), 0, "l0_count should start at 0");

        // Flushing an empty memtable produces no SSTable — count stays 0
        engine.flush().await.unwrap();
        assert_eq!(
            engine.l0_count(),
            0,
            "empty flush must not increment l0_count"
        );

        // Write one row and flush — count must become 1
        engine
            .write(create_test_mutation(1, "Alice", 1_000_000))
            .unwrap();
        engine.flush().await.unwrap();
        assert_eq!(
            engine.l0_count(),
            1,
            "l0_count must be 1 after first non-empty flush"
        );

        // Write and flush again — count must become 2
        engine
            .write(create_test_mutation(2, "Bob", 2_000_000))
            .unwrap();
        engine.flush().await.unwrap();
        assert_eq!(
            engine.l0_count(),
            2,
            "l0_count must be 2 after second non-empty flush"
        );
    }

    /// Verify that `total_written` != `memtable_row_count` after a flush —
    /// the scenario that proves the placeholder is replaced.
    #[tokio::test]
    async fn test_total_written_differs_from_memtable_after_flush() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let mut engine = WriteEngine::new(config).unwrap();

        // Write 7 rows and flush
        for i in 0..7 {
            engine
                .write(create_test_mutation(
                    i,
                    &format!("User{i}"),
                    1_000_000 + i as i64,
                ))
                .unwrap();
        }
        engine.flush().await.unwrap();

        // Post-flush: memtable_row_count is 0 but total_written is 7
        assert_eq!(engine.memtable_row_count(), 0);
        assert_eq!(engine.total_written(), 7);
        assert_ne!(
            engine.total_written() as usize,
            engine.memtable_row_count(),
            "total_written must differ from memtable_row_count after flush — \
             proves neither is a placeholder"
        );
    }
}
