//! The cross-generation partition point read: [`SSTableManager::get`] (split out of
//! `mod.rs` per the campsite rule, epic #1116).
//!
//! Two builds, one contract: the default build returns the first matching
//! generation, the `tombstones` build collects every generation's value and
//! reconciles them through [`TombstoneMerger`]. Both walk the SAME resolved reader
//! list (issue #1321) and both are ONE logical read operation.
//!
//! # Why the read metrics live at THIS level (issue #1701, roborev B1)
//!
//! A logical point read may probe several SSTable generations. Metering each
//! per-reader lookup made one `get` emit one `cqlite.read.duration` sample PER
//! CANDIDATE and count a row once per matching generation instead of once per
//! reconciled result — a metric that overstates both the read rate and the read
//! count. So the per-reader lookups are unmetered here
//! ([`SSTableReader::get_with_resolution_unmetered`]) and the whole operation is
//! metered ONCE, format-agnostically, because the generations of one table need not
//! share an on-disk format and a fabricated single label would be a lie.

use super::SSTableManager;
use crate::observability::read_metrics::ReadOpMeter;
use crate::types::{ScanRow, TableId};
use crate::{Result, RowKey};

#[cfg(feature = "tombstones")]
use super::tombstone_merger::{EntryMetadata, GenerationValue, TombstoneMerger};

impl SSTableManager {
    /// Get a value by key from all SSTables with proper tombstone merging
    #[cfg(feature = "tombstones")]
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<ScanRow>> {
        // Resolve the applicable reader list FIRST, exactly like the non-tombstones
        // `get()` path (issue #1321). The previous code iterated EVERY reader in
        // `self.readers` and passed one global relaxed `fully_qualified_match` flag
        // to all of them, so same-named tables in OTHER keyspaces passed the relaxed
        // BTI guard and wrongly contributed values/tombstones to the merge — a
        // cross-keyspace data-bleed bug. `resolve_reader_list` returns precisely the
        // readers for the resolved target table across generations, so the relaxed
        // guard can only ever apply to the readers that ARE the target table; a
        // wrong-keyspace same-named reader is never in the merge set.
        //
        // Issue #1591: snapshot the resolved readers + the authoritative
        // `fully_qualified_match` signal and DROP the read guard before any I/O.
        let (reader_list, fully_qualified_match) = self.resolve_reader_snapshot(table_id).await;
        if reader_list.is_empty() {
            return Ok(None);
        }

        // ONE meter for the WHOLE logical point read (issue #1701, roborev B1): the
        // per-reader lookups below are UNMETERED, so a read that walks N generations
        // reports ONE duration sample and ONE reconciled row — not one per candidate
        // SSTable. FORMAT-AGNOSTIC (`None`): the generations of one table may differ
        // in on-disk format, so no single `sstable.format` label is honest at this
        // grain; the meter never picks one arbitrarily. The single-reader path keeps
        // its labelled meter (`SSTableReader::get_with_resolution`).
        let mut meter = ReadOpMeter::start(None);
        let mut all_values = Vec::new();

        // Collect each applicable generation's value (tombstone-merge semantics are
        // unchanged: still build a `GenerationValue` per reader and resolve via
        // `TombstoneMerger::merge_generations`). Only the SET of readers being merged
        // changed — the resolved list instead of every reader globally.
        for reader in &reader_list {
            if let Some(value) = reader
                .get_with_resolution_unmetered(table_id, key, fully_qualified_match)
                .await?
            {
                let generation = reader.generation;
                let write_time = reader.extract_write_time_from_entry(key, &value);

                let gen_value = GenerationValue {
                    value,
                    metadata: EntryMetadata {
                        write_time,
                        generation,
                        ttl: None, // Would be extracted from SSTable metadata
                    },
                };
                all_values.push(gen_value);
            }
        }

        // Use tombstone merger to resolve conflicts across generations
        let merger = TombstoneMerger::new();
        let reconciled = merger.merge_generations(all_values);
        // Count the RECONCILED result, never the per-generation matches: a row that
        // exists in three generations is ONE row of ONE partition to a reader.
        if let Ok(Some(_)) = &reconciled {
            meter.record_row(key);
        }
        meter.finish();
        reconciled
    }

    /// Get a value by key from all SSTables (simple version without tombstone merging)
    ///
    /// Uses `table_readers` (keyed by fully-qualified `"keyspace.table"`) so that only the
    /// SSTables for the requested table are searched (Issue #680).  Same-named tables in
    /// different keyspaces (e.g. `test_basic.simple_table` and `test_oa.simple_table`) are
    /// now correctly distinguished.
    ///
    /// Lookup order:
    ///   1. Exact match on the full `table_id` string (e.g. `"test_basic.simple_table"`)
    ///   2. Unqualified table name (e.g. `"simple_table"`) — for backward compatibility
    ///      with flat/non-Cassandra directory layouts that have no keyspace parent.
    #[cfg(not(feature = "tombstones"))]
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<ScanRow>> {
        // Issue #1591: snapshot the resolved readers + the authoritative
        // `fully_qualified_match` signal and DROP the read guard before any I/O,
        // so a queued writer never FIFO-parks this point read behind a slow scan.
        //
        // `fully_qualified_match`: did resolution match the FULLY-QUALIFIED
        // `keyspace.table` key exactly, or fall back to the bare table name? An
        // unqualified query is treated as an exact match (no keyspace to mismatch).
        // This authoritative signal gates the get() point-lookup table-consistency
        // guard exactly like the seek path (#1284): only an exact FQ match may relax
        // to a name-only check across a header-keyspace divergence; a fully-qualified
        // query resolved via the bare-name fallback keeps strict keyspace matching so
        // get() never returns another keyspace's same-named rows (issue #1321).
        let (reader_list, fully_qualified_match) = self.resolve_reader_snapshot(table_id).await;

        // ONE meter for the WHOLE logical point read (issue #1701, roborev B1), for
        // the same reason as the `tombstones` variant above: the per-reader lookups
        // are UNMETERED, so walking N generations (a MISS on the first candidate then
        // a hit on the second is the common shape) reports ONE duration sample, not
        // one per candidate. Format-agnostic: generations may differ in format.
        let mut meter = ReadOpMeter::start(None);

        // Return the first value found across all SSTables for this table
        for reader in &reader_list {
            if let Some(value) = reader
                .get_with_resolution_unmetered(table_id, key, fully_qualified_match)
                .await?
            {
                meter.record_row(key);
                meter.finish();
                return Ok(Some(value));
            }
        }

        // A read that resolved ABSENCE still reports its latency (0 rows): dropping it
        // would bias the distribution toward hits.
        meter.finish();
        Ok(None)
    }
}
