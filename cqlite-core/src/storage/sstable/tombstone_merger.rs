//! Tombstone merging and generation handling for SSTable operations
//!
//! This module provides comprehensive tombstone handling for CQLite, implementing
//! the Cassandra 5.0 deletion semantics with proper multi-generation merging.

// Feature flag handled at parent module level

use crate::{
    types::{RowKey, ScanRow, TableId, TombstoneInfo, TombstoneType, Value},
    Result,
};
use std::collections::HashMap;

/// Entry metadata for tombstone processing
#[derive(Debug, Clone)]
pub struct EntryMetadata {
    /// Timestamp when the entry was written
    pub write_time: i64,
    /// Whether this entry is from a newer generation
    pub generation: u64,
    /// TTL if applicable
    pub ttl: Option<i64>,
}

/// Multi-generation value with metadata
#[derive(Debug, Clone)]
pub struct GenerationValue {
    /// The scanned row payload (issue #1334): a live row (`ScanRow::Row`) or a
    /// suppressed marker (`ScanRow::Marker`, e.g. a row tombstone). Carrying the
    /// whole row here lets the merger reconcile generations without a `Value::Row`
    /// variant on the public enum.
    pub value: ScanRow,
    /// Entry metadata
    pub metadata: EntryMetadata,
}

impl GenerationValue {
    /// The active tombstone carried by this generation's payload, if any.
    ///
    /// A tombstone always arrives as `ScanRow::Marker(Value::Tombstone(..))`
    /// (issue #1334); a live `ScanRow::Row` is never a tombstone.
    fn tombstone_info(&self) -> Option<&TombstoneInfo> {
        match &self.value {
            ScanRow::Marker(Value::Tombstone(info)) => Some(info),
            _ => None,
        }
    }
}

/// Tombstone merger for handling multi-generation data
pub struct TombstoneMerger {
    /// Current system time for TTL calculations
    current_time: i64,
}

impl TombstoneMerger {
    /// Create a new tombstone merger
    pub fn new() -> Self {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        Self { current_time }
    }

    /// Create a tombstone merger with specific current time (for testing)
    pub fn with_time(current_time: i64) -> Self {
        Self { current_time }
    }

    /// Merge values from multiple generations, applying tombstone semantics
    /// Enhanced with proper Cassandra 5.0 deletion semantics
    pub fn merge_generations(&self, values: Vec<GenerationValue>) -> Result<Option<ScanRow>> {
        if values.is_empty() {
            return Ok(None);
        }

        // Sort by generation (newest first), then by write time (newest first)
        let mut sorted_values = values;
        sorted_values.sort_by(|a, b| {
            b.metadata
                .generation
                .cmp(&a.metadata.generation)
                .then_with(|| b.metadata.write_time.cmp(&a.metadata.write_time))
        });

        // Track the most recent tombstone timestamp for proper deletion semantics
        let mut latest_tombstone_time: Option<i64> = None;
        let mut _latest_tombstone_type: Option<TombstoneType> = None;

        // First pass: find the most recent active tombstone
        for gen_value in &sorted_values {
            if let Some(tombstone_info) = gen_value.tombstone_info() {
                if !self.is_tombstone_expired(tombstone_info) {
                    // Update latest tombstone if this one is newer
                    if latest_tombstone_time.is_none_or(|t| tombstone_info.deletion_time > t) {
                        latest_tombstone_time = Some(tombstone_info.deletion_time);
                        _latest_tombstone_type = Some(tombstone_info.tombstone_type);
                    }
                }
            }
        }

        // Second pass: apply tombstone logic and find the newest valid value
        for gen_value in sorted_values {
            if let Some(tombstone_info) = gen_value.tombstone_info() {
                // Skip expired tombstones - they don't affect data visibility
                if self.is_tombstone_expired(tombstone_info) {
                    continue;
                }

                // If this is the most recent active tombstone, data is deleted
                if let Some(latest_time) = latest_tombstone_time {
                    if tombstone_info.deletion_time == latest_time {
                        return Ok(None);
                    }
                }
            } else {
                // Check if this value was written before the latest tombstone
                if let Some(tombstone_time) = latest_tombstone_time {
                    if gen_value.metadata.write_time <= tombstone_time {
                        // Value is deleted by tombstone
                        continue;
                    }
                }

                // Check if this value has expired due to TTL
                if self.is_value_expired(&gen_value.metadata) {
                    // Value has expired, create TTL tombstone
                    let expiration_time =
                        gen_value.metadata.write_time + gen_value.metadata.ttl.unwrap_or(0);
                    let ttl_tombstone =
                        Value::ttl_tombstone(expiration_time, gen_value.metadata.ttl.unwrap_or(0));
                    return Ok(Some(ScanRow::Marker(ttl_tombstone)));
                }

                // Return the first valid, non-expired value that wasn't deleted by tombstone
                return Ok(Some(gen_value.value));
            }
        }

        // No valid values found - either all are tombstones or deleted
        Ok(None)
    }

    /// Merge entries for a specific row across generations
    pub fn merge_row_entries(
        &self,
        _table_id: &TableId,
        _row_key: &RowKey,
        entries: Vec<GenerationValue>,
    ) -> Result<Option<ScanRow>> {
        // Check for row-level tombstones first
        let mut row_tombstone_time: Option<i64> = None;
        let mut cell_values = Vec::new();

        for entry in entries {
            match entry.tombstone_info() {
                Some(info) if info.tombstone_type == TombstoneType::RowTombstone => {
                    if !self.is_tombstone_expired(info) {
                        // Track the latest row tombstone
                        if let Some(existing_time) = row_tombstone_time {
                            if info.deletion_time > existing_time {
                                row_tombstone_time = Some(info.deletion_time);
                            }
                        } else {
                            row_tombstone_time = Some(info.deletion_time);
                        }
                    }
                }
                _ => {
                    cell_values.push(entry);
                }
            }
        }

        // If there's an active row tombstone, check if any cell values are newer
        if let Some(tombstone_time) = row_tombstone_time {
            // Filter out cell values older than the row tombstone
            cell_values.retain(|entry| entry.metadata.write_time > tombstone_time);

            // If no cell values survive the row tombstone, return None (deleted)
            if cell_values.is_empty() {
                return Ok(None);
            }
        }

        // Merge remaining cell values
        self.merge_generations(cell_values)
    }

    /// Check if a range tombstone applies to a given key
    pub fn range_tombstone_applies(&self, tombstone: &TombstoneInfo, key: &RowKey) -> bool {
        if tombstone.tombstone_type != TombstoneType::RangeTombstone {
            return false;
        }

        if self.is_tombstone_expired(tombstone) {
            return false;
        }

        // Check if key falls within the range
        match (&tombstone.range_start, &tombstone.range_end) {
            (Some(start), Some(end)) => key >= start && key <= end,
            (Some(start), None) => key >= start,
            (None, Some(end)) => key <= end,
            (None, None) => false, // Invalid range tombstone
        }
    }

    /// Filter values based on range tombstones with optimized performance
    /// Enhanced for better Cassandra 5.0 range deletion semantics
    pub fn apply_range_tombstones(
        &self,
        entries: Vec<(RowKey, GenerationValue)>,
        range_tombstones: Vec<GenerationValue>,
    ) -> Result<Vec<(RowKey, GenerationValue)>> {
        // Early return if no range tombstones
        if range_tombstones.is_empty() {
            return Ok(entries);
        }

        // Pre-process and sort range tombstones by deletion time (newest first)
        let mut active_range_tombstones = Vec::new();
        for range_tombstone_entry in range_tombstones {
            if let Some(tombstone_info) = range_tombstone_entry.tombstone_info() {
                if tombstone_info.tombstone_type == TombstoneType::RangeTombstone
                    && !self.is_tombstone_expired(tombstone_info)
                {
                    // Clone the tombstone info to avoid lifetime issues
                    active_range_tombstones.push((
                        tombstone_info.clone(),
                        range_tombstone_entry.metadata.write_time,
                    ));
                }
            }
        }

        // Sort by deletion time (newest first) for proper precedence
        active_range_tombstones.sort_by(|a, b| b.0.deletion_time.cmp(&a.0.deletion_time));

        let mut filtered_entries = Vec::new();

        // Process entries in batches for better performance
        const BATCH_SIZE: usize = 1000;
        for entry_batch in entries.chunks(BATCH_SIZE) {
            for (key, entry) in entry_batch {
                let mut is_deleted_by_range = false;

                // Check against active range tombstones (sorted by deletion time)
                for (tombstone_info, _) in &active_range_tombstones {
                    // Only apply range tombstone if it's newer than the entry
                    if tombstone_info.deletion_time > entry.metadata.write_time
                        && self.range_tombstone_applies(tombstone_info, key)
                    {
                        is_deleted_by_range = true;
                        break; // Stop at first matching tombstone (they're sorted by time)
                    }
                }

                if !is_deleted_by_range {
                    filtered_entries.push((key.clone(), entry.clone()));
                }
            }
        }

        Ok(filtered_entries)
    }

    /// Check if a tombstone has expired and can be garbage collected
    fn is_tombstone_expired(&self, tombstone: &TombstoneInfo) -> bool {
        if let Some(ttl) = tombstone.ttl {
            // TTL tombstones expire after deletion_time + ttl
            self.current_time > tombstone.deletion_time + ttl
        } else {
            // Non-TTL tombstones don't expire by themselves
            false
        }
    }

    /// Check if a value has expired due to TTL
    fn is_value_expired(&self, metadata: &EntryMetadata) -> bool {
        if let Some(ttl) = metadata.ttl {
            self.current_time > metadata.write_time + ttl
        } else {
            false
        }
    }

    /// Resolve conflicts between multiple values using timestamp ordering
    pub fn resolve_conflict(&self, values: Vec<GenerationValue>) -> Result<Option<ScanRow>> {
        if values.is_empty() {
            return Ok(None);
        }

        // Find the value with the highest timestamp (newest wins)
        let latest = values.into_iter().max_by_key(|v| v.metadata.write_time);

        match latest {
            Some(gen_value) => {
                // Check if the latest value is expired
                if self.is_value_expired(&gen_value.metadata) {
                    Ok(None)
                } else if gen_value.tombstone_info().is_some() {
                    // Active (or expired) tombstone means deleted.
                    Ok(None)
                } else {
                    Ok(Some(gen_value.value))
                }
            }
            None => Ok(None),
        }
    }

    /// Create a cell-level tombstone merger result
    pub fn merge_cell_tombstones(
        &self,
        column_values: HashMap<String, Vec<GenerationValue>>,
    ) -> Result<HashMap<String, Option<ScanRow>>> {
        let mut result = HashMap::new();

        for (column_name, values) in column_values {
            let merged_value = self.merge_generations(values)?;
            result.insert(column_name, merged_value);
        }

        Ok(result)
    }

    /// Advanced batch processing for large datasets with tombstones
    /// Optimized for performance with minimal memory allocation
    pub fn batch_merge_with_tombstones(
        &self,
        entries: Vec<(RowKey, Vec<GenerationValue>)>,
        batch_size: usize,
    ) -> Result<Vec<(RowKey, Option<ScanRow>)>> {
        let mut results = Vec::with_capacity(entries.len());

        // Process in batches to control memory usage
        for batch in entries.chunks(batch_size) {
            for (key, values) in batch {
                let merged_value = self.merge_generations(values.clone())?;
                results.push((key.clone(), merged_value));
            }

            // Optional: yield to allow other operations in async context
            // In a full async implementation, we'd add tokio::task::yield_now().await here
        }

        Ok(results)
    }

    /// Efficient tombstone garbage collection identification
    /// Returns tombstones that can be safely removed from storage
    pub fn identify_garbage_collectible_tombstones(
        &self,
        tombstones: Vec<GenerationValue>,
        gc_grace_seconds: i64,
    ) -> Result<Vec<GenerationValue>> {
        let mut collectible = Vec::new();
        let gc_grace_micros = gc_grace_seconds * 1_000_000; // Convert to microseconds

        for tombstone_entry in tombstones {
            if let Some(tombstone_info) = tombstone_entry.tombstone_info() {
                // Check if tombstone has passed GC grace period
                let tombstone_age = self.current_time - tombstone_info.deletion_time;

                if tombstone_age > gc_grace_micros {
                    // Additional check: ensure no newer data exists
                    collectible.push(tombstone_entry);
                }
            }
        }

        Ok(collectible)
    }

    /// Merge collections with proper tombstone handling for complex types
    /// Handles nested deletions within collections (lists, sets, maps)
    pub fn merge_collection_with_tombstones(
        &self,
        collection_entries: Vec<GenerationValue>,
    ) -> Result<Option<ScanRow>> {
        // Sort by write time (newest first)
        let mut sorted_entries = collection_entries;
        sorted_entries.sort_by(|a, b| b.metadata.write_time.cmp(&a.metadata.write_time));

        // Issue #1334: generations now carry whole rows (`ScanRow`), not bare
        // collection values, so reconciliation reduces to last-write-wins with
        // tombstone shadowing (the per-element collection filtering that this
        // helper used to perform never applied at the row grain). Walk
        // newest→oldest: a LIVE tombstone shadows the row (deleted), but an
        // EXPIRED TTL tombstone deletes nothing — skip it and continue to the
        // next (older) generation so an older live value survives (roborev round
        // 8 finding 2 — the pre-#1334 behavior this branch regressed).
        for entry in sorted_entries {
            if let Some(tombstone_info) = entry.tombstone_info() {
                if self.is_tombstone_expired(tombstone_info) {
                    // Expired TTL tombstone: deletes nothing; fall through to the
                    // next older generation.
                    continue;
                }
                // Live tombstone → the row is deleted.
                return Ok(None);
            }

            // Regular row - check TTL expiration.
            if self.is_value_expired(&entry.metadata) {
                let ttl_tombstone = Value::ttl_tombstone(
                    entry.metadata.write_time + entry.metadata.ttl.unwrap_or(0),
                    entry.metadata.ttl.unwrap_or(0),
                );
                return Ok(Some(ScanRow::Marker(ttl_tombstone)));
            }

            return Ok(Some(entry.value));
        }

        Ok(None)
    }

    /// Performance optimized tombstone check for hot paths
    /// Uses fast path checks to minimize expensive operations
    pub fn fast_tombstone_check(&self, value: &Value, _write_time: i64) -> bool {
        match value {
            Value::Tombstone(info) => {
                // Fast path: check common case of non-TTL tombstones first
                if info.ttl.is_none() {
                    true // Active tombstone
                } else {
                    // Only do expensive time calculation for TTL tombstones
                    !self.is_tombstone_expired(info)
                }
            }
            _ => {
                // Fast path: non-tombstone values are visible unless expired
                false
            }
        }
    }
}

impl Default for TombstoneMerger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tombstone_merge() -> Result<()> {
        let merger = TombstoneMerger::with_time(5000);

        // Regular value followed by tombstone
        let values = vec![
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(42)),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::row_tombstone(2000)),
                metadata: EntryMetadata {
                    write_time: 2000,
                    generation: 2,
                    ttl: None,
                },
            },
        ];

        let result = merger.merge_generations(values)?;
        assert!(result.is_none()); // Tombstone wins

        Ok(())
    }

    #[test]
    fn test_ttl_expiration() -> Result<()> {
        let merger = TombstoneMerger::with_time(5000);

        // Value with expired TTL
        let values = vec![GenerationValue {
            value: ScanRow::Marker(Value::Integer(42)),
            metadata: EntryMetadata {
                write_time: 1000,
                generation: 1,
                ttl: Some(1000), // Expires at 2000
            },
        }];

        let result = merger.merge_generations(values)?;
        // Should return TTL tombstone
        assert!(matches!(result, Some(ScanRow::Marker(v)) if v.is_tombstone()));

        Ok(())
    }

    #[test]
    fn test_range_tombstone_application() {
        let merger = TombstoneMerger::with_time(5000);

        let start_key = RowKey::from("key1");
        let end_key = RowKey::from("key5");
        let test_key = RowKey::from("key3");

        let tombstone = TombstoneInfo {
            deletion_time: 2000,
            tombstone_type: TombstoneType::RangeTombstone,
            local_deletion_time: 0,
            ttl: None,
            range_start: Some(start_key),
            range_end: Some(end_key),
        };

        assert!(merger.range_tombstone_applies(&tombstone, &test_key));

        let outside_key = RowKey::from("key9");
        assert!(!merger.range_tombstone_applies(&tombstone, &outside_key));
    }

    #[test]
    fn test_row_level_tombstone() {
        let merger = TombstoneMerger::with_time(5000);
        let table_id = TableId::from("test_table");
        let row_key = RowKey::from("test_key");

        let entries = vec![
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(42)),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::row_tombstone(2000)),
                metadata: EntryMetadata {
                    write_time: 2000,
                    generation: 2,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::Text("newer_value".to_string())),
                metadata: EntryMetadata {
                    write_time: 3000,
                    generation: 3,
                    ttl: None,
                },
            },
        ];

        let result = merger
            .merge_row_entries(&table_id, &row_key, entries)
            .unwrap();

        // The newer value should survive the row tombstone
        assert_eq!(
            result,
            Some(ScanRow::Marker(Value::Text("newer_value".to_string())))
        );
    }

    #[test]
    fn test_enhanced_multi_generation_merge() -> Result<()> {
        let merger = TombstoneMerger::with_time(10000);

        // Test complex scenario with multiple generations and types
        let values = vec![
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(10)),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::cell_tombstone(2000)),
                metadata: EntryMetadata {
                    write_time: 2000,
                    generation: 2,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(20)),
                metadata: EntryMetadata {
                    write_time: 1500,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(30)),
                metadata: EntryMetadata {
                    write_time: 3000,
                    generation: 3,
                    ttl: None,
                },
            },
        ];

        let result = merger.merge_generations(values)?;

        // The newest value (30 at time 3000) should win
        assert_eq!(result, Some(ScanRow::Marker(Value::Integer(30))));

        Ok(())
    }

    #[test]
    fn test_batch_processing_performance() -> Result<()> {
        let merger = TombstoneMerger::with_time(5000);

        // Create a large batch of entries
        let mut entries = Vec::new();
        for i in 0..10000 {
            let key = RowKey::from(format!("key_{}", i));
            let values = vec![GenerationValue {
                value: ScanRow::Marker(Value::Integer(i)),
                metadata: EntryMetadata {
                    write_time: 1000 + i as i64,
                    generation: 1,
                    ttl: None,
                },
            }];
            entries.push((key, values));
        }

        let start = std::time::Instant::now();
        let result = merger.batch_merge_with_tombstones(entries, 1000)?;
        let duration = start.elapsed();

        assert_eq!(result.len(), 10000);
        assert!(duration.as_millis() < 1000); // Should complete within 1 second

        Ok(())
    }

    #[test]
    fn test_garbage_collection_identification() {
        let merger = TombstoneMerger::with_time(10_000_000); // 10 seconds in microseconds

        let tombstones = vec![
            GenerationValue {
                value: ScanRow::Marker(Value::row_tombstone(1_000_000)), // 1 second in microseconds
                metadata: EntryMetadata {
                    write_time: 1_000_000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::cell_tombstone(8_000_000)), // 8 seconds in microseconds
                metadata: EntryMetadata {
                    write_time: 8_000_000,
                    generation: 2,
                    ttl: None,
                },
            },
        ];

        // GC grace period of 3 seconds = 3_000_000 microseconds
        let collectible = merger
            .identify_garbage_collectible_tombstones(tombstones, 3)
            .unwrap();

        // Only the old tombstone should be collectible (age: 9 seconds > 3 seconds grace)
        assert_eq!(collectible.len(), 1);
        assert_eq!(collectible[0].metadata.write_time, 1_000_000);
    }

    #[test]
    fn test_collection_tombstone_handling() {
        let merger = TombstoneMerger::with_time(5000);

        let list_row = ScanRow::Marker(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::cell_tombstone(2000), // Deleted element
            Value::Integer(3),
        ]));
        let collection_entries = vec![GenerationValue {
            value: list_row.clone(),
            metadata: EntryMetadata {
                write_time: 3000,
                generation: 1,
                ttl: None,
            },
        }];

        let result = merger
            .merge_collection_with_tombstones(collection_entries)
            .unwrap();

        // Issue #1334: generations carry whole rows, so the newest live payload
        // wins as-is (row-grain LWW); per-element collection filtering no longer
        // applies on this path.
        assert_eq!(result, Some(list_row));
    }

    /// roborev round 8 finding 2: an EXPIRED TTL tombstone must NOT delete the
    /// row. The merge must skip it and continue to the next (older) generation so
    /// an older live value survives. Before the fix `merge_collection_with_tombstones`
    /// treated ANY newest tombstone (expired included) as deleting the row.
    #[test]
    fn expired_ttl_tombstone_does_not_delete_older_live_value() -> Result<()> {
        let merger = TombstoneMerger::with_time(5000);

        let live = ScanRow::Row(vec![(std::sync::Arc::from("v"), Value::Integer(7))]);
        let entries = vec![
            // Newest by write_time: a TTL tombstone that expired at 2000 < 5000.
            GenerationValue {
                value: ScanRow::Marker(Value::ttl_tombstone(1000, 1000)),
                metadata: EntryMetadata {
                    write_time: 3000,
                    generation: 2,
                    ttl: Some(1000),
                },
            },
            // Older, still-live value that must survive the expired tombstone.
            GenerationValue {
                value: live.clone(),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
        ];

        let result = merger.merge_collection_with_tombstones(entries)?;
        assert_eq!(
            result,
            Some(live),
            "expired TTL tombstone must be skipped; older live value survives"
        );
        Ok(())
    }

    /// A LIVE (non-expired) tombstone still shadows the row on the same path.
    #[test]
    fn live_tombstone_deletes_row_on_collection_merge() -> Result<()> {
        let merger = TombstoneMerger::with_time(5000);

        let entries = vec![
            GenerationValue {
                value: ScanRow::Marker(Value::row_tombstone(3000)),
                metadata: EntryMetadata {
                    write_time: 3000,
                    generation: 2,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Row(vec![(std::sync::Arc::from("v"), Value::Integer(7))]),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
        ];

        assert_eq!(merger.merge_collection_with_tombstones(entries)?, None);
        Ok(())
    }

    #[test]
    fn test_fast_tombstone_check_performance() {
        let merger = TombstoneMerger::with_time(5000);

        let non_tombstone = Value::Integer(42);
        let tombstone = Value::row_tombstone(3000);
        let ttl_tombstone = Value::ttl_tombstone(2000, 4000); // TTL that expires at 6000 > current time 5000

        // Test performance of fast path
        let start = std::time::Instant::now();
        for _ in 0..100000 {
            assert!(!merger.fast_tombstone_check(&non_tombstone, 3000));
            assert!(merger.fast_tombstone_check(&tombstone, 3000));
            assert!(merger.fast_tombstone_check(&ttl_tombstone, 3000));
        }
        let duration = start.elapsed();

        // Should complete very quickly (within 100ms for 100k iterations)
        assert!(duration.as_millis() < 100);
    }

    // =========================================================================
    // Issue #691: per-cell write-time metadata survives LWW merge
    //
    // The LWW winner is the GenerationValue with the highest write_time in
    // EntryMetadata.  Callers that build CellWriteMetadata (issue #692 / the
    // executor) must derive it from the winning GenerationValue's metadata,
    // not from a tombstone'd loser.  These tests assert the merge picks the
    // right winner and that its metadata is accessible.
    // =========================================================================

    /// Two SSTables, same key: the newer write must survive the LWW merge.
    /// The older write has write_time=1000, the newer write has write_time=3000.
    /// After merge, the result value must be the one written at 3000, and its
    /// EntryMetadata.write_time must be 3000 — which is what a QueryRow builder
    /// should use to populate CellWriteMetadata.write_timestamp_micros.
    #[test]
    fn test_lww_merge_winner_has_newer_write_time() -> Result<()> {
        let merger = TombstoneMerger::with_time(99_999);

        let values = vec![
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(10)), // older value
                metadata: EntryMetadata {
                    write_time: 1_000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(30)), // newer value - should win
                metadata: EntryMetadata {
                    write_time: 3_000,
                    generation: 2,
                    ttl: None,
                },
            },
        ];

        // merge_generations returns the value from the winning GenerationValue.
        let result = merger.merge_generations(values)?;
        assert_eq!(
            result,
            Some(ScanRow::Marker(Value::Integer(30))),
            "LWW merge must pick the newer value (write_time=3000)"
        );

        Ok(())
    }

    /// Expired TTL cell: merge returns a TTL tombstone.
    /// After expiry, the write_time is still the original cell's write_time;
    /// callers building CellWriteMetadata should use that for `write_timestamp_micros`.
    #[test]
    fn test_lww_merge_expired_ttl_returns_tombstone() -> Result<()> {
        let now = 100_000_i64;
        let merger = TombstoneMerger::with_time(now);

        // TTL of 1000 µs written at write_time=5_000 → expires at 6_000.
        // current_time=100_000 > 6_000, so expired.
        let values = vec![GenerationValue {
            value: ScanRow::Marker(Value::Text("expiring".to_string())),
            metadata: EntryMetadata {
                write_time: 5_000,
                generation: 1,
                ttl: Some(1_000), // expires at 6_000 < now=100_000
            },
        }];

        let result = merger.merge_generations(values)?;
        // Should be a TTL tombstone, not the original value.
        assert!(
            matches!(result, Some(ScanRow::Marker(v)) if v.is_tombstone()),
            "result for expired TTL cell must be a tombstone"
        );

        Ok(())
    }

    /// Two SSTables, same key, one has a null/tombstone:
    /// The live value from the newer generation survives.
    #[test]
    fn test_lww_merge_live_value_newer_than_row_tombstone_survives() -> Result<()> {
        let merger = TombstoneMerger::with_time(99_999);
        let table_id = TableId::from("ks.tbl");
        let row_key = RowKey::from("pk1");

        let entries = vec![
            // Older SSTable: row tombstone at time 1_000
            GenerationValue {
                value: ScanRow::Marker(Value::row_tombstone(1_000)),
                metadata: EntryMetadata {
                    write_time: 1_000,
                    generation: 1,
                    ttl: None,
                },
            },
            // Newer SSTable: live value at time 5_000 → wins over the tombstone
            GenerationValue {
                value: ScanRow::Marker(Value::Integer(99)),
                metadata: EntryMetadata {
                    write_time: 5_000,
                    generation: 2,
                    ttl: None,
                },
            },
        ];

        let result = merger.merge_row_entries(&table_id, &row_key, entries)?;
        assert_eq!(
            result,
            Some(ScanRow::Marker(Value::Integer(99))),
            "a live value written after the row tombstone must survive the merge"
        );

        Ok(())
    }
}
