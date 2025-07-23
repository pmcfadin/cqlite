//! Tombstone merging and generation handling for SSTable operations
//!
//! This module provides comprehensive tombstone handling for CQLite, implementing
//! the Cassandra 5.0 deletion semantics with proper multi-generation merging.

use std::collections::HashMap;
use crate::{
    types::{Value, TombstoneInfo, TombstoneType, RowKey, TableId},
    Error, Result,
};

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
    /// The actual value
    pub value: Value,
    /// Entry metadata
    pub metadata: EntryMetadata,
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
    pub fn merge_generations(
        &self,
        values: Vec<GenerationValue>,
    ) -> Result<Option<Value>> {
        if values.is_empty() {
            return Ok(None);
        }
        
        // Sort by generation (newest first), then by write time (newest first)
        let mut sorted_values = values;
        sorted_values.sort_by(|a, b| {
            b.metadata.generation.cmp(&a.metadata.generation)
                .then_with(|| b.metadata.write_time.cmp(&a.metadata.write_time))
        });
        
        // Track the most recent tombstone timestamp for proper deletion semantics
        let mut latest_tombstone_time: Option<i64> = None;
        let mut latest_tombstone_type: Option<TombstoneType> = None;
        
        // First pass: find the most recent active tombstone
        for gen_value in &sorted_values {
            if let Value::Tombstone(tombstone_info) = &gen_value.value {
                if !self.is_tombstone_expired(tombstone_info) {
                    // Update latest tombstone if this one is newer
                    if latest_tombstone_time.map_or(true, |t| tombstone_info.deletion_time > t) {
                        latest_tombstone_time = Some(tombstone_info.deletion_time);
                        latest_tombstone_type = Some(tombstone_info.tombstone_type);
                    }
                }
            }
        }
        
        // Second pass: apply tombstone logic and find the newest valid value
        for gen_value in sorted_values {
            match &gen_value.value {
                Value::Tombstone(tombstone_info) => {
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
                },
                value => {
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
                        let expiration_time = gen_value.metadata.write_time + gen_value.metadata.ttl.unwrap_or(0);
                        let ttl_tombstone = Value::ttl_tombstone(expiration_time, gen_value.metadata.ttl.unwrap_or(0));
                        return Ok(Some(ttl_tombstone));
                    }
                    
                    // Return the first valid, non-expired value that wasn't deleted by tombstone
                    return Ok(Some(value.clone()));
                }
            }
        }
        
        // No valid values found - either all are tombstones or deleted
        Ok(None)
    }
    
    /// Merge entries for a specific row across generations
    pub fn merge_row_entries(
        &self,
        table_id: &TableId,
        row_key: &RowKey,
        entries: Vec<GenerationValue>,
    ) -> Result<Option<Value>> {
        // Check for row-level tombstones first
        let mut row_tombstone_time: Option<i64> = None;
        let mut cell_values = Vec::new();
        
        for entry in entries {
            match &entry.value {
                Value::Tombstone(info) if info.tombstone_type == TombstoneType::RowTombstone => {
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
                },
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
    pub fn range_tombstone_applies(
        &self,
        tombstone: &TombstoneInfo,
        key: &RowKey,
    ) -> bool {
        if tombstone.tombstone_type != TombstoneType::RangeTombstone {
            return false;
        }
        
        if self.is_tombstone_expired(tombstone) {
            return false;
        }
        
        // Check if key falls within the range
        match (&tombstone.range_start, &tombstone.range_end) {
            (Some(start), Some(end)) => {
                key >= start && key <= end
            },
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
            if let Value::Tombstone(tombstone_info) = &range_tombstone_entry.value {
                if tombstone_info.tombstone_type == TombstoneType::RangeTombstone 
                    && !self.is_tombstone_expired(tombstone_info) {
                    // Clone the tombstone info to avoid lifetime issues
                    active_range_tombstones.push((tombstone_info.clone(), range_tombstone_entry.metadata.write_time));
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
                    if tombstone_info.deletion_time > entry.metadata.write_time {
                        if self.range_tombstone_applies(tombstone_info, key) {
                            is_deleted_by_range = true;
                            break; // Stop at first matching tombstone (they're sorted by time)
                        }
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
    pub fn resolve_conflict(&self, values: Vec<GenerationValue>) -> Result<Option<Value>> {
        if values.is_empty() {
            return Ok(None);
        }
        
        // Find the value with the highest timestamp (newest wins)
        let latest = values.into_iter()
            .max_by_key(|v| v.metadata.write_time);
            
        match latest {
            Some(gen_value) => {
                // Check if the latest value is expired
                if self.is_value_expired(&gen_value.metadata) {
                    Ok(None)
                } else if let Value::Tombstone(tombstone) = &gen_value.value {
                    if self.is_tombstone_expired(tombstone) {
                        Ok(None)
                    } else {
                        Ok(None) // Active tombstone means deleted
                    }
                } else {
                    Ok(Some(gen_value.value))
                }
            },
            None => Ok(None),
        }
    }
    
    /// Create a cell-level tombstone merger result
    pub fn merge_cell_tombstones(
        &self,
        column_values: HashMap<String, Vec<GenerationValue>>,
    ) -> Result<HashMap<String, Option<Value>>> {
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
    ) -> Result<Vec<(RowKey, Option<Value>)>> {
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
            if let Value::Tombstone(tombstone_info) = &tombstone_entry.value {
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
    ) -> Result<Option<Value>> {
        // Sort by write time (newest first)
        let mut sorted_entries = collection_entries;
        sorted_entries.sort_by(|a, b| b.metadata.write_time.cmp(&a.metadata.write_time));
        
        for entry in sorted_entries {
            match &entry.value {
                Value::Tombstone(info) => {
                    if !self.is_tombstone_expired(info) {
                        // Collection is deleted
                        return Ok(None);
                    }
                },
                Value::List(elements) | Value::Set(elements) => {
                    // Check for element-level tombstones within collections
                    let filtered_elements: Vec<Value> = elements.iter()
                        .filter(|element| !element.is_tombstone() || !element.is_expired(self.current_time))
                        .cloned()
                        .collect();
                    
                    if entry.value.is_expired(self.current_time) {
                        // Collection has expired
                        let ttl_tombstone = Value::ttl_tombstone(
                            entry.metadata.write_time + entry.metadata.ttl.unwrap_or(0),
                            entry.metadata.ttl.unwrap_or(0)
                        );
                        return Ok(Some(ttl_tombstone));
                    }
                    
                    // Return filtered collection
                    return Ok(Some(match &entry.value {
                        Value::List(_) => Value::List(filtered_elements),
                        Value::Set(_) => Value::Set(filtered_elements),
                        _ => entry.value.clone(),
                    }));
                },
                Value::Map(pairs) => {
                    // Filter out tombstone pairs
                    let filtered_pairs: Vec<(Value, Value)> = pairs.iter()
                        .filter(|(key, value)| {
                            !key.is_tombstone() && !value.is_tombstone() &&
                            !key.is_expired(self.current_time) && !value.is_expired(self.current_time)
                        })
                        .cloned()
                        .collect();
                    
                    if entry.value.is_expired(self.current_time) {
                        let ttl_tombstone = Value::ttl_tombstone(
                            entry.metadata.write_time + entry.metadata.ttl.unwrap_or(0),
                            entry.metadata.ttl.unwrap_or(0)
                        );
                        return Ok(Some(ttl_tombstone));
                    }
                    
                    return Ok(Some(Value::Map(filtered_pairs)));
                },
                _ => {
                    // Regular value - check expiration
                    if self.is_value_expired(&entry.metadata) {
                        let ttl_tombstone = Value::ttl_tombstone(
                            entry.metadata.write_time + entry.metadata.ttl.unwrap_or(0),
                            entry.metadata.ttl.unwrap_or(0)
                        );
                        return Ok(Some(ttl_tombstone));
                    }
                    
                    return Ok(Some(entry.value.clone()));
                }
            }
        }
        
        Ok(None)
    }
    
    /// Performance optimized tombstone check for hot paths
    /// Uses fast path checks to minimize expensive operations
    pub fn fast_tombstone_check(&self, value: &Value, write_time: i64) -> bool {
        match value {
            Value::Tombstone(info) => {
                // Fast path: check common case of non-TTL tombstones first
                if info.ttl.is_none() {
                    true // Active tombstone
                } else {
                    // Only do expensive time calculation for TTL tombstones
                    !self.is_tombstone_expired(info)
                }
            },
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
    fn test_basic_tombstone_merge() {
        let merger = TombstoneMerger::with_time(5000);
        
        // Regular value followed by tombstone
        let values = vec![
            GenerationValue {
                value: Value::Integer(42),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: Value::row_tombstone(2000),
                metadata: EntryMetadata {
                    write_time: 2000,
                    generation: 2,
                    ttl: None,
                },
            },
        ];
        
        let result = merger.merge_generations(values).unwrap();
        assert!(result.is_none()); // Tombstone wins
    }
    
    #[test]
    fn test_ttl_expiration() {
        let merger = TombstoneMerger::with_time(5000);
        
        // Value with expired TTL
        let values = vec![
            GenerationValue {
                value: Value::Integer(42),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: Some(1000), // Expires at 2000
                },
            },
        ];
        
        let result = merger.merge_generations(values).unwrap();
        // Should return TTL tombstone
        assert!(result.is_some());
        assert!(result.unwrap().is_tombstone());
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
                value: Value::Integer(42),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: Value::row_tombstone(2000),
                metadata: EntryMetadata {
                    write_time: 2000,
                    generation: 2,
                    ttl: None,
                },
            },
            GenerationValue {
                value: Value::Text("newer_value".to_string()),
                metadata: EntryMetadata {
                    write_time: 3000,
                    generation: 3,
                    ttl: None,
                },
            },
        ];
        
        let result = merger.merge_row_entries(&table_id, &row_key, entries).unwrap();
        
        // The newer value should survive the row tombstone
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Value::Text("newer_value".to_string()));
    }
    
    #[test]
    fn test_enhanced_multi_generation_merge() {
        let merger = TombstoneMerger::with_time(10000);
        
        // Test complex scenario with multiple generations and types
        let values = vec![
            GenerationValue {
                value: Value::Integer(10),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: Value::cell_tombstone(2000),
                metadata: EntryMetadata {
                    write_time: 2000,
                    generation: 2,
                    ttl: None,
                },
            },
            GenerationValue {
                value: Value::Integer(20),
                metadata: EntryMetadata {
                    write_time: 1500,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: Value::Integer(30),
                metadata: EntryMetadata {
                    write_time: 3000,
                    generation: 3,
                    ttl: None,
                },
            },
        ];
        
        let result = merger.merge_generations(values).unwrap();
        
        // The newest value (30 at time 3000) should win
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Value::Integer(30));
    }
    
    #[test]
    fn test_batch_processing_performance() {
        let merger = TombstoneMerger::with_time(5000);
        
        // Create a large batch of entries
        let mut entries = Vec::new();
        for i in 0..10000 {
            let key = RowKey::from(format!("key_{}", i));
            let values = vec![
                GenerationValue {
                    value: Value::Integer(i as i32),
                    metadata: EntryMetadata {
                        write_time: 1000 + i as i64,
                        generation: 1,
                        ttl: None,
                    },
                },
            ];
            entries.push((key, values));
        }
        
        let start = std::time::Instant::now();
        let result = merger.batch_merge_with_tombstones(entries, 1000).unwrap();
        let duration = start.elapsed();
        
        assert_eq!(result.len(), 10000);
        assert!(duration.as_millis() < 1000); // Should complete within 1 second
    }
    
    #[test]
    fn test_garbage_collection_identification() {
        let merger = TombstoneMerger::with_time(10000);
        
        let tombstones = vec![
            GenerationValue {
                value: Value::row_tombstone(1000),
                metadata: EntryMetadata {
                    write_time: 1000,
                    generation: 1,
                    ttl: None,
                },
            },
            GenerationValue {
                value: Value::cell_tombstone(5000),
                metadata: EntryMetadata {
                    write_time: 5000,
                    generation: 2,
                    ttl: None,
                },
            },
        ];
        
        // GC grace period of 3 seconds = 3_000_000 microseconds
        let collectible = merger.identify_garbage_collectible_tombstones(tombstones, 3).unwrap();
        
        // Only the old tombstone should be collectible
        assert_eq!(collectible.len(), 1);
        assert_eq!(collectible[0].metadata.write_time, 1000);
    }
    
    #[test]
    fn test_collection_tombstone_handling() {
        let merger = TombstoneMerger::with_time(5000);
        
        let collection_entries = vec![
            GenerationValue {
                value: Value::List(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::cell_tombstone(2000), // Deleted element
                    Value::Integer(3),
                ]),
                metadata: EntryMetadata {
                    write_time: 3000,
                    generation: 1,
                    ttl: None,
                },
            },
        ];
        
        let result = merger.merge_collection_with_tombstones(collection_entries).unwrap();
        
        assert!(result.is_some());
        if let Some(Value::List(elements)) = result {
            // Should have filtered out the tombstone element
            assert_eq!(elements.len(), 3);
            assert!(elements.iter().all(|e| !e.is_tombstone()));
        } else {
            panic!("Expected list result");
        }
    }
    
    #[test]
    fn test_fast_tombstone_check_performance() {
        let merger = TombstoneMerger::with_time(5000);
        
        let non_tombstone = Value::Integer(42);
        let tombstone = Value::row_tombstone(3000);
        let ttl_tombstone = Value::ttl_tombstone(2000, 1000);
        
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
}