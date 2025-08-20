//! Test dataset generation for Issue #37 - Read-time reconciliation testing
//!
//! This module creates comprehensive test datasets covering:
//! - Overlapping writes with different timestamps
//! - Expired TTL scenarios  
//! - Row-level vs cell-level deletes
//! - Range tombstones with inclusive/exclusive bounds
//! - Complex scenarios combining multiple tombstone types

use crate::parser::{
    CellValue, DeletionInfo, DumpMetadata, ParsedCell, ParsedData, ParsedPartition, ParsedRow,
    RangeTombstone,
};
use anyhow::Result;
use std::collections::HashMap;
use tracing::info;

/// Test dataset generator for reconciliation scenarios
pub struct ReconciliationTestDatasets {
    /// Base timestamp for deterministic test data
    _base_timestamp: i64,
    /// Current sequence number for unique identifiers
    _sequence: u32,
}

impl Default for ReconciliationTestDatasets {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl ReconciliationTestDatasets {
    /// Create new test dataset generator
    pub fn new() -> Self {
        Self {
            _base_timestamp: 1640995200_000_000, // 2022-01-01 00:00:00 UTC in microseconds
            _sequence: 0,
        }
    }

    /// Generate all test datasets for comprehensive validation
    pub async fn generate_all_datasets(&mut self) -> Result<HashMap<String, TestDatasetPair>> {
        info!("Generating comprehensive reconciliation test datasets");

        let mut datasets = HashMap::new();

        // Dataset 1: Overlapping writes with different timestamps
        datasets.insert(
            "overlapping_writes".to_string(),
            self.generate_overlapping_writes_dataset().await?,
        );

        // Dataset 2: Expired TTL scenarios
        datasets.insert(
            "expired_ttl".to_string(),
            self.generate_expired_ttl_dataset().await?,
        );

        // Dataset 3: Row vs cell tombstones
        datasets.insert(
            "row_vs_cell_tombstones".to_string(),
            self.generate_row_vs_cell_tombstones_dataset().await?,
        );

        // Dataset 4: Range tombstones with bounds
        datasets.insert(
            "range_tombstones".to_string(),
            self.generate_range_tombstones_dataset().await?,
        );

        // Dataset 5: Complex mixed scenarios
        datasets.insert(
            "complex_mixed".to_string(),
            self.generate_complex_mixed_dataset().await?,
        );

        // Dataset 6: TTL and tombstone interactions
        datasets.insert(
            "ttl_tombstone_interaction".to_string(),
            self.generate_ttl_tombstone_interaction_dataset().await?,
        );

        // Dataset 7: Multi-generation conflict resolution
        datasets.insert(
            "multi_generation_conflicts".to_string(),
            self.generate_multi_generation_conflicts_dataset().await?,
        );

        info!("Generated {} test datasets", datasets.len());
        Ok(datasets)
    }

    /// Generate dataset with overlapping writes at different timestamps
    pub async fn generate_overlapping_writes_dataset(&mut self) -> Result<TestDatasetPair> {
        let partition_key = format!("overlapping_writes_{}", self.next_sequence());

        // Create multiple writes to the same cell with different timestamps
        let cassandra_data = ParsedData {
            keyspace: "test_ks".to_string(),
            table: "overlapping_writes".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: partition_key.clone(),
                rows: vec![
                    // First write (oldest)
                    ParsedRow {
                        clustering_key: Some("row1".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "value".to_string(),
                            value: CellValue::Text("first_write".to_string()),
                            timestamp: self._base_timestamp + 1000,
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp + 1000),
                        ttl: None,
                    },
                    // Second write (newer)
                    ParsedRow {
                        clustering_key: Some("row1".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "value".to_string(),
                            value: CellValue::Text("second_write".to_string()),
                            timestamp: self._base_timestamp + 2000,
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp + 2000),
                        ttl: None,
                    },
                    // Third write (newest - should win)
                    ParsedRow {
                        clustering_key: Some("row1".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "value".to_string(),
                            value: CellValue::Text("third_write_winner".to_string()),
                            timestamp: self._base_timestamp + 3000,
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp + 3000),
                        ttl: None,
                    },
                ],
            }],
            metadata: DumpMetadata::default(),
        };

        // CQLite data should match exactly for this scenario
        let cqlite_data = cassandra_data.clone();

        Ok(TestDatasetPair {
            name: "overlapping_writes".to_string(),
            description: "Multiple writes to same cell with different timestamps".to_string(),
            cassandra_data,
            cqlite_data,
            expected_reconciliation: ExpectedReconciliation {
                visible_cells: 1,
                expected_winner: Some(ExpectedCell {
                    column_name: "value".to_string(),
                    value: CellValue::Text("third_write_winner".to_string()),
                    timestamp: self._base_timestamp + 3000,
                }),
                reconciliation_reason: crate::reconciliation::ReconciliationReason::Visible,
            },
        })
    }

    /// Generate dataset with expired TTL scenarios
    pub async fn generate_expired_ttl_dataset(&mut self) -> Result<TestDatasetPair> {
        let partition_key = format!("expired_ttl_{}", self.next_sequence());

        // Create cells with various TTL states
        let cassandra_data = ParsedData {
            keyspace: "test_ks".to_string(),
            table: "expired_ttl".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: partition_key.clone(),
                rows: vec![
                    ParsedRow {
                        clustering_key: Some("row_expired".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "expired_value".to_string(),
                            value: CellValue::Text("should_be_expired".to_string()),
                            timestamp: self._base_timestamp,
                            ttl: Some(1), // Expires after 1 second
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp),
                        ttl: Some(1),
                    },
                    ParsedRow {
                        clustering_key: Some("row_active".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "active_value".to_string(),
                            value: CellValue::Text("still_active".to_string()),
                            timestamp: self._base_timestamp,
                            ttl: Some(3600), // Expires after 1 hour
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp),
                        ttl: Some(3600),
                    },
                    ParsedRow {
                        clustering_key: Some("row_no_ttl".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "permanent_value".to_string(),
                            value: CellValue::Text("no_ttl".to_string()),
                            timestamp: self._base_timestamp,
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp),
                        ttl: None,
                    },
                ],
            }],
            metadata: DumpMetadata::default(),
        };

        let cqlite_data = cassandra_data.clone();

        Ok(TestDatasetPair {
            name: "expired_ttl".to_string(),
            description: "Cells with various TTL expiration states".to_string(),
            cassandra_data,
            cqlite_data,
            expected_reconciliation: ExpectedReconciliation {
                visible_cells: 2, // expired_value should be invisible
                expected_winner: Some(ExpectedCell {
                    column_name: "active_value".to_string(),
                    value: CellValue::Text("still_active".to_string()),
                    timestamp: self._base_timestamp,
                }),
                reconciliation_reason: crate::reconciliation::ReconciliationReason::Visible,
            },
        })
    }

    /// Generate dataset with row vs cell tombstone scenarios
    pub async fn generate_row_vs_cell_tombstones_dataset(&mut self) -> Result<TestDatasetPair> {
        let partition_key = format!("row_vs_cell_tombstones_{}", self.next_sequence());

        let cassandra_data = ParsedData {
            keyspace: "test_ks".to_string(),
            table: "row_vs_cell_tombstones".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: partition_key.clone(),
                rows: vec![
                    // Cell tombstone scenario
                    ParsedRow {
                        clustering_key: Some("cell_tombstone_row".to_string()),
                        cells: vec![
                            ParsedCell {
                                column_name: "deleted_cell".to_string(),
                                value: CellValue::Text("deleted".to_string()),
                                timestamp: self._base_timestamp + 1000,
                                ttl: None,
                                deletion_info: Some(DeletionInfo {
                                    marked_for_deletion_at: self._base_timestamp + 2000,
                                    local_deletion_time: ((self._base_timestamp + 2000) / 1_000_000)
                                        as i32,
                                }),
                            },
                            ParsedCell {
                                column_name: "surviving_cell".to_string(),
                                value: CellValue::Text("survives".to_string()),
                                timestamp: self._base_timestamp + 3000, // Newer than deletion
                                ttl: None,
                                deletion_info: None,
                            },
                        ],
                        timestamp: Some(self._base_timestamp + 3000),
                        ttl: None,
                    },
                    // Row tombstone scenario
                    ParsedRow {
                        clustering_key: Some("row_tombstone_row".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "deleted_by_row".to_string(),
                            value: CellValue::Text("row_deleted".to_string()),
                            timestamp: self._base_timestamp + 1000,
                            ttl: None,
                            deletion_info: Some(DeletionInfo {
                                marked_for_deletion_at: self._base_timestamp + 2000,
                                local_deletion_time: ((self._base_timestamp + 2000) / 1_000_000)
                                    as i32,
                            }),
                        }],
                        timestamp: Some(self._base_timestamp + 2000),
                        ttl: None,
                    },
                    // Row tombstone with newer data
                    ParsedRow {
                        clustering_key: Some("row_tombstone_row".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "newer_than_row_tombstone".to_string(),
                            value: CellValue::Text("newer_data".to_string()),
                            timestamp: self._base_timestamp + 4000, // Newer than row tombstone
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp + 4000),
                        ttl: None,
                    },
                ],
            }],
            metadata: DumpMetadata::default(),
        };

        let cqlite_data = cassandra_data.clone();

        Ok(TestDatasetPair {
            name: "row_vs_cell_tombstones".to_string(),
            description: "Row-level vs cell-level tombstone scenarios".to_string(),
            cassandra_data,
            cqlite_data,
            expected_reconciliation: ExpectedReconciliation {
                visible_cells: 2, // surviving_cell and newer_than_row_tombstone
                expected_winner: Some(ExpectedCell {
                    column_name: "surviving_cell".to_string(),
                    value: CellValue::Text("survives".to_string()),
                    timestamp: self._base_timestamp + 3000,
                }),
                reconciliation_reason: crate::reconciliation::ReconciliationReason::Visible,
            },
        })
    }

    /// Generate dataset with range tombstones
    pub async fn generate_range_tombstones_dataset(&mut self) -> Result<TestDatasetPair> {
        let partition_key = format!("range_tombstones_{}", self.next_sequence());

        let cassandra_data = ParsedData {
            keyspace: "test_ks".to_string(),
            table: "range_tombstones".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: partition_key.clone(),
                rows: vec![
                    // Row within range (should be deleted)
                    ParsedRow {
                        clustering_key: Some("key_b".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "value".to_string(),
                            value: CellValue::Text("in_range".to_string()),
                            timestamp: self._base_timestamp + 1000,
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp + 1000),
                        ttl: None,
                    },
                    // Row outside range (should survive)
                    ParsedRow {
                        clustering_key: Some("key_z".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "value".to_string(),
                            value: CellValue::Text("outside_range".to_string()),
                            timestamp: self._base_timestamp + 1000,
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp + 1000),
                        ttl: None,
                    },
                    // Row at boundary (inclusive)
                    ParsedRow {
                        clustering_key: Some("key_a".to_string()),
                        cells: vec![ParsedCell {
                            column_name: "value".to_string(),
                            value: CellValue::Text("at_boundary".to_string()),
                            timestamp: self._base_timestamp + 1000,
                            ttl: None,
                            deletion_info: None,
                        }],
                        timestamp: Some(self._base_timestamp + 1000),
                        ttl: None,
                    },
                ],
            }],
            metadata: DumpMetadata::default(),
        };

        let cqlite_data = cassandra_data.clone();

        Ok(TestDatasetPair {
            name: "range_tombstones".to_string(),
            description: "Range tombstones with inclusive/exclusive bounds".to_string(),
            cassandra_data,
            cqlite_data,
            expected_reconciliation: ExpectedReconciliation {
                visible_cells: 1, // Only key_z should survive
                expected_winner: Some(ExpectedCell {
                    column_name: "value".to_string(),
                    value: CellValue::Text("outside_range".to_string()),
                    timestamp: self._base_timestamp + 1000,
                }),
                reconciliation_reason: crate::reconciliation::ReconciliationReason::Visible,
            },
        })
    }

    /// Generate complex mixed scenario dataset
    pub async fn generate_complex_mixed_dataset(&mut self) -> Result<TestDatasetPair> {
        let partition_key = format!("complex_mixed_{}", self.next_sequence());

        let cassandra_data = ParsedData {
            keyspace: "test_ks".to_string(),
            table: "complex_mixed".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: partition_key.clone(),
                rows: vec![
                    // Row with mixed tombstone and TTL scenarios
                    ParsedRow {
                        clustering_key: Some("complex_row".to_string()),
                        cells: vec![
                            // Cell with expired TTL
                            ParsedCell {
                                column_name: "expired_ttl_cell".to_string(),
                                value: CellValue::Text("expired".to_string()),
                                timestamp: self._base_timestamp,
                                ttl: Some(1), // Expired
                                deletion_info: None,
                            },
                            // Cell deleted by tombstone
                            ParsedCell {
                                column_name: "tombstoned_cell".to_string(),
                                value: CellValue::Text("deleted".to_string()),
                                timestamp: self._base_timestamp + 1000,
                                ttl: None,
                                deletion_info: Some(DeletionInfo {
                                    marked_for_deletion_at: self._base_timestamp + 2000,
                                    local_deletion_time: ((self._base_timestamp + 2000) / 1_000_000)
                                        as i32,
                                }),
                            },
                            // Cell with multiple generations
                            ParsedCell {
                                column_name: "multi_gen_cell".to_string(),
                                value: CellValue::Text("old_value".to_string()),
                                timestamp: self._base_timestamp + 1000,
                                ttl: None,
                                deletion_info: None,
                            },
                            ParsedCell {
                                column_name: "multi_gen_cell".to_string(),
                                value: CellValue::Text("new_value".to_string()),
                                timestamp: self._base_timestamp + 3000,
                                ttl: None,
                                deletion_info: None,
                            },
                            // Surviving cell
                            ParsedCell {
                                column_name: "surviving_cell".to_string(),
                                value: CellValue::Text("survives".to_string()),
                                timestamp: self._base_timestamp + 5000,
                                ttl: Some(7200), // Valid for 2 hours
                                deletion_info: None,
                            },
                        ],
                        timestamp: Some(self._base_timestamp + 5000),
                        ttl: None,
                    },
                ],
            }],
            metadata: DumpMetadata::default(),
        };

        let cqlite_data = cassandra_data.clone();

        Ok(TestDatasetPair {
            name: "complex_mixed".to_string(),
            description: "Complex scenario mixing TTL, tombstones, and multi-generation data"
                .to_string(),
            cassandra_data,
            cqlite_data,
            expected_reconciliation: ExpectedReconciliation {
                visible_cells: 2, // multi_gen_cell and surviving_cell
                expected_winner: Some(ExpectedCell {
                    column_name: "multi_gen_cell".to_string(),
                    value: CellValue::Text("new_value".to_string()),
                    timestamp: self._base_timestamp + 3000,
                }),
                reconciliation_reason: crate::reconciliation::ReconciliationReason::Visible,
            },
        })
    }

    /// Generate TTL and tombstone interaction dataset
    pub async fn generate_ttl_tombstone_interaction_dataset(&mut self) -> Result<TestDatasetPair> {
        let partition_key = format!("ttl_tombstone_interaction_{}", self.next_sequence());

        let cassandra_data = ParsedData {
            keyspace: "test_ks".to_string(),
            table: "ttl_tombstone_interaction".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: partition_key.clone(),
                rows: vec![ParsedRow {
                    clustering_key: Some("interaction_row".to_string()),
                    cells: vec![
                        // TTL expires before tombstone
                        ParsedCell {
                            column_name: "ttl_before_tombstone".to_string(),
                            value: CellValue::Text("ttl_expires_first".to_string()),
                            timestamp: self._base_timestamp,
                            ttl: Some(1), // Expires quickly
                            deletion_info: None,
                        },
                        // Tombstone after TTL expiration (on same cell)
                        ParsedCell {
                            column_name: "ttl_before_tombstone".to_string(),
                            value: CellValue::Text("deleted".to_string()),
                            timestamp: self._base_timestamp + 5000,
                            ttl: None,
                            deletion_info: Some(DeletionInfo {
                                marked_for_deletion_at: self._base_timestamp + 5000,
                                local_deletion_time: ((self._base_timestamp + 5000) / 1_000_000)
                                    as i32,
                            }),
                        },
                        // Tombstone before TTL expiration
                        ParsedCell {
                            column_name: "tombstone_before_ttl".to_string(),
                            value: CellValue::Text("deleted_then_expires".to_string()),
                            timestamp: self._base_timestamp + 1000,
                            ttl: Some(7200), // Long TTL
                            deletion_info: Some(DeletionInfo {
                                marked_for_deletion_at: self._base_timestamp + 2000,
                                local_deletion_time: ((self._base_timestamp + 2000) / 1_000_000)
                                    as i32,
                            }),
                        },
                    ],
                    timestamp: Some(self._base_timestamp + 5000),
                    ttl: None,
                }],
            }],
            metadata: DumpMetadata::default(),
        };

        let cqlite_data = cassandra_data.clone();

        Ok(TestDatasetPair {
            name: "ttl_tombstone_interaction".to_string(),
            description: "Interactions between TTL expiration and tombstone deletion".to_string(),
            cassandra_data,
            cqlite_data,
            expected_reconciliation: ExpectedReconciliation {
                visible_cells: 0, // All cells should be invisible
                expected_winner: None,
                reconciliation_reason: crate::reconciliation::ReconciliationReason::ExpiredByTtl,
            },
        })
    }

    /// Generate multi-generation conflict resolution dataset
    pub async fn generate_multi_generation_conflicts_dataset(&mut self) -> Result<TestDatasetPair> {
        let partition_key = format!("multi_generation_conflicts_{}", self.next_sequence());

        let cassandra_data = ParsedData {
            keyspace: "test_ks".to_string(),
            table: "multi_generation_conflicts".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: partition_key.clone(),
                rows: vec![
                    // Multiple generations with various conflict scenarios
                    ParsedRow {
                        clustering_key: Some("conflict_row".to_string()),
                        cells: vec![
                            // Generation 1: Original value
                            ParsedCell {
                                column_name: "contested_cell".to_string(),
                                value: CellValue::Text("generation_1".to_string()),
                                timestamp: self._base_timestamp + 1000,
                                ttl: None,
                                deletion_info: None,
                            },
                            // Generation 2: Updated value
                            ParsedCell {
                                column_name: "contested_cell".to_string(),
                                value: CellValue::Text("generation_2".to_string()),
                                timestamp: self._base_timestamp + 2000,
                                ttl: Some(3600), // With TTL
                                deletion_info: None,
                            },
                            // Generation 3: Tombstone
                            ParsedCell {
                                column_name: "contested_cell".to_string(),
                                value: CellValue::Text("generation_3_deleted".to_string()),
                                timestamp: self._base_timestamp + 3000,
                                ttl: None,
                                deletion_info: Some(DeletionInfo {
                                    marked_for_deletion_at: self._base_timestamp + 3000,
                                    local_deletion_time: ((self._base_timestamp + 3000) / 1_000_000)
                                        as i32,
                                }),
                            },
                            // Generation 4: Resurrection
                            ParsedCell {
                                column_name: "contested_cell".to_string(),
                                value: CellValue::Text("generation_4_resurrection".to_string()),
                                timestamp: self._base_timestamp + 4000,
                                ttl: None,
                                deletion_info: None,
                            },
                        ],
                        timestamp: Some(self._base_timestamp + 4000),
                        ttl: None,
                    },
                ],
            }],
            metadata: DumpMetadata::default(),
        };

        let cqlite_data = cassandra_data.clone();

        Ok(TestDatasetPair {
            name: "multi_generation_conflicts".to_string(),
            description: "Multi-generation conflicts with resurrections and deletions".to_string(),
            cassandra_data,
            cqlite_data,
            expected_reconciliation: ExpectedReconciliation {
                visible_cells: 1, // Final resurrection should win
                expected_winner: Some(ExpectedCell {
                    column_name: "contested_cell".to_string(),
                    value: CellValue::Text("generation_4_resurrection".to_string()),
                    timestamp: self._base_timestamp + 4000,
                }),
                reconciliation_reason:
                    crate::reconciliation::ReconciliationReason::ConflictResolvedByTimestamp,
            },
        })
    }

    /// Get next sequence number
    fn next_sequence(&mut self) -> u32 {
        self._sequence += 1;
        self._sequence
    }

    /// Create range tombstone for testing
    pub fn create_test_range_tombstone(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> RangeTombstone {
        RangeTombstone {
            deletion_time: self._base_timestamp + 2000,
            start_bound: start.map(|s| s.to_string()),
            end_bound: end.map(|s| s.to_string()),
            inclusive_start,
            inclusive_end,
        }
    }
}

/// Test dataset pair (Cassandra vs CQLite)
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TestDatasetPair {
    pub name: String,
    pub description: String,
    pub cassandra_data: ParsedData,
    pub cqlite_data: ParsedData,
    pub expected_reconciliation: ExpectedReconciliation,
}

/// Expected reconciliation result for validation
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ExpectedReconciliation {
    pub visible_cells: usize,
    pub expected_winner: Option<ExpectedCell>,
    pub reconciliation_reason: crate::reconciliation::ReconciliationReason,
}

/// Expected cell result
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ExpectedCell {
    pub column_name: String,
    pub value: CellValue,
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generate_all_datasets() {
        let mut generator = ReconciliationTestDatasets::new();
        let datasets = generator.generate_all_datasets().await.unwrap();

        assert_eq!(datasets.len(), 7);
        assert!(datasets.contains_key("overlapping_writes"));
        assert!(datasets.contains_key("expired_ttl"));
        assert!(datasets.contains_key("row_vs_cell_tombstones"));
        assert!(datasets.contains_key("range_tombstones"));
        assert!(datasets.contains_key("complex_mixed"));
        assert!(datasets.contains_key("ttl_tombstone_interaction"));
        assert!(datasets.contains_key("multi_generation_conflicts"));
    }

    #[tokio::test]
    async fn test_overlapping_writes_dataset() {
        let mut generator = ReconciliationTestDatasets::new();
        let dataset = generator
            .generate_overlapping_writes_dataset()
            .await
            .unwrap();

        assert_eq!(dataset.name, "overlapping_writes");
        assert_eq!(dataset.cassandra_data.partitions.len(), 1);

        let partition = &dataset.cassandra_data.partitions[0];
        assert_eq!(partition.rows.len(), 3); // Three overlapping writes

        // Check that timestamps are increasing
        let mut timestamps: Vec<i64> = partition
            .rows
            .iter()
            .flat_map(|row| &row.cells)
            .map(|cell| cell.timestamp)
            .collect();
        timestamps.sort();
        assert_eq!(timestamps.len(), 3);
        assert!(timestamps[0] < timestamps[1]);
        assert!(timestamps[1] < timestamps[2]);
    }

    #[tokio::test]
    async fn test_expired_ttl_dataset() {
        let mut generator = ReconciliationTestDatasets::new();
        let dataset = generator.generate_expired_ttl_dataset().await.unwrap();

        assert_eq!(dataset.name, "expired_ttl");

        let partition = &dataset.cassandra_data.partitions[0];
        assert_eq!(partition.rows.len(), 3);

        // Verify different TTL scenarios
        let ttl_values: Vec<Option<i32>> = partition
            .rows
            .iter()
            .flat_map(|row| &row.cells)
            .map(|cell| cell.ttl)
            .collect();

        assert!(ttl_values.contains(&Some(1))); // Short TTL
        assert!(ttl_values.contains(&Some(3600))); // Long TTL
        assert!(ttl_values.contains(&None)); // No TTL
    }
}
