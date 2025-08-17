use crate::parser::{CellValue, ParsedCell, ParsedData, ParsedPartition, ParsedRow};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonResult {
    pub summary: ComparisonSummary,
    pub differences: Vec<CellDifference>,
    pub statistics: ComparisonStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonSummary {
    pub total_cells_compared: u64,
    pub matching_cells: u64,
    pub different_cells: u64,
    pub missing_in_cassandra: u64,
    pub missing_in_cqlite: u64,
    pub compatibility_score: f64, // 0.0 to 1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellDifference {
    pub location: CellLocation,
    pub difference_type: DifferenceType,
    pub cassandra_value: Option<CellValue>,
    pub cqlite_value: Option<CellValue>,
    pub severity: DifferenceSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellLocation {
    pub partition_key: String,
    pub clustering_key: Option<String>,
    pub column_name: String,
    pub row_index: usize,
    pub cell_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DifferenceType {
    ValueMismatch,
    TimestampMismatch,
    TtlMismatch,
    TypeMismatch,
    MissingInCassandra,
    MissingInCqlite,
    DeletionInfoMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum DifferenceSeverity {
    Critical, // Data corruption or major incompatibility
    High,     // Significant functional difference
    Medium,   // Minor compatibility issue
    Low,      // Cosmetic or formatting difference
    Info,     // Informational only
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonStatistics {
    pub cassandra_partitions: usize,
    pub cqlite_partitions: usize,
    pub cassandra_rows: usize,
    pub cqlite_rows: usize,
    pub cassandra_cells: usize,
    pub cqlite_cells: usize,
    pub comparison_duration_ms: u128,
}

pub struct CellByCell {
    // Configuration for comparison sensitivity
    pub zero_tolerance: bool,
    pub ignore_timestamp_precision: bool,
    pub ignore_formatting_differences: bool,
}

impl Default for CellByCell {
    fn default() -> Self {
        Self::new()
    }
}

impl CellByCell {
    pub fn new() -> Self {
        Self {
            zero_tolerance: true, // Default to zero tolerance as per requirements
            ignore_timestamp_precision: false,
            ignore_formatting_differences: false,
        }
    }

    #[allow(dead_code)]
    pub fn with_zero_tolerance(mut self, zero_tolerance: bool) -> Self {
        self.zero_tolerance = zero_tolerance;
        self
    }

    /// Perform comprehensive cell-by-cell comparison
    pub async fn compare_cell_by_cell(
        &self,
        cassandra_data: &ParsedData,
        cqlite_data: &ParsedData,
    ) -> Result<ComparisonResult> {
        let start_time = std::time::Instant::now();

        debug!("Starting cell-by-cell comparison");
        debug!("Cassandra: {} partitions", cassandra_data.partitions.len());
        debug!("CQLite: {} partitions", cqlite_data.partitions.len());

        let mut differences = Vec::new();
        let mut total_cells = 0u64;
        let mut matching_cells = 0u64;

        // Create lookup maps for efficient comparison
        let cassandra_lookup = self.create_partition_lookup(&cassandra_data.partitions);
        let cqlite_lookup = self.create_partition_lookup(&cqlite_data.partitions);

        // Compare all partitions from Cassandra
        for (partition_key, cassandra_partition) in &cassandra_lookup {
            if let Some(cqlite_partition) = cqlite_lookup.get(partition_key) {
                let partition_diffs = self
                    .compare_partitions(
                        partition_key,
                        cassandra_partition,
                        cqlite_partition,
                        &mut total_cells,
                        &mut matching_cells,
                    )
                    .await?;
                differences.extend(partition_diffs);
            } else {
                // Partition missing in CQLite
                for (row_idx, row) in cassandra_partition.rows.iter().enumerate() {
                    for (cell_idx, cell) in row.cells.iter().enumerate() {
                        differences.push(CellDifference {
                            location: CellLocation {
                                partition_key: partition_key.clone(),
                                clustering_key: row.clustering_key.clone(),
                                column_name: cell.column_name.clone(),
                                row_index: row_idx,
                                cell_index: cell_idx,
                            },
                            difference_type: DifferenceType::MissingInCqlite,
                            cassandra_value: Some(cell.value.clone()),
                            cqlite_value: None,
                            severity: DifferenceSeverity::Critical,
                        });
                        total_cells += 1;
                    }
                }
            }
        }

        // Check for partitions that exist only in CQLite
        for (partition_key, cqlite_partition) in &cqlite_lookup {
            if !cassandra_lookup.contains_key(partition_key) {
                // Partition exists only in CQLite
                for (row_idx, row) in cqlite_partition.rows.iter().enumerate() {
                    for (cell_idx, cell) in row.cells.iter().enumerate() {
                        differences.push(CellDifference {
                            location: CellLocation {
                                partition_key: partition_key.clone(),
                                clustering_key: row.clustering_key.clone(),
                                column_name: cell.column_name.clone(),
                                row_index: row_idx,
                                cell_index: cell_idx,
                            },
                            difference_type: DifferenceType::MissingInCassandra,
                            cassandra_value: None,
                            cqlite_value: Some(cell.value.clone()),
                            severity: DifferenceSeverity::Critical,
                        });
                        total_cells += 1;
                    }
                }
            }
        }

        let different_cells = differences.len() as u64;
        let compatibility_score = if total_cells > 0 {
            matching_cells as f64 / total_cells as f64
        } else {
            1.0
        };

        let duration = start_time.elapsed().as_millis();

        let result = ComparisonResult {
            summary: ComparisonSummary {
                total_cells_compared: total_cells,
                matching_cells,
                different_cells,
                missing_in_cassandra: differences
                    .iter()
                    .filter(|d| matches!(d.difference_type, DifferenceType::MissingInCassandra))
                    .count() as u64,
                missing_in_cqlite: differences
                    .iter()
                    .filter(|d| matches!(d.difference_type, DifferenceType::MissingInCqlite))
                    .count() as u64,
                compatibility_score,
            },
            differences,
            statistics: ComparisonStatistics {
                cassandra_partitions: cassandra_data.partitions.len(),
                cqlite_partitions: cqlite_data.partitions.len(),
                cassandra_rows: cassandra_data.partitions.iter().map(|p| p.rows.len()).sum(),
                cqlite_rows: cqlite_data.partitions.iter().map(|p| p.rows.len()).sum(),
                cassandra_cells: cassandra_lookup
                    .values()
                    .flat_map(|p| &p.rows)
                    .map(|r| r.cells.len())
                    .sum(),
                cqlite_cells: cqlite_lookup
                    .values()
                    .flat_map(|p| &p.rows)
                    .map(|r| r.cells.len())
                    .sum(),
                comparison_duration_ms: duration,
            },
        };

        if result.has_differences() {
            warn!(
                "Cell-by-cell comparison found {} differences",
                result.difference_count()
            );
            if self.zero_tolerance {
                error!("ZERO TOLERANCE MODE: Any differences will fail validation");
            }
        } else {
            debug!("Perfect match: No differences found");
        }

        Ok(result)
    }

    async fn compare_partitions(
        &self,
        partition_key: &str,
        cassandra_partition: &ParsedPartition,
        cqlite_partition: &ParsedPartition,
        total_cells: &mut u64,
        matching_cells: &mut u64,
    ) -> Result<Vec<CellDifference>> {
        let mut differences = Vec::new();

        // Create row lookup for efficient comparison
        let cassandra_rows = self.create_row_lookup(&cassandra_partition.rows);
        let cqlite_rows = self.create_row_lookup(&cqlite_partition.rows);

        // Compare rows
        for (clustering_key, cassandra_row) in &cassandra_rows {
            if let Some(cqlite_row) = cqlite_rows.get(clustering_key) {
                let row_diffs = self
                    .compare_rows(
                        partition_key,
                        clustering_key,
                        cassandra_row,
                        cqlite_row,
                        total_cells,
                        matching_cells,
                    )
                    .await?;
                differences.extend(row_diffs);
            } else {
                // Row missing in CQLite
                for (cell_idx, cell) in cassandra_row.cells.iter().enumerate() {
                    differences.push(CellDifference {
                        location: CellLocation {
                            partition_key: partition_key.to_string(),
                            clustering_key: Some(clustering_key.clone()),
                            column_name: cell.column_name.clone(),
                            row_index: 0,
                            cell_index: cell_idx,
                        },
                        difference_type: DifferenceType::MissingInCqlite,
                        cassandra_value: Some(cell.value.clone()),
                        cqlite_value: None,
                        severity: DifferenceSeverity::Critical,
                    });
                    *total_cells += 1;
                }
            }
        }

        // Check for rows that exist only in CQLite
        for (clustering_key, cqlite_row) in &cqlite_rows {
            if !cassandra_rows.contains_key(clustering_key) {
                for (cell_idx, cell) in cqlite_row.cells.iter().enumerate() {
                    differences.push(CellDifference {
                        location: CellLocation {
                            partition_key: partition_key.to_string(),
                            clustering_key: Some(clustering_key.clone()),
                            column_name: cell.column_name.clone(),
                            row_index: 0,
                            cell_index: cell_idx,
                        },
                        difference_type: DifferenceType::MissingInCassandra,
                        cassandra_value: None,
                        cqlite_value: Some(cell.value.clone()),
                        severity: DifferenceSeverity::Critical,
                    });
                    *total_cells += 1;
                }
            }
        }

        Ok(differences)
    }

    async fn compare_rows(
        &self,
        partition_key: &str,
        clustering_key: &str,
        cassandra_row: &ParsedRow,
        cqlite_row: &ParsedRow,
        total_cells: &mut u64,
        matching_cells: &mut u64,
    ) -> Result<Vec<CellDifference>> {
        let mut differences = Vec::new();

        // Create cell lookup
        let cassandra_cells = self.create_cell_lookup(&cassandra_row.cells);
        let cqlite_cells = self.create_cell_lookup(&cqlite_row.cells);

        // Compare cells
        for (column_name, cassandra_cell) in &cassandra_cells {
            *total_cells += 1;

            if let Some(cqlite_cell) = cqlite_cells.get(column_name) {
                if let Some(diff) = self.compare_cells(
                    partition_key,
                    clustering_key,
                    column_name,
                    cassandra_cell,
                    cqlite_cell,
                ) {
                    differences.push(diff);
                } else {
                    *matching_cells += 1;
                }
            } else {
                // Cell missing in CQLite
                differences.push(CellDifference {
                    location: CellLocation {
                        partition_key: partition_key.to_string(),
                        clustering_key: Some(clustering_key.to_string()),
                        column_name: column_name.clone(),
                        row_index: 0,
                        cell_index: 0,
                    },
                    difference_type: DifferenceType::MissingInCqlite,
                    cassandra_value: Some(cassandra_cell.value.clone()),
                    cqlite_value: None,
                    severity: DifferenceSeverity::Critical,
                });
            }
        }

        // Check for cells that exist only in CQLite
        for (column_name, cqlite_cell) in &cqlite_cells {
            if !cassandra_cells.contains_key(column_name) {
                *total_cells += 1;
                differences.push(CellDifference {
                    location: CellLocation {
                        partition_key: partition_key.to_string(),
                        clustering_key: Some(clustering_key.to_string()),
                        column_name: column_name.clone(),
                        row_index: 0,
                        cell_index: 0,
                    },
                    difference_type: DifferenceType::MissingInCassandra,
                    cassandra_value: None,
                    cqlite_value: Some(cqlite_cell.value.clone()),
                    severity: DifferenceSeverity::Critical,
                });
            }
        }

        Ok(differences)
    }

    fn compare_cells(
        &self,
        partition_key: &str,
        clustering_key: &str,
        column_name: &str,
        cassandra_cell: &ParsedCell,
        cqlite_cell: &ParsedCell,
    ) -> Option<CellDifference> {
        // Compare cell values
        if !self.values_match(&cassandra_cell.value, &cqlite_cell.value) {
            return Some(CellDifference {
                location: CellLocation {
                    partition_key: partition_key.to_string(),
                    clustering_key: Some(clustering_key.to_string()),
                    column_name: column_name.to_string(),
                    row_index: 0,
                    cell_index: 0,
                },
                difference_type: DifferenceType::ValueMismatch,
                cassandra_value: Some(cassandra_cell.value.clone()),
                cqlite_value: Some(cqlite_cell.value.clone()),
                severity: DifferenceSeverity::Critical,
            });
        }

        // Compare timestamps if not ignoring precision
        if !self.ignore_timestamp_precision && cassandra_cell.timestamp != cqlite_cell.timestamp {
            return Some(CellDifference {
                location: CellLocation {
                    partition_key: partition_key.to_string(),
                    clustering_key: Some(clustering_key.to_string()),
                    column_name: column_name.to_string(),
                    row_index: 0,
                    cell_index: 0,
                },
                difference_type: DifferenceType::TimestampMismatch,
                cassandra_value: Some(cassandra_cell.value.clone()),
                cqlite_value: Some(cqlite_cell.value.clone()),
                severity: if self.zero_tolerance {
                    DifferenceSeverity::Critical
                } else {
                    DifferenceSeverity::Medium
                },
            });
        }

        // Compare TTL values
        if cassandra_cell.ttl != cqlite_cell.ttl {
            return Some(CellDifference {
                location: CellLocation {
                    partition_key: partition_key.to_string(),
                    clustering_key: Some(clustering_key.to_string()),
                    column_name: column_name.to_string(),
                    row_index: 0,
                    cell_index: 0,
                },
                difference_type: DifferenceType::TtlMismatch,
                cassandra_value: Some(cassandra_cell.value.clone()),
                cqlite_value: Some(cqlite_cell.value.clone()),
                severity: DifferenceSeverity::High,
            });
        }

        None // No differences found
    }

    fn values_match(&self, val1: &CellValue, val2: &CellValue) -> bool {
        match (val1, val2) {
            (CellValue::Null, CellValue::Null) => true,
            (CellValue::Text(s1), CellValue::Text(s2)) => {
                if self.ignore_formatting_differences {
                    s1.trim() == s2.trim()
                } else {
                    s1 == s2
                }
            }
            (CellValue::Integer(i1), CellValue::Integer(i2)) => i1 == i2,
            (CellValue::Boolean(b1), CellValue::Boolean(b2)) => b1 == b2,
            (CellValue::Float(f1), CellValue::Float(f2)) => {
                // Handle floating point comparison with epsilon
                (f1 - f2).abs() < f64::EPSILON
            }
            (CellValue::Uuid(u1), CellValue::Uuid(u2)) => u1 == u2,
            (CellValue::Timestamp(t1), CellValue::Timestamp(t2)) => t1 == t2,
            (CellValue::Blob(b1), CellValue::Blob(b2)) => b1 == b2,
            _ => false, // Different types or unhandled cases
        }
    }

    // Helper methods for creating lookup tables

    fn create_partition_lookup<'a>(
        &self,
        partitions: &'a [ParsedPartition],
    ) -> HashMap<String, &'a ParsedPartition> {
        partitions
            .iter()
            .map(|p| (p.partition_key.clone(), p))
            .collect()
    }

    fn create_row_lookup<'a>(&self, rows: &'a [ParsedRow]) -> HashMap<String, &'a ParsedRow> {
        rows.iter()
            .enumerate()
            .map(|(idx, row)| {
                let key = row
                    .clustering_key
                    .clone()
                    .unwrap_or_else(|| format!("row_{idx}"));
                (key, row)
            })
            .collect()
    }

    fn create_cell_lookup<'a>(&self, cells: &'a [ParsedCell]) -> HashMap<String, &'a ParsedCell> {
        cells
            .iter()
            .map(|cell| (cell.column_name.clone(), cell))
            .collect()
    }
}

impl ComparisonResult {
    pub fn has_differences(&self) -> bool {
        !self.differences.is_empty()
    }

    pub fn difference_count(&self) -> usize {
        self.differences.len()
    }

    pub fn critical_differences(&self) -> impl Iterator<Item = &CellDifference> {
        self.differences
            .iter()
            .filter(|d| d.severity == DifferenceSeverity::Critical)
    }

    pub fn report(&self) -> String {
        format!(
            "Comparison Report:\n\
             - Total cells compared: {}\n\
             - Matching cells: {}\n\
             - Different cells: {}\n\
             - Compatibility score: {:.2}%\n\
             - Critical differences: {}\n",
            self.summary.total_cells_compared,
            self.summary.matching_cells,
            self.summary.different_cells,
            self.summary.compatibility_score * 100.0,
            self.critical_differences().count()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::*;

    #[tokio::test]
    async fn test_identical_data_comparison() {
        let comparator = CellByCell::new();

        let data = create_test_data();
        let result = comparator.compare_cell_by_cell(&data, &data).await.unwrap();

        assert!(!result.has_differences());
        assert_eq!(result.summary.compatibility_score, 1.0);
    }

    #[tokio::test]
    async fn test_different_values_comparison() {
        let comparator = CellByCell::new();

        let data1 = create_test_data();
        let mut data2 = create_test_data();

        // Modify one value
        if let Some(partition) = data2.partitions.get_mut(0) {
            if let Some(row) = partition.rows.get_mut(0) {
                if let Some(cell) = row.cells.get_mut(0) {
                    cell.value = CellValue::Text("different_value".to_string());
                }
            }
        }

        let result = comparator
            .compare_cell_by_cell(&data1, &data2)
            .await
            .unwrap();

        assert!(result.has_differences());
        assert!(result.summary.compatibility_score < 1.0);
    }

    fn create_test_data() -> ParsedData {
        ParsedData {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partitions: vec![ParsedPartition {
                partition_key: "partition1".to_string(),
                rows: vec![ParsedRow {
                    clustering_key: None,
                    cells: vec![ParsedCell {
                        column_name: "col1".to_string(),
                        value: CellValue::Text("value1".to_string()),
                        timestamp: 1234567890,
                        ttl: None,
                        deletion_info: None,
                    }],
                    timestamp: Some(1234567890),
                    ttl: None,
                }],
            }],
            metadata: DumpMetadata::default(),
        }
    }
}
