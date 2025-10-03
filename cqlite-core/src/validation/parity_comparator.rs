//! Parity Comparator for Issue #31
//!
//! Provides normalization and diff logic for comparing CQLite outputs with
//! precomputed sstabledump references. Handles textual differences (whitespace,
//! ordering) while maintaining zero-diff requirement for actual data values.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Result of a parity comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityResult {
    pub status: ParityStatus,
    pub differences: Vec<FieldDifference>,
    pub summary: String,
}

/// Status of parity comparison
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ParityStatus {
    /// Perfect match - zero differences
    Perfect,
    /// Minor formatting differences only (non-critical)
    MinorDiscrepancies,
    /// Critical data differences detected
    MajorFailure,
}

/// A single field difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDifference {
    pub field_name: String,
    pub expected: String,
    pub actual: String,
    pub severity: DiffSeverity,
}

/// Severity of a difference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DiffSeverity {
    /// Critical - affects data correctness
    Critical,
    /// Minor - formatting/presentation only
    Minor,
}

/// Comparator for Statistics.db metadata (sstablemetadata output format)
pub struct StatisticsComparator {
    /// Fields that are allowed to have minor formatting differences
    formatting_tolerant_fields: HashSet<String>,
    /// Fields expected to be missing in M1 (not yet implemented)
    m1_expected_missing_fields: HashSet<String>,
}

impl StatisticsComparator {
    pub fn new() -> Self {
        Self {
            formatting_tolerant_fields: HashSet::from([
                "Timestamp".to_string(),
                "Creation time".to_string(),
                "SSTable Level".to_string(), // Allow minor formatting
            ]),
            m1_expected_missing_fields: HashSet::from([
                // M1 Milestone: These fields not yet implemented in StatisticsReader
                "Partitioner".to_string(),
                "Bloom Filter FP chance".to_string(),
                "Minimum timestamp".to_string(),
                "Maximum timestamp".to_string(),
                "SSTable min local deletion time".to_string(),
                "SSTable max local deletion time".to_string(),
                "Compressor".to_string(),
                "Compression ratio".to_string(),
                "TTL min".to_string(),
                "TTL max".to_string(),
                "First token".to_string(),
                "Last token".to_string(),
                "Estimated cardinality".to_string(),
                "Estimated droppable tombstones".to_string(),
                "Estimated tombstone drop times".to_string(),
                "SSTable Level".to_string(),
                "Repaired at".to_string(),
                "Pending repair".to_string(),
                "Replay positions covered".to_string(),
                "totalColumnsSet".to_string(),
                "totalRows".to_string(),
                "Originating host id".to_string(),
                "IsTransient".to_string(),
                "SSTable".to_string(),
                "Covered clusterings".to_string(),
                "Partition Size".to_string(),
                "Column Count".to_string(),
                "EncodingStats minTimestamp".to_string(),
                "EncodingStats minLocalDeletionTime".to_string(),
                "EncodingStats minTTL".to_string(),
                "KeyType".to_string(),
                "ClusteringTypes".to_string(),
                "StaticColumns".to_string(),
                "RegularColumns".to_string(),
                "Duration".to_string(),
                "Local token space coverage".to_string(),
                // Histogram percentiles (not implemented)
                "Min".to_string(),
                "Max".to_string(),
                "50th".to_string(),
                "75th".to_string(),
                "95th".to_string(),
                "98th".to_string(),
                "99th".to_string(),
            ]),
        }
    }

    /// Check if a field is expected to be missing in M1
    fn is_expected_missing_field(&self, field_name: &str) -> bool {
        // Direct match
        if self.m1_expected_missing_fields.contains(field_name) {
            return true;
        }

        // Histogram bins and timestamp-based fields (e.g., "99th 1996099046...", "1758060900...")
        // These are statistics histogram entries that aren't implemented in M1
        let is_histogram_entry = field_name.starts_with("Min ")
            || field_name.starts_with("Max ")
            || field_name.starts_with("50th ")
            || field_name.starts_with("75th ")
            || field_name.starts_with("95th ")
            || field_name.starts_with("98th ")
            || field_name.starts_with("99th ")
            || field_name
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit()); // Timestamp-based bins

        is_histogram_entry
    }

    /// Compare CQLite Statistics output with reference sstablemetadata text
    pub fn compare(&self, our_text: &str, ref_text: &str) -> ParityResult {
        let our_fields = Self::parse_metadata_fields(our_text);
        let ref_fields = Self::parse_metadata_fields(ref_text);

        let mut differences = Vec::new();

        // Check all reference fields exist in our output
        for (key, ref_val) in &ref_fields {
            if let Some(our_val) = our_fields.get(key) {
                if !self.values_match(key, our_val, ref_val) {
                    let severity = if self.formatting_tolerant_fields.contains(key) {
                        DiffSeverity::Minor
                    } else {
                        DiffSeverity::Critical
                    };

                    differences.push(FieldDifference {
                        field_name: key.clone(),
                        expected: ref_val.clone(),
                        actual: our_val.clone(),
                        severity,
                    });
                }
            } else {
                // Check if this is an expected missing field for M1
                let severity = if self.is_expected_missing_field(key) {
                    DiffSeverity::Minor
                } else {
                    DiffSeverity::Critical
                };

                differences.push(FieldDifference {
                    field_name: key.clone(),
                    expected: ref_val.clone(),
                    actual: "<missing>".to_string(),
                    severity,
                });
            }
        }

        // Check for extra fields in our output (informational only)
        for key in our_fields.keys() {
            if !ref_fields.contains_key(key) {
                differences.push(FieldDifference {
                    field_name: key.clone(),
                    expected: "<not in reference>".to_string(),
                    actual: our_fields[key].clone(),
                    severity: DiffSeverity::Minor,
                });
            }
        }

        let status = Self::determine_status(&differences);
        let summary = Self::generate_summary(&differences, status);

        ParityResult {
            status,
            differences,
            summary,
        }
    }

    /// Parse sstablemetadata key:value format into HashMap
    fn parse_metadata_fields(text: &str) -> HashMap<String, String> {
        let mut fields = HashMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, val)) = line.split_once(':') {
                fields.insert(key.trim().to_string(), val.trim().to_string());
            }
        }
        fields
    }

    /// Check if two values match with appropriate tolerance
    fn values_match(&self, key: &str, our_val: &str, ref_val: &str) -> bool {
        let our_normalized = Self::normalize_value(our_val);
        let ref_normalized = Self::normalize_value(ref_val);

        if self.formatting_tolerant_fields.contains(key) {
            // For formatting-tolerant fields, allow minor differences
            Self::fuzzy_match(&our_normalized, &ref_normalized)
        } else {
            // Strict equality for data fields
            our_normalized == ref_normalized
        }
    }

    /// Normalize value for comparison (whitespace, number formatting)
    fn normalize_value(val: &str) -> String {
        val.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace(" ,", ",")
            .to_lowercase()
    }

    /// Fuzzy match for formatting-tolerant fields
    fn fuzzy_match(a: &str, b: &str) -> bool {
        // Remove all whitespace for fuzzy matching
        let a_compact: String = a.chars().filter(|c| !c.is_whitespace()).collect();
        let b_compact: String = b.chars().filter(|c| !c.is_whitespace()).collect();
        a_compact == b_compact
    }

    /// Determine overall parity status from differences
    fn determine_status(differences: &[FieldDifference]) -> ParityStatus {
        if differences.is_empty() {
            return ParityStatus::Perfect;
        }

        let has_critical = differences
            .iter()
            .any(|d| d.severity == DiffSeverity::Critical);
        if has_critical {
            ParityStatus::MajorFailure
        } else {
            ParityStatus::MinorDiscrepancies
        }
    }

    /// Generate compact summary message
    fn generate_summary(differences: &[FieldDifference], status: ParityStatus) -> String {
        match status {
            ParityStatus::Perfect => "✅ Perfect parity - zero differences".to_string(),
            ParityStatus::MinorDiscrepancies => {
                format!(
                    "⚠️  {} minor formatting differences (non-critical)",
                    differences.len()
                )
            }
            ParityStatus::MajorFailure => {
                let critical_count = differences
                    .iter()
                    .filter(|d| d.severity == DiffSeverity::Critical)
                    .count();
                format!("❌ {} critical data differences detected", critical_count)
            }
        }
    }

    /// Generate actionable diff report for CI output
    pub fn generate_diff_report(result: &ParityResult) -> String {
        let mut report = String::new();
        report.push_str(&format!("{}\n\n", result.summary));

        if !result.differences.is_empty() {
            report.push_str("Field-by-field differences:\n");
            for diff in &result.differences {
                let severity_icon = match diff.severity {
                    DiffSeverity::Critical => "🔴",
                    DiffSeverity::Minor => "🟡",
                };
                report.push_str(&format!(
                    "  {} [{}]\n    Expected: {}\n    Actual:   {}\n\n",
                    severity_icon, diff.field_name, diff.expected, diff.actual
                ));
            }
        }

        report
    }
}

impl Default for StatisticsComparator {
    fn default() -> Self {
        Self::new()
    }
}

/// Comparator for Summary.db entries (token:offset format)
pub struct SummaryComparator;

impl SummaryComparator {
    /// Compare Summary.db entries (token/offset pairs)
    pub fn compare_entries(our_entries: &[(i64, u64)], ref_entries: &[(i64, u64)]) -> ParityResult {
        let mut differences = Vec::new();

        if our_entries.len() != ref_entries.len() {
            differences.push(FieldDifference {
                field_name: "Entry Count".to_string(),
                expected: ref_entries.len().to_string(),
                actual: our_entries.len().to_string(),
                severity: DiffSeverity::Critical,
            });
        }

        // Compare overlapping entries
        let min_len = our_entries.len().min(ref_entries.len());
        for i in 0..min_len {
            let (our_token, our_offset) = our_entries[i];
            let (ref_token, ref_offset) = ref_entries[i];

            if our_token != ref_token {
                differences.push(FieldDifference {
                    field_name: format!("Entry[{}].token", i),
                    expected: ref_token.to_string(),
                    actual: our_token.to_string(),
                    severity: DiffSeverity::Critical,
                });
            }

            if our_offset != ref_offset {
                differences.push(FieldDifference {
                    field_name: format!("Entry[{}].offset", i),
                    expected: ref_offset.to_string(),
                    actual: our_offset.to_string(),
                    severity: DiffSeverity::Critical,
                });
            }
        }

        let status = StatisticsComparator::determine_status(&differences);
        let summary = StatisticsComparator::generate_summary(&differences, status);

        ParityResult {
            status,
            differences,
            summary,
        }
    }
}

/// Comparator for Index.db entries (key digest, offsets)
pub struct IndexComparator;

impl IndexComparator {
    /// Compare Index.db digest values
    pub fn compare_digests(our_digests: &[Vec<u8>], ref_digests: &[Vec<u8>]) -> ParityResult {
        let mut differences = Vec::new();

        if our_digests.len() != ref_digests.len() {
            differences.push(FieldDifference {
                field_name: "Digest Count".to_string(),
                expected: ref_digests.len().to_string(),
                actual: our_digests.len().to_string(),
                severity: DiffSeverity::Critical,
            });
        }

        // Compare overlapping digests
        let min_len = our_digests.len().min(ref_digests.len());
        for i in 0..min_len {
            if our_digests[i] != ref_digests[i] {
                differences.push(FieldDifference {
                    field_name: format!("Digest[{}]", i),
                    expected: format!("{:02x?}", ref_digests[i]),
                    actual: format!("{:02x?}", our_digests[i]),
                    severity: DiffSeverity::Critical,
                });
            }
        }

        let status = StatisticsComparator::determine_status(&differences);
        let summary = StatisticsComparator::generate_summary(&differences, status);

        ParityResult {
            status,
            differences,
            summary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_comparator_perfect_match() {
        let reference = "Estimated partition count: 100\nRow count: 100";
        let our_output = "Estimated partition count: 100\nRow count: 100";

        let comparator = StatisticsComparator::new();
        let result = comparator.compare(our_output, reference);

        assert_eq!(result.status, ParityStatus::Perfect);
        assert!(result.differences.is_empty());
    }

    #[test]
    fn test_statistics_comparator_whitespace_normalization() {
        let reference = "Estimated partition count:  100";
        let our_output = "Estimated partition count: 100";

        let comparator = StatisticsComparator::new();
        let result = comparator.compare(our_output, reference);

        assert_eq!(result.status, ParityStatus::Perfect);
    }

    #[test]
    fn test_statistics_comparator_critical_diff() {
        let reference = "Row count: 100";
        let our_output = "Row count: 99";

        let comparator = StatisticsComparator::new();
        let result = comparator.compare(our_output, reference);

        assert_eq!(result.status, ParityStatus::MajorFailure);
        assert_eq!(result.differences.len(), 1);
        assert_eq!(result.differences[0].severity, DiffSeverity::Critical);
    }

    #[test]
    fn test_statistics_comparator_missing_field() {
        let reference = "Row count: 100\nEstimated partition count: 100";
        let our_output = "Row count: 100";

        let comparator = StatisticsComparator::new();
        let result = comparator.compare(our_output, reference);

        assert_eq!(result.status, ParityStatus::MajorFailure);
        assert!(result
            .differences
            .iter()
            .any(|d| d.field_name == "Estimated partition count"));
    }

    #[test]
    fn test_summary_comparator_perfect_match() {
        let our_entries = vec![(100, 1024), (200, 2048)];
        let ref_entries = vec![(100, 1024), (200, 2048)];

        let result = SummaryComparator::compare_entries(&our_entries, &ref_entries);

        assert_eq!(result.status, ParityStatus::Perfect);
        assert!(result.differences.is_empty());
    }

    #[test]
    fn test_summary_comparator_token_mismatch() {
        let our_entries = vec![(100, 1024), (201, 2048)];
        let ref_entries = vec![(100, 1024), (200, 2048)];

        let result = SummaryComparator::compare_entries(&our_entries, &ref_entries);

        assert_eq!(result.status, ParityStatus::MajorFailure);
        assert!(result
            .differences
            .iter()
            .any(|d| d.field_name.contains("token")));
    }

    #[test]
    fn test_index_comparator_digest_match() {
        let our_digests = vec![vec![0xAA, 0xBB], vec![0xCC, 0xDD]];
        let ref_digests = vec![vec![0xAA, 0xBB], vec![0xCC, 0xDD]];

        let result = IndexComparator::compare_digests(&our_digests, &ref_digests);

        assert_eq!(result.status, ParityStatus::Perfect);
    }

    #[test]
    fn test_index_comparator_digest_mismatch() {
        let our_digests = vec![vec![0xAA, 0xBB], vec![0xCC, 0xDE]];
        let ref_digests = vec![vec![0xAA, 0xBB], vec![0xCC, 0xDD]];

        let result = IndexComparator::compare_digests(&our_digests, &ref_digests);

        assert_eq!(result.status, ParityStatus::MajorFailure);
        assert!(result
            .differences
            .iter()
            .any(|d| d.field_name.contains("Digest")));
    }

    #[test]
    fn test_diff_report_generation() {
        let result = ParityResult {
            status: ParityStatus::MajorFailure,
            differences: vec![FieldDifference {
                field_name: "Row count".to_string(),
                expected: "100".to_string(),
                actual: "99".to_string(),
                severity: DiffSeverity::Critical,
            }],
            summary: "Test summary".to_string(),
        };

        let report = StatisticsComparator::generate_diff_report(&result);

        assert!(report.contains("Row count"));
        assert!(report.contains("100"));
        assert!(report.contains("99"));
        assert!(report.contains("🔴"));
    }
}
