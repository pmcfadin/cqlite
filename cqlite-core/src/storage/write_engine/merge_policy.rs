//! Compaction merge policies for SSTable selection (M5.2)
//!
//! This module implements compaction strategies for selecting which SSTables
//! to merge. The primary implementation is Size-Tiered Compaction Strategy (STCS),
//! which groups SSTables by size into buckets and selects buckets for compaction
//! when they exceed a threshold.
//!
//! ## STCS Algorithm
//!
//! 1. Group SSTables by size into buckets where sizes are within a ratio range
//!    (controlled by `bucket_low` and `bucket_high`)
//! 2. Select buckets with at least `min_threshold` SSTables
//! 3. Limit selected buckets to at most `max_threshold` SSTables
//!
//! ## References
//!
//! - Cassandra 5.0 SizeTieredCompactionStrategy:
//!   https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/compaction/SizeTieredCompactionStrategy.java

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::PathBuf;

/// Metadata about an SSTable for compaction selection
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
struct SSTableMetadata {
    /// Path to the Data.db file
    data_path: PathBuf,
    /// Size of the Data.db file in bytes
    data_size: u64,
}

#[cfg(feature = "write-support")]
impl SSTableMetadata {
    /// Create new SSTable metadata
    fn new(data_path: PathBuf, data_size: u64) -> Self {
        Self {
            data_path,
            data_size,
        }
    }
}

/// Size-Tiered Compaction Strategy (STCS) policy
///
/// Groups SSTables of similar size into buckets and selects buckets
/// for compaction when they contain enough SSTables.
///
/// ## Algorithm
///
/// 1. Sort SSTables by size
/// 2. Group into buckets where each file's size is within [bucket_low, bucket_high]
///    ratio of the bucket's average size
/// 3. Select buckets with at least `min_threshold` SSTables
/// 4. Limit to `max_threshold` SSTables per compaction
///
/// ## Parameters
///
/// - `min_threshold`: Minimum number of SSTables to trigger compaction (default: 4)
/// - `max_threshold`: Maximum number of SSTables to compact at once (default: 32)
/// - `bucket_low`: Lower bound ratio for bucket grouping (default: 0.5)
/// - `bucket_high`: Upper bound ratio for bucket grouping (default: 1.5)
/// - `min_sstable_size`: Minimum size for applying bucket ratio logic (default: 50MB)
///
/// ## Example
///
/// ```rust,ignore
/// use cqlite_core::storage::write_engine::STCSPolicy;
///
/// let policy = STCSPolicy::default();
/// let paths = policy.select_merge(&candidate_paths)?;
/// ```
///
/// ## References
///
/// Based on Cassandra's SizeTieredCompactionStrategy (5.0.0)
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct STCSPolicy {
    /// Minimum number of SSTables to trigger compaction
    pub min_threshold: usize,
    /// Maximum number of SSTables to compact at once
    pub max_threshold: usize,
    /// Lower bound ratio for bucket grouping (e.g., 0.5 = 50%)
    pub bucket_low: f64,
    /// Upper bound ratio for bucket grouping (e.g., 1.5 = 150%)
    pub bucket_high: f64,
    /// Minimum SSTable size in bytes for applying bucket ratio logic
    /// SSTables smaller than this are grouped together regardless of ratio
    pub min_sstable_size: u64,
}

#[cfg(feature = "write-support")]
impl STCSPolicy {
    /// Default minimum SSTable size (50 MB)
    pub const DEFAULT_MIN_SSTABLE_SIZE: u64 = 50 * 1024 * 1024;

    /// Create a new STCS policy with custom parameters
    pub fn new(
        min_threshold: usize,
        max_threshold: usize,
        bucket_low: f64,
        bucket_high: f64,
        min_sstable_size: u64,
    ) -> Result<Self> {
        // Validate parameters
        if min_threshold == 0 {
            return Err(Error::InvalidInput(
                "min_threshold must be greater than 0".to_string(),
            ));
        }

        if max_threshold < min_threshold {
            return Err(Error::InvalidInput(format!(
                "max_threshold ({}) must be >= min_threshold ({})",
                max_threshold, min_threshold
            )));
        }

        if bucket_high <= bucket_low {
            return Err(Error::InvalidInput(format!(
                "bucket_high ({}) must be > bucket_low ({})",
                bucket_high, bucket_low
            )));
        }

        if bucket_low <= 0.0 {
            return Err(Error::InvalidInput(format!(
                "bucket_low ({}) must be > 0.0",
                bucket_low
            )));
        }

        Ok(Self {
            min_threshold,
            max_threshold,
            bucket_low,
            bucket_high,
            min_sstable_size,
        })
    }

    /// Group SSTables into size buckets
    ///
    /// This is the core STCS bucketing algorithm from Cassandra.
    /// SSTables are grouped by size where each SSTable's size is within
    /// the [bucket_low, bucket_high] ratio of the bucket's average size.
    ///
    /// Small SSTables (< min_sstable_size) are grouped together regardless
    /// of exact size ratios.
    fn group_into_buckets(&self, sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableMetadata>> {
        if sstables.is_empty() {
            return Vec::new();
        }

        // Sort by size (ascending), tie-breaking on path so equal-sized inputs
        // are processed in a fully deterministic order (independent of the
        // caller's candidate ordering).
        let mut sorted = sstables.to_vec();
        sorted.sort_by(|a, b| {
            a.data_size
                .cmp(&b.data_size)
                .then_with(|| a.data_path.cmp(&b.data_path))
        });

        // Map of average size -> bucket. The HashMap is retained for O(1)
        // average-key updates, but the fit-search below never iterates it
        // directly: iteration order of `HashMap` is unspecified, which would
        // make bucket membership nondeterministic across runs (issue #1666).
        let mut buckets: HashMap<u64, Vec<SSTableMetadata>> = HashMap::new();

        for sstable in sorted {
            let size = sstable.data_size;

            // Look for a bucket containing similar-sized files. We iterate the
            // existing bucket average-sizes in ascending order (a deterministic
            // total order over the unique u64 keys) so that the first-fit
            // decision is reproducible and prefers the smallest matching tier,
            // matching Cassandra's smallest-tier-first intent.
            let mut avg_sizes: Vec<u64> = buckets.keys().copied().collect();
            avg_sizes.sort_unstable();

            let mut found_bucket = false;
            let mut old_average = 0u64;

            for &avg_size in &avg_sizes {
                // Check if this SSTable fits in the bucket:
                // 1. Size is within [bucket_low, bucket_high] ratio of bucket average
                // 2. OR both this SSTable and bucket average are below min_sstable_size
                let within_ratio = (size as f64) >= (avg_size as f64 * self.bucket_low)
                    && (size as f64) <= (avg_size as f64 * self.bucket_high);

                let both_small = size < self.min_sstable_size && avg_size < self.min_sstable_size;

                if within_ratio || both_small {
                    old_average = avg_size;
                    found_bucket = true;
                    break;
                }
            }

            if found_bucket {
                // Remove bucket under old average
                if let Some(mut bucket) = buckets.remove(&old_average) {
                    // Calculate new average size
                    let total_size = (bucket.len() as u64).saturating_mul(old_average);
                    let new_average = total_size.saturating_add(size) / (bucket.len() as u64 + 1);

                    // Add SSTable to bucket
                    bucket.push(sstable);

                    // Re-insert under new average
                    buckets.insert(new_average, bucket);
                }
            } else {
                // No matching bucket found, create new one
                buckets.insert(size, vec![sstable]);
            }
        }

        // Convert to a deterministically ordered Vec of buckets.
        let mut result: Vec<Vec<SSTableMetadata>> = buckets.into_values().collect();

        // Sort each bucket's members by (size, path) so that any downstream
        // `take(max_threshold)` selects a stable subset.
        for bucket in &mut result {
            bucket.sort_by(|a, b| {
                a.data_size
                    .cmp(&b.data_size)
                    .then_with(|| a.data_path.cmp(&b.data_path))
            });
        }

        // Order buckets smallest-tier-first: representative size is the bucket's
        // minimum member size (its first element after the sort above), with the
        // corresponding path as a total tie-break. Empty buckets never occur, but
        // are ordered last defensively without panicking.
        result.sort_by(|a, b| {
            let key = |b: &[SSTableMetadata]| match b.first() {
                Some(s) => (s.data_size, Some(s.data_path.clone())),
                None => (u64::MAX, None),
            };
            key(a).cmp(&key(b))
        });

        result
    }
}

#[cfg(feature = "write-support")]
impl Default for STCSPolicy {
    /// Create STCS policy with Cassandra defaults:
    /// - min_threshold: 4
    /// - max_threshold: 32
    /// - bucket_low: 0.5
    /// - bucket_high: 1.5
    /// - min_sstable_size: 50MB
    fn default() -> Self {
        Self {
            min_threshold: 4,
            max_threshold: 32,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: Self::DEFAULT_MIN_SSTABLE_SIZE,
        }
    }
}

#[cfg(feature = "write-support")]
impl STCSPolicy {
    /// Select SSTables for compaction using STCS algorithm
    ///
    /// This method implements the MergePolicy trait's select_merge interface.
    /// It loads file sizes, groups into buckets, and selects the first eligible bucket.
    fn select_merge_internal(&self, candidates: &[PathBuf]) -> Result<Vec<PathBuf>> {
        // Need at least min_threshold SSTables to compact
        if candidates.len() < self.min_threshold {
            return Ok(Vec::new());
        }

        // Load file sizes for all candidates
        let mut sstables = Vec::new();
        for path in candidates {
            // Get file size
            let metadata = std::fs::metadata(path).map_err(|e| {
                Error::Storage(format!(
                    "Failed to read SSTable metadata for {:?}: {}",
                    path, e
                ))
            })?;

            sstables.push(SSTableMetadata::new(path.clone(), metadata.len()));
        }

        // Group into buckets. `group_into_buckets` returns buckets ordered
        // smallest-tier-first with a total tie-break, and each bucket's members
        // sorted by (size, path), so the selection below is fully deterministic
        // (issue #1666).
        let buckets = self.group_into_buckets(&sstables);

        // Pick the smallest eligible tier: the first bucket (smallest
        // representative size) with at least min_threshold SSTables.
        // (In real Cassandra, this would use hotness/read metrics to select best bucket.)
        for bucket in buckets {
            if bucket.len() >= self.min_threshold {
                // Limit to max_threshold SSTables
                let selected: Vec<PathBuf> = bucket
                    .into_iter()
                    .take(self.max_threshold)
                    .map(|s| s.data_path)
                    .collect();

                return Ok(selected);
            }
        }

        Ok(Vec::new())
    }
}

// Implement the MergePolicy trait from parent module
#[cfg(feature = "write-support")]
impl super::MergePolicy for STCSPolicy {
    #[tracing::instrument(name = "compaction.policy_select", skip(self, candidates), fields(candidates = candidates.len()))]
    fn select_merge(&self, candidates: &[PathBuf]) -> Result<Vec<PathBuf>> {
        self.select_merge_internal(candidates)
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::MergePolicy;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_sstable(generation: u64, size_mb: u64) -> SSTableMetadata {
        SSTableMetadata::new(
            PathBuf::from(format!("nb-{}-big-Data.db", generation)),
            size_mb * 1024 * 1024,
        )
    }

    fn create_temp_sstables(sizes_mb: &[u64]) -> (TempDir, Vec<PathBuf>) {
        let temp_dir = TempDir::new().unwrap();
        let mut paths = Vec::new();

        for (i, &size_mb) in sizes_mb.iter().enumerate() {
            let path = temp_dir.path().join(format!("nb-{}-big-Data.db", i + 1));
            let size_bytes = size_mb * 1024 * 1024;

            // Create file with specific size
            let file = std::fs::File::create(&path).unwrap();
            file.set_len(size_bytes).unwrap();

            paths.push(path);
        }

        (temp_dir, paths)
    }

    #[test]
    fn test_stcs_policy_default() {
        let policy = STCSPolicy::default();
        assert_eq!(policy.min_threshold, 4);
        assert_eq!(policy.max_threshold, 32);
        assert_eq!(policy.bucket_low, 0.5);
        assert_eq!(policy.bucket_high, 1.5);
        assert_eq!(policy.min_sstable_size, 50 * 1024 * 1024);
    }

    #[test]
    fn test_stcs_policy_new_validates_min_threshold() {
        let result = STCSPolicy::new(0, 32, 0.5, 1.5, 50 * 1024 * 1024);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("min_threshold"));
    }

    #[test]
    fn test_stcs_policy_new_validates_max_threshold() {
        let result = STCSPolicy::new(10, 5, 0.5, 1.5, 50 * 1024 * 1024);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_threshold"));
    }

    #[test]
    fn test_stcs_policy_new_validates_bucket_ratio() {
        let result = STCSPolicy::new(4, 32, 1.5, 0.5, 50 * 1024 * 1024);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bucket_high"));
    }

    #[test]
    fn test_stcs_policy_new_validates_bucket_low_positive() {
        let result = STCSPolicy::new(4, 32, 0.0, 1.5, 50 * 1024 * 1024);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bucket_low"));
    }

    #[test]
    fn test_stcs_no_compaction_below_threshold() {
        let policy = STCSPolicy::default();

        // Only 3 SSTables, need 4
        let (_temp, paths) = create_temp_sstables(&[100, 100, 100]);

        let result = policy.select_merge(&paths).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_stcs_compaction_at_threshold() {
        let policy = STCSPolicy::default();

        // 4 SSTables of same size (100MB each)
        let (_temp, paths) = create_temp_sstables(&[100, 100, 100, 100]);

        let result = policy.select_merge(&paths).unwrap();
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_stcs_bucket_grouping_same_size() {
        let policy = STCSPolicy::default();

        // All SSTables are exactly 100MB
        let sstables = vec![
            create_sstable(1, 100),
            create_sstable(2, 100),
            create_sstable(3, 100),
            create_sstable(4, 100),
            create_sstable(5, 100),
        ];

        let buckets = policy.group_into_buckets(&sstables);

        // All should be in one bucket
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].len(), 5);
    }

    #[test]
    fn test_stcs_bucket_grouping_within_ratio() {
        let policy = STCSPolicy::default();

        // bucket_low = 0.5, bucket_high = 1.5
        // If avg = 100MB, then 50MB to 150MB are in same bucket
        let sstables = vec![
            create_sstable(1, 100),
            create_sstable(2, 120), // Within ratio
            create_sstable(3, 80),  // Within ratio
            create_sstable(4, 110), // Within ratio
        ];

        let buckets = policy.group_into_buckets(&sstables);

        // All should be in one bucket (within ratio)
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].len(), 4);
    }

    #[test]
    fn test_stcs_bucket_grouping_outside_ratio() {
        let policy = STCSPolicy::default();

        // bucket_low = 0.5, bucket_high = 1.5
        // 100MB bucket: 50-150MB range
        // 200MB is outside this range
        let sstables = vec![
            create_sstable(1, 100),
            create_sstable(2, 100),
            create_sstable(3, 100),
            create_sstable(4, 100),
            create_sstable(5, 200), // Outside ratio, new bucket
            create_sstable(6, 200),
            create_sstable(7, 200),
            create_sstable(8, 200),
        ];

        let buckets = policy.group_into_buckets(&sstables);

        // Should have 2 buckets
        assert_eq!(buckets.len(), 2);

        // Find bucket sizes
        let mut bucket_sizes: Vec<_> = buckets.iter().map(|b| b.len()).collect();
        bucket_sizes.sort();
        assert_eq!(bucket_sizes, vec![4, 4]);
    }

    #[test]
    fn test_stcs_small_sstables_grouped_together() {
        let policy = STCSPolicy::default();
        // min_sstable_size = 50MB

        // All SSTables below 50MB should be grouped together
        // regardless of exact size ratios
        let sstables = vec![
            create_sstable(1, 10),  // Small
            create_sstable(2, 20),  // Small
            create_sstable(3, 30),  // Small
            create_sstable(4, 40),  // Small
            create_sstable(5, 100), // Large, different bucket
        ];

        let buckets = policy.group_into_buckets(&sstables);

        // Should have 2 buckets: one for small, one for large
        assert_eq!(buckets.len(), 2);

        // Find the bucket with 4 SSTables (small ones)
        let small_bucket = buckets.iter().find(|b| b.len() == 4);
        assert!(small_bucket.is_some());
    }

    #[test]
    fn test_stcs_respects_max_threshold() {
        let policy = STCSPolicy::default();
        // max_threshold = 32

        // Create 50 SSTables of same size (100MB each)
        let sizes: Vec<u64> = (1..=50).map(|_| 100).collect();
        let (_temp, paths) = create_temp_sstables(&sizes);

        let result = policy.select_merge(&paths).unwrap();
        // Should limit to max_threshold
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_stcs_empty_input() {
        let policy = STCSPolicy::default();
        let paths = vec![];

        let result = policy.select_merge(&paths).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_stcs_multiple_buckets_selects_first_eligible() {
        let policy = STCSPolicy::default();

        // Create two eligible buckets: 4x100MB and 5x500MB
        let (_temp, paths) = create_temp_sstables(&[
            100, 100, 100, 100, // Bucket 1
            500, 500, 500, 500, 500, // Bucket 2
        ]);

        let result = policy.select_merge(&paths).unwrap();
        // Should select one of the buckets
        assert!(result.len() >= 4);
    }

    #[test]
    fn test_stcs_varied_sizes() {
        let policy = STCSPolicy::default();

        // Mix of very different sizes
        let sstables = vec![
            create_sstable(1, 1),    // 1MB - small bucket
            create_sstable(2, 2),    // 2MB - small bucket
            create_sstable(3, 3),    // 3MB - small bucket
            create_sstable(4, 5),    // 5MB - small bucket
            create_sstable(5, 100),  // 100MB - medium bucket
            create_sstable(6, 110),  // 110MB - medium bucket
            create_sstable(7, 120),  // 120MB - medium bucket
            create_sstable(8, 130),  // 130MB - medium bucket
            create_sstable(9, 1000), // 1000MB - large bucket
        ];

        let buckets = policy.group_into_buckets(&sstables);

        // Should have multiple buckets for different size ranges
        assert!(buckets.len() >= 2);
    }

    #[test]
    fn test_stcs_selection_is_deterministic_smallest_tier() {
        use std::collections::HashSet;

        let policy = STCSPolicy::default();

        // Two eligible tiers, each with min_threshold members:
        //   - small tier: 4 x 100MB
        //   - large tier: 4 x 500MB (500MB is outside [0.5,1.5]x of 100MB and
        //     both are >= min_sstable_size, so they form a distinct bucket).
        // The smaller tier must be selected on every run.
        let (_temp, paths) = create_temp_sstables(&[500, 500, 500, 500, 100, 100, 100, 100]);

        // Expected pick: the four 100MB paths (indices 4..8), returned sorted by
        // (size, path). All are equal-sized, so the deterministic order is by path.
        let mut expected: Vec<PathBuf> = paths[4..8].to_vec();
        expected.sort();

        // Run selection many times; on `main` (HashMap iteration order) the pick
        // is order-dependent — either it varies or it picks the larger 500MB tier.
        let picks: HashSet<Vec<PathBuf>> = (0..100)
            .map(|_| policy.select_merge(&paths).expect("select_merge"))
            .collect();

        assert_eq!(
            picks.len(),
            1,
            "STCS selection must be deterministic across runs, got {} distinct results",
            picks.len()
        );
        let pick = picks.into_iter().next().expect("one pick");
        assert_eq!(
            pick, expected,
            "STCS must deterministically select the smallest eligible tier (4x100MB)"
        );

        // The grouped buckets themselves must be returned smallest-tier-first.
        let sstables: Vec<SSTableMetadata> = paths
            .iter()
            .map(|p| {
                let len = std::fs::metadata(p).expect("stat").len();
                SSTableMetadata::new(p.clone(), len)
            })
            .collect();
        let buckets = policy.group_into_buckets(&sstables);
        let reps: Vec<u64> = buckets
            .iter()
            .map(|b| b.iter().map(|s| s.data_size).min().unwrap_or(u64::MAX))
            .collect();
        let mut sorted_reps = reps.clone();
        sorted_reps.sort_unstable();
        assert_eq!(
            reps, sorted_reps,
            "group_into_buckets must return buckets in ascending representative size order"
        );
    }

    #[test]
    fn test_sstable_metadata_new() {
        let metadata = SSTableMetadata::new(PathBuf::from("test.db"), 12345);
        assert_eq!(metadata.data_path, PathBuf::from("test.db"));
        assert_eq!(metadata.data_size, 12345);
    }

    #[test]
    fn test_stcs_edge_case_exact_boundary() {
        let policy = STCSPolicy::default();
        // bucket_low = 0.5, bucket_high = 1.5

        // If we have 100MB average, boundary is 50MB and 150MB
        let sstables = vec![
            create_sstable(1, 100),
            create_sstable(2, 50),  // Exactly at lower boundary
            create_sstable(3, 150), // Exactly at upper boundary
        ];

        // All should be grouped (boundaries are inclusive in range check)
        let buckets = policy.group_into_buckets(&sstables);

        // Should group into reasonable buckets
        assert!(!buckets.is_empty());
    }

    #[test]
    fn test_stcs_policy_clone() {
        let policy = STCSPolicy::default();
        let cloned = policy.clone();
        assert_eq!(policy.min_threshold, cloned.min_threshold);
        assert_eq!(policy.max_threshold, cloned.max_threshold);
    }

    #[test]
    fn test_sstable_metadata_clone() {
        let metadata = SSTableMetadata::new(PathBuf::from("test.db"), 12345);
        let cloned = metadata.clone();
        assert_eq!(metadata.data_path, cloned.data_path);
        assert_eq!(metadata.data_size, cloned.data_size);
    }
}
