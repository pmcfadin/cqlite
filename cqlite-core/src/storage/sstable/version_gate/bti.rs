//! Feature gates for BTI-format SSTables (`BtiFormat.java`).
//!
//! BTI has a single supported version (`da`); all modern gates are TRUE for it
//! (BtiFormat.java:321-418). See [`BtiVersionGates::from_version`].

use crate::{Error, Result};

/// Feature gates for a BTI-format SSTable.
///
/// BtiFormat only has one version (`da`).  All modern feature gates are TRUE
/// for `da` (BtiFormat.java:321-418).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtiVersionGates {
    /// Raw version string (always `"da"` for BTI).
    pub version: String,

    /// All gates are TRUE for `da`.  Fields mirror the BIG gates for API parity.
    pub has_commit_log_lower_bound: bool,
    pub has_commit_log_intervals: bool,
    pub has_max_compressed_length: bool,
    pub has_pending_repair: bool,
    pub has_is_transient: bool,
    pub has_metadata_checksum: bool,
    /// `hasOldBfFormat` is **FALSE** for BTI (BtiFormat.java:357-360).
    pub has_old_bf_format: bool,
    pub has_originating_host_id: bool,
    /// `hasAccurateMinMax` — **TRUE** for BTI `da`.
    ///
    /// Source: BtiFormat.java:363-366
    /// ```java
    /// public boolean hasAccurateMinMax() { return true; }
    /// ```
    pub has_accurate_min_max: bool,
    /// `hasLegacyMinMax` — **FALSE** for BTI `da`.
    ///
    /// Source: BtiFormat.java:368-371
    /// ```java
    /// public boolean hasLegacyMinMax() { return false; }
    /// ```
    pub has_legacy_min_max: bool,
    pub has_improved_min_max: bool,
    pub has_token_space_coverage: bool,
    pub has_partition_level_deletion_presence_marker: bool,
    pub has_key_range: bool,
    pub has_uint_deletion_time: bool,
}

impl BtiVersionGates {
    /// Compute BTI gates for the given version string.
    ///
    /// # Errors
    ///
    /// Returns `Err(Error::UnsupportedVersion)` if the version is not `"da"`
    /// (the only supported BTI version; #1249 version floor).
    pub fn from_version(version: &str) -> Result<Self> {
        if version != "da" {
            return Err(Error::UnsupportedVersion {
                version: version.to_string(),
                floor: "da".to_string(),
            });
        }
        Ok(Self {
            version: version.to_string(),
            has_commit_log_lower_bound: true,
            has_commit_log_intervals: true,
            has_max_compressed_length: true,
            has_pending_repair: true,
            has_is_transient: true,
            has_metadata_checksum: true,
            has_old_bf_format: false, // Always false for BTI (BtiFormat.java:357-360)
            has_originating_host_id: true,
            // BtiFormat.java:363-366: `public boolean hasAccurateMinMax() { return true; }`
            has_accurate_min_max: true,
            // BtiFormat.java:368-371: `public boolean hasLegacyMinMax() { return false; }`
            has_legacy_min_max: false,
            has_improved_min_max: true,
            has_token_space_coverage: true,
            has_partition_level_deletion_presence_marker: true,
            has_key_range: true,
            has_uint_deletion_time: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bti_da_gates() {
        let g = BtiVersionGates::from_version("da").unwrap();
        assert!(g.has_commit_log_lower_bound);
        assert!(g.has_commit_log_intervals);
        assert!(g.has_max_compressed_length);
        assert!(g.has_pending_repair);
        assert!(g.has_is_transient);
        assert!(g.has_metadata_checksum);
        assert!(!g.has_old_bf_format, "da: !hasOldBfFormat");
        assert!(g.has_originating_host_id);
        // BtiFormat.java:363-366: hasAccurateMinMax() → true
        assert!(
            g.has_accurate_min_max,
            "da: hasAccurateMinMax (BtiFormat.java:363)"
        );
        // BtiFormat.java:368-371: hasLegacyMinMax() → false
        assert!(
            !g.has_legacy_min_max,
            "da: !hasLegacyMinMax (BtiFormat.java:368)"
        );
        assert!(g.has_improved_min_max);
        assert!(g.has_token_space_coverage);
        assert!(g.has_partition_level_deletion_presence_marker);
        assert!(g.has_key_range);
        assert!(g.has_uint_deletion_time);
    }

    /// R1: a non-`da` BTI version is rejected with the typed
    /// `Error::UnsupportedVersion` (naming the `da` floor), not a generic
    /// `InvalidFormat`.
    #[test]
    fn test_bti_rejects_non_da() {
        for v in &["nb", "oa", "na", "ca"] {
            let err = match BtiVersionGates::from_version(v) {
                Ok(_) => panic!("{} is not da and must be rejected", v),
                Err(e) => e,
            };
            match err {
                Error::UnsupportedVersion { version, floor } => {
                    assert_eq!(version, *v, "error must name the offending version");
                    assert_eq!(floor, "da", "error must name the da floor");
                }
                other => panic!("{}: expected UnsupportedVersion, got {:?}", v, other),
            }
        }
        // The single supported BTI version still constructs gates.
        assert!(BtiVersionGates::from_version("da").is_ok());
    }
}
