//! Per-letter feature gates for BIG-format SSTables (`BigFormat.java`).
//!
//! Gates are derived solely from the two-letter version string. The supported
//! BIG set is an exact allowlist `{na, nb, oa}` (#1249 floor + #1297 ceiling);
//! see [`BigVersionGates::from_version`]. Pre-`na` is out of scope and rejected.

use crate::{Error, Result};

/// Per-letter feature gates for a BIG-format SSTable.
///
/// Each boolean field corresponds exactly to the gate computed in
/// `BigFormat.BigVersion` (Cassandra 5.0.8, lines 395-410).
///
/// Gates are derived solely from the **two-letter version string**; they do
/// not depend on file content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigVersionGates {
    /// Raw version string this gate set was computed from.
    pub version: String,

    // ---- Gates in order as they appear in BigFormat.java ----
    /// `hasCommitLogLowerBound` — version >= `mb`
    /// (BigFormat.java:395)
    pub has_commit_log_lower_bound: bool,

    /// `hasCommitLogIntervals` — version >= `mc`
    /// (BigFormat.java:396)
    pub has_commit_log_intervals: bool,

    /// `hasAccurateMinMax` — matches `m[d-z]` or `n[a-z]`; **deprecated in `oa`**
    /// (BigFormat.java:397)
    pub has_accurate_min_max: bool,

    /// `hasLegacyMinMax` — matches `m[a-z]` or `n[a-z]`; **deprecated in `oa`**
    /// (BigFormat.java:398)
    pub has_legacy_min_max: bool,

    /// `hasOriginatingHostId` — version >= `nb` **OR** matches `m[e-z]`
    ///
    /// This is the straddle gate: it fires for the `me`–`mz` block of the `m`
    /// series AND for all versions >= `nb` in the `n`/`o` series.
    /// (BigFormat.java:400)
    pub has_originating_host_id: bool,

    /// `hasMaxCompressedLength` — version >= `na`
    /// (BigFormat.java:401)
    pub has_max_compressed_length: bool,

    /// `hasPendingRepair` — version >= `na`
    /// (BigFormat.java:402)
    pub has_pending_repair: bool,

    /// `hasIsTransient` — version >= `na`
    /// (BigFormat.java:403)
    pub has_is_transient: bool,

    /// `hasMetadataChecksum` — version >= `na`
    /// (BigFormat.java:404)
    pub has_metadata_checksum: bool,

    /// `hasOldBfFormat` — version < `na`  (old bloom-filter format)
    /// (BigFormat.java:405)
    pub has_old_bf_format: bool,

    /// `hasImprovedMinMax` — version >= `oa`  (**oa-only**)
    /// (BigFormat.java:406)
    pub has_improved_min_max: bool,

    /// `hasPartitionLevelDeletionPresenceMarker` — version >= `oa`  (**oa-only**)
    /// (BigFormat.java:407)
    pub has_partition_level_deletion_presence_marker: bool,

    /// `hasKeyRange` — version >= `oa`  (**oa-only**)
    /// (BigFormat.java:408)
    pub has_key_range: bool,

    /// `hasUIntDeletionTime` — version >= `oa`  (**oa-only**, 2106-safe TTL)
    /// (BigFormat.java:409)
    pub has_uint_deletion_time: bool,

    /// `hasTokenSpaceCoverage` — version >= `oa`  (**oa-only**)
    /// (BigFormat.java:410)
    pub has_token_space_coverage: bool,
}

impl BigVersionGates {
    /// Compute all gates for the given two-letter BIG-format version string.
    ///
    /// The version comparison uses lexicographic ordering of the raw string,
    /// which is correct because Cassandra uses single-character prefix letters
    /// (`m`, `n`, `o`) followed by a single lowercase suffix.  The Cassandra
    /// source code does the same (`version.compareTo("oa") >= 0`).
    ///
    /// # Errors
    ///
    /// Returns `Err(Error::InvalidFormat)` if `version` is not exactly two
    /// ASCII lowercase letters, and `Err(Error::UnsupportedVersion)` if the
    /// version is below the supported floor (`na`).
    pub fn from_version(version: &str) -> Result<Self> {
        if version.len() != 2 || !version.chars().all(|c| c.is_ascii_lowercase()) {
            return Err(Error::InvalidFormat(format!(
                "BIG version must be 2 lowercase letters, got {:?}",
                version
            )));
        }

        let v = version;

        // #1249: the supported BIG floor is `na` (Cassandra 5.0). Pre-`na`
        // (`ma`–`me`, Cassandra 3.x) is out of scope and rejected here so the
        // struct never *models* a below-floor version as readable. Because of
        // this, every gate threshold below collapses to "`na` and above" — the
        // old `mb`/`mc`/`md`/`me` branches are unreachable and are gone.
        if v < "na" {
            return Err(Error::UnsupportedVersion {
                version: version.to_string(),
                floor: "na".to_string(),
            });
        }

        // #1297: the supported set is an EXACT allowlist, not just a floor.
        // CQLite targets Cassandra 5.0 ONLY. The BIG-format versions in scope
        // are exactly `na`, `nb`, and `oa` (the latter is written with the
        // `big` filename segment in Cassandra 5.0 `storage_compatibility_mode =
        // NONE`). Any other above-floor BIG version (`nc`, a typo like `nz`, a
        // hypothetical future `pa`, …) has NO validated read path, so we reject
        // it here rather than parse an unvalidated layout with nb-compatible
        // gates (no-heuristics). This keeps the reader in agreement with
        // `FormatDetector::is_supported()` / `supported_versions()`, which
        // already advertise exactly `{na, nb, oa, da}`. A genuine future format
        // would be added deliberately, once validated. The `< na` floor above
        // is preserved; this ceiling is purely additive.
        if !matches!(v, "na" | "nb" | "oa") {
            return Err(Error::UnsupportedVersion {
                version: version.to_string(),
                floor: "na".to_string(),
            });
        }

        // BigFormat.java:397-398. Both predicates match only `n[a-z]` once the
        // pre-`na` arms are gone: `m[d-z]`/`m[a-z]` are unreachable below the
        // floor, and `oa` (first letter `o`) matches neither (both deprecated
        // in `oa`). So both gates are exactly "first letter is `n`".
        let first = v.chars().next().unwrap_or('\0');
        let has_accurate_min_max = first == 'n';
        let has_legacy_min_max = first == 'n';

        // `version.compareTo("nb") >= 0` (BigFormat.java:400); the `m[e-z]` arm
        // is unreachable now that pre-`na` is rejected.
        let has_originating_host_id = v >= "nb";

        Ok(Self {
            version: version.to_string(),
            // All of these were introduced at or before `na`, so they are
            // unconditionally TRUE at the floor and above.
            has_commit_log_lower_bound: true,
            has_commit_log_intervals: true,
            has_accurate_min_max,
            has_legacy_min_max,
            has_originating_host_id,
            has_max_compressed_length: true,
            has_pending_repair: true,
            has_is_transient: true,
            has_metadata_checksum: true,
            // `hasOldBfFormat` is `v < "na"`, which is never true at the floor.
            has_old_bf_format: false,
            // oa-only gates: all false for nb, all true for oa
            has_improved_min_max: v >= "oa",
            has_partition_level_deletion_presence_marker: v >= "oa",
            has_key_range: v >= "oa",
            has_uint_deletion_time: v >= "oa",
            has_token_space_coverage: v >= "oa",
        })
    }

    /// Returns `true` when this is a stock Cassandra 5.0 default-mode SSTable
    /// (`nb` version — `storage_compatibility_mode = CASSANDRA_4`).
    pub fn is_cassandra5_compat_mode(&self) -> bool {
        self.version == "nb"
    }

    /// Returns `true` when this is a full Cassandra 5.0 SSTable (`oa` version —
    /// `storage_compatibility_mode = NONE`).
    pub fn is_cassandra5_native(&self) -> bool {
        self.version == "oa"
    }

    /// Infallible constructor returning gates for the `nb` version (stock Cassandra 5.0
    /// `storage_compatibility_mode = CASSANDRA_4`).
    ///
    /// Use this instead of `from_version("nb").expect(…)` in library code, which
    /// violates the project's no-`expect` mandate.  The field values are the literal
    /// results of evaluating `from_version("nb")`; a unit test in this module keeps
    /// them in sync with `from_version`.
    ///
    /// VG3 fall-back: when the SSTable filename cannot be parsed the reader defaults
    /// to these gates so existing behaviour is preserved.
    pub fn nb_fallback() -> Self {
        Self {
            version: "nb".to_string(),
            // Gates matching BigFormat.java for version "nb" ----------------
            has_commit_log_lower_bound: true, // "nb" >= "mb"
            has_commit_log_intervals: true,   // "nb" >= "mc"
            has_accurate_min_max: true,       // "nb" in n[a-z]
            has_legacy_min_max: true,         // "nb" in n[a-z]
            has_originating_host_id: true,    // "nb" >= "nb"
            has_max_compressed_length: true,  // "nb" >= "na"
            has_pending_repair: true,         // "nb" >= "na"
            has_is_transient: true,           // "nb" >= "na"
            has_metadata_checksum: true,      // "nb" >= "na"
            has_old_bf_format: false,         // "nb" NOT < "na"
            // oa-only gates — all FALSE for nb
            has_improved_min_max: false,
            has_partition_level_deletion_presence_marker: false,
            has_key_range: false,
            has_uint_deletion_time: false,
            has_token_space_coverage: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // BigVersionGates: nb (stock Cassandra 5.0 default)
    // -----------------------------------------------------------------------

    #[test]
    fn test_big_nb_gates() {
        let g = BigVersionGates::from_version("nb").unwrap();

        // Gates that ARE set for nb
        assert!(g.has_commit_log_lower_bound, "nb: hasCommitLogLowerBound");
        assert!(g.has_commit_log_intervals, "nb: hasCommitLogIntervals");
        assert!(g.has_max_compressed_length, "nb: hasMaxCompressedLength");
        assert!(g.has_pending_repair, "nb: hasPendingRepair");
        assert!(g.has_is_transient, "nb: hasIsTransient");
        assert!(g.has_metadata_checksum, "nb: hasMetadataChecksum");
        assert!(!g.has_old_bf_format, "nb: !hasOldBfFormat");
        assert!(
            g.has_originating_host_id,
            "nb: hasOriginatingHostId (nb >= nb)"
        );

        // oa-only gates must be FALSE for nb
        assert!(!g.has_improved_min_max, "nb: !hasImprovedMinMax (oa-only)");
        assert!(
            !g.has_partition_level_deletion_presence_marker,
            "nb: !hasPartitionLevelDeletionPresenceMarker (oa-only)"
        );
        assert!(!g.has_key_range, "nb: !hasKeyRange (oa-only)");
        assert!(
            !g.has_uint_deletion_time,
            "nb: !hasUIntDeletionTime (oa-only)"
        );
        assert!(
            !g.has_token_space_coverage,
            "nb: !hasTokenSpaceCoverage (oa-only)"
        );
    }

    // -----------------------------------------------------------------------
    // BigVersionGates: oa (Cassandra 5.0 native mode)
    // -----------------------------------------------------------------------

    #[test]
    fn test_big_oa_gates() {
        let g = BigVersionGates::from_version("oa").unwrap();

        // All na+ gates still set
        assert!(g.has_commit_log_lower_bound);
        assert!(g.has_commit_log_intervals);
        assert!(g.has_max_compressed_length);
        assert!(g.has_pending_repair);
        assert!(g.has_is_transient);
        assert!(g.has_metadata_checksum);
        assert!(!g.has_old_bf_format);
        assert!(g.has_originating_host_id, "oa >= nb");

        // oa-only gates must be TRUE for oa
        assert!(g.has_improved_min_max, "oa: hasImprovedMinMax");
        assert!(
            g.has_partition_level_deletion_presence_marker,
            "oa: hasPartitionLevelDeletionPresenceMarker"
        );
        assert!(g.has_key_range, "oa: hasKeyRange");
        assert!(g.has_uint_deletion_time, "oa: hasUIntDeletionTime");
        assert!(g.has_token_space_coverage, "oa: hasTokenSpaceCoverage");

        // AccurateMinMax is deprecated in oa — should be FALSE
        assert!(
            !g.has_accurate_min_max,
            "oa: hasAccurateMinMax MUST be false (deprecated)"
        );
        // LegacyMinMax also deprecated in oa
        assert!(
            !g.has_legacy_min_max,
            "oa: hasLegacyMinMax MUST be false (deprecated)"
        );
    }

    // -----------------------------------------------------------------------
    // BigVersionGates: oa-only gates are NOT set on nb  (core correctness)
    // -----------------------------------------------------------------------

    #[test]
    fn test_oa_only_gates_absent_from_nb() {
        let nb = BigVersionGates::from_version("nb").unwrap();
        let oa = BigVersionGates::from_version("oa").unwrap();

        let oa_only_gate_names = [
            (
                "hasImprovedMinMax",
                nb.has_improved_min_max,
                oa.has_improved_min_max,
            ),
            (
                "hasPartitionLevelDeletionPresenceMarker",
                nb.has_partition_level_deletion_presence_marker,
                oa.has_partition_level_deletion_presence_marker,
            ),
            ("hasKeyRange", nb.has_key_range, oa.has_key_range),
            (
                "hasUIntDeletionTime",
                nb.has_uint_deletion_time,
                oa.has_uint_deletion_time,
            ),
            (
                "hasTokenSpaceCoverage",
                nb.has_token_space_coverage,
                oa.has_token_space_coverage,
            ),
        ];

        for (name, nb_val, oa_val) in &oa_only_gate_names {
            assert!(!nb_val, "nb.{} must be FALSE (oa-only gate)", name);
            assert!(oa_val, "oa.{} must be TRUE", name);
        }
    }

    // -----------------------------------------------------------------------
    // BigVersionGates: hasOriginatingHostId straddle gate
    // -----------------------------------------------------------------------

    /// `hasOriginatingHostId` is TRUE from `nb` onward (the `m[e-z]` arm of the
    /// Cassandra predicate is unreachable now that pre-`na` is rejected at the
    /// floor; see `from_version`). `na` (< `nb`) must be FALSE.
    #[test]
    fn test_originating_host_id_straddle_gate() {
        // na (< nb): FALSE
        let na = BigVersionGates::from_version("na").unwrap();
        assert!(
            !na.has_originating_host_id,
            "na: hasOriginatingHostId must be FALSE (na < nb)"
        );

        // nb and above (within the supported allowlist): TRUE. `nc` is no
        // longer constructible (#1297 ceiling), so only in-scope versions are
        // exercised here.
        for v in &["nb", "oa"] {
            let g = BigVersionGates::from_version(v).unwrap();
            assert!(
                g.has_originating_host_id,
                "{}: hasOriginatingHostId must be TRUE (>= nb)",
                v
            );
        }
    }

    // -----------------------------------------------------------------------
    // BigVersionGates: na (the supported floor)
    // -----------------------------------------------------------------------

    #[test]
    fn test_big_na_gates() {
        let g = BigVersionGates::from_version("na").unwrap();
        assert!(g.has_commit_log_lower_bound);
        assert!(g.has_commit_log_intervals);
        assert!(g.has_accurate_min_max, "na is in n[a-z]");
        assert!(g.has_legacy_min_max, "na is in n[a-z]");
        assert!(!g.has_originating_host_id, "na < nb");
        assert!(g.has_max_compressed_length);
        assert!(g.has_pending_repair);
        assert!(!g.has_old_bf_format);
        assert!(!g.has_improved_min_max, "oa-only");
    }

    // -----------------------------------------------------------------------
    // Version floor (#1249): pre-`na` BIG is rejected at the floor with a
    // typed `Error::UnsupportedVersion` naming the version + floor.
    // -----------------------------------------------------------------------

    /// R1/R3: a below-`na` BIG version yields `UnsupportedVersion`, not gates.
    #[test]
    fn test_big_below_na_rejected_with_typed_error() {
        for v in &["ma", "mb", "mc", "md", "me"] {
            let err = match BigVersionGates::from_version(v) {
                Ok(_) => panic!("{} is below the na floor and must be rejected", v),
                Err(e) => e,
            };
            match err {
                Error::UnsupportedVersion { version, floor } => {
                    assert_eq!(version, *v, "error must name the offending version");
                    assert_eq!(floor, "na", "error must name the na floor");
                }
                other => panic!("{}: expected UnsupportedVersion, got {:?}", v, other),
            }
        }
    }

    /// R3: there is no branch returning usable gates for a below-floor version.
    #[test]
    fn test_big_below_na_yields_no_usable_gates() {
        assert!(
            BigVersionGates::from_version("ma").is_err(),
            "ma must not construct usable gates"
        );
        // Supported floor and above still succeed.
        for v in &["na", "nb", "oa"] {
            assert!(
                BigVersionGates::from_version(v).is_ok(),
                "{} must still construct gates",
                v
            );
        }
    }

    // -----------------------------------------------------------------------
    // Exact allowlist ceiling (#1297): unknown ABOVE-floor BIG versions are
    // rejected. The supported BIG set is exactly {na, nb, oa}; anything else
    // in the n/o/p… space (nc, typos, hypothetical future letters) has no
    // validated read path and yields a typed `UnsupportedVersion`.
    // -----------------------------------------------------------------------

    /// #1297 R1: an above-floor but out-of-allowlist BIG version (`nc`, a typo
    /// `nz`, a hypothetical `pa`/`ob`) yields `UnsupportedVersion`, never gates.
    #[test]
    fn test_big_above_floor_unknown_rejected_with_typed_error() {
        for v in &["nc", "nz", "pa", "ob", "zz"] {
            let err = match BigVersionGates::from_version(v) {
                Ok(_) => panic!(
                    "{} is outside the supported allowlist and must be rejected",
                    v
                ),
                Err(e) => e,
            };
            match err {
                Error::UnsupportedVersion { version, floor } => {
                    assert_eq!(version, *v, "error must name the offending version");
                    assert_eq!(floor, "na", "error must name the na floor");
                }
                other => panic!("{}: expected UnsupportedVersion, got {:?}", v, other),
            }
        }
    }

    /// #1297: exactly the in-scope BIG allowlist constructs gates; the ceiling
    /// is additive over the `< na` floor (both below-floor and above-allowlist
    /// versions are rejected, the three supported versions still succeed).
    #[test]
    fn test_big_exact_allowlist_only() {
        for v in &["na", "nb", "oa"] {
            assert!(
                BigVersionGates::from_version(v).is_ok(),
                "{} is in the supported BIG allowlist and must construct gates",
                v
            );
        }
        // Below the floor (additive ceiling did not remove the floor):
        assert!(
            BigVersionGates::from_version("me").is_err(),
            "below-floor still rejected"
        );
        // Above the floor but outside the allowlist:
        assert!(
            BigVersionGates::from_version("nc").is_err(),
            "above-allowlist rejected"
        );
    }

    /// #1297: reader-side allowlist now agrees with `FormatDetector::is_supported`
    /// for these BIG cases — both accept exactly {na, nb, oa} and reject `nc`.
    #[test]
    fn test_big_gate_matches_format_detector_is_supported() {
        use crate::storage::sstable::format_detector::FormatDetector;
        let detector = FormatDetector::new();
        for v in &["na", "nb", "oa", "nc", "me"] {
            let gate_ok = BigVersionGates::from_version(v).is_ok();
            let detector_ok = detector.is_supported(v);
            assert_eq!(
                gate_ok, detector_ok,
                "{}: BigVersionGates::from_version and FormatDetector::is_supported must agree",
                v
            );
        }
    }

    #[test]
    fn test_big_is_cassandra5_mode() {
        let nb = BigVersionGates::from_version("nb").unwrap();
        assert!(nb.is_cassandra5_compat_mode());
        assert!(!nb.is_cassandra5_native());

        let oa = BigVersionGates::from_version("oa").unwrap();
        assert!(!oa.is_cassandra5_compat_mode());
        assert!(oa.is_cassandra5_native());
    }

    // -----------------------------------------------------------------------
    // BigVersionGates::nb_fallback — must match from_version("nb") exactly
    // -----------------------------------------------------------------------

    /// Verify that `BigVersionGates::nb_fallback()` produces the same gate
    /// values as `BigVersionGates::from_version("nb")`.  This test is the
    /// automated guard that keeps the two in sync.
    #[test]
    fn test_nb_fallback_matches_from_version() {
        let from_fn = BigVersionGates::from_version("nb").unwrap();
        let fallback = BigVersionGates::nb_fallback();

        assert_eq!(fallback.version, from_fn.version);
        assert_eq!(
            fallback.has_commit_log_lower_bound,
            from_fn.has_commit_log_lower_bound
        );
        assert_eq!(
            fallback.has_commit_log_intervals,
            from_fn.has_commit_log_intervals
        );
        assert_eq!(fallback.has_accurate_min_max, from_fn.has_accurate_min_max);
        assert_eq!(fallback.has_legacy_min_max, from_fn.has_legacy_min_max);
        assert_eq!(
            fallback.has_originating_host_id,
            from_fn.has_originating_host_id
        );
        assert_eq!(
            fallback.has_max_compressed_length,
            from_fn.has_max_compressed_length
        );
        assert_eq!(fallback.has_pending_repair, from_fn.has_pending_repair);
        assert_eq!(fallback.has_is_transient, from_fn.has_is_transient);
        assert_eq!(
            fallback.has_metadata_checksum,
            from_fn.has_metadata_checksum
        );
        assert_eq!(fallback.has_old_bf_format, from_fn.has_old_bf_format);
        assert_eq!(fallback.has_improved_min_max, from_fn.has_improved_min_max);
        assert_eq!(
            fallback.has_partition_level_deletion_presence_marker,
            from_fn.has_partition_level_deletion_presence_marker
        );
        assert_eq!(fallback.has_key_range, from_fn.has_key_range);
        assert_eq!(
            fallback.has_uint_deletion_time,
            from_fn.has_uint_deletion_time
        );
        assert_eq!(
            fallback.has_token_space_coverage,
            from_fn.has_token_space_coverage
        );
    }

    // -----------------------------------------------------------------------
    // BigVersionGates: invalid input
    // -----------------------------------------------------------------------

    #[test]
    fn test_big_invalid_version() {
        assert!(BigVersionGates::from_version("n").is_err());
        assert!(BigVersionGates::from_version("nba").is_err());
        assert!(BigVersionGates::from_version("NB").is_err());
        assert!(BigVersionGates::from_version("").is_err());
    }
}
