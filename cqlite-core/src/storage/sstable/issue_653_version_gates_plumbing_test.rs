//! VG1: Thread VersionGates through the read path — unit tests (Issue #653)
//!
//! These tests verify the plumbing changes described in issue #653:
//! - `BtiVersionGates` exposes `has_accurate_min_max` / `has_legacy_min_max`
//!   matching `BtiFormat.java:363-371`.
//! - `FormatDetector` returns a non-Unknown variant for `da`.
//! - `VersionGates::from_path` succeeds for nb, oa, and da filenames (both
//!   sequential and UUID-based SSTable id forms).
//! - `SsTableDescriptor::parse_filename` extracts the version letter from all
//!   real-world filename patterns.
//!
//! Tests for `parse_sstable_filename` (version letter in returned tuple) live in
//! `directory/tests.rs` because they access the private `scan` sub-module.

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::storage::sstable::{
        format_detector::{FormatDetector, SSTableFormat},
        version_gate::{BtiVersionGates, SsTableDescriptor, SsTableFormat, VersionGates},
    };

    // -----------------------------------------------------------------------
    // BtiVersionGates: has_accurate_min_max / has_legacy_min_max
    // -----------------------------------------------------------------------

    /// `BtiVersionGates::from_version("da")` must expose the two min/max gates
    /// matching `BtiFormat.java:363-371`.
    #[test]
    fn test_bti_version_gates_accurate_and_legacy_min_max() {
        let g = BtiVersionGates::from_version("da").unwrap();

        // BtiFormat.java:363-366: `public boolean hasAccurateMinMax() { return true; }`
        assert!(
            g.has_accurate_min_max,
            "da: hasAccurateMinMax must be TRUE (BtiFormat.java:363)"
        );

        // BtiFormat.java:368-371: `public boolean hasLegacyMinMax() { return false; }`
        assert!(
            !g.has_legacy_min_max,
            "da: hasLegacyMinMax must be FALSE (BtiFormat.java:368)"
        );
    }

    /// The combined `VersionGates` path must also expose the BTI min/max gates.
    #[test]
    fn test_version_gates_from_path_da_exposes_min_max_gates() {
        let gates = VersionGates::from_path(&PathBuf::from("da-1-bti-Data.db")).unwrap();
        match gates {
            VersionGates::Bti(g) => {
                assert!(g.has_accurate_min_max, "da VersionGates: hasAccurateMinMax");
                assert!(!g.has_legacy_min_max, "da VersionGates: !hasLegacyMinMax");
            }
            VersionGates::Big(_) => panic!("Expected Bti gates for da-1-bti-Data.db"),
        }
    }

    // -----------------------------------------------------------------------
    // FormatDetector: `da` must not be Unknown
    // -----------------------------------------------------------------------

    /// `FormatDetector` must return `V5x("da")` for the `da` version letter.
    /// Before VG1, `da` fell through to `Unknown("da")`.
    #[test]
    fn test_format_detector_da_is_not_unknown() {
        let detector = FormatDetector::new();
        let fmt = detector.detect_from_version("da").unwrap();

        assert_ne!(
            fmt,
            SSTableFormat::Unknown("da".to_string()),
            "da must NOT map to Unknown after VG1"
        );
        assert_eq!(
            fmt,
            SSTableFormat::V5x("da".to_string()),
            "da must map to V5x"
        );
    }

    /// `da` must appear in the supported versions list.
    #[test]
    fn test_format_detector_da_is_supported() {
        let detector = FormatDetector::new();
        assert!(
            detector.is_supported("da"),
            "FormatDetector must report 'da' as supported after VG1"
        );
    }

    /// All three Cassandra 5.x version letters (`oa` and `da`) map to V5x.
    #[test]
    fn test_format_detector_v5x_versions() {
        let detector = FormatDetector::new();
        for v in &["oa", "da"] {
            let fmt = detector.detect_from_version(v).unwrap();
            assert!(
                matches!(fmt, SSTableFormat::V5x(_)),
                "{} must map to V5x",
                v
            );
        }
    }

    // -----------------------------------------------------------------------
    // SsTableDescriptor: version-letter extraction for nb/oa/da filenames
    // -----------------------------------------------------------------------

    /// Sequential-id filenames for nb, oa, da all yield the correct version letter.
    #[test]
    fn test_descriptor_parses_nb_oa_da_sequential() {
        for (filename, expected_ver, expected_fmt) in &[
            ("nb-1-big-Data.db", "nb", SsTableFormat::Big),
            ("oa-2-big-Data.db", "oa", SsTableFormat::Big),
            ("da-3-bti-Data.db", "da", SsTableFormat::Bti),
        ] {
            let desc = SsTableDescriptor::parse_filename(filename).unwrap();
            assert_eq!(&desc.version, expected_ver, "version for {}", filename);
            assert_eq!(&desc.format, expected_fmt, "format for {}", filename);
        }
    }

    /// UUID-based ids (both compact and hyphenated forms) must parse correctly.
    /// UUID form is the Cassandra 5.0.0 default (Descriptor.java:95).
    #[test]
    fn test_descriptor_parses_uuid_id_forms() {
        // 32-hex-char compact UUID id
        let desc =
            SsTableDescriptor::parse_filename("nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db")
                .unwrap();
        assert_eq!(desc.version, "nb");
        assert_eq!(desc.sstable_id, "6aa08200a25111f0a3fef1a551383fb9");
        assert_eq!(desc.format, SsTableFormat::Big);

        // oa with compact UUID id
        let desc =
            SsTableDescriptor::parse_filename("oa-6aa08200a25111f0a3fef1a551383fb9-big-Data.db")
                .unwrap();
        assert_eq!(desc.version, "oa");
        assert_eq!(desc.format, SsTableFormat::Big);
    }

    /// `VersionGates::from_path` succeeds for nb, oa, and da sequential-id paths.
    #[test]
    fn test_version_gates_from_path_nb_oa_da() {
        let nb = VersionGates::from_path(&PathBuf::from("nb-1-big-Data.db")).unwrap();
        assert!(matches!(nb, VersionGates::Big(ref g) if g.version == "nb"));

        let oa = VersionGates::from_path(&PathBuf::from("oa-1-big-Data.db")).unwrap();
        assert!(matches!(oa, VersionGates::Big(ref g) if g.version == "oa"));

        let da = VersionGates::from_path(&PathBuf::from("da-1-bti-Data.db")).unwrap();
        assert!(matches!(da, VersionGates::Bti(ref g) if g.version == "da"));
    }

    /// `VersionGates::from_path` succeeds for UUID-based id paths.
    #[test]
    fn test_version_gates_from_path_uuid_id() {
        let path = PathBuf::from("nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db");
        let gates = VersionGates::from_path(&path).unwrap();
        match gates {
            VersionGates::Big(g) => {
                assert_eq!(g.version, "nb");
                // oa-only gates must be absent for nb
                assert!(!g.has_improved_min_max);
                assert!(!g.has_uint_deletion_time);
            }
            VersionGates::Bti(_) => panic!("Expected Big gates for nb UUID-id path"),
        }
    }
}
