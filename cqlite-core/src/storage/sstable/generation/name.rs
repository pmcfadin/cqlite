//! SSTable component file names and the descriptor-first version floor (requirements G5, G9).
//!
//! `cassandra-5.0.8` `io/sstable/Descriptor.java:364-410`: the name is split on `-` into exactly
//! four tokens, `<version>-<id>-<format>-<component>`. The version must match `[a-z]+`
//! (`format/Version.java:36,111`), the id must parse (`SSTableIdFactory.fromString`), the format
//! must be a known format name. A name that fails any step "is not an SSTable component" and is
//! ignored (`SSTable.tryComponentFromFilename` returns null).
//!
//! CQLite deliberately parts from Cassandra at one point: Cassandra rejects an incompatible
//! version string at the same step, so a `ma-*` file is invisible to it. Here such a file still
//! parses, and carries the `UnsupportedVersion` condition (G9), so a verb can refuse it by name.

use super::id::GenerationId;
use crate::storage::sstable::version_gate::{BigVersionGates, BtiVersionGates, SsTableFormat};

/// A parsed component file name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ComponentName {
    pub version: String,
    pub id: GenerationId,
    pub format: SsTableFormat,
    /// Everything after the format token, e.g. `Data.db`, `TOC.txt`, `Statistics.db.txt`.
    pub component: String,
}

/// Parse `name` as an SSTable component file name, or `None` if it is not one.
pub(super) fn parse_component_name(name: &str) -> Option<ComponentName> {
    let mut tokens = name.split('-');
    let (version, id, format, component) = (
        tokens.next()?,
        tokens.next()?,
        tokens.next()?,
        tokens.next()?,
    );
    // Exactly four tokens (`Descriptor.filenameTokens`).
    if tokens.next().is_some() {
        return None;
    }
    if version.is_empty() || !version.bytes().all(|b| b.is_ascii_lowercase()) {
        return None;
    }
    if component.is_empty() {
        return None;
    }
    Some(ComponentName {
        version: version.to_string(),
        id: GenerationId::parse(id)?,
        format: SsTableFormat::parse(format)?,
        component: component.to_string(),
    })
}

/// A version the floor refuses: below it, or not a version the format defines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BelowFloor {
    /// The version letters from the file name, e.g. `ma`.
    pub version: String,
    /// The lowest version the format supports (`na` for BIG, `da` for BTI).
    pub floor: &'static str,
}

/// The version-floor decision, made from the descriptor's strings alone: no path, no file access
/// (G9). Delegates to the gates the readers already use, so the module and the readers cannot
/// disagree about the floor.
pub fn check_floor(version: &str, format: SsTableFormat) -> Result<(), BelowFloor> {
    let (accepted, floor) = match format {
        SsTableFormat::Big => (BigVersionGates::from_version(version).is_ok(), "na"),
        SsTableFormat::Bti => (BtiVersionGates::from_version(version).is_ok(), "da"),
    };
    if accepted {
        Ok(())
    } else {
        Err(BelowFloor {
            version: version.to_string(),
            floor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_four_tokens() {
        let n = parse_component_name("nb-1-big-Data.db").unwrap();
        assert_eq!(n.version, "nb");
        assert_eq!(n.id, GenerationId::Sequence(1));
        assert_eq!(n.format, SsTableFormat::Big);
        assert_eq!(n.component, "Data.db");
        let n = parse_component_name("da-5-bti-TOC.txt").unwrap();
        assert_eq!(n.format, SsTableFormat::Bti);
        assert_eq!(n.component, "TOC.txt");
    }

    /// Sidecar suffixes and unknown components still belong to their generation: Cassandra's
    /// `Component.parse` makes a custom component of them, and G13 wants every file accounted for.
    #[test]
    fn sidecars_are_components_of_their_generation() {
        let n = parse_component_name("nb-1-big-Data.db.jsonl").unwrap();
        assert_eq!(n.component, "Data.db.jsonl");
    }

    #[test]
    fn uuid_id_name_parses() {
        let n = parse_component_name("nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big-Data.db").unwrap();
        assert_eq!(n.id.to_string(), "3gb2_0rbd_2lhua2pw7a2hlyvbsp");
    }

    #[test]
    fn non_components_do_not_parse() {
        for name in [
            ".gitignore",
            "notes.txt",
            "nb-1-big",
            "nb-1-big-Data-db",
            "nb-1-xyz-Data.db",
            "NB-1-big-Data.db",
            "n1-1-big-Data.db",
            "nb-2147483648-big-Data.db",
            "nb-12345678901-big-Data.db",
            "nb-1-big-",
            "-1-big-Data.db",
            "nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db",
            "nb_txn_compaction_6aa08200-a251-11f0-a3fe-f1a551383fb9.log",
        ] {
            assert!(parse_component_name(name).is_none(), "{name}");
        }
    }

    /// G9.2
    #[test]
    fn supported_versions_pass_the_floor() {
        assert!(check_floor("na", SsTableFormat::Big).is_ok());
        assert!(check_floor("nb", SsTableFormat::Big).is_ok());
        assert!(check_floor("oa", SsTableFormat::Big).is_ok());
        assert!(check_floor("da", SsTableFormat::Bti).is_ok());
    }

    /// G9.1 (the pure half; the no-I/O half is in `metadata`'s tests).
    #[test]
    fn below_floor_and_undefined_versions_are_refused_by_name() {
        for v in ["ma", "mb", "me"] {
            let e = check_floor(v, SsTableFormat::Big).unwrap_err();
            assert_eq!((e.version.as_str(), e.floor), (v, "na"));
        }
        // not a version the format defines
        assert!(check_floor("zz", SsTableFormat::Big).is_err());
        assert!(check_floor("nb", SsTableFormat::Bti).is_err());
        assert!(check_floor("oa", SsTableFormat::Bti).is_err());
        assert!(check_floor("da", SsTableFormat::Big).is_err());
        assert_eq!(
            check_floor("abc", SsTableFormat::Bti).unwrap_err().floor,
            "da"
        );
    }
}
