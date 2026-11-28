//! The trusted-metadata accessor (requirement G11).
//!
//! One accessor per generation reads `Statistics.db` once, in a fixed order, and caches the value
//! or the typed failure:
//!
//! 1. the floor, from the descriptor (G9), before any file is opened;
//! 2. a bounded read, 3. the strict layout and checksum check of
//!    `statistics_reader/integrity.rs` (the port of `MetadataSerializer.deserialize`), both in
//!    `integrity::read_checked_statistics`;
//! 4. one parse, with the version gates the descriptor selects.
//!
//! The lenient parser (`parse_statistics_with_fallback_detailed`) is called from here and nowhere
//! in this module's public surface; callers cannot reach it through a generation. Removing its
//! other call sites is the job of the migration phases, each of which deletes the copy it
//! replaces.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use super::model::{Condition, Generation};
#[cfg(feature = "write-support")]
use super::name::check_floor;
use crate::error::{Error, Result};
use crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback_detailed;
#[cfg(feature = "write-support")]
use crate::parser::repair_metadata::{
    parse_repair_metadata, parse_stats_extras, RepairMetadata, StatsExtras,
};
use crate::parser::statistics::SSTableStatistics;
use crate::storage::sstable::statistics_reader::integrity;
use crate::storage::sstable::version_gate::{
    BigVersionGates, BtiVersionGates, SsTableFormat, VersionGates,
};

const STATISTICS_COMPONENT: &str = "Statistics.db";

/// The shared, lazily filled cache of one generation's metadata outcome. Cloning a
/// [`Generation`] shares it, so a failure is judged once however many copies exist.
pub(super) type MetadataCell = Arc<OnceLock<std::result::Result<Loaded, MetadataFailure>>>;

/// What one successful load keeps: the parsed value and the small STATS fields `diagnose` needs
/// that the typed value does not carry. The `Statistics.db` bytes themselves are NOT kept: a file
/// may be up to `MAX_STATISTICS_BYTES` and a listing holds one cell per generation, so retaining
/// them would hold every generation's buffer for the whole run.
#[derive(Debug, Clone)]
pub(super) struct Loaded {
    stats: Arc<SSTableStatistics>,
    /// The repair fields (`repairedAt`, `pendingRepair`, `isTransient`) decoded from the same
    /// bytes with the descriptor's gates (compaction's repair-state classification, #4626).
    #[cfg(feature = "write-support")]
    repair: std::result::Result<Arc<RepairMetadata>, String>,
    /// The STATS-extras histograms decoded from the same bytes; compaction's baseline seeding
    /// reads the tombstone-drop-time histogram as its authoritative "has a tombstone" signal.
    #[cfg(feature = "write-support")]
    extras: std::result::Result<Arc<StatsExtras>, String>,
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    facts: std::result::Result<Arc<StatisticsFacts>, String>,
    /// The `SerializationHeader.EncodingStats` triple `(minTimestamp, minLocalDeletionTime,
    /// minTTL)` from the same bytes, `None` when the header does not carry it. Kept apart from
    /// `facts` so an undecodable repair block does not hide it (`rebuild`, GD3).
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    encoding_baseline: Option<(i64, i64, i64)>,
}

/// STATS fields decoded once, inside the loader, from the same bytes as the typed value.
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
#[derive(Debug, Clone)]
pub struct StatisticsFacts {
    pub(crate) repair: crate::parser::repair_metadata::RepairMetadata,
    pub(crate) cell_per_partition:
        Option<crate::parser::repair_metadata::diagnose_fields::CellPerPartitionStats>,
    pub(crate) compression_ratio: Option<f64>,
    /// The STATS component's own `(minTimestamp, minLocalDeletionTime)`.
    pub(crate) minimums: Option<(i64, i64)>,
}

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
fn decode_facts(
    bytes: &[u8],
    gates: &VersionGates,
) -> std::result::Result<StatisticsFacts, String> {
    use crate::parser::repair_metadata::diagnose_fields::{
        read_cell_per_partition_stats, read_compression_ratio, read_stats_minimums,
    };
    let e = |e: Error| e.to_string();
    Ok(StatisticsFacts {
        repair: crate::parser::repair_metadata::parse_repair_metadata(bytes, Some(gates))
            .map_err(e)?,
        cell_per_partition: read_cell_per_partition_stats(bytes).map_err(e)?,
        compression_ratio: read_compression_ratio(bytes).map_err(e)?,
        minimums: read_stats_minimums(bytes, Some(gates)).map_err(e)?,
    })
}

/// Why a generation's trusted metadata is unavailable. Each variant is one [`Condition`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum MetadataFailure {
    /// The version is below the floor or not defined by the format. No file was opened.
    #[error(
        "{version} is below the supported floor {floor} (or is not a version this format defines)"
    )]
    UnsupportedVersion {
        /// The version letters.
        version: String,
        /// The floor for the format.
        floor: &'static str,
    },
    /// The generation has no `Statistics.db`.
    #[error("Statistics.db is absent: {}", path.display())]
    Absent {
        /// Where it would be.
        path: PathBuf,
    },
    /// `Statistics.db` could not be read.
    #[error("Statistics.db {} could not be read: {detail}", path.display())]
    Unreadable {
        /// The file.
        path: PathBuf,
        /// The I/O failure.
        detail: String,
    },
    /// `Statistics.db` failed the integrity check or did not parse.
    #[error("Statistics.db {} is corrupt: {detail}", path.display())]
    Corrupt {
        /// The file.
        path: PathBuf,
        /// What failed.
        detail: String,
    },
}

impl MetadataFailure {
    /// The closed condition this failure is.
    pub fn condition(&self) -> Condition {
        match self {
            Self::UnsupportedVersion { .. } => Condition::UnsupportedVersion,
            Self::Absent { .. } => Condition::Absent,
            Self::Unreadable { .. } => Condition::Unreadable,
            Self::Corrupt { .. } => Condition::Corrupt,
        }
    }
}

/// The trusted values of one `Statistics.db` read by path, for a `Data.db` the listing does not
/// return (hidden by a transaction log, a refused listing, a name that is not an SSTable
/// component). The same order as [`Generation::trusted_metadata`]: the floor from the file name,
/// then the bounded read, the strict integrity check and one parse. Gates come from the name when
/// it has a descriptor, else the parser's nb-compatible default (as `StatisticsReader::open`).
#[derive(Debug, Clone)]
#[cfg(feature = "write-support")]
pub(crate) struct DirectStatistics {
    pub(crate) stats: Arc<SSTableStatistics>,
    pub(crate) repair: std::result::Result<Arc<RepairMetadata>, String>,
    pub(crate) extras: std::result::Result<Arc<StatsExtras>, String>,
}

#[cfg(feature = "write-support")]
pub(crate) fn read_direct(path: &Path) -> std::result::Result<DirectStatistics, MetadataFailure> {
    // The format and version come from the FILE NAME's parsed descriptor, the parser the rest of
    // the module uses, never from the directory path. A name with no parseable descriptor has no
    // authoritative version, so it gets no gates and the parser's nb-compatible defaults (as
    // `StatisticsReader::open` does); guessing a format from other text would be a heuristic (#28).
    let descriptor = path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(super::name::parse_component_name);
    let gates = match descriptor {
        None => None,
        Some(d) => {
            if let Err(below) = check_floor(&d.version, d.format) {
                return Err(MetadataFailure::UnsupportedVersion {
                    version: below.version,
                    floor: below.floor,
                });
            }
            match d.format {
                SsTableFormat::Big => {
                    BigVersionGates::from_version(&d.version).map(VersionGates::Big)
                }
                SsTableFormat::Bti => {
                    BtiVersionGates::from_version(&d.version).map(VersionGates::Bti)
                }
            }
            .ok()
        }
    };
    let bytes = read_checked(path).map_err(|e| classify_read_error(path, e))?;
    let corrupt = |e: Error| MetadataFailure::Corrupt {
        path: path.to_path_buf(),
        detail: e.to_string(),
    };
    let (_, stats) =
        parse_statistics_with_fallback_detailed(&bytes, gates.as_ref()).map_err(corrupt)?;
    Ok(DirectStatistics {
        stats: Arc::new(stats),
        repair: parse_repair_metadata(&bytes, gates.as_ref())
            .map(Arc::new)
            .map_err(|e| e.to_string()),
        extras: parse_stats_extras(&bytes, gates.as_ref())
            .map(Arc::new)
            .map_err(|e| e.to_string()),
    })
}

/// The production read: bounded, then the strict integrity check.
pub(super) fn read_checked(path: &Path) -> Result<Vec<u8>> {
    integrity::read_checked_statistics(path)
}

fn classify_read_error(path: &Path, error: Error) -> MetadataFailure {
    match error {
        Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound => MetadataFailure::Absent {
            path: path.to_path_buf(),
        },
        Error::Io(e) => MetadataFailure::Unreadable {
            path: path.to_path_buf(),
            detail: e.to_string(),
        },
        other => MetadataFailure::Corrupt {
            path: path.to_path_buf(),
            detail: other.to_string(),
        },
    }
}

impl Generation {
    /// [`Generation::trusted_metadata`] with the file read injected, so a test can count or forbid
    /// opens. Production passes [`read_checked`].
    pub(super) fn trusted_metadata_with(
        &self,
        read: &dyn Fn(&Path) -> Result<Vec<u8>>,
    ) -> std::result::Result<Arc<SSTableStatistics>, MetadataFailure> {
        self.loaded_with(read).map(|l| l.stats)
    }

    fn loaded_with(
        &self,
        read: &dyn Fn(&Path) -> Result<Vec<u8>>,
    ) -> std::result::Result<Loaded, MetadataFailure> {
        self.metadata
            .get_or_init(|| self.load_metadata(read))
            .clone()
    }

    /// The repair fields of the trusted `Statistics.db`, from the same single read (G11). A decode
    /// failure is `Corrupt`, naming the file.
    #[cfg(feature = "write-support")]
    pub(crate) fn repair_metadata(
        &self,
    ) -> std::result::Result<Arc<RepairMetadata>, MetadataFailure> {
        self.loaded_with(&read_checked)?
            .repair
            .map_err(|detail| self.corrupt_decode(detail))
    }

    /// The STATS-extras histograms of the trusted `Statistics.db`, from the same single read
    /// (G11). A decode failure is `Corrupt`, naming the file.
    #[cfg(feature = "write-support")]
    pub(crate) fn stats_extras(&self) -> std::result::Result<Arc<StatsExtras>, MetadataFailure> {
        self.loaded_with(&read_checked)?
            .extras
            .map_err(|detail| self.corrupt_decode(detail))
    }

    #[cfg(feature = "write-support")]
    fn corrupt_decode(&self, detail: String) -> MetadataFailure {
        MetadataFailure::Corrupt {
            path: self
                .directory
                .join(format!("{}-{STATISTICS_COMPONENT}", self.base_name())),
            detail,
        }
    }

    /// The STATS fields `diagnose` reports beyond the typed value, from the same single read
    /// (G11, GD9). A decode failure is `Corrupt`, naming the file.
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    pub fn statistics_facts(&self) -> std::result::Result<Arc<StatisticsFacts>, MetadataFailure> {
        self.statistics_facts_via(&read_checked)
    }

    /// The `SerializationHeader.EncodingStats` baseline `(minTimestamp, minLocalDeletionTime,
    /// minTTL)` of the trusted `Statistics.db`, or `None` when its header does not carry one
    /// (`rebuild` reads it as the delta-encoding baseline of the unchanged `Data.db`, GD3).
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    pub fn encoding_stats_baseline(
        &self,
    ) -> std::result::Result<Option<(i64, i64, i64)>, MetadataFailure> {
        self.loaded_with(&read_checked).map(|l| l.encoding_baseline)
    }

    /// [`Generation::statistics_facts`] with the file read injected. Not part of the stable
    /// surface.
    #[doc(hidden)]
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    pub fn statistics_facts_via(
        &self,
        read: &dyn Fn(&Path) -> Result<Vec<u8>>,
    ) -> std::result::Result<Arc<StatisticsFacts>, MetadataFailure> {
        self.loaded_with(read)?
            .facts
            .map_err(|detail| MetadataFailure::Corrupt {
                path: self
                    .directory
                    .join(format!("{}-{STATISTICS_COMPONENT}", self.base_name())),
                detail,
            })
    }

    /// [`Generation::trusted_metadata`] with the file read injected, so a caller can count or
    /// forbid opens (the GD9 one-read-per-run proof). Not part of the stable surface.
    #[doc(hidden)]
    pub fn trusted_metadata_via(
        &self,
        read: &dyn Fn(&Path) -> Result<Vec<u8>>,
    ) -> std::result::Result<Arc<SSTableStatistics>, MetadataFailure> {
        self.trusted_metadata_with(read)
    }

    fn load_metadata(
        &self,
        read: &dyn Fn(&Path) -> Result<Vec<u8>>,
    ) -> std::result::Result<Loaded, MetadataFailure> {
        // 1. The floor, from the descriptor alone: no file has been touched.
        if let Some(below) = &self.below_floor {
            return Err(MetadataFailure::UnsupportedVersion {
                version: below.version.clone(),
                floor: below.floor,
            });
        }
        let path = match self.component_path(STATISTICS_COMPONENT) {
            Some(p) => p.to_path_buf(),
            None => {
                return Err(MetadataFailure::Absent {
                    path: self
                        .directory
                        .join(format!("{}-{STATISTICS_COMPONENT}", self.base_name())),
                })
            }
        };
        // 2 and 3. Bounded read and the strict layout and checksum check.
        let bytes = read(&path).map_err(|e| classify_read_error(&path, e))?;
        // 4. One parse. The floor passed, so the gates exist; a failure here is a defect of the
        // file, reported as such.
        let gates = match self.format {
            SsTableFormat::Big => {
                BigVersionGates::from_version(&self.version).map(VersionGates::Big)
            }
            SsTableFormat::Bti => {
                BtiVersionGates::from_version(&self.version).map(VersionGates::Bti)
            }
        }
        .map_err(|e| MetadataFailure::Corrupt {
            path: path.clone(),
            detail: e.to_string(),
        })?;
        let parsed = parse_statistics_with_fallback_detailed(&bytes, Some(&gates)).map(|(_, s)| s);
        #[cfg(feature = "write-support")]
        let repair = parse_repair_metadata(&bytes, Some(&gates))
            .map(Arc::new)
            .map_err(|e| e.to_string());
        #[cfg(feature = "write-support")]
        let extras = parse_stats_extras(&bytes, Some(&gates))
            .map(Arc::new)
            .map_err(|e| e.to_string());
        // The facts are decoded from the same buffer; it is dropped when this function returns.
        #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
        let facts = decode_facts(&bytes, &gates).map(Arc::new);
        #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
        let encoding_baseline =
            crate::parser::enhanced_statistics_parser::read_encoding_stats_baseline(&bytes);
        match parsed {
            Ok(stats) => Ok(Loaded {
                stats: Arc::new(stats),
                #[cfg(feature = "write-support")]
                repair,
                #[cfg(feature = "write-support")]
                extras,
                #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
                facts,
                #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
                encoding_baseline,
            }),
            Err(e) => Err(MetadataFailure::Corrupt {
                path,
                detail: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::generation::{list_generations, Scope, TocPolicy};
    use std::cell::Cell;

    /// A one-generation directory. The `Data.db` is a placeholder the listing only measures; no
    /// test here parses it.
    fn one_generation(prefix: &str, statistics: Option<&[u8]>) -> (tempfile::TempDir, Generation) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(format!("{prefix}-Data.db")), b"x").unwrap();
        std::fs::write(dir.path().join(format!("{prefix}-TOC.txt")), b"Data.db\n").unwrap();
        if let Some(bytes) = statistics {
            std::fs::write(dir.path().join(format!("{prefix}-Statistics.db")), bytes).unwrap();
        }
        let mut listing = list_generations(dir.path(), &Scope::Live, TocPolicy::Accept).unwrap();
        assert_eq!(listing.live.len(), 1);
        let generation = listing.live.remove(0);
        (dir, generation)
    }

    fn forbidden(_: &Path) -> Result<Vec<u8>> {
        panic!("a component was opened");
    }

    /// G9.1: below the floor, with no `Statistics.db` beside it, and a reader that fails on any
    /// open.
    #[test]
    fn below_floor_returns_unsupported_version_without_any_open() {
        let (_dir, generation) = one_generation("ma-1-big", None);
        match generation.trusted_metadata_with(&forbidden) {
            Err(MetadataFailure::UnsupportedVersion { version, floor }) => {
                assert_eq!((version.as_str(), floor), ("ma", "na"));
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
        assert!(generation.has_condition(Condition::UnsupportedVersion));
    }

    /// G11.3: a truncated `ma-*` Statistics.db is still `UnsupportedVersion`, never `Corrupt`.
    #[test]
    fn floor_is_checked_before_integrity() {
        let (_dir, generation) = one_generation("ma-1-big", Some(b"truncated"));
        let reads = Cell::new(0);
        let counting = |p: &Path| {
            reads.set(reads.get() + 1);
            read_checked(p)
        };
        let outcome = generation.trusted_metadata_with(&counting);
        assert_eq!(
            outcome.unwrap_err().condition(),
            Condition::UnsupportedVersion
        );
        assert_eq!(reads.get(), 0);
    }

    /// G11.2: a failure is cached, so the file is read once and both calls agree.
    #[test]
    fn a_failure_is_cached_and_the_file_is_read_once() {
        let (_dir, generation) = one_generation("nb-1-big", Some(b"truncated"));
        let reads = Cell::new(0);
        let counting = |p: &Path| {
            reads.set(reads.get() + 1);
            read_checked(p)
        };
        let first = generation.trusted_metadata_with(&counting).unwrap_err();
        let second = generation.trusted_metadata_with(&forbidden).unwrap_err();
        assert_eq!(reads.get(), 1);
        assert_eq!(first.condition(), Condition::Corrupt);
        assert_eq!(first.to_string(), second.to_string());
        assert!(first.to_string().contains("nb-1-big-Statistics.db"));
    }

    #[test]
    fn no_statistics_component_is_absent() {
        let (_dir, generation) = one_generation("nb-1-big", None);
        let failure = generation.trusted_metadata_with(&forbidden).unwrap_err();
        assert_eq!(failure.condition(), Condition::Absent);
    }

    /// The direct read's floor follows the file name's descriptor, not a `-bti-` in the directory.
    #[cfg(feature = "write-support")]
    #[test]
    fn direct_read_floor_comes_from_the_file_name_not_the_directory() {
        let dir = tempfile::tempdir().unwrap().keep().join("x-bti-y");
        std::fs::create_dir_all(&dir).unwrap();
        match read_direct(&dir.join("ma-1-big-Statistics.db")) {
            Err(MetadataFailure::UnsupportedVersion { version, floor }) => {
                assert_eq!((version.as_str(), floor), ("ma", "na"));
            }
            other => panic!("expected the BIG floor, got {other:?}"),
        }
        match read_direct(&dir.join("na-1-bti-Statistics.db")) {
            Err(MetadataFailure::UnsupportedVersion { floor, .. }) => assert_eq!(floor, "da"),
            other => panic!("expected the BTI floor, got {other:?}"),
        }
    }
}
