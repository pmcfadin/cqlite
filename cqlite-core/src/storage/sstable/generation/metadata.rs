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
use crate::error::{Error, Result};
use crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback_detailed;
use crate::parser::statistics::SSTableStatistics;
use crate::storage::sstable::statistics_reader::integrity;
use crate::storage::sstable::version_gate::{
    BigVersionGates, BtiVersionGates, SsTableFormat, VersionGates,
};

const STATISTICS_COMPONENT: &str = "Statistics.db";

/// The shared, lazily filled cache of one generation's metadata outcome. Cloning a
/// [`Generation`] shares it, so a failure is judged once however many copies exist.
pub(super) type MetadataCell =
    Arc<OnceLock<std::result::Result<Arc<SSTableStatistics>, MetadataFailure>>>;

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
        self.metadata
            .get_or_init(|| self.load_metadata(read))
            .clone()
    }

    fn load_metadata(
        &self,
        read: &dyn Fn(&Path) -> Result<Vec<u8>>,
    ) -> std::result::Result<Arc<SSTableStatistics>, MetadataFailure> {
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
        match parse_statistics_with_fallback_detailed(&bytes, Some(&gates)) {
            Ok((_, stats)) => Ok(Arc::new(stats)),
            Err(e) => Err(MetadataFailure::Corrupt {
                path,
                detail: e.to_string(),
            }),
        }
    }
}
