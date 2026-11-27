//! The data a listing returns: generations, conditions, scopes and the accounted-for file sets.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::id::GenerationId;
use super::metadata::{MetadataCell, MetadataFailure};
use super::name::BelowFloor;
use crate::parser::statistics::SSTableStatistics;
use crate::storage::sstable::version_gate::SsTableFormat;

/// A closed, named fact about a generation or its metadata (G12).
///
/// Matched exhaustively: a verb renders each variant in its own vocabulary but adds none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Condition {
    /// `Data.db` present and non-empty, `TOC.txt` absent. Live (G3).
    TocMissing,
    /// `Data.db` missing or zero-length. Reported, not live (G4).
    Orphan,
    /// Version below the floor, or not a version the format defines. Listed, refused on use (G9).
    UnsupportedVersion,
    /// A component could not be read (I/O error). Live.
    Unreadable,
    /// `Statistics.db` failed the integrity check. Live; raised by the metadata accessor.
    Corrupt,
    /// The requested generation or component does not exist. Raised by the metadata accessor.
    Absent,
}

/// Which one directory a listing reads (G8). No scope recurses.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Scope {
    /// The table directory itself.
    #[default]
    Live,
    /// `<table-dir>/backups`.
    Backups,
    /// Both of the above.
    LiveAndBackups,
    /// `<table-dir>/snapshots/<tag>` and nothing else.
    Snapshot(String),
}

/// Whether a caller can handle a generation whose `TOC.txt` is missing (G10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TocPolicy {
    /// Show it with its condition (read-only verbs and `rebuild`).
    Accept,
    /// Refuse the listing by name (`compact` and the write verbs).
    Refuse,
}

/// One component file of a generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Component {
    /// The part after `<version>-<id>-<format>-`, e.g. `Data.db`.
    pub name: String,
    /// Where it is.
    pub path: PathBuf,
}

/// One SSTable: the component files sharing a descriptor in one directory.
#[derive(Debug, Clone)]
pub struct Generation {
    pub(super) directory: PathBuf,
    pub(super) version: String,
    pub(super) id: GenerationId,
    pub(super) format: SsTableFormat,
    pub(super) components: Vec<Component>,
    pub(super) conditions: Vec<Condition>,
    pub(super) below_floor: Option<BelowFloor>,
    pub(super) metadata: MetadataCell,
}

impl Generation {
    /// The directory the files are in.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    /// Version letters, e.g. `nb`.
    pub fn version(&self) -> &str {
        &self.version
    }
    /// The generation id.
    pub fn id(&self) -> &GenerationId {
        &self.id
    }
    /// BIG or BTI.
    pub fn format(&self) -> SsTableFormat {
        self.format
    }
    /// `<version>-<id>-<format>`, e.g. `nb-1-big`: the name verbs refuse a generation by.
    pub fn base_name(&self) -> String {
        format!("{}-{}-{}", self.version, self.id, self.format.as_str())
    }
    /// The component files found on disk, sorted by name.
    pub fn components(&self) -> &[Component] {
        &self.components
    }
    /// The path of the component named `name` (e.g. `Data.db`), if it exists.
    pub fn component_path(&self, name: &str) -> Option<&Path> {
        self.components
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.path.as_path())
    }
    /// The conditions known from listing, sorted. Metadata failures are not here; they come from
    /// [`Generation::trusted_metadata`].
    pub fn conditions(&self) -> &[Condition] {
        &self.conditions
    }
    /// Whether `condition` is among [`Generation::conditions`].
    pub fn has_condition(&self, condition: Condition) -> bool {
        self.conditions.contains(&condition)
    }
    /// The floor refusal, if this generation is below it (G9).
    pub fn below_floor(&self) -> Option<&BelowFloor> {
        self.below_floor.as_ref()
    }
    /// True unless this generation is an Orphan.
    pub fn is_live(&self) -> bool {
        !self.has_condition(Condition::Orphan)
    }

    /// The generation's `Statistics.db`, read once and cached, value or failure (G11): floor
    /// first, then the bounded read, the strict integrity check and a single parse. A later call
    /// returns the cached outcome without touching the disk.
    pub fn trusted_metadata(&self) -> Result<Arc<SSTableStatistics>, MetadataFailure> {
        self.trusted_metadata_with(&super::metadata::read_checked)
    }
}

/// Why a file in a scope directory is not part of any listed generation, though it is an SSTable
/// component: a transaction log hides it (G1, G13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiddenBy {
    /// The log committed and this file belongs to a REMOVE record (an obsoleted input).
    CommittedRemove,
    /// The log did not commit and this file belongs to an ADD record (an unfinished output).
    UncommittedAdd,
}

/// A file a transaction log hides, with the log that hides it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenFile {
    /// The hidden file.
    pub path: PathBuf,
    /// The log that hides it.
    pub log: PathBuf,
    /// Why.
    pub reason: HiddenBy,
}

/// A directory-level fact that is not a generation condition (owner ruling Q44).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryNote {
    /// A transaction log had records that failed verification. The listing was classified from
    /// the readable records, as Cassandra's `OnTxnErr.IGNORE` lister does; a verb MAY refuse.
    CorruptTxnLog {
        /// The log.
        path: PathBuf,
        /// What failed, in Cassandra's words where it has them.
        reasons: Vec<String>,
    },
    /// A transaction log applied to this directory although its records name another absolute
    /// prefix: the table directory was relocated or copied. Owner ruling Q47 (2026-11-27), a
    /// documented deviation from Cassandra's `LogRecord.isInFolder`.
    RelocatedTxnLog {
        /// The log.
        path: PathBuf,
        /// The absolute prefix (the directory above the keyspace directory) its records name.
        original_prefix: PathBuf,
    },
}

/// Everything one directory contributed, with every file accounted for (G13).
#[derive(Debug, Clone, Default)]
pub(super) struct DirectoryListing {
    /// Live generations, sorted by `SSTableIdFactory.COMPARATOR`.
    pub live: Vec<Generation>,
    /// Orphans: reported, not live.
    pub orphans: Vec<Generation>,
    /// Files transaction logs hide.
    pub hidden: Vec<HiddenFile>,
    /// The transaction logs.
    pub txn_logs: Vec<PathBuf>,
    /// Files whose names are not SSTable components.
    pub ignored: Vec<PathBuf>,
    /// Directory-level notes.
    pub notes: Vec<DirectoryNote>,
    /// Ids of hidden SSTable components; they still occupy their id (allocation, G6).
    pub(super) hidden_ids: Vec<GenerationId>,
}

/// The result of a listing for one [`Scope`]: the union of its directories.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    /// Live generations of every directory in the scope, sorted by id.
    pub live: Vec<Generation>,
    /// Orphans of every directory in the scope, sorted by id.
    pub orphans: Vec<Generation>,
    /// Files transaction logs hide.
    pub hidden: Vec<HiddenFile>,
    /// Transaction logs.
    pub txn_logs: Vec<PathBuf>,
    /// Files whose names are not SSTable components.
    pub ignored: Vec<PathBuf>,
    /// Directory-level notes.
    pub notes: Vec<DirectoryNote>,
    pub(super) hidden_ids: Vec<GenerationId>,
}
