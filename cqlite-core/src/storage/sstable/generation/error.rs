//! The named errors of the generation module.
//!
//! These fail a whole listing or an allocation. A per-generation fact is a
//! [`Condition`](super::Condition), never an error here.

use std::path::PathBuf;

/// Why a listing or an id allocation was refused.
#[derive(Debug, thiserror::Error)]
pub enum GenerationError {
    /// A transaction log is not complete, an old file it names is missing, and a re-read did not
    /// complete it: "inconsistent disk state" (`LogAwareFileLister.java:156-186`, throw at `:184`).
    /// No partial set is returned (G2).
    #[error(
        "inconsistent transaction state in {}: log {log} is not complete and a file it removes is missing",
        directory.display()
    )]
    TxnInconsistent {
        /// The directory being listed.
        directory: PathBuf,
        /// The log's file name.
        log: String,
    },

    /// A transaction log could not be read (I/O error or invalid UTF-8), which makes Cassandra's
    /// listing throw as well (`FileUtils.readLines`).
    #[error("transaction log {} could not be read: {source}", path.display())]
    TxnLogUnreadable {
        /// The log.
        path: PathBuf,
        /// The underlying failure.
        #[source]
        source: std::io::Error,
    },

    /// A file name matches `LogFile.FILE_REGEX` but its operation or id does not parse, which makes
    /// `LogFile.make` throw (`db/lifecycle/LogFile.java:95-108`).
    #[error("transaction log name {} is not a valid log name: {reason}", path.display())]
    TxnLogName {
        /// The log.
        path: PathBuf,
        /// What did not parse.
        reason: String,
    },

    /// The caller does not accept `TocMissing` and these generations carry it (G3.2). Every one is
    /// named; none is dropped.
    #[error("generations without TOC.txt are refused here: {}", generations.join(", "))]
    TocMissingRefused {
        /// The generations' base names, e.g. `nb-1-big`.
        generations: Vec<String>,
    },

    /// The next sequence id would exceed the Java `int` range (G7).
    #[error(
        "no sequence id is left: the highest id is {highest} and the Java int limit is 2147483647"
    )]
    SequenceIdExhausted {
        /// The highest sequence id found.
        highest: u32,
    },

    /// An explicitly requested sequence id is outside the Java `int` range (G7).
    #[error("sequence id {requested} is outside the Java int range 0..=2147483647")]
    SequenceIdOutOfRange {
        /// The id requested.
        requested: u64,
    },

    /// An explicitly requested sequence id is already used by a descriptor in the directory (any
    /// format, version or state), so a writer would publish onto an existing name.
    #[error(
        "sequence id {id} is already in use in {}: {}",
        directory.display(),
        generations.join(", ")
    )]
    IdInUse {
        /// The id requested.
        id: u32,
        /// The directory a writer would publish into.
        directory: PathBuf,
        /// The base names (or hidden file names) that hold the id.
        generations: Vec<String>,
    },

    /// A snapshot tag that is not a single directory name.
    #[error("snapshot tag {tag:?} is not a single directory name")]
    InvalidSnapshotTag {
        /// The tag as given.
        tag: String,
    },

    /// A directory could not be listed.
    #[error("cannot list {}: {source}", path.display())]
    Io {
        /// The directory.
        path: PathBuf,
        /// The underlying failure.
        #[source]
        source: std::io::Error,
    },
}

/// Convenience alias for this module.
pub type GenerationResult<T> = std::result::Result<T, GenerationError>;
