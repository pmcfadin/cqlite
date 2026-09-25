//! The filesystem reads a directory scan performs, and how it RECORDS what it
//! could not read (issue #4159).
//!
//! Split out of `scanner.rs` per the campsite rule (epic #1116): the walk-recording
//! logic is a separate responsibility from [`Scanner`](super::Scanner)'s
//! keyspace/table enumeration, and this branch's changes pushed the combined file
//! over the size threshold.
//!
//! Every function here exists because its `std` counterpart answers an unreadable
//! directory the same way it answers an empty one:
//!
//! * [`read_dir_named`] over `std::fs::read_dir` — names the directory AND what it
//!   was being read as, and preserves the [`std::io::ErrorKind`];
//! * [`dir_entry_named`] over `Iterator::flatten` on a `ReadDir` — whose items are
//!   `io::Result<DirEntry>`, so `flatten` drops each `Err` with no log at all and an
//!   entry that failed mid-iteration silently never existed;
//! * [`entry_is_dir`] over `Path::is_dir()` — which answers `false` for a directory
//!   it could not `stat`, silently pruning a whole subtree.
//!
//! [`note_unreadable`] is the recording side: a gap becomes a structured
//! [`UnreadableDirectory`] on the [`ScanResult`](super::ScanResult), not a prose
//! warning, because a caller has to be able to fail closed on it without parsing
//! text. See `scanner`'s module doc for the four-outcome contract this serves and
//! why recording — rather than swallowing OR aborting — is the answer.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// One directory a scan could not read.
///
/// Structured rather than a `warnings` string: a caller has to be able to FAIL
/// CLOSED on this (and to name the path to an operator) without parsing prose, and
/// the [`std::io::ErrorKind`] is what separates a permissions problem from a
/// directory that was removed mid-scan.
#[derive(Debug, Clone)]
pub struct UnreadableDirectory {
    /// The directory that could not be read.
    pub path: PathBuf,
    /// What it was being read as ("keyspace", "table", …).
    pub role: String,
    /// The original failure's kind.
    pub kind: std::io::ErrorKind,
    /// The original failure, rendered.
    pub message: String,
}

/// Read `dir`, naming it (and what it was being read AS) on failure.
///
/// Issue #4159: a directory this scanner cannot read must not read as an EMPTY
/// directory — an unreadable keyspace and a keyspace with no tables are different
/// facts, and a caller cannot tell them apart from a short result set.
pub(super) fn read_dir_named(dir: &Path, what: &str) -> Result<std::fs::ReadDir> {
    std::fs::read_dir(dir).map_err(|e| {
        Error::Io(std::io::Error::new(
            e.kind(),
            format!("Failed to read {what} directory {}: {e}", dir.display()),
        ))
    })
}

/// Unwrap one `ReadDir` item, naming the directory it came from on failure.
///
/// `ReadDir` yields `io::Result<DirEntry>`: an entry can fail MID-ITERATION (a
/// concurrent unlink, an I/O fault). `Iterator::flatten` discards those silently,
/// which is how an entry came to "never exist".
pub(super) fn dir_entry_named(
    item: std::io::Result<std::fs::DirEntry>,
    dir: &Path,
) -> Result<std::fs::DirEntry> {
    item.map_err(|e| {
        Error::Io(std::io::Error::new(
            e.kind(),
            format!("Failed to read an entry of {}: {e}", dir.display()),
        ))
    })
}

/// Record `e` against `dir` as a gap, rather than swallowing it or aborting.
pub(super) fn note_unreadable(
    into: &mut Vec<UnreadableDirectory>,
    dir: &Path,
    role: &str,
    e: &Error,
    kind: std::io::ErrorKind,
) {
    tracing::warn!(
        "SSTable discovery could not read {role} directory {}: {e}. The scan is \
         INCOMPLETE.",
        dir.display()
    );
    into.push(UnreadableDirectory {
        path: dir.to_path_buf(),
        role: role.to_string(),
        kind,
        message: e.to_string(),
    });
}

/// The [`std::io::ErrorKind`] behind a crate [`Error`], for the gap record.
pub(super) fn kind_of(e: &Error) -> std::io::ErrorKind {
    match e {
        Error::Io(io) => io.kind(),
        _ => std::io::ErrorKind::Other,
    }
}

/// Is `entry` a directory, FOLLOWING symlinks? Propagates the `stat` failure
/// instead of answering `false`, which is what `Path::is_dir()` does for a
/// directory it cannot stat.
///
/// # Why `std::fs::metadata` and not `DirEntry::file_type`
///
/// `DirEntry::file_type` does NOT follow symlinks — it reports the LINK. The
/// `Path::is_dir()` this replaced does follow them, so using `file_type` here
/// silently skipped a symlinked keyspace or table directory with no
/// unreadable-directory record at all: incomplete results reported as complete,
/// which is the exact swallow class this module exists to remove, reintroduced by
/// the fix for it. A data directory whose keyspaces are symlinks onto separate
/// mounts is a normal Cassandra layout, not an exotic one.
///
/// A DANGLING symlink is `Ok(false)`, not an error: `metadata` fails with
/// `NotFound` because the TARGET does not exist, and "there is provably no
/// directory here" is genuine absence — the one answer that is not a swallow.
/// Every other failure (`PermissionDenied`, `ELOOP`, EIO) is propagated so the
/// caller records a gap.
pub(super) fn entry_is_dir(entry: &std::fs::DirEntry) -> Result<bool> {
    let path = entry.path();
    match std::fs::metadata(&path) {
        Ok(md) => Ok(md.is_dir()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::Io(std::io::Error::new(
            e.kind(),
            format!(
                "Failed to determine the file type of {}: {e}",
                path.display()
            ),
        ))),
    }
}
