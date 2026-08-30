//! THREE-VALUED filesystem answers for the AD2 golden-parity lane (issue #1491).
//!
//! # The defect class this module removes
//!
//! Every `std::path` predicate is TWO-valued: `Path::is_dir`, `Path::is_file` and
//! `Path::exists` all answer `bool`, so each has to collapse "I could not tell"
//! onto one of its two answers — and each collapses it onto the PERMISSIVE one
//! (`false`, i.e. "not there"). `read_dir` is the same shape once its `Result`s are
//! dropped: `.filter_map(Result::ok)` turns an entry the filesystem could not
//! describe into an entry that was not there. CLAUDE.md records the rule and the
//! remedy — a three-valued answer of `verified-absent` / `present` / `unreadable`.
//!
//! The consequence in THIS lane is specific and it has been found three separate
//! times, in three different places (review findings M3, N4 and V1):
//!
//!   * a fetched-corpus case is allowed to report `NOT PRESENT` and pass when its
//!     fixture is VERIFIABLY absent;
//!   * an unreadable fixture read as an absent one therefore turns a FAILURE into a
//!     legal skip — a failure wearing a skip's clothes, which is the exact shape
//!     this lane exists to prevent.
//!
//! Point-fixing one predicate at a time is what let it recur, so the three answers
//! are produced HERE, once, and every filesystem question in the lane's support
//! code is asked through this module.
//!
//! # Where the line between "absent" and "unreadable" is drawn
//!
//! `ErrorKind::NotFound` is an ANSWER: the filesystem told us there is nothing at
//! that path. Every other error means it could not answer (a permission denied on
//! an ancestor, a non-directory component in the path, an I/O error), and that is
//! reported rather than collapsed.
//!
//! Symlinks are FOLLOWED, as `Path::is_dir`/`Path::is_file` do, so a dangling
//! symlink is `NotFound` on its target and counts as verified absent.

use std::ffi::OsStr;
use std::fs::DirEntry;
use std::path::Path;

/// What the filesystem ANSWERED about one path.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Presence {
    /// The filesystem answered that there is nothing there (`ENOENT`).
    Absent,
    /// A directory (or a symlink to one).
    Dir,
    /// A regular file (or a symlink to one).
    File,
    /// Something else entirely — a socket, a fifo, a device node. Named rather
    /// than folded into `Absent`, so a caller that wanted a file can say what it
    /// found instead of claiming it found nothing.
    Other,
}

impl Presence {
    /// How this answer is named in a diagnostic.
    pub fn describe(self) -> &'static str {
        match self {
            Presence::Absent => "absent",
            Presence::Dir => "a directory",
            Presence::File => "a regular file",
            Presence::Other => "neither a regular file nor a directory",
        }
    }
}

/// What the filesystem says `path` is — `Err` when it could not say.
///
/// The `Err` names the path and the OS error, and says why an unreadable path is
/// not an absent one, because that sentence is the whole point of the module and a
/// caller's own message usually adds only its subject.
pub fn presence(path: &Path) -> Result<Presence, String> {
    match std::fs::metadata(path) {
        Ok(md) if md.is_dir() => Ok(Presence::Dir),
        Ok(md) if md.is_file() => Ok(Presence::File),
        Ok(_) => Ok(Presence::Other),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Presence::Absent),
        Err(e) => Err(format!(
            "{}: cannot be described ({e}) — a path the filesystem could not describe \
             is not a path that verifiably does not exist",
            path.display()
        )),
    }
}

/// Is `path` a directory? `false` only for a VERIFIED answer of absent, or of
/// something that is not a directory; `Err` when the filesystem could not answer.
pub fn is_dir(path: &Path) -> Result<bool, String> {
    Ok(presence(path)? == Presence::Dir)
}

/// Is `path` a regular file? Three-valued like [`is_dir`].
pub fn is_file(path: &Path) -> Result<bool, String> {
    Ok(presence(path)? == Presence::File)
}

/// Every entry of `dir`, or `Ok(None)` when the filesystem ANSWERED that `dir`
/// does not exist. `Err` when it could not answer — including for one entry of an
/// otherwise readable directory, which `.filter_map(Result::ok)` would have
/// dropped as if that entry were not there.
pub fn dir_entries(dir: &Path) -> Result<Option<Vec<DirEntry>>, String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(format!(
                "{}: cannot be listed ({e}) — a directory the filesystem could not list \
                 is not a directory that verifiably holds nothing",
                dir.display()
            ))
        }
    };
    let mut out: Vec<DirEntry> = Vec::new();
    for entry in entries {
        out.push(entry.map_err(|e| {
            format!(
                "{}: an entry could not be enumerated ({e}) — an entry the filesystem \
                 could not describe is not an entry that is absent",
                dir.display()
            )
        })?);
    }
    Ok(Some(out))
}

/// Does this file name start with `prefix`, compared as BYTES?
///
/// Not `file_name().and_then(OsStr::to_str)`: that answers `None` for a name that
/// is not valid UTF-8, and every caller in this lane then treated the name as one
/// that does not match — the same permissive collapse one level over. A fixture
/// directory whose name begins `<table>-` and continues with bytes that are not
/// UTF-8 IS a candidate, and a `*-Data.db` whose stem is not UTF-8 IS an SSTable,
/// so both are decided on the bytes the filesystem actually gave us.
pub fn name_starts_with(name: &OsStr, prefix: &str) -> bool {
    name.as_encoded_bytes().starts_with(prefix.as_bytes())
}

/// Does this file name end with `suffix`, compared as BYTES? See
/// [`name_starts_with`] for why the comparison is not made through `to_str`.
pub fn name_ends_with(name: &OsStr, suffix: &str) -> bool {
    name.as_encoded_bytes().ends_with(suffix.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> tempfile::TempDir {
        tempfile::TempDir::new().expect("tempdir")
    }

    /// The three answers, on the two shapes a real corpus has.
    #[test]
    fn presence_distinguishes_absent_from_a_dir_and_a_file() {
        let tmp = tmp();
        std::fs::create_dir(tmp.path().join("d")).expect("mkdir");
        std::fs::write(tmp.path().join("f"), b"x").expect("write");
        assert_eq!(
            presence(&tmp.path().join("d")).expect("answers"),
            Presence::Dir
        );
        assert_eq!(
            presence(&tmp.path().join("f")).expect("answers"),
            Presence::File
        );
        assert_eq!(
            presence(&tmp.path().join("gone")).expect("answers"),
            Presence::Absent,
            "ENOENT is an ANSWER, not an absence of one"
        );
        assert!(is_dir(&tmp.path().join("d")).expect("answers"));
        assert!(!is_dir(&tmp.path().join("f")).expect("answers"));
        assert!(is_file(&tmp.path().join("f")).expect("answers"));
        assert!(
            !is_file(&tmp.path().join("gone")).expect("answers"),
            "a verified absence is a legal `false`"
        );
    }

    /// A path the filesystem cannot describe is an ERROR, where `Path::is_dir`
    /// answered `false` — the whole defect class (issue #1491 review finding V1).
    ///
    /// Staged without any permission games, which are unreliable as root: a path
    /// that walks THROUGH a regular file cannot be resolved, so `metadata` fails
    /// with `ENOTDIR` rather than `ENOENT`.
    #[test]
    fn a_path_the_filesystem_cannot_describe_is_an_error() {
        let tmp = tmp();
        let file = tmp.path().join("f");
        std::fs::write(&file, b"x").expect("write");
        let through = file.join("inside");
        let why = presence(&through).expect_err("a path through a file cannot be described");
        assert!(
            why.contains("inside") && why.contains("verifiably does not exist"),
            "{why}"
        );
        assert!(is_dir(&through).is_err(), "and the bool form propagates it");
        assert!(is_file(&through).is_err());
        assert!(
            dir_entries(&through).is_err(),
            "as does the listing form — `ENOTDIR` is not `holds nothing`"
        );
    }

    /// A listing distinguishes "no such directory" from "could not list it".
    #[test]
    fn dir_entries_is_three_valued() {
        let tmp = tmp();
        std::fs::write(tmp.path().join("a"), b"x").expect("write");
        let listed = dir_entries(tmp.path()).expect("readable").expect("present");
        assert_eq!(listed.len(), 1);
        assert!(
            dir_entries(&tmp.path().join("gone"))
                .expect("ENOENT is an answer")
                .is_none(),
            "a verified absence is `Ok(None)`, which is a legal skip"
        );
    }

    /// Names are matched on BYTES, so a name that is not valid UTF-8 is still
    /// classified instead of being silently dropped.
    #[test]
    fn names_are_matched_as_bytes() {
        assert!(name_starts_with(OsStr::new("t-abc"), "t-"));
        assert!(!name_starts_with(OsStr::new("other-abc"), "t-"));
        assert!(name_ends_with(OsStr::new("nb-1-big-Data.db"), "-Data.db"));
        assert!(!name_ends_with(
            OsStr::new("nb-1-big-Data.db.jsonl"),
            "-Data.db"
        ));
    }

    /// The discriminating case for the byte comparison: a name `OsStr::to_str`
    /// answers `None` for. Through `to_str` it matched NOTHING, so a fixture
    /// directory or an SSTable carrying such a name was dropped from every scan as
    /// if it were not there.
    ///
    /// `#[cfg(unix)]` because constructing such a name portably is not possible in
    /// safe code — on Windows an `OsStr`'s encoded bytes are WTF-8, not arbitrary
    /// bytes. The FUNCTIONS are portable: both compare against an ASCII pattern,
    /// and UTF-8 and WTF-8 are self-synchronizing, so an ASCII byte run cannot
    /// match the interior of a multi-byte sequence.
    #[cfg(unix)]
    #[test]
    fn a_name_that_is_not_utf8_is_still_classified() {
        use std::os::unix::ffi::OsStrExt;
        let raw = OsStr::from_bytes(b"t-\xff\xfe-Data.db");
        assert!(raw.to_str().is_none(), "the staged name is not valid UTF-8");
        assert!(name_starts_with(raw, "t-"), "the prefix is still seen");
        assert!(name_ends_with(raw, "-Data.db"), "and so is the suffix");
    }
}
