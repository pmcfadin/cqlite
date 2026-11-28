//! Which files a directory's transaction logs hide (requirements G1, G2), ported from
//! `LogAwareFileLister` (`cassandra-5.0.8` `db/lifecycle/LogAwareFileLister.java:88-203`).
//!
//! The lister is Cassandra's `OnTxnErr.IGNORE` one (`ColumnFamilyStore.java:519`,
//! `Directories.java:1397`): a log whose records fail verification is still classified from the
//! records that did verify, and the failure is returned as a directory note (owner ruling Q44).
//! An inconsistent state still fails the whole listing, as it does in Cassandra under either
//! setting.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::error::{GenerationError, GenerationResult};
use super::model::{DirectoryNote, HiddenBy, HiddenFile};
use super::txn_log::{split_lines, verify, Record, RecordType, TxnLog};

/// Operation names a log file name may carry: `OperationType.fileName` for every value of the
/// enum at `cassandra-5.0.8` (`db/compaction/OperationType.java:27-52`).
const OPERATION_NAMES: &[&str] = &[
    "cancelalloperations",
    "cleanup",
    "scrub",
    "upgradesstables",
    "verify",
    "relocatesstablestocorrectdisk",
    "removedeleteddata",
    "flush",
    "write",
    "anticompactionafterrepair",
    "validation",
    "secondaryindexbuild",
    "viewbuild",
    "compaction",
    "tombstonecompaction",
    "unknowncompactiontype",
    "stream",
    "keycachesave",
    "rowcachesave",
    "countercachesave",
    "indexsummaryredistribution",
];

/// `LogFile.FILE_REGEX` = `^((?:[a-z]+-)?.{2}_)?txn_(.*)_(.*)\.log$`, as `(operation, id)`.
///
/// Returns `None` when the name is not a transaction log at all (`LogFile.isLogFile`). The prefix
/// group is optional and `(.*)_(.*)` backtracks, so the operation is everything before the LAST
/// `_` that follows `txn_`.
pub(super) fn match_log_name(name: &str) -> Option<(&str, &str)> {
    let stem = name.strip_suffix(".log")?;
    // Optional `((?:[a-z]+-)?.{2}_)?` prefix, then `txn_`. Find a `txn_` that is at the start or is
    // preceded by a valid prefix; the prefix is `<fmt->?<2 chars>_`.
    let candidates = stem.match_indices("txn_").map(|(i, _)| i);
    for at in candidates {
        let prefix = &stem[..at];
        if prefix.is_empty() || valid_log_prefix(prefix) {
            let body = &stem[at + 4..];
            let split = body.rfind('_')?;
            return Some((&body[..split], &body[split + 1..]));
        }
    }
    None
}

fn valid_log_prefix(prefix: &str) -> bool {
    // `(?:[a-z]+-)?.{2}_`
    let Some(without_underscore) = prefix.strip_suffix('_') else {
        return false;
    };
    let chars: Vec<char> = without_underscore.chars().collect();
    match chars.len() {
        2 => true,
        n if n > 3 => {
            // `[a-z]+-` then two chars
            let head: String = chars[..n - 2].iter().collect();
            head.strip_suffix('-')
                .is_some_and(|h| !h.is_empty() && h.bytes().all(|b| b.is_ascii_lowercase()))
        }
        _ => false,
    }
}

/// `LogFile.make`: the operation and the id must parse, or the listing fails.
fn check_log_name(path: &Path, operation: &str, id: &str) -> GenerationResult<()> {
    let name_error = |reason: String| GenerationError::TxnLogName {
        path: path.to_path_buf(),
        reason,
    };
    if !OPERATION_NAMES.contains(&operation) {
        return Err(name_error(format!("unknown operation {operation:?}")));
    }
    let parts: Vec<&str> = id.split('-').collect();
    let widths = [8usize, 4, 4, 4, 12];
    let ok = parts.len() == widths.len()
        && parts
            .iter()
            .zip(widths)
            .all(|(p, w)| p.len() == w && p.bytes().all(|b| b.is_ascii_hexdigit()));
    if !ok {
        return Err(name_error(format!("{id:?} is not a UUID")));
    }
    Ok(())
}

/// The outcome of classifying every log of one directory.
#[derive(Debug, Default)]
pub(super) struct Classification {
    /// File name to the log hiding it and why. First log wins.
    pub hidden: BTreeMap<String, (PathBuf, HiddenBy)>,
    pub notes: Vec<DirectoryNote>,
}

/// One log read from disk: `None` when the file is gone.
fn read_log(path: &Path, folder: &Path, relocatable: bool) -> GenerationResult<Option<TxnLog>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        // `FileUtils.readLines` returns no lines for a file that vanished.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(GenerationError::TxnLogUnreadable {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let text = String::from_utf8(bytes).map_err(|e| GenerationError::TxnLogUnreadable {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
    })?;
    Ok(Some(verify(&split_lines(&text), &|r| {
        disk_path(folder, r, relocatable)
    })))
}

/// How a record's path relates to the listed directory.
#[derive(Debug, PartialEq, Eq)]
enum Applies {
    /// Not this directory's record.
    No,
    /// The record's path is inside the canonical directory (Cassandra's `isInFolder`).
    Exact,
    /// Owner ruling Q47 (2026-11-27), a deliberate deviation from `isInFolder`: the record's last
    /// path components (keyspace directory, `<table>-<tableid>` directory) equal the listed
    /// directory's, only the absolute prefix differs. Carries the record's original prefix.
    Relocated(PathBuf),
}

/// The record applies to the directory when its file is a DIRECT child of the canonical folder
/// (a transaction log applies within its one directory only, G8; Cassandra's `isContained` also
/// accepts a deeper path, which this module does not). A record path names a file that need not
/// exist, so its parent is canonicalised.
///
/// `relocatable` is true only when the listed directory IS the table directory (`Scope::Live`):
/// then the Q47 rule also accepts a record whose keyspace and table directory names match. For a
/// snapshot or backups directory the last two components are not (keyspace, table), so only the
/// exact rule is sound there.
fn applies_to_folder(canonical_folder: &Path, record: &Record, relocatable: bool) -> Applies {
    let Some(abs) = record.absolute_path.as_deref() else {
        return Applies::No;
    };
    let p = Path::new(abs);
    // `new File(path).name()`: a path with no final element names no file.
    let (Some(_), Some(record_table)) = (p.file_name(), p.parent()) else {
        return Applies::No;
    };
    let canonical_parent =
        std::fs::canonicalize(record_table).unwrap_or_else(|_| record_table.to_path_buf());
    if canonical_parent == canonical_folder {
        return Applies::Exact;
    }
    if !relocatable {
        return Applies::No;
    }
    // Q47: the table id in the directory name is authoritative metadata, so matching the keyspace
    // and table directory names (the file name is matched by the caller) is not a heuristic.
    let (Some(table_dir), Some(folder_ks)) = (
        canonical_folder.file_name(),
        canonical_folder.parent().and_then(Path::file_name),
    ) else {
        return Applies::No;
    };
    let Some(record_ks) = record_table.parent() else {
        return Applies::No;
    };
    if record_table.file_name() == Some(table_dir) && record_ks.file_name() == Some(folder_ks) {
        Applies::Relocated(
            record_ks
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default(),
        )
    } else {
        Applies::No
    }
}

/// Where a record's files are checked on disk: inside the listed directory, whatever prefix the
/// record names (a relocated record is re-rooted, G8), or nowhere when it does not apply.
pub(super) fn disk_path(folder: &Path, record: &Record, relocatable: bool) -> Option<PathBuf> {
    match applies_to_folder(folder, record, relocatable) {
        Applies::No => None,
        Applies::Exact | Applies::Relocated(_) => {
            Some(folder.join(Path::new(record.absolute_path.as_deref()?).file_name()?))
        }
    }
}

/// One applicable record: the record, the directory files its name prefixes, and the original
/// prefix when the record applied through the Q47 relocation rule.
type Applied<'a> = (&'a Record, Vec<String>, Option<PathBuf>);

/// `LogFile.getFilesOfType`: valid records of `ty` for this folder, each with the files (from
/// `files`) whose names start with the record's file name.
fn files_of_type<'a>(
    log: &'a TxnLog,
    folder: &Path,
    relocatable: bool,
    files: &BTreeSet<String>,
    ty: RecordType,
) -> Vec<Applied<'a>> {
    log.records
        .iter()
        .filter(|r| r.ty == ty && r.is_valid())
        .filter_map(|r| {
            let relocated = match applies_to_folder(folder, r, relocatable) {
                Applies::No => return None,
                Applies::Exact => None,
                Applies::Relocated(prefix) => Some(prefix),
            };
            let matched = files
                .iter()
                .filter(|f| f.starts_with(r.file_name()))
                .cloned()
                .collect();
            Some((r, matched, relocated))
        })
        .collect()
}

/// Classify `logs` (names in `dir`; `table_dir` when `dir` is the table directory itself) against `files` (every non-log, non-directory name in `dir`).
pub(super) fn classify(
    dir: &Path,
    files: &BTreeSet<String>,
    logs: &[String],
    table_dir: bool,
) -> GenerationResult<Classification> {
    let folder = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let mut out = Classification::default();
    let mut ordered: Vec<&String> = logs.iter().collect();
    ordered.sort();

    for name in ordered {
        let path = dir.join(name);
        if let Some((operation, id)) = match_log_name(name) {
            check_log_name(&path, operation, id)?;
        }
        let Some(mut log) = read_log(&path, &folder, table_dir)? else {
            // gone between the listing and the read: nothing to apply
            continue;
        };
        note_problems(&mut out, &path, &log);

        let old = files_of_type(&log, &folder, table_dir, files, RecordType::Remove);
        let new = files_of_type(&log, &folder, table_dir, files, RecordType::Add);
        note_relocation(&mut out, &path, old.iter().chain(new.iter()));
        let old_files: Vec<String> = old.iter().flat_map(|(_, f, _)| f.clone()).collect();
        let new_files: Vec<String> = new.iter().flat_map(|(_, f, _)| f.clone()).collect();
        // `allFilesPresent`: every old record has at least `numFiles` files.
        let all_old_present = old
            .iter()
            .all(|(r, f, _)| i64::from(r.num_files) <= f.len() as i64);

        if log.completed || all_old_present {
            set_temporary(&mut out, &path, &log, &old_files, &new_files);
            continue;
        }

        // Some old files are missing and the log is not complete: re-read once
        // (`LogAwareFileLister.java:163-169`).
        let Some(reread) = read_log(&path, &folder, table_dir)? else {
            continue; // the log is gone: the transaction finished, nothing to hide
        };
        log = reread;
        if log.completed {
            set_temporary(&mut out, &path, &log, &old_files, &new_files);
            continue;
        }
        return Err(GenerationError::TxnInconsistent {
            directory: dir.to_path_buf(),
            log: name.clone(),
        });
    }
    Ok(out)
}

/// `setTemporary`: a committed transaction hides its REMOVE files, anything else its ADD files.
fn set_temporary(
    out: &mut Classification,
    log_path: &Path,
    log: &TxnLog,
    old_files: &[String],
    new_files: &[String],
) {
    let (victims, reason) = if log.committed() {
        (old_files, HiddenBy::CommittedRemove)
    } else {
        (new_files, HiddenBy::UncommittedAdd)
    };
    for file in victims {
        out.hidden
            .entry(file.clone())
            .or_insert_with(|| (log_path.to_path_buf(), reason));
    }
}

/// Q47: one note per log whose applied records carry an absolute prefix other than the listed
/// directory's, naming the first such original prefix.
fn note_relocation<'a>(
    out: &mut Classification,
    log_path: &Path,
    applied: impl Iterator<Item = &'a Applied<'a>>,
) {
    let original = applied.into_iter().find_map(|(_, _, rel)| rel.clone());
    if let Some(original_prefix) = original {
        out.notes.push(DirectoryNote::RelocatedTxnLog {
            path: log_path.to_path_buf(),
            original_prefix,
        });
    }
}

fn note_problems(out: &mut Classification, path: &Path, log: &TxnLog) {
    if !log.problems.is_empty() {
        out.notes.push(DirectoryNote::CorruptTxnLog {
            path: path.to_path_buf(),
            reasons: log.problems.clone(),
        });
    }
}

/// The paths of `HiddenFile`s for a classification, in file-name order.
pub(super) fn hidden_files(dir: &Path, c: &Classification) -> Vec<HiddenFile> {
    c.hidden
        .iter()
        .map(|(file, (log, reason))| HiddenFile {
            path: dir.join(file),
            log: log.clone(),
            reason: *reason,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_names_match_file_regex() {
        let id = "6aa08200-a251-11f0-a3fe-f1a551383fb9";
        assert_eq!(
            match_log_name(&format!("nb_txn_compaction_{id}.log")),
            Some(("compaction", id))
        );
        assert_eq!(
            match_log_name(&format!("bti-da_txn_flush_{id}.log")),
            Some(("flush", id))
        );
        assert_eq!(
            match_log_name(&format!("txn_compaction_{id}.log")),
            Some(("compaction", id))
        );
        assert!(match_log_name("nb-1-big-Data.db").is_none());
        assert!(match_log_name("notes.log").is_none());
        assert!(match_log_name(&format!("abc_txn_flush_{id}.log")).is_none());
        assert!(match_log_name(&format!("nb_txn_flush_{id}.txt")).is_none());
    }

    #[test]
    fn log_name_validation_rejects_unknown_operation_and_bad_uuid() {
        let p = Path::new("x.log");
        let id = "6aa08200-a251-11f0-a3fe-f1a551383fb9";
        assert!(check_log_name(p, "compaction", id).is_ok());
        assert!(check_log_name(p, "bogus", id).is_err());
        assert!(check_log_name(p, "compaction", "not-a-uuid").is_err());
    }

    fn remove_record(path: &str) -> Record {
        Record::parse(&super::super::txn_log::record_line("REMOVE", path, 0, 1))
    }

    /// `new File("<dir>/").name()` is the last element, never "": an empty name would prefix-match
    /// every file in the directory.
    #[test]
    fn record_file_name_is_the_last_element_even_after_a_trailing_slash() {
        assert_eq!(remove_record("/a/b/").file_name(), "b");
        assert_eq!(remove_record("/a/b/nb-1-big-").file_name(), "nb-1-big-");
        assert_eq!(remove_record("").file_name(), "");
    }

    #[test]
    fn a_record_without_a_file_name_never_applies() {
        let folder = Path::new("/nonexistent-4626/ks/t-1");
        for path in [
            "",
            "/",
            "/nonexistent-4626/ks/t-1/..",
            "/nonexistent-4626/ks/t-1/",
        ] {
            let r = remove_record(path);
            assert_eq!(applies_to_folder(folder, &r, true), Applies::No, "{path:?}");
        }
    }
}
