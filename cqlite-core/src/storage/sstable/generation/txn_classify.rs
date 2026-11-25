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
fn read_log(path: &Path) -> GenerationResult<Option<TxnLog>> {
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
    Ok(Some(verify(&split_lines(&text))))
}

/// `PathUtils.isContained(folder, record path)` on canonical paths. A record path names a file
/// that need not exist, so its parent is canonicalised and the name re-attached.
fn is_in_folder(canonical_folder: &Path, record: &Record) -> bool {
    let Some(abs) = record.absolute_path.as_deref() else {
        return false;
    };
    let p = Path::new(abs);
    let canonical = match (p.parent().map(std::fs::canonicalize), p.file_name()) {
        (Some(Ok(parent)), Some(name)) => parent.join(name),
        _ => p.to_path_buf(),
    };
    canonical.starts_with(canonical_folder)
}

/// `LogFile.getFilesOfType`: valid records of `ty` inside the folder, each with the files (from
/// `files`) whose names start with the record's file name.
fn files_of_type<'a>(
    log: &'a TxnLog,
    folder: &Path,
    files: &BTreeSet<String>,
    ty: RecordType,
) -> Vec<(&'a Record, Vec<String>)> {
    log.records
        .iter()
        .filter(|r| r.ty == ty && r.is_valid() && is_in_folder(folder, r))
        .map(|r| {
            let matched = files
                .iter()
                .filter(|f| f.starts_with(r.file_name()))
                .cloned()
                .collect();
            (r, matched)
        })
        .collect()
}

/// Classify `logs` (names in `dir`) against `files` (every non-log, non-directory name in `dir`).
pub(super) fn classify(
    dir: &Path,
    files: &BTreeSet<String>,
    logs: &[String],
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
        let Some(mut log) = read_log(&path)? else {
            // gone between the listing and the read: nothing to apply
            continue;
        };
        note_problems(&mut out, &path, &log);

        let old = files_of_type(&log, &folder, files, RecordType::Remove);
        let new = files_of_type(&log, &folder, files, RecordType::Add);
        let old_files: Vec<String> = old.iter().flat_map(|(_, f)| f.clone()).collect();
        let new_files: Vec<String> = new.iter().flat_map(|(_, f)| f.clone()).collect();
        // `allFilesPresent`: every old record has at least `numFiles` files.
        let all_old_present = old
            .iter()
            .all(|(r, f)| i64::from(r.num_files) <= f.len() as i64);

        if log.completed || all_old_present {
            set_temporary(&mut out, &path, &log, &old_files, &new_files);
            continue;
        }

        // Some old files are missing and the log is not complete: re-read once
        // (`LogAwareFileLister.java:163-169`).
        let Some(reread) = read_log(&path)? else {
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
}
