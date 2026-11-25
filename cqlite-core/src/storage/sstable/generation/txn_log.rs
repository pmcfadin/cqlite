//! Reading and verifying one transaction log (requirement G1, G2), ported from Cassandra.
//!
//! Authority (`cassandra-5.0.8`, `src/java/org/apache/cassandra/db/lifecycle/`):
//! - `LogRecord.java`: the line format `(add|remove|commit|abort):[path,updateTime,numFiles][checksum]`
//!   (`REGEX`, case-insensitive), `make(String)`, `computeChecksum`, the normalisation of fields a
//!   record type does not carry.
//! - `LogReplicaSet.readRecords` (single replica): duplicate records and a final record that is not
//!   last stop the read.
//! - `LogFile.verify`, `verifyRecord`, `verifyRecordWithCorruptedLastRecord`: which records are
//!   valid, and when `completed` is set.
//!
//! `verifyRecord`'s REMOVE-record check of the logged update time against the files' current
//! modification times (`LogFile.java:271-300`) is ported exactly. A directory whose mtimes were not
//! preserved therefore has stale REMOVE records, which follow the IGNORE path: classification
//! continues from the other records and the listing carries a directory note (G2.2).

use std::path::Path;

use crc32fast::Hasher;

/// `LogRecord.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecordType {
    Unknown,
    Add,
    Remove,
    Commit,
    Abort,
}

impl RecordType {
    fn has_file(self) -> bool {
        matches!(self, Self::Add | Self::Remove)
    }
    fn is_final(self) -> bool {
        matches!(self, Self::Commit | Self::Abort)
    }
    /// `Type.toString()`, which `computeChecksum` feeds to the CRC.
    fn java_name(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Add => "ADD",
            Self::Remove => "REMOVE",
            Self::Commit => "COMMIT",
            Self::Abort => "ABORT",
        }
    }
}

/// One decoded line (`LogRecord`).
#[derive(Debug, Clone)]
pub(super) struct Record {
    pub ty: RecordType,
    /// `absolutePath`, present for ADD and REMOVE.
    pub absolute_path: Option<String>,
    pub update_time: i64,
    pub num_files: i32,
    checksum: i64,
    pub error: Option<String>,
    /// Files present on disk for a REMOVE record that passed its checksum (`onDiskRecord.numFiles`).
    on_disk_files: Option<usize>,
}

impl Record {
    fn unknown(reason: String) -> Self {
        Self {
            ty: RecordType::Unknown,
            absolute_path: None,
            update_time: 0,
            num_files: 0,
            checksum: 0,
            error: Some(reason),
            on_disk_files: None,
        }
    }

    /// `LogRecord.make(String line)`.
    pub(super) fn parse(line: &str) -> Self {
        let failed = || Self::unknown(format!("Failed to parse [{line}]"));
        let Some(colon) = line.find(':') else {
            return failed();
        };
        let ty = match line[..colon].to_ascii_uppercase().as_str() {
            "ADD" => RecordType::Add,
            "REMOVE" => RecordType::Remove,
            "COMMIT" => RecordType::Commit,
            "ABORT" => RecordType::Abort,
            _ => return failed(),
        };
        // `:[` inner `][` digits `]`
        let Some(rest) = line[colon + 1..].strip_prefix('[') else {
            return failed();
        };
        let Some(rest) = rest.strip_suffix(']') else {
            return failed();
        };
        let Some(sep) = rest.rfind("][") else {
            return failed();
        };
        let (inner, digits) = (&rest[..sep], &rest[sep + 2..]);
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return failed();
        }
        // `([^,]*),?([^,]*),?([^,]*)`: one to three comma-separated fields, missing ones empty.
        let fields: Vec<&str> = inner.split(',').collect();
        if fields.len() > 3 {
            return failed();
        }
        let field = |i: usize| fields.get(i).copied().unwrap_or("");
        let (path, update_time, num_files, checksum) = match (
            field(1).parse::<i64>(),
            field(2).parse::<i32>(),
            digits.parse::<i64>(),
        ) {
            (Ok(t), Ok(n), Ok(c)) => (field(0), t, n, c),
            _ => return Self::unknown(format!("Failed to parse line: invalid number in [{line}]")),
        };
        Self {
            ty,
            absolute_path: ty.has_file().then(|| path.to_string()),
            // The constructor zeroes what a record type does not carry (`LogRecord.java:263-265`).
            update_time: if ty == RecordType::Remove {
                update_time
            } else {
                0
            },
            num_files: if ty.has_file() { num_files } else { 0 },
            checksum,
            error: None,
            on_disk_files: None,
        }
    }

    /// `LogRecord.computeChecksum`.
    pub(super) fn compute_checksum(&self) -> i64 {
        let mut crc = Hasher::new();
        crc.update(self.absolute_path.as_deref().unwrap_or("").as_bytes());
        crc.update(self.ty.java_name().as_bytes());
        // `FBUtilities.updateChecksumInt`: four big-endian bytes.
        crc.update(&(self.update_time as i32).to_be_bytes());
        crc.update(&((self.update_time as u64 >> 32) as i32).to_be_bytes());
        crc.update(&self.num_files.to_be_bytes());
        i64::from(crc.finalize())
    }

    /// `LogRecord.isValid`.
    pub(super) fn is_valid(&self) -> bool {
        self.error.is_none() && self.ty != RecordType::Unknown
    }

    /// `LogRecord.fileName`: the last path element, e.g. `nb-1-big-`.
    pub(super) fn file_name(&self) -> &str {
        self.absolute_path
            .as_deref()
            .and_then(|p| p.rsplit('/').next())
            .unwrap_or("")
    }

    fn same_record(&self, other: &Self) -> bool {
        // `LogRecord.equals`: checksum, error and the replica's path are excluded on purpose.
        self.ty == other.ty
            && self.absolute_path == other.absolute_path
            && self.num_files == other.num_files
            && self.update_time == other.update_time
    }

    fn set_error(&mut self, error: String) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

/// A verified transaction log (`LogFile` after `verify()`).
#[derive(Debug, Clone, Default)]
pub(super) struct TxnLog {
    pub records: Vec<Record>,
    /// `LogFile.completed()`: the last record is a valid COMMIT or ABORT. Set BEFORE record
    /// checksums are verified, as `LogFile.verify` does (`:222-227`).
    pub completed: bool,
    /// Everything `verify()` found wrong, for the directory note.
    pub problems: Vec<String>,
}

impl TxnLog {
    /// `LogFile.committed`: the last record is a valid COMMIT.
    pub(super) fn committed(&self) -> bool {
        self.records
            .last()
            .is_some_and(|r| r.ty == RecordType::Commit && r.is_valid())
    }
}

/// Split as `Files.readAllLines` does: on `\n`, `\r\n` and lone `\r`, with no trailing empty line.
pub(super) fn split_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let bytes = text.as_bytes();
    let (mut start, mut i) = (0, 0);
    while i < bytes.len() {
        match bytes[i] {
            b'\n' => {
                lines.push(&text[start..i]);
                i += 1;
                start = i;
            }
            b'\r' => {
                lines.push(&text[start..i]);
                i += if bytes.get(i + 1) == Some(&b'\n') {
                    2
                } else {
                    1
                };
                start = i;
            }
            _ => i += 1,
        }
    }
    if start < bytes.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// A file found by `LogRecord.getExistingFiles(absolutePath)`: an entry of the record path's
/// parent whose name starts with the record path's last element.
struct ExistingFile {
    name: String,
    modified_ms: i64,
}

fn existing_files(absolute_path: &str) -> Vec<ExistingFile> {
    let path = Path::new(absolute_path);
    let (Some(parent), Some(prefix)) = (path.parent(), path.file_name().and_then(|n| n.to_str()))
    else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(parent) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            if !name.starts_with(prefix) {
                return None;
            }
            // `File.lastModified()`: positive only while the file exists.
            let modified_ms = e
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_millis() as i64);
            Some(ExistingFile { name, modified_ms })
        })
        .collect()
}

/// `(updateTime, numFiles)` of `LogRecord.make(type, files, minFiles, path, includeStatsFile)`
/// (`LogRecord.java:200-240`): the stats file's mtime is left out unless `include_stats`, but it is
/// still counted.
fn on_disk_record(files: &[ExistingFile], min_files: usize, include_stats: bool) -> (i64, usize) {
    let is_stats = |f: &ExistingFile| f.name.ends_with("-Statistics.db");
    let is_tmp = |f: &ExistingFile| f.name.ends_with(".tmp");
    let (to_verify, stats_exists): (Vec<&ExistingFile>, bool) =
        if !include_stats && !files.is_empty() {
            (
                files
                    .iter()
                    .filter(|f| !is_tmp(f) && !is_stats(f))
                    .collect(),
                files.iter().any(|f| !is_tmp(f) && is_stats(f)),
            )
        } else {
            (files.iter().collect(), false)
        };
    let positive: Vec<i64> = to_verify
        .iter()
        .map(|f| f.modified_ms)
        .filter(|m| *m > 0)
        .collect();
    let last = positive.iter().copied().max().unwrap_or(0);
    let count = positive.len() + usize::from(stats_exists);
    (last, count.max(min_files))
}

/// `LogFile.truncateMillis`: whole seconds, so a jdk8 and a jdk11 log compare equal.
fn truncate_millis(ms: i64) -> i64 {
    ms - (ms % 1000)
}

/// `LogFile.verify()` for a single replica, in IGNORE mode: returns the log as far as it could be
/// read and verified, with `problems` listing what failed. The caller classifies from
/// [`Record::is_valid`] records exactly as `LogAwareFileLister` does.
pub(super) fn verify(lines: &[&str]) -> TxnLog {
    let mut log = TxnLog::default();

    // `LogReplicaSet.readRecords`
    let count = lines.len();
    for (i, line) in lines.iter().enumerate() {
        let mut record = Record::parse(line);
        if log.records.iter().any(|r| r.same_record(&record)) {
            log.problems.push(format!("Duplicated record: {line}"));
            return log;
        }
        if record.ty.is_final() && i != count - 1 {
            record.set_error("This record should have been the last one in all replicas".into());
            log.problems
                .push(format!("Found too many lines after final record: {line}"));
            log.records.push(record);
            return log;
        }
        log.records.push(record);
    }

    // `LogFile.verify`: completed is decided before checksums are checked.
    if let Some(last) = log.records.last() {
        if last.ty.is_final() && last.is_valid() {
            log.completed = true;
        }
    }

    for record in &mut log.records {
        verify_record(record);
    }

    let Some(failed_on) = log.records.iter().position(|r| !r.is_valid()) else {
        return log;
    };
    for r in &log.records {
        if let Some(e) = &r.error {
            log.problems.push(e.clone());
        }
    }
    if failed_on != log.records.len() - 1 {
        return log;
    }
    // Only the last record is bad: tolerated when every earlier REMOVE still has all its files
    // (`verifyRecordWithCorruptedLastRecord`).
    for record in &mut log.records[..failed_on] {
        if record.ty == RecordType::Remove
            && record
                .on_disk_files
                .is_some_and(|n| (n as i64) < i64::from(record.num_files))
        {
            let reason = format!(
                "Incomplete fileset detected for sstable [{}]: number of files [{}] should have been [{}].",
                record.file_name(),
                record.on_disk_files.unwrap_or(0),
                record.num_files
            );
            record.set_error(reason.clone());
            log.problems.push(reason);
        }
    }
    log
}

/// `LogFile.verifyRecord` (`LogFile.java:243-300`): checksum first, then for a REMOVE record the
/// update time of what is on disk now. A record whose files are not where it says (nothing found,
/// time 0) passes the time comparison, as in Cassandra.
fn verify_record(record: &mut Record) {
    if record.error.is_some() || record.ty == RecordType::Unknown {
        return;
    }
    if record.checksum != record.compute_checksum() {
        let expected = record.compute_checksum();
        record.set_error(format!(
            "Invalid checksum for sstable [{}]: [{}] should have been [{}]",
            record.file_name(),
            record.checksum,
            expected
        ));
        return;
    }
    if record.ty != RecordType::Remove {
        return;
    }
    let Some(path) = record.absolute_path.clone() else {
        return;
    };
    let files = existing_files(&path);
    let (disk_time, disk_count) = on_disk_record(&files, 0, false);
    record.on_disk_files = Some(disk_count);
    if truncate_millis(record.update_time) != truncate_millis(disk_time) && disk_time > 0 {
        // A log written by a release that included the stats file's mtime
        // (`LogFile.java:281-290`): accepted.
        let (with_stats, _) = on_disk_record(&files, files.len(), true);
        if truncate_millis(with_stats) == truncate_millis(record.update_time) {
            return;
        }
        record.set_error(format!(
            "Unexpected files detected for sstable [{}]: last update time ({}) should have been ({})",
            record.file_name(),
            disk_time,
            record.update_time
        ));
    }
}

#[cfg(test)]
/// Build a record line the way `LogRecord.format()` writes it.
pub(super) fn record_line(ty: &str, path: &str, update_time: i64, num_files: i32) -> String {
    let probe = Record {
        ty: match ty {
            "ADD" => RecordType::Add,
            "REMOVE" => RecordType::Remove,
            "COMMIT" => RecordType::Commit,
            _ => RecordType::Abort,
        },
        absolute_path: matches!(ty, "ADD" | "REMOVE").then(|| path.to_string()),
        update_time: if ty == "REMOVE" { update_time } else { 0 },
        num_files: if matches!(ty, "ADD" | "REMOVE") {
            num_files
        } else {
            0
        },
        checksum: 0,
        error: None,
        on_disk_files: None,
    };
    format!(
        "{ty}:[{},{},{}][{}]",
        probe.absolute_path.as_deref().unwrap_or(""),
        probe.update_time,
        probe.num_files,
        probe.compute_checksum()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_matches_java_for_a_known_record() {
        // Independent computation of crc32("" + "COMMIT" + 0i32 + 0i32 + 0i32) with zlib's crc32.
        let mut expected = Hasher::new();
        expected.update(b"COMMIT");
        expected.update(&[0u8; 12]);
        let record = Record::parse("COMMIT:[,0,0][0]");
        assert_eq!(record.compute_checksum(), i64::from(expected.finalize()));
    }

    /// The committed log Cassandra 5.0.8 wrote for `gd4626.txn_compaction`
    /// (`test-data/fixtures/issue_4626/txn_committed`), byte for byte. Its record paths are the
    /// container's, so no file is found for the REMOVE records and only the checksums are
    /// exercised: every one must verify against `computeChecksum`.
    #[test]
    fn cassandra_written_records_verify_their_checksums() {
        let dir = "/var/lib/cassandra/data/gd4626/txn_compaction-d2afe860e89911f18c249315fc18c6f9";
        let lines = [
            format!("ADD:[{dir}/nb-4-big-,0,8][2436736489]"),
            format!("REMOVE:[{dir}/nb-3-big-,1795588286893,8][1736859284]"),
            format!("REMOVE:[{dir}/nb-2-big-,1795588283873,8][3206408674]"),
            format!("REMOVE:[{dir}/nb-1-big-,1795588282962,8][1949738987]"),
            "COMMIT:[,0,0][2613697770]".to_string(),
        ];
        let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
        let log = verify(&refs);
        assert!(log.problems.is_empty(), "{:?}", log.problems);
        assert!(log.records.iter().all(Record::is_valid));
        assert!(log.completed && log.committed());
        assert_eq!(log.records.len(), 5);
    }

    #[test]
    fn parses_cassandras_documented_forms() {
        let r = Record::parse("ADD:[/data/ks/t-1/nb-3-big-,0,8][2633027732]");
        assert_eq!(r.ty, RecordType::Add);
        assert_eq!(r.absolute_path.as_deref(), Some("/data/ks/t-1/nb-3-big-"));
        assert_eq!((r.update_time, r.num_files), (0, 8));
        assert_eq!(r.file_name(), "nb-3-big-");

        let r = Record::parse("remove:[/d/nb-1-big-,1700000000000,8][5]");
        assert_eq!(r.ty, RecordType::Remove);
        assert_eq!(r.update_time, 1_700_000_000_000);

        // type text is case-insensitive; a commit record carries neither path nor counts
        let r = Record::parse("Commit:[,123,9][7]");
        assert_eq!(r.ty, RecordType::Commit);
        assert_eq!((r.absolute_path, r.update_time, r.num_files), (None, 0, 0));
    }

    #[test]
    fn malformed_lines_are_unknown_records() {
        for bad in [
            "",
            "garbage",
            "ADD:[a,1,2]",
            "ADD:[a,1,2][x]",
            "ADD:[a,1,2][]",
            "ADD:[a,b,c][1]",
            "ADD:[a,1,2,3][1]",
            "FLUSH:[a,1,2][1]",
            "ADD:[a,1,99999999999][1]",
        ] {
            let r = Record::parse(bad);
            assert_eq!(r.ty, RecordType::Unknown, "{bad:?}");
            assert!(!r.is_valid());
        }
    }

    #[test]
    fn split_lines_matches_read_all_lines() {
        assert_eq!(split_lines("a\nb\n"), vec!["a", "b"]);
        assert_eq!(split_lines("a\r\nb\rc"), vec!["a", "b", "c"]);
        assert_eq!(split_lines("a\n\nb"), vec!["a", "", "b"]);
        assert!(split_lines("").is_empty());
    }

    #[test]
    fn complete_committed_log_verifies() {
        let l = [
            record_line("ADD", "/x/nb-3-big-", 0, 8),
            record_line("REMOVE", "/nonexistent/nb-1-big-", 5, 0),
            record_line("COMMIT", "", 0, 0),
        ];
        let refs: Vec<&str> = l.iter().map(String::as_str).collect();
        let log = verify(&refs);
        assert!(log.completed && log.committed());
        assert!(log.problems.is_empty(), "{:?}", log.problems);
    }

    #[test]
    fn bad_checksum_on_final_record_is_completed_but_not_committed() {
        let l = [
            record_line("ADD", "/x/nb-3-big-", 0, 8),
            "COMMIT:[,0,0][12345]".to_string(),
        ];
        let refs: Vec<&str> = l.iter().map(String::as_str).collect();
        let log = verify(&refs);
        assert!(log.completed);
        assert!(!log.committed());
        assert!(!log.problems.is_empty());
    }

    #[test]
    fn corrupt_middle_record_is_invalid_and_reported() {
        let l = [
            "ADD:[/x/nb-3-big-,0,8][1]".to_string(),
            record_line("COMMIT", "", 0, 0),
        ];
        let refs: Vec<&str> = l.iter().map(String::as_str).collect();
        let log = verify(&refs);
        assert!(!log.records[0].is_valid());
        assert!(log.records[1].is_valid());
        assert!(log.committed());
        assert!(!log.problems.is_empty());
    }

    #[test]
    fn duplicate_record_stops_the_read() {
        let a = record_line("ADD", "/x/nb-3-big-", 0, 8);
        let log = verify(&[a.as_str(), a.as_str()]);
        assert_eq!(log.records.len(), 1);
        assert!(!log.completed);
        assert!(!log.problems.is_empty());
    }

    #[test]
    fn final_record_that_is_not_last_stops_the_read() {
        let c = record_line("COMMIT", "", 0, 0);
        let a = record_line("ADD", "/x/nb-3-big-", 0, 8);
        let log = verify(&[c.as_str(), a.as_str()]);
        assert!(!log.completed);
        assert!(!log.records[0].is_valid());
    }
}
