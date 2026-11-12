//! Invariants 1 and 2 (BIG): `Index.db` entry -> `Data.db` header, and
//! `Summary.db` sample -> `Index.db` entry.
//!
//! Both consume ONE authoritative walk of `Index.db` that records, per entry,
//! its own byte offset in `Index.db`, its key and its declared `Data.db` offset.

use super::data_view::DataView;
use super::{skip, AuditVerdict, BOUNDARY_SOURCE_UNREADABLE};
use crate::platform::Platform;
use crate::storage::sstable::index_reader::parse_big_index_entry;
use crate::storage::sstable::summary_reader::SummaryReader;
use crate::storage::sstable::verify::{VerifyErrorClass, VerifyFinding};
use std::path::Path;
use std::sync::Arc;

/// Cap on per-invariant findings so a wrecked file cannot flood the report.
const MAX_FINDINGS_PER_INVARIANT: usize = 8;

pub(super) struct IndexEntryAt {
    pub(super) index_offset: u64,
    pub(super) key: Vec<u8>,
    pub(super) data_offset: u64,
}

pub(super) enum IndexWalk {
    Entries(Vec<IndexEntryAt>),
    /// The walk could not be completed (the cause is logged at debug; the
    /// structural check already pushed the finding that names it).
    Unreadable,
}

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unreadable(why: String) -> IndexWalk {
    tracing::debug!("audit: Index.db walk unreadable: {why}");
    IndexWalk::Unreadable
}

/// Walk `Index.db` end to end. Any parse failure, no-progress entry, trailing
/// garbage or empty file is `Unreadable` (never a partial list: a partial walk
/// would make later "not found" verdicts false).
pub(super) fn walk_index(path: &Path) -> IndexWalk {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => return unreadable(format!("cannot read Index.db: {e}")),
    };
    if bytes.is_empty() {
        return unreadable("Index.db is empty".to_string());
    }
    let total = bytes.len();
    let mut remaining: &[u8] = &bytes;
    let mut out = Vec::new();
    while !remaining.is_empty() {
        let at = total - remaining.len();
        match parse_big_index_entry(remaining) {
            Ok((rest, entry)) if rest.len() < remaining.len() => {
                out.push(IndexEntryAt {
                    index_offset: at as u64,
                    key: entry.key_digest.to_vec(),
                    data_offset: entry.data_offset,
                });
                remaining = rest;
            }
            Ok(_) => return unreadable(format!("entry at {at} made no progress")),
            Err(e) => return unreadable(format!("entry at {at} failed to parse: {e:?}")),
        }
    }
    IndexWalk::Entries(out)
}

/// Invariant 1: every entry's `(key, data_offset)` lands on a `Data.db`
/// partition header carrying that key. Reads only the header of each partition
/// `Index.db` names.
pub(super) fn check_index_data_identity(
    entries: &[IndexEntryAt],
    view: &mut DataView,
    findings: &mut Vec<VerifyFinding>,
) -> AuditVerdict {
    if let Some(why) = view.unavailable() {
        return skip(format!("{BOUNDARY_SOURCE_UNREADABLE}: {why}"));
    }
    let mut failures = 0usize;
    let mut unreadable: Option<String> = None;
    for (i, e) in entries.iter().enumerate() {
        match view.key_at(e.data_offset) {
            Ok(found) if found == e.key => {}
            Ok(found) => {
                failures += 1;
                if failures <= MAX_FINDINGS_PER_INVARIANT {
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::IndexPositionKeyMismatch,
                        "Index.db",
                        format!(
                            "Index.db entry {i} (at Index.db offset {}) names key {} at Data.db \
                             offset {}, but the partition header there carries key {}",
                            e.index_offset,
                            hex(&e.key),
                            e.data_offset,
                            hex(&found)
                        ),
                    ));
                }
            }
            Err(why) => {
                // A header that cannot be read at an index-named offset is a
                // disagreement between Index.db and Data.db, not an unmeasured row.
                failures += 1;
                unreadable.get_or_insert_with(|| why.clone());
                if failures <= MAX_FINDINGS_PER_INVARIANT {
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::IndexPositionKeyMismatch,
                        "Index.db",
                        format!(
                            "Index.db entry {i} (at Index.db offset {}) names key {} at Data.db \
                             offset {}, but no partition header can be read there: {why}",
                            e.index_offset,
                            hex(&e.key),
                            e.data_offset
                        ),
                    ));
                }
            }
        }
    }
    if failures > MAX_FINDINGS_PER_INVARIANT {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::IndexPositionKeyMismatch,
            "Index.db",
            format!(
                "{} further Index.db entr(ies) disagree with Data.db (reported {} above)",
                failures - MAX_FINDINGS_PER_INVARIANT,
                MAX_FINDINGS_PER_INVARIANT
            ),
        ));
    }
    if failures == 0 {
        AuditVerdict::Pass
    } else {
        AuditVerdict::Fail
    }
}

/// Invariant 2: every `Summary.db` sample's `(partition_key, position)` resolves,
/// at that byte offset INTO `Index.db`, to an entry carrying the same key.
pub(super) async fn check_summary_index_correlation(
    summary_path: &Path,
    entries: &[IndexEntryAt],
    platform: &Arc<Platform>,
    findings: &mut Vec<VerifyFinding>,
) -> AuditVerdict {
    if !summary_path.exists() {
        return skip("no Summary.db");
    }
    let reader = match SummaryReader::open(summary_path, platform.clone()).await {
        Ok(r) => r,
        Err(e) => {
            // Cause named; the corruption itself is the SummaryCorrupt finding.
            return skip(format!("Summary.db unreadable: {e}"));
        }
    };
    let samples = reader.get_entries();
    if samples.is_empty() {
        return skip("Summary.db carries no samples");
    }
    let mut failures = 0usize;
    for (i, s) in samples.iter().enumerate() {
        let at = entries.binary_search_by_key(&s.position, |e| e.index_offset);
        let problem = match at {
            Ok(j) if entries[j].key == s.partition_key => None,
            Ok(j) => Some(format!(
                "sample {i} claims key {} at Index.db offset {}, but the entry there carries key {}",
                hex(&s.partition_key),
                s.position,
                hex(&entries[j].key)
            )),
            Err(_) => Some(format!(
                "sample {i} claims key {} at Index.db offset {}, which is not the start of any \
                 Index.db entry",
                hex(&s.partition_key),
                s.position
            )),
        };
        if let Some(detail) = problem {
            failures += 1;
            if failures <= MAX_FINDINGS_PER_INVARIANT {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::SummarySampleMismatch,
                    "Summary.db",
                    detail,
                ));
            }
        }
    }
    if failures > MAX_FINDINGS_PER_INVARIANT {
        findings.push(VerifyFinding::new(
            VerifyErrorClass::SummarySampleMismatch,
            "Summary.db",
            format!(
                "{} further sample(s) disagree with Index.db",
                failures - MAX_FINDINGS_PER_INVARIANT
            ),
        ));
    }
    if failures == 0 {
        AuditVerdict::Pass
    } else {
        AuditVerdict::Fail
    }
}
