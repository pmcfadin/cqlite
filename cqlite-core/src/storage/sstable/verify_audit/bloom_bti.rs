//! Invariant 3 (bloom, BIG and BTI) and invariant 4 (BTI trie bounds).

use super::data_view::DataView;
use super::index_checks::{hex, IndexEntryAt};
use super::{skip, AuditVerdict, BOUNDARY_SOURCE_UNREADABLE};
use crate::storage::sstable::bloom::BloomFilter;
use crate::storage::sstable::verify::{BtiResolvedLeaf, VerifyErrorClass, VerifyFinding};
use std::path::Path;

fn load_filter(path: &Path) -> Result<BloomFilter, AuditVerdict> {
    if !path.exists() {
        return Err(skip("no Filter.db"));
    }
    let bytes = std::fs::read(path).map_err(|e| skip(format!("Filter.db unreadable: {e}")))?;
    BloomFilter::deserialize(&bytes).map_err(|e| skip(format!("Filter.db unreadable: {e}")))
}

fn false_negative_finding(false_negatives: usize, present: usize, source: &str) -> VerifyFinding {
    VerifyFinding::new(
        VerifyErrorClass::FilterFalseNegative,
        "Filter.db",
        format!(
            "Bloom filter reported {false_negatives} false negative(s) over {present} present \
             partition key(s) ({source}): a present key hashes to a bit the filter reports unset"
        ),
    )
}

/// BIG: probe every `Index.db` key.
pub(super) fn check_bloom_big(
    filter_path: &Path,
    entries: &[IndexEntryAt],
    findings: &mut Vec<VerifyFinding>,
) -> AuditVerdict {
    let bloom = match load_filter(filter_path) {
        Ok(b) => b,
        Err(v) => return v,
    };
    let false_negatives = entries
        .iter()
        .filter(|e| !bloom.might_contain(&e.key))
        .count();
    if false_negatives > 0 {
        findings.push(false_negative_finding(
            false_negatives,
            entries.len(),
            "keys from Index.db",
        ));
        return AuditVerdict::Fail;
    }
    AuditVerdict::Pass
}

/// BTI: probe every key resolved from the trie leaves. A leaf whose key is not
/// inline in `Rows.db` is resolved by a header-only read at its data position.
pub(super) fn check_bloom_bti(
    filter_path: &Path,
    leaves: Option<&[BtiResolvedLeaf]>,
    view: &mut DataView,
    findings: &mut Vec<VerifyFinding>,
) -> AuditVerdict {
    let Some(leaves) = leaves else {
        return skip(BOUNDARY_SOURCE_UNREADABLE);
    };
    let bloom = match load_filter(filter_path) {
        Ok(b) => b,
        Err(v) => return v,
    };
    if let Some(why) = view.unavailable() {
        return skip(format!("{BOUNDARY_SOURCE_UNREADABLE}: {why}"));
    }
    let mut false_negatives = 0usize;
    for leaf in leaves {
        let key = match &leaf.inline_raw_key {
            Some(k) => k.clone(),
            None => match view.key_at(leaf.data_position) {
                Ok(k) => k,
                // Not resolvable here: the bounds/identity row reports it. An
                // unresolvable key cannot be probed, so the row is unmeasured.
                Err(why) => {
                    return skip(format!(
                        "partition key at Data.db offset {} unresolvable: {why}",
                        leaf.data_position
                    ))
                }
            },
        };
        if !bloom.might_contain(&key) {
            false_negatives += 1;
        }
    }
    if false_negatives > 0 {
        findings.push(false_negative_finding(
            false_negatives,
            leaves.len(),
            "keys from BTI trie leaves",
        ));
        return AuditVerdict::Fail;
    }
    AuditVerdict::Pass
}

/// BTI: every `DataOffset` leaf points inside `Data.db`, and (header-only) every
/// leaf with an inline key lands on a partition header carrying that key.
/// `--deep` identity against a full scan is added by the caller in `verify.rs`.
pub(super) fn check_bti_bounds(
    leaves: Option<&[BtiResolvedLeaf]>,
    view: &mut DataView,
    findings: &mut Vec<VerifyFinding>,
) -> AuditVerdict {
    let Some(leaves) = leaves else {
        return skip(BOUNDARY_SOURCE_UNREADABLE);
    };
    let Some(data_len) = view.logical_len() else {
        return skip(format!(
            "{BOUNDARY_SOURCE_UNREADABLE}: {}",
            view.unavailable().unwrap_or("Data.db unavailable")
        ));
    };
    let mut failed = false;
    for leaf in leaves {
        if leaf.inline_raw_key.is_none() && leaf.data_position >= data_len {
            failed = true;
            findings.push(VerifyFinding::new(
                VerifyErrorClass::BtiDataOffsetOutOfBounds,
                "Partitions.db",
                format!(
                    "DataOffset leaf (trie prefix {}) points at Data.db position {}, but Data.db \
                     holds {data_len} bytes",
                    hex(leaf.prefix()),
                    leaf.data_position
                ),
            ));
            continue;
        }
        if let Some(inline) = &leaf.inline_raw_key {
            match view.key_at(leaf.data_position) {
                Ok(found) if &found == inline => {}
                other => {
                    failed = true;
                    let got = match other {
                        Ok(f) => format!("key {}", hex(&f)),
                        Err(why) => format!("no readable header ({why})"),
                    };
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::BtiRootPointerCorrupt,
                        "Partitions.db",
                        format!(
                            "BTI leaf resolves to key {} at Data.db position {}, but the \
                             partition header there has {got}",
                            hex(inline),
                            leaf.data_position
                        ),
                    ));
                }
            }
        }
    }
    if failed {
        AuditVerdict::Fail
    } else {
        AuditVerdict::Pass
    }
}
