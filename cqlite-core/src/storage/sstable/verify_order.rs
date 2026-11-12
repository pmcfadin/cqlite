//! Check 8 of `verify` (FULL): partition key / clustering-row ordering and
//! partition-level `localDeletionTime` validity (issue #1282).
//!
//! Relocated verbatim from `verify.rs` (issue #4195 campsite relocation, epic
//! #1116: `verify.rs` is far over the file-size threshold and the audit mode had
//! to add to it). No behaviour change.

use crate::platform::Platform;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::sstable::verify::{VerifyErrorClass, VerifyFinding};
use crate::{Config, Error, Result};
use std::path::Path;
use std::sync::Arc;

/// Check 8 (FULL): partition key/row ordering + partition-level
/// `localDeletionTime` validity (issue #1282).
///
/// Two corruption classes Cassandra's `sstableverify` rejects that the earlier
/// checks did not classify:
///
/// * **Out-of-order key/row** ([`VerifyErrorClass::OutOfOrderKeyOrRow`]).
///   Cassandra stores partitions in ascending **Murmur3 token** order (ties
///   broken by the raw key bytes). We recompute each partition's token with the
///   authoritative [`cassandra_murmur3_token`] (Murmur3Partitioner, matching the
///   rest of CQLite's BTI read path, issue #755) and flag the first
///   `(token, key)` pair that is not strictly greater than its predecessor.
///
/// * **Invalid partition-level local-deletion-time**
///   ([`VerifyErrorClass::InvalidLocalDeletionTime`]). `localDeletionTime` is
///   seconds since the Unix epoch; the only special non-negative value is the
///   live sentinel `i32::MAX`. On the legacy signed (`nb`) `DeletionTime` form a
///   NEGATIVE partition-level value is unambiguously corrupt (Cassandra's
///   `DeletionTime`/`Verifier` rejects it). The unsigned `oa`/`da` form
///   legitimately represents far-future times in `[2^31, 2^32)` as a negative
///   `i32`, so we ONLY flag a negative value when the on-disk format is the
///   signed legacy form — the format, not a heuristic, decides.
///
/// Both facts come from the SAME authoritative partition-header decode the scan
/// already performs (see [`SSTableReader::partition_verify_scan`]); this is not a
/// second guessing pass. Environmental errors (reader open) are surfaced through
/// the existing scan-error classifier rather than aborting verification.
pub(super) async fn check_key_order_and_ldt(
    data_path: &Path,
    config: &Config,
    platform: Arc<Platform>,
    findings: &mut Vec<VerifyFinding>,
) {
    let reader = match SSTableReader::open(data_path, config, platform).await {
        Ok(r) => r,
        Err(_) => {
            // A reader-open failure here is already surfaced by the Check 7 scan
            // (it opens the same reader first); do not double-report it.
            return;
        }
    };
    let signed_ldt = !reader.has_uint_deletion_time();
    let partitions = match reader.partition_verify_scan().await {
        Ok(p) => p,
        Err(_) => {
            // A parse failure is Check 7's territory (RowScanFailed / decode);
            // avoid a duplicate, differently-classed finding for the same cause.
            return;
        }
    };

    findings.extend(classify_order_and_ldt(&partitions, signed_ldt));

    // Row-order half of OutOfOrderKeyOrRow (issue #1282 roborev follow-up):
    // Cassandra's Verifier also rejects out-of-order CLUSTERING rows within a
    // partition. Decode each partition's clustering rows in on-disk order and flag
    // a non-increasing clustering step using the authoritative schema comparator
    // (which respects reversed/DESC clustering order). A table with no clustering
    // columns yields an empty scan and produces no findings.
    if let Some(schema) = reader.effective_schema() {
        // A decode failure is Check 7's territory; do not double-report. Only a
        // successful scan feeds the clustering-order classifier.
        if !schema.clustering_keys.is_empty() {
            if let Ok(partition_rows) = reader.partition_clustering_verify_scan().await {
                findings.extend(classify_clustering_row_order(&partition_rows, &schema));
            }
        }
    }
}

/// Compare two clustering-key tuples in the authoritative schema clustering
/// order (issue #1282 roborev follow-up).
///
/// Each column is compared with its non-gated [`ComparatorType`] (derived from the
/// schema clustering type) and the result reversed for a DESC column, mirroring
/// Cassandra's reversed-type ordering. An absent trailing component (a shorter
/// tuple) is treated as NULL, which sorts first regardless of ASC/DESC — matching
/// `ClusteringKey::compare`. NO heuristics: the format-derived comparator and the
/// schema's ASC/DESC flag decide.
fn compare_clustering_tuples(
    a: &[crate::types::Value],
    b: &[crate::types::Value],
    schema: &crate::schema::TableSchema,
) -> Result<std::cmp::Ordering> {
    use crate::types::Value;
    use std::cmp::Ordering;

    let comparators = schema.get_clustering_key_comparators()?;
    for (i, ck) in schema.clustering_keys.iter().enumerate() {
        let av = a.get(i).unwrap_or(&Value::Null);
        let bv = b.get(i).unwrap_or(&Value::Null);
        // NULL/absent component sorts first regardless of ASC/DESC (no reversal).
        let ord = match (av, bv) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (_, _) => {
                let cmp = comparators
                    .get(i)
                    .ok_or_else(|| {
                        Error::Schema(format!(
                            "missing clustering comparator for column {}",
                            ck.name
                        ))
                    })?
                    .compare(av, bv)?;
                if ck.order == crate::schema::ClusteringOrder::Desc {
                    cmp.reverse()
                } else {
                    cmp
                }
            }
        };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

/// Pure classifier for the ROW half of Check 8 (issue #1282 roborev follow-up):
/// given each partition's clustering-key tuples in on-disk order and the
/// authoritative schema, flag the first partition whose clustering rows are not in
/// strictly ascending schema order as [`VerifyErrorClass::OutOfOrderKeyOrRow`].
///
/// The comparison applies each clustering column's ASC/DESC order via
/// [`compare_clustering_tuples`] — NO heuristics. A non-increasing step (a row
/// equal to or before its predecessor) is corruption Cassandra's `Verifier`
/// rejects.
///
/// Kept side-effect-free so the public verify path and the unit tests drive the
/// EXACT same classification (wiring evidence: `check_key_order_and_ldt` calls
/// this, and `verify_sstable` calls that in FULL mode).
pub(super) fn classify_clustering_row_order(
    partition_rows: &[(usize, Vec<Vec<crate::types::Value>>)],
    schema: &crate::schema::TableSchema,
) -> Vec<VerifyFinding> {
    use std::cmp::Ordering;

    let mut findings = Vec::new();
    for (part_idx, rows) in partition_rows {
        for pair in rows.windows(2) {
            let (prev, cur) = (&pair[0], &pair[1]);
            // A comparator error (schema/type mismatch) is not an ordering fault;
            // Check 7 owns decode/type failures, so skip rather than misclassify.
            let ord = match compare_clustering_tuples(cur, prev, schema) {
                Ok(o) => o,
                Err(_) => continue,
            };
            // On disk a later clustering row MUST be strictly greater than its
            // predecessor; Equal or Less is out-of-order corruption.
            if ord != Ordering::Greater {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::OutOfOrderKeyOrRow,
                    "Data.db",
                    format!(
                        "partition {} has an out-of-order clustering row: {:?} is not strictly after the previous row {:?} in schema clustering order",
                        part_idx, cur, prev,
                    ),
                ));
                break;
            }
        }
    }
    findings
}

/// Pure classifier for Check 8 (issue #1282): given the on-disk-ordered
/// `(raw_partition_key, partition_local_deletion_time)` list from
/// [`SSTableReader::partition_verify_scan`] and whether the on-disk
/// `DeletionTime` is the legacy SIGNED form, return any order / LDT findings.
///
/// Kept side-effect-free so both the public verify path and the unit tests drive
/// the EXACT same classification (wiring evidence: `check_key_order_and_ldt`
/// calls this, and `verify_sstable` calls that in FULL mode).
pub(super) fn classify_order_and_ldt(
    partitions: &[(Vec<u8>, Option<i32>)],
    signed_ldt: bool,
) -> Vec<VerifyFinding> {
    use crate::util::cassandra_murmur3::cassandra_murmur3_token;

    let mut findings = Vec::new();
    let hex = |b: &[u8]| b.iter().map(|x| format!("{:02x}", x)).collect::<String>();

    // ---- Out-of-order partition keys (Murmur3 token order) -----------------
    let mut prev: Option<(i64, Vec<u8>)> = None;
    for (idx, (key, _ldt)) in partitions.iter().enumerate() {
        let token = cassandra_murmur3_token(key);
        if let Some((prev_token, prev_key)) = prev.as_ref() {
            // Cassandra orders by (token, key bytes). A later partition MUST be
            // strictly greater; equal or lesser is out-of-order corruption.
            let ordered = (*prev_token, prev_key.as_slice()) < (token, key.as_slice());
            if !ordered {
                findings.push(VerifyFinding::new(
                    VerifyErrorClass::OutOfOrderKeyOrRow,
                    "Data.db",
                    format!(
                        "partition {} (key {}, token {}) is not strictly after the previous partition (key {}, token {}) — partitions are stored out of Murmur3 token order",
                        idx,
                        hex(key),
                        token,
                        hex(prev_key),
                        prev_token,
                    ),
                ));
                break;
            }
        }
        prev = Some((token, key.clone()));
    }

    // ---- Negative (invalid) partition-level localDeletionTime (nb) ---------
    if signed_ldt {
        for (key, ldt) in partitions {
            if let Some(ldt) = ldt {
                // A deleted partition's localDeletionTime is epoch-seconds; it
                // cannot be negative. (The live sentinel i32::MAX is positive and
                // is already resolved to `None` by the header parser.)
                if *ldt < 0 {
                    findings.push(VerifyFinding::new(
                        VerifyErrorClass::InvalidLocalDeletionTime,
                        "Data.db",
                        format!(
                            "partition (key {}) has a negative localDeletionTime {} (0x{:08x}) on the signed (nb) DeletionTime form — a valid deletion time is >= 0 seconds since epoch",
                            hex(key),
                            ldt,
                            *ldt as u32,
                        ),
                    ));
                    break;
                }
            }
        }
    }

    findings
}
