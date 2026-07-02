//! Partition- and clustering-lookup classification for the SELECT executor.
//!
//! These helpers decide whether the pushed-down predicates fully constrain the
//! partition key (so a targeted lookup can prune SSTables) and whether a
//! single-column clustering restriction can be pushed to a within-partition
//! seek. They NEVER change correctness — they only choose a narrower read path;
//! the post-scan predicate evaluation applies the exact bound.

use super::super::access_path::{AccessPath, FallbackReason};
use super::super::select_optimizer::SSTablePredicate;
use crate::types::{RowKey, ScanRow, Value};

/// Outcome of classifying whether a SELECT can use a partition-targeted lookup.
///
/// Issue #960: this replaces the previous `Option<Vec<u8>>` so the caller can
/// record the *honest* reason a full scan was chosen, rather than collapsing all
/// fallback causes into `None`.
#[derive(Debug)]
pub(super) enum PartitionLookupOutcome {
    /// A fully-constrained partition key; carries its on-disk bytes for the lookup.
    Targeted(Vec<u8>),
    /// Several fully-constrained partition keys (`WHERE pk IN (a, b, c)`) over the
    /// complete partition key (Issue #955). Carries the *deduplicated* on-disk key
    /// bytes, in the order they should be probed (input order, first occurrence
    /// wins). Each is served by an independent partition-targeted lookup.
    MultiTargeted(Vec<Vec<u8>>),
    /// No targeted lookup is possible; carries the documented reason for the scan.
    Fallback(FallbackReason),
}

/// Resolve the HONEST access path for a partition-targeted storage call (Epic
/// #951).
///
/// The executor decides a query *could* use a targeted lookup (`targeted` is the
/// label it would report — `PartitionLookup`, `MultiPartitionLookup`,
/// `MetadataPartitionLookup`, or `StreamingPartitionLookup`). But the storage
/// call reports, via `engaged`, whether it *actually* pruned the SSTable set. On
/// the `tombstones` build the targeted surfaces compile out the prune and become
/// full-scan + retain fallbacks (`engaged == false`); claiming a targeted label
/// there would dishonestly report a targeted path for a query that opened the
/// whole table. When `engaged` is false this returns
/// `FallbackFullScan { TombstonesBuildNoPrune }`. The returned ROWS are identical
/// either way — this only governs the *reported* access path.
pub(super) fn honest_targeted_path(targeted: AccessPath, engaged: bool) -> AccessPath {
    if engaged {
        targeted
    } else {
        AccessPath::FallbackFullScan {
            reason: FallbackReason::TombstonesBuildNoPrune,
        }
    }
}

/// Maximum number of `IN` partition keys served by independent targeted lookups
/// before falling back to a full scan (Issue #955).
///
/// An `IN` list expands to one `scan_partition` per key. Each lookup prunes the
/// SSTable set, but a pathologically large list (thousands of keys) would issue
/// thousands of lookups and could touch every SSTable anyway, defeating the
/// prune and risking unbounded work. Past this cap we choose a single full scan
/// with an in-memory `IN` filter instead: one pass over the data rather than `N`
/// pruned passes, and the per-row `IN` predicate still yields correct rows. The
/// value is deliberately generous (real point-lookup `IN` lists are small) while
/// bounding worst-case fan-out. Reported honestly as a fallback so the cap being
/// hit is observable.
pub(super) const MAX_IN_TARGETED_LOOKUPS: usize = 64;

/// Classify whether the pushed-down predicates fully constrain the partition key
/// (Issue #949, extended for `IN` in Issue #955), returning the on-disk key
/// bytes (one for `=`, several for `IN`) when they do, or a documented
/// [`FallbackReason`] when they do not (Issue #960).
///
/// Returns a fallback — and the caller falls back to a full table scan — when:
/// - no schema is available ([`FallbackReason::NoSchema`]): we cannot identify
///   the partition-key columns,
/// - any partition-key column is missing an `=`/`IN` predicate
///   ([`FallbackReason::PartitionKeyNotFullyConstrained`]): partial key or a
///   range restriction (those still require the scan path today),
/// - the constrained values cannot be encoded to the on-disk key form
///   ([`FallbackReason::PartitionKeyEncodingFailed`], e.g. a type mismatch), or
/// - the expanded `IN` key set exceeds [`MAX_IN_TARGETED_LOOKUPS`]
///   ([`FallbackReason::PartitionKeyNotFullyConstrained`]): a single full scan +
///   in-memory `IN` filter is preferred over a huge targeted fan-out.
///
/// Each partition-key column must be constrained by `=` (a singleton value set)
/// or `IN` (its value list); the targeted key set is the cartesian product of
/// the per-column value sets — exactly the set Cassandra would read as the union
/// of the equivalent single-key queries. A single combination yields
/// [`PartitionLookupOutcome::Targeted`]; multiple yield
/// [`PartitionLookupOutcome::MultiTargeted`] (deduplicated, input order
/// preserved). Token predicates never qualify here.
pub(super) fn classify_partition_lookup(
    predicates: &[SSTablePredicate],
    schema: Option<&crate::schema::TableSchema>,
) -> PartitionLookupOutcome {
    use super::super::select_optimizer::SSTableFilterOp;

    let Some(schema) = schema else {
        return PartitionLookupOutcome::Fallback(FallbackReason::NoSchema);
    };
    if schema.partition_keys.is_empty() {
        return PartitionLookupOutcome::Fallback(FallbackReason::NoSchema);
    }

    // Per partition-key column, collect its constrained value set: `=` is a
    // singleton, `IN` is the list. Token predicates (`is_token`) are skipped —
    // they never name a real partition-key column.
    let mut per_column_values: Vec<Vec<Value>> = Vec::with_capacity(schema.partition_keys.len());
    for pk in &schema.partition_keys {
        let predicate = predicates.iter().find(|p| {
            !p.is_token()
                && p.column == pk.name
                && matches!(p.operation, SSTableFilterOp::Equal | SSTableFilterOp::In)
        });
        let Some(predicate) = predicate else {
            return PartitionLookupOutcome::Fallback(
                FallbackReason::PartitionKeyNotFullyConstrained,
            );
        };
        if predicate.values.is_empty() {
            return PartitionLookupOutcome::Fallback(
                FallbackReason::PartitionKeyNotFullyConstrained,
            );
        }
        per_column_values.push(predicate.values.clone());
    }

    // FINDING 3: bound the fan-out with CHECKED arithmetic BEFORE materializing
    // the product. A composite `IN` over several multi-value columns can have a
    // product that is astronomically large (e.g. 1000 x 1000 x ...); expanding
    // it first would allocate far more than the cap before we ever check it.
    // `checked_mul` over the per-column counts saturates to "too big" on
    // overflow, so we fall back without over-allocating.
    let product_size = per_column_values
        .iter()
        .try_fold(1usize, |acc, vals| acc.checked_mul(vals.len()));
    match product_size {
        Some(n) if n <= MAX_IN_TARGETED_LOOKUPS => {}
        // Over the cap (or overflowed `usize`): a single full scan + in-memory
        // `IN` filter is preferred over a huge targeted fan-out.
        _ => {
            return PartitionLookupOutcome::Fallback(
                FallbackReason::PartitionKeyNotFullyConstrained,
            );
        }
    }

    // Cartesian product of the per-column value sets = the full set of complete
    // partition keys to probe. With all-`=` columns this is a single tuple. The
    // product size is bounded by `MAX_IN_TARGETED_LOOKUPS` above, so this
    // allocation is safe.
    let combinations = cartesian_product(&per_column_values);

    // Encode each combination to on-disk key bytes, deduplicating (first
    // occurrence wins so input order is preserved). An encoding failure for any
    // combination makes the whole lookup unsafe → full-scan fallback.
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(combinations.len());
    for values in &combinations {
        match crate::storage::partition_key_codec::encode_partition_key_columns(values, schema) {
            Ok(bytes) => {
                if seen.insert(bytes.clone()) {
                    keys.push(bytes);
                }
            }
            Err(_) => {
                return PartitionLookupOutcome::Fallback(
                    FallbackReason::PartitionKeyEncodingFailed,
                );
            }
        }
    }

    match keys.len() {
        // An empty `IN` list cannot reach here (the parser drops empty `IN`,
        // and an empty value set was rejected above), but guard defensively.
        0 => PartitionLookupOutcome::Fallback(FallbackReason::PartitionKeyNotFullyConstrained),
        1 => PartitionLookupOutcome::Targeted(keys.into_iter().next().unwrap_or_default()),
        _ => PartitionLookupOutcome::MultiTargeted(keys),
    }
}

/// Coerce a clustering bound `Value` to the clustering column's declared CQL type
/// so it encodes to the SAME byte-comparable separator form the `Rows.db` row
/// index stores (Issue #954).
///
/// The optimizer widens an integer literal (`100`) to `Value::Integer`/`BigInt`
/// without regard to the column's narrower type, so an `int` clustering column's
/// `100` can arrive as `BigInt(100)` and encode to 8 bytes while the on-disk
/// separator is the 4-byte `int` form — a width mismatch that selects no block.
/// This narrows/normalises the common comparable types to the column type. A type
/// that cannot be safely coerced returns `None` (the caller then decodes the
/// whole partition — correctness preserved, just no narrowing).
#[cfg(not(feature = "tombstones"))]
fn coerce_clustering_value(value: &Value, cql_type: &str) -> Option<Value> {
    use crate::schema::CqlType;

    let ty = CqlType::parse(cql_type).ok()?;
    // Extract an integer from any integer-ish Value for the integer target types.
    let as_i128 = |v: &Value| -> Option<i128> {
        match v {
            Value::TinyInt(i) => Some(*i as i128),
            Value::SmallInt(i) => Some(*i as i128),
            Value::Integer(i) => Some(*i as i128),
            Value::BigInt(i) | Value::Counter(i) | Value::Timestamp(i) => Some(*i as i128),
            _ => None,
        }
    };
    match ty {
        CqlType::TinyInt => Some(Value::TinyInt(i8::try_from(as_i128(value)?).ok()?)),
        CqlType::SmallInt => Some(Value::SmallInt(i16::try_from(as_i128(value)?).ok()?)),
        CqlType::Int => Some(Value::Integer(i32::try_from(as_i128(value)?).ok()?)),
        CqlType::BigInt | CqlType::Counter => {
            Some(Value::BigInt(i64::try_from(as_i128(value)?).ok()?))
        }
        CqlType::Timestamp => Some(Value::Timestamp(i64::try_from(as_i128(value)?).ok()?)),
        // Already-correct comparable types pass through unchanged.
        CqlType::Text | CqlType::Varchar | CqlType::Ascii => match value {
            Value::Text(_) => Some(value.clone()),
            _ => None,
        },
        CqlType::Uuid | CqlType::TimeUuid => match value {
            Value::Uuid(_) => Some(value.clone()),
            _ => None,
        },
        CqlType::Boolean => match value {
            Value::Boolean(_) => Some(value.clone()),
            _ => None,
        },
        CqlType::Blob => match value {
            Value::Blob(_) => Some(value.clone()),
            _ => None,
        },
        // Any other clustering type is out of the encodable scope for now.
        _ => None,
    }
}

/// Classify whether the pushed-down predicates carry a SINGLE-COLUMN clustering
/// restriction on the FIRST clustering column that can be pushed down to a
/// within-partition seek (Issue #954, Epic #951).
///
/// Returns `Some(slice)` for `ck </>/=/<=/>= ?`, a two-bound contiguous range
/// (`ck >= a AND ck < b`, lowered by the optimizer to a separate `Gte`/`Gt` and
/// `Lt`/`Lte` pair), a `BETWEEN`-lowered `Range`, or `ck = ?` (`Equal`) on the
/// FIRST clustering column — the shapes `evaluate_leaf` already enforces. The
/// lower and upper bounds of a two-bound query arrive as TWO predicates on the
/// same column (Issue #788 lowers `>=`/`<` independently, NOT as a `Range`), so
/// this MERGES all bounds on the first clustering column into one slice — without
/// the merge a `ck >= a AND ck < b` would pick only one bound and decode far more
/// than the slice.
///
/// Returns `None` (decode the whole partition, report `PartitionLookup`) when:
/// - there is no schema or no clustering key,
/// - no predicate names the first clustering column,
/// - the restriction is a shape outside the single-column scope (`In`, `Prefix`,
///   or any restriction on a NON-first clustering column — multi-column prefixes
///   are a documented follow-up), or
/// - a bound value is missing.
///
/// This NEVER changes correctness: the slice only narrows the seek's decode, and
/// the caller's post-scan `evaluate_leaf` applies the exact predicate. A `None`
/// result simply decodes the full partition.
#[cfg(not(feature = "tombstones"))]
pub(super) fn classify_clustering_slice(
    predicates: &[SSTablePredicate],
    schema: Option<&crate::schema::TableSchema>,
) -> Option<crate::storage::sstable::reader::ClusteringSlice> {
    use super::super::select_optimizer::SSTableFilterOp;
    use crate::storage::sstable::reader::ClusteringSlice;

    let schema = schema?;
    let first_ck = schema.clustering_keys.first()?;
    let ck_type = first_ck.data_type.as_str();
    let coerce = |v: &Value| coerce_clustering_value(v, ck_type);

    // Any restriction on a NON-first clustering column is a multi-column prefix —
    // out of single-column scope (#954). Bail so the whole partition is decoded.
    let non_first_ck_restricted = schema.clustering_keys.iter().skip(1).any(|ck| {
        predicates
            .iter()
            .any(|p| !p.is_token() && p.column == ck.name)
    });
    if non_first_ck_restricted {
        return None;
    }

    // Collect every restriction on the FIRST clustering column and fold them into
    // a single (start, end) slice.
    let mut start: Vec<Value> = Vec::new();
    let mut start_inclusive = false;
    let mut end: Vec<Value> = Vec::new();
    let mut end_inclusive = false;
    let mut saw_supported = false;

    for p in predicates
        .iter()
        .filter(|p| !p.is_token() && p.column == first_ck.name)
    {
        match &p.operation {
            SSTableFilterOp::Equal => {
                let v = coerce(p.values.first()?)?;
                start = vec![v.clone()];
                start_inclusive = true;
                end = vec![v];
                end_inclusive = true;
                saw_supported = true;
            }
            SSTableFilterOp::Gt => {
                start = vec![coerce(p.values.first()?)?];
                start_inclusive = false;
                saw_supported = true;
            }
            SSTableFilterOp::Gte => {
                start = vec![coerce(p.values.first()?)?];
                start_inclusive = true;
                saw_supported = true;
            }
            SSTableFilterOp::Lt => {
                end = vec![coerce(p.values.first()?)?];
                end_inclusive = false;
                saw_supported = true;
            }
            SSTableFilterOp::Lte => {
                end = vec![coerce(p.values.first()?)?];
                end_inclusive = true;
                saw_supported = true;
            }
            SSTableFilterOp::Range => {
                if p.values.len() < 2 {
                    return None;
                }
                start = vec![coerce(&p.values[0])?];
                start_inclusive = true;
                end = vec![coerce(&p.values[1])?];
                end_inclusive = true;
                saw_supported = true;
            }
            // `In`/`Prefix`/`BloomFilter` on the clustering column are out of
            // single-column-slice scope; decode the whole partition.
            SSTableFilterOp::In | SSTableFilterOp::Prefix | SSTableFilterOp::BloomFilter => {
                return None;
            }
        }
    }

    if !saw_supported {
        return None;
    }
    Some(ClusteringSlice {
        start,
        start_inclusive,
        end,
        end_inclusive,
    })
}

/// Stable-sort scan results by their partition token, then by raw key bytes —
/// the on-disk storage order, so the union of several `scan_partition` lookups
/// (`WHERE pk IN (...)`, Issue #955) equals a full scan filtered to the same
/// keys. Uses the same `Murmur3Partitioner` token the rest of the codebase uses.
/// Stability preserves each partition's clustering order, since one partition's
/// rows arrive contiguously from a single `scan_partition` call.
pub(super) fn sort_rows_by_token(rows: &mut [(RowKey, ScanRow)]) {
    rows.sort_by(|a, b| {
        let ta = crate::util::cassandra_murmur3::cassandra_murmur3_token(&a.0 .0);
        let tb = crate::util::cassandra_murmur3::cassandra_murmur3_token(&b.0 .0);
        ta.cmp(&tb).then_with(|| a.0 .0.cmp(&b.0 .0))
    });
}

/// True when `ORDER BY` is a single item on the FIRST clustering column whose
/// requested direction is the REVERSE of that column's stored clustering order
/// (Issue #1184) — i.e. a true reverse partition traversal is needed. A query that
/// asks for the stored order (or orders by a non-clustering / multi-column key) is
/// not a reverse scan and keeps the normal path.
#[cfg(not(feature = "tombstones"))]
pub(super) fn requests_clustering_reverse(
    order_by: &crate::query::select_ast::OrderByClause,
    schema: &crate::schema::TableSchema,
) -> bool {
    use crate::query::select_ast::{SelectExpression, SortDirection};
    use crate::schema::ClusteringOrder;

    if order_by.items.len() != 1 {
        return false;
    }
    let item = &order_by.items[0];
    let SelectExpression::Column(col_ref) = &item.expression else {
        return false;
    };
    let Some(first_ck) = schema.clustering_keys.first() else {
        return false;
    };
    if col_ref.column != first_ck.name {
        return false;
    }
    let stored_desc = matches!(first_ck.order, ClusteringOrder::Desc);
    let requested_desc = matches!(item.direction, SortDirection::Descending);
    requested_desc != stored_desc
}

#[cfg(not(feature = "tombstones"))]
impl super::SelectExecutor {
    /// Serve a fully-constrained `WHERE pk = ?` (optionally with a single-column
    /// clustering restriction and/or `ORDER BY <ck>`) from a partition-targeted
    /// read, recording the honest access path (Issue #954 / #960 / #1184). The
    /// returned raw `(RowKey, ScanRow)` rows flow through the SAME post-scan row-build
    /// + predicate backstop the caller applies, so output is byte-identical.
    ///
    /// `ORDER BY <ck>` whose direction is the REVERSE of the stored clustering order
    /// on a BIG wide partition is served by the reverse promoted-index iterator
    /// (block walk back-to-front), marking `context.reverse_served` so the executor
    /// skips the in-memory `Sort`. Every other case takes the forward seek (which
    /// narrows via the promoted index when a clustering slice is present) and keeps
    /// the in-memory sort as the ordering fallback.
    pub(super) async fn targeted_partition_rows(
        &self,
        table: &crate::types::TableId,
        pk_bytes: &[u8],
        predicates: &[SSTablePredicate],
        order_by: Option<&crate::query::select_ast::OrderByClause>,
        schema: Option<&crate::schema::TableSchema>,
        context: &mut super::ExecutionContext,
    ) -> crate::Result<Vec<(RowKey, ScanRow)>> {
        let clustering = classify_clustering_slice(predicates, schema);
        // Reverse path: ORDER BY <first ck> opposite to the stored clustering order.
        if let (Some(order_by), Some(schema)) = (order_by, schema) {
            if requests_clustering_reverse(order_by, schema) {
                if let Some(rows) = self
                    .storage
                    .scan_partition_clustering_reverse(table, pk_bytes, Some(schema))
                    .await?
                {
                    // HONEST access path (Finding 1, roborev #1184): the reverse
                    // iterator (`big_reverse_partition_rows`) walks EVERY promoted-index
                    // block back-to-front — it is NOT narrowed by the clustering slice
                    // (it is only passed `(table, pk_bytes, schema)`). So even when a
                    // clustering predicate is present this is a full-partition read, and
                    // reporting `ClusteringSlice` would dishonestly claim a pruned path.
                    // Record `PartitionLookup`; correctness for a bounded reverse query
                    // (`WHERE ck<N ORDER BY ck DESC`) comes from the post-scan predicate
                    // backstop the caller applies to every returned row.
                    let path = AccessPath::PartitionLookup;
                    context.access_path = Some(path.clone());
                    crate::query::access_path::record(path);
                    context.reverse_served = true;
                    return Ok(rows);
                }
            }
        }
        // Forward seek (clustering-narrowed when a slice is present).
        let (rows, engaged) = self
            .storage
            .scan_partition_clustering(table, pk_bytes, clustering.as_ref(), schema)
            .await?;
        let path = if engaged {
            AccessPath::ClusteringSlice
        } else {
            AccessPath::PartitionLookup
        };
        context.access_path = Some(path.clone());
        crate::query::access_path::record(path);
        Ok(rows)
    }
}

/// Cartesian product of per-column value sets, preserving column order and the
/// input order within each column. Empty input yields a single empty tuple.
fn cartesian_product(per_column: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let mut out: Vec<Vec<Value>> = vec![Vec::new()];
    for column_values in per_column {
        let mut next = Vec::with_capacity(out.len() * column_values.len());
        for prefix in &out {
            for value in column_values {
                let mut combo = prefix.clone();
                combo.push(value.clone());
                next.push(combo);
            }
        }
        out = next;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{composite_pk_schema, single_pk_schema};
    use super::*;

    /// Epic #951 (honest access paths): the executor's per-branch record decision
    /// must report the TARGETED label only when the storage call actually engaged
    /// a pruned path (`engaged == true`); when the prune is compiled out on the
    /// `tombstones` build the call returns `engaged == false` and the reported
    /// path MUST be the honest `FallbackFullScan { TombstonesBuildNoPrune }`. This
    /// pins the record-decision in EVERY build (including `tombstones`, where the
    /// integration tests that assert targeted labels are cfg'd out).
    #[test]
    fn honest_targeted_path_reports_fallback_when_not_engaged() {
        // Engaged: the targeted label is reported as-is, for each targeted surface.
        for targeted in [
            AccessPath::PartitionLookup,
            AccessPath::MultiPartitionLookup,
            AccessPath::MetadataPartitionLookup,
            AccessPath::StreamingPartitionLookup,
        ] {
            let engaged = honest_targeted_path(targeted.clone(), true);
            assert_eq!(engaged, targeted, "engaged must keep the targeted label");
            assert!(engaged.is_targeted());

            // Not engaged (tombstones build / no prune): honest full-scan fallback,
            // regardless of which targeted surface was attempted.
            let fallback = honest_targeted_path(targeted, false);
            assert_eq!(
                fallback,
                AccessPath::FallbackFullScan {
                    reason: FallbackReason::TombstonesBuildNoPrune,
                },
                "a non-engaged targeted call must report the honest no-prune fallback"
            );
            assert!(fallback.is_full_scan());
            assert!(!fallback.is_targeted());
        }
    }

    /// Issue #956: a `WHERE id = <uuid-literal>` against a single UUID partition
    /// key must engage the #949 partition-targeted fast path, i.e.
    /// `classify_partition_lookup` returns `Targeted` with the raw 16-byte key.
    /// This is the unit-level evidence that the parser's new `Value::Uuid`
    /// literal flows all the way into the fast path (the e2e parity test proves
    /// the rows it returns are correct).
    #[test]
    fn classify_partition_lookup_targets_uuid_literal() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let uuid = [
            0x55u8, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let schema = single_pk_schema("id", "uuid");
        let predicate =
            SSTablePredicate::column("id", SSTableFilterOp::Equal, vec![Value::Uuid(uuid)]);

        match classify_partition_lookup(std::slice::from_ref(&predicate), Some(&schema)) {
            PartitionLookupOutcome::Targeted(pk_bytes) => assert_eq!(
                pk_bytes,
                uuid.to_vec(),
                "fast path must encode the UUID literal to the raw 16-byte on-disk key"
            ),
            PartitionLookupOutcome::MultiTargeted(keys) => panic!(
                "Issue #956: a single UUID-literal `=` must be a single Targeted lookup, not \
                 MultiTargeted (got {} keys)",
                keys.len()
            ),
            PartitionLookupOutcome::Fallback(reason) => panic!(
                "Issue #956: UUID-literal `=` predicate must engage the partition fast path, \
                 got fallback {reason:?}"
            ),
        }
    }

    /// A non-equality (or partial) restriction must NOT engage the fast path, so
    /// the executor falls back to a full scan with the documented
    /// `PartitionKeyNotFullyConstrained` reason (Issue #960). Guards against the
    /// UUID change accidentally widening fast-path eligibility.
    #[test]
    fn classify_partition_lookup_falls_back_for_uuid_range_predicate() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let uuid = [1u8; 16];
        let schema = single_pk_schema("id", "uuid");
        let predicate =
            SSTablePredicate::column("id", SSTableFilterOp::Gt, vec![Value::Uuid(uuid)]);

        assert!(
            matches!(
                classify_partition_lookup(std::slice::from_ref(&predicate), Some(&schema)),
                PartitionLookupOutcome::Fallback(FallbackReason::PartitionKeyNotFullyConstrained)
            ),
            "a range restriction on the partition key must report the \
             PartitionKeyNotFullyConstrained fallback, not a targeted lookup",
        );
    }

    /// Issue #955: `WHERE pk IN (a, b, c)` over the complete single-column
    /// partition key classifies as `MultiTargeted` with one encoded key per IN
    /// element, in input order.
    #[test]
    fn classify_partition_lookup_in_yields_multi_targeted() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = single_pk_schema("id", "int");
        let predicate = SSTablePredicate::column(
            "id",
            SSTableFilterOp::In,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );
        match classify_partition_lookup(std::slice::from_ref(&predicate), Some(&schema)) {
            PartitionLookupOutcome::MultiTargeted(keys) => {
                assert_eq!(keys.len(), 3, "one targeted key per IN element");
                // Single int column → raw 4-byte big-endian value (1, 2, 3).
                assert_eq!(keys[0], 1i32.to_be_bytes().to_vec());
                assert_eq!(keys[1], 2i32.to_be_bytes().to_vec());
                assert_eq!(keys[2], 3i32.to_be_bytes().to_vec());
            }
            other => panic!("IN over the complete pk must be MultiTargeted, got {other:?}"),
        }
    }

    /// Issue #955: a single-element `IN` collapses to a single `Targeted` lookup
    /// (not `MultiTargeted`), and duplicate IN elements are deduplicated.
    #[test]
    fn classify_partition_lookup_in_dedupes_and_collapses_singletons() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = single_pk_schema("id", "int");

        // Single element → Targeted.
        let one = SSTablePredicate::column("id", SSTableFilterOp::In, vec![Value::Integer(7)]);
        assert!(
            matches!(
                classify_partition_lookup(std::slice::from_ref(&one), Some(&schema)),
                PartitionLookupOutcome::Targeted(_)
            ),
            "a single-element IN must collapse to a single Targeted lookup",
        );

        // Duplicates collapse: IN (5, 5, 6) → two distinct keys.
        let dup = SSTablePredicate::column(
            "id",
            SSTableFilterOp::In,
            vec![Value::Integer(5), Value::Integer(5), Value::Integer(6)],
        );
        match classify_partition_lookup(std::slice::from_ref(&dup), Some(&schema)) {
            PartitionLookupOutcome::MultiTargeted(keys) => {
                assert_eq!(keys.len(), 2, "duplicate IN elements must be deduplicated");
                assert_eq!(keys[0], 5i32.to_be_bytes().to_vec());
                assert_eq!(keys[1], 6i32.to_be_bytes().to_vec());
            }
            other => panic!("IN (5,5,6) must dedupe to 2 MultiTargeted keys, got {other:?}"),
        }
    }

    /// Issue #955: an `IN` list larger than `MAX_IN_TARGETED_LOOKUPS` falls back
    /// to a single full scan (the per-row IN filter still yields correct rows),
    /// reported honestly as `PartitionKeyNotFullyConstrained`.
    #[test]
    fn classify_partition_lookup_large_in_falls_back() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = single_pk_schema("id", "int");
        let values: Vec<Value> = (0..(MAX_IN_TARGETED_LOOKUPS as i32 + 1))
            .map(Value::Integer)
            .collect();
        let predicate = SSTablePredicate::column("id", SSTableFilterOp::In, values);
        assert!(
            matches!(
                classify_partition_lookup(std::slice::from_ref(&predicate), Some(&schema)),
                PartitionLookupOutcome::Fallback(FallbackReason::PartitionKeyNotFullyConstrained)
            ),
            "an IN list over the cap must fall back to a full scan",
        );

        // Exactly at the cap is still targeted.
        let at_cap: Vec<Value> = (0..(MAX_IN_TARGETED_LOOKUPS as i32))
            .map(Value::Integer)
            .collect();
        let at_cap_pred = SSTablePredicate::column("id", SSTableFilterOp::In, at_cap);
        assert!(
            matches!(
                classify_partition_lookup(std::slice::from_ref(&at_cap_pred), Some(&schema)),
                PartitionLookupOutcome::MultiTargeted(_)
            ),
            "an IN list exactly at the cap must still be MultiTargeted",
        );
    }

    /// FINDING 3: a composite `IN` whose cartesian product EXCEEDS the cap must
    /// fall back BEFORE materializing the product (checked arithmetic), and still
    /// reports the honest `PartitionKeyNotFullyConstrained` fallback. The product
    /// here (1000 x 1000 = 1_000_000) is far over `MAX_IN_TARGETED_LOOKUPS`;
    /// expanding it first would allocate a million combinations.
    #[test]
    fn classify_partition_lookup_composite_in_over_cap_falls_back_without_overalloc() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = composite_pk_schema(("a", "int"), ("b", "int"));
        let a_vals: Vec<Value> = (0..1000).map(Value::Integer).collect();
        let b_vals: Vec<Value> = (0..1000).map(Value::Integer).collect();
        let preds = vec![
            SSTablePredicate::column("a", SSTableFilterOp::In, a_vals),
            SSTablePredicate::column("b", SSTableFilterOp::In, b_vals),
        ];
        assert!(
            matches!(
                classify_partition_lookup(&preds, Some(&schema)),
                PartitionLookupOutcome::Fallback(FallbackReason::PartitionKeyNotFullyConstrained)
            ),
            "a composite IN whose product exceeds the cap must fall back to a full scan",
        );
    }

    /// FINDING 3: a composite `IN` whose product is WITHIN the cap is still
    /// served by a targeted MultiTargeted lookup (the checked-arithmetic guard
    /// must not over-reject). 4 x 4 = 16 <= 64.
    #[test]
    fn classify_partition_lookup_composite_in_within_cap_is_targeted() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = composite_pk_schema(("a", "int"), ("b", "int"));
        let a_vals: Vec<Value> = (0..4).map(Value::Integer).collect();
        let b_vals: Vec<Value> = (0..4).map(Value::Integer).collect();
        let preds = vec![
            SSTablePredicate::column("a", SSTableFilterOp::In, a_vals),
            SSTablePredicate::column("b", SSTableFilterOp::In, b_vals),
        ];
        match classify_partition_lookup(&preds, Some(&schema)) {
            PartitionLookupOutcome::MultiTargeted(keys) => assert_eq!(
                keys.len(),
                16,
                "4 x 4 composite IN must yield 16 targeted keys (the full product)"
            ),
            other => panic!("composite IN within the cap must be MultiTargeted, got {other:?}"),
        }
    }

    /// Issue #955: a `token(pk)` range restriction does NOT engage the targeted
    /// fast path (partitions are token-ordered but we do not yet seek a span);
    /// it reports the honest `PartitionKeyNotFullyConstrained` fallback and the
    /// token predicate is applied per-row (verified by `evaluate_leaf` below).
    #[test]
    fn classify_partition_lookup_token_range_falls_back() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = single_pk_schema("id", "int");
        let predicate = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gte,
            vec![Value::BigInt(-100)],
        );
        assert!(
            matches!(
                classify_partition_lookup(std::slice::from_ref(&predicate), Some(&schema)),
                PartitionLookupOutcome::Fallback(FallbackReason::PartitionKeyNotFullyConstrained)
            ),
            "a token-range restriction must fall back honestly (no fake pruning)",
        );
    }

    /// Issue #960: no schema means we cannot identify the partition-key columns,
    /// so the classifier reports the `NoSchema` fallback reason.
    #[test]
    fn classify_partition_lookup_falls_back_without_schema() {
        assert!(
            matches!(
                classify_partition_lookup(&[], None),
                PartitionLookupOutcome::Fallback(FallbackReason::NoSchema)
            ),
            "no schema must report the NoSchema fallback reason",
        );
    }
}
