//! Schema-less point-lookup classification for the SELECT executor (issue #1750).
//!
//! Split out of `lookup.rs` (campsite rule, epic #1116) so the over-threshold
//! classifier file does not grow. This holds ONLY the structural schema-less
//! point-read recogniser + its single-component key encoder; the schema-aware
//! `classify_partition_lookup` stays in `lookup.rs`.

use super::super::select_optimizer::SSTablePredicate;
use crate::types::Value;

/// Structurally classify a SCHEMA-LESS point read (issue #1750 regression fix).
///
/// When NO schema is available, `classify_partition_lookup` can only report
/// `NoSchema` and fall back to a full scan. But a schema-less full scan CANNOT
/// reconstruct the partition-key column (Cassandra never serialises it in the
/// cell payload, and `build_row_from_scan` needs the schema to decode it from the
/// row key), so a `WHERE pk = <literal>` predicate then rejects EVERY row in the
/// post-scan backstop — the read returns 0 rows. Before #1750 retired the
/// `is_simple_id_lookup` fork, such a read routed to the legacy `QueryExecutor`,
/// which looked the partition up BY KEY BYTES via `storage.get()` and never
/// re-evaluated the predicate, so it returned the row.
///
/// This restores that behaviour WITHOUT any text heuristic: the decision is made
/// purely from the parsed predicate STRUCTURE. It returns the on-disk key bytes
/// for a targeted lookup ONLY when the predicates are EXACTLY a single non-token
/// `column = <literal>` equality — the unambiguous single-component point-read
/// shape a schema-less reader can serve by encoding the literal to raw key bytes
/// (the same single-component encoding `RowKey`/the write engine use). Any other
/// shape (no predicate, a range/`IN`, multiple predicates, a token predicate, or a
/// value that has no single-component key encoding) returns `None` so the caller
/// keeps the honest full-scan path. The targeted read is self-verifying: the
/// storage seek only returns rows whose raw partition key equals these bytes, so
/// a wrong/absent key yields no rows — never another partition's rows.
pub(super) fn classify_schemaless_point_lookup(predicates: &[SSTablePredicate]) -> Option<Vec<u8>> {
    use super::super::select_optimizer::SSTableFilterOp;

    // Exactly one predicate, a non-token single-value equality.
    let [predicate] = predicates else {
        return None;
    };
    if predicate.is_token() || !matches!(predicate.operation, SSTableFilterOp::Equal) {
        return None;
    }
    let [value] = predicate.values.as_slice() else {
        return None;
    };
    encode_single_component_key(value)
}

/// Encode a single `Value` to the raw single-component on-disk partition-key
/// bytes — the schema-less counterpart of the write engine's
/// `PartitionKey::to_bytes` single-component form (raw value bytes, NO framing),
/// used by [`classify_schemaless_point_lookup`].
///
/// Only the value kinds with an UNAMBIGUOUS single-component key encoding are
/// accepted; anything else (collections, blobs, etc.) returns `None` so the
/// caller falls back to a full scan rather than guessing a byte layout.
fn encode_single_component_key(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Uuid(bytes) => Some(bytes.to_vec()),
        Value::Integer(i) => Some(i.to_be_bytes().to_vec()),
        Value::BigInt(i) => Some(i.to_be_bytes().to_vec()),
        Value::Text(s) => Some(s.as_bytes().to_vec()),
        Value::Boolean(b) => Some(vec![u8::from(*b)]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};
    use super::*;

    /// Issue #1750 (regression fix): the schema-less point-lookup classifier
    /// recognises EXACTLY the single non-token `col = <single-component literal>`
    /// shape and returns its raw on-disk key bytes, so a schema-less `WHERE pk =
    /// <uuid>` can be served by a key-byte-targeted seek (never returning 0 rows
    /// because the schema-less scan can't reconstruct the pk column). The decision
    /// is structural — never a text guess.
    #[test]
    fn classify_schemaless_point_lookup_targets_single_equality() {
        let uuid = [7u8; 16];
        let predicate =
            SSTablePredicate::column("id", SSTableFilterOp::Equal, vec![Value::Uuid(uuid)]);
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&predicate)),
            Some(uuid.to_vec()),
            "a single UUID `=` must yield the raw 16-byte single-component key",
        );

        // int / text single-component encodings.
        let int_pred =
            SSTablePredicate::column("id", SSTableFilterOp::Equal, vec![Value::Integer(42)]);
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&int_pred)),
            Some(42i32.to_be_bytes().to_vec()),
        );
        let text_pred = SSTablePredicate::column(
            "id",
            SSTableFilterOp::Equal,
            vec![Value::Text("k0".to_string())],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&text_pred)),
            Some(b"k0".to_vec()),
        );
    }

    /// Issue #1750: the schema-less classifier must NOT fire on any non-point
    /// shape — no predicate, a range, an `IN`, multiple predicates, a token
    /// predicate, or an unencodable value — so those keep the honest full-scan
    /// path (never a wrong targeted read).
    #[test]
    fn classify_schemaless_point_lookup_rejects_non_point_shapes() {
        // No predicate (bare SELECT *).
        assert_eq!(classify_schemaless_point_lookup(&[]), None);

        // A range restriction.
        let range = SSTablePredicate::column("id", SSTableFilterOp::Gt, vec![Value::Integer(1)]);
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&range)),
            None
        );

        // An `IN` (multi-value) — not a single point key here.
        let in_pred = SSTablePredicate::column(
            "id",
            SSTableFilterOp::In,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&in_pred)),
            None
        );

        // Two predicates (pk + clustering) — ambiguous without a schema.
        let a = SSTablePredicate::column("pk", SSTableFilterOp::Equal, vec![Value::Integer(1)]);
        let b = SSTablePredicate::column("ck", SSTableFilterOp::Equal, vec![Value::Integer(2)]);
        assert_eq!(classify_schemaless_point_lookup(&[a, b]), None);

        // A token predicate is never a real partition-key column.
        let tok = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Equal,
            vec![Value::BigInt(5)],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&tok)),
            None
        );

        // A value kind with no unambiguous single-component key encoding.
        let blob = SSTablePredicate::column(
            "id",
            SSTableFilterOp::Equal,
            vec![Value::Blob(vec![1, 2, 3])],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&blob)),
            None
        );
    }
}
