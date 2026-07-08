//! Schema-less point-lookup classification for the SELECT executor (issue #1750).
//!
//! Split out of `lookup.rs` (campsite rule, epic #1116) so the over-threshold
//! classifier file does not grow. This holds ONLY the structural schema-less
//! point-read recogniser + its single-component key encoder; the schema-aware
//! `classify_partition_lookup` stays in `lookup.rs`.

use super::super::select_optimizer::SSTablePredicate;
use crate::storage::sstable::PartitionKeyShape;
use crate::types::Value;

/// Classify a SCHEMA-LESS point read against AUTHORITATIVE partition-key metadata
/// (issue #1750 regression fix, then re-scoped to stop over-firing).
///
/// When NO schema is available, `classify_partition_lookup` can only report
/// `NoSchema` and fall back to a full scan. For a `WHERE pk = <literal>` that scan
/// CANNOT reconstruct the partition-key column (Cassandra never serialises the pk
/// column value in the cell payload, and `build_row_from_scan` needs the schema to
/// decode it from the row key), so the post-scan predicate backstop rejects EVERY
/// row and the read returns 0 rows. Serving such a read by a key-byte-targeted seek
/// (which never re-evaluates the predicate) fixes that — but ONLY when the equality
/// column really is the partition key. A `WHERE <regular_col> = <literal>` seeks a
/// nonexistent partition and returns 0 rows, whereas a full scan correctly matches
/// the regular-column cell (regular cells ARE decodable schema-less). So the
/// targeted seek must fire ONLY for a metadata-CONFIRMED partition-key equality.
///
/// The confirmation is by ELIMINATION from authoritative metadata (the Statistics.db
/// SerializationHeader, [`PartitionKeyShape`]) — NEVER a text/name guess (#28).
/// Cassandra does not serialise the pk column NAME, but the header authoritatively
/// gives the pk-component count, the clustering-key count, and the REAL names of the
/// non-key columns. The seek fires ONLY when:
///   * the predicates are EXACTLY one non-token single-value `column = <literal>`,
///   * the value has an unambiguous single-component key encoding,
///   * the table has exactly ONE partition-key component and ZERO clustering keys
///     (a single-component point key), AND
///   * the predicate column is ABSENT from the authoritative non-key column names —
///     so it can ONLY be the sole partition key.
///
/// Any other shape — or a `shape` we could not resolve (`None`) — returns `None`, so
/// the caller keeps the honest full-scan path (correct for regular-column equalities
/// and safe when metadata is unavailable). The targeted read is self-verifying: the
/// storage seek only returns rows whose raw partition key equals these bytes.
pub(super) fn classify_schemaless_point_lookup(
    predicates: &[SSTablePredicate],
    shape: Option<&PartitionKeyShape>,
) -> Option<Vec<u8>> {
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

    // Metadata-confirm the predicate column is the SOLE partition key by
    // elimination — a single-component pk, no clustering keys, and the column is
    // NOT one of the authoritative non-key column names. Without a resolvable
    // shape we cannot confirm, so we do NOT seek (full-scan stays correct).
    let shape = shape?;
    let is_confirmed_sole_pk = shape.partition_key_count == 1
        && shape.clustering_key_count == 0
        && !shape.non_key_column_names.contains(&predicate.column);
    if !is_confirmed_sole_pk {
        return None;
    }

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

    /// A single-component-pk, no-clustering table whose only non-key column is
    /// `name` — mirrors `test_basic.simple_table` (`id UUID` pk, `name TEXT`).
    fn simple_table_shape() -> PartitionKeyShape {
        PartitionKeyShape {
            partition_key_count: 1,
            clustering_key_count: 0,
            non_key_column_names: ["name".to_string(), "age".to_string()]
                .into_iter()
                .collect(),
        }
    }

    /// Issue #1750 (regression fix): a schema-less `col = <literal>` whose column is
    /// a metadata-CONFIRMED sole partition key (single pk component, no clustering
    /// keys, column absent from the authoritative non-key names) yields its raw
    /// single-component key bytes for a targeted seek. The confirmation is by
    /// elimination from authoritative metadata — never a pk-name/text guess.
    #[test]
    fn classify_schemaless_point_lookup_targets_confirmed_pk_equality() {
        let shape = simple_table_shape();
        let uuid = [7u8; 16];
        let predicate =
            SSTablePredicate::column("id", SSTableFilterOp::Equal, vec![Value::Uuid(uuid)]);
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&predicate), Some(&shape)),
            Some(uuid.to_vec()),
            "a single UUID `=` on the confirmed pk must yield the raw 16-byte key",
        );

        // int / text single-component encodings (pk column named `pk`, absent from
        // the non-key names, so confirmed by elimination).
        let pk_shape = PartitionKeyShape {
            partition_key_count: 1,
            clustering_key_count: 0,
            non_key_column_names: ["v".to_string()].into_iter().collect(),
        };
        let int_pred =
            SSTablePredicate::column("pk", SSTableFilterOp::Equal, vec![Value::Integer(42)]);
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&int_pred), Some(&pk_shape)),
            Some(42i32.to_be_bytes().to_vec()),
        );
        let text_pred = SSTablePredicate::column(
            "pk",
            SSTableFilterOp::Equal,
            vec![Value::Text("k0".to_string())],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&text_pred), Some(&pk_shape)),
            Some(b"k0".to_vec()),
        );
    }

    /// Issue #1750 (the confirmed regression): a schema-less `WHERE <non_pk_col> =
    /// <literal>` must NOT take the by-key seek — the column is one of the
    /// authoritative non-key names, so it can never be the partition key. Returning
    /// `None` keeps the honest full-scan path, which correctly matches the
    /// regular-column cell (the by-key seek would seek a nonexistent partition and
    /// return 0 rows — the 1→0 over-firing this fix removes).
    #[test]
    fn classify_schemaless_point_lookup_rejects_non_pk_column_equality() {
        let shape = simple_table_shape();
        let name_eq = SSTablePredicate::column(
            "name",
            SSTableFilterOp::Equal,
            vec![Value::Text("Mr. James Hoffman".to_string())],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&name_eq), Some(&shape)),
            None,
            "a regular-column equality must NOT take the pk-key seek (would return 0 rows); \
             it must full-scan and match the cell",
        );
    }

    /// The seek must NOT fire when the shape shows a composite pk or any clustering
    /// key — a single raw value can't be the whole point key there — nor when the
    /// authoritative shape is unavailable (`None`): both keep the full-scan path.
    #[test]
    fn classify_schemaless_point_lookup_requires_single_component_point_shape() {
        let value = vec![Value::Uuid([1u8; 16])];
        let pred = SSTablePredicate::column("id", SSTableFilterOp::Equal, value);

        // Composite partition key.
        let composite = PartitionKeyShape {
            partition_key_count: 2,
            clustering_key_count: 0,
            non_key_column_names: Default::default(),
        };
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&pred), Some(&composite)),
            None,
        );

        // Has clustering keys (the equality alone doesn't pin a full point key).
        let clustered = PartitionKeyShape {
            partition_key_count: 1,
            clustering_key_count: 1,
            non_key_column_names: Default::default(),
        };
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&pred), Some(&clustered)),
            None,
        );

        // No authoritative shape → cannot confirm → full-scan.
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&pred), None),
            None,
        );
    }

    /// Issue #1750: the schema-less classifier must NOT fire on any non-point
    /// shape — no predicate, a range, an `IN`, multiple predicates, a token
    /// predicate, or an unencodable value — so those keep the honest full-scan
    /// path (never a wrong targeted read), even on a confirmed single-pk table.
    #[test]
    fn classify_schemaless_point_lookup_rejects_non_point_shapes() {
        let shape = simple_table_shape();
        let s = Some(&shape);

        // No predicate (bare SELECT *).
        assert_eq!(classify_schemaless_point_lookup(&[], s), None);

        // A range restriction.
        let range = SSTablePredicate::column("id", SSTableFilterOp::Gt, vec![Value::Integer(1)]);
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&range), s),
            None
        );

        // An `IN` (multi-value) — not a single point key here.
        let in_pred = SSTablePredicate::column(
            "id",
            SSTableFilterOp::In,
            vec![Value::Integer(1), Value::Integer(2)],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&in_pred), s),
            None
        );

        // Two predicates (pk + clustering) — ambiguous without a schema.
        let a = SSTablePredicate::column("pk", SSTableFilterOp::Equal, vec![Value::Integer(1)]);
        let b = SSTablePredicate::column("ck", SSTableFilterOp::Equal, vec![Value::Integer(2)]);
        assert_eq!(classify_schemaless_point_lookup(&[a, b], s), None);

        // A token predicate is never a real partition-key column.
        let tok = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Equal,
            vec![Value::BigInt(5)],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&tok), s),
            None
        );

        // A value kind with no unambiguous single-component key encoding.
        let blob = SSTablePredicate::column(
            "id",
            SSTableFilterOp::Equal,
            vec![Value::Blob(vec![1, 2, 3])],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&blob), s),
            None
        );
    }
}
