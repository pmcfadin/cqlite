//! Schema-less point-lookup classification for the SELECT executor (issue #1750).
//!
//! Split out of `lookup.rs` (campsite rule, epic #1116) so the over-threshold
//! classifier file does not grow. This holds ONLY the structural schema-less
//! point-read recogniser + its single-component key encoder; the schema-aware
//! `classify_partition_lookup` stays in `lookup.rs`.
//!
//! # Known schemaless-seek limitations (tracked in #TBD)
//!
//! These are ACCEPTED limitations of the schema-less single-component seek path.
//! Each is fail-SAFE (it yields the correct 0 rows or declines to a full scan, never
//! a wrong non-empty answer) and none is fixed here — they are documented so the
//! follow-up issue and the next reader are grounded:
//!
//! 1. **WRITETIME()/TTL() projection + schemaless pk read → 0 rows.** When a
//!    `WHERE pk = <lit>` also projects cell metadata (WRITETIME/TTL), the seek path
//!    is NOT taken (`execute.rs` gates it on `!include_cell_metadata`) and the
//!    metadata full-scan cannot reconstruct the partition-key column schema-less, so
//!    the post-scan predicate backstop rejects every row → 0 rows
//!    (`execute.rs:465`).
//! 2. **Post-seek guard false-negative for a TIMESTAMP pk (and any seek-firing type
//!    outside the numeric `values_equal`/`as_f64` set) with a numeric literal.** The
//!    seek finds the partition, but the post-seek guard's
//!    `values_equal(Timestamp, BigInt)` is `false`, so the valid row is rejected →
//!    0 rows. The seek still self-verifies on the raw key bytes, so this is a
//!    false-negative (0 rows), never a wrong row.
//! 3. **`coerce_value_for_comparator` has no Date/Varint/Decimal arm for numeric
//!    literals — by design.** Those types decline via a serialize error rather than
//!    risk a wrong-width key, so a `WHERE date_pk = <numeric_lit>` full-scans (safe)
//!    instead of taking the typed seek.

use super::super::select_optimizer::SSTablePredicate;
use crate::storage::sstable::PartitionKeyShape;

/// The classified schema-less single-component point-key seek (issue #1750).
///
/// Carries the raw on-disk partition-key `bytes` to seek by AND the authoritative
/// pk component (`name` + `cql_type`) needed to (a) reconstruct the seeked row's
/// partition-key column schema-less and (b) re-evaluate the predicate against it
/// (the post-seek guard for roborev 3784 FINDING 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SchemalessPointSeek {
    /// Raw single-component partition-key bytes to seek by.
    pub bytes: Vec<u8>,
    /// The header-synthesised pk column name (`id` / `partition_key`).
    pub pk_name: String,
    /// The authoritative CQL type of the pk component.
    pub pk_cql_type: String,
}

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
) -> Option<SchemalessPointSeek> {
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

    // The pk component's authoritative CQL TYPE (from the SerializationHeader
    // `keyType`, never a guess — #28). Without a provable single-component type we
    // DECLINE the seek (full-scan stays correct) rather than encode a
    // possibly-wrong-width key (issue #1750, roborev 3784 FINDING 2).
    let component = shape.single_pk_component.as_ref()?;

    // Encode the literal through the TYPED single-component partition-key codec
    // for that pk type — the SAME path the schema-aware Targeted seek uses — so a
    // parsed integer literal (`Value::BigInt`) encodes to the width Cassandra
    // wrote (`int` → 4 bytes, `bigint` → 8, `uuid` → 16, `text` → utf8). An
    // unencodable literal/type yields `None` → decline the seek.
    let bytes = crate::storage::partition_key_codec::encode_single_component_key_typed(
        value,
        &component.cql_type,
    )
    .ok()?;

    Some(SchemalessPointSeek {
        bytes,
        pk_name: component.name.clone(),
        pk_cql_type: component.cql_type.clone(),
    })
}

/// Build the user-visible `QueryRow` for ONE row returned by a schema-less
/// single-component point seek, reconstructing the partition-key column and
/// applying the post-seek predicate guard (issue #1750, roborev 3784 FINDING 1).
///
/// The seek self-verifies on raw KEY bytes but never re-checks the predicate
/// COLUMN, and the classifier admits the predicate column BY ELIMINATION (absent
/// from the authoritative non-key names) — which also admits a misspelled or
/// nonexistent column. If such a literal happens to encode to a real partition
/// key's bytes, the seek would return that partition's row for a column the user
/// never has. So we:
///   1. reconstruct the sole pk column from the raw key under its authoritative
///      TYPE and header-synthesised NAME (schema-less `build_row_from_scan` omits
///      the pk), and
///   2. re-evaluate the predicate against the materialised row — a predicate on
///      the pk name matches; one on a nonexistent/misspelled column is `Unknown`
///      and rejects the row (yielding the correct 0 rows).
///
/// Returns `None` for a tombstoned/absent row, a pk that cannot be decoded, or a
/// row the predicate rejects. The projection is honoured for output (the pk is
/// materialised for the guard regardless, then dropped from output if the
/// projection excludes it) so this matches the schema-aware path's row shape.
pub(super) fn finalize_schemaless_seek_row(
    key: crate::types::RowKey,
    value: crate::types::ScanRow,
    projection: &[String],
    predicates: &[SSTablePredicate],
    seek: &SchemalessPointSeek,
) -> Option<crate::query::result::QueryRow> {
    use super::predicate::evaluate_predicates;
    use super::row_build::build_row_from_scan;

    // Decode the sole pk value from the raw key under its authoritative type
    // (BEFORE `key` is moved into the row builder). A decode failure means we
    // cannot validate the row, so drop it (honest 0 rather than an unvalidated 1).
    let comparator = crate::types::ComparatorType::from_data_type(&seek.pk_cql_type).ok()?;
    let pk_value =
        crate::storage::partition_key_codec::deserialize_value_bytes(&key.0, &comparator).ok()?;

    // Schema-less row build (schema = None) surfaces the decoded non-key cells but
    // NOT the pk column.
    let mut row = build_row_from_scan(key, value, projection, None)?;

    // Materialise the pk column under its authoritative name for the guard.
    let pk_name: std::sync::Arc<str> = seek.pk_name.as_str().into();
    let already_present = row.values.contains_key(&pk_name);
    if !already_present {
        row.values.insert(pk_name.clone(), pk_value);
    }

    // Post-seek guard: re-evaluate the predicate. A predicate on the pk name
    // matches the reconstructed value; one on a nonexistent/misspelled column is
    // Unknown → row rejected (the FINDING 1 over-firing this removes).
    if !evaluate_predicates(&row, predicates).ok()? {
        return None;
    }

    // Honour projection for OUTPUT: drop the pk again if we injected it only for
    // the guard and the projection does not include it (empty projection = all).
    if !already_present
        && !projection.is_empty()
        && !projection.iter().any(|p| p.as_str() == seek.pk_name)
    {
        row.values.remove(&pk_name);
    }

    Some(row)
}

#[cfg(test)]
mod tests {
    use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};
    use super::*;

    use super::super::super::select_optimizer::SSTablePredicate as Pred;
    use crate::query::result::QueryRow;
    use crate::storage::sstable::PartitionKeyComponent;
    use crate::types::{RowKey, ScanRow, Value};
    use std::sync::Arc;

    /// A single-component-pk, no-clustering table whose only non-key column is
    /// `name`/`age` — mirrors `test_basic.simple_table` (`id UUID` pk).
    fn simple_table_shape() -> PartitionKeyShape {
        PartitionKeyShape {
            partition_key_count: 1,
            clustering_key_count: 0,
            non_key_column_names: ["name".to_string(), "age".to_string()]
                .into_iter()
                .collect(),
            single_pk_component: Some(PartitionKeyComponent {
                name: "id".to_string(),
                cql_type: "uuid".to_string(),
            }),
        }
    }

    /// A single-component `int` pk table (`partition_key` pk, `v` regular) —
    /// mirrors `test_compactionparity.live_no_clustering`.
    fn int_pk_shape() -> PartitionKeyShape {
        PartitionKeyShape {
            partition_key_count: 1,
            clustering_key_count: 0,
            non_key_column_names: ["v".to_string()].into_iter().collect(),
            single_pk_component: Some(PartitionKeyComponent {
                name: "partition_key".to_string(),
                cql_type: "int".to_string(),
            }),
        }
    }

    /// Issue #1750 (regression fix): a schema-less `col = <literal>` whose column is
    /// a metadata-CONFIRMED sole partition key yields the TYPED single-component key
    /// bytes for a targeted seek. Confirmation is by elimination from authoritative
    /// metadata — never a pk-name/text guess.
    #[test]
    fn classify_schemaless_point_lookup_targets_confirmed_pk_equality() {
        let shape = simple_table_shape();
        let uuid = [7u8; 16];
        let predicate =
            SSTablePredicate::column("id", SSTableFilterOp::Equal, vec![Value::Uuid(uuid)]);
        let seek = classify_schemaless_point_lookup(std::slice::from_ref(&predicate), Some(&shape))
            .expect("a single UUID `=` on the confirmed pk must classify");
        assert_eq!(seek.bytes, uuid.to_vec(), "raw 16-byte uuid key");
        assert_eq!(seek.pk_name, "id");
        assert_eq!(seek.pk_cql_type, "uuid");

        // roborev 3784 FINDING 2: a parsed integer literal arrives as `Value::BigInt`
        // (the SELECT parser widens every integer); on an `int` pk it must encode to
        // Cassandra's 4-byte `int` key, NOT an 8-byte one.
        let int_shape = int_pk_shape();
        let int_pred = SSTablePredicate::column(
            "partition_key",
            SSTableFilterOp::Equal,
            vec![Value::BigInt(42)],
        );
        let int_seek =
            classify_schemaless_point_lookup(std::slice::from_ref(&int_pred), Some(&int_shape))
                .expect("int pk `=` must classify");
        assert_eq!(
            int_seek.bytes,
            42i32.to_be_bytes().to_vec(),
            "a BigInt literal on an int pk must encode to the 4-byte int key (roborev 3784)",
        );
        assert_eq!(int_seek.bytes.len(), 4);
    }

    /// A literal that cannot be encoded for the pk's authoritative type (e.g. a text
    /// literal against a `uuid` pk) DECLINES the seek → full-scan, never a
    /// wrong-width key (roborev 3784 FINDING 2, correctness-first).
    #[test]
    fn classify_schemaless_point_lookup_declines_type_mismatch() {
        let shape = simple_table_shape(); // uuid pk
        let text_on_uuid = SSTablePredicate::column(
            "id",
            SSTableFilterOp::Equal,
            vec![Value::Text("not-a-uuid".to_string())],
        );
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&text_on_uuid), Some(&shape)),
            None,
            "a text literal on a uuid pk must decline (no provable key encoding)",
        );
    }

    /// Issue #1750 (the confirmed regression): a schema-less `WHERE <non_pk_col> =
    /// <literal>` must NOT take the by-key seek — the column is one of the
    /// authoritative non-key names, so it can never be the partition key.
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
            "a regular-column equality must NOT take the pk-key seek; it must full-scan",
        );
    }

    /// The seek must NOT fire when the shape shows a composite pk or any clustering
    /// key, when the pk type is absent, nor when the shape is unavailable (`None`).
    #[test]
    fn classify_schemaless_point_lookup_requires_single_component_point_shape() {
        let value = vec![Value::Uuid([1u8; 16])];
        let pred = SSTablePredicate::column("id", SSTableFilterOp::Equal, value);

        // Composite partition key (no single-component type).
        let composite = PartitionKeyShape {
            partition_key_count: 2,
            clustering_key_count: 0,
            non_key_column_names: Default::default(),
            single_pk_component: None,
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
            single_pk_component: Some(PartitionKeyComponent {
                name: "id".to_string(),
                cql_type: "uuid".to_string(),
            }),
        };
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&pred), Some(&clustered)),
            None,
        );

        // Single-component shape but no provable pk type → decline (correctness).
        let no_type = PartitionKeyShape {
            partition_key_count: 1,
            clustering_key_count: 0,
            non_key_column_names: Default::default(),
            single_pk_component: None,
        };
        assert_eq!(
            classify_schemaless_point_lookup(std::slice::from_ref(&pred), Some(&no_type)),
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
    /// predicate, or an unencodable value.
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

    fn scan_row(cells: &[(&str, Value)]) -> ScanRow {
        ScanRow::Row(
            cells
                .iter()
                .map(|(n, v)| (Arc::<str>::from(*n), v.clone()))
                .collect(),
        )
    }

    /// roborev 3784 FINDING 1: the post-seek guard re-evaluates the predicate. A
    /// `WHERE <pk_name> = <value>` matches the reconstructed pk column and returns
    /// the row; a `WHERE <nonexistent_col> = <value>` (admitted by elimination) is
    /// Unknown against the materialised row and yields 0 rows.
    #[test]
    fn finalize_seek_row_guards_on_reconstructed_pk() {
        // int pk = 42, one regular col `v`.
        let seek = SchemalessPointSeek {
            bytes: 42i32.to_be_bytes().to_vec(),
            pk_name: "partition_key".to_string(),
            pk_cql_type: "int".to_string(),
        };
        let key = RowKey::new(42i32.to_be_bytes().to_vec());
        let value = scan_row(&[("v", Value::Text("hi".to_string()))]);

        // Predicate on the pk name → matches → row returned, pk reconstructed.
        let pk_pred = vec![Pred::column(
            "partition_key",
            SSTableFilterOp::Equal,
            vec![Value::BigInt(42)],
        )];
        let row: QueryRow =
            finalize_schemaless_seek_row(key.clone(), value.clone(), &[], &pk_pred, &seek)
                .expect("pk-name predicate must match the reconstructed row");
        assert_eq!(row.values.get("partition_key"), Some(&Value::Integer(42)));
        assert_eq!(row.values.get("v"), Some(&Value::Text("hi".to_string())));

        // Predicate on a nonexistent column → Unknown against the materialised row
        // → row REJECTED (the FINDING 1 over-firing this removes).
        let bogus_pred = vec![Pred::column(
            "not_a_column",
            SSTableFilterOp::Equal,
            vec![Value::BigInt(42)],
        )];
        assert!(
            finalize_schemaless_seek_row(key, value, &[], &bogus_pred, &seek).is_none(),
            "a nonexistent predicate column must yield 0 rows post-seek (roborev 3784 FINDING 1)",
        );
    }

    /// The projection is honoured for output: the pk is reconstructed for the guard
    /// but dropped from the row when the projection excludes it.
    #[test]
    fn finalize_seek_row_honours_projection_for_output() {
        let seek = SchemalessPointSeek {
            bytes: 7i32.to_be_bytes().to_vec(),
            pk_name: "partition_key".to_string(),
            pk_cql_type: "int".to_string(),
        };
        let key = RowKey::new(7i32.to_be_bytes().to_vec());
        let value = scan_row(&[("v", Value::Text("x".to_string()))]);
        let pk_pred = vec![Pred::column(
            "partition_key",
            SSTableFilterOp::Equal,
            vec![Value::BigInt(7)],
        )];
        // Projection = [v] only → pk used for the guard, then dropped from output.
        let row = finalize_schemaless_seek_row(key, value, &["v".to_string()], &pk_pred, &seek)
            .expect("row passes the guard");
        assert!(
            !row.values.contains_key("partition_key"),
            "pk excluded from projection must not appear in output",
        );
        assert_eq!(row.values.get("v"), Some(&Value::Text("x".to_string())));
    }
}
