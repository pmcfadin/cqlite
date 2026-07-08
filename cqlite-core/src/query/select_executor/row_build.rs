//! Row assembly and table-id/type helpers for the SELECT executor.
//!
//! [`build_row_from_scan`] turns a raw `(RowKey, ScanRow)` scan entry into a
//! `QueryRow`, reconstructing partition-key columns from the raw key. It is part
//! of the public surface (re-exported via `query::mod`) so other readers (e.g.
//! the Arrow Flight compaction-merge producer) assemble rows identically.

use super::super::result::{cql_type_to_data_type, ColumnInfo, QueryRow};
use crate::{
    parser::complex_types::ComplexTypeParser,
    schema::CqlType,
    types::{RowKey, ScanRow, Value},
    TableId,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Split a `TableId` of the form `"keyspace.table"` into its parts.
///
/// If no dot is present, the whole name becomes the table component and the
/// keyspace is `None`.
pub(super) fn parse_table_id(table_id: &TableId) -> (Option<String>, String) {
    let table_str = table_id.name();
    match table_str.rfind('.') {
        Some(dot) => (
            Some(table_str[..dot].to_string()),
            table_str[dot + 1..].to_string(),
        ),
        None => (None, table_str.to_string()),
    }
}

/// Parse a CQL type string (e.g. `"list<int>"`, `"text"`) into a [`CqlType`].
///
/// Returns `None` when the type string cannot be parsed (unknown or malformed
/// types). Used to populate `ColumnInfo::cql_type` from the schema's string
/// representation, satisfying the no-heuristics mandate (Issue #28).
pub(super) fn parse_cql_type_str(type_str: &str) -> Option<CqlType> {
    let parser = ComplexTypeParser::new();
    parser
        .parse_type(type_str)
        .ok()
        .map(|parsed| parsed.cql_type)
}

/// Build a `ColumnInfo` from a schema column name + CQL type string, deriving
/// the flat `DataType` from the parsed `CqlType` (Issue #674). Centralises the
/// "name + type string → ColumnInfo" pattern shared by the SELECT-* and
/// explicit-projection column builders.
pub(super) fn column_info_from_type_str(
    name: String,
    type_str: &str,
    position: usize,
    table_name: Option<String>,
) -> ColumnInfo {
    let cql_type_opt = parse_cql_type_str(type_str);
    let data_type = cql_type_opt
        .as_ref()
        .map(cql_type_to_data_type)
        .unwrap_or(crate::types::DataType::Text);
    let mut col_info = ColumnInfo {
        name,
        data_type,
        nullable: true,
        position,
        table_name,
        cql_type: None,
    };
    if let Some(cql_type) = cql_type_opt {
        col_info = col_info.with_cql_type(cql_type);
    }
    col_info
}

/// Cross-row memoization of the decoded partition-key columns (Issue #1817).
///
/// Partition-key column VALUES are constant within a partition, but
/// [`build_row_from_scan`] historically re-decoded them from the raw key bytes
/// for EVERY row. Because a scan yields the rows of one partition consecutively
/// (token-ordered full scans, partition-targeted lookups, and the Flight k-way
/// merge all group a partition's rows), memoizing the last partition's decoded
/// columns makes the byte-parse + name-intern happen ONCE per partition
/// (`O(partitions)`) rather than once per row (`O(rows)`). The decoded values are
/// `clone`d into each row (an `Arc` ref-count bump for the name + a `Value`
/// clone — each row needs its own copy), so the materialized rows are
/// byte-identical to the prior per-row decode.
///
/// If the input is NOT partition-ordered the cache simply misses and re-decodes,
/// so correctness never depends on ordering — only the decode COUNT does.
/// A decoded partition's key bytes paired with its interned `(name, value)`
/// partition-key columns (Issue #1817). Names are interned once per partition.
type DecodedPartitionKey = (Arc<[u8]>, Vec<(Arc<str>, Value)>);

#[derive(Default)]
pub struct PartitionKeyCache {
    /// Raw partition-key bytes of the last decoded partition and its decoded
    /// `(interned name, value)` columns (unfiltered by projection — the
    /// projection filter is applied when cloning into each row, so a cache entry
    /// is reusable across queries with different projections).
    decoded: Option<DecodedPartitionKey>,
}

impl PartitionKeyCache {
    /// Return the decoded partition-key columns for `key_bytes`, decoding through
    /// the canonical codec (and interning names once) only on a cache MISS — i.e.
    /// once per distinct partition when rows arrive partition-grouped.
    fn columns_for<'a>(
        &'a mut self,
        key_bytes: &Arc<[u8]>,
        schema: &crate::schema::TableSchema,
    ) -> &'a [(Arc<str>, Value)] {
        let hit = self
            .decoded
            .as_ref()
            .is_some_and(|(cached, _)| cached.as_ref() == key_bytes.as_ref());
        if !hit {
            // Test-only decode/name-derivation counter (Issue #1817): a cache MISS
            // is exactly one decode. A partition-grouped scan of N rows over P
            // partitions increments this P times, not N — the O(partitions) pin.
            #[cfg(test)]
            super::PARTITION_KEY_DECODES.with(|c| c.set(c.get() + 1));

            let cols = match crate::storage::partition_key_codec::decode_partition_key_columns(
                key_bytes, schema,
            ) {
                // Intern each name into a shared `Arc<str>` ONCE (per partition),
                // so cloning into a row is a ref-count bump, not a `String` alloc.
                Ok(pk_columns) => pk_columns
                    .into_iter()
                    .map(|(name, value)| (Arc::<str>::from(name), value))
                    .collect(),
                // Surface — never silently swallow — a decode failure, so a
                // missing partition-key column can't ship invisibly (Issue #586).
                // Cache the empty result so a failing partition is not re-decoded
                // (and not re-warned) once per row.
                Err(e) => {
                    tracing::warn!(
                        "Failed to reconstruct partition-key columns from row key \
                         (len={} bytes) for {}.{}: {}",
                        key_bytes.len(),
                        schema.keyspace,
                        schema.table,
                        e
                    );
                    Vec::new()
                }
            };
            self.decoded = Some((Arc::clone(key_bytes), cols));
        }
        match &self.decoded {
            Some((_, cols)) => cols,
            None => &[],
        }
    }
}

/// Build a `QueryRow` from a single `(RowKey, ScanRow)` produced by storage scan,
/// applying optional projection and synthesising partition-key columns from the
/// raw key bytes when a schema is available.
///
/// Partition-key columns are never stored in the cell payload, so they are
/// reconstructed from the raw row key via the canonical
/// [`crate::storage::partition_key_codec::decode_partition_key_columns`] (the
/// same codec the write engine uses). This is the fix for Issue #586: the
/// previous decoder assumed a `u16` length prefix for every TEXT key, which is
/// only correct for composite components — a single-component TEXT partition key
/// is raw bytes, so its column was silently dropped from scan-built rows.
///
/// Returns `None` for tombstoned rows (so the caller can `continue`).
///
/// Exposed publicly so other readers (e.g. the Arrow Flight server's compaction
/// merge producer) can assemble rows identically to the SELECT path, guaranteeing
/// output parity. The `row` carries the decoded non-partition-key cells as the
/// single [`ScanRow`] row carrier (issue #1334); partition-key columns are
/// reconstructed from `key`.
///
/// This is a thin wrapper over [`build_row_from_scan_cached`] with a single-use
/// [`PartitionKeyCache`], so a one-off build decodes the key exactly once (as
/// before). Loops over many rows should call [`build_row_from_scan_cached`] with
/// a shared cache to hoist the per-partition decode (Issue #1817).
pub fn build_row_from_scan(
    key: RowKey,
    row: ScanRow,
    projection: &[String],
    schema: Option<&crate::schema::TableSchema>,
) -> Option<QueryRow> {
    let mut pk_cache = PartitionKeyCache::default();
    build_row_from_scan_cached(key, row, projection, schema, &mut pk_cache)
}

/// [`build_row_from_scan`] with a caller-owned [`PartitionKeyCache`] that hoists
/// the partition-key decode across the rows of a partition (Issue #1817).
///
/// The output is byte-identical to [`build_row_from_scan`] — the cache only
/// avoids re-decoding the partition key for consecutive rows that share it.
pub fn build_row_from_scan_cached(
    key: RowKey,
    row: ScanRow,
    projection: &[String],
    schema: Option<&crate::schema::TableSchema>,
    pk_cache: &mut PartitionKeyCache,
) -> Option<QueryRow> {
    // Suppress tombstoned / absent rows from user-visible output. A row tombstone
    // or null row reaches here as `ScanRow::Marker` (Issue #505); it must never
    // appear in query results. A live row is always `ScanRow::Row`, so there is
    // exactly ONE row-carrier path — a live row's column values can never silently
    // fall through to a non-row fallback (issue #1334 / roborev H2).
    let cells = row.into_cells()?;

    // Issue #1584: pre-size the value map to the upper bound of inserts — the
    // decoded cell count plus the reconstructed partition-key columns. This is a
    // single sized allocation with no rehash growth (for `SELECT *` it is exact;
    // for a narrower projection it slightly over-reserves, still one alloc).
    let pk_hint = schema.map(|s| s.partition_keys.len()).unwrap_or(0);
    let mut row_values: HashMap<Arc<str>, Value> = HashMap::with_capacity(cells.len() + pk_hint);
    let project = |name: &str| projection.is_empty() || projection.iter().any(|p| p == name);

    // Issue #1334: the decoder carries interned `Arc<str>` column-name handles in
    // the row carrier; move them straight into `QueryRow.values` (an `Arc` move —
    // NO `String` re-allocation of the name).
    for (name, col_value) in cells {
        if project(&name) {
            row_values.insert(name, col_value);
        }
    }
    // Cassandra never serialises partition-key columns in the cell payload;
    // reconstruct them from the raw row key when the schema is known. We decode
    // through the canonical codec shared with the write engine so single-component
    // (raw bytes) and composite (`[u16 len][bytes][0x00]`) keys are handled
    // identically on both paths (Issue #586). Issue #1817: the decode is memoized
    // per partition by `pk_cache`, so consecutive rows of a partition clone the
    // already-decoded columns instead of re-parsing the key bytes.
    if let Some(schema) = schema {
        for (name, value) in pk_cache.columns_for(&key.0, schema) {
            if project(name) {
                row_values.insert(Arc::clone(name), value.clone());
            }
        }
    }

    Some(QueryRow {
        values: row_values,
        key,
        metadata: Default::default(),
        cell_metadata: None,
    })
}

#[cfg(test)]
mod tests {
    use super::super::predicate::evaluate_predicates;
    use super::super::test_support::single_pk_schema;
    use super::*;

    /// Issue #586: a single-component TEXT partition key is stored as raw bytes
    /// with NO length prefix. `build_row_from_scan` must materialise it from the
    /// `RowKey`. Before the fix the column was silently dropped (the decoder
    /// read a phantom `u16` prefix, errored, and the error was swallowed).
    #[test]
    fn build_row_from_scan_materialises_single_text_pk() {
        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = ScanRow::Row(vec![(Arc::from("name"), Value::Text("name-0".to_string()))]);
        let schema = single_pk_schema("id", "text");

        let row = build_row_from_scan(key, value, &[], Some(&schema))
            .expect("row must be built (not tombstoned)");

        assert_eq!(
            row.values.get("id"),
            Some(&Value::Text("k0000000000000000".to_string())),
            "Issue #586: single TEXT PK column must be reconstructed from the raw row key"
        );
        // Regular columns must still be present.
        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text("name-0".to_string()))
        );
    }

    /// Issue #586: with the PK column materialised, a residual `WHERE id = '...'`
    /// (the path TEXT single-PK queries fall through to) now matches.
    #[test]
    fn scan_built_row_matches_text_pk_equality_predicate() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = ScanRow::Row(vec![(Arc::from("age"), Value::Integer(0))]);
        let schema = single_pk_schema("id", "text");
        let row = build_row_from_scan(key, value, &[], Some(&schema)).unwrap();

        let predicate = SSTablePredicate::column(
            "id",
            SSTableFilterOp::Equal,
            vec![Value::Text("k0000000000000000".to_string())],
        );

        assert!(
            evaluate_predicates(&row, std::slice::from_ref(&predicate)).unwrap(),
            "Issue #586: WHERE id = '<literal>' must match the reconstructed PK column"
        );
    }

    /// Issue #1334 / roborev H2: a multi-column live `ScanRow::Row` must
    /// disassemble into EVERY named column value — never collapse into a
    /// synthetic `"data"` fallback (the bug where a non-`ScanRow::Row` carrier
    /// dropped all column values). This pins the single-carrier contract:
    /// `build_row_from_scan` yields the real columns and NO fallback key.
    #[test]
    fn build_row_from_scan_multi_column_row_has_no_data_fallback() {
        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = ScanRow::Row(vec![
            (Arc::from("name"), Value::Text("alice".to_string())),
            (Arc::from("score"), Value::Integer(42)),
        ]);

        // No schema → no partition-key reconstruction; only the row's own cells.
        let row = build_row_from_scan(key, value, &[], None)
            .expect("a live row must build (not tombstoned)");

        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text("alice".to_string())),
            "real text column value must survive the row-carrier disassembly"
        );
        assert_eq!(
            row.values.get("score"),
            Some(&Value::Integer(42)),
            "real int column value must survive the row-carrier disassembly"
        );
        assert!(
            !row.values.contains_key("data"),
            "roborev H2: column values must NOT collapse into a synthetic 'data' fallback"
        );
        assert_eq!(
            row.values.len(),
            2,
            "exactly the two real columns, no extras"
        );
    }

    /// Issue #1584: the row value map is pre-sized to the decoded cell count so a
    /// single sized allocation covers the row (no rehash growth). Pinned via a
    /// narrow projection: 8 cells are decoded but only ONE survives projection —
    /// the map's capacity must still reflect the 8-cell hint (`>= 8`). With the
    /// pre-fix `HashMap::new()` the map is grown from empty to the single inserted
    /// entry (capacity 3), which fails this lower bound.
    #[test]
    fn build_row_from_scan_presizes_value_map() {
        let cells: Vec<(Arc<str>, Value)> = (0..8)
            .map(|i| (Arc::from(format!("c{i}").as_str()), Value::Integer(i)))
            .collect();
        let key = RowKey::new(b"k".to_vec());

        let row = build_row_from_scan(key, ScanRow::Row(cells), &["c0".to_string()], None)
            .expect("a live row must build");

        assert_eq!(row.values.len(), 1, "projection keeps exactly one column");
        assert!(
            row.values.capacity() >= 8,
            "issue #1584: value map must be pre-sized to the decoded cell count \
             (>= 8), not grown from empty to the projected size; got capacity {}",
            row.values.capacity()
        );
    }

    /// Issue #1817: partition-key columns are CONSTANT within a partition, so a
    /// [`PartitionKeyCache`] shared across the rows of a partition must decode the
    /// key ONCE (`O(partitions)`), not once per row (`O(rows)`). Pinned by the
    /// deterministic `PARTITION_KEY_DECODES` counter — NOT a wall-clock bench.
    ///
    /// Red-first: with the pre-hoist per-row decode (or a fresh cache per row),
    /// `N` rows of one partition would record `N` decodes; the shared cache makes
    /// it exactly `1`. Output must stay byte-identical — every row still carries
    /// the reconstructed `id` PK column plus its own regular cell.
    #[test]
    fn pk_decode_is_once_per_partition_not_per_row() {
        use super::super::PARTITION_KEY_DECODES;

        let schema = single_pk_schema("id", "text");
        let key_bytes = b"partition-A".to_vec();
        const N: usize = 50;

        PARTITION_KEY_DECODES.with(|c| c.set(0));
        let mut pk_cache = PartitionKeyCache::default();
        let mut rows = Vec::with_capacity(N);
        for i in 0..N {
            let cells = ScanRow::Row(vec![(Arc::from("v"), Value::Integer(i as i32))]);
            let row = build_row_from_scan_cached(
                RowKey::new(key_bytes.clone()),
                cells,
                &[],
                Some(&schema),
                &mut pk_cache,
            )
            .expect("a live row must build");
            rows.push(row);
        }
        let decodes = PARTITION_KEY_DECODES.with(|c| c.get());

        assert_eq!(
            decodes, 1,
            "issue #1817: {N} rows of ONE partition must decode the partition key \
             ONCE (O(partitions)), not once per row (would be {N})"
        );
        // Output byte-identical: every row has the reconstructed PK + its own cell.
        assert_eq!(rows.len(), N);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row.values.get("id"),
                Some(&Value::Text("partition-A".to_string())),
                "each row must carry the reconstructed PK column value"
            );
            assert_eq!(row.values.get("v"), Some(&Value::Integer(i as i32)));
        }
    }

    /// Issue #1817: across `P` DISTINCT partitions (each with several rows), a
    /// shared cache decodes exactly `P` times — the decode count tracks
    /// `O(partitions)`, independent of the total row count. Rows are supplied
    /// partition-grouped, as every scan/merge site delivers them.
    #[test]
    fn pk_decode_counts_partitions_not_rows() {
        use super::super::PARTITION_KEY_DECODES;

        let schema = single_pk_schema("id", "text");
        const P: usize = 4;
        const ROWS_PER_PART: usize = 10;

        PARTITION_KEY_DECODES.with(|c| c.set(0));
        let mut pk_cache = PartitionKeyCache::default();
        let mut total_rows = 0;
        for p in 0..P {
            let key_bytes = format!("partition-{p}").into_bytes();
            for r in 0..ROWS_PER_PART {
                let cells = ScanRow::Row(vec![(Arc::from("v"), Value::Integer(r as i32))]);
                let row = build_row_from_scan_cached(
                    RowKey::new(key_bytes.clone()),
                    cells,
                    &[],
                    Some(&schema),
                    &mut pk_cache,
                )
                .expect("a live row must build");
                assert_eq!(
                    row.values.get("id"),
                    Some(&Value::Text(format!("partition-{p}"))),
                    "PK column reconstructed per partition"
                );
                total_rows += 1;
            }
        }
        let decodes = PARTITION_KEY_DECODES.with(|c| c.get());

        assert_eq!(total_rows, P * ROWS_PER_PART);
        assert_eq!(
            decodes,
            P,
            "issue #1817: decode count must be O(partitions) = {P}, not O(rows) = {}",
            P * ROWS_PER_PART
        );
    }

    /// Issue #1817 (control / red-anchor): the wrapper [`build_row_from_scan`]
    /// uses a FRESH single-use cache each call, so calling it per row decodes per
    /// row — this is exactly the O(rows) behavior the shared-cache hoist replaces.
    /// It documents that the hoist requires threading ONE cache through the loop
    /// (a per-row fresh cache is the pre-fix cost). Output is still byte-identical.
    #[test]
    fn build_row_from_scan_wrapper_decodes_per_call() {
        use super::super::PARTITION_KEY_DECODES;

        let schema = single_pk_schema("id", "text");
        let key_bytes = b"partition-A".to_vec();
        const N: usize = 20;

        PARTITION_KEY_DECODES.with(|c| c.set(0));
        for i in 0..N {
            let cells = ScanRow::Row(vec![(Arc::from("v"), Value::Integer(i as i32))]);
            let row =
                build_row_from_scan(RowKey::new(key_bytes.clone()), cells, &[], Some(&schema))
                    .expect("a live row must build");
            assert_eq!(
                row.values.get("id"),
                Some(&Value::Text("partition-A".to_string()))
            );
        }
        let decodes = PARTITION_KEY_DECODES.with(|c| c.get());
        assert_eq!(
            decodes, N,
            "the single-use wrapper decodes once per call ({N}); the shared-cache \
             `build_row_from_scan_cached` is what makes a loop O(partitions)"
        );
    }

    /// A suppressed marker (row tombstone / null row) yields no user-visible row.
    #[test]
    fn build_row_from_scan_marker_is_suppressed() {
        let key = RowKey::new(b"k".to_vec());
        assert!(
            build_row_from_scan(key, ScanRow::Marker(Value::Null), &[], None).is_none(),
            "a marker (tombstone/null) row must be suppressed from user output"
        );
    }

    /// Issue #1334 (roborev round 9, finding 2): the canonical query consumer
    /// SUPPRESSES a `ScanRow::Marker` but SURFACES a live `ScanRow::Row`. This is
    /// exactly why the CLI `read`/`inspect`/`benchmark` bulletproof-reader fallback
    /// producers must NOT wrap a LIVE synthetic value in `Marker` — doing so drops
    /// it from user-visible output. The SAME live value must survive as a `Row`.
    #[test]
    fn live_value_dropped_as_marker_surfaces_as_row() {
        let key = RowKey::new(b"k".to_vec());
        let live = Value::Text("synthetic-fallback".to_string());

        // Wrapped as a Marker (the pre-fix producer): dropped entirely.
        assert!(
            build_row_from_scan(key.clone(), ScanRow::Marker(live.clone()), &[], None).is_none(),
            "a LIVE value mis-wrapped as Marker is dropped — the bug the producers must avoid"
        );

        // Wrapped as a live Row (the fixed producer): surfaces under its cell name.
        let row = build_row_from_scan(
            key,
            ScanRow::Row(vec![(Arc::from("data"), live.clone())]),
            &[],
            None,
        )
        .expect("a live Row must surface");
        assert_eq!(row.values.get("data"), Some(&live));
    }
}
