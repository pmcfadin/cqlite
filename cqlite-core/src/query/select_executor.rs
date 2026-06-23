//! CQL SELECT Query Executor for Direct SSTable Access
//!
//! This module implements the REVOLUTIONARY query executor that can run
//! CQL SELECT statements directly on SSTable files without Cassandra.
//!
//! Features:
//! - Direct SSTable file scanning with predicate pushdown
//! - Streaming results for memory efficiency
//! - Parallel execution across multiple SSTable files
//! - Advanced aggregation with hash-based grouping
//! - Collection operations (list[index], map['key'])

use super::{
    access_path::{AccessPath, FallbackReason},
    result::{
        cql_type_to_data_type, ColumnInfo, ProjectionFlags, QueryMetadata, QueryResult,
        QueryResultIterator, QueryRow, StreamingConfig,
    },
    select_ast::*,
    select_optimizer::{AggregationPlan, ExecutionStep, OptimizedQueryPlan, SSTablePredicate},
};
use crate::{
    parser::complex_types::ComplexTypeParser,
    schema::{CqlType, SchemaManager},
    storage::StorageEngine,
    types::{RowKey, Value},
    Error, Result, TableId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Clock abstraction for TTL "now" injection.
///
/// This trait exists solely so that tests can inject a deterministic timestamp
/// instead of reading `SystemTime`. The only production implementation is
/// `SystemClock`; tests use `FixedClock`.
pub trait NowSeconds: Send + Sync {
    /// Return the current time as seconds since Unix epoch.
    fn now_seconds(&self) -> i64;
}

/// Production clock: reads from the system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl NowSeconds for SystemClock {
    fn now_seconds(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }
}

/// Test clock: always returns a fixed value.
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(pub i64);

impl NowSeconds for FixedClock {
    fn now_seconds(&self) -> i64 {
        self.0
    }
}

/// SELECT query executor for SSTable-based storage
pub struct SelectExecutor {
    /// Schema manager for metadata
    _schema: Arc<SchemaManager>,
    /// Storage engine for SSTable access
    storage: Arc<StorageEngine>,
    /// Clock used for TTL "remaining seconds" computation (injectable for tests).
    clock: Arc<dyn NowSeconds>,
}

impl std::fmt::Debug for SelectExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectExecutor")
            .field("_schema", &self._schema)
            .field("storage", &self.storage)
            .finish_non_exhaustive()
    }
}

/// Query execution context
///
/// Pure bookkeeping for an in-flight query. Only used internally; the public
/// API surface is `SelectExecutor` itself.
#[derive(Debug)]
struct ExecutionContext {
    /// Current table being queried
    pub table_id: TableId,
    /// Column metadata
    pub columns: Vec<ColumnInfo>,
    /// Row count processed so far
    pub rows_processed: u64,
    /// Projection flags controlling opt-in metadata collection (Issue #692).
    ///
    /// Set to `include_cell_metadata = true` when any `WRITETIME` or `TTL`
    /// select item is detected during planning so the reader can thread
    /// per-cell write metadata.
    pub projection_flags: ProjectionFlags,
    /// Access path chosen by the SSTable-scan step for THIS query (Issue #960).
    ///
    /// Per-query state, set where the scan step decides its path. The
    /// result-attached `QueryMetadata.access_path` is read from here, NOT from
    /// the process-global probe, so concurrent SELECTs cannot overwrite each
    /// other's reported path between `record()` and the result build. The global
    /// probe (`access_path::record/last`) remains for test assertions only.
    pub access_path: Option<AccessPath>,
}

/// Aggregation state for GROUP BY operations
#[derive(Debug)]
struct AggregationState {
    /// Vector for grouping since Value doesn't implement Hash
    groups: Vec<(Vec<Value>, Vec<AggregateValue>)>,
    /// Memory usage tracking
    memory_usage_bytes: usize,
    /// Maximum memory limit
    memory_limit_bytes: usize,
}

/// Aggregate value accumulator
#[derive(Debug, Clone)]
enum AggregateValue {
    Count(u64),
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Value),
    Max(Value),
}

// ---------------------------------------------------------------------------
// Free helpers: pure functions that don't depend on `&self`. These were
// previously duplicated as `_static` methods on `SelectExecutor`; centralising
// them lets both the streaming background task and the synchronous executor
// share one implementation.
// ---------------------------------------------------------------------------

/// Split a `TableId` of the form `"keyspace.table"` into its parts.
///
/// If no dot is present, the whole name becomes the table component and the
/// keyspace is `None`.
fn parse_table_id(table_id: &TableId) -> (Option<String>, String) {
    let table_str = table_id.name();
    match table_str.rfind('.') {
        Some(dot) => (
            Some(table_str[..dot].to_string()),
            table_str[dot + 1..].to_string(),
        ),
        None => (None, table_str.to_string()),
    }
}

/// Compare two `Value`s for equality, including limited cross-type numeric
/// coercion (int↔bigint, int↔float, bigint↔float).
///
/// `Value` implements `PartialEq` natively but only matches identical variants;
/// we additionally treat the small set of cross-numeric cases that show up in
/// CQL predicates.
fn values_equal(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    // Only coerce when both operands are numeric — otherwise non-numeric
    // pairs (e.g. Text vs Integer) would spuriously compare equal via `as_f64`.
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x == y;
        }
    }
    false
}

/// True when both `Value`s are numeric variants eligible for cross-type coercion.
fn same_numeric_family(a: &Value, b: &Value) -> bool {
    a.as_f64().is_some() && b.as_f64().is_some()
}

/// Compare two `Value`s for ordering, returning `Ordering::Equal` for
/// incomparable variants. Used by sorting/aggregation paths that historically
/// swallowed comparison errors via `unwrap_or(0)`.
fn compare_values_ordering(a: &Value, b: &Value) -> std::cmp::Ordering {
    try_compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
}

/// Compare two `Value`s for ordering, returning an error when the operand
/// types are not comparable. Preferred in WHERE-clause evaluation so users see
/// a real diagnostic rather than a silent equality.
///
/// Cross-type numerics are coerced via `f64` first; same-variant comparisons
/// fall back to `Value::partial_cmp`. We deliberately avoid `partial_cmp` for
/// non-matching variants because it stringifies and would produce surprising
/// orderings (e.g. `Text("9")` < `Text("10")` lexicographically).
fn try_compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return Ok(x.partial_cmp(&y).unwrap_or(Ordering::Equal));
        }
    }
    if std::mem::discriminant(a) == std::mem::discriminant(b) {
        return a.partial_cmp(b).ok_or_else(|| {
            Error::query_execution("Cannot compare incompatible types".to_string())
        });
    }
    log::debug!("Cannot compare {:?} with {:?}", a, b);
    Err(Error::query_execution(
        "Cannot compare incompatible types".to_string(),
    ))
}

/// Three-valued (SQL Kleene) outcome of evaluating a single leaf predicate
/// against one row.
///
/// `Unknown` is produced when the predicate references a column that is absent
/// from the row or whose value is `Null` — i.e. a SQL `NULL` operand. Callers
/// that only need pure-`AND` rejection (the historical [`evaluate_predicates`])
/// treat `Unknown` and `False` identically; callers that evaluate `OR`/`NOT`
/// (the Flight nested-predicate evaluator, issue #834) must distinguish them to
/// match SQL `WHERE` semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafOutcome {
    /// The predicate is definitely satisfied.
    True,
    /// The predicate is definitely not satisfied.
    False,
    /// The predicate's column is missing or `NULL` (SQL `UNKNOWN`).
    Unknown,
}

/// Evaluate a single SSTable leaf predicate against one `QueryRow` with SQL
/// three-valued (Kleene) semantics.
///
/// Returns [`LeafOutcome::Unknown`] when the predicate's column is absent from
/// the row or its value is `Null`; otherwise a definite `True`/`False` from the
/// typed comparison. This is the finer-grained primitive underlying both
/// [`evaluate_predicates`] (pure AND, where `Unknown` rejects like `False`) and
/// the Flight nested-predicate evaluator (issue #834), so all three paths share
/// one copy of the comparison logic (`values_equal` / `compare_values_ordering`).
///
/// `IN` and `Prefix` follow `evaluate_predicates`: `IN` is membership over the
/// value list, `Prefix` is a text `starts_with`. `Range` (two-bound) is included
/// for completeness even though Flight lowers single bounds to `Gt`/`Lt`/etc.
/// `BloomFilter` is always `True` (checked upstream).
pub fn evaluate_leaf(row: &QueryRow, predicate: &SSTablePredicate) -> LeafOutcome {
    use super::select_optimizer::SSTableFilterOp;

    // Issue #955: a `token(pk)` predicate constrains the partition-key token, not
    // a stored column. Compute the Murmur3 token of the row's raw partition key
    // (`row.key`) using the same partitioner the rest of the codebase uses, then
    // compare against the i64 bound. An empty key cannot be hashed meaningfully
    // (a synthesised/aggregate row); treat it as Unknown so it is rejected.
    if predicate.is_token() {
        if row.key.0.is_empty() {
            return LeafOutcome::Unknown;
        }
        let row_token = crate::util::cassandra_murmur3::cassandra_murmur3_token(&row.key.0);
        let Some(Value::BigInt(bound)) = predicate.values.first() else {
            return LeafOutcome::Unknown;
        };
        let matches = match &predicate.operation {
            SSTableFilterOp::Gt => row_token > *bound,
            SSTableFilterOp::Gte => row_token >= *bound,
            SSTableFilterOp::Lt => row_token < *bound,
            SSTableFilterOp::Lte => row_token <= *bound,
            SSTableFilterOp::Equal => row_token == *bound,
            // token() only produces inequality/equality predicates upstream.
            _ => return LeafOutcome::Unknown,
        };
        return if matches {
            LeafOutcome::True
        } else {
            LeafOutcome::False
        };
    }

    let column_value = match row.values.get(&predicate.column) {
        // A SQL `NULL` operand (absent column or explicit `Null`) is `UNKNOWN`.
        None | Some(Value::Null) => return LeafOutcome::Unknown,
        Some(v) => v,
    };
    let matches = match &predicate.operation {
        SSTableFilterOp::Equal => predicate
            .values
            .first()
            .is_some_and(|v| values_equal(column_value, v)),
        // Membership uses the same coercing equality as `Equal` so a pushed-down
        // `IN` operand that lowers to a wider numeric type (e.g. `Integer`) still
        // matches a narrow column value (`TinyInt`/`SmallInt`/`Float32`).
        SSTableFilterOp::In => predicate
            .values
            .iter()
            .any(|v| values_equal(column_value, v)),
        SSTableFilterOp::Range => {
            if predicate.values.len() < 2 {
                false
            } else {
                let lo = &predicate.values[0];
                let hi = &predicate.values[1];
                compare_values_ordering(column_value, lo).is_ge()
                    && compare_values_ordering(column_value, hi).is_le()
            }
        }
        // Single-bound clustering inequalities (Issue #788). A missing bound
        // rejects the row, mirroring the `Range` len-guard above.
        SSTableFilterOp::Gt => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_gt()),
        SSTableFilterOp::Gte => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_ge()),
        SSTableFilterOp::Lt => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_lt()),
        SSTableFilterOp::Lte => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_le()),
        SSTableFilterOp::Prefix => matches!(
            (column_value, predicate.values.first()),
            (Value::Text(s), Some(Value::Text(p))) if s.starts_with(p)
        ),
        SSTableFilterOp::BloomFilter => true, // already checked upstream
    };
    if matches {
        LeafOutcome::True
    } else {
        LeafOutcome::False
    }
}

/// Evaluate the SSTable predicate set against a single `QueryRow`.
///
/// Returns `Ok(true)` only if every predicate is satisfied. A missing column
/// causes the row to be rejected.
///
/// Exposed publicly so the Arrow Flight server can apply identical predicate
/// pushdown semantics to its merged rows (output parity with SELECT).
///
/// Implemented in terms of [`evaluate_leaf`]: under pure `AND`, both
/// [`LeafOutcome::False`] and [`LeafOutcome::Unknown`] reject the row, so this
/// preserves the historical "missing column → false" behaviour exactly.
pub fn evaluate_predicates(row: &QueryRow, predicates: &[SSTablePredicate]) -> Result<bool> {
    for predicate in predicates {
        if evaluate_leaf(row, predicate) != LeafOutcome::True {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Build a `QueryRow` from a single `(RowKey, Value)` produced by storage scan,
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
/// output parity. The `value` is expected to be a `Value::Map` of decoded
/// non-partition-key cells; partition-key columns are reconstructed from `key`.
pub fn build_row_from_scan(
    key: RowKey,
    value: Value,
    projection: &[String],
    schema: Option<&crate::schema::TableSchema>,
) -> Option<QueryRow> {
    // Suppress tombstoned rows from user-visible output. A row tombstone reaches
    // here as `Value::Tombstone` (Issue #505); before that change it was `Value::Null`.
    // Both must be suppressed identically so deleted rows never appear in query results.
    if matches!(value, Value::Null | Value::Tombstone(_)) {
        return None;
    }

    let mut row_values = HashMap::new();
    let project = |name: &str| projection.is_empty() || projection.iter().any(|p| p == name);

    if let Value::Map(map) = value {
        for (col_name, col_value) in map {
            if let Value::Text(name) = col_name {
                if project(&name) {
                    row_values.insert(name, col_value);
                }
            }
        }
        // Cassandra never serialises partition-key columns in the cell payload;
        // reconstruct them from the raw row key when the schema is known. We
        // decode through the canonical codec shared with the write engine so
        // single-component (raw bytes) and composite (`[u16 len][bytes][0x00]`)
        // keys are handled identically on both paths (Issue #586).
        if let Some(schema) = schema {
            match crate::storage::partition_key_codec::decode_partition_key_columns(&key.0, schema)
            {
                Ok(pk_columns) => {
                    for (name, value) in pk_columns {
                        if project(&name) {
                            row_values.insert(name, value);
                        }
                    }
                }
                // Surface — never silently swallow — a decode failure, so a
                // missing partition-key column can't ship invisibly (Issue #586).
                Err(e) => {
                    log::warn!(
                        "Failed to reconstruct partition-key columns from row key \
                         (len={} bytes) for {}.{}: {}",
                        key.0.len(),
                        schema.keyspace,
                        schema.table,
                        e
                    );
                }
            }
        }
    } else {
        // Non-map fallback: expose the raw value plus a debug-formatted id.
        row_values.insert("data".to_string(), value);
        if project("id") {
            row_values.insert("id".to_string(), Value::Text(format!("{:?}", key)));
        }
    }

    Some(QueryRow {
        values: row_values,
        key,
        metadata: Default::default(),
        cell_metadata: None,
    })
}

/// Outcome of classifying whether a SELECT can use a partition-targeted lookup.
///
/// Issue #960: this replaces the previous `Option<Vec<u8>>` so the caller can
/// record the *honest* reason a full scan was chosen, rather than collapsing all
/// fallback causes into `None`.
#[derive(Debug)]
enum PartitionLookupOutcome {
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
const MAX_IN_TARGETED_LOOKUPS: usize = 64;

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
fn classify_partition_lookup(
    predicates: &[SSTablePredicate],
    schema: Option<&crate::schema::TableSchema>,
) -> PartitionLookupOutcome {
    use super::select_optimizer::SSTableFilterOp;

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

/// Validate that every `token(...)` predicate's argument columns match the
/// table's FULL partition-key column list, in declared order (Issue #955
/// follow-up, FINDING 2).
///
/// `evaluate_leaf` evaluates a token predicate by hashing the row's *raw
/// partition key* — it does NOT recompute a token over the columns named in the
/// predicate. So a `token(col, ...)` whose columns are not exactly the partition
/// key (a non-pk column, a strict subset, or a reordering) would be evaluated
/// against the real partition-key token, silently returning rows for a different
/// expression than the user wrote.
///
/// Cassandra rejects `token()` on anything other than the full partition key in
/// declared order, so we do the same: reject with a clear error rather than
/// trust `token_columns`. With no schema we cannot know the partition key, so we
/// also reject (we cannot prove the columns are correct).
fn validate_token_predicates(
    predicates: &[SSTablePredicate],
    schema: Option<&crate::schema::TableSchema>,
) -> Result<()> {
    let token_predicates: Vec<&SSTablePredicate> =
        predicates.iter().filter(|p| p.is_token()).collect();
    if token_predicates.is_empty() {
        return Ok(());
    }

    let Some(schema) = schema else {
        return Err(Error::query_execution(
            "token() restriction requires a known table schema to validate its argument \
             against the partition key"
                .to_string(),
        ));
    };

    // The partition key in declared order (positions are 0-based and dense).
    let mut pk_cols: Vec<&crate::schema::KeyColumn> = schema.partition_keys.iter().collect();
    pk_cols.sort_by_key(|c| c.position);
    let expected: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();

    for predicate in token_predicates {
        let cols = predicate.token_columns.as_deref().unwrap_or(&[]);
        let matches = cols.len() == expected.len()
            && cols
                .iter()
                .zip(expected.iter())
                .all(|(got, want)| got == want);
        if !matches {
            return Err(Error::query_execution(format!(
                "token() must be applied to the entire partition key in declared order \
                 ({}); got token({})",
                expected.join(", "),
                cols.join(", "),
            )));
        }
    }
    Ok(())
}

/// Stable-sort scan results by their partition token, then by raw key bytes —
/// the on-disk storage order, so the union of several `scan_partition` lookups
/// (`WHERE pk IN (...)`, Issue #955) equals a full scan filtered to the same
/// keys. Uses the same `Murmur3Partitioner` token the rest of the codebase uses.
/// Stability preserves each partition's clustering order, since one partition's
/// rows arrive contiguously from a single `scan_partition` call.
fn sort_rows_by_token(rows: &mut [(RowKey, Value)]) {
    rows.sort_by(|a, b| {
        let ta = crate::util::cassandra_murmur3::cassandra_murmur3_token(&a.0 .0);
        let tb = crate::util::cassandra_murmur3::cassandra_murmur3_token(&b.0 .0);
        ta.cmp(&tb).then_with(|| a.0 .0.cmp(&b.0 .0))
    });
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

/// Apply an `ArithmeticOperator` to two same-typed numeric `Value`s.
///
/// Behaviour matches the previous inline implementations: same-type only
/// (no implicit coercion), and division/modulo by zero are reported as
/// query-execution errors. Float division-by-zero (matching the original
/// runtime path) yields IEEE inf/NaN rather than an error.
fn eval_arithmetic(op: &ArithmeticOperator, left: Value, right: Value) -> Result<Value> {
    use ArithmeticOperator::*;
    macro_rules! int_op {
        ($a:expr, $b:expr, $ctor:expr) => {
            match op {
                Add => Ok($ctor($a + $b)),
                Subtract => Ok($ctor($a - $b)),
                Multiply => Ok($ctor($a * $b)),
                Divide => {
                    if $b == 0 {
                        Err(Error::query_execution("Division by zero".to_string()))
                    } else {
                        Ok($ctor($a / $b))
                    }
                }
                Modulo => {
                    if $b == 0 {
                        Err(Error::query_execution("Modulo by zero".to_string()))
                    } else {
                        Ok($ctor($a % $b))
                    }
                }
            }
        };
    }
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => int_op!(a, b, Value::Integer),
        (Value::BigInt(a), Value::BigInt(b)) => int_op!(a, b, Value::BigInt),
        (Value::Float(a), Value::Float(b)) => match op {
            Add => Ok(Value::Float(a + b)),
            Subtract => Ok(Value::Float(a - b)),
            Multiply => Ok(Value::Float(a * b)),
            Divide => Ok(Value::Float(a / b)),
            Modulo => Ok(Value::Float(a % b)),
        },
        _ => Err(Error::query_execution(
            "Incompatible types for arithmetic".to_string(),
        )),
    }
}

/// Build the GROUP BY key for `row`. With no GROUP BY, all rows hash into a
/// single `[Null]` bucket (global aggregation).
fn build_group_key(row: &QueryRow, group_by_columns: &[String]) -> Vec<Value> {
    if group_by_columns.is_empty() {
        return vec![Value::Null];
    }
    group_by_columns
        .iter()
        .map(|col| row.values.get(col).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Locate the group matching `key` in `groups`, or push a fresh entry with
/// initial aggregator state. Returns the index into `groups`.
///
/// `Value` doesn't implement `Hash`, so groups live in a `Vec` and lookup is
/// linear. This is unchanged from the legacy implementation; switching to a
/// hash map would change result-row ordering for callers that rely on
/// insertion order.
fn find_or_init_group(
    groups: &mut Vec<(Vec<Value>, Vec<AggregateValue>)>,
    key: Vec<Value>,
    aggregates: &[super::select_optimizer::AggregateComputation],
) -> usize {
    if let Some(idx) = groups.iter().position(|(k, _)| k == &key) {
        return idx;
    }
    let initial: Vec<_> = aggregates
        .iter()
        .map(|c| match c.function {
            AggregateType::Count => AggregateValue::Count(0),
            AggregateType::Sum => AggregateValue::Sum(0.0),
            AggregateType::Avg => AggregateValue::Avg { sum: 0.0, count: 0 },
            AggregateType::Min => AggregateValue::Min(Value::Null),
            AggregateType::Max => AggregateValue::Max(Value::Null),
        })
        .collect();
    groups.push((key, initial));
    groups.len() - 1
}

/// Apply one row's contribution to a single aggregate accumulator.
///
/// COUNT(*) always increments; COUNT(col) only increments on non-null. SUM and
/// AVG ignore non-numeric values. MIN/MAX clone the value only when it
/// becomes the new extremum, sparing per-row clones in the common case.
fn update_aggregate(
    state: &mut AggregateValue,
    agg_comp: &super::select_optimizer::AggregateComputation,
    row: &QueryRow,
) {
    let is_star = agg_comp.column == "*";
    // Look up the column once; for COUNT(*) we don't need it.
    let value: Option<&Value> = if is_star {
        None
    } else {
        row.values.get(&agg_comp.column)
    };
    let is_null = !is_star && value.is_none_or(Value::is_null);

    match state {
        AggregateValue::Count(count) => {
            if is_star || !is_null {
                *count += 1;
            }
        }
        AggregateValue::Sum(sum) => {
            if let Some(v) = value.and_then(Value::as_f64) {
                *sum += v;
            }
        }
        AggregateValue::Avg { sum, count } => {
            if let Some(v) = value.and_then(Value::as_f64) {
                *sum += v;
                *count += 1;
            }
        }
        AggregateValue::Min(min_val) => {
            if let Some(v) = value {
                if !v.is_null()
                    && (min_val.is_null() || compare_values_ordering(v, min_val).is_lt())
                {
                    *min_val = v.clone();
                }
            }
        }
        AggregateValue::Max(max_val) => {
            if let Some(v) = value {
                if !v.is_null()
                    && (max_val.is_null() || compare_values_ordering(v, max_val).is_gt())
                {
                    *max_val = v.clone();
                }
            }
        }
    }
}

/// Materialize a single aggregation group into a `QueryRow`.
fn finalize_group(
    group_key: Vec<Value>,
    group_aggregates: Vec<AggregateValue>,
    agg_plan: &AggregationPlan,
) -> QueryRow {
    let mut row_values = HashMap::new();

    for (i, col) in agg_plan.group_by_columns.iter().enumerate() {
        if let Some(v) = group_key.get(i) {
            row_values.insert(col.clone(), v.clone());
        }
    }

    for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
        let result_value = match &group_aggregates[i] {
            AggregateValue::Count(count) => Value::BigInt(*count as i64),
            AggregateValue::Sum(sum) => Value::Float(*sum),
            AggregateValue::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float(sum / (*count as f64))
                } else {
                    Value::Null
                }
            }
            AggregateValue::Min(val) | AggregateValue::Max(val) => val.clone(),
        };
        row_values.insert(agg_comp.alias.clone(), result_value);
    }

    QueryRow {
        values: row_values,
        key: RowKey::new(vec![]),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

/// Constant-folding arithmetic. Same operand-type rules as `eval_arithmetic`,
/// plus BigInt support and per-operator error wording matching the legacy
/// implementation (e.g. `"Cannot add incompatible types"` and
/// `"Modulo only supported for integers"`).
fn const_arithmetic(op: &ArithmeticOperator, left: Value, right: Value) -> Result<Value> {
    use ArithmeticOperator::*;

    // Modulo's error wording is special: any non-integer combination must
    // report `"Modulo only supported for integers"` regardless of which side
    // is offending.
    if matches!(op, Modulo) {
        return match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => {
                eval_arithmetic(op, Value::Integer(a), Value::Integer(b))
            }
            (Value::BigInt(a), Value::BigInt(b)) => {
                eval_arithmetic(op, Value::BigInt(a), Value::BigInt(b))
            }
            _ => Err(Error::query_execution(
                "Modulo only supported for integers".to_string(),
            )),
        };
    }

    let verb = match op {
        Add => "add",
        Subtract => "subtract",
        Multiply => "multiply",
        Divide => "divide",
        Modulo => unreachable!("handled above"),
    };

    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            eval_arithmetic(op, Value::Integer(a), Value::Integer(b))
        }
        (Value::BigInt(a), Value::BigInt(b)) => {
            eval_arithmetic(op, Value::BigInt(a), Value::BigInt(b))
        }
        (Value::Float(a), Value::Float(b)) => {
            // Constant Float Divide rejects 0.0 (legacy behaviour); runtime
            // Float divide does not. Modulo on Float is rejected above.
            if matches!(op, Divide) && b == 0.0 {
                return Err(Error::query_execution("Division by zero".to_string()));
            }
            eval_arithmetic(op, Value::Float(a), Value::Float(b))
        }
        _ => Err(Error::query_execution(format!(
            "Cannot {} incompatible types",
            verb
        ))),
    }
}

/// Return `true` when the select clause contains at least one `WRITETIME` or
/// `TTL` call — used during planning to set `ProjectionFlags::include_cell_metadata`.
fn select_has_writetime_ttl(statement: &SelectStatement) -> bool {
    let exprs = match &statement.select_clause {
        SelectClause::All => return false,
        SelectClause::Columns(e) | SelectClause::Distinct(e) => e,
    };
    exprs
        .iter()
        .any(|e| matches!(e, SelectExpression::WriteTimeTtl(_)))
}

/// Compute the Cassandra-convention output column name for a `WriteTimeTtlCall`.
///
/// - No alias: `writetime(col)` or `ttl(col)` (lowercase, matching Cassandra).
/// - Explicit alias: the alias string, exactly as parsed.
fn writetime_ttl_column_name(call: &WriteTimeTtlCall) -> String {
    if let Some(alias) = &call.alias {
        return alias.clone();
    }
    match call.function {
        WriteTimeTtlFunction::WriteTime => format!("writetime({})", call.column),
        WriteTimeTtlFunction::Ttl => format!("ttl({})", call.column),
    }
}

/// Evaluate a `WRITETIME(col)` or `TTL(col)` call against a single `QueryRow`.
///
/// `now_secs` is the current epoch-second used only for TTL subtraction. It
/// **must** be injected by the caller rather than read here so that unit tests
/// can produce deterministic results.
///
/// Return values:
/// - `WRITETIME(col)` → `Value::BigInt(micros)` when metadata exists; `Value::Null` otherwise.
/// - `TTL(col)` → `Value::Integer(remaining_secs)` when the cell has an unexpired TTL;
///   `Value::Null` when no expiration exists **or** the cell has already expired.
fn evaluate_writetime_ttl(call: &WriteTimeTtlCall, row: &QueryRow, now_secs: i64) -> Value {
    let meta = match row.get_cell_metadata(&call.column) {
        Some(m) => m,
        None => return Value::Null,
    };

    match call.function {
        WriteTimeTtlFunction::WriteTime => Value::BigInt(meta.write_timestamp_micros),
        WriteTimeTtlFunction::Ttl => match &meta.expiration {
            None => Value::Null,
            Some(exp) => {
                let remaining = exp.expires_at_seconds - now_secs;
                if remaining <= 0 {
                    // Cell has already expired — Cassandra returns NULL.
                    Value::Null
                } else {
                    // Safe cast: remaining is in (0, i32::MAX] range for any
                    // realistic TTL (Cassandra caps TTL at 630_720_000 seconds).
                    Value::Integer(remaining.min(i32::MAX as i64) as i32)
                }
            }
        },
    }
}

/// Translate a CQL LIKE pattern (`%`, `_`) into an anchored regex.
fn like_pattern_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 4);
    out.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            _ => out.push(ch),
        }
    }
    out.push('$');
    out
}

/// Parse a CQL type string (e.g. `"list<int>"`, `"text"`) into a [`CqlType`].
///
/// Returns `None` when the type string cannot be parsed (unknown or malformed
/// types). Used to populate `ColumnInfo::cql_type` from the schema's string
/// representation, satisfying the no-heuristics mandate (Issue #28).
fn parse_cql_type_str(type_str: &str) -> Option<CqlType> {
    let parser = ComplexTypeParser::new();
    parser
        .parse_type(type_str)
        .ok()
        .map(|parsed| parsed.cql_type)
}

impl SelectExecutor {
    /// Create a new SELECT executor with a system (wall-clock) now source.
    pub fn new(schema: Arc<SchemaManager>, storage: Arc<StorageEngine>) -> Self {
        Self {
            _schema: schema,
            storage,
            clock: Arc::new(SystemClock),
        }
    }

    /// Create a SELECT executor with a custom clock (for deterministic tests).
    #[cfg(test)]
    pub fn with_clock(
        schema: Arc<SchemaManager>,
        storage: Arc<StorageEngine>,
        clock: Arc<dyn NowSeconds>,
    ) -> Self {
        Self {
            _schema: schema,
            storage,
            clock,
        }
    }

    /// Execute an optimized query plan
    pub async fn execute(&self, plan: OptimizedQueryPlan) -> Result<QueryResult> {
        // Issue #960: clear the global access-path probe so a stale value from a
        // previous query cannot satisfy a test assertion against this one.
        crate::query::access_path::reset();

        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), use a dummy table ID
            TableId::new("_dummy_")
        };

        // Issue #692: detect whether any WRITETIME/TTL select items are present
        // during planning and set the opt-in flag so the reader threads per-cell
        // metadata. This is the "planning" half of the executor wiring; the
        // "evaluation" half lives in `evaluate_select_expression`.
        let projection_flags = ProjectionFlags {
            include_cell_metadata: select_has_writetime_ttl(&plan.statement),
        };
        log::debug!(
            "Query plan: include_cell_metadata={}",
            projection_flags.include_cell_metadata
        );

        let mut context = ExecutionContext {
            table_id,
            columns: self.get_result_columns(&plan.statement).await?,
            rows_processed: 0,
            projection_flags,
            access_path: None,
        };

        // Handle queries without FROM clause (like SELECT 1)
        if plan.statement.from_clause.is_none() {
            return self.execute_constant_query(&plan.statement, &context).await;
        }

        // Execute the plan step by step
        let mut intermediate_results = Vec::new();

        // If no execution steps are provided, add a default table scan
        let execution_steps = if plan.execution_steps.is_empty() {
            vec![ExecutionStep::SSTableScan {
                table: context.table_id.clone(),
                predicates: vec![],
                projection: context.columns.iter().map(|c| c.name.clone()).collect(),
            }]
        } else {
            plan.execution_steps.clone()
        };

        for step in &execution_steps {
            match step {
                ExecutionStep::SSTableScan {
                    table,
                    predicates,
                    projection,
                    ..
                } => {
                    let rows = self
                        .execute_sstable_scan(table, predicates, projection, &mut context)
                        .await?;
                    intermediate_results = rows;
                }
                ExecutionStep::Filter { expression, .. } => {
                    intermediate_results = self
                        .execute_filter(intermediate_results, expression, &mut context)
                        .await?;
                }
                ExecutionStep::Sort { order_by, .. } => {
                    intermediate_results = self
                        .execute_sort(intermediate_results, order_by, &mut context)
                        .await?;
                }
                ExecutionStep::Aggregate { plan: agg_plan, .. } => {
                    intermediate_results = self
                        .execute_aggregation(intermediate_results, agg_plan, &mut context)
                        .await?;
                }
                ExecutionStep::PerPartitionLimit { count } => {
                    intermediate_results =
                        Self::execute_per_partition_limit(intermediate_results, *count);
                }
                ExecutionStep::Limit { count, offset } => {
                    intermediate_results = self
                        .execute_limit(intermediate_results, *count, *offset, &mut context)
                        .await?;
                }
                ExecutionStep::Project { columns } => {
                    intermediate_results = self
                        .execute_projection(intermediate_results, columns, &mut context)
                        .await?;
                }
            }
        }

        let total_rows = intermediate_results.len() as u64;

        // CRITICAL FIX (Issue #129/#140): Populate metadata.columns for SELECT *
        // When SELECT * is used and no schema was found, context.columns is empty.
        // Fall back to inferring column names from the first row's HashMap keys.
        // IMPORTANT: Must be sorted alphabetically for deterministic JSON output (Issue #129)!
        let mut columns = context.columns;
        if columns.is_empty() && !intermediate_results.is_empty() {
            // Try to resolve schema to get proper CQL types (Issue #674).
            let schema_opt = if let Some(ref from_clause) = plan.statement.from_clause {
                if let Ok(table_id) = self.extract_table_id(from_clause) {
                    let (keyspace, table_name) = parse_table_id(&table_id);
                    self._schema
                        .find_schema_by_table(&keyspace, &table_name)
                        .await
                } else {
                    None
                }
            } else {
                None
            };

            let first_row = &intermediate_results[0];
            let mut col_names: Vec<_> = first_row.values.keys().collect();
            col_names.sort(); // Sort alphabetically for deterministic ordering (Issue #129)

            let table_name_for_meta = schema_opt
                .as_ref()
                .map(|s| format!("{}.{}", s.keyspace, s.table));

            for (idx, col_name) in col_names.iter().enumerate() {
                // Look up CQL type from schema; derive flat DataType from it (Issue #674).
                let cql_type_opt = schema_opt.as_ref().and_then(|schema| {
                    schema
                        .columns
                        .iter()
                        .find(|c| c.name.as_str() == col_name.as_str())
                        .and_then(|c| parse_cql_type_str(&c.data_type))
                });

                let data_type = cql_type_opt
                    .as_ref()
                    .map(cql_type_to_data_type)
                    .unwrap_or(crate::types::DataType::Text);

                let mut col_info = ColumnInfo {
                    name: (*col_name).clone(),
                    data_type,
                    nullable: true,
                    position: idx,
                    table_name: table_name_for_meta.clone(),
                    cql_type: None,
                };
                if let Some(cql_type) = cql_type_opt {
                    col_info = col_info.with_cql_type(cql_type);
                }
                columns.push(col_info);
            }
        }

        Ok(QueryResult {
            rows: intermediate_results,
            rows_affected: total_rows, // Use actual number of rows returned
            execution_time_ms: 0,      // Will be set by the engine
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(total_rows),
                plan_info: None,
                performance: Default::default(),
                warnings: vec![],
                // Issue #960: surface the access path the SSTable-scan step chose
                // on the result from PER-QUERY state (not the global probe), so a
                // concurrent SELECT cannot overwrite it between record() and here.
                access_path: context.access_path.clone(),
            },
        })
    }

    /// Execute an optimized query plan with streaming results (Issue #280)
    ///
    /// Instead of materializing all rows in memory, this method returns a
    /// `QueryResultIterator` that yields rows incrementally via a bounded channel.
    /// This enables memory-efficient processing of large result sets.
    ///
    /// # Memory Budget
    ///
    /// With default `StreamingConfig::buffer_size` of 1024 rows and ~1KB avg row size:
    /// - Channel buffer: ~1MB in flight
    /// - Background task: minimal overhead
    /// - Total streaming overhead: ~1-2MB (well within 128MB target)
    ///
    /// # Limitations
    ///
    /// Currently supports:
    /// - SSTableScan with predicates (streaming)
    /// - Filter/Limit/Project (applied during scan)
    ///
    /// `LIMIT` (and `OFFSET`, when present in the plan) is enforced by the
    /// streaming producer (`execute_streaming_background`): it skips `OFFSET`
    /// matches and stops scanning once `count` rows have been sent, so a
    /// `LIMIT N` query yields exactly `N` rows without materializing the rest
    /// (Issue #581).
    ///
    /// For ORDER BY/GROUP BY/DISTINCT, falls back to full execution then streams results.
    pub async fn execute_streaming(
        &self,
        plan: OptimizedQueryPlan,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        // Issue #960: clear the global access-path probe so a stale value from a
        // previous query cannot satisfy a test assertion against this one.
        crate::query::access_path::reset();

        // Check if query requires full materialization (ORDER BY, GROUP BY, aggregates)
        if self.requires_materialization(&plan) {
            log::info!("Query requires materialization (ORDER BY/GROUP BY/aggregates), using execute-then-stream");
            return self.execute_and_stream(plan, config).await;
        }

        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), fall back to execute
            return self.execute_and_stream(plan, config).await;
        };

        let columns = self.get_result_columns(&plan.statement).await?;

        // Create bounded channel for backpressure
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // Determine execution steps
        let execution_steps = if plan.execution_steps.is_empty() {
            vec![ExecutionStep::SSTableScan {
                table: table_id.clone(),
                predicates: vec![],
                projection: columns.iter().map(|c| c.name.clone()).collect(),
            }]
        } else {
            plan.execution_steps.clone()
        };

        // FINDING 1 (roborev, Issue #955 follow-up): synchronous preconditions
        // that should FAIL the query must be checked BEFORE spawning the
        // streaming task. Errors raised inside `execute_streaming_background`
        // are only logged by the spawn closure (the channel then closes), so the
        // caller would receive an apparently-successful iterator that yields zero
        // rows — silently hiding an invalid `token(...)` query. Validating here
        // surfaces the error synchronously from `execute_streaming`, matching the
        // materializing `execute()` path. The schema must be resolved before the
        // spawn for this, so we resolve it per scan step here.
        for step in &execution_steps {
            if let ExecutionStep::SSTableScan {
                table, predicates, ..
            } = step
            {
                let (keyspace, table_name) = parse_table_id(table);
                let schema_opt = self
                    ._schema
                    .find_schema_by_table(&keyspace, &table_name)
                    .await;
                validate_token_predicates(predicates, schema_opt.as_ref())?;
            }
        }

        // Clone what we need for the background task
        let storage = Arc::clone(&self.storage);
        let schema_manager = Arc::clone(&self._schema);
        let buffer_size = config.buffer_size;

        // Spawn background task to stream rows
        tokio::spawn(async move {
            if let Err(e) = Self::execute_streaming_background(
                storage,
                schema_manager,
                table_id,
                execution_steps,
                tx,
                buffer_size,
            )
            .await
            {
                log::error!("Streaming execution error: {}", e);
                // Error is logged; channel will close and consumer will see None
            }
        });

        // Create metadata for the iterator
        let metadata = QueryMetadata {
            columns,
            total_rows: None, // Unknown for streaming
            plan_info: None,
            performance: Default::default(),
            warnings: vec![],
            // Issue #960: the streaming scan runs in the spawned task above, so the
            // access path is not yet recorded when this iterator is constructed.
            // Streaming surfaces report the path via the global probe
            // (`crate::query::access_path::last()`) after at least one row is
            // pulled, not on the iterator metadata.
            access_path: None,
        };

        Ok(QueryResultIterator::new(rx, metadata))
    }

    /// Check if query plan requires full materialization before streaming
    fn requires_materialization(&self, plan: &OptimizedQueryPlan) -> bool {
        for step in &plan.execution_steps {
            match step {
                ExecutionStep::Sort { .. } => return true,
                ExecutionStep::Aggregate { .. } => return true,
                _ => {}
            }
        }

        // Check for DISTINCT
        if matches!(plan.statement.select_clause, SelectClause::Distinct(_)) {
            return true;
        }

        // Issue #693: WRITETIME()/TTL() expressions require full materialisation
        // because the streaming background task only emits raw scan rows without
        // applying the WRITETIME/TTL projection (cell metadata extraction and
        // value computation).  Falling back to execute_and_stream ensures the
        // complete execute() path runs, which correctly populates writetime(col)/
        // ttl(col) keys in each row's values map.
        select_has_writetime_ttl(&plan.statement)
    }

    /// Fallback: Execute query fully, then stream the results
    async fn execute_and_stream(
        &self,
        plan: OptimizedQueryPlan,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        // Execute full query
        let result = self.execute(plan).await?;

        // Create channel to stream results
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // Spawn task to send rows through channel
        tokio::spawn(async move {
            for row in result.rows {
                if tx.send(Ok(row)).await.is_err() {
                    break; // Consumer dropped
                }
            }
            // Channel closes automatically when tx drops
        });

        Ok(QueryResultIterator::new(rx, result.metadata))
    }

    /// Background task: Execute streaming scan and send rows through channel
    async fn execute_streaming_background(
        storage: Arc<StorageEngine>,
        schema_manager: Arc<SchemaManager>,
        _table_id: TableId,
        execution_steps: Vec<ExecutionStep>,
        tx: mpsc::Sender<Result<QueryRow>>,
        buffer_size: usize,
    ) -> Result<()> {
        // Issue #581: LIMIT/OFFSET must be enforced by the producer in the
        // streaming path. The `ExecutionStep::Limit` arm previously only logged a
        // message and relied on a consumer that never applied it, so
        // `execute_streaming` yielded the full result set regardless of LIMIT.
        // Extract the bound up front (steps are ordered with Limit after the scan)
        // and stop sending once it is satisfied — mirroring `execute_limit`
        // (drain OFFSET, then truncate to `count`) row-by-row so the producer
        // stops scanning early.
        let limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::Limit { count, offset } => Some((*count, offset.unwrap_or(0))),
            _ => None,
        });
        let (limit_count, mut offset_remaining) = match limit {
            Some((count, offset)) => (Some(count), offset),
            None => (None, 0),
        };

        // A `LIMIT 0` means no rows can ever be sent; return before scanning.
        if limit_count == Some(0) {
            return Ok(());
        }

        // Issue #757: PER PARTITION LIMIT caps rows per partition before the
        // query-wide LIMIT/OFFSET. The scan yields rows grouped by partition
        // key, so we track the current partition (by its raw key bytes) and
        // reset the counter at each boundary.
        let per_partition_limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::PerPartitionLimit { count } => Some(*count),
            _ => None,
        });
        let mut current_partition: Option<Vec<u8>> = None;
        let mut partition_count: u64 = 0;

        let mut sent: u64 = 0;

        for step in &execution_steps {
            match step {
                ExecutionStep::SSTableScan {
                    table,
                    predicates,
                    projection,
                    ..
                } => {
                    let (keyspace, table_name) = parse_table_id(table);
                    let schema_opt = schema_manager
                        .find_schema_by_table(&keyspace, &table_name)
                        .await;

                    // FINDING 2 (Issue #955 follow-up): reject a `token(...)` whose
                    // columns are not the full partition key in declared order
                    // before scanning (same rule as the materializing path).
                    validate_token_predicates(predicates, schema_opt.as_ref())?;

                    // Issue #949: a fully-constrained `WHERE pk = ?` is served by a
                    // partition-targeted lookup that prunes SSTables via bloom/BTI,
                    // instead of streaming a scan over every SSTable. The resulting
                    // rows are sent through the same per-row pipeline below
                    // (predicates, PER PARTITION LIMIT, OFFSET, LIMIT). Note
                    // `scan_partition` reconciles across SSTable generations like the
                    // materializing `scan()` (last-write-wins + tombstone shadowing),
                    // which is the authoritative read semantics; it does not merely
                    // mirror `scan_stream`'s per-key merge.
                    let lookup = classify_partition_lookup(predicates, schema_opt.as_ref());
                    if let PartitionLookupOutcome::Targeted(ref pk_bytes) = lookup {
                        // Issue #960: the streaming analogue of the materializing
                        // partition-targeted lookup.
                        crate::query::access_path::record(AccessPath::StreamingPartitionLookup);
                        let rows = storage
                            .scan_partition(table, pk_bytes, schema_opt.as_ref())
                            .await?;
                        for (key, value) in rows {
                            let part_sig = per_partition_limit.map(|_| key.0.clone());
                            let Some(row) =
                                build_row_from_scan(key, value, projection, schema_opt.as_ref())
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                if current_partition.as_deref() != Some(sig.as_slice()) {
                                    current_partition = Some(sig);
                                    partition_count = 0;
                                }
                                if partition_count >= cap {
                                    continue;
                                }
                                partition_count += 1;
                            }
                            if offset_remaining > 0 {
                                offset_remaining -= 1;
                                continue;
                            }
                            if tx.send(Ok(row)).await.is_err() {
                                return Ok(());
                            }
                            sent += 1;
                            if let Some(count) = limit_count {
                                if sent >= count {
                                    return Ok(());
                                }
                            }
                        }
                        // This SSTableScan step is fully served by the lookup.
                        continue;
                    }

                    // Issue #955: `WHERE pk IN (...)` over the complete key is the
                    // union of N partition-targeted lookups. Gather them, sort by
                    // token to match full-scan order, then drive the same per-row
                    // pipeline (predicates, PER PARTITION LIMIT, OFFSET, LIMIT).
                    if let PartitionLookupOutcome::MultiTargeted(ref pk_keys) = lookup {
                        crate::query::access_path::record(AccessPath::MultiPartitionLookup);
                        let mut combined = Vec::new();
                        for pk_bytes in pk_keys {
                            let rows = storage
                                .scan_partition(table, pk_bytes, schema_opt.as_ref())
                                .await?;
                            combined.extend(rows);
                        }
                        sort_rows_by_token(&mut combined);
                        for (key, value) in combined {
                            let part_sig = per_partition_limit.map(|_| key.0.clone());
                            let Some(row) =
                                build_row_from_scan(key, value, projection, schema_opt.as_ref())
                            else {
                                continue;
                            };
                            if !evaluate_predicates(&row, predicates)? {
                                continue;
                            }
                            if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                                if current_partition.as_deref() != Some(sig.as_slice()) {
                                    current_partition = Some(sig);
                                    partition_count = 0;
                                }
                                if partition_count >= cap {
                                    continue;
                                }
                                partition_count += 1;
                            }
                            if offset_remaining > 0 {
                                offset_remaining -= 1;
                                continue;
                            }
                            if tx.send(Ok(row)).await.is_err() {
                                return Ok(());
                            }
                            sent += 1;
                            if let Some(count) = limit_count {
                                if sent >= count {
                                    return Ok(());
                                }
                            }
                        }
                        // This SSTableScan step is fully served by the lookups.
                        continue;
                    }

                    // Issue #960: the streaming path did not take a targeted
                    // lookup; report the honest fallback reason. `lookup` is the
                    // `Fallback` arm here (the `Targeted`/`MultiTargeted` arms
                    // returned above via `continue`).
                    if let PartitionLookupOutcome::Fallback(reason) = lookup {
                        crate::query::access_path::record(AccessPath::FallbackFullScan { reason });
                    }

                    // Issue #790: pull rows lazily from a bounded streaming scan
                    // instead of materializing the full result `Vec`. The reader
                    // parses one entry at a time into this channel, so live heap
                    // stays bounded by `buffer_size` rather than O(result rows).
                    let mut scan_stream = storage
                        .scan_stream(table, None, None, schema_opt.as_ref(), buffer_size)
                        .await?;

                    while let Some(item) = scan_stream.recv().await {
                        let (key, value) = item?;
                        // Capture the partition key bytes before `key` is moved
                        // into row construction (only when needed).
                        let part_sig = per_partition_limit.map(|_| key.0.clone());
                        let Some(row) =
                            build_row_from_scan(key, value, projection, schema_opt.as_ref())
                        else {
                            continue;
                        };

                        if !evaluate_predicates(&row, predicates)? {
                            continue;
                        }

                        // Apply PER PARTITION LIMIT: cap matching rows per
                        // partition, before OFFSET/LIMIT (Cassandra semantics).
                        if let (Some(cap), Some(sig)) = (per_partition_limit, part_sig) {
                            if current_partition.as_deref() != Some(sig.as_slice()) {
                                current_partition = Some(sig);
                                partition_count = 0;
                            }
                            if partition_count >= cap {
                                continue;
                            }
                            partition_count += 1;
                        }

                        // Apply OFFSET: skip the first `offset_remaining` matches.
                        if offset_remaining > 0 {
                            offset_remaining -= 1;
                            continue;
                        }

                        // Send row through channel (with backpressure). Consumer drop ends the scan.
                        if tx.send(Ok(row)).await.is_err() {
                            return Ok(());
                        }
                        sent += 1;

                        // Apply LIMIT: stop scanning once `count` rows have been
                        // sent. Dropping `scan_stream` here signals the producer
                        // (via a closed channel) to stop parsing early.
                        if let Some(count) = limit_count {
                            if sent >= count {
                                return Ok(());
                            }
                        }
                    }
                }
                ExecutionStep::Limit { .. } | ExecutionStep::PerPartitionLimit { .. } => {
                    // Enforced inline during the scan above (see the bounds
                    // extracted before the loop).
                }
                // Projection and predicate filtering are pushed into SSTableScan above.
                ExecutionStep::Project { .. } | ExecutionStep::Filter { .. } => {}
                _ => {
                    log::warn!("Streaming execution: skipping unsupported step {:?}", step);
                }
            }
        }

        Ok(())
    }

    /// Execute SSTable scan with predicate pushdown.
    ///
    /// Per-row work (build row, decode partition key, evaluate predicates) is
    /// handled by the free helpers `build_row_from_scan` and
    /// `evaluate_predicates`, which are shared with the streaming background
    /// task to keep the two execution paths in lockstep.
    async fn execute_sstable_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const MAX_RESULTS: usize = 1_000_000;

        log::info!(
            "Executing SSTableScan: table=\"{}\", predicates={:?}, include_cell_metadata={}",
            table,
            predicates,
            context.projection_flags.include_cell_metadata,
        );

        let (keyspace, table_name) = parse_table_id(table);
        let schema_opt = self
            ._schema
            .find_schema_by_table(&keyspace, &table_name)
            .await;

        match schema_opt.as_ref() {
            Some(schema) => log::info!(
                "Found schema for {}.{} with {} columns",
                schema.keyspace,
                schema.table,
                schema.columns.len()
            ),
            None => log::info!(
                "No schema found for {}.{}, proceeding without schema-aware parsing",
                keyspace.as_deref().unwrap_or("unknown"),
                table_name
            ),
        }

        // FINDING 2 (Issue #955 follow-up): a `token(...)` predicate is evaluated
        // by hashing the row's raw partition key, so its argument columns MUST be
        // the full partition key in declared order or the result is silently
        // wrong. Reject (Cassandra-style) before scanning/evaluating.
        validate_token_predicates(predicates, schema_opt.as_ref())?;

        // Issue #693: When WRITETIME(col) or TTL(col) is in the SELECT, use the
        // metadata-carrying scan so per-cell timestamps reach the QueryRow.
        let mut results = Vec::new();
        if context.projection_flags.include_cell_metadata {
            // Issue #960: the WRITETIME/TTL metadata path always full-scans today
            // (it passes `None, None` to `scan_with_cell_metadata`). Report this
            // honestly as a fallback so it cannot masquerade as a targeted lookup;
            // #962 will flip it to `AccessPath::MetadataPartitionLookup` once the
            // metadata scan accepts a partition-targeted lookup.
            let metadata_path = AccessPath::FallbackFullScan {
                reason: FallbackReason::MetadataScanPath,
            };
            context.access_path = Some(metadata_path.clone());
            crate::query::access_path::record(metadata_path);
            let scan_results = self
                .storage
                .scan_with_cell_metadata(table, None, None, None, schema_opt.as_ref())
                .await?;

            log::info!("Scan (with metadata) returned {} rows", scan_results.len());

            for (key, value, cell_meta) in scan_results {
                context.rows_processed += 1;

                let Some(mut row) =
                    build_row_from_scan(key, value, projection, schema_opt.as_ref())
                else {
                    continue;
                };

                // Attach per-cell metadata so evaluate_writetime_ttl can read it.
                if !cell_meta.is_empty() {
                    row.set_cell_metadata(cell_meta);
                }

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }

                if results.len() > MAX_RESULTS {
                    return Err(Error::query_execution(
                        "Result set too large, consider adding LIMIT".to_string(),
                    ));
                }
            }
        } else {
            // Issue #949: a fully-constrained `WHERE pk = ?` is served by a
            // partition-targeted lookup that prunes SSTables via bloom/BTI and only
            // parses the candidates, instead of scanning every SSTable for the
            // table. Falls back to a full scan when the partition key isn't fully
            // pinned or can't be encoded. The per-row predicate evaluation below is
            // unchanged, so clustering predicates and the pk equality itself are
            // still applied (and any over-inclusion is filtered out).
            let scan_results = match classify_partition_lookup(predicates, schema_opt.as_ref()) {
                PartitionLookupOutcome::Targeted(pk_bytes) => {
                    log::info!(
                        "SSTableScan: partition-key point lookup (key len={}) for \"{}\"",
                        pk_bytes.len(),
                        table
                    );
                    // Issue #960: a fully-constrained `WHERE pk = ?` served by a
                    // partition-targeted lookup that prunes SSTables.
                    context.access_path = Some(AccessPath::PartitionLookup);
                    crate::query::access_path::record(AccessPath::PartitionLookup);
                    self.storage
                        .scan_partition(table, &pk_bytes, schema_opt.as_ref())
                        .await?
                }
                PartitionLookupOutcome::MultiTargeted(pk_keys) => {
                    log::info!(
                        "SSTableScan: multi-partition lookup ({} keys) for \"{}\"",
                        pk_keys.len(),
                        table
                    );
                    // Issue #955/#960: `WHERE pk IN (...)` over the complete key
                    // is the union of N independent partition-targeted lookups,
                    // each of which prunes SSTables. Report MultiPartitionLookup.
                    context.access_path = Some(AccessPath::MultiPartitionLookup);
                    crate::query::access_path::record(AccessPath::MultiPartitionLookup);
                    let mut combined = Vec::new();
                    for pk_bytes in &pk_keys {
                        let rows = self
                            .storage
                            .scan_partition(table, pk_bytes, schema_opt.as_ref())
                            .await?;
                        combined.extend(rows);
                    }
                    // Order the union to equal a full scan filtered to these keys:
                    // partitions are stored token-ordered, so sort the combined
                    // rows by (partition token, raw key bytes). A *stable* sort
                    // keeps each partition's clustering order (rows for one key
                    // arrive contiguously from one `scan_partition`) intact.
                    sort_rows_by_token(&mut combined);
                    combined
                }
                PartitionLookupOutcome::Fallback(reason) => {
                    // Issue #960: report the honest reason a full scan was chosen.
                    context.access_path = Some(AccessPath::FallbackFullScan { reason });
                    crate::query::access_path::record(AccessPath::FallbackFullScan { reason });
                    self.storage
                        .scan(table, None, None, None, schema_opt.as_ref())
                        .await?
                }
            };

            log::info!("Scan returned {} rows", scan_results.len());

            for (key, value) in scan_results {
                context.rows_processed += 1;

                // build_row_from_scan returns None for tombstoned/null rows (Issue #191).
                let Some(row) = build_row_from_scan(key, value, projection, schema_opt.as_ref())
                else {
                    continue;
                };

                if evaluate_predicates(&row, predicates)? {
                    results.push(row);
                }

                if results.len() > MAX_RESULTS {
                    return Err(Error::query_execution(
                        "Result set too large, consider adding LIMIT".to_string(),
                    ));
                }
            }
        }

        Ok(results)
    }

    /// Execute filtering step
    async fn execute_filter(
        &self,
        rows: Vec<QueryRow>,
        filter_expr: &WhereExpression,
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut filtered_rows = Vec::new();

        for row in rows {
            if self.evaluate_where_expression(filter_expr, &row)? {
                filtered_rows.push(row);
            }
            context.rows_processed += 1;
        }

        Ok(filtered_rows)
    }

    /// Evaluate WHERE expression against a row
    fn evaluate_where_expression(&self, expr: &WhereExpression, row: &QueryRow) -> Result<bool> {
        match expr {
            WhereExpression::Comparison(comp) => self.evaluate_comparison(comp, row),
            WhereExpression::And(exprs) => {
                for expr in exprs {
                    if !self.evaluate_where_expression(expr, row)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            WhereExpression::Or(exprs) => {
                for expr in exprs {
                    if self.evaluate_where_expression(expr, row)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            WhereExpression::Not(expr) => Ok(!self.evaluate_where_expression(expr, row)?),
            WhereExpression::Parentheses(expr) => self.evaluate_where_expression(expr, row),
        }
    }

    /// Evaluate comparison expression. Operators that need a single right
    /// operand share one `evaluate` call; IN/LIKE/IS NULL fall through to
    /// their custom branches.
    fn evaluate_comparison(&self, comp: &ComparisonExpression, row: &QueryRow) -> Result<bool> {
        use ComparisonOperator::*;

        let left_value = self.evaluate_select_expression(&comp.left, row)?;

        // Fast path for null tests, which ignore the right side.
        match comp.operator {
            IsNull => return Ok(left_value.is_null()),
            IsNotNull => return Ok(!left_value.is_null()),
            _ => {}
        }

        match (&comp.operator, &comp.right) {
            (
                op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan
                | GreaterThanOrEqual),
                ComparisonRightSide::Value(right_expr),
            ) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                let result = match op {
                    Equal => values_equal(&left_value, &right_value),
                    NotEqual => !values_equal(&left_value, &right_value),
                    LessThan => try_compare_values(&left_value, &right_value)?.is_lt(),
                    LessThanOrEqual => try_compare_values(&left_value, &right_value)?.is_le(),
                    GreaterThan => try_compare_values(&left_value, &right_value)?.is_gt(),
                    GreaterThanOrEqual => try_compare_values(&left_value, &right_value)?.is_ge(),
                    _ => unreachable!("guarded by outer match"),
                };
                Ok(result)
            }
            (In, ComparisonRightSide::ValueList(value_exprs)) => {
                for value_expr in value_exprs {
                    let value = self.evaluate_select_expression(value_expr, row)?;
                    if left_value == value {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            (Like, ComparisonRightSide::Value(pattern_expr)) => {
                let pattern = self.evaluate_select_expression(pattern_expr, row)?;
                if let (Value::Text(text), Value::Text(pattern_str)) = (&left_value, &pattern) {
                    Ok(self.match_like_pattern(text, pattern_str))
                } else {
                    Ok(false)
                }
            }
            _ => Err(Error::query_execution(
                "Unsupported comparison operator".to_string(),
            )),
        }
    }

    /// Evaluate SELECT expression against a row
    fn evaluate_select_expression(&self, expr: &SelectExpression, row: &QueryRow) -> Result<Value> {
        match expr {
            SelectExpression::Column(col_ref) => {
                row.values.get(&col_ref.column).cloned().ok_or_else(|| {
                    Error::query_execution(format!("Column not found: {}", col_ref.column))
                })
            }
            SelectExpression::Literal(value) => Ok(value.clone()),
            // Issue #961: a `?` placeholder must be bound to a concrete value
            // before execution. Reaching here means binding was skipped, which is
            // an internal logic error rather than user input — report it instead
            // of panicking.
            SelectExpression::BindMarker(idx) => Err(Error::query_execution(format!(
                "Unbound parameter placeholder ?{idx} reached execution; \
                 parameters must be bound before the query runs"
            ))),
            SelectExpression::CollectionAccess(access) => {
                self.evaluate_collection_access(access, row)
            }
            SelectExpression::Arithmetic(arith) => {
                let left = self.evaluate_select_expression(&arith.left, row)?;
                let right = self.evaluate_select_expression(&arith.right, row)?;
                self.evaluate_arithmetic(&arith.operator, left, right)
            }
            SelectExpression::Aliased(expr, _) => self.evaluate_select_expression(expr, row),
            SelectExpression::Aggregate(_) => {
                // Aggregate expressions should not be evaluated at row level
                // They should only be processed during the aggregation step
                Err(Error::query_execution(
                    "Aggregate expressions should be processed during aggregation step, not row evaluation".to_string(),
                ))
            }
            SelectExpression::Function(_) => {
                // Function expressions not yet implemented
                Err(Error::query_execution(
                    "Function expressions not yet implemented".to_string(),
                ))
            }
            // Issue #692: evaluate WRITETIME(col) / TTL(col) against the per-cell
            // metadata carrier threaded by the reader when `ProjectionFlags::include_cell_metadata`
            // is set. Returns `Value::Null` when metadata is absent (e.g. no schema-aware
            // read path or the column was a partition-key column with no cell header).
            SelectExpression::WriteTimeTtl(call) => {
                let now_secs = self.clock.now_seconds();
                Ok(evaluate_writetime_ttl(call, row, now_secs))
            }
        }
    }

    /// Evaluate collection access operations (`list[idx]`, `map['key']`,
    /// `value IN set_column`).
    fn evaluate_collection_access(
        &self,
        access: &CollectionAccessExpression,
        row: &QueryRow,
    ) -> Result<Value> {
        let lookup_column = |col: &ColumnRef| -> Result<&Value> {
            row.values
                .get(&col.column)
                .ok_or_else(|| Error::query_execution(format!("Column not found: {}", col.column)))
        };

        match access {
            CollectionAccessExpression::ListIndex(col_ref, index_expr) => {
                let list_value = lookup_column(col_ref)?;
                let index_value = self.evaluate_select_expression(index_expr, row)?;

                let (Value::List(list), Value::Integer(index)) = (list_value, &index_value) else {
                    return Err(Error::query_execution("Invalid list access".to_string()));
                };
                if *index >= 0 && (*index as usize) < list.len() {
                    Ok(list[*index as usize].clone())
                } else {
                    Ok(Value::Null)
                }
            }
            CollectionAccessExpression::MapKey(col_ref, key_expr) => {
                let map_value = lookup_column(col_ref)?;
                let key_value = self.evaluate_select_expression(key_expr, row)?;

                let Value::Map(map) = map_value else {
                    return Err(Error::query_execution("Invalid map access".to_string()));
                };
                Ok(map
                    .iter()
                    .find(|(k, _)| *k == key_value)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Null))
            }
            CollectionAccessExpression::SetContains(col_ref, value_expr) => {
                let set_value = lookup_column(col_ref)?;
                let test_value = self.evaluate_select_expression(value_expr, row)?;

                let Value::Set(set) = set_value else {
                    return Err(Error::query_execution(
                        "Invalid set contains operation".to_string(),
                    ));
                };
                Ok(Value::Boolean(set.contains(&test_value)))
            }
        }
    }

    /// Evaluate arithmetic expressions on a (left, op, right) triple.
    ///
    /// Runtime arithmetic supports same-type Integer or Float operands. Mixed
    /// types or non-numeric operands return an error. (Constant-folding
    /// arithmetic additionally accepts BigInt — see
    /// `evaluate_constant_expression`.)
    fn evaluate_arithmetic(
        &self,
        op: &ArithmeticOperator,
        left: Value,
        right: Value,
    ) -> Result<Value> {
        match (&left, &right) {
            (Value::Integer(_), Value::Integer(_)) | (Value::Float(_), Value::Float(_)) => {
                eval_arithmetic(op, left, right)
            }
            _ => Err(Error::query_execution(
                "Incompatible types for arithmetic".to_string(),
            )),
        }
    }

    /// Simple LIKE pattern matching. The CQL pattern syntax (`%`, `_`) is
    /// translated by `like_pattern_to_regex` before compilation.
    fn match_like_pattern(&self, text: &str, pattern: &str) -> bool {
        regex::Regex::new(&like_pattern_to_regex(pattern))
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }

    /// Execute sorting step
    async fn execute_sort(
        &self,
        mut rows: Vec<QueryRow>,
        order_by: &OrderByClause,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        rows.sort_by(|a, b| {
            for item in &order_by.items {
                let a_val = self
                    .evaluate_select_expression(&item.expression, a)
                    .unwrap_or(Value::Null);
                let b_val = self
                    .evaluate_select_expression(&item.expression, b)
                    .unwrap_or(Value::Null);

                let ordering = match item.direction {
                    SortDirection::Ascending => compare_values_ordering(&a_val, &b_val),
                    SortDirection::Descending => compare_values_ordering(&b_val, &a_val),
                };
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Execute the aggregation step. Splits naturally into three phases:
    /// build group key, accumulate per-aggregate state, then finalize each
    /// group into a result row.
    async fn execute_aggregation(
        &self,
        rows: Vec<QueryRow>,
        agg_plan: &AggregationPlan,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const PER_ROW_MEMORY_ESTIMATE_BYTES: usize = 100;
        const DEFAULT_AGGREGATION_MEMORY_LIMIT: usize = 512 * 1024 * 1024;

        let mut agg_state = AggregationState {
            groups: Vec::new(),
            memory_usage_bytes: 0,
            memory_limit_bytes: DEFAULT_AGGREGATION_MEMORY_LIMIT,
        };

        for row in rows {
            let group_key = build_group_key(&row, &agg_plan.group_by_columns);
            let group_index =
                find_or_init_group(&mut agg_state.groups, group_key, &agg_plan.aggregates);
            let group_aggregates = &mut agg_state.groups[group_index].1;

            for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
                update_aggregate(&mut group_aggregates[i], agg_comp, &row);
            }

            agg_state.memory_usage_bytes += PER_ROW_MEMORY_ESTIMATE_BYTES;
            if agg_state.memory_usage_bytes > agg_state.memory_limit_bytes {
                return Err(Error::query_execution(
                    "Aggregation memory limit exceeded".to_string(),
                ));
            }
        }

        let result_rows = agg_state
            .groups
            .into_iter()
            .map(|(group_key, group_aggregates)| {
                finalize_group(group_key, group_aggregates, agg_plan)
            })
            .collect();

        Ok(result_rows)
    }

    /// Execute PER PARTITION LIMIT: keep at most `count` rows per partition,
    /// preserving order (Issue #757). Counts are keyed on the partition (raw key
    /// bytes) rather than tracking only the most recent partition, so the cap
    /// holds even when a partition's rows are not contiguous — e.g. when an
    /// upstream `ORDER BY` interleaves rows from different partitions (roborev
    /// job 38).
    fn execute_per_partition_limit(rows: Vec<QueryRow>, count: u64) -> Vec<QueryRow> {
        let mut out = Vec::with_capacity(rows.len());
        let mut counts: HashMap<Vec<u8>, u64> = HashMap::new();
        for row in rows {
            let seen = counts.entry(row.key.0.clone()).or_insert(0);
            if *seen < count {
                *seen += 1;
                out.push(row);
            }
        }
        out
    }

    /// Execute limit step (apply OFFSET then truncate to LIMIT).
    async fn execute_limit(
        &self,
        mut rows: Vec<QueryRow>,
        count: u64,
        offset: Option<u64>,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let start_index = offset.unwrap_or(0) as usize;
        if start_index >= rows.len() {
            return Ok(Vec::new());
        }
        rows.drain(..start_index);
        rows.truncate(count as usize);
        Ok(rows)
    }

    /// Execute projection step
    async fn execute_projection(
        &self,
        rows: Vec<QueryRow>,
        columns: &[SelectExpression],
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut projected_rows = Vec::new();

        for row in rows {
            let mut projected_values = HashMap::new();

            for (i, expr) in columns.iter().enumerate() {
                let value = self.evaluate_select_expression(expr, &row)?;
                // Issue #692: WriteTimeTtl expressions use Cassandra-convention column names.
                let column_name = match expr {
                    SelectExpression::Column(col_ref) => col_ref.column.clone(),
                    SelectExpression::Aliased(_, alias) => alias.clone(),
                    SelectExpression::WriteTimeTtl(call) => writetime_ttl_column_name(call),
                    _ => format!("col_{i}"),
                };
                projected_values.insert(column_name, value);
            }

            projected_rows.push(QueryRow {
                values: projected_values,
                key: RowKey::new(vec![]),
                metadata: Default::default(),
                cell_metadata: None,
            });
        }

        Ok(projected_rows)
    }

    /// Execute a query without FROM clause (constant expressions like SELECT 1)
    async fn execute_constant_query(
        &self,
        statement: &SelectStatement,
        _context: &ExecutionContext,
    ) -> Result<QueryResult> {
        let mut values = HashMap::new();
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                return Err(Error::query_execution(
                    "SELECT * requires a FROM clause".to_string(),
                ));
            }
            SelectClause::Columns(expressions) | SelectClause::Distinct(expressions) => {
                for (i, expr) in expressions.iter().enumerate() {
                    let (value, column_name) = self.evaluate_constant_expression(expr)?;
                    let key = column_name.unwrap_or_else(|| format!("column_{}", i));
                    values.insert(key.clone(), value);
                    columns.push(ColumnInfo {
                        name: key,
                        data_type: crate::types::DataType::Text, // Constant expressions have no schema type
                        nullable: true,
                        position: i,
                        table_name: None, // No table for constant expressions
                        cql_type: None,
                    });
                }
            }
        }

        let row = QueryRow::with_values(RowKey::new(vec![1]), values);

        Ok(QueryResult {
            rows: vec![row],
            rows_affected: 1, // Constant queries return 1 row
            execution_time_ms: 0,
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(1),
                plan_info: None,
                performance: crate::query::result::PerformanceMetrics::default(),
                warnings: Vec::new(),
                // Constant queries (e.g. `SELECT 1`) touch no SSTable.
                access_path: None,
            },
        })
    }

    /// Evaluate a constant expression (no table access needed).
    ///
    /// Accepts literals, aliases, and arithmetic over same-typed Integer,
    /// BigInt, or Float operands. Modulo is restricted to integers (matching
    /// the original behaviour). Error messages are kept verbatim from the
    /// legacy implementation so any callers asserting on them still pass.
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_constant_expression(
        &self,
        expr: &SelectExpression,
    ) -> Result<(Value, Option<String>)> {
        match expr {
            SelectExpression::Literal(value) => Ok((value.clone(), None)),
            SelectExpression::Aliased(inner_expr, alias) => {
                let (value, _) = self.evaluate_constant_expression(inner_expr)?;
                Ok((value, Some(alias.clone())))
            }
            SelectExpression::Arithmetic(arith) => {
                let (left_val, _) = self.evaluate_constant_expression(&arith.left)?;
                let (right_val, _) = self.evaluate_constant_expression(&arith.right)?;
                let result = const_arithmetic(&arith.operator, left_val, right_val)?;
                Ok((result, None))
            }
            _ => Err(Error::query_execution(
                "Expression type not supported in constant queries".to_string(),
            )),
        }
    }

    /// Extract a `TableId` from a FROM clause. Cassandra CQL has no JOINs, so
    /// either form (bare table or aliased table) yields the same result.
    fn extract_table_id(&self, from_clause: &FromClause) -> Result<TableId> {
        match from_clause {
            FromClause::Table(table_id) | FromClause::TableAlias(table_id, _) => {
                Ok(table_id.clone())
            }
        }
    }

    async fn get_result_columns(&self, statement: &SelectStatement) -> Result<Vec<ColumnInfo>> {
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                // For SELECT *, look up the schema to get column names and CQL types.
                // This is needed for streaming mode where we can't wait for the first row.
                if let Some(ref from_clause) = statement.from_clause {
                    let table_id = self.extract_table_id(from_clause)?;
                    let (keyspace_opt, table_name) = parse_table_id(&table_id);

                    // Look up schema from SchemaManager
                    if let Some(schema) = self
                        ._schema
                        .find_schema_by_table(&keyspace_opt, &table_name)
                        .await
                    {
                        // Collect all schema columns (sorted alphabetically for determinism)
                        let mut schema_cols: Vec<&crate::schema::Column> =
                            schema.columns.iter().collect();
                        schema_cols.sort_by_key(|c| c.name.as_str());

                        let keyspace_str = keyspace_opt.as_deref().unwrap_or("");
                        let table_name_str = format!("{}.{}", keyspace_str, table_name);

                        for (idx, schema_col) in schema_cols.iter().enumerate() {
                            // Parse the CQL type string into a structured CqlType (Issue #674).
                            let cql_type_opt = parse_cql_type_str(&schema_col.data_type);
                            // Derive the flat DataType from the CqlType; avoids hardcoded Text.
                            let data_type = cql_type_opt
                                .as_ref()
                                .map(cql_type_to_data_type)
                                .unwrap_or(crate::types::DataType::Text);

                            let mut col_info = ColumnInfo {
                                name: schema_col.name.clone(),
                                data_type,
                                nullable: true,
                                position: idx,
                                table_name: Some(table_name_str.clone()),
                                cql_type: None,
                            };
                            if let Some(cql_type) = cql_type_opt {
                                col_info = col_info.with_cql_type(cql_type);
                            }
                            columns.push(col_info);
                        }

                        log::debug!(
                            "SELECT * resolved {} columns from schema for {:?}.{}",
                            columns.len(),
                            keyspace_opt,
                            table_name
                        );
                    }
                    // If schema not found, columns stay empty - will be populated from first row at runtime
                }
            }
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => {
                // Try to resolve a schema for the FROM table (if present) so we can
                // attach authoritative CQL types to explicitly projected columns (Issue #674).
                let schema_opt = if let Some(ref from_clause) = statement.from_clause {
                    if let Ok(table_id) = self.extract_table_id(from_clause) {
                        let (keyspace_opt, table_name) = parse_table_id(&table_id);
                        self._schema
                            .find_schema_by_table(&keyspace_opt, &table_name)
                            .await
                    } else {
                        None
                    }
                } else {
                    None
                };

                for (i, expr) in exprs.iter().enumerate() {
                    // Issue #692: WriteTimeTtl expressions produce fixed-schema output
                    // columns with Cassandra-convention names, independent of the table schema.
                    if let SelectExpression::WriteTimeTtl(call) = expr {
                        let col_name = writetime_ttl_column_name(call);
                        let (data_type, cql_type) = match call.function {
                            // WRITETIME returns bigint (µs since epoch)
                            WriteTimeTtlFunction::WriteTime => {
                                (crate::types::DataType::BigInt, Some(CqlType::BigInt))
                            }
                            // TTL returns int (remaining seconds)
                            WriteTimeTtlFunction::Ttl => {
                                (crate::types::DataType::Integer, Some(CqlType::Int))
                            }
                        };
                        let mut col_info = ColumnInfo {
                            name: col_name,
                            data_type,
                            nullable: true, // always nullable — absent cell → NULL
                            position: i,
                            table_name: None,
                            cql_type: None,
                        };
                        if let Some(ct) = cql_type {
                            col_info = col_info.with_cql_type(ct);
                        }
                        columns.push(col_info);
                        continue;
                    }

                    let column_name = match expr {
                        SelectExpression::Column(col_ref) => col_ref.column.clone(),
                        SelectExpression::Aliased(_, alias) => alias.clone(),
                        _ => format!("col_{i}"),
                    };

                    // Look up CQL type for this column in the schema (Issue #674).
                    let cql_type_opt = schema_opt.as_ref().and_then(|schema| {
                        schema
                            .columns
                            .iter()
                            .find(|c| c.name == column_name)
                            .and_then(|c| parse_cql_type_str(&c.data_type))
                    });
                    let data_type = cql_type_opt
                        .as_ref()
                        .map(cql_type_to_data_type)
                        .unwrap_or(crate::types::DataType::Text);

                    let mut col_info = ColumnInfo {
                        name: column_name,
                        data_type,
                        nullable: true,
                        position: i,
                        table_name: None,
                        cql_type: None,
                    };
                    if let Some(cql_type) = cql_type_opt {
                        col_info = col_info.with_cql_type(cql_type);
                    }
                    columns.push(col_info);
                }
            }
        }

        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{platform::Platform, Config};
    use tempfile::TempDir;

    async fn create_test_executor() -> SelectExecutor {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        SelectExecutor::new(schema, storage)
    }

    /// Create an executor with a fixed clock (deterministic TTL tests).
    async fn create_test_executor_with_clock(now_secs: i64) -> SelectExecutor {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        SelectExecutor::with_clock(schema, storage, Arc::new(FixedClock(now_secs)))
    }

    #[test]
    fn test_value_comparison() {
        use std::cmp::Ordering;
        assert_eq!(
            try_compare_values(&Value::Integer(5), &Value::Integer(3)).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            try_compare_values(&Value::Integer(3), &Value::Integer(5)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            try_compare_values(&Value::Integer(5), &Value::Integer(5)).unwrap(),
            Ordering::Equal
        );
    }

    #[tokio::test]
    async fn test_like_pattern_matching() {
        let executor = create_test_executor().await;

        assert!(executor.match_like_pattern("hello", "h%"));
        assert!(executor.match_like_pattern("hello", "%lo"));
        assert!(executor.match_like_pattern("hello", "h_llo"));
        assert!(!executor.match_like_pattern("hello", "h_l"));
    }

    // ------------------------------------------------------------------
    // Issue #586: partition-key reconstruction on the scan path.
    // ------------------------------------------------------------------

    fn single_pk_schema(name: &str, data_type: &str) -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: name.to_string(),
                data_type: data_type.to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    /// Issue #586: a single-component TEXT partition key is stored as raw bytes
    /// with NO length prefix. `build_row_from_scan` must materialise it from the
    /// `RowKey`. Before the fix the column was silently dropped (the decoder
    /// read a phantom `u16` prefix, errored, and the error was swallowed).
    #[test]
    fn build_row_from_scan_materialises_single_text_pk() {
        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = Value::Map(vec![(
            Value::Text("name".to_string()),
            Value::Text("name-0".to_string()),
        )]);
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
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = Value::Map(vec![(Value::Text("age".to_string()), Value::Integer(0))]);
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

    /// Issue #956: a `WHERE id = <uuid-literal>` against a single UUID partition
    /// key must engage the #949 partition-targeted fast path, i.e.
    /// `classify_partition_lookup` returns `Targeted` with the raw 16-byte key.
    /// This is the unit-level evidence that the parser's new `Value::Uuid`
    /// literal flows all the way into the fast path (the e2e parity test proves
    /// the rows it returns are correct).
    #[test]
    fn classify_partition_lookup_targets_uuid_literal() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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

    /// Build a two-column composite partition-key schema (`a`, `b` in declared
    /// order). Used by the FINDING 2/3 token-validation and composite-IN tests.
    fn composite_pk_schema(
        first: (&str, &str),
        second: (&str, &str),
    ) -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![
                crate::schema::KeyColumn {
                    name: first.0.to_string(),
                    data_type: first.1.to_string(),
                    position: 0,
                },
                crate::schema::KeyColumn {
                    name: second.0.to_string(),
                    data_type: second.1.to_string(),
                    position: 1,
                },
            ],
            clustering_keys: vec![],
            columns: vec![],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    /// FINDING 2: a `token(...)` over the FULL partition key in declared order is
    /// accepted (validation passes), so the existing token fast-path/evaluation
    /// behaviour is preserved.
    #[test]
    fn validate_token_predicates_accepts_full_key_in_order() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = composite_pk_schema(("a", "int"), ("b", "int"));
        let pred = SSTablePredicate::token(
            vec!["a".to_string(), "b".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred), Some(&schema)).is_ok(),
            "token(a, b) over the full partition key in declared order must be accepted",
        );

        // Single-column key: token(id) is the full key.
        let single = single_pk_schema("id", "int");
        let pred_single = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gte,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred_single), Some(&single)).is_ok(),
            "token(id) over a single-column partition key must be accepted",
        );
    }

    /// FINDING 2: `token(non_pk_col)` must be REJECTED — `evaluate_leaf` would
    /// otherwise hash the real partition key and silently return rows for a
    /// different expression than the user wrote.
    #[test]
    fn validate_token_predicates_rejects_non_pk_column() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = single_pk_schema("id", "int");
        let pred = SSTablePredicate::token(
            vec!["not_the_pk".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred), Some(&schema)).is_err(),
            "token(non_pk_col) must be rejected, not evaluated against the real pk token",
        );
    }

    /// FINDING 2: `token(b, a)` on a `(a, b)` key — right columns, WRONG order —
    /// must be rejected (Cassandra requires declared order).
    #[test]
    fn validate_token_predicates_rejects_reordered_composite() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let schema = composite_pk_schema(("a", "int"), ("b", "int"));
        let reordered = SSTablePredicate::token(
            vec!["b".to_string(), "a".to_string()],
            SSTableFilterOp::Lt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&reordered), Some(&schema)).is_err(),
            "token(b, a) on a (a, b) key must be rejected (wrong order)",
        );

        // A strict subset (missing the second column) is also rejected.
        let subset = SSTablePredicate::token(
            vec!["a".to_string()],
            SSTableFilterOp::Lt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&subset), Some(&schema)).is_err(),
            "token(a) on a (a, b) key must be rejected (partial key)",
        );
    }

    /// FINDING 2: with no schema we cannot prove the token columns are the
    /// partition key, so any token predicate is rejected (never trusted).
    #[test]
    fn validate_token_predicates_rejects_without_schema() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let pred = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred), None).is_err(),
            "a token predicate with no schema must be rejected",
        );
        // Ordinary column predicates are unaffected by token validation.
        let col = SSTablePredicate::column("id", SSTableFilterOp::Equal, vec![Value::Integer(1)]);
        assert!(
            validate_token_predicates(std::slice::from_ref(&col), None).is_ok(),
            "non-token predicates must pass token validation even without a schema",
        );
    }

    /// FINDING 3: a composite `IN` whose cartesian product EXCEEDS the cap must
    /// fall back BEFORE materializing the product (checked arithmetic), and still
    /// reports the honest `PartitionKeyNotFullyConstrained` fallback. The product
    /// here (1000 x 1000 = 1_000_000) is far over `MAX_IN_TARGETED_LOOKUPS`;
    /// expanding it first would allocate a million combinations.
    #[test]
    fn classify_partition_lookup_composite_in_over_cap_falls_back_without_overalloc() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

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

    /// Issue #955: `evaluate_leaf` on a token predicate hashes the row's raw
    /// partition key with the canonical Murmur3 partitioner and compares against
    /// the i64 bound — matching Cassandra's token inclusivity (`>` exclusive,
    /// `>=` inclusive, etc.).
    #[test]
    fn evaluate_leaf_token_predicate_filters_by_token() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let key = b"some-partition-key".to_vec();
        let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(&key);
        let row = row_with_key(&key);

        // token >= exact token → included; token > exact token → excluded.
        let gte = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gte,
            vec![Value::BigInt(token)],
        );
        assert_eq!(evaluate_leaf(&row, &gte), LeafOutcome::True);

        let gt = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(token)],
        );
        assert_eq!(evaluate_leaf(&row, &gt), LeafOutcome::False);

        // A bound above the row's token excludes it under `>=`.
        let gte_above = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gte,
            vec![Value::BigInt(token.saturating_add(1))],
        );
        assert_eq!(evaluate_leaf(&row, &gte_above), LeafOutcome::False);

        // An empty key (synthesised row) is Unknown, never spuriously matched.
        let empty = row_with_key(&[]);
        assert_eq!(evaluate_leaf(&empty, &gte), LeafOutcome::Unknown);
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

    /// Build a one-column `QueryRow` for predicate-evaluation tests.
    fn row_with_int(column: &str, value: i64) -> QueryRow {
        let mut values = std::collections::HashMap::new();
        values.insert(column.to_string(), Value::Integer(value as i32));
        QueryRow {
            values,
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    fn row_with_key(partition: &[u8]) -> QueryRow {
        QueryRow {
            values: std::collections::HashMap::new(),
            key: RowKey::new(partition.to_vec()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    /// Regression (roborev job 38): in the batch path PER PARTITION LIMIT must
    /// cap per partition even when a partition's rows are NOT contiguous (e.g.
    /// after ORDER BY interleaves them). Counting must key on the partition, not
    /// just track the most recent one.
    #[test]
    fn per_partition_limit_caps_interleaved_partitions() {
        let a = b"A".as_slice();
        let b = b"B".as_slice();
        // Partition A appears 3 times but is split by a B row in the middle.
        let rows = vec![
            row_with_key(a),
            row_with_key(b),
            row_with_key(a),
            row_with_key(a),
            row_with_key(b),
        ];
        let out = SelectExecutor::execute_per_partition_limit(rows, 2);
        let count = |p: &[u8]| out.iter().filter(|r| r.key.0 == p).count();
        assert_eq!(
            count(a),
            2,
            "partition A must be capped at 2 despite interleaving"
        );
        assert_eq!(count(b), 2, "partition B has 2 rows, all kept");
        assert_eq!(out.len(), 4);
    }

    /// Issue #788: each clustering-key inequality op must include/exclude rows on
    /// the correct side of its bound when evaluated post-scan.
    #[test]
    fn inequality_predicates_apply_single_bound() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let bound =
            |op: SSTableFilterOp| SSTablePredicate::column("ck", op, vec![Value::Integer(200)]);
        let eval = |op: SSTableFilterOp, ck: i64| {
            evaluate_predicates(&row_with_int("ck", ck), std::slice::from_ref(&bound(op))).unwrap()
        };

        // ck > 200
        assert!(!eval(SSTableFilterOp::Gt, 200));
        assert!(eval(SSTableFilterOp::Gt, 201));
        // ck >= 200
        assert!(eval(SSTableFilterOp::Gte, 200));
        assert!(!eval(SSTableFilterOp::Gte, 199));
        // ck < 200
        assert!(eval(SSTableFilterOp::Lt, 199));
        assert!(!eval(SSTableFilterOp::Lt, 200));
        // ck <= 200
        assert!(eval(SSTableFilterOp::Lte, 200));
        assert!(!eval(SSTableFilterOp::Lte, 201));
    }

    /// Issue #834: `evaluate_leaf` distinguishes the three SQL truth values.
    /// A present, comparable value yields True/False; an absent column or an
    /// explicit `Null` value yields Unknown so OR/NOT callers get SQL semantics.
    #[test]
    fn evaluate_leaf_is_three_valued() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let gt200 = SSTablePredicate::column("ck", SSTableFilterOp::Gt, vec![Value::Integer(200)]);

        // Present value → definite True/False.
        assert_eq!(
            evaluate_leaf(&row_with_int("ck", 201), &gt200),
            LeafOutcome::True
        );
        assert_eq!(
            evaluate_leaf(&row_with_int("ck", 200), &gt200),
            LeafOutcome::False
        );

        // Absent column → Unknown (not False).
        assert_eq!(
            evaluate_leaf(&row_with_int("other", 999), &gt200),
            LeafOutcome::Unknown
        );

        // Explicit Null value → Unknown.
        let mut values = std::collections::HashMap::new();
        values.insert("ck".to_string(), Value::Null);
        let null_row = QueryRow {
            values,
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        };
        assert_eq!(evaluate_leaf(&null_row, &gt200), LeafOutcome::Unknown);
    }

    /// Pushed-down `IN` operands lower to wide numeric types (`Integer`), but
    /// the row value for a CQL `tinyint`/`smallint`/`float` column is a narrow
    /// variant. Membership must coerce (like `Equal`) so the match still holds.
    #[test]
    fn evaluate_leaf_in_coerces_narrow_numeric_columns() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        // Operands as they arrive from a Flight ticket (wide types).
        let in_pred = SSTablePredicate::column(
            "v",
            SSTableFilterOp::In,
            vec![Value::Integer(7), Value::Integer(9)],
        );

        let row_of = |val: Value| {
            let mut values = std::collections::HashMap::new();
            values.insert("v".to_string(), val);
            QueryRow {
                values,
                key: RowKey::new(Vec::new()),
                metadata: Default::default(),
                cell_metadata: None,
            }
        };

        // Narrow column values must still match the wide IN operands.
        assert_eq!(
            evaluate_leaf(&row_of(Value::TinyInt(7)), &in_pred),
            LeafOutcome::True
        );
        assert_eq!(
            evaluate_leaf(&row_of(Value::SmallInt(9)), &in_pred),
            LeafOutcome::True
        );
        assert_eq!(
            evaluate_leaf(&row_of(Value::Float32(7.0)), &in_pred),
            LeafOutcome::True
        );
        // A non-member narrow value is still False (not Unknown).
        assert_eq!(
            evaluate_leaf(&row_of(Value::TinyInt(8)), &in_pred),
            LeafOutcome::False
        );
    }

    /// Issue #788: the `pk = ? AND ck >= 0 AND ck < 200` shape — a two-bound AND
    /// set — must include `[0, 199]` and exclude `200`/`1000`, reproducing the
    /// 200-row slice the issue expects (previously the whole partition leaked).
    #[test]
    fn two_bound_inequality_slice_selects_half_open_range() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let predicates = vec![
            SSTablePredicate::column("ck", SSTableFilterOp::Gte, vec![Value::Integer(0)]),
            SSTablePredicate::column("ck", SSTableFilterOp::Lt, vec![Value::Integer(200)]),
        ];
        let in_slice = |ck: i64| evaluate_predicates(&row_with_int("ck", ck), &predicates).unwrap();

        assert!(in_slice(0), "lower bound is inclusive");
        assert!(in_slice(199), "last row in [0, 200) is included");
        assert!(!in_slice(200), "upper bound is exclusive");
        assert!(!in_slice(1000), "rows past the slice are excluded");
        assert!(!in_slice(-1), "rows below the slice are excluded");
    }

    // =========================================================================
    // Issue #692: WRITETIME() / TTL() executor wiring tests
    // =========================================================================

    use crate::query::result::{CellExpiration, CellWriteMetadata};

    /// Helper: build a QueryRow with a given column value and optional cell metadata.
    fn row_with_cell_meta(column: &str, value: Value, meta: Option<CellWriteMetadata>) -> QueryRow {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set(column.to_string(), value);
        if let Some(m) = meta {
            row.insert_cell_metadata(column.to_string(), m);
        }
        row
    }

    // --- evaluate_writetime_ttl free-function tests ---

    /// WRITETIME(col) returns Value::BigInt(micros) when metadata is present.
    #[test]
    fn test_writetime_returns_bigint_when_metadata_present() {
        let write_ts = 1_700_000_000_000_000_i64;
        let row = row_with_cell_meta(
            "name",
            Value::Text("Alice".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: write_ts,
                expiration: None,
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 0 /* now unused for WRITETIME */);
        assert_eq!(
            result,
            Value::BigInt(write_ts),
            "WRITETIME(col) must return Value::BigInt(micros)"
        );
    }

    /// WRITETIME(col) returns Value::Null when cell metadata is absent.
    #[test]
    fn test_writetime_returns_null_when_no_metadata() {
        // Row has a value for the column but no cell metadata (e.g. partition-key column).
        let row = row_with_cell_meta("id", Value::Integer(1), None);

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "id".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 0);
        assert_eq!(
            result,
            Value::Null,
            "WRITETIME(col) must return NULL when no cell metadata is threaded"
        );
    }

    /// TTL(col) returns Value::Integer(remaining) for a live TTL cell.
    #[test]
    fn test_ttl_returns_remaining_seconds_for_live_cell() {
        // Cell was written at epoch 0, TTL = 3600s, expires at epoch 3600.
        // Now = epoch 1000. Remaining = 2600s.
        let now_secs: i64 = 1000;
        let expires_at: i64 = 3600;
        let row = row_with_cell_meta(
            "score",
            Value::Integer(42),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 3600,
                    expires_at_seconds: expires_at,
                }),
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "score".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, now_secs);
        assert_eq!(
            result,
            Value::Integer(2600),
            "TTL(col) must return remaining seconds for a live cell"
        );
    }

    /// TTL(col) returns Value::Null when the cell has no expiration.
    #[test]
    fn test_ttl_returns_null_when_no_expiration() {
        let row = row_with_cell_meta(
            "name",
            Value::Text("Bob".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 100,
                expiration: None, // no TTL written
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "name".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 9999);
        assert_eq!(
            result,
            Value::Null,
            "TTL(col) must return NULL when the cell has no TTL"
        );
    }

    /// TTL(col) returns Value::Null when the cell is expired.
    #[test]
    fn test_ttl_returns_null_for_expired_cell() {
        // Cell expires at epoch 100; now is epoch 200 → expired.
        let row = row_with_cell_meta(
            "token",
            Value::Text("abc".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 100,
                    expires_at_seconds: 100,
                }),
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "token".to_string(),
            alias: None,
        };

        // now_secs = 200 > expires_at = 100 → expired
        let result = evaluate_writetime_ttl(&call, &row, 200);
        assert_eq!(
            result,
            Value::Null,
            "TTL(col) must return NULL when the cell is expired"
        );
    }

    /// TTL(col) returns Value::Null when cell metadata is entirely absent.
    #[test]
    fn test_ttl_returns_null_when_no_metadata() {
        let row = row_with_cell_meta("x", Value::Integer(7), None);

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "x".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 1000);
        assert_eq!(result, Value::Null);
    }

    // --- column name convention tests ---

    /// Cassandra convention: `writetime(col)` (no alias).
    #[test]
    fn test_writetime_ttl_column_name_no_alias() {
        let wt_call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        };
        assert_eq!(writetime_ttl_column_name(&wt_call), "writetime(name)");

        let ttl_call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "name".to_string(),
            alias: None,
        };
        assert_eq!(writetime_ttl_column_name(&ttl_call), "ttl(name)");
    }

    /// Explicit alias overrides the Cassandra-convention name.
    #[test]
    fn test_writetime_ttl_column_name_with_alias() {
        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "score".to_string(),
            alias: Some("wt".to_string()),
        };
        assert_eq!(writetime_ttl_column_name(&call), "wt");
    }

    // --- planning flag tests ---

    /// `select_has_writetime_ttl` returns true only when a WriteTimeTtl expression is present.
    #[test]
    fn test_select_has_writetime_ttl_detection() {
        // No WriteTimeTtl → false
        let stmt_no_wt = SelectStatement {
            select_clause: SelectClause::Columns(vec![
                SelectExpression::Column(ColumnRef::new("id")),
                SelectExpression::Column(ColumnRef::new("name")),
            ]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };
        assert!(!select_has_writetime_ttl(&stmt_no_wt));

        // With WriteTimeTtl → true
        let stmt_wt = SelectStatement {
            select_clause: SelectClause::Columns(vec![
                SelectExpression::Column(ColumnRef::new("id")),
                SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::WriteTime,
                    column: "name".to_string(),
                    alias: None,
                }),
            ]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };
        assert!(select_has_writetime_ttl(&stmt_wt));

        // SELECT * → false (no expression list to inspect)
        let stmt_star = SelectStatement {
            select_clause: SelectClause::All,
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };
        assert!(!select_has_writetime_ttl(&stmt_star));
    }

    // --- executor integration tests ---

    /// The executor's `evaluate_select_expression` returns the correct value for
    /// a WRITETIME call when cell metadata is pre-attached to the row.
    #[tokio::test]
    async fn test_executor_evaluate_writetime_reads_cell_metadata() {
        let executor = create_test_executor_with_clock(0).await;

        let write_ts = 1_700_000_000_000_000_i64;
        let row = row_with_cell_meta(
            "name",
            Value::Text("Carol".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: write_ts,
                expiration: None,
            }),
        );

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(result, Value::BigInt(write_ts));
    }

    /// The executor's `evaluate_select_expression` returns NULL for WRITETIME
    /// when cell metadata is absent (the common case before the storage reader
    /// is updated to thread metadata).
    #[tokio::test]
    async fn test_executor_evaluate_writetime_null_when_no_metadata() {
        let executor = create_test_executor_with_clock(0).await;

        // Row has the column value but no attached cell metadata.
        let row = row_with_cell_meta("name", Value::Text("Dave".to_string()), None);

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(result, Value::Null);
    }

    /// The executor returns correct TTL using the injected fixed clock.
    #[tokio::test]
    async fn test_executor_evaluate_ttl_with_injected_clock() {
        // now = epoch 1000; cell expires at epoch 5000 → remaining = 4000s
        let now_secs: i64 = 1000;
        let executor = create_test_executor_with_clock(now_secs).await;

        let row = row_with_cell_meta(
            "session",
            Value::Text("tok".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 5000,
                    expires_at_seconds: 5000,
                }),
            }),
        );

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "session".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(
            result,
            Value::Integer(4000),
            "TTL must use the injected clock, not the wall clock"
        );
    }

    /// Expired cell: executor returns NULL via injected clock.
    #[tokio::test]
    async fn test_executor_evaluate_ttl_expired_cell_returns_null() {
        // now = epoch 9999; cell expired at epoch 100 → NULL
        let executor = create_test_executor_with_clock(9999).await;

        let row = row_with_cell_meta(
            "cache",
            Value::Text("val".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 100,
                    expires_at_seconds: 100,
                }),
            }),
        );

        let expr = SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "cache".to_string(),
            alias: None,
        });

        let result = executor.evaluate_select_expression(&expr, &row).unwrap();
        assert_eq!(result, Value::Null, "Expired TTL cell must produce NULL");
    }

    /// Column info for WRITETIME uses BigInt data type and bigint cql_type.
    #[tokio::test]
    async fn test_get_result_columns_writetime_has_bigint_type() {
        let executor = create_test_executor().await;

        let stmt = SelectStatement {
            select_clause: SelectClause::Columns(vec![SelectExpression::WriteTimeTtl(
                WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::WriteTime,
                    column: "name".to_string(),
                    alias: None,
                },
            )]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };

        let cols = executor.get_result_columns(&stmt).await.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "writetime(name)");
        assert_eq!(cols[0].data_type, crate::types::DataType::BigInt);
        assert!(cols[0].nullable, "WRITETIME column must be nullable");
        assert_eq!(cols[0].cql_type, Some(CqlType::BigInt));
    }

    /// Column info for TTL uses Integer data type and int cql_type.
    #[tokio::test]
    async fn test_get_result_columns_ttl_has_int_type() {
        let executor = create_test_executor().await;

        let stmt = SelectStatement {
            select_clause: SelectClause::Columns(vec![SelectExpression::WriteTimeTtl(
                WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::Ttl,
                    column: "score".to_string(),
                    alias: None,
                },
            )]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };

        let cols = executor.get_result_columns(&stmt).await.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "ttl(score)");
        assert_eq!(cols[0].data_type, crate::types::DataType::Integer);
        assert!(cols[0].nullable, "TTL column must be nullable");
        assert_eq!(cols[0].cql_type, Some(CqlType::Int));
    }

    /// Column name uses alias when provided, overriding convention.
    #[tokio::test]
    async fn test_get_result_columns_writetime_with_alias() {
        let executor = create_test_executor().await;

        let stmt = SelectStatement {
            select_clause: SelectClause::Columns(vec![SelectExpression::WriteTimeTtl(
                WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::WriteTime,
                    column: "name".to_string(),
                    alias: Some("wt".to_string()),
                },
            )]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };

        let cols = executor.get_result_columns(&stmt).await.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].name, "wt",
            "Alias must override Cassandra convention"
        );
    }
}
