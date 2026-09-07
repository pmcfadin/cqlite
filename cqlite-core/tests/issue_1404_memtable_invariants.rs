//! Memtable native invariant tests (Issue #1404, epic #1381).
//!
//! These are CQLite-native *invariant* tests (not byte-parity) that exercise the
//! real memtable public surface, modeled on Apache Cassandra's memtable test
//! analogues:
//!
//! - **accounting drift** — Cassandra's `MemtableNegativeReleasedCQLReproTest`
//!   guards against the memory allocator under-/over-counting, which leads to
//!   premature or missing flushes. CQLite's `Memtable` tracks `size_bytes` /
//!   `row_count`; here we assert the ledger never regresses (a `usize` "negative"
//!   surfaces as a wrap, i.e. a *decrease*) across arbitrary write/overwrite/delete
//!   interleavings and returns to the empty baseline after `clear()`.
//! - **token-order iteration** — Data.db partition ordering depends on
//!   `Memtable::iter()` yielding partitions in Murmur3 token order (ties broken by
//!   raw key bytes). We assert this directly against independently computed
//!   `cassandra_murmur3_token` values rather than only indirectly via downstream
//!   byte parity.
//! - **estimate sanity** — `estimate_mutation_size` (exercised through
//!   `insert_with_key` -> `size_bytes()`) must stay within a documented factor of
//!   the raw payload so a silent estimator regression fails loudly.
//! - **flush-set completeness** — every mutation written before a flush is
//!   accounted for exactly once against the memtable's own ledger, and the flush
//!   preserves every distinct partition and drains the ledger (FlushSet analogue).
//!
//! Routing: oracle/invariant-driven — no OpenSpec (per issue #1404).

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, DecoratedKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{UdtField, UdtValue, Value};
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use proptest::prelude::*;
use std::collections::HashMap;
use tempfile::TempDir;

// ============================================================================
// Helpers
// ============================================================================

/// Build a single-partition-key schema whose one PK column has `data_type`.
///
/// Used to drive the *real* `PartitionKey::to_decorated_key` production path
/// (which serializes the value per its comparator type before hashing).
fn single_pk_schema(data_type: &str) -> TableSchema {
    TableSchema {
        keyspace: "ks_1404".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: data_type.to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: data_type.to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// A `Write` mutation for partition key `pk_value` with a single text column.
fn text_write_mutation(pk_value: Value, text: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("ks_1404", "t"),
        PartitionKey::single("pk", pk_value),
        None,
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text(text.to_string()),
        }],
        ts,
        None,
    )
}

/// Build a `DecoratedKey` from an `int` partition value the same way the write
/// path does (schema-serialized bytes -> Murmur3 token).
fn int_key(id: i32) -> DecoratedKey {
    let schema = single_pk_schema("int");
    PartitionKey::single("pk", Value::Integer(id))
        .to_decorated_key(&schema)
        .expect("decorated key for int pk")
}

// ============================================================================
// 1. Accounting non-negativity + bounded drift (property test)
// ============================================================================

/// One randomized op against the memtable.
#[derive(Debug, Clone)]
struct AcctOp {
    partition: i32,
    kind: u8,
    payload_len: usize,
}

fn acct_op_strategy() -> impl Strategy<Value = AcctOp> {
    (0i32..6, 0u8..4, 0usize..48).prop_map(|(partition, kind, payload_len)| AcctOp {
        partition,
        kind,
        payload_len,
    })
}

proptest! {
    /// For arbitrary interleavings of writes / overwrites / deletes across
    /// partitions, the tracked ledger is internally consistent, never regresses
    /// (a `usize` wrap would appear as a decrease), and `clear()` returns the
    /// memtable to the exact empty baseline with no residual drift.
    #[test]
    fn prop_accounting_no_drift(ops in proptest::collection::vec(acct_op_strategy(), 0..200)) {
        use cqlite_core::storage::write_engine::Memtable;

        let mut memtable = Memtable::new();
        let mut prev_size = memtable.size_bytes();

        for (idx, op) in ops.iter().enumerate() {
            let inserts = idx + 1;
            let key = int_key(op.partition);
            let payload = "x".repeat(op.payload_len);
            let mutation = match op.kind {
                // write / overwrite: same partition may be hit repeatedly
                0 | 1 => text_write_mutation(Value::Integer(op.partition), &payload, 1),
                // delete a cell (tombstone) — still an appended mutation
                2 => Mutation::new(
                    TableId::new("ks_1404", "t"),
                    PartitionKey::single("pk", Value::Integer(op.partition)),
                    None,
                    vec![CellOperation::Delete {
                        column: "v".to_string(),
                        local_deletion_time: None,
                    }],
                    1,
                    None,
                ),
                // delete the whole row (row tombstone)
                _ => Mutation::new(
                    TableId::new("ks_1404", "t"),
                    PartitionKey::single("pk", Value::Integer(op.partition)),
                    None,
                    vec![CellOperation::DeleteRow],
                    1,
                    None,
                ),
            };

            memtable.insert_with_key(key, mutation).expect("insert");

            // Ledger never regresses (guards a usize wrap == "negative").
            let size = memtable.size_bytes();
            prop_assert!(
                size >= prev_size,
                "size_bytes regressed: {} -> {}",
                prev_size,
                size
            );
            prev_size = size;

            // row_count matches the number of appended mutations.
            prop_assert_eq!(memtable.row_count(), inserts);

            // The iterator's ledger agrees with row_count (nothing dropped/dup'd).
            let iter_total: usize = memtable.iter().map(|(_, m)| m.len()).sum();
            prop_assert_eq!(iter_total, inserts);
        }

        // After clear, everything returns to the empty baseline: no residual drift.
        memtable.clear();
        prop_assert!(memtable.is_empty());
        prop_assert_eq!(memtable.size_bytes(), 0);
        prop_assert_eq!(memtable.row_count(), 0);
        prop_assert_eq!(memtable.iter().count(), 0);
    }
}

// ============================================================================
// 2. Estimate sanity: bounded vs raw payload
// ============================================================================

/// Documented estimator envelope. The estimator is intentionally *conservative*
/// (adds fixed per-mutation / per-column / per-op overhead), so it must never
/// undercount the raw content payload, and must stay within `MAX_FACTOR`x of it
/// plus a fixed `MAX_BASE_OVERHEAD` allowance. Asserting BOTH bounds makes a
/// silent estimator regression (e.g. returning 0, or ballooning) fail.
const MAX_FACTOR: usize = 4;
const MAX_BASE_OVERHEAD: usize = 512;

/// Independently computed *content floor* for a mutation: the raw bytes the
/// payload actually carries (column-name bytes + value content bytes), with NO
/// estimator overhead. Deliberately not a re-implementation of
/// `estimate_mutation_size` — it is a lower bound the estimator must dominate.
fn content_floor(m: &Mutation) -> usize {
    let mut floor = 0usize;
    for (name, value) in &m.partition_key.columns {
        floor += name.len() + value_content_bytes(value);
    }
    if let Some(ck) = &m.clustering_key {
        for (name, value) in &ck.columns {
            floor += name.len() + value_content_bytes(value);
        }
    }
    for op in &m.operations {
        match op {
            CellOperation::Write { column, value }
            | CellOperation::WriteWithTtl { column, value, .. } => {
                floor += column.len() + value_content_bytes(value);
            }
            CellOperation::Delete { column, .. } => floor += column.len(),
            CellOperation::DeleteRow => {}
            CellOperation::WriteComplexElement { column, value, .. } => {
                floor += column.len() + value.as_ref().map_or(0, value_content_bytes);
            }
            CellOperation::ComplexDeletion { column, .. } => floor += column.len(),
        }
    }
    floor
}

/// Raw content byte count of a value (independent of the estimator).
fn value_content_bytes(v: &Value) -> usize {
    match v {
        Value::Null => 0,
        // The empty-buffer sentinel's payload IS the empty buffer, so it has
        // zero content bytes (issue #3805).
        Value::Empty(_) => 0,
        Value::Boolean(_) | Value::TinyInt(_) => 1,
        Value::SmallInt(_) => 2,
        Value::Integer(_) | Value::Float32(_) | Value::Date(_) => 4,
        Value::BigInt(_)
        | Value::Counter(_)
        | Value::Timestamp(_)
        | Value::Time(_)
        | Value::Float(_) => 8,
        Value::Uuid(_) => 16,
        Value::Text(s) => s.len(),
        Value::Blob(b) | Value::Varint(b) | Value::Inet(b) => b.len(),
        Value::Decimal { unscaled, .. } => unscaled.len(),
        Value::Duration { .. } => 12,
        Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
            items.iter().map(value_content_bytes).sum()
        }
        Value::Map(entries) => entries
            .iter()
            .map(|(k, val)| value_content_bytes(k) + value_content_bytes(val))
            .sum(),
        Value::Udt(u) => u
            .fields
            .iter()
            .map(|f| f.name.len() + f.value.as_ref().map_or(0, value_content_bytes))
            .sum(),
        Value::Json(j) => j.to_string().len(),
        Value::Frozen(inner) => value_content_bytes(inner),
        Value::Tombstone(_) => 0,
    }
}

/// The estimate for a single mutation is the size the memtable attributes to it:
/// insert into an empty memtable and read `size_bytes()`.
fn estimate_of(m: Mutation) -> usize {
    use cqlite_core::storage::write_engine::Memtable;
    let mut memtable = Memtable::new();
    let key = int_key(1);
    memtable.insert_with_key(key, m).expect("insert");
    memtable.size_bytes()
}

#[test]
fn estimate_within_documented_factor_for_representative_corpus() {
    let udt = Value::Udt(Box::new(UdtValue {
        type_name: "addr".to_string(),
        keyspace: "ks_1404".to_string(),
        fields: vec![
            UdtField {
                name: "street".to_string(),
                value: Some(Value::text("100 Main Street".to_string())),
            },
            UdtField {
                name: "zip".to_string(),
                value: Some(Value::Integer(94105)),
            },
        ],
    }));

    // A corpus of representative mutations (text / blob / collections / UDT /
    // tombstones). Each entry is (label, mutation).
    let corpus: Vec<(&str, Mutation)> = vec![
        (
            "small_text",
            text_write_mutation(Value::Integer(1), "hi", 1),
        ),
        (
            "large_text",
            text_write_mutation(Value::Integer(2), &"a".repeat(1000), 1),
        ),
        (
            "blob",
            Mutation::new(
                TableId::new("ks_1404", "t"),
                PartitionKey::single("pk", Value::Integer(3)),
                None,
                vec![CellOperation::Write {
                    column: "b".to_string(),
                    value: Value::blob(vec![0u8; 256]),
                }],
                1,
                None,
            ),
        ),
        (
            "list",
            Mutation::new(
                TableId::new("ks_1404", "t"),
                PartitionKey::single("pk", Value::Integer(4)),
                None,
                vec![CellOperation::Write {
                    column: "l".to_string(),
                    value: Value::List((0..20).map(Value::Integer).collect()),
                }],
                1,
                None,
            ),
        ),
        (
            "map",
            Mutation::new(
                TableId::new("ks_1404", "t"),
                PartitionKey::single("pk", Value::Integer(5)),
                None,
                vec![CellOperation::Write {
                    column: "m".to_string(),
                    value: Value::Map(
                        (0..10)
                            .map(|i| (Value::Integer(i), Value::text(format!("val{i}"))))
                            .collect(),
                    ),
                }],
                1,
                None,
            ),
        ),
        (
            "udt",
            Mutation::new(
                TableId::new("ks_1404", "t"),
                PartitionKey::single("pk", Value::Integer(6)),
                None,
                vec![CellOperation::Write {
                    column: "addr".to_string(),
                    value: udt,
                }],
                1,
                None,
            ),
        ),
        (
            "cell_tombstone",
            Mutation::new(
                TableId::new("ks_1404", "t"),
                PartitionKey::single("pk", Value::Integer(7)),
                None,
                vec![CellOperation::Delete {
                    column: "v".to_string(),
                    local_deletion_time: None,
                }],
                1,
                None,
            ),
        ),
        (
            "row_tombstone",
            Mutation::new(
                TableId::new("ks_1404", "t"),
                PartitionKey::single("pk", Value::Integer(8)),
                None,
                vec![CellOperation::DeleteRow],
                1,
                None,
            ),
        ),
    ];

    for (label, mutation) in corpus {
        let floor = content_floor(&mutation);
        let estimate = estimate_of(mutation);

        assert!(
            estimate >= floor,
            "[{label}] estimator undercounts raw payload: estimate {estimate} < floor {floor}"
        );
        let upper = floor * MAX_FACTOR + MAX_BASE_OVERHEAD;
        assert!(
            estimate <= upper,
            "[{label}] estimator over-counts beyond documented envelope: \
             estimate {estimate} > {MAX_FACTOR}*{floor}+{MAX_BASE_OVERHEAD}={upper}"
        );
    }
}

// ============================================================================
// 3. Token-order iteration invariant
// ============================================================================

#[test]
fn iter_yields_nondecreasing_token_order_across_pk_types() {
    use cqlite_core::storage::write_engine::Memtable;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    // Deterministic RNG (fixed seed): no wall-clock / nondeterministic input.
    let mut rng = StdRng::seed_from_u64(0x1404_1404_1404_1404);

    // Cover the representative supported partition-key types.
    let int_schema = single_pk_schema("int");
    let bigint_schema = single_pk_schema("bigint");
    let text_schema = single_pk_schema("text");
    let uuid_schema = single_pk_schema("uuid");
    let blob_schema = single_pk_schema("blob");

    let mut memtable = Memtable::new();
    // Track (independently computed token, raw key bytes) for every insert.
    let mut expected: Vec<(i64, Vec<u8>)> = Vec::new();

    for _ in 0..400 {
        let (pk_value, schema): (Value, &TableSchema) = match rng.gen_range(0..5) {
            0 => (Value::Integer(rng.gen()), &int_schema),
            1 => (Value::BigInt(rng.gen()), &bigint_schema),
            2 => {
                let len = rng.gen_range(1..24);
                let s: String = (0..len)
                    .map(|_| rng.gen_range(b'a'..=b'z') as char)
                    .collect();
                (Value::Text(s.into()), &text_schema)
            }
            3 => {
                let mut b = [0u8; 16];
                rng.fill(&mut b);
                (Value::Uuid(b), &uuid_schema)
            }
            _ => {
                let len = rng.gen_range(1..20);
                let bytes: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
                (Value::Blob(bytes.into()), &blob_schema)
            }
        };

        let pk = PartitionKey::single("pk", pk_value.clone());
        let key_bytes = pk.to_bytes(schema).expect("serialize pk");
        let decorated = pk.to_decorated_key(schema).expect("decorate pk");

        // Direct assertion: the decorated token equals an independently computed
        // Murmur3 token over the exact serialized key bytes.
        assert_eq!(
            decorated.token,
            cassandra_murmur3_token(&key_bytes),
            "decorated token diverges from independent Murmur3 for {pk_value:?}"
        );
        assert_eq!(decorated.key, key_bytes, "decorated key bytes mismatch");

        let mutation = Mutation::new(
            TableId::new("ks_1404", "t"),
            pk,
            None,
            vec![CellOperation::Write {
                column: "v".to_string(),
                value: Value::text("x".to_string()),
            }],
            1,
            None,
        );
        memtable
            .insert_with_key(decorated.clone(), mutation)
            .expect("insert");
        expected.push((decorated.token, decorated.key));
    }

    // iter() must yield partitions in strictly non-decreasing (token, key-bytes)
    // order — the exact ordering Data.db partition layout depends on.
    let observed: Vec<(i64, Vec<u8>)> = memtable
        .iter()
        .map(|(k, _)| (k.token, k.key.clone()))
        .collect();

    for pair in observed.windows(2) {
        let a = (&pair[0].0, &pair[0].1);
        let b = (&pair[1].0, &pair[1].1);
        assert!(
            a <= b,
            "iter() out of token order: ({}, {:?}) > ({}, {:?})",
            pair[0].0,
            pair[0].1,
            pair[1].0,
            pair[1].1
        );
    }

    // Cross-check against the independently sorted expectation.
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(
        observed, expected_sorted,
        "iter() order disagrees with independent (token, key) sort"
    );
}

#[test]
fn token_tie_broken_by_key_bytes() {
    use cqlite_core::storage::write_engine::Memtable;

    // Regression fixture: two keys that COLLIDE on token must iterate ordered by
    // raw key bytes (Cassandra's DecoratedKey tie-break rule). Construct the
    // collision directly via DecoratedKey::new to make the invariant explicit.
    let shared_token = 0x0BAD_F00D_i64;
    let key_hi = DecoratedKey::new(shared_token, vec![0xFF, 0x00]);
    let key_lo = DecoratedKey::new(shared_token, vec![0x00, 0x01]);

    let mut memtable = Memtable::new();
    // Insert in "wrong" order to prove ordering is by key, not insertion.
    memtable
        .insert_with_key(
            key_hi.clone(),
            text_write_mutation(Value::Integer(1), "hi", 1),
        )
        .expect("insert hi");
    memtable
        .insert_with_key(
            key_lo.clone(),
            text_write_mutation(Value::Integer(2), "lo", 1),
        )
        .expect("insert lo");

    let ordered: Vec<Vec<u8>> = memtable.iter().map(|(k, _)| k.key.clone()).collect();
    assert_eq!(
        ordered,
        vec![key_lo.key.clone(), key_hi.key.clone()],
        "token-tie must be resolved by ascending key bytes"
    );
    // Both share the token; the tie-break is purely the key bytes.
    assert_eq!(key_hi.token, key_lo.token);
}

// ============================================================================
// 4. Flush-set completeness
// ============================================================================

#[test]
fn memtable_ledger_accounts_every_mutation_exactly_once() {
    use cqlite_core::storage::write_engine::Memtable;

    let mut memtable = Memtable::new();
    let partitions = 7;
    let per_partition = 5;

    for p in 0..partitions {
        for r in 0..per_partition {
            let key = int_key(p);
            let mutation = text_write_mutation(Value::Integer(p), &format!("p{p}r{r}"), 1);
            memtable.insert_with_key(key, mutation).expect("insert");
        }
    }

    let total = (partitions * per_partition) as usize;

    // Ledger completeness: row_count == inserts, and the iterator surfaces every
    // mutation exactly once (sum of per-partition vectors == row_count).
    assert_eq!(memtable.row_count(), total);
    let iter_total: usize = memtable.iter().map(|(_, m)| m.len()).sum();
    assert_eq!(iter_total, total, "iter() dropped or duplicated mutations");

    // Every distinct partition is present exactly once, each holding exactly the
    // mutations written to it — nothing merged away or duplicated.
    assert_eq!(memtable.iter().count(), partitions as usize);
    for p in 0..partitions {
        let got = memtable.get(&int_key(p)).expect("partition present");
        assert_eq!(got.len(), per_partition as usize);
    }
}

#[tokio::test]
async fn flush_preserves_every_partition_and_drains_ledger() {
    let temp_dir = TempDir::new().unwrap();
    let schema = single_pk_schema("int");
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    let mut engine = WriteEngine::new(config).expect("engine");

    let partitions = 20;
    let per_partition = 3;
    for p in 0..partitions {
        for r in 0..per_partition {
            let mutation =
                text_write_mutation(Value::Integer(p), &format!("p{p}r{r}"), 1000 + r as i64);
            engine.write_async(mutation).await.expect("write");
        }
    }

    let total = (partitions * per_partition) as usize;

    // The memtable's own ledger sees every write before the flush.
    assert_eq!(engine.memtable_row_count(), total);

    // Flush: FlushSet completeness — every DISTINCT partition appears in the
    // flushed SSTable exactly once (not dropped, not duplicated).
    let info = engine.flush().await.expect("flush").expect("sstable info");
    assert_eq!(
        info.partition_count, partitions as usize,
        "flushed SSTable partition count must equal distinct partitions written"
    );

    // Flush drains the memtable ledger back to the empty baseline (no residual).
    assert_eq!(engine.memtable_row_count(), 0);
    assert_eq!(engine.memtable_size(), 0);

    // The flushed Data.db exists and is non-empty.
    assert!(info.data_path.exists());
    assert!(info.data_size > 0);
}
