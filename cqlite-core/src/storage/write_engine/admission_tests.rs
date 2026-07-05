//! Honest memtable hard-limit admission tests (issue #1625).
//!
//! The pre-#1625 admission check compared only the *current* memtable size
//! against the hard limit and never measured the incoming mutation, so a single
//! jumbo mutation could be admitted into an empty memtable (blowing past the
//! limit on the first insert) and a near-full memtable could be pushed over the
//! limit before the *next* write was rejected. In addition, the size estimator
//! returned a flat ~1KB for anything nested past the recursion depth cap, so a
//! deeply/widely nested value was systematically under-counted.
//!
//! These tests exercise the real write surface (`WriteEngine::write`) so
//! admission and accounting are validated together.

use super::{Durability, Mutation, WriteEngine, WriteEngineConfig};
use crate::error::Error;
use crate::storage::write_engine::mutation::{CellOperation, PartitionKey, TableId};
use crate::storage::write_engine::test_support::{create_test_mutation, create_test_schema};
use crate::types::Value;
use tempfile::TempDir;

/// Build a mutation whose single text value is `bytes` long, used to force a
/// jumbo/near-limit admission decision.
fn create_sized_mutation(id: i32, bytes: usize, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text("x".repeat(bytes)),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// A single jumbo mutation whose estimate alone exceeds the hard limit must be
/// REJECTED even into an empty memtable (previously it was admitted because the
/// check ignored the incoming mutation), with a distinct single-mutation error.
#[test]
fn test_single_jumbo_mutation_rejected_into_empty_memtable() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_flush_threshold(1024 * 1024)
    .with_hard_limit(64 * 1024) // 64KB
    .with_durability(Durability::Disabled);

    let mut engine = WriteEngine::new(config).unwrap();
    assert_eq!(engine.memtable_size(), 0);

    // 128KB value >> 64KB hard limit.
    let jumbo = create_sized_mutation(1, 128 * 1024, 1_000_000);
    let err = engine.write(jumbo).unwrap_err();
    match err {
        Error::Storage(msg) => {
            assert!(
                msg.contains("single mutation") && msg.contains("hard limit"),
                "expected distinct single-mutation ceiling error, got: {msg}"
            );
        }
        other => panic!("expected Storage error, got: {other:?}"),
    }

    // Nothing was admitted.
    assert_eq!(engine.memtable_size(), 0);
    assert_eq!(engine.memtable_row_count(), 0);
}

/// A write whose incoming size would push `current_size + incoming` over the
/// hard limit is rejected PRE-admission — the mutation that crosses the line
/// does not itself blow the budget.
#[test]
fn test_projected_sum_over_hard_limit_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_flush_threshold(1024 * 1024)
    .with_hard_limit(20 * 1024) // 20KB
    .with_durability(Durability::Disabled);

    let mut engine = WriteEngine::new(config).unwrap();

    // First 12KB write fits.
    engine
        .write(create_sized_mutation(1, 12 * 1024, 1_000_000))
        .unwrap();
    let size_after_first = engine.memtable_size();
    assert!(size_after_first >= 12 * 1024);

    // Second 12KB write: 12KB + ~12KB > 20KB → rejected, memtable unchanged.
    let err = engine
        .write(create_sized_mutation(2, 12 * 1024, 1_000_001))
        .unwrap_err();
    match err {
        Error::Storage(msg) => assert!(
            msg.contains("would exceed hard limit"),
            "expected projected-sum error, got: {msg}"
        ),
        other => panic!("expected Storage error, got: {other:?}"),
    }
    assert_eq!(
        engine.memtable_size(),
        size_after_first,
        "rejected write must not change memtable size"
    );
    assert_eq!(engine.memtable_row_count(), 1);
}

/// A normal write still succeeds, and the memtable's post-insert size equals
/// `size_before + estimate_mutation_size(&m)` — admission and accounting agree
/// by construction.
#[test]
fn test_admission_and_accounting_agree() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_durability(Durability::Disabled);

    let mut engine = WriteEngine::new(config).unwrap();

    let mutation = create_test_mutation(1, "a normal name", 1_000_000);
    let predicted = engine.memtable.estimate_mutation_size(&mutation);
    let before = engine.memtable_size();

    engine.write(mutation).unwrap();

    assert_eq!(
        engine.memtable_size(),
        before + predicted,
        "post-insert size must equal size_before + estimate_mutation_size"
    );
    assert_eq!(engine.memtable_row_count(), 1);
}

/// A deeply nested value carrying a genuinely large payload must trip the
/// hard-limit gate. The iterative estimator (issue #1625) counts REAL bytes at
/// every depth (no floor scaling), so a deep list of 500 × 200-byte strings
/// (~100KB) is estimated accurately and exceeds a 64KB hard limit.
#[test]
fn test_deeply_nested_mutation_trips_gate() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_flush_threshold(1024 * 1024)
    .with_hard_limit(64 * 1024) // 64KB
    .with_durability(Durability::Disabled);

    let mut engine = WriteEngine::new(config).unwrap();

    // 32 single-element wrapper lists around a wide list of 500 × 200-byte
    // strings (~100KB of real payload).
    let mut nested = Value::List((0..500).map(|_| Value::Text("y".repeat(200))).collect());
    for _ in 0..32 {
        nested = Value::List(vec![nested]);
    }
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: nested,
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1_000_000, None);

    // ~100KB of real payload (500 * 200), accurately estimated (not a floor).
    let estimate = engine.memtable.estimate_mutation_size(&mutation);
    assert!(
        estimate >= 500 * 200,
        "estimate must reflect ~100KB of real payload, got {estimate}"
    );

    let err = engine.write(mutation).unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("hard limit")),
        "deeply nested value must trip the hard-limit gate, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// A deep NARROW collection wrapping a LARGE direct scalar must also trip the
/// gate (issue #1625 roborev finding). Pre-fix, any node past the recursion
/// depth cap collapsed to a ~1KB floor, so `List([Text(128KB)])` parked at the
/// cap was counted as ~1KB and passed a 64KB hard limit while retaining 128KB.
/// The iterative estimator counts the large scalar at its real heap size
/// regardless of depth, so the write is rejected.
#[test]
fn test_deep_narrow_collection_with_large_scalar_trips_gate() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_flush_threshold(1024 * 1024)
    .with_hard_limit(64 * 1024) // 64KB
    .with_durability(Durability::Disabled);

    let mut engine = WriteEngine::new(config).unwrap();

    // Innermost: a single-element list holding one 128KB text scalar. Wrapped in
    // 32 single-element lists so the `List([Text(128KB)])` lands exactly at the
    // recursion depth cap (its Text child is a DIRECT child of the capped node).
    let mut nested = Value::List(vec![Value::Text("x".repeat(128 * 1024))]);
    for _ in 0..32 {
        nested = Value::List(vec![nested]);
    }
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: nested,
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1_000_000, None);

    let err = engine.write(mutation).unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("hard limit")),
        "deep narrow collection with a large scalar must trip the gate, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// Build a `WriteEngine` with a 64KB hard limit for the deep-scalar tests.
fn engine_with_64k_limit() -> (TempDir, WriteEngine) {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_flush_threshold(1024 * 1024)
    .with_hard_limit(64 * 1024) // 64KB
    .with_durability(Durability::Disabled);
    let engine = WriteEngine::new(config).unwrap();
    (temp_dir, engine)
}

/// Wrap a value in `levels` single-element lists.
fn wrap_in_lists(mut inner: Value, levels: usize) -> Value {
    for _ in 0..levels {
        inner = Value::List(vec![inner]);
    }
    inner
}

fn mutation_with_value(value: Value) -> Mutation {
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value,
    }];
    Mutation::new(table_id, pk, None, ops, 1_000_000, None)
}

/// A large scalar buried a FEW levels deep (`List([List([Text(128KB)])])`,
/// 2 levels) must be estimated at its REAL heap size — not the old ~1KB
/// depth-cap floor — and REJECTED against a 64KB hard limit. The iterative
/// estimator has no depth cap, so no wrapping level can hide the scalar
/// (issue #1625, 3rd iteration).
#[test]
fn test_large_scalar_two_levels_deep_rejected() {
    let (_tmp, mut engine) = engine_with_64k_limit();

    // List([List([Text(128KB)])]).
    let value = wrap_in_lists(Value::List(vec![Value::Text("x".repeat(128 * 1024))]), 1);
    let mutation = mutation_with_value(value);

    // Estimate must be ~128KB (real size), NOT the ~1KB floor.
    let estimate = engine.memtable.estimate_mutation_size(&mutation);
    assert!(
        estimate >= 128 * 1024,
        "estimate must reflect the buried 128KB scalar, got {estimate}"
    );

    let err = engine.write(mutation).unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("hard limit")),
        "2-level-buried large scalar must trip the gate, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// The same bypass at 3+ levels: `List([List([List([Text(128KB)])])])`.
/// Pre-fix this reached the depth cap far enough down that the child collapsed
/// to the ~1KB floor and admission ADMITTED it; the iterative estimator counts
/// the real size and REJECTS it (issue #1625).
#[test]
fn test_large_scalar_three_levels_deep_rejected() {
    let (_tmp, mut engine) = engine_with_64k_limit();

    // List([List([List([Text(128KB)])])]).
    let value = wrap_in_lists(Value::List(vec![Value::Text("x".repeat(128 * 1024))]), 2);
    let mutation = mutation_with_value(value);

    let estimate = engine.memtable.estimate_mutation_size(&mutation);
    assert!(
        estimate >= 128 * 1024,
        "estimate must reflect the buried 128KB scalar, got {estimate}"
    );

    let err = engine.write(mutation).unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("hard limit")),
        "3-level-buried large scalar must trip the gate, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// A large scalar buried BELOW where the old 32-level depth cap sat (33 wrapper
/// lists, so the innermost `Text(128KB)` is at depth 34) must still be counted
/// at its real size and rejected. Pre-fix, any node past depth 32 collapsed to
/// the shallow floor and this slipped past admission (issue #1625).
#[test]
fn test_large_scalar_below_old_depth_cap_rejected() {
    let (_tmp, mut engine) = engine_with_64k_limit();

    // 33 wrapper lists around Text(128KB): innermost scalar is well past the
    // old MAX_NESTING_DEPTH (32).
    let value = wrap_in_lists(Value::Text("x".repeat(128 * 1024)), 33);
    let mutation = mutation_with_value(value);

    let estimate = engine.memtable.estimate_mutation_size(&mutation);
    assert!(
        estimate >= 128 * 1024,
        "estimate must reflect the 128KB scalar below the old cap, got {estimate}"
    );

    let err = engine.write(mutation).unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("hard limit")),
        "scalar below old depth cap must trip the gate, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// A pathological WIDE value that exceeds `MAX_ESTIMATE_NODES` (1_000_000) must
/// fail CLOSED: the bounded iterative estimator stops at the node cap and
/// returns `usize::MAX`, so the mutation is REJECTED — without stack overflow,
/// arithmetic overflow, or hang. Construction is a single flat allocation so the
/// test runs fast (issue #1625 DoS guard).
#[test]
fn test_pathological_node_cap_fails_closed() {
    let (_tmp, mut engine) = engine_with_64k_limit();

    // A flat list of 1_000_001 small integers: outer node + 1_000_001 children
    // = 1_000_002 visited > MAX_ESTIMATE_NODES (1_000_000) → fail closed.
    let value = Value::List((0..1_000_001i32).map(Value::Integer).collect());
    let mutation = mutation_with_value(value);

    // Estimator saturates to usize::MAX (fail-closed), never a small under-count.
    let estimate = engine.memtable.estimate_mutation_size(&mutation);
    assert_eq!(
        estimate,
        usize::MAX,
        "hitting the node cap must fail closed with usize::MAX"
    );

    let err = engine.write(mutation).unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("fail-closed sentinel")),
        "node-cap fail-closed value must be rejected via the sentinel guard, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// The estimator's `usize::MAX` fail-closed sentinel must be rejected EXPLICITLY,
/// independent of `hard_limit`. With `hard_limit == usize::MAX` (a configurable
/// value), the `incoming > hard_limit` ceiling check is `usize::MAX > usize::MAX`
/// = false, so pre-fix the pathological mutation was ADMITTED, defeating the
/// fail-closed guard. The dedicated sentinel check makes admission REJECT it
/// regardless of how `hard_limit` is configured (issue #1625).
#[test]
fn test_sentinel_rejected_with_max_hard_limit() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_flush_threshold(1024 * 1024)
    .with_hard_limit(usize::MAX)
    .with_durability(Durability::Disabled);
    let mut engine = WriteEngine::new(config).unwrap();

    // Pathological wide value that trips MAX_ESTIMATE_NODES → estimator returns
    // usize::MAX.
    let value = Value::List((0..1_000_001i32).map(Value::Integer).collect());
    let mutation = mutation_with_value(value);
    assert_eq!(
        engine.memtable.estimate_mutation_size(&mutation),
        usize::MAX,
        "hitting the node cap must fail closed with usize::MAX"
    );

    let err = engine.write(mutation).unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("fail-closed sentinel")),
        "sentinel must be rejected even when hard_limit == usize::MAX, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// The projected-sum uses `saturating_add`, so a memtable size near `usize::MAX`
/// cannot overflow (a plain `+` would panic under debug overflow checks). The
/// write is cleanly rejected instead of panicking.
#[test]
fn test_projected_sum_saturating_no_overflow() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
    .with_durability(Durability::Disabled);

    let mut engine = WriteEngine::new(config).unwrap();
    // Force an unreachable near-max size to exercise the saturating add.
    engine.memtable.set_size_bytes_for_test(usize::MAX - 8);

    // A small mutation (< hard limit) passes the single-mutation ceiling but
    // saturates the projected sum to usize::MAX > hard limit → rejected, with no
    // arithmetic overflow/panic.
    let err = engine
        .write(create_test_mutation(1, "small", 1_000_000))
        .unwrap_err();
    assert!(
        matches!(err, Error::Storage(ref m) if m.contains("would exceed hard limit")),
        "near-usize::MAX size must reject via saturating add, got: {err:?}"
    );
}
