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

/// A deeply nested (> MAX_NESTING_DEPTH) value must be estimated large enough to
/// trip the hard-limit gate rather than counted as the old flat ~1KB. A
/// 500-element list parked at the depth cap estimates in the hundreds of KB, so
/// it exceeds a 64KB hard limit and is rejected.
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

    // 32 single-element wrapper lists with a wide list at the depth cap.
    let mut nested = Value::List((0..500).map(Value::Integer).collect());
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
        "deeply nested value must trip the hard-limit gate, got: {err:?}"
    );
    assert_eq!(engine.memtable_size(), 0);
}

/// A deep NARROW collection wrapping a LARGE direct scalar must also trip the
/// gate (issue #1625 roborev finding). Pre-fix, the depth-cap estimator scaled a
/// collection by element COUNT × 1024, so `List([Text(128KB)])` parked at the cap
/// was counted as ~1KB and passed a 64KB hard limit while retaining 128KB. The
/// shallow per-child estimate now counts the large direct scalar at its real
/// heap size, so the write is rejected.
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
