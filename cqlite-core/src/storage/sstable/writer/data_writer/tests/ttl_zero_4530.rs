//! Issue #4530 (roborev round 4) — TTL 0 is `NO_TTL`, never an immediately-expiring cell.
//!
//! Cassandra 5.0.8: `USING TTL 0` means no TTL (`Attributes.getTimeToLive`, `LivenessInfo.NO_TTL`),
//! and a cell is expiring only when `ttl != NO_TTL` (`AbstractCell.isExpiring`). The writer must
//! therefore serialize a TTL-0 write byte-for-byte like a plain write: no IS_EXPIRING flag, no TTL
//! or LDT, no min-TTL statistic. Asserted on the SERIALIZED output, not on the read-back, with a
//! negative control (a real TTL differs) so the comparison can see an expiring cell.

use super::super::*;
use super::support::*;
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::Value;

const TS: i64 = 1_000_000;

fn mutation(op: CellOperation, row_ttl: Option<u32>) -> Mutation {
    let mut m = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![op],
        TS,
        None,
    );
    // set directly: bypasses `Mutation::new`'s normalization, as a deserialized (WAL/JSON) mutation would
    m.ttl_seconds = row_ttl;
    m
}

fn write(m: &Mutation) -> Vec<u8> {
    let mut stats = create_test_stats();
    stats.min_timestamp = TS;
    let mut w = DataWriter::new(stats);
    w.write_row(m, &create_test_schema()).expect("write_row");
    w.finish().expect("finish")
}

fn plain() -> CellOperation {
    CellOperation::Write {
        column: "name".into(),
        value: Value::text("v".to_string()),
    }
}

fn with_ttl(ttl: u32) -> CellOperation {
    CellOperation::WriteWithTtl {
        column: "name".into(),
        value: Value::text("v".to_string()),
        ttl_seconds: ttl,
        local_deletion_time: None,
    }
}

#[test]
fn per_cell_ttl_zero_serializes_like_a_plain_write() {
    assert_eq!(
        write(&mutation(with_ttl(0), None)),
        write(&mutation(plain(), None))
    );
}

#[test]
fn row_level_ttl_zero_serializes_like_no_ttl() {
    assert_eq!(
        write(&mutation(plain(), Some(0))),
        write(&mutation(plain(), None))
    );
}

#[test]
fn per_cell_ttl_zero_with_row_level_ttl_zero_is_plain_too() {
    assert_eq!(
        write(&mutation(with_ttl(0), Some(0))),
        write(&mutation(plain(), None))
    );
}

/// Negative control: a REAL TTL does change the bytes, so the equalities above are meaningful.
#[test]
fn a_real_ttl_is_distinguishable() {
    assert_ne!(
        write(&mutation(with_ttl(3600), None)),
        write(&mutation(plain(), None))
    );
    assert_ne!(
        write(&mutation(plain(), Some(3600))),
        write(&mutation(plain(), None))
    );
}

#[test]
fn mutation_new_normalizes_ttl_zero() {
    let m = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![plain()],
        TS,
        Some(0),
    );
    assert_eq!(m.ttl_seconds, None);
}
