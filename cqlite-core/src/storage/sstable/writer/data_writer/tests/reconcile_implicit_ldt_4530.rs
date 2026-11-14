//! Issue #4530 (roborev round 1) — the flush path ranks an expiring cell WITHOUT an explicit
//! `localDeletionTime` by the SAME value it stamps on disk.
//!
//! The implicit expiration is `now + ttl`, CAPPED (Cassandra 5.0.8
//! `ExpirationDateOverflowHandling` policy CAP, cap = `Cell.MAX_DELETION_TIME`; here `i32::MAX`),
//! with `now` read ONCE per merged row group and carried on `RowWrite::now_seconds` to emission.
//! Ranking uses `Cells.resolveRegular` (`Cells.java:79-129`): at equal timestamps both expiring ->
//! larger LDT wins, then LOWER TTL, then larger value.

use super::super::*;
use super::support::*;
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::Value;

const TS: i64 = 1_000_000;

fn mutation(op: CellOperation) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![op],
        TS,
        None,
    )
}

fn ttl_cell(value: &str, ttl: u32, ldt: Option<i32>) -> Mutation {
    mutation(CellOperation::WriteWithTtl {
        column: "name".into(),
        value: Value::text(value.to_string()),
        ttl_seconds: ttl,
        local_deletion_time: ldt,
    })
}

/// Winner's value after merging `a`,`b` in `order`; asserts ranking clock == emission clock.
fn winner(a: &Mutation, b: &Mutation) -> String {
    let schema = create_test_schema();
    let row = DataWriter::merge_row_group(&[a, b], &schema, false, None).expect("row");
    assert_eq!(row.ops.len(), 1);
    assert_eq!(
        row.ops[0].now_seconds, row.now_seconds,
        "ranking clock must be the emission clock"
    );
    match row.ops[0].op {
        CellOperation::WriteWithTtl {
            value: Value::Text(t),
            ..
        } => String::from_utf8_lossy(t).into_owned(),
        other => panic!("unexpected op {other:?}"),
    }
}

fn both_orders(a: &Mutation, b: &Mutation) -> String {
    let (x, y) = (winner(a, b), winner(b, a));
    assert_eq!(x, y, "winner depends on arrival order");
    x
}

#[test]
fn explicit_ldt_above_the_implicit_expiration_wins_in_both_orders() {
    let now = types::wall_clock_seconds();
    let ttl = 3600;
    let explicit = ttl_cell("explicit", ttl, Some(now + ttl as i32 + 1000));
    let implicit = ttl_cell("implicit", ttl, None);
    assert_eq!(both_orders(&explicit, &implicit), "explicit");
}

#[test]
fn explicit_ldt_below_the_implicit_expiration_loses_in_both_orders() {
    let now = types::wall_clock_seconds();
    let ttl = 3600;
    let explicit = ttl_cell("explicit", ttl, Some(now + ttl as i32 - 1000));
    let implicit = ttl_cell("implicit", ttl, None);
    assert_eq!(both_orders(&explicit, &implicit), "implicit");
}

/// The client timestamp has nothing to do with expiration: an enormous timestamp (which
/// previously fed `timestamp/1e6 + ttl` and could wrap) must not change the ranking.
#[test]
fn client_timestamp_never_enters_the_ldt() {
    let schema = create_test_schema();
    let big = i64::MAX / 2;
    let mk = |v: &str, ttl: u32| {
        Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::WriteWithTtl {
                column: "name".into(),
                value: Value::text(v.to_string()),
                ttl_seconds: ttl,
                local_deletion_time: None,
            }],
            big,
            None,
        )
    };
    // larger ttl => later implicit expiration => wins, whatever the (huge) client timestamp
    let (short, long) = (mk("short", 100), mk("long", 200));
    for (a, b) in [(&short, &long), (&long, &short)] {
        let row = DataWriter::merge_row_group(&[a, b], &schema, false, None).expect("row");
        match row.ops[0].op {
            CellOperation::WriteWithTtl {
                value: Value::Text(t),
                ..
            } => {
                assert_eq!(String::from_utf8_lossy(t), "long")
            }
            other => panic!("{other:?}"),
        }
    }
}

/// Overflow: `now + ttl` saturates at the cap for both cells, so they TIE on LDT and the LOWER
/// TTL wins (`Cells.java:120-125`) - identically in both orders, no wraparound, no panic, even for
/// a TTL beyond `i32::MAX`.
#[test]
fn overflowing_expirations_cap_and_fall_through_to_lower_ttl() {
    assert_eq!(
        types::implicit_expiration(2_000_000_000, 2_000_000_000),
        i32::MAX
    );
    assert_eq!(
        types::implicit_expiration(2_000_000_000, u32::MAX),
        i32::MAX
    );
    let a = ttl_cell("lower-ttl", 2_000_000_000, None);
    let b = ttl_cell("higher-ttl", 2_100_000_000, None);
    assert_eq!(both_orders(&a, &b), "lower-ttl");
    let c = ttl_cell("max-ttl", u32::MAX, None);
    assert_eq!(both_orders(&a, &c), "lower-ttl");
}
