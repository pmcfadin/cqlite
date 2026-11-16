//! Issue #4531 — the OSS50 clustering encoding of `uuid` and `timeuuid` preserves the typed
//! comparator's order, ASC and DESC. (The #4497 per-type byte-comparable suite carries the same
//! uuid property on its own branch; its `#[ignore]` goes away once both are on main.)
//!
//! Property, over COMPLETE clusterings (`40 <component> 38`, `38` = `ByteSource.TERMINATOR`, the
//! `Kind.CLUSTERING` byte): `cmp(enc(x), enc(y)) == typed(x, y)`, reversed for DESC
//! (`ReversedType.compareCustom`, `ReversedType.java:95-98`).
//!
//! Typed comparators, ported independently of the crate's helpers from cassandra-5.0.8:
//! * `uuid`     — `UUIDType.compareCustom` (`UUIDType.java:74-119`)
//! * `timeuuid` — `AbstractTimeUUIDType.compareCustom` (`AbstractTimeUUIDType.java:57-87`):
//!   reordered timestamp, then the lsb with `signedBytesToNativeLong` (`:72-75`).
//!
//! `uuid` vs `timeuuid` is the DECLARED type, passed to `encode_clustering_bound_oss50_typed`.

use std::cmp::Ordering;

use cqlite_core::storage::sstable::bti::encode_clustering_bound_oss50_typed;
use cqlite_core::types::Value;
use proptest::prelude::*;

const KIND_CLUSTERING: u8 = 0x38;

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

fn enc(u: [u8; 16], reversed: bool, timeuuid: bool) -> Vec<u8> {
    let mut b = encode_clustering_bound_oss50_typed(&[Value::Uuid(u)], &[reversed], &[timeuuid])
        .expect("uuid encodes");
    b.push(KIND_CLUSTERING);
    b
}

fn msb(u: &[u8; 16]) -> u64 {
    u64::from_be_bytes(u[..8].try_into().expect("8"))
}
fn lsb(u: &[u8; 16]) -> u64 {
    u64::from_be_bytes(u[8..].try_into().expect("8"))
}
fn reorder(m: u64) -> u64 {
    (m << 48) | ((m << 16) & 0xFFFF_0000_0000) | (m >> 32)
}

fn uuid_type(a: &[u8; 16], b: &[u8; 16]) -> Ordering {
    let (m1, m2) = (msb(a), msb(b));
    let (v1, v2) = ((m1 >> 12) & 0xf, (m2 >> 12) & 0xf);
    if v1 != v2 {
        return v1.cmp(&v2);
    }
    let c = if v1 == 1 {
        (reorder(m1) as i64).cmp(&(reorder(m2) as i64))
    } else {
        m1.cmp(&m2)
    };
    c.then(lsb(a).cmp(&lsb(b)))
}

fn timeuuid_type(a: &[u8; 16], b: &[u8; 16]) -> Ordering {
    let c = (reorder(msb(a)) as i64).cmp(&(reorder(msb(b)) as i64));
    let s = |x: u64| (x ^ 0x0080_8080_8080_8080) as i64;
    c.then(s(lsb(a)).cmp(&s(lsb(b))))
}

fn bytes16() -> impl Strategy<Value = [u8; 16]> {
    prop::array::uniform16(prop_oneof![
        Just(0u8),
        Just(0x7f),
        Just(0x80),
        Just(0xff),
        any::<u8>()
    ])
}

/// A `uuid`: any of versions 0/1/3/4/5.
fn arb_uuid() -> impl Strategy<Value = [u8; 16]> {
    (
        prop_oneof![Just(0u8), Just(1), Just(3), Just(4), Just(5)],
        bytes16(),
    )
        .prop_map(|(ver, mut b)| {
            b[6] = (b[6] & 0x0f) | (ver << 4);
            b
        })
}

/// A `timeuuid`: always version 1.
fn arb_timeuuid() -> impl Strategy<Value = [u8; 16]> {
    bytes16().prop_map(|mut b| {
        b[6] = (b[6] & 0x0f) | 0x10;
        b
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn uuid_clustering_encoding_preserves_order(x in arb_uuid(), y in arb_uuid()) {
        for reversed in [false, true] {
            let typed = uuid_type(&x, &y);
            let want = if reversed { typed.reverse() } else { typed };
            prop_assert_eq!(enc(x, reversed, false).cmp(&enc(y, reversed, false)), want, "reversed={}", reversed);
        }
    }

    #[test]
    fn timeuuid_clustering_encoding_preserves_order(x in arb_timeuuid(), y in arb_timeuuid()) {
        for reversed in [false, true] {
            let typed = timeuuid_type(&x, &y);
            let want = if reversed { typed.reverse() } else { typed };
            prop_assert_eq!(enc(x, reversed, true).cmp(&enc(y, reversed, true)), want, "reversed={}", reversed);
        }
    }
}
