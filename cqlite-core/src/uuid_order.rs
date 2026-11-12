//! Cassandra `uuid` / `timeuuid` ordering and byte-comparable production (issue #4534, #4531).
//!
//! `uuid` (`UUIDType`) and `timeuuid` (`TimeUUIDType`) do NOT order by their raw 16 bytes, and
//! they do not order the same way as each other. Both orderings, and the matching OSS50
//! `asComparableBytes` swizzles the `Rows.db` row index is built from, are ported here from the
//! pinned `cassandra-5.0.8` sources (`src/java/org/apache/cassandra/db/marshal/`):
//!
//! * `UUIDType.compareCustom` (`UUIDType.java:74-119`): version nibble first; for version 1 the
//!   reordered timestamp (signed long compare); otherwise unsigned msb; then UNSIGNED lsb.
//! * `AbstractTimeUUIDType.compareCustom` (`:57-87`): reordered timestamp, then the lsb compared
//!   with SIGNED per-byte semantics (`signedBytesToNativeLong`, `:72-75` — CASSANDRA-8730).
//! * `UUIDType.asComparableBytes` (`:122-140`) and `AbstractTimeUUIDType.asComparableBytes`
//!   (`:89-102`).
//!
//! `uuid` vs `timeuuid` is decided by the DECLARED column type, never by inspecting the bytes
//! (#28): every caller passes the schema's type. A `timeuuid` value that is not version 1 is a
//! `MarshalException` in Cassandra; here it simply orders by the same formulas.

use std::cmp::Ordering;

fn msb(u: &[u8; 16]) -> u64 {
    u64::from_be_bytes([u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7]])
}

fn lsb(u: &[u8; 16]) -> u64 {
    u64::from_be_bytes([u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]])
}

fn version(msb: u64) -> u64 {
    (msb >> 12) & 0xf
}

/// `AbstractTimeUUIDType.reorderTimestampBytes` (`:85-90`).
fn reorder_timestamp_bytes(input: u64) -> u64 {
    (input << 48) | ((input << 16) & 0xFFFF_0000_0000) | (input >> 32)
}

/// `UUIDType.compareCustom` — the order of a declared `uuid` column.
pub(crate) fn uuid_type_cmp(a: &[u8; 16], b: &[u8; 16]) -> Ordering {
    let (m1, m2) = (msb(a), msb(b));
    let (v1, v2) = (version(m1), version(m2));
    if v1 != v2 {
        return v1.cmp(&v2);
    }
    let first = if v1 == 1 {
        // reordered longs are non-negative (top bits are the 12-bit time_hi), so the signed
        // `Long.compare` equals the unsigned one.
        reorder_timestamp_bytes(m1).cmp(&reorder_timestamp_bytes(m2))
    } else {
        m1.cmp(&m2) // UnsignedLongs.compare
    };
    first.then_with(|| lsb(a).cmp(&lsb(b))) // UnsignedLongs.compare
}

/// `AbstractTimeUUIDType.compareCustom` — the order of a declared `timeuuid` column.
pub(crate) fn timeuuid_type_cmp(a: &[u8; 16], b: &[u8; 16]) -> Ordering {
    let first =
        (reorder_timestamp_bytes(msb(a)) as i64).cmp(&(reorder_timestamp_bytes(msb(b)) as i64));
    first.then_with(|| {
        // signedBytesToNativeLong: lsb ^ 0x0080808080808080, then a SIGNED long compare.
        let s = |x: u64| (x ^ 0x0080_8080_8080_8080) as i64;
        s(lsb(a)).cmp(&s(lsb(b)))
    })
}

/// `UUIDType.asComparableBytes` (`:122-140`): the 16 bytes a declared `uuid` contributes to a
/// byte-comparable key.
pub(crate) fn uuid_comparable_bytes(u: &[u8; 16]) -> [u8; 16] {
    let m = msb(u);
    let v = version(m);
    let hi = if v == 1 {
        reorder_timestamp_bytes(m)
    } else {
        (v << 60) | ((m >> 4) & 0x0FFF_FFFF_FFFF_F000) | (m & 0xFFF)
    };
    concat(hi, lsb(u))
}

/// `AbstractTimeUUIDType.asComparableBytes` (`:89-102`): reordered timestamp msb, then the lsb
/// XOR `0x8080808080808080`.
pub(crate) fn timeuuid_comparable_bytes(u: &[u8; 16]) -> [u8; 16] {
    concat(
        reorder_timestamp_bytes(msb(u)),
        lsb(u) ^ 0x8080_8080_8080_8080,
    )
}

fn concat(hi: u64, lo: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&hi.to_be_bytes());
    out[8..].copy_from_slice(&lo.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v1(time_low: u32, time_mid: u16, time_hi: u16, lsb_bytes: [u8; 8]) -> [u8; 16] {
        let mut u = [0u8; 16];
        u[0..4].copy_from_slice(&time_low.to_be_bytes());
        u[4..6].copy_from_slice(&time_mid.to_be_bytes());
        u[6..8].copy_from_slice(&((1u16 << 12) | (time_hi & 0x0fff)).to_be_bytes());
        u[8..].copy_from_slice(&lsb_bytes);
        u
    }

    /// The two orders differ ONLY in the lsb tie-break, at an identical v1 timestamp
    /// (CASSANDRA-8730): lsb byte 0x7f vs 0x80.
    #[test]
    fn lsb_tiebreak_is_unsigned_for_uuid_and_signed_for_timeuuid() {
        let lo = v1(5, 5, 5, [0x7f, 0, 0, 0, 0, 0, 0, 0]);
        let hi = v1(5, 5, 5, [0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            uuid_type_cmp(&lo, &hi),
            Ordering::Less,
            "unsigned: 0x7f < 0x80"
        );
        assert_eq!(
            timeuuid_type_cmp(&lo, &hi),
            Ordering::Greater,
            "signed: 0x7f > 0x80 (-128)"
        );
    }

    #[test]
    fn timeuuid_orders_by_timestamp_not_by_raw_time_low() {
        // later timestamp but smaller time_low: raw bytes would sort it first.
        let early = v1(0xFFFF_FFFF, 0, 1, [0; 8]);
        let late = v1(0, 0, 2, [0; 8]);
        assert!(early[..] > late[..], "raw bytes disagree");
        assert_eq!(timeuuid_type_cmp(&early, &late), Ordering::Less);
        assert_eq!(uuid_type_cmp(&early, &late), Ordering::Less);
    }

    #[test]
    fn comparable_bytes_are_order_isomorphic() {
        let samples = [
            v1(1, 0, 0, [0; 8]),
            v1(2, 0, 0, [0xff; 8]),
            v1(0, 1, 0, [0x7f, 0, 0, 0, 0, 0, 0, 0]),
            v1(0, 0, 1, [0x80, 0, 0, 0, 0, 0, 0, 0]),
            v1(0xFFFF_FFFF, 0xFFFF, 0x0FFF, [0xff; 8]),
        ];
        for a in &samples {
            for b in &samples {
                assert_eq!(
                    timeuuid_comparable_bytes(a).cmp(&timeuuid_comparable_bytes(b)),
                    timeuuid_type_cmp(a, b)
                );
                assert_eq!(
                    uuid_comparable_bytes(a).cmp(&uuid_comparable_bytes(b)),
                    uuid_type_cmp(a, b)
                );
            }
        }
        // mixed versions
        let mut v4 = [0x11u8; 16];
        v4[6] = 0x40 | (v4[6] & 0x0f);
        let mut v3 = [0xeeu8; 16];
        v3[6] = 0x30 | (v3[6] & 0x0f);
        for (a, b) in [(v4, v3), (v3, v4), (v4, samples[0]), (samples[0], v3)] {
            assert_eq!(
                uuid_comparable_bytes(&a).cmp(&uuid_comparable_bytes(&b)),
                uuid_type_cmp(&a, &b)
            );
        }
    }
}
