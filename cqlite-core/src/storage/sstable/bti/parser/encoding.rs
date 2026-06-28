//! Cassandra OSS50 **byte-comparable** clustering-bound encoding.
//!
//! These encoders reproduce the on-disk separator bytes the `Rows.db` row-index
//! trie stores (the SAME encoding), NOT CQLite's custom
//! [`ByteComparableEncoder`](crate::storage::sstable::bti::encoder::ByteComparableEncoder)
//! (which prepends a 1-byte type discriminator and so is byte-incompatible with
//! the on-disk trie).

use crate::{error::Error, storage::sstable::bti::node::BtiResult, types::Value};

/// Cassandra component separator used between clustering components in the
/// OSS50 byte-comparable form.  Mirrors `ByteSource.NEXT_COMPONENT` (value
/// `0x40`) emitted by `ClusteringComparator.asByteComparable` /
/// `ByteSource.withTerminator`/`ByteSource.of(...)` in
/// `org.apache.cassandra.utils.bytecomparable.ByteSource` (cassandra-5.0.0).
const OSS50_NEXT_COMPONENT: u8 = 0x40;

/// OSS50 variable-length byte-comparable encoding for `BytesType`/`UTF8Type`
/// (Cassandra `ByteSource`): every literal `0x00` byte is escaped as
/// `0x00 0xFE` (`ESCAPE` + `ESCAPED_0_CONT`) and the component is terminated
/// with `0x00 0xFF` (`ESCAPE` + `ESCAPED_0_DONE`).  This makes the encoding
/// weakly prefix-free, so a shorter value sorts before a longer value that
/// shares its prefix — matching the separators stored in a real `Rows.db`
/// trie.  (The project's `ByteComparableEncoder` uses a different,
/// non-Cassandra escape scheme and must NOT be used for trie-compatible bounds.)
fn encode_varlen_oss50(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0xFE);
        }
    }
    out.push(0x00);
    out.push(0xFF);
}

/// Encode a single clustering-component [`Value`] in Cassandra OSS50
/// **byte-comparable** form (the SAME encoding the `Rows.db` row-index trie
/// stores its separators in), appending to `out`.
///
/// This reproduces the per-type `AbstractType.asComparableBytes(...)` production
/// used by Cassandra to build the byte-comparable keys:
///
/// - `Int32Type` → `(v as u32 ^ 0x8000_0000)` big-endian (sign-flip, 4B).
/// - `LongType` → `(v as u64 ^ 0x8000_0000_0000_0000)` big-endian (8B).
/// - `ShortType`/`ByteType` (`smallint`/`tinyint`) → sign-flip big-endian.
/// - `BooleanType` → single byte `0x00`/`0x01`.
/// - `TimestampType` (`timestamp`) → `LongType`-style sign-flip 8B.
/// - `UUIDType`/`TimeUUIDType` → the 16 raw bytes.
/// - `UTF8Type`/`AsciiType` and `BytesType` (`blob`) → raw value bytes,
///   terminated by the OSS50 variable-length component terminator.
///
/// Any clustering type not enumerated here returns an explicit parse error
/// (NO silent wrong results — issue #28 no-heuristics mandate).
fn encode_clustering_component_oss50(value: &Value, out: &mut Vec<u8>) -> BtiResult<()> {
    match value {
        // int — Int32Type: sign-flip, big-endian (matches `wide_table` separators,
        // e.g. ck=8 → 80 00 00 08).
        Value::Integer(v) => {
            out.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes());
            Ok(())
        }
        // bigint — LongType: sign-flip, big-endian, 8 bytes.
        Value::BigInt(v) | Value::Counter(v) => {
            out.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
            Ok(())
        }
        // smallint — ShortType: sign-flip, big-endian, 2 bytes.
        Value::SmallInt(v) => {
            out.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes());
            Ok(())
        }
        // tinyint — ByteType: sign-flip, 1 byte.
        Value::TinyInt(v) => {
            out.push((*v as u8) ^ 0x80);
            Ok(())
        }
        // boolean — BooleanType: single 0x00/0x01 byte.
        Value::Boolean(b) => {
            out.push(if *b { 0x01 } else { 0x00 });
            Ok(())
        }
        // timestamp — TimestampType shares LongType's comparable form (8-byte
        // sign-flip big-endian of the millisecond value).
        Value::Timestamp(v) => {
            out.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
            Ok(())
        }
        // uuid / timeuuid — raw 16 bytes (already byte-comparable in the on-disk
        // separator form for this fixed-length type).
        Value::Uuid(bytes) => {
            out.extend_from_slice(bytes);
            Ok(())
        }
        // text / ascii — OSS50 variable-length byte-comparable encoding.
        Value::Text(s) => {
            encode_varlen_oss50(s.as_bytes(), out);
            Ok(())
        }
        // blob / inet — OSS50 variable-length byte-comparable encoding.
        Value::Blob(b) | Value::Inet(b) => {
            encode_varlen_oss50(b, out);
            Ok(())
        }
        other => Err(Error::Parse(format!(
            "BTI range_query: byte-comparable encoding not implemented for {:?}",
            other.data_type()
        ))),
    }
}

/// Encode a multi-component clustering bound (`&[Value]`) in Cassandra OSS50
/// byte-comparable form — the SAME encoding the `Rows.db` trie stores.
///
/// A single-component clustering encodes to the bare component bytes (matching
/// the `wide_table` fixture separators, e.g. ck=8 → `80 00 00 08`, NO framing).
/// Multi-component clusterings concatenate per-component byte-comparable
/// encodings separated by [`OSS50_NEXT_COMPONENT`] (`ByteSource.NEXT_COMPONENT`,
/// per `ClusteringComparator.asByteComparable`), with no leading/trailing frame
/// so a prefix bound sorts before any longer key sharing it.
pub fn encode_clustering_bound_oss50(values: &[Value]) -> BtiResult<Vec<u8>> {
    // All-ascending convenience: every component uses its base byte-comparable
    // form. Equivalent to `encode_clustering_bound_oss50_with_order` with all
    // `is_reversed = false`.
    let mut out = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(OSS50_NEXT_COMPONENT);
        }
        encode_clustering_component_oss50(v, &mut out)?;
    }
    Ok(out)
}

/// Encode a multi-component clustering bound applying each column's clustering
/// **ORDER** (ASC/DESC), producing Cassandra OSS50 byte-comparable separators
/// that sort in the SAME order the rows are physically written.
///
/// ## Why DESC must invert the component bytes
///
/// Cassandra wraps a `CLUSTERING ORDER BY (c DESC)` column's type in
/// `ReversedType`. `ReversedType.asComparableBytes` delegates to the base type's
/// byte-comparable production and then inverts the resulting `ByteSource` via
/// `ByteSource.invert(...)`, which complements every emitted data byte
/// (`b -> 0xFF ^ b`). Complementing every byte of a (weakly prefix-free)
/// byte-comparable encoding reverses its lexicographic order, so a DESCENDING
/// value order maps to an ASCENDING byte order. This is exactly what a `Rows.db`
/// row-index trie needs: separators are always stored in ascending *byte* order,
/// while the underlying clustering values run descending for a DESC column.
/// (cassandra-5.0.0 `org.apache.cassandra.db.marshal.ReversedType.asComparableBytes`
/// + `org.apache.cassandra.utils.bytecomparable.ByteSource.invert`.)
///
/// The inter-component framing byte ([`OSS50_NEXT_COMPONENT`], `0x40`) is emitted
/// by the *comparator* (`ClusteringComparator.asByteComparable`), NOT by a
/// component's type, so it is **not** inverted even when neighbouring components
/// are DESC — only the per-component byte-comparable bytes are complemented. This
/// matches Cassandra, where each `subtype(i)` (possibly a `ReversedType`) emits
/// its own (already inverted) byte source and the comparator separates them with
/// the un-inverted `NEXT_COMPONENT` byte.
///
/// `is_reversed[i]` MUST correspond positionally to `values[i]` (i.e. the schema
/// clustering-key order). A short `is_reversed` slice treats missing entries as
/// ascending (`false`).
pub fn encode_clustering_bound_oss50_with_order(
    values: &[Value],
    is_reversed: &[bool],
) -> BtiResult<Vec<u8>> {
    let mut out = Vec::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(OSS50_NEXT_COMPONENT);
        }
        if is_reversed.get(i).copied().unwrap_or(false) {
            // Encode the base component into a scratch buffer, then complement
            // every byte (ReversedType / ByteSource.invert).
            let mut scratch = Vec::new();
            encode_clustering_component_oss50(v, &mut scratch)?;
            for b in &scratch {
                out.push(0xFF ^ *b);
            }
        } else {
            encode_clustering_component_oss50(v, &mut out)?;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OSS50 byte-comparable clustering encoder (issue #832 Finding 1)
    /// reproduces the on-disk trie separator bytes.
    #[test]
    fn oss50_clustering_encoder() {
        // Single int component — bare, sign-flip big-endian (matches fixture).
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(8)]).unwrap(),
            vec![0x80, 0x00, 0x00, 0x08]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(-1)]).unwrap(),
            vec![0x7F, 0xFF, 0xFF, 0xFF]
        );
        // Ordering is preserved: -1 < 0 < 100 byte-comparably.
        let neg = encode_clustering_bound_oss50(&[Value::Integer(-1)]).unwrap();
        let zero = encode_clustering_bound_oss50(&[Value::Integer(0)]).unwrap();
        let pos = encode_clustering_bound_oss50(&[Value::Integer(100)]).unwrap();
        assert!(neg < zero && zero < pos);

        // bigint — 8-byte sign-flip BE.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::BigInt(1)]).unwrap(),
            vec![0x80, 0, 0, 0, 0, 0, 0, 0x01]
        );

        // Multi-component: int(1) + text("ab") joined with 0x40 NEXT_COMPONENT,
        // text terminated by the OSS50 end marker 0x00 0xFF.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(1), Value::Text("ab".to_string())])
                .unwrap(),
            vec![0x80, 0x00, 0x00, 0x01, 0x40, b'a', b'b', 0x00, 0xFF]
        );

        // Variable-length OSS50: text terminates with 0x00 0xFF; a literal 0x00
        // byte is escaped as 0x00 0xFE so it never collides with the terminator.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Text("a".to_string())]).unwrap(),
            vec![b'a', 0x00, 0xFF]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Blob(vec![0x01, 0x00, 0x02])]).unwrap(),
            vec![0x01, 0x00, 0xFE, 0x02, 0x00, 0xFF]
        );
        // Prefix-free ordering: "a" sorts before "ab".
        let a = encode_clustering_bound_oss50(&[Value::Text("a".to_string())]).unwrap();
        let ab = encode_clustering_bound_oss50(&[Value::Text("ab".to_string())]).unwrap();
        assert!(a < ab);

        // Unsupported clustering type errors out explicitly.
        assert!(encode_clustering_bound_oss50(&[Value::Float(1.0)]).is_err());
    }

    /// Order-aware OSS50 encoder (DESC clustering): a `CLUSTERING ORDER BY DESC`
    /// column wraps in Cassandra `ReversedType`, whose `asComparableBytes`
    /// complements every byte of the base byte-comparable form.
    #[test]
    fn oss50_clustering_encoder_reversed_order() {
        // DESC int(8): base 80 00 00 08, complemented -> 7F FF FF F7.
        assert_eq!(
            encode_clustering_bound_oss50_with_order(&[Value::Integer(8)], &[true]).unwrap(),
            vec![0x7F, 0xFF, 0xFF, 0xF7]
        );
        // ASC path is identical to the plain encoder.
        assert_eq!(
            encode_clustering_bound_oss50_with_order(&[Value::Integer(8)], &[false]).unwrap(),
            encode_clustering_bound_oss50(&[Value::Integer(8)]).unwrap()
        );

        // The decisive property: for DESC, larger VALUE => smaller BYTES.
        let enc = |v: i32| {
            encode_clustering_bound_oss50_with_order(&[Value::Integer(v)], &[true]).unwrap()
        };
        // Physical write order for DESC: 5,4,3,2,1,0. Their separator bytes must
        // be strictly ASCENDING in that same sequence.
        let write_order = [5, 4, 3, 2, 1, 0];
        for w in write_order.windows(2) {
            assert!(
                enc(w[0]) < enc(w[1]),
                "DESC: value {} written before {} must yield smaller separator bytes",
                w[0],
                w[1]
            );
        }

        // Mixed ASC/DESC: first column ASC int, second column DESC int.
        let mixed = encode_clustering_bound_oss50_with_order(
            &[Value::Integer(1), Value::Integer(8)],
            &[false, true],
        )
        .unwrap();
        assert_eq!(
            mixed,
            vec![0x80, 0x00, 0x00, 0x01, 0x40, 0x7F, 0xFF, 0xFF, 0xF7],
            "ASC component bare, 0x40 framing un-inverted, DESC component complemented"
        );

        // Mixed-order monotonicity.
        let m = |a: i32, b: i32| {
            encode_clustering_bound_oss50_with_order(
                &[Value::Integer(a), Value::Integer(b)],
                &[false, true],
            )
            .unwrap()
        };
        // Same first key (1), second key descending 9,8,7 -> ascending bytes.
        assert!(m(1, 9) < m(1, 8) && m(1, 8) < m(1, 7));
        // Different first key dominates regardless of second (ASC leading).
        assert!(m(1, 0) < m(2, 9));
    }
}
