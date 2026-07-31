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

/// `ByteSource.ESCAPE` (cassandra-5.0.8
/// `utils/bytecomparable/ByteSource.java:48`): both the escape introducer for a
/// literal `0x00` in the data AND the component terminator.
const OSS50_ESCAPE: u8 = 0x00;
/// `ByteSource.ESCAPED_0_CONT` (`ByteSource.java:51`): emitted for a run of
/// literal zeros, and as the TERMINATOR of a component whose data ends in `0x00`.
const OSS50_ESCAPED_0_CONT: u8 = 0xFE;
/// `ByteSource.ESCAPED_0_DONE` (`ByteSource.java:52`): closes an escape sequence
/// when the byte following the literal zero(s) is NOT itself a zero.
const OSS50_ESCAPED_0_DONE: u8 = 0xFF;

/// OSS50 variable-length byte-comparable encoding for `BytesType`/`UTF8Type` —
/// `ByteSource.AbstractEscaper` in cassandra-5.0.8
/// (`utils/bytecomparable/ByteSource.java:309-380`, `Version.OSS50`).
///
/// The escaper's contract, quoting the pinned source's own doc and worked
/// examples: "Escapes 0s as ESCAPE + zero or more ESCAPED_0_CONT +
/// ESCAPED_0_DONE. If the source ends in 0, we use ESCAPED_0_CONT to make sure
/// that the encoding remains smaller than that source with a further 0 at the
/// end."
///
/// ```text
/// "A\0\0B" → 41 00 FE FF 42 00
/// "A\0B\0" → 41 00 FF 42 00 FE
/// "A\0"    → 41 00 FE
/// "AB"     → 41 42 00
/// ```
///
/// So the terminator of a component whose data does NOT end in `0x00` is a
/// SINGLE `ESCAPE` byte — not `ESCAPE + ESCAPED_0_DONE`. That distinction is
/// load-bearing: the terminator must sort BELOW the `NEXT_COMPONENT` (`0x40`)
/// byte that introduces the following clustering component, so that a shorter
/// component sorts before a longer one sharing its prefix ("any permitted
/// separator byte will be smaller than the byte following the prefix"). A
/// two-byte `00 FF` terminator sorts ABOVE `00 40`, i.e. above EVERY key in the
/// subtree it is meant to bound — which silently truncated a `text`-clustering
/// slice to the tail of its bucket (issue #3032; a real Cassandra 5.0 `Rows.db`
/// separator for `('bo', 12)` is `40 62 6f 00 40 80 00 00 0c`, terminator `00`).
///
/// ## The EMPTY component (issue #3032 roborev N8)
///
/// An empty variable-length component encodes to a bare terminator — one `0x00` —
/// so a full empty `text` clustering component is `40 00`. This is NOT a guess: it
/// falls straight out of `AbstractEscaper.next()` (`bufpos == limit == 0`, not
/// `escaped`, so the first call returns `ESCAPE` and the second `END_OF_STREAM`),
/// and cassandra-5.0.8 `ClusteringComparator.java:271-273` states it outright in its
/// worked examples: `"", null, Clustering -> 40 00 3F 40` and
/// `"", 0000, Clustering -> 40 00 40 0000 40` — the empty first component is `40 00`
/// in both.
///
/// `ByteSource.NEXT_COMPONENT_EMPTY` (`0x3F`, the `3F` above) is therefore **NOT**
/// the encoding of an empty variable-length component. It replaces the leading
/// `0x40` only when `AbstractType.asComparableBytes` returns Java `null`, which
/// happens for a type whose production is `optionalSignedFixedLengthNumber` /
/// `optionalFixedLength` (`ByteSource.java:176-179`, `:740-743`: `null` when
/// `accessor.isEmpty(data)`) — i.e. an empty BUFFER for a fixed-length numeric.
/// `ClusteringComparator.java:328-329` says so explicitly: "null values for some
/// types (e.g. int, varint **but not text**) that are encoded as empty buffers".
/// Text/blob go through `ByteSource.of(accessor, data, version)`
/// (`ByteSource.java:91-94`), an `AccessorEscaper` that is never `null`.
///
/// That `0x3F` case is unreachable from this encoder by construction: a
/// [`Value::Integer`]/[`Value::BigInt`]/… always carries a full-width number, so
/// CQLite's value model cannot express the empty fixed-length buffer that produces
/// it. If a representable empty-fixed-length value is ever added, this function's
/// callers must emit `NEXT_COMPONENT_EMPTY` in place of `NEXT_COMPONENT` for it.
///
/// (The project's `ByteComparableEncoder` uses a different, non-Cassandra escape
/// scheme and must NOT be used for trie-compatible bounds.)
fn encode_varlen_oss50(bytes: &[u8], out: &mut Vec<u8>) {
    // Mirrors `AbstractEscaper.next()`'s `escaped` state machine exactly.
    let mut escaped = false;
    for &b in bytes {
        if escaped {
            if b == OSS50_ESCAPE {
                // A further zero continues the escape run (and is REPLACED by the
                // continuation byte — the literal zero is not re-emitted).
                out.push(OSS50_ESCAPED_0_CONT);
                continue;
            }
            // A non-zero byte closes the run, then is emitted unchanged.
            out.push(OSS50_ESCAPED_0_DONE);
            escaped = false;
        }
        out.push(b);
        if b == OSS50_ESCAPE {
            escaped = true;
        }
    }
    // End of data: a still-open escape run terminates with ESCAPED_0_CONT
    // (keeping the encoding below that of the same value plus a trailing zero);
    // otherwise the terminator is a single ESCAPE byte.
    out.push(if escaped {
        OSS50_ESCAPED_0_CONT
    } else {
        OSS50_ESCAPE
    });
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
            encode_varlen_oss50(s.as_ref(), out);
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
/// A [`OSS50_NEXT_COMPONENT`] (`0x40`) byte precedes **EVERY** component,
/// including the FIRST: `ClusteringComparator.ByteComparableClustering`
/// (cassandra-5.0.8 `ClusteringComparator.java:257-275`) "adds a NEXT_COMPONENT
/// byte before each component ... and finishes with a suitable byte for the
/// clustering kind", e.g. a COMPLETE `("A", 0005)` clustering →
/// `40 4100 40 0005 38`, whose last byte is the `Kind` byte
/// `ClusteringPrefix.Kind.CLUSTERING.asByteComparableValue(OSS50)` =
/// `ByteSource.TERMINATOR` (`0x38`), NOT another `NEXT_COMPONENT`. (Cassandra's own
/// javadoc renders that example as `... 0005 40` because it is written for
/// `Version.LEGACY`, where `CLUSTERING` maps to `ByteSource.NEXT_COMPONENT`
/// instead — `ClusteringPrefix.java:75-76`.) So a
/// single `int` clustering ck=8 encodes to `40 80 00 00 08`, which is exactly the
/// separator byte string a real `wide_table` `Rows.db` trie stores (issue #3002 —
/// the previous "bare component bytes, NO framing" claim was calibrated against a
/// mis-rooted traversal that never saw the root's `0x40` transition).
///
/// No TERMINATOR (`0x38`) is appended: row-index separators are `separatorGt` /
/// `nudge` PREFIXES of a clustering (`RowIndexWriter.add`/`complete`), not
/// complete clusterings, so no `ByteSource.TERMINATOR` is present on disk. A
/// prefix bound therefore still sorts before any longer key sharing it.
pub fn encode_clustering_bound_oss50(values: &[Value]) -> BtiResult<Vec<u8>> {
    // All-ascending convenience: every component uses its base byte-comparable
    // form. Equivalent to `encode_clustering_bound_oss50_with_order` with all
    // `is_reversed = false`.
    let mut out = Vec::new();
    for v in values {
        out.push(OSS50_NEXT_COMPONENT);
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
/// The framing byte ([`OSS50_NEXT_COMPONENT`], `0x40`) — emitted before EVERY
/// component including the first, per
/// `ClusteringComparator.ByteComparableClustering` — comes from the *comparator*,
/// NOT from a component's type, so it is **not** inverted even when the component
/// it precedes is DESC; only the per-component byte-comparable bytes are
/// complemented. This matches Cassandra, where each `subtype(i)` (possibly a
/// `ReversedType`) emits its own (already inverted) byte source and the comparator
/// prefixes each with the un-inverted `NEXT_COMPONENT` byte.
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
        out.push(OSS50_NEXT_COMPONENT);
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
    /// reproduces the on-disk trie separator bytes — including the leading
    /// `0x40 NEXT_COMPONENT` byte every component carries (issue #3002; the real
    /// `wide_table` `Rows.db` separator for ck=8 is `40 80 00 00 08`).
    #[test]
    fn oss50_clustering_encoder() {
        // Single int component — 0x40 NEXT_COMPONENT + sign-flip big-endian.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(8)]).unwrap(),
            vec![0x40, 0x80, 0x00, 0x00, 0x08]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(-1)]).unwrap(),
            vec![0x40, 0x7F, 0xFF, 0xFF, 0xFF]
        );
        // Ordering is preserved: -1 < 0 < 100 byte-comparably.
        let neg = encode_clustering_bound_oss50(&[Value::Integer(-1)]).unwrap();
        let zero = encode_clustering_bound_oss50(&[Value::Integer(0)]).unwrap();
        let pos = encode_clustering_bound_oss50(&[Value::Integer(100)]).unwrap();
        assert!(neg < zero && zero < pos);

        // bigint — 8-byte sign-flip BE behind the 0x40 framing byte.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::BigInt(1)]).unwrap(),
            vec![0x40, 0x80, 0, 0, 0, 0, 0, 0, 0x01]
        );

        // Multi-component: 0x40 before EACH of int(1) and text("ab") (the
        // `("A", 0005) -> 40 4100 40 0005` worked example in
        // `ClusteringComparator.ByteComparableClustering`), the text component
        // terminated by a SINGLE `ByteSource.ESCAPE` (0x00) byte.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Integer(1), Value::text("ab".to_string())])
                .unwrap(),
            vec![0x40, 0x80, 0x00, 0x00, 0x01, 0x40, b'a', b'b', 0x00]
        );

        // Variable-length OSS50 — the four worked examples in the pinned
        // cassandra-5.0.8 `ByteSource.AbstractEscaper` javadoc
        // (`utils/bytecomparable/ByteSource.java:314-317`), which are the
        // AUTHORITY for this encoding (issue #3032):
        //
        //     "A\0\0B" -> 41 00 FE FF 42 00
        //     "A\0B\0" -> 41 00 FF 42 00 FE
        //     "A\0"    -> 41 00 FE
        //     "AB"     -> 41 42 00
        for (data, expected) in [
            (
                vec![b'A', 0x00, 0x00, b'B'],
                vec![0x41u8, 0x00, 0xFE, 0xFF, 0x42, 0x00],
            ),
            (
                vec![b'A', 0x00, b'B', 0x00],
                vec![0x41u8, 0x00, 0xFF, 0x42, 0x00, 0xFE],
            ),
            (vec![b'A', 0x00], vec![0x41u8, 0x00, 0xFE]),
            (vec![b'A', b'B'], vec![0x41u8, 0x42, 0x00]),
        ] {
            let mut got = Vec::new();
            encode_varlen_oss50(&data, &mut got);
            assert_eq!(
                got, expected,
                "AbstractEscaper worked example for {data:02x?}"
            );
        }

        // Same, through the public clustering-bound entry point (framing byte
        // included). A `text` component terminates with a LONE 0x00 — the real
        // Cassandra 5.0 `Rows.db` separator for the compound clustering
        // `('bo', 12)` is `40 62 6f 00 40 80 00 00 0c` (issue #3032 fixture
        // `test_da/multiclustering_table`), which a `00 FF` terminator would sort
        // ABOVE instead of below.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::text("a".to_string())]).unwrap(),
            vec![0x40, b'a', 0x00]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::text("bo".to_string())]).unwrap(),
            vec![0x40, b'b', b'o', 0x00]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::blob(vec![0x01, 0x00, 0x02])]).unwrap(),
            vec![0x40, 0x01, 0x00, 0xFF, 0x02, 0x00]
        );
        // Weakly prefix-free ordering: "a" sorts before "ab", and a text bound
        // sorts BELOW every longer clustering that extends it — the terminator
        // (0x00) is smaller than the NEXT_COMPONENT byte (0x40) that would
        // introduce the following component.
        let a = encode_clustering_bound_oss50(&[Value::text("a".to_string())]).unwrap();
        let ab = encode_clustering_bound_oss50(&[Value::text("ab".to_string())]).unwrap();
        assert!(a < ab);
        let bo = encode_clustering_bound_oss50(&[Value::text("bo".to_string())]).unwrap();
        let bo_12 =
            encode_clustering_bound_oss50(&[Value::text("bo".to_string()), Value::Integer(12)])
                .unwrap();
        assert!(
            bo < bo_12,
            "a prefix bound on the FIRST clustering component must sort below every \
             full clustering that extends it: {bo:02x?} vs {bo_12:02x?}"
        );
        // And a trailing literal zero still sorts above the same value without it.
        let z0 = encode_clustering_bound_oss50(&[Value::blob(vec![b'A'])]).unwrap();
        let z1 = encode_clustering_bound_oss50(&[Value::blob(vec![b'A', 0x00])]).unwrap();
        let z2 = encode_clustering_bound_oss50(&[Value::blob(vec![b'A', 0x00, 0x00])]).unwrap();
        assert!(z0 < z1 && z1 < z2, "{z0:02x?} < {z1:02x?} < {z2:02x?}");

        // Unsupported clustering type errors out explicitly.
        assert!(encode_clustering_bound_oss50(&[Value::Float(1.0)]).is_err());
    }

    /// Order-aware OSS50 encoder (DESC clustering): a `CLUSTERING ORDER BY DESC`
    /// column wraps in Cassandra `ReversedType`, whose `asComparableBytes`
    /// complements every byte of the base byte-comparable form.
    #[test]
    fn oss50_clustering_encoder_reversed_order() {
        // DESC int(8): base 80 00 00 08, complemented -> 7F FF FF F7, behind the
        // UN-inverted 0x40 framing byte (emitted by the comparator, #3002).
        assert_eq!(
            encode_clustering_bound_oss50_with_order(&[Value::Integer(8)], &[true]).unwrap(),
            vec![0x40, 0x7F, 0xFF, 0xFF, 0xF7]
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
            vec![0x40, 0x80, 0x00, 0x00, 0x01, 0x40, 0x7F, 0xFF, 0xFF, 0xF7],
            "0x40 framing before EACH component (un-inverted), DESC component complemented"
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

    /// An EMPTY variable-length component encodes to a bare terminator: `40 00`
    /// (issue #3032 roborev N8). NOT `ByteSource.NEXT_COMPONENT_EMPTY` (`0x3F`).
    ///
    /// Authority (pinned cassandra-5.0.8, never CQLite's own behavior):
    ///  * `ByteSource.AbstractEscaper.next()` on an empty buffer — `bufpos == limit
    ///    == 0`, not `escaped` — returns `ESCAPE` (`0x00`) once, then `END_OF_STREAM`.
    ///  * `ClusteringComparator.java:271-272` states the result in its own worked
    ///    examples: `"", null, Clustering -> 40 00 3F 40` and
    ///    `"", 0000, Clustering -> 40 00 40 0000 40`.
    ///  * `0x3F` there is the SECOND component's marker, emitted only when
    ///    `asComparableBytes` returns Java `null` — an empty buffer for a
    ///    fixed-length numeric (`ByteSource.java:176-179`). Text/blob use
    ///    `AccessorEscaper`, which is never `null`
    ///    (`ClusteringComparator.java:328-329`: "e.g. int, varint but not text").
    #[test]
    fn oss50_empty_varlen_component_is_a_bare_terminator() {
        // text "" and blob b"" both encode to NEXT_COMPONENT + a lone ESCAPE.
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Text("".into())]).unwrap(),
            vec![0x40, 0x00]
        );
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Blob(Vec::new().into())]).unwrap(),
            vec![0x40, 0x00]
        );

        // The `"", 0000, Clustering` shape from the Cassandra javadoc: the empty
        // leading component is `40 00`, then the int component's own `0x40` framing
        // (`0000` there is schematic; CQLite emits the sign-flipped Int32Type form).
        assert_eq!(
            encode_clustering_bound_oss50(&[Value::Text("".into()), Value::Integer(0)]).unwrap(),
            vec![0x40, 0x00, 0x40, 0x80, 0x00, 0x00, 0x00]
        );

        // The load-bearing ORDER property: an empty component sorts below every
        // non-empty one, and below a longer key sharing it as a prefix.
        let empty = encode_clustering_bound_oss50(&[Value::Text("".into())]).unwrap();
        let a = encode_clustering_bound_oss50(&[Value::Text("a".into())]).unwrap();
        let nul = encode_clustering_bound_oss50(&[Value::Text("\u{0}".into())]).unwrap();
        assert!(empty < a, "empty text must sort before \"a\"");
        assert!(empty < nul, "empty text must sort before a single NUL");
        let empty_then_int =
            encode_clustering_bound_oss50(&[Value::Text("".into()), Value::Integer(0)]).unwrap();
        assert!(
            empty < empty_then_int,
            "the empty component's terminator must sort below the following 0x40"
        );
    }
}
