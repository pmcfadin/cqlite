//! Type-aware skip of the `improvedMinMax` covered-clustering `Slice` inside the
//! `Statistics.db` STATS component (issue #1021).
//!
//! The `oa`/`da` STATS layout serializes, in place of the legacy min/max
//! clustering-value lists, a `clusteringTypes` list followed by a covered
//! `Slice` (two `ClusteringBound`s). To reach the `pendingRepair` / `isTransient`
//! fields that follow, that `Slice` must be walked PAST.
//!
//! Unlike the legacy lists, the bound VALUES are NOT length-prefixed: they are
//! raw comparator-encoded, exactly as Cassandra's
//! `ClusteringPrefix.Serializer.serializeValuesWithoutSize` writes them (verified
//! against cassandra-5.0.0). Each bound is:
//!
//! ```text
//! [byte kind ordinal][unsigned short size][values...]
//! ```
//!
//! where `values` is, per `serializeValuesWithoutSize`:
//!   * a sequence of 32-value BATCHES; each batch starts with an unsigned-VInt
//!     "header" carrying 2 bits per value (null / empty / present), then the raw
//!     bytes of every non-null, non-empty value in batch order.
//!   * each present value is written by `AbstractType.writeValue`, i.e. RAW
//!     fixed-width bytes for a fixed-length type (`valueLengthIfFixed() >= 0`),
//!     or `writeWithVIntLength` (unsigned-VInt length prefix + bytes) for a
//!     variable-length type.
//!
//! Skipping therefore mirrors `ClusteringBoundOrBoundary.Serializer.skipValues`
//! → `ClusteringPrefix.Serializer.skipValuesWithoutSize` → `AbstractType.skipValue`.
//! No value bytes are interpreted; only their lengths are computed. This is the
//! ONLY type knowledge required, so it is authoritative (no heuristics, #28): a
//! fixed-width type contributes exactly `valueLengthIfFixed()` bytes; any other
//! recognized type is variable (vint-length-prefixed); an UNRECOGNIZED type makes
//! the skip impossible (we cannot know fixed-vs-variable) and the caller reports
//! the trailing fields honestly as `Unparsed` rather than guessing.

use crate::error::Result;

/// How a clustering column's value is serialized inside a `ClusteringBound`,
/// resolved authoritatively from the column's persisted `AbstractType` name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClusteringValueLayout {
    /// `valueLengthIfFixed() >= 0` — exactly this many raw bytes, no length
    /// prefix (Cassandra `AbstractType.writeValue` fixed branch).
    Fixed(usize),
    /// Variable-length — an unsigned-VInt length prefix then that many bytes
    /// (`writeWithVIntLength`).
    Variable,
}

/// Resolve a clustering column's value layout from the fully-qualified Cassandra
/// `AbstractType` name as persisted by `AbstractTypeSerializer.serialize`
/// (`UTF8(type.toString())`).
///
/// Returns `None` for a type whose fixed-vs-variable nature this decoder does not
/// model — in that case the covered `Slice` cannot be skipped safely and the
/// caller must report the trailing repair fields as `Unparsed` (never guess).
///
/// The fixed lengths come straight from each type's `valueLengthIfFixed()`
/// override in cassandra-5.0.0 `org.apache.cassandra.db.marshal.*`. `ReversedType`
/// delegates to its base type, and frozen-collection / tuple / UDT comparators are
/// variable. Types that do NOT override `valueLengthIfFixed()` (all string/blob/
/// number/collection comparators) inherit `VARIABLE_LENGTH` and so are `Variable`
/// here.
///
/// AUTHORITY NOTE — do NOT "fix" the tinyint/smallint/date/time entries to fixed
/// widths. The serialization layer (`AbstractType.writeValue` / `writtenLength` /
/// `skipValue`) branches PURELY on `valueLengthIfFixed()`, NOT on the CQL type's
/// logical byte width. Verified against cassandra-5.0.0 (and 5.0.2) marshal source,
/// none of `ByteType` (tinyint) / `ShortType` (smallint) — both extending
/// `NumberType` — nor `SimpleDateType` (date) / `TimeType` (time) — both extending
/// `TemporalType` — override `valueLengthIfFixed()`. `NumberType`/`TemporalType` do
/// not override it either, so all four inherit `AbstractType.VARIABLE_LENGTH == -1`
/// and are written/skipped via `writeWithVIntLength` (unsigned-VInt length prefix +
/// bytes) — i.e. genuinely `Variable`. Classifying them as `Fixed(1/2/4/8)` would
/// read the first VALUE byte as the VInt length and misalign the cursor before
/// `pendingRepair`/`isTransient`. (`TimeUUIDType` is the inverse case: no direct
/// override but its parent `AbstractTimeUUIDType.valueLengthIfFixed()` returns 16,
/// hence `Fixed(16)`.)
pub(crate) fn resolve_clustering_value_layout(type_name: &str) -> Option<ClusteringValueLayout> {
    // Strip a `ReversedType(...)` wrapper: it delegates valueLengthIfFixed to the
    // wrapped base type (ReversedType.valueLengthIfFixed -> baseType...).
    let inner = strip_reversed(type_name);
    // The type CONSTRUCTOR is everything before the first `(` (parametric types
    // like `SetType(...)` carry their argument list there). Strip the package
    // prefix off that constructor so both fully-qualified and short names resolve;
    // a `.` inside the parenthesised argument list must not be mistaken for the
    // constructor's own package separator.
    let ctor = inner.split('(').next().unwrap_or(inner);
    let simple = ctor.rsplit('.').next().unwrap_or(ctor);

    // `VectorType(element , n)` — issue #4114. This CANNOT be decided from the
    // constructor name, which is why listing it below as `Variable` was a BUG:
    // `VectorType.valueLengthIfFixed()` is `element.valueLengthIfFixed() * n` when
    // the ELEMENT is fixed, and `VARIABLE_LENGTH` otherwise (VectorType.java:94-96
    // + AbstractType.java:62,490-493), so `vector<float, 3>` is Fixed(12) while
    // `vector<text, 3>` is genuinely Variable. One classification cannot cover both,
    // and the `ctor` computed above DISCARDS the argument list — so the element type
    // is parsed here and its own layout resolved recursively. That is exactly the
    // rule this module's AUTHORITY NOTE states: the serialization layer branches
    // PURELY on `valueLengthIfFixed()`.
    //
    // A malformed vector type is `None` (UNKNOWN), never a guessed width: the caller
    // then reports the trailing repair fields as `Unparsed`, which is the honest
    // outcome when the skip cannot be computed.
    if let Some(args_inner) = crate::schema::vector_type::marshal_vector_inner(inner) {
        let Ok(args) = crate::schema::vector_type::split_vector_args(args_inner, inner) else {
            return None;
        };
        return match resolve_clustering_value_layout(args.element)? {
            ClusteringValueLayout::Fixed(element_width) => {
                crate::schema::vector_type::vector_byte_width(element_width, args.dimension)
                    .map(ClusteringValueLayout::Fixed)
            }
            ClusteringValueLayout::Variable => Some(ClusteringValueLayout::Variable),
        };
    }

    // Fixed-width comparators (exact `valueLengthIfFixed()` from cassandra-5.0.0).
    let fixed = match simple {
        "BooleanType" => Some(1),
        "Int32Type" | "FloatType" => Some(4),
        "LongType" | "DoubleType" | "TimestampType" | "DateType" => Some(8),
        "UUIDType" | "TimeUUIDType" | "LexicalUUIDType" => Some(16),
        // EmptyType.valueLengthIfFixed() == 0: contributes no value bytes.
        "EmptyType" => Some(0),
        _ => None,
    };
    if let Some(n) = fixed {
        return Some(ClusteringValueLayout::Fixed(n));
    }

    // Recognized variable-length comparators that may legitimately appear as (or
    // wrap, for frozen) clustering columns — scalar variable types plus the
    // parametric collection / tuple / UDT comparators (each serialized as a
    // single opaque blob via `writeWithVIntLength`, so the parameterised argument
    // list need not be modeled). Anything not enumerated is treated as UNKNOWN
    // (None) so the walk fails honest-Unparsed rather than mis-skipping.
    let variable = matches!(
        simple,
        "UTF8Type"
            | "AsciiType"
            | "BytesType"
            | "IntegerType"
            | "DecimalType"
            | "InetAddressType"
            // tinyint / smallint / date / time: VARIABLE per cassandra-5.0.0 (no
            // valueLengthIfFixed() override — see AUTHORITY NOTE above). They are
            // vint-length-prefixed, NOT fixed 1/2/4/8.
            | "ByteType"
            | "ShortType"
            | "TimeType"
            | "SimpleDateType"
            | "DurationType"
            | "MapType"
            | "SetType"
            | "ListType"
            | "TupleType"
            | "UserType"
            | "FrozenType"
            | "CompositeType"
            | "DynamicCompositeType" // NOT "VectorType": it is handled ABOVE, from its ELEMENT's layout.
                                     // Leaving it here made CQLite read a phantom vint length Cassandra never
                                     // wrote for every fixed-element vector (issue #4114).
    );
    if variable {
        return Some(ClusteringValueLayout::Variable);
    }
    None
}

/// Unwrap a `ReversedType(inner)` to its inner type name; returns the input
/// unchanged when it is not a reversed type. The `ReversedType` constructor is
/// matched on its (package-stripped) leading name, then its single parenthesised
/// argument is returned.
fn strip_reversed(type_name: &str) -> &str {
    // Leading constructor name, package-stripped.
    let ctor_full = type_name.split('(').next().unwrap_or(type_name);
    let ctor = ctor_full.rsplit('.').next().unwrap_or(ctor_full);
    if ctor == "ReversedType" {
        if let Some(open) = type_name.find('(') {
            let body = &type_name[open + 1..];
            return body.strip_suffix(')').unwrap_or(body);
        }
    }
    type_name
}

/// A minimal cursor abstraction the skip logic reads through, so it can be shared
/// with the STATS-component `Cursor` without coupling to it.
pub(crate) trait ByteSkip {
    fn read_u8(&mut self) -> Result<u8>;
    fn read_u16(&mut self) -> Result<u16>;
    fn read_unsigned_vint(&mut self) -> Result<u64>;
    fn skip(&mut self, n: usize) -> Result<()>;
}

/// Skip the covered-clustering `Slice` (its two `ClusteringBound`s) given the
/// resolved per-column value layouts, mirroring
/// `ClusteringBoundOrBoundary.Serializer.skipValues`.
///
/// Returns `Ok(true)` when both bounds were fully skipped (the walk may continue
/// to `pendingRepair` / `isTransient`), or `Ok(false)` when a bound carries more
/// values than there are known clustering types, or any required type is unknown
/// — in which case the caller reports the trailing fields as `Unparsed`.
pub(crate) fn skip_covered_slice<C: ByteSkip>(
    c: &mut C,
    layouts: &[Option<ClusteringValueLayout>],
) -> Result<bool> {
    for _ in 0..2 {
        let _kind = c.read_u8()?; // ClusteringPrefix.Kind ordinal
        let size = c.read_u16()? as usize; // bound value count (writeShort)
        if size == 0 {
            continue;
        }
        if size > layouts.len() {
            // The bound claims more values than the clusteringTypes list
            // describes — we cannot know each value's length. Bail honestly.
            return Ok(false);
        }
        if !skip_values_without_size(c, size, layouts)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Mirror `ClusteringPrefix.Serializer.skipValuesWithoutSize`: 32-value header
/// batches, each value present/empty/null per its 2-bit header slot, present
/// non-empty values skipped per their type layout.
fn skip_values_without_size<C: ByteSkip>(
    c: &mut C,
    size: usize,
    layouts: &[Option<ClusteringValueLayout>],
) -> Result<bool> {
    let mut offset = 0usize;
    while offset < size {
        let header = c.read_unsigned_vint()?;
        let limit = size.min(offset + 32);
        while offset < limit {
            // 2 bits per value at position `offset` (modulo 32 within the batch,
            // which the shift's RH-operand modulus handles exactly as Cassandra's
            // `makeHeader` does). Bit `2*i+1` = null, bit `2*i` = empty.
            let shift = (offset * 2) % 64;
            let is_null = (header >> (shift + 1)) & 1 == 1;
            let is_empty = (header >> shift) & 1 == 1;
            if !is_null && !is_empty {
                match layouts[offset] {
                    Some(ClusteringValueLayout::Fixed(n)) => c.skip(n)?,
                    Some(ClusteringValueLayout::Variable) => {
                        // writeWithVIntLength: unsigned-VInt length then bytes.
                        let len = c.read_unsigned_vint()? as usize;
                        c.skip(len)?;
                    }
                    None => return Ok(false), // unknown type → cannot skip safely
                }
            }
            offset += 1;
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn fixed_widths_match_cassandra() {
        use ClusteringValueLayout::*;
        let cases = [
            ("org.apache.cassandra.db.marshal.Int32Type", Fixed(4)),
            ("org.apache.cassandra.db.marshal.LongType", Fixed(8)),
            ("org.apache.cassandra.db.marshal.TimestampType", Fixed(8)),
            ("org.apache.cassandra.db.marshal.DateType", Fixed(8)),
            ("org.apache.cassandra.db.marshal.DoubleType", Fixed(8)),
            ("org.apache.cassandra.db.marshal.FloatType", Fixed(4)),
            ("org.apache.cassandra.db.marshal.BooleanType", Fixed(1)),
            ("org.apache.cassandra.db.marshal.UUIDType", Fixed(16)),
            ("org.apache.cassandra.db.marshal.TimeUUIDType", Fixed(16)),
            ("org.apache.cassandra.db.marshal.LexicalUUIDType", Fixed(16)),
            ("org.apache.cassandra.db.marshal.EmptyType", Fixed(0)),
        ];
        for (name, want) in cases {
            assert_eq!(
                resolve_clustering_value_layout(name),
                Some(want),
                "type {name}"
            );
        }
    }

    #[test]
    fn variable_types_resolve_variable() {
        for name in [
            "org.apache.cassandra.db.marshal.UTF8Type",
            "org.apache.cassandra.db.marshal.AsciiType",
            "org.apache.cassandra.db.marshal.BytesType",
            "org.apache.cassandra.db.marshal.IntegerType",
            "org.apache.cassandra.db.marshal.DecimalType",
            "org.apache.cassandra.db.marshal.ByteType",
            "org.apache.cassandra.db.marshal.ShortType",
            "org.apache.cassandra.db.marshal.SimpleDateType",
            "org.apache.cassandra.db.marshal.TimeType",
        ] {
            assert_eq!(
                resolve_clustering_value_layout(name),
                Some(ClusteringValueLayout::Variable),
                "type {name}"
            );
        }
    }

    #[test]
    fn reversed_delegates_to_base() {
        assert_eq!(
            resolve_clustering_value_layout(
                "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)"
            ),
            Some(ClusteringValueLayout::Fixed(4))
        );
        assert_eq!(
            resolve_clustering_value_layout(
                "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)"
            ),
            Some(ClusteringValueLayout::Variable)
        );
    }

    #[test]
    fn parametric_collections_are_variable() {
        for name in [
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)",
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)",
        ] {
            assert_eq!(
                resolve_clustering_value_layout(name),
                Some(ClusteringValueLayout::Variable),
                "type {name}"
            );
        }
    }

    #[test]
    fn unknown_type_is_none() {
        assert_eq!(
            resolve_clustering_value_layout("org.apache.cassandra.db.marshal.SomeFutureType"),
            None
        );
    }

    /// Tiny in-memory cursor implementing `ByteSkip` for the slice-skip tests.
    struct Buf<'a> {
        b: &'a [u8],
        p: usize,
    }
    impl ByteSkip for Buf<'_> {
        fn read_u8(&mut self) -> Result<u8> {
            let v = *self
                .b
                .get(self.p)
                .ok_or_else(|| Error::Corruption("eof".into()))?;
            self.p += 1;
            Ok(v)
        }
        fn read_u16(&mut self) -> Result<u16> {
            let hi = self.read_u8()? as u16;
            let lo = self.read_u8()? as u16;
            Ok((hi << 8) | lo)
        }
        fn read_unsigned_vint(&mut self) -> Result<u64> {
            // Single-byte vints suffice for these tests.
            Ok(self.read_u8()? as u64)
        }
        fn skip(&mut self, n: usize) -> Result<()> {
            let end = self
                .p
                .checked_add(n)
                .filter(|&e| e <= self.b.len())
                .ok_or_else(|| Error::Corruption("eof".into()))?;
            self.p = end;
            Ok(())
        }
    }

    #[test]
    fn skip_empty_slice() {
        // start bound kind=1 size=0, end bound kind=6 size=0.
        let bytes = [1u8, 0, 0, 6, 0, 0];
        let mut c = Buf { b: &bytes, p: 0 };
        assert!(skip_covered_slice(&mut c, &[]).unwrap());
        assert_eq!(c.p, bytes.len());
    }

    #[test]
    fn skip_slice_with_fixed_int_values() {
        // One Int32 clustering column. Each bound carries size=1, one present
        // value (header 0x00 => present), 4 raw bytes.
        let layouts = vec![Some(ClusteringValueLayout::Fixed(4))];
        let mut bytes = Vec::new();
        for kind in [1u8, 6u8] {
            bytes.push(kind);
            bytes.extend_from_slice(&1u16.to_be_bytes()); // size = 1
            bytes.push(0x00); // header: value 0 present (not null/empty)
            bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]); // 4 raw bytes
        }
        let mut c = Buf { b: &bytes, p: 0 };
        assert!(skip_covered_slice(&mut c, &layouts).unwrap());
        assert_eq!(c.p, bytes.len());
    }

    #[test]
    fn skip_slice_with_variable_value() {
        // One UTF8 clustering column; one present value of length 3.
        let layouts = vec![Some(ClusteringValueLayout::Variable)];
        let mut bytes = Vec::new();
        bytes.push(1); // start kind
        bytes.extend_from_slice(&1u16.to_be_bytes()); // size = 1
        bytes.push(0x00); // header present
        bytes.push(3); // vint length
        bytes.extend_from_slice(b"abc");
        bytes.push(6); // end kind
        bytes.extend_from_slice(&0u16.to_be_bytes()); // empty end bound
        let mut c = Buf { b: &bytes, p: 0 };
        assert!(skip_covered_slice(&mut c, &layouts).unwrap());
        assert_eq!(c.p, bytes.len());
    }

    #[test]
    fn skip_slice_unknown_type_bails_false() {
        let layouts = vec![None];
        let mut bytes = Vec::new();
        bytes.push(1);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(0x00);
        // (no value bytes follow because we expect a bail before reading them)
        let mut c = Buf { b: &bytes, p: 0 };
        assert!(!skip_covered_slice(&mut c, &layouts).unwrap());
    }

    #[test]
    fn skip_slice_size_exceeds_types_bails_false() {
        let layouts = vec![Some(ClusteringValueLayout::Fixed(4))];
        let mut bytes = Vec::new();
        bytes.push(1);
        bytes.extend_from_slice(&2u16.to_be_bytes()); // size 2 > 1 type
        let mut c = Buf { b: &bytes, p: 0 };
        assert!(!skip_covered_slice(&mut c, &layouts).unwrap());
    }

    /// tinyint / smallint / date / time clustering values are VARIABLE-length in
    /// cassandra-5.0.0 (they do not override `valueLengthIfFixed()`), so each is
    /// serialized with an unsigned-VInt length prefix even though its natural CQL
    /// width is 1/2/4/8. These tests pin that the covered-slice skip advances by
    /// `1 (header) + 1 (vint len) + width` per present value and lands exactly at
    /// the byte where `pendingRepair`/`isTransient` would begin — guarding against
    /// the misclassification that would read the first VALUE byte as a VInt length.
    fn assert_variable_covered_skip(type_name: &str, natural_width: usize) {
        // Confirm the authoritative classification first.
        assert_eq!(
            resolve_clustering_value_layout(type_name),
            Some(ClusteringValueLayout::Variable),
            "type {type_name} must resolve Variable (no valueLengthIfFixed override)"
        );
        let layouts = vec![Some(ClusteringValueLayout::Variable)];
        let value: Vec<u8> = (0..natural_width as u8).collect();
        let mut bytes = Vec::new();
        // start bound: kind, size=1, header (present), vint len, value bytes.
        bytes.push(1u8);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(0x00); // value 0 present
        bytes.push(natural_width as u8); // single-byte unsigned vint length
        bytes.extend_from_slice(&value);
        let start_len = bytes.len();
        // end bound: empty.
        bytes.push(6u8);
        bytes.extend_from_slice(&0u16.to_be_bytes());
        // A trailing sentinel standing in for pendingRepair's first byte: the walk
        // must STOP before it.
        bytes.push(0xAB);

        let mut c = Buf { b: &bytes, p: 0 };
        assert!(
            skip_covered_slice(&mut c, &layouts).unwrap(),
            "covered slice for {type_name} should skip cleanly"
        );
        // Cursor lands at the end bound's terminator, just before the sentinel.
        assert_eq!(
            c.p,
            bytes.len() - 1,
            "type {type_name}: skip must advance past kind+size+header+vint+{natural_width}B value \
             and the empty end bound, stopping before pendingRepair"
        );
        // And specifically the start bound consumed kind+size+header+vint+width.
        assert_eq!(start_len, 1 + 2 + 1 + 1 + natural_width);
    }

    #[test]
    fn skip_slice_tinyint_clustering_advances_vint_prefixed() {
        // ByteType (tinyint) natural width 1 — but vint-length-prefixed.
        assert_variable_covered_skip("org.apache.cassandra.db.marshal.ByteType", 1);
    }

    #[test]
    fn skip_slice_smallint_clustering_advances_vint_prefixed() {
        // ShortType (smallint) natural width 2 — vint-length-prefixed.
        assert_variable_covered_skip("org.apache.cassandra.db.marshal.ShortType", 2);
    }

    #[test]
    fn skip_slice_date_clustering_advances_vint_prefixed() {
        // SimpleDateType (date) natural width 4 — vint-length-prefixed.
        assert_variable_covered_skip("org.apache.cassandra.db.marshal.SimpleDateType", 4);
    }

    #[test]
    fn skip_slice_time_clustering_advances_vint_prefixed() {
        // TimeType (time) natural width 8 — vint-length-prefixed.
        assert_variable_covered_skip("org.apache.cassandra.db.marshal.TimeType", 8);
    }

    #[test]
    fn skip_slice_null_value_consumes_no_bytes() {
        // size=1 but value 0 is null (header bit 1 set => 0b10 = 0x02).
        let layouts = vec![Some(ClusteringValueLayout::Fixed(4))];
        let mut bytes = Vec::new();
        bytes.push(1);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(0x02); // header: value 0 is null
        bytes.push(6);
        bytes.extend_from_slice(&0u16.to_be_bytes());
        let mut c = Buf { b: &bytes, p: 0 };
        assert!(skip_covered_slice(&mut c, &layouts).unwrap());
        assert_eq!(c.p, bytes.len());
    }
}
