//! `vector<float, n>` value decoding (issue #4114).
//!
//! # The layout, from pinned `cassandra-5.0.8` source (never CQLite's own code, #3041)
//!
//! `VectorType`'s constructor picks BOTH its width and its serializer from the
//! ELEMENT type (`VectorType.java:94-101`):
//!
//! ```text
//! this.valueLengthIfFixed = elementType.isValueLengthFixed()
//!                         ? elementType.valueLengthIfFixed() * dimension
//!                         : super.valueLengthIfFixed();
//! this.serializer = elementType.isValueLengthFixed()
//!                 ? new FixedLengthSerializer() : new VariableLengthSerializer();
//! ```
//!
//! `FloatType.valueLengthIfFixed() == 4` (`FloatType.java:148-152`), so a
//! `vector<float, n>` is FIXED at exactly `4 * n` bytes and takes
//! `FixedLengthSerializer`, whose `split` (`VectorType.java:445-460`) slices
//! `dimension` successive 4-byte windows — reading NO prefix of any kind — and then
//! calls `checkConsumedFully`. Elements are big-endian IEEE-754 binary32
//! (`ByteBufferUtil.java:512-515` `bytes.getFloat(...)`; Java `ByteBuffer` defaults
//! to `BIG_ENDIAN`).
//!
//! Consequences that this module exists to enforce:
//!
//! * **NO value-length prefix.** `Cell.java:304,333` and
//!   `ClusteringPrefix.java:473,536` both delegate to
//!   `AbstractType.writeValue`/`skipValue`, which branch PURELY on
//!   `valueLengthIfFixed()` (`AbstractType.java:535-552`): `>= 0` writes the value
//!   RAW. Reading a phantom vint length is the #4114 defect — it consumed the first
//!   float's leading byte AS a length and, when that byte happened to make the
//!   arithmetic balance, returned a WRONG VALUE with exit 0.
//! * **NO element count and NO per-element framing.** The dimension comes from the
//!   DECLARED type and from nowhere else (#28).
//! * **Trailing bytes are an ERROR**, not something to ignore:
//!   `checkConsumedFully` (`VectorType.java:358-363`) throws
//!   "Unexpected N extraneous bytes after … value".
//! * **A zero-length value is an ERROR, never an empty vector.** Cassandra has no
//!   empty vector: `dimension <= 0` is rejected at construction
//!   (`VectorType.java:89-90`) and `rejectNullOrEmptyValue` (`:365-368`, reached
//!   from `validate` at `:515-517`/`:653-655`) throws
//!   `MarshalException("Invalid empty vector value")`. NULL is legal and distinct
//!   (`VectorType.java:409-414`: "we don't allow empty vectors, so we can just
//!   check for null").
//!
//! # Scope: `float` elements only, and every other element type is REFUSED BY NAME
//!
//! Issue #4114's subject is `vector<float, n>`. The shape generalises to any
//! fixed-width element, but "the shape generalises" is not evidence that the decode
//! is right, and there is no Cassandra-written fixture in this repo for any other
//! element type. So a non-`float` element is refused with a named error naming the
//! element type — never a fallback decode, and never a blob (#28).

use super::{vector_byte_width, FLOAT_ELEMENT_WIDTH};
use crate::error::{Error, Result};
use crate::schema::CqlType;
use crate::types::Value;

/// Refuse any vector element type this module does not implement, naming it.
///
/// The refusal is an `unsupported_format`, not a corruption: the DATA is fine and
/// CQLite is what is incomplete. `Ok(())` only for CQL `float`.
pub(crate) fn require_float_element(element: &CqlType, dimension: usize) -> Result<()> {
    if matches!(element, CqlType::Float) {
        return Ok(());
    }
    Err(Error::unsupported_format(format!(
        "vector element type {element:?} (dimension {dimension}) is not implemented: \
         CQLite decodes vector<float, n> only (issue #4114). The value is NOT \
         returned as a blob or a partial decode, because a vector's on-disk layout \
         depends entirely on its element type and guessing it would silently \
         produce wrong numbers."
    )))
}

/// Decode `dimension` big-endian `f32` elements from `data[offset..]`, advancing
/// `offset` by exactly `4 * dimension`.
///
/// This is the CELL / IN-STREAM entry point: the value is NOT length-prefixed, so
/// the width is taken from the declared dimension and the only rejection is having
/// fewer bytes left in the row than the type requires.
pub(crate) fn decode_float_vector_at(
    data: &[u8],
    offset: &mut usize,
    column_name: &str,
    dimension: usize,
) -> Result<Value> {
    let width = float_vector_width(column_name, dimension)?;
    let available = data.len().saturating_sub(*offset);
    if available < width {
        return Err(Error::corruption(format!(
            "Cell '{column_name}': vector<float, {dimension}> is a FIXED {width}-byte \
             value (4 bytes per element, no length prefix) but only {available} byte(s) \
             remain at offset {offset}"
        )));
    }
    let end = *offset + width;
    let value = float_elements(&data[*offset..end], column_name, dimension)?;
    *offset = end;
    Ok(value)
}

/// Decode a vector from a slice that is EXACTLY the value (the bounded
/// UDT-field / collection-element / tuple-component framing already delimited it).
///
/// Requires `data.len() == 4 * dimension` in BOTH directions: too few bytes is a
/// truncated value, and trailing bytes are `checkConsumedFully`'s
/// "extraneous bytes" error. Returns the consumed length alongside the value so a
/// reporting caller can assert exhaustion.
pub(crate) fn decode_float_vector_exact(
    data: &[u8],
    column_name: &str,
    dimension: usize,
) -> Result<(Value, usize)> {
    let width = float_vector_width(column_name, dimension)?;
    if data.len() != width {
        // A zero-length value lands here too, and that is deliberate: Cassandra has
        // no empty vector, so an empty buffer is a malformed value, never
        // `Vector([])`.
        return Err(Error::corruption(format!(
            "'{column_name}': vector<float, {dimension}> is a FIXED {width}-byte value \
             (4 bytes per element, no length prefix) but the framed value is {} byte(s){}",
            data.len(),
            if data.is_empty() {
                " — an EMPTY value is not an empty vector; Cassandra rejects it \
                 (VectorType.java:365-368, \"Invalid empty vector value\")"
            } else if data.len() > width {
                " — accepting it would silently ignore the extraneous trailing bytes \
                 Cassandra rejects (VectorType.java:358-363)"
            } else {
                " — the value is truncated"
            }
        )));
    }
    Ok((float_elements(data, column_name, dimension)?, width))
}

/// The `nom`-shaped entry point used by the schema-aware value parsers
/// (`parser::types::parse_cql_value_with_schema`), which consume a value off the
/// FRONT of a stream and return the remainder.
///
/// Fails closed as a nom `Verify` error — that signature carries no message, so
/// nothing here can explain itself; what matters is that it never falls through to
/// `parse_blob`, which is what read a phantom vint length (the #4114 defect).
pub(crate) fn parse_float_vector_nom<'a>(
    input: &'a [u8],
    element: &CqlType,
    dimension: usize,
) -> nom::IResult<&'a [u8], Value> {
    let verify = || nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify));
    require_float_element(element, dimension).map_err(|_| verify())?;
    let width = vector_byte_width(FLOAT_ELEMENT_WIDTH, dimension).ok_or_else(verify)?;
    if input.len() < width {
        return Err(verify());
    }
    let value = float_elements(&input[..width], "vector", dimension).map_err(|_| verify())?;
    Ok((&input[width..], value))
}

/// `4 * dimension`, or a named error when that product overflows `usize`.
fn float_vector_width(column_name: &str, dimension: usize) -> Result<usize> {
    vector_byte_width(FLOAT_ELEMENT_WIDTH, dimension).ok_or_else(|| {
        Error::corruption(format!(
            "'{column_name}': declared vector dimension {dimension} overflows the \
             addressable byte width (4 * {dimension})"
        ))
    })
}

/// Split `exactly_the_value` (already known to be `4 * dimension` bytes) into its
/// big-endian binary32 elements.
///
/// `Value::List` of [`Value::Float32`] — the same per-element representation the
/// scalar CQL `float` cell arm produces (`Value::Float32`, issue #1884), so a
/// vector element and a `float` column of the same bytes render identically, and
/// the same array shape `sstabledump` prints for a vector value.
fn float_elements(exactly_the_value: &[u8], column_name: &str, dimension: usize) -> Result<Value> {
    // `checked` even in the assert: a raw `FLOAT_ELEMENT_WIDTH * dimension` here
    // would itself panic on overflow in a debug build, which is the failure mode
    // the callers' `float_vector_width` exists to turn into an error.
    debug_assert_eq!(
        vector_byte_width(FLOAT_ELEMENT_WIDTH, dimension),
        Some(exactly_the_value.len())
    );
    // Bounded by the bytes actually in hand, so a declared dimension can never
    // drive an allocation larger than the value it describes.
    let mut elements = Vec::with_capacity(dimension);
    for (index, chunk) in exactly_the_value.chunks(FLOAT_ELEMENT_WIDTH).enumerate() {
        let bytes: [u8; FLOAT_ELEMENT_WIDTH] = chunk.try_into().map_err(|_| {
            Error::corruption(format!(
                "'{column_name}': vector<float, {dimension}> element {index} is \
                 {} byte(s), not {FLOAT_ELEMENT_WIDTH}",
                chunk.len()
            ))
        })?;
        elements.push(Value::Float32(f32::from_be_bytes(bytes)));
    }
    Ok(Value::List(elements))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Oracle: `.drive-issue-4114/format-authority.md`'s byte-level verification
    /// against the committed Cassandra-written fixture —
    /// `[1.0, 2.5, -3.75]` is `3f800000 40200000 c0700000`.
    const THREE: [u8; 12] = [
        0x3f, 0x80, 0x00, 0x00, 0x40, 0x20, 0x00, 0x00, 0xc0, 0x70, 0x00, 0x00,
    ];

    fn floats(v: &Value) -> Vec<f32> {
        match v {
            Value::List(items) => items
                .iter()
                .map(|i| match i {
                    Value::Float32(f) => *f,
                    other => panic!("a vector element must be Float32, got {other:?}"),
                })
                .collect(),
            other => panic!("a vector must decode to a List, got {other:?}"),
        }
    }

    #[test]
    fn twelve_raw_bytes_decode_to_three_big_endian_floats_with_no_prefix() {
        let mut offset = 0usize;
        let value = decode_float_vector_at(&THREE, &mut offset, "v3", 3).expect("a 12-byte vector");
        assert_eq!(floats(&value), vec![1.0f32, 2.5, -3.75]);
        assert_eq!(offset, 12, "exactly 4*n bytes consumed, no length prefix");
    }

    #[test]
    fn the_cursor_advances_by_exactly_the_declared_width_leaving_the_next_cell_intact() {
        // The #4114 defect was a DESYNC: a phantom vint length moved the cursor
        // somewhere other than the end of the value, corrupting every later column.
        let mut data = THREE.to_vec();
        data.extend_from_slice(&[0xde, 0xad]);
        let mut offset = 0usize;
        decode_float_vector_at(&data, &mut offset, "v3", 3).expect("a 12-byte vector");
        assert_eq!(&data[offset..], &[0xde, 0xad], "the tail must be untouched");
    }

    #[test]
    fn a_leading_byte_that_looks_like_a_vint_length_is_not_read_as_one() {
        // `0x0b` is the byte that made the mis-decode balance the row body and
        // return a wrong value at exit 0 (`vector_exact`). It must now be the high
        // byte of element 0 and nothing else.
        let bytes = [
            0x0b, 0x00, 0x00, 0x00, 0x3f, 0x80, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
        ];
        let mut offset = 0usize;
        let value = decode_float_vector_at(&bytes, &mut offset, "v3", 3).expect("a 12-byte vector");
        assert_eq!(
            floats(&value),
            vec![f32::from_be_bytes([0x0b, 0, 0, 0]), 1.0, 2.0]
        );
        assert_eq!(offset, 12);
    }

    #[test]
    fn too_few_bytes_is_refused_with_the_declared_width_named() {
        let mut offset = 0usize;
        let err = decode_float_vector_at(&THREE[..11], &mut offset, "v3", 3)
            .expect_err("11 bytes cannot hold a vector<float, 3>");
        let msg = err.to_string();
        assert!(
            msg.contains("vector<float, 3>") && msg.contains("12"),
            "{msg}"
        );
        assert_eq!(offset, 0, "a refused decode must not advance the cursor");
    }

    #[test]
    fn a_bounded_value_must_be_exactly_four_n_bytes_in_both_directions() {
        let (value, consumed) =
            decode_float_vector_exact(&THREE, "v3", 3).expect("an exact-width value");
        assert_eq!(floats(&value), vec![1.0f32, 2.5, -3.75]);
        assert_eq!(consumed, 12);

        let short = decode_float_vector_exact(&THREE[..8], "v3", 3)
            .expect_err("a truncated value is refused")
            .to_string();
        assert!(short.contains("truncated"), "{short}");

        let mut long = THREE.to_vec();
        long.push(0x00);
        let trailing = decode_float_vector_exact(&long, "v3", 3)
            .expect_err("trailing bytes are refused, per checkConsumedFully")
            .to_string();
        assert!(trailing.contains("extraneous"), "{trailing}");
    }

    #[test]
    fn an_empty_bounded_value_is_an_error_not_an_empty_vector() {
        let err = decode_float_vector_exact(&[], "v3", 3)
            .expect_err("Cassandra has no empty vector")
            .to_string();
        assert!(
            err.contains("EMPTY value is not an empty vector"),
            "the refusal must say why: {err}"
        );
    }

    #[test]
    fn n_equals_one_is_a_vector_not_a_bare_float() {
        // The degenerate lower bound, and the shape most easily confused with a
        // scalar `float`: it must still be a one-element sequence.
        let (value, consumed) =
            decode_float_vector_exact(&[0x3f, 0xc0, 0x00, 0x00], "v1", 1).expect("4 bytes, n=1");
        assert_eq!(floats(&value), vec![1.5f32]);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn a_dimension_whose_width_overflows_is_refused_rather_than_wrapped() {
        let err = decode_float_vector_exact(&[], "v", usize::MAX)
            .expect_err("4 * usize::MAX does not fit")
            .to_string();
        assert!(err.contains("overflows"), "{err}");
    }

    #[test]
    fn every_element_type_other_than_float_is_refused_by_name() {
        assert!(require_float_element(&CqlType::Float, 3).is_ok());
        for (element, named) in [
            (CqlType::Double, "Double"),
            (CqlType::Int, "Int"),
            (CqlType::BigInt, "BigInt"),
            (CqlType::Text, "Text"),
            (CqlType::TinyInt, "TinyInt"),
            (CqlType::Boolean, "Boolean"),
            (CqlType::List(Box::new(CqlType::Float)), "List"),
        ] {
            let err = require_float_element(&element, 3)
                .expect_err("only float is implemented (issue #4114 AC4)")
                .to_string();
            assert!(
                err.contains("not implemented") && err.contains("vector<float, n>"),
                "the refusal must be explicit about what IS implemented: {err}"
            );
            assert!(
                err.contains(named),
                "the refusal must NAME the element type it refused: {err}"
            );
        }
    }
}
