//! Shared low-level helpers for the CQL → Arrow converter (feature = "arrow").
//!
//! Split out of `arrow_convert` (epic #1116 file-size split, issue #3096 Phase 0a)
//! with no behaviour change. Everything here is used by more than one of the
//! converter's modules:
//!
//! * the fail-closed `i32` offset/byte guards ([`checked_offset`],
//!   [`checked_value_bytes`], [`checked_string_offsets`], [`checked_binary_offsets`])
//!   — issues #1486 / #2235;
//! * the `Frozen` unwrapping helpers ([`unwrap_frozen_type`], [`unwrap_frozen_value`]);
//! * [`bigint_to_i128`], shared with the sibling `arrow_decimal` rescaler;
//! * the [`Cells`] alias naming a column's pre-resolved, row-aligned cell slice.

use super::arrow_convert::ArrowConvertError;
use crate::schema::CqlType;
use crate::types::Value;

/// A column's pre-resolved, row-aligned cell slice (issue #1495): element `i` is
/// row `i`'s value for the column, or `None` when absent. Produced once by
/// [`transpose_columns`](super::arrow_columnar::transpose_columns).
pub(super) type Cells<'a> = &'a [Option<&'a Value>];

// ============================================================================
// Fail-closed i32 offset / byte-length guards
// ============================================================================

/// Convert an accumulated collection-element count to an Arrow 32-bit offset,
/// failing closed instead of silently wrapping.
///
/// Arrow `List`/`Map` offset buffers are `i32`-backed. A plain `usize as i32`
/// cast wraps to a **negative** value once the flattened element count of a
/// row group crosses `i32::MAX` (2,147,483,647) — exactly the wide-partition
/// case — producing non-monotonic offsets that either panic
/// `OffsetBuffer::new` (monotonicity assert, on a library data path) or yield
/// a structurally corrupt array. This returns
/// [`ArrowConvertError::InvalidValue`] at that boundary instead. See issue
/// #1486. Normal-size collections take the identical fast path.
#[inline]
pub(super) fn checked_offset(len: usize) -> Result<i32, ArrowConvertError> {
    i32::try_from(len).map_err(|_| {
        ArrowConvertError::InvalidValue(format!(
            "collection offset {} exceeds i32::MAX ({}); Arrow List/Map offsets \
             are 32-bit — split the row group (fewer rows) or export via LargeList",
            len,
            i32::MAX
        ))
    })
}

/// Fail closed when the **cumulative byte length** of a `Utf8`/`Binary` column
/// would overflow the `i32`-backed value-offset buffer.
///
/// `StringArray`/`BinaryArray` (unlike their `Large*` siblings) store value
/// end-offsets as `i32`. The offset of the last value equals the total byte
/// length of the column; once that total crosses `i32::MAX` (2 GiB) the arrow
/// builder either panics on the offset conversion or silently produces a
/// non-monotonic/corrupt buffer. Flight/export batches are bounded by **row
/// count** (default 8192), not bytes, so a batch of moderately wide text/blob
/// values (e.g. ≥256 KiB each — all well within Cassandra's per-value limits)
/// can cross this ceiling on a library data path. This returns
/// [`ArrowConvertError::InvalidValue`] at that boundary instead. This is the
/// scalar analogue of [`checked_offset`]'s List/Map element-count guard (issue
/// #1486); see issue #2235. Normal-size columns take the identical fast path.
#[inline]
pub(super) fn checked_value_bytes(total_bytes: usize) -> Result<(), ArrowConvertError> {
    if total_bytes > i32::MAX as usize {
        return Err(ArrowConvertError::InvalidValue(format!(
            "cumulative Utf8/Binary byte length {} exceeds i32::MAX ({}); Arrow \
             StringArray/BinaryArray value offsets are 32-bit — reduce the batch \
             row count (byte-bounded batching) or export via LargeUtf8/LargeBinary",
            total_bytes,
            i32::MAX
        )));
    }
    Ok(())
}

/// Guard the cumulative byte length of a nullable `Utf8` column before handing
/// it to `StringArray::from`. Saturating summation cannot itself overflow.
///
/// Generic over `AsRef<str>` so the wide-text scalar/element paths can guard on
/// **borrowed** `&str`/`Cow<str>` slices of the already-materialized row values
/// — the check runs before any owned copy is made, so the fail-closed path
/// never clones ~2 GiB just to reject it (issue #2235).
#[inline]
pub(super) fn checked_string_offsets<S: AsRef<str>>(
    values: &[Option<S>],
) -> Result<(), ArrowConvertError> {
    let total = values
        .iter()
        .flatten()
        .fold(0usize, |acc, s| acc.saturating_add(s.as_ref().len()));
    checked_value_bytes(total)
}

/// Guard the cumulative byte length of a nullable `Binary` column before
/// handing it to `BinaryArray::from`. Saturating summation cannot overflow.
///
/// Generic over `AsRef<[u8]>` so callers can guard on **borrowed** `&[u8]`
/// slices of the row values before any owned copy — see
/// [`checked_string_offsets`] (issue #2235).
#[inline]
pub(super) fn checked_binary_offsets<B: AsRef<[u8]>>(
    values: &[Option<B>],
) -> Result<(), ArrowConvertError> {
    let total = values
        .iter()
        .flatten()
        .fold(0usize, |acc, b| acc.saturating_add(b.as_ref().len()));
    checked_value_bytes(total)
}

// ============================================================================
// BigInt → i128 helper
// ============================================================================

/// Convert a `num_bigint::BigInt` to `i128`, sign-extending if necessary.
///
/// Uses the two's-complement big-endian representation via
/// `to_signed_bytes_be()` and sign-extends to 16 bytes before reinterpreting
/// as `i128`.  Returns an error if the value requires more than 16 bytes
/// (i.e. exceeds the i128 range).
pub(crate) fn bigint_to_i128(n: &num_bigint::BigInt) -> Result<i128, ArrowConvertError> {
    let tc_bytes = n.to_signed_bytes_be();
    if tc_bytes.len() > 16 {
        return Err(ArrowConvertError::InvalidValue(
            "BigInt value requires more than 16 bytes; cannot fit in i128".to_string(),
        ));
    }
    // Determine the sign-extension byte: 0x00 for non-negative, 0xFF for negative.
    let pad: u8 = if n.sign() == num_bigint::Sign::Minus {
        0xFF
    } else {
        0x00
    };
    let mut buf = [pad; 16];
    // Copy the two's-complement bytes into the *right* side of the buffer.
    buf[16 - tc_bytes.len()..].copy_from_slice(&tc_bytes);
    Ok(i128::from_be_bytes(buf))
}

// ============================================================================
// Frozen unwrapping
// ============================================================================

/// Unwrap nested `CqlType::Frozen` wrappers to reach the effective type.
///
/// `Frozen(Frozen(T))` → `T`. This handles the rare but valid case of
/// double-frozen types in schema definitions.
pub(crate) fn unwrap_frozen_type(cql_type: &CqlType) -> &CqlType {
    let mut t = cql_type;
    while let CqlType::Frozen(inner) = t {
        t = inner.as_ref();
    }
    t
}

/// Unwrap a `Value::Frozen(inner)` reference to its inner value.
///
/// Returns the inner value reference if `v` is `Frozen`, or the original
/// reference otherwise.  `None` (absent column value) is passed through.
pub(crate) fn unwrap_frozen_value(v: Option<&Value>) -> Option<&Value> {
    match v {
        Some(Value::Frozen(inner)) => Some(inner.as_ref()),
        other => other,
    }
}
