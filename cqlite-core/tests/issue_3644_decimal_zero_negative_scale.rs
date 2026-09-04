//! `ValueFormatter::format_decimal` at the zero-magnitude / negative-scale
//! boundary (issue #3644).
//!
//! A `decimal` whose unscaled magnitude is ZERO and whose scale is NEGATIVE
//! (`BigInteger.ZERO` at scale `-1` — Cassandra's `0E+1`) used to take the
//! positional branch and render `"0" + "0".repeat(-scale)`, i.e. `00`. That is
//! not a valid JSON number (JSON forbids a leading zero followed by a digit), so
//! the JSON egress had to quote it to keep the document parseable. It now takes
//! `format_decimal`'s existing bounded exponent form (`<digits>e<-scale>`, the
//! #1754 spelling) and renders `0e1`.
//!
//! HONEST SCOPE: `0e1` is NOT Java's `BigDecimal.toString()` spelling `0E+1`.
//! What is fixed here is JSON VALIDITY, not spelling parity. `format_decimal`
//! diverges from `BigDecimal.toString()` in wider ways that this change does not
//! touch (a non-zero magnitude at a negative scale: Java `1.23E+3` vs CQLite
//! `1230`; an adjusted exponent below −6: Java `1E-10` vs CQLite
//! `0.0000000001`). Those are a separate divergence class needing their own
//! per-column parity evidence, and no committed fixture covers them.
//!
//! `format_decimal` is private, so every case goes through the public
//! `ValueFormatter::format_value` — the same entry point every text egress
//! (JSON, CSV, table) uses.

use cqlite_core::types::Value;
use cqlite_core::util::value_fmt::ValueFormatter;

fn rendered(scale: i32, unscaled: Vec<u8>) -> String {
    ValueFormatter::format_value(&Value::Decimal { scale, unscaled })
}

/// The fixed case: zero magnitude at a negative scale renders in the exponent
/// form, which is a valid JSON number, and it PRESERVES the scale rather than
/// collapsing to `0` (`BigDecimal(0, -1)` and `BigDecimal(0, 0)` are different
/// values, and the old `00`/`0` renderings distinguished them).
#[test]
fn a_zero_magnitude_at_a_negative_scale_renders_in_exponent_form() {
    assert_eq!(rendered(-1, vec![0x00]), "0e1");
    assert_eq!(rendered(-2, vec![0x00]), "0e2");
    // Multi-byte zero (a legal two's-complement encoding of ZERO) is the same
    // value and must render identically — the trigger is the MAGNITUDE, not the
    // byte count.
    assert_eq!(rendered(-1, vec![0x00, 0x00]), "0e1");
    assert_eq!(rendered(-9, vec![0x00, 0x00, 0x00, 0x00]), "0e9");
}

/// Zero at scale 0 is unaffected: it is already a valid JSON number and stays
/// the bare `0`. The trigger is `scale < 0`, not `scale <= 0`.
#[test]
fn a_zero_magnitude_at_scale_zero_stays_a_bare_zero() {
    assert_eq!(rendered(0, vec![0x00]), "0");
    assert_eq!(rendered(0, vec![0x00, 0x00]), "0");
    // An EMPTY unscaled slice short-circuits before any branch; also `0`.
    assert_eq!(rendered(0, vec![]), "0");
    assert_eq!(rendered(-1, vec![]), "0");
}

/// Zero at a POSITIVE scale keeps its positional form — `0.00` is valid JSON and
/// was never part of the defect.
#[test]
fn a_zero_magnitude_at_a_positive_scale_stays_positional() {
    assert_eq!(rendered(2, vec![0x00]), "0.00");
    assert_eq!(rendered(1, vec![0x00]), "0.0");
}

/// A NON-zero magnitude at a negative scale MUST stay positional. `50` is
/// already a valid JSON number, so routing it through the exponent form would be
/// a gratuitous output change (and a wider divergence than this fix claims).
#[test]
fn a_non_zero_magnitude_at_a_negative_scale_stays_positional() {
    assert_eq!(rendered(-1, vec![0x05]), "50");
    assert_eq!(rendered(-2, vec![0x05]), "500");
    // Negative magnitude, negative scale — the sign is carried positionally too.
    assert_eq!(rendered(-1, vec![0xFB]), "-50"); // -5 × 10^1
                                                 // The smallest non-zero magnitudes on either side of zero.
    assert_eq!(rendered(-1, vec![0x01]), "10");
    assert_eq!(rendered(-1, vec![0xFF]), "-10");
}

/// The `scale == i32::MIN` boundary the existing code guards with
/// `unsigned_abs()`: `-scale` is not representable in `i32`, so the function
/// must stay panic-free. It is over the scale cap, so it took the exponent form
/// already; the zero trigger must not change that.
#[test]
fn the_extreme_negative_scale_stays_panic_free_in_exponent_form() {
    assert_eq!(rendered(i32::MIN, vec![0x00]), "0e2147483648");
    assert_eq!(rendered(i32::MIN, vec![0x05]), "5e2147483648");
    assert_eq!(rendered(i32::MAX, vec![0x00]), "0e-2147483647");
}
