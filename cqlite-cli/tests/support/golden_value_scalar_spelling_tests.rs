//! Unit coverage for the per-type SPELLING predicates (issue #3726, roborev job 105).
//!
//! Every accepted spelling below is either QUOTED from `cassandra-5.0.8` (the
//! authorities are enumerated in the module doc) or MEASURED against what the CLI
//! emits for a committed fixture — never taken from taste. Every rule is pinned from
//! BOTH sides, so widening one back to "any text" reds a case here rather than
//! quietly restoring the suppression this narrowing removed.

use super::super::schema::NATIVE_OPAQUE;
use super::{is_blob_hex, is_canonical_timestamp, opaque_spelling_matches, DURATION_UNITS};

/// The narrowing's whole subject: `0x` + an even-length hex byte string, and nothing
/// else. `BytesType.toJSONString` is `"\"0x" + bytesToHex(buffer) + '"'`.
#[test]
fn a_blob_must_be_the_0x_hex_spelling() {
    // Measured on `test_basic.simple_table`'s `description`, truncated.
    assert!(is_blob_hex("0x94df07b2"));
    // The empty blob: `bytesToHex` of an empty buffer is the empty string.
    assert!(is_blob_hex("0x"));
    for not_a_blob in [
        "deadbeef", // the `getString` spelling — not what toJSONString emits
        "0xabc",    // odd digit count: not a byte string
        "0xzz",     // not hex at all
        "not-a-blob",
        "",
    ] {
        assert!(
            !is_blob_hex(not_a_blob),
            "`{not_a_blob}` is not a spelling BytesType.toJSONString can produce"
        );
    }
}

/// A timestamp is accepted only where `canon_timestamp` RECOGNISED one, asked as a
/// fixed point of its own output.
#[test]
fn a_timestamp_must_be_its_own_canonical_form() {
    // The canonical form `canon_typed` stores, and what both legitimate spellings
    // reduce to: Cassandra's `yyyy-MM-dd'T'HH:mm:ss.SSSX` at UTC and the CLI's
    // `2025-10-06 01:12:05.394+0000` (measured on `test_basic.simple_table`).
    assert!(is_canonical_timestamp("2025-10-06T01:12:05.394Z"));
    assert!(is_canonical_timestamp("2025-10-06T01:12:05Z"));
    for not_canonical in [
        // Legitimate INPUT spellings — but `canon_typed` stores their canonical
        // form, so seeing them in a `Canon` means no canonicalization happened.
        "2025-10-06 01:12:05.394+0000",
        "2025-10-06T01:12:05.394+00:00",
        // A non-zero offset: `canon_timestamp` declines it deliberately rather than
        // shifting the instant, so the gap must stop suppressing here.
        "2025-10-06T01:12:05.394+0100",
        "not-a-timestamp",
        "",
    ] {
        assert!(
            !is_canonical_timestamp(not_canonical),
            "`{not_canonical}` is not `canon_timestamp`'s own output, so the \
             canonicalizer did not read a timestamp there"
        );
    }
}

/// EVERY native opaque type the schema reader recognises must have a spelling rule.
///
/// Derived from `schema::NATIVE_OPAQUE` rather than curated, for the reason that
/// list exists (roborev job 21 F2): a native opaque type added to the reader with no
/// rule here would answer `None`, the caller would refuse to match it, and the
/// undecoded-golden gap would silently stop covering a position it legitimately
/// covers. This case is what turns that into a FAILURE.
#[test]
fn every_native_opaque_type_has_a_spelling_rule() {
    for name in NATIVE_OPAQUE {
        assert!(
            opaque_spelling_matches(name, "").is_some(),
            "`{name}` is a native opaque type with no spelling rule — add one \
             (with its cassandra-5.0.8 authority) rather than leaving the caller to \
             refuse every value of it"
        );
    }
    // And a name no authority has been read for answers `None`, so the caller
    // REFUSES rather than accepting arbitrary text. `vector` is deliberately chosen:
    // it is a real CQL 5.0 type this reader does not implement.
    assert_eq!(opaque_spelling_matches("vector", "anything"), None);
}

/// Each opaque type's accepted and refused spellings, in one table.
///
/// The accepted column is measured CLI output (`test_basic.simple_table` for
/// uuid/timeuuid/date/time/duration/inet, `test_comparator_order.collection_order`'s
/// `pair_set` for the compressed IPv6 and the nine-digit time); the refused column
/// is what the single `Canon::Text` arm used to accept.
#[test]
fn each_opaque_type_accepts_only_its_own_spelling() {
    let accepted: &[(&str, &[&str])] = &[
        ("uuid", &["15291a77-d739-4e73-8397-b787442f3a1f"]),
        ("timeuuid", &["78f64100-a251-11f0-a18d-d6726a637a4c"]),
        // ISO_LOCAL_DATE, plus the signed expanded-year form outside four digits.
        ("date", &["2025-06-18", "+10000-01-01", "-0001-12-31"]),
        ("time", &["01:12:05.394017000", "00:00:10.000000000"]),
        // `Duration.toString`: ordered `<digits><unit>` groups; the zero duration is
        // the empty string; `46702000000000ns` is what the CLI emits.
        (
            "duration",
            &[
                "46702000000000ns",
                "12h58m22s",
                "-3y6mo",
                "1mo2d3ms4us5ns",
                "",
            ],
        ),
        (
            "inet",
            &[
                "154.47.65.214",
                "2001:db8::1",
                "2001:0db8:0000:0000:0000:0000:0000:0001",
            ],
        ),
    ];
    for (name, spellings) in accepted {
        for spelling in *spellings {
            assert_eq!(
                opaque_spelling_matches(name, spelling),
                Some(true),
                "`{spelling}` is a spelling a `{name}` value has"
            );
        }
    }

    let refused: &[(&str, &[&str])] = &[
        (
            "uuid",
            // Not-a-uuid, then the three ways the shape can be wrong: too short,
            // a hyphen displaced, a non-hex digit.
            &[
                "not-a-uuid",
                "15291a77-d739-4e73-8397-b787442f3a1",
                "15291a77d739-4e73-8397-b787442f3a1f",
                "15291a77-d739-4e73-8397-b787442f3a1z",
                "",
            ],
        ),
        ("timeuuid", &["not-a-timeuuid", ""]),
        (
            "date",
            &["2025-6-18", "2025-06-18-01", "20250618", "not-a-date", ""],
        ),
        (
            "time",
            // Fixed width: `TimeSerializer.toString` always writes nine fraction
            // digits, so a millisecond-precision spelling is not its output.
            &[
                "01:12:05.394",
                "1:12:05.394017000",
                "01:12:05:394017000",
                "not-a-time",
                "",
            ],
        ),
        (
            "duration",
            &[
                "not-a-duration",
                "12",   // digits with no unit
                "h",    // a unit with no digits
                "-",    // a sign alone is not producible (the zero duration is "")
                "3s5m", // units out of `Duration.toString`'s emission order
                "3d3d", // a unit twice
                "3mo4",
            ],
        ),
        ("inet", &["not-an-ip", "154.47.65", "154.47.65.256", ""]),
    ];
    for (name, spellings) in refused {
        for spelling in *spellings {
            assert_eq!(
                opaque_spelling_matches(name, spelling),
                Some(false),
                "`{spelling}` is not a spelling a `{name}` value has, so it must not \
                 qualify as a decode of that type"
            );
        }
    }
}

/// `Duration.toString` emits its units in ONE order, and the scan depends on the
/// table being in it.
///
/// Quoted from `cassandra-5.0.8 cql3.Duration.toString`, whose appends are `"y"`,
/// `"mo"`, `"d"`, then (when `nanoseconds != 0`) `"h"`, `"m"`, `"s"`, `"ms"`,
/// `"us"`, `"ns"`. Pinned because reordering the table would silently accept a
/// duration spelling `toString` cannot produce.
#[test]
fn the_duration_unit_table_is_in_cassandras_emission_order() {
    assert_eq!(
        DURATION_UNITS,
        &["y", "mo", "d", "h", "m", "s", "ms", "us", "ns"]
    );
}
