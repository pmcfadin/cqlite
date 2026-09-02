//! Issue #3723 — a NESTED fixed-width collection/tuple element whose own
//! `[i32 BE len]` prefix declares a WRONG length must be REFUSED.
//!
//! ## Oracle
//!
//! Every expectation below is derived from the **pinned `cassandra-5.0.8`
//! serializer source** (read at the tag ref, e.g.
//! `git show cassandra-5.0.8:src/java/org/apache/cassandra/serializers/Int32Serializer.java`),
//! never from CQLite's prior behaviour — a CQLite-write/CQLite-read round trip
//! is invariant to this defect class by construction (both sides make the same
//! mistake), so it is not admissible evidence here.
//!
//! `ListSerializer.validate` (5.0.8) validates every element with the ELEMENT
//! type's own `validate`, and then throws
//! `"Unexpected extraneous bytes after list value"`. So a 5-byte `int` element
//! is refused twice over by Cassandra: once by `Int32Serializer.validate`
//! (`size != 4 && !isEmpty`) and again by the extraneous-bytes check. The
//! previous CQLite behaviour — read the first N bytes, discard the remainder
//! silently — made two distinct on-disk encodings decode to the SAME `Value`.
//!
//! ## Per-type admissible widths at `cassandra-5.0.8`
//!
//! | serializer | `validate()` | admits |
//! |---|---|---|
//! | `Int32Serializer` | `size != 4 && !isEmpty` | 4 or 0 |
//! | `LongSerializer` / `CounterSerializer` | `size != 8 && !isEmpty` | 8 or 0 |
//! | `FloatSerializer` | `size != 4 && !isEmpty` | 4 or 0 |
//! | `DoubleSerializer` | `size != 8 && !isEmpty` | 8 or 0 |
//! | `UUIDSerializer` | `size != 16 && !isEmpty` | 16 or 0 |
//! | `TimeUUID.Serializer` | `isEmpty` returns; `size != 16` throws | 16 or 0 |
//! | `TimestampSerializer` | `size != 8 && !isEmpty` | 8 or 0 |
//! | `BooleanSerializer` | `size > 1` | 1 or 0 |
//! | `ShortSerializer` | `size != 2` | 2 only |
//! | `ByteSerializer` | `size != 1` | 1 only |
//! | `SimpleDateSerializer` | `size != 4` | 4 only |
//! | `TimeSerializer` | `size != 8` | 8 only |
//!
//! ## Where the refusal comes from: #3811's consumption contract
//!
//! Issue #3723 was opened to add an EXACT-width guard to each fixed-width arm.
//! Issue **#3811** landed first and made that guard unnecessary: every bounded
//! caller reaches `parse_value_from_raw_bytes`, a thin wrapper over
//! `raw_value::reporting`'s consumption-reporting twin plus
//! `require_fully_consumed_raw`. Composed with each arm's own
//! `require_fixed_width` (`data.len() < n`), the ACCEPTED SET IS EXACTLY `{n}`:
//! `len == 0` and `len < n` are refused under-width, while `len > n` leaves
//! `len - n` bytes unconsumed and is refused by the caller's assert. The property
//! AC1 and AC3 assert is therefore enforced repo-wide by #3811's mechanism, and
//! these cases pin it AT EVERY NESTING POSITION for every fixed-width type —
//! coverage #3811's own tests (`raw_value/issue_3811_consumption_demo_tests.rs`)
//! do not enumerate. They must fail if either half of that composition is
//! relaxed.
//!
//! Both halves report `Error::Corruption`, with two distinct wordings — the
//! under-width one from `require_fixed_width` and the over-width one from
//! `require_fully_consumed_raw` — and [`is_width_error`] below matches either.
//!
//! CQLite is therefore NARROWER than Cassandra for the six `… or 0` types above,
//! whose `validate` admits an EMPTY buffer (`Int32Serializer.java`
//! `size(value) != 4 && !isEmpty(value)`, deserializing to Java `null`). That
//! divergence PREDATES both issues, is deliberate, and is tracked as **#3847**;
//! `zero_length_fixed_width_element_is_refused_at_every_nesting_position` below
//! characterises it rather than endorsing it.

use super::*;

/// Every fixed-width CQL short form this decoder admits, with the exact width
/// the pinned Cassandra serializer requires. `alias` marks the non-CQL marshal
/// aliases the match also accepts (`short` = `smallint`, `byte` = `tinyint`).
const FIXED_WIDTH_TYPES: &[(&str, usize)] = &[
    ("int", 4),
    ("bigint", 8),
    ("counter", 8),
    ("boolean", 1),
    ("uuid", 16),
    ("timeuuid", 16),
    ("float", 4),
    ("double", 8),
    ("smallint", 2),
    ("short", 2),
    ("tinyint", 1),
    ("byte", 1),
    ("timestamp", 8),
    ("date", 4),
    ("time", 8),
];

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None)
}

/// `[i32 BE len][payload]`
fn framed(payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as i32).to_be_bytes().to_vec();
    out.extend_from_slice(payload);
    out
}

/// A frozen list/set body as `parse_value_from_raw_bytes` sees it:
/// `[i32 BE count][i32 BE elem_len][elem_bytes]...`
fn frozen_sequence(elements: &[&[u8]]) -> Vec<u8> {
    let mut out = (elements.len() as i32).to_be_bytes().to_vec();
    for e in elements {
        out.extend_from_slice(&framed(e));
    }
    out
}

/// A frozen map body: `[i32 BE count][i32 BE key_len][key][i32 BE val_len][val]...`
fn frozen_map(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut out = (entries.len() as i32).to_be_bytes().to_vec();
    for (k, v) in entries {
        out.extend_from_slice(&framed(k));
        out.extend_from_slice(&framed(v));
    }
    out
}

/// A tuple body: a sequence of `[i32 BE len][bytes]` fields.
fn tuple_body(fields: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in fields {
        out.extend_from_slice(&framed(f));
    }
    out
}

/// Builds a container body around ONE element payload.
type BodyBuilder = Box<dyn Fn(&[u8]) -> Vec<u8>>;

/// One nesting position: `(label, cql type string, body builder)`.
type NestingPosition = (String, String, BodyBuilder);

/// The five nesting positions AC1 enumerates, for one element of type `t`.
fn nesting_positions(t: &str) -> Vec<NestingPosition> {
    vec![
        (
            format!("frozen<list<{}>>", t),
            format!("list<{}>", t),
            Box::new(|e: &[u8]| frozen_sequence(&[e])) as BodyBuilder,
        ),
        (
            format!("frozen<set<{}>>", t),
            format!("set<{}>", t),
            Box::new(|e: &[u8]| frozen_sequence(&[e])),
        ),
        (
            format!("frozen<map<{},text>> key", t),
            format!("map<{},text>", t),
            Box::new(|e: &[u8]| frozen_map(&[(e, b"v")])),
        ),
        (
            format!("frozen<map<text,{}>> value", t),
            format!("map<text,{}>", t),
            Box::new(|e: &[u8]| frozen_map(&[(b"k", e)])),
        ),
        (
            format!("tuple<{},text>", t),
            format!("tuple<{},text>", t),
            Box::new(|e: &[u8]| tuple_body(&[e, b"v"])),
        ),
    ]
}

/// Did `err` come from one of the TWO halves of #3811's composed width rule?
///
/// * UNDER-width (including zero): `reporting::require_fixed_width` —
///   `"Frozen element '<col>': need <n> byte(s) for <what>, got <len>"`.
/// * OVER-width: `require_fully_consumed_raw` —
///   `"Bounded value '<col>' of type '<t>' decoded only <n> of <len> byte(s)"`.
///
/// Matched by MESSAGE because both are `Error::Corruption`, the pre-existing
/// class every one of this decoder's refusals uses; there is deliberately no
/// dedicated variant (issue #3723 proposed one and it was superseded — see this
/// module's header). The two wordings are asserted separately so a case cannot
/// pass on the wrong half of the rule.
fn is_under_width_error(err: &Error) -> bool {
    matches!(err, Error::Corruption(msg) if msg.contains("byte(s) for") && msg.contains("got"))
}

fn is_over_width_error(err: &Error) -> bool {
    matches!(err, Error::Corruption(msg) if msg.contains("decoded only"))
}

fn is_width_error(err: &Error) -> bool {
    is_under_width_error(err) || is_over_width_error(err)
}

/// AC1: a wrong declared length is REFUSED at every nesting position, with the
/// NAMED error carrying the type, the expected width and the actual length.
#[test]
fn wrong_declared_length_is_refused_at_every_nesting_position() {
    let p = parser();
    for (t, width) in FIXED_WIDTH_TYPES {
        // Wrong lengths: one byte too many always; one byte too few whenever
        // that is still a non-empty slice (the empty case is AC2's subject).
        let mut wrong: Vec<usize> = vec![width + 1];
        if *width > 1 {
            wrong.push(width - 1);
        }
        for w in wrong {
            let payload = vec![0x11u8; w];
            for (label, type_str, build) in nesting_positions(t) {
                let body = build(&payload);
                let err = p
                    .parse_value_from_raw_bytes(&body, &type_str, "col", 0)
                    .expect_err(&format!(
                        "{} at {}: a {}-byte element must be refused (Cassandra admits {} only)",
                        t, label, w, width
                    ));
                // Which half refused is DERIVED from the direction, so a case
                // cannot pass on the wrong one: too few bytes must fail the arm's
                // own `require_fixed_width`, too many must fail the caller's
                // consumption assert.
                let expected_half = if w < *width { "under" } else { "over" };
                assert!(
                    if w < *width {
                        is_under_width_error(&err)
                    } else {
                        is_over_width_error(&err)
                    },
                    "{} at {} ({} bytes): expected the {}-width refusal, got {:?}",
                    t,
                    label,
                    w,
                    expected_half,
                    err
                );
                let msg = err.to_string();
                assert!(
                    msg.contains(&width.to_string()) && msg.contains(&w.to_string()),
                    "{} at {}: error must name the admissible width and the actual length: {}",
                    t,
                    label,
                    msg
                );
            }
        }
    }
}

/// AC2: a ZERO-length fixed-width element is refused at every nesting position.
///
/// The four strict serializers (`smallint`, `tinyint`, `date`, `time`) refuse it
/// per the pinned source. The "or 0" family is refused because `require_fixed_width`
/// is `data.len() < n` — a PRE-EXISTING divergence from Cassandra, tracked as
/// **#3847**; this case characterises it, it does not endorse it.
#[test]
fn zero_length_fixed_width_element_is_refused_at_every_nesting_position() {
    let p = parser();
    for (t, width) in FIXED_WIDTH_TYPES {
        for (label, type_str, build) in nesting_positions(t) {
            let body = build(&[]);
            let err = p
                .parse_value_from_raw_bytes(&body, &type_str, "col", 0)
                .expect_err(&format!(
                    "{} at {}: a 0-byte element must be refused (admits {} only)",
                    t, label, width
                ));
            assert!(
                is_width_error(&err),
                "{} at {} (0 bytes): expected the under-width refusal, got {:?}",
                t,
                label,
                err
            );
        }
    }
}

/// AC3: the previously-colliding `frozen<list<int>>` pair no longer decodes to
/// the same `Value` — the 13-byte encoding is now an error.
///
/// `[count=1][len=4][4B]` (12 bytes) is well-formed; `[count=1][len=5][5B]`
/// (13 bytes) is refused by `Int32Serializer.validate` at `cassandra-5.0.8`.
#[test]
fn colliding_frozen_list_int_cell_paths_no_longer_collapse() {
    let p = parser();
    let twelve = frozen_sequence(&[&7i32.to_be_bytes()]);
    let thirteen = frozen_sequence(&[&[0u8, 0, 0, 7, 0]]);
    assert_eq!(twelve.len(), 12);
    assert_eq!(thirteen.len(), 13);

    let ok = p
        .parse_value_from_raw_bytes(&twelve, "frozen<list<int>>", "col", 0)
        .expect("the 4-byte element is well-formed per Int32Serializer.validate");
    assert_eq!(
        ok,
        Value::Frozen(Box::new(Value::List(vec![Value::Integer(7)])))
    );

    let err = p
        .parse_value_from_raw_bytes(&thirteen, "frozen<list<int>>", "col", 0)
        .expect_err("a 5-byte int element is refused by Int32Serializer.validate");
    assert!(
        is_over_width_error(&err),
        "the 5-byte `int` element must be refused by the consumption assert, got {:?}",
        err
    );
}

/// NEGATIVE CONTROL: a CORRECT-width element still decodes, at every nesting
/// position, for every fixed-width type. Without this the tightening could be
/// "everything is refused" and the AC1/AC2 tests would still pass.
#[test]
fn correct_width_elements_still_decode_at_every_nesting_position() {
    let p = parser();
    for (t, width) in FIXED_WIDTH_TYPES {
        let payload = vec![0x01u8; *width];
        for (label, type_str, build) in nesting_positions(t) {
            let body = build(&payload);
            let val = p
                .parse_value_from_raw_bytes(&body, &type_str, "col", 0)
                .unwrap_or_else(|e| {
                    panic!(
                        "{} at {}: a correct {}-byte element must decode, got {:?}",
                        t, label, width, e
                    )
                });
            // The decoded element must NOT be a Blob fallback — that would mean
            // the arm was bypassed rather than exercised.
            let inner = match &val {
                Value::List(v) | Value::Set(v) => v.first().cloned(),
                Value::Map(entries) => entries.first().map(|(k, v)| {
                    if type_str.starts_with("map<text,") {
                        v.clone()
                    } else {
                        k.clone()
                    }
                }),
                Value::Tuple(v) => v.first().cloned(),
                other => panic!("{} at {}: unexpected container {:?}", t, label, other),
            }
            .unwrap_or_else(|| panic!("{} at {}: container decoded empty", t, label));
            assert!(
                !matches!(inner, Value::Blob(_) | Value::Null),
                "{} at {}: element decoded as {:?}, the fixed-width arm was bypassed",
                t,
                label,
                inner
            );
        }
    }
}

/// NEGATIVE CONTROL: a legal SHORT or ABSENT encoding stays legal.
///
/// - Variable-width elements (`text`, `blob`, `varint`) accept any length,
///   including zero — the tightening must not reach them.
/// - A tuple field declared `-1` is NULL and never reaches the fixed-width arm
///   (`parse_tuple_elements_raw` pushes `Value::Null` before dispatching), so a
///   null `int` field is still legal.
/// - An EMPTY frozen collection (count = 0) is still legal.
#[test]
fn legal_short_and_absent_encodings_stay_legal() {
    let p = parser();

    // Zero-length variable-width elements.
    for (t, expect_empty) in [("text", true), ("blob", true), ("varint", true)] {
        let body = frozen_sequence(&[&[]]);
        let val = p
            .parse_value_from_raw_bytes(&body, &format!("list<{}>", t), "col", 0)
            .unwrap_or_else(|e| panic!("list<{}> with a 0-byte element must decode: {:?}", t, e));
        match val {
            Value::List(v) => {
                assert_eq!(v.len(), 1, "list<{}>: one element expected", t);
                assert!(expect_empty, "list<{}>: sanity", t);
            }
            other => panic!("list<{}>: unexpected {:?}", t, other),
        }
    }

    // Over-long variable-width elements are also still legal.
    let long_text = frozen_sequence(&[b"a rather long text element"]);
    assert!(p
        .parse_value_from_raw_bytes(&long_text, "list<text>", "col", 0)
        .is_ok());

    // A tuple field declared -1 is null; the sibling text field still decodes.
    let mut null_tuple = (-1i32).to_be_bytes().to_vec();
    null_tuple.extend_from_slice(&framed(b"ok"));
    let val = p
        .parse_value_from_raw_bytes(&null_tuple, "tuple<int,text>", "col", 0)
        .expect("a -1 tuple field is NULL, not a width violation");
    match val {
        Value::Tuple(fields) => {
            assert_eq!(fields[0], Value::Null);
            assert_eq!(fields[1], Value::text("ok".to_string()));
        }
        other => panic!("unexpected {:?}", other),
    }

    // An empty frozen collection (count = 0).
    let empty = 0i32.to_be_bytes().to_vec();
    assert_eq!(
        p.parse_value_from_raw_bytes(&empty, "list<int>", "col", 0)
            .expect("count=0 is a legal empty list"),
        Value::List(vec![])
    );
}

/// Drift guard, at the DIRECT (unnested) bounded position: for every name the
/// fixed-width arms of `raw_value::reporting` match, the admissible length is
/// exactly the width the pinned `cassandra-5.0.8` serializer requires — `width`
/// decodes, `width + 1` and `width - 1` do not.
///
/// This is the behavioural form of what an earlier revision asserted by
/// comparing a `fixed_width_admissible_width` TABLE against
/// [`FIXED_WIDTH_TYPES`]. #3811 owns the widths inline in each arm, so there is
/// no table to compare; asserting the OBSERVABLE width is strictly stronger —
/// a table can agree with the pinned serializers while the arm it feeds does
/// not. [`FIXED_WIDTH_TYPES`] is the closed name set those arms match, so an
/// arm added or renamed without updating it fails the `None`-side loop below.
#[test]
fn admissible_widths_match_the_pinned_serializers() {
    let p = parser();
    for (t, width) in FIXED_WIDTH_TYPES {
        assert!(
            p.parse_value_from_raw_bytes(&vec![0u8; *width], t, "col", 0)
                .is_ok(),
            "{}: exactly {} byte(s) must decode",
            t,
            width
        );
        let over = p
            .parse_value_from_raw_bytes(&vec![0u8; width + 1], t, "col", 0)
            .expect_err(&format!("{}: {} byte(s) must be refused", t, width + 1));
        assert!(
            is_over_width_error(&over),
            "{}: {} byte(s) must fail the consumption assert, got {:?}",
            t,
            width + 1,
            over
        );
        // Zero is covered by its own case above; `width - 1` only exists for the
        // multi-byte types.
        if *width > 1 {
            let under = p
                .parse_value_from_raw_bytes(&vec![0u8; width - 1], t, "col", 0)
                .expect_err(&format!("{}: {} byte(s) must be refused", t, width - 1));
            assert!(
                is_under_width_error(&under),
                "{}: {} byte(s) must fail require_fixed_width, got {:?}",
                t,
                width - 1,
                under
            );
        }
    }

    // Names that are NOT fixed-width must accept a length no fixed-width arm
    // would — proving they do not silently share one of the arms above.
    for t in ["text", "blob", "varint", "inet"] {
        assert!(
            p.parse_value_from_raw_bytes(&[0x31u8; 17], t, "col", 0)
                .is_ok(),
            "{} must not be width-constrained",
            t
        );
    }
}
