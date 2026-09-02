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
//! AC1 and AC3 assert is therefore enforced on THAT PATH by #3811's mechanism,
//! and these cases pin it for every fixed-width type at the five nesting
//! positions [`nesting_positions`] enumerates — coverage #3811's own tests
//! (`raw_value/issue_3811_consumption_demo_tests.rs`) do not enumerate. They
//! must fail if either half of that composition is relaxed.
//!
//! Both halves report `Error::Corruption`, with two distinct wordings — the
//! under-width one from `require_fixed_width` and the over-width one from
//! `require_fully_consumed_raw` — and [`is_width_error`] below matches either.
//!
//! ## What is COVERED, and what is NOT
//!
//! **Covered:** the bounded scalar decoder path — everything that reaches
//! `V5CompressedLegacyParser::parse_value_from_raw_bytes`, i.e. a frozen
//! `list`/`set` element, a frozen `map` key or value, a `tuple` field, and the
//! DIRECT (unnested) scalar position.
//!
//! **NOT covered:** a **UDT FIELD**, which does not reach that entry point at
//! all — `parse_inline_udt_value` / `parse_nested_udt_from_registry` dispatch a
//! scalar field to `parse_simple_udt_field_value` (`row_decoder/udt.rs`), whose
//! `_ =>` **blob fallback** applies NO width check to `tinyint`, `smallint`,
//! `date`, `time` or `counter`. That is a real, open gap, and #3811's
//! consumption assert is structurally unable to see it (the blob consumes the
//! whole slice, so `consumed == len`). It is CHARACTERISED at the end of this
//! file by
//! `wrong_width_udt_field_of_five_types_is_tolerated_today_known_gap`, which is
//! also where the five types and the mechanism are stated in full. Read no
//! claim here as covering a UDT field.
//!
//! CQLite is therefore NARROWER than Cassandra for every `… or 0` row of the table above,
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
///
/// These five are the WHOLE of "every nesting position" as this module's cases
/// use that phrase: they are the positions that reach
/// `parse_value_from_raw_bytes`. A UDT FIELD is NOT among them and is not
/// reachable through this helper — see the module header's coverage section and
/// `wrong_width_udt_field_of_five_types_is_tolerated_today_known_gap`.
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

/// AC1: a wrong declared length is REFUSED at each of the five nesting
/// positions [`nesting_positions`] enumerates, with the NAMED error carrying
/// the type, the expected width and the actual length. Scope: the bounded
/// scalar path only — NOT a UDT field (module header, "What is COVERED").
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
                    .err()
                    .unwrap_or_else(|| {
                        panic!(
                            "{} at {}: a {}-byte element must be refused (Cassandra admits {} only)",
                            t, label, w, width
                        )
                    });
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

/// AC2: a ZERO-length fixed-width element is refused at each of the five
/// nesting positions [`nesting_positions`] enumerates. A zero-length UDT FIELD
/// of the five unguarded types is NOT refused — see
/// `wrong_width_udt_field_of_five_types_is_tolerated_today_known_gap`.
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
                .err()
                .unwrap_or_else(|| {
                    panic!(
                        "{} at {}: a 0-byte element must be refused (admits {} only)",
                        t, label, width
                    )
                });
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
            .err()
            .unwrap_or_else(|| panic!("{}: {} byte(s) must be refused", t, width + 1));
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
                .err()
                .unwrap_or_else(|| panic!("{}: {} byte(s) must be refused", t, width - 1));
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

// ---------------------------------------------------------------------------
// KNOWN-TOLERATED GAP: a UDT FIELD is not a nesting position this module's
// width property reaches (roborev r7 / job 71).
// ---------------------------------------------------------------------------

/// Every fixed-width type whose UDT-field decode has **NO width check today**,
/// with the width the pinned `cassandra-5.0.8` serializer requires.
///
/// `parse_simple_udt_field_value` (`row_decoder/udt.rs`) enumerates arms for
/// `Int`, `BigInt`, `Boolean`, `Float`, `Double`, `Uuid`/`TimeUuid` and
/// `Timestamp` only; these five reach its `_ =>` **blob fallback**.
const UNGUARDED_UDT_FIELD_TYPES: &[(&str, CqlType, usize)] = &[
    ("tinyint", CqlType::TinyInt, 1),
    ("smallint", CqlType::SmallInt, 2),
    ("date", CqlType::Date, 4),
    ("time", CqlType::Time, 8),
    ("counter", CqlType::Counter, 8),
];

/// One inline UDT field of type `ft`, as `parse_inline_udt_value` sees it:
/// a `[i32 BE len][bytes]` component.
fn one_field_udt(ft: CqlType) -> Vec<(String, CqlType)> {
    vec![("f".to_string(), ft)]
}

/// CHARACTERISATION of the gap this module's width property does NOT cover —
/// **not desired behaviour**. A wrong-width fixed-width value in a **UDT
/// FIELD** of one of five types is accepted silently today.
///
/// ## Why #3811's consumption assert cannot see it
///
/// `parse_simple_udt_field_value` has no arm for `TinyInt`, `SmallInt`, `Date`,
/// `Time` or `Counter`, so each falls to `_ => Ok(Value::Blob(..))`, which
/// consumes the **whole** bounded field slice whatever its length. The outer
/// `parse_inline_udt_value` therefore reaches
/// `require_fully_consumed_raw(current_offset, data.len(), ..)` with
/// `current_offset == data.len()` and the assert PASSES. #3811's mechanism is a
/// consumption comparison, so an arm that consumes everything is invisible to
/// it — which is exactly why this gap survived a module header claiming the
/// property held at "every nesting position for every fixed-width type".
///
/// Closing it means adding a width check to those five arms, which is a
/// TIGHTENING that turns today-accepted bytes into errors: it needs its own
/// Cassandra oracle and a corpus measurement, and is deliberately NOT done
/// here. Cassandra 5.0.8 refuses all of these — `ShortSerializer`
/// (`size != 2`), `ByteSerializer` (`size != 1`), `SimpleDateSerializer`
/// (`size != 4`), `TimeSerializer` (`size != 8`), `CounterSerializer`
/// (`size != 8 && !isEmpty`) — so CQLite is WIDER than Cassandra here.
///
/// The case fails in BOTH directions: if the five arms gain a width check (a
/// real behaviour change that must update this test and both scoping comments
/// in the same commit), or if control (b)/(c) stops refusing.
#[test]
fn wrong_width_udt_field_of_five_types_is_tolerated_today_known_gap() {
    let p = parser();

    for (name, ft, width) in UNGUARDED_UDT_FIELD_TYPES {
        // Anti-empty-pass control: the CORRECT width decodes, and it decodes to
        // a Blob too — the blob fallback is the ONLY behaviour this field type
        // has, so there is no "right" decode being displaced.
        let good = framed(&vec![0x11u8; *width]);
        match p
            .parse_inline_udt_value(&good, "t", &one_field_udt(ft.clone()), 0)
            .unwrap_or_else(|e| panic!("{name}: a correct-width field must decode, got {e:?}"))
        {
            Value::Udt(u) => assert!(
                matches!(u.fields[0].value, Some(Value::Blob(_))),
                "{name}: control expects the blob fallback, got {:?}",
                u.fields[0].value
            ),
            other => panic!("{name}: expected a UDT, got {other:?}"),
        }

        // (a) THE GAP: every wrong width — under, over, and ZERO — decodes,
        // yielding a Blob of exactly those bytes. Zero is included because a
        // `field_len == 0` component takes the same `_ =>` arm with an empty
        // slice.
        let mut wrong: Vec<usize> = vec![0, width + 1];
        if *width > 1 {
            wrong.push(width - 1);
        }
        for w in wrong {
            let payload = vec![0x11u8; w];
            let body = framed(&payload);
            let value = p
                .parse_inline_udt_value(&body, "t", &one_field_udt(ft.clone()), 0)
                .unwrap_or_else(|e| {
                    panic!(
                        "KNOWN GAP: a {w}-byte `{name}` UDT field is accepted today \
                         (Cassandra admits {width} only). If this now refuses, the gap \
                         is CLOSED — update this test, this module's header and \
                         `complex_column/cell_path_key.rs`'s AC7 note together. Got {e:?}"
                    )
                });
            match value {
                Value::Udt(u) => match &u.fields[0].value {
                    Some(Value::Blob(bytes)) => assert_eq!(
                        bytes.as_ref(),
                        payload.as_slice(),
                        "KNOWN GAP: the blob fallback returns the field bytes verbatim"
                    ),
                    other => panic!(
                        "KNOWN GAP: a {w}-byte `{name}` field is a Blob today, got {other:?}"
                    ),
                },
                other => panic!("{name}: expected a UDT, got {other:?}"),
            }
        }
    }

    // (b) LIVE CONTROL: the SAME shape with a width-CHECKED type IS refused,
    // by `parse_simple_udt_field_value`'s own arm — a different layer from this
    // module's two wordings, so the contrast is what makes (a) evidence rather
    // than an assertion about a decoder that validates nothing.
    for w in [0usize, 3, 5] {
        let body = framed(&vec![0x11u8; w]);
        let err = p
            .parse_inline_udt_value(&body, "t", &one_field_udt(CqlType::Int), 0)
            .expect_err("control: a wrong-width `int` UDT field must be refused");
        assert!(
            matches!(&err, Error::Corruption(msg)
                if msg.contains(&format!("Int field requires 4 bytes, got {w}"))),
            "control: the refusal must come from the Int arm's own width check, got {err:?}"
        );
    }

    // (c) LIVE CONTROL: #3811's `require_fully_consumed_raw` IS wired into this
    // very path — a TRAILING byte after a correct-width `date` field is refused.
    // So (a) is the assert being BLIND to a full-slice blob, not the assert
    // being absent from the UDT decoder.
    let mut trailing = framed(&[0x11u8; 4]);
    trailing.push(0x22);
    let err = p
        .parse_inline_udt_value(&trailing, "t", &one_field_udt(CqlType::Date), 0)
        .expect_err("control: a trailing byte after the last UDT field must be refused");
    assert!(
        is_over_width_error(&err),
        "control: #3811's consumption assert must be live in this path, got {err:?}"
    );
}

/// roborev r8 (job 80), Low: the AC7 text claimed all four nested-consumption
/// classes were MEASURED while `duration` was pinned by no test, so a
/// regression admitting trailing bytes after the three VInts would have gone
/// undetected. The review offered two remedies — narrow the claim, or add the
/// pin. This is the pin, because it makes the claim TRUE rather than smaller.
///
/// `duration` is fixed-FORM rather than fixed-WIDTH: its extent is whatever its
/// three VInts occupy, so no `require_fixed_width` arm can express it. The
/// refusal therefore comes from #3811's consumption assert alone
/// (`raw_value/reporting.rs:67`), which is exactly why the pin matters — this is
/// the one class where the composed rule reduces to a single check.
///
/// The exact form is the CONTROL: without it, a refusal of the trailing-byte
/// form would prove only that the decoder rejects something.
#[test]
fn bounded_duration_with_trailing_bytes_is_refused() {
    let p = parser();

    // Three zigzag VInts, each zero: months=0, days=0, nanos=0.
    let exact: [u8; 3] = [0x00, 0x00, 0x00];

    // CONTROL / NON-DISCRIMINATING: the exact form decodes.
    p.parse_value_from_raw_bytes(&exact, "duration", "col", 0)
        .unwrap_or_else(|e| panic!("the exact 3-VInt duration must decode, got {e:?}"));

    // DISCRIMINATING: one trailing byte leaves the assert short.
    let mut trailing = exact.to_vec();
    trailing.push(0xAA);
    let err = p
        .parse_value_from_raw_bytes(&trailing, "duration", "col", 0)
        .expect_err("a duration carrying a trailing byte must be refused");
    assert!(
        is_over_width_error(&err),
        "expected #3811's short-consumption refusal, got {err:?}"
    );
}
