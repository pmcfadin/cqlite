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
//! type's own `validate`, and — if bytes remain — throws
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
//! `require_fully_consumed`. Composed with each arm's own
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
//! `require_fully_consumed` — and the two matchers below cover those halves
//! SEPARATELY, so a case cannot pass on the wrong one. (The composed either-half
//! matcher went with #3847: zero length is no longer an error at all.)
//!
//! ## What is COVERED, and what is NOT
//!
//! **Covered:** the bounded scalar decoder path — everything that reaches
//! `V5CompressedLegacyParser::parse_value_from_raw_bytes`, i.e. a frozen
//! `list`/`set` element, a frozen `map` key or value, a `tuple` field, and the
//! DIRECT (unnested) scalar position.
//!
//! **Covered by a DIFFERENT mechanism:** a **UDT FIELD**, which does not reach
//! that entry point through any of the five positions above —
//! `parse_inline_udt_value` (`row_decoder/udt/inline.rs`), `parse_udt_value` and
//! `parse_nested_udt_from_registry` (`row_decoder/udt.rs`) each dispatch a
//! scalar field to `parse_simple_udt_field_value_at`
//! (`row_decoder/typed_value.rs`). That function used to end in a `_ =>`
//! **blob fallback**, which applied NO width check to `tinyint`, `smallint`,
//! `date`, `time` or `counter`; #3811's consumption assert was structurally
//! unable to see it (the blob consumed the whole slice, so `consumed == len`).
//! **Issue #3631 removed the fallback.** The trailing arm is now
//! `other => self.parse_typed_value(data, other, "UDT field", depth)`, whose
//! scalar branch refuses any length outside the declared type's allowed-width
//! set — `"UDT field: declared type '<t>' is <n> bytes wide …"` — so those five
//! are now width-checked too, by a THIRD wording this module matches separately
//! ([`is_declared_width_error`]). That is pinned at the end of this file by
//! `wrong_width_udt_field_of_five_types_is_refused_since_3631`, which is also
//! where the five types and the mechanism are stated in full.
//!
//! A ZERO-length UDT field is a SEPARATE, PRE-EXISTING disposition and is NOT
//! part of what #3631 closed: it decodes to `Value::Null`, never a width error,
//! because `parse_simple_udt_field_value_at` redirects an empty slice to the
//! typed decoder, whose `empty_is_a_value` reads Cassandra's
//! `accessor.isEmpty(value) ? null : …` guard. The same case characterises it.
//!
//! CQLite is therefore NARROWER than Cassandra for every `… or 0` row of the table above,
//! whose `validate` admits an EMPTY buffer (`Int32Serializer.java`
//! `size(value) != 4 && !isEmpty(value)`, deserializing to Java `null`). That
//! divergence PREDATES both issues, is deliberate, and is tracked as **#3847**;
//! `zero_length_fixed_width_element_is_refused_at_every_nesting_position` below
//! characterises it rather than endorsing it.

use super::*;

/// The fixed-width CQL short forms this decoder admits, with the exact width the
/// pinned Cassandra serializer requires. `short` = `smallint` and `byte` =
/// `tinyint` are the non-CQL marshal aliases the match also accepts.
///
/// **CURATED, NOT DERIVED — a DECLARED GAP, not a drift guard (roborev r12).**
/// This list is maintained BY HAND. Every case below iterates it, so a
/// fixed-width arm ADDED to `raw_value/reporting.rs` and omitted here is never
/// exercised and these tests stay GREEN: they cannot detect a new arm, only
/// re-assert the ones named. Read no case here as proving the arm set complete.
///
/// Deriving it was considered and rejected rather than overlooked: the arms are
/// `match` string literals in another module, so deriving them means parsing
/// Rust to find declarations in arbitrary source — the unbounded-scanner class
/// this repo REMOVED on purpose (#1712 deleted the rustdoc-derived `pub-surface`
/// snapshot for exactly that reason: a scanner that cannot abstain). A curated
/// list that DECLARES its incompleteness is worth more than a derivation whose
/// own correctness nobody can establish.
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
/// reachable through this helper — it is width-checked one layer up, by
/// `parse_typed_value`'s allowed-width set since #3631; see the module header's
/// coverage section and
/// `wrong_width_udt_field_of_five_types_is_refused_since_3631`.
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
/// * OVER-width: `require_fully_consumed` —
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

/// The THIRD width wording, from a THIRD layer: `parse_typed_value`'s scalar
/// branch (`row_decoder/typed_value/scalar_rules.rs`'s allowed-width set), which
/// refuses a declared type's framed value whose length is outside that set —
/// `"<ctx>: declared type '<t>' is <n> bytes wide (or empty, meaning null) but
/// the framed value is <len> bytes; … (issue #3631)"`.
///
/// It is matched SEPARATELY from the two above, and never folded into
/// an either-half matcher, because the layer is what the UDT-field case is evidence
/// ABOUT: a case that accepted any of the three wordings could pass on #3811's
/// consumption assert firing at the enclosing frame instead of the field's own
/// declared width being checked.
fn is_declared_width_error(err: &Error) -> bool {
    matches!(err, Error::Corruption(msg) if msg.contains("declared type") && msg.contains("bytes wide"))
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

/// AC2, POST-#3847: a ZERO-length fixed-width element DECODES TO NULL at the four
/// VALUE positions — it is no longer refused.
///
/// **This replaces a characterisation test #3723 deliberately did not endorse.**
/// Its own comment read: *"The 'or 0' family is refused because
/// `require_fixed_width` is `data.len() < n` — a PRE-EXISTING divergence from
/// Cassandra, tracked as #3847; this case characterises it, it does not endorse
/// it."* #3847 closed that divergence, so the expectation FLIPS here rather than
/// the test being deleted: #3723's coverage (all fifteen types x every nesting
/// position) is kept and only the asserted answer moves. The old name asserted a
/// refusal, so it could not survive the flip — a name is a claim about behaviour.
///
/// Oracle: `deserialize()`, uniformly, at the pinned `cassandra-5.0.8` tag — every
/// fixed-width `TypeSerializer` maps an EMPTY buffer to null, the wire spelling of
/// null. `validate()` is the WRITE path and is NON-uniform (`smallint`, `tinyint`,
/// `date`, `time` reject empty there); that asymmetry is why the cell-path KEY
/// table stays strict while this VALUE path does not.
///
/// The MAP KEY position is excluded and has its own case below.
#[test]
fn zero_length_fixed_width_element_decodes_to_null_at_the_four_value_positions() {
    let p = parser();
    for (t, _width) in FIXED_WIDTH_TYPES {
        for (label, type_str, build) in nesting_positions(t) {
            if label.contains("key") || label.contains("set<") {
                continue; // KEY-LIKE positions (map key, set member) are the sibling case
            }
            let body = build(&[]);
            let decoded = p
                .parse_value_from_raw_bytes(&body, &type_str, "col", 0)
                .unwrap_or_else(|e| {
                    panic!("{t} at {label}: #3847 admits a 0-byte element; got {e:?}")
                });
            let element = match &decoded {
                Value::List(xs) | Value::Set(xs) | Value::Tuple(xs) => xs
                    .first()
                    .unwrap_or_else(|| panic!("{t} at {label}: container empty"))
                    .clone(),
                Value::Map(kv) => kv
                    .first()
                    .unwrap_or_else(|| panic!("{t} at {label}: map empty"))
                    .1
                    .clone(),
                other => panic!("{t} at {label}: unexpected container {other:?}"),
            };
            assert_eq!(
                element,
                Value::Null,
                "{t} at {label}: an empty fixed-width element is NULL, not a refusal"
            );
        }
    }
}

/// AC2's sibling: a ZERO-length fixed-width KEY-LIKE member — a MAP KEY or a SET
/// MEMBER — is preserved OPAQUELY and is NEVER null (#3847, roborev jobs 153/170).
///
/// Cassandra stores a `set<T>` member in the CELL PATH, exactly as it stores a map
/// key, so a set member IS a key: `Set([Null])` is as unexpressible as a null map
/// key. An earlier revision of THIS test asserted `Null` for the set position and
/// was wrong; job 170 caught it.
///
/// `Value::Null` is the right answer for a VALUE and an impossible one for a KEY —
/// Cassandra cannot express a null map key — so the key path applies #3747's opaque
/// answer instead of inheriting the value rule. The rule, and the four defects one
/// root cause produced, are documented in `row_decoder::frozen_map`.
#[test]
fn zero_length_fixed_width_key_like_member_is_opaque_never_null() {
    let p = parser();
    for (t, _width) in FIXED_WIDTH_TYPES {
        for (label, type_str, build) in nesting_positions(t) {
            if !(label.contains("key") || label.contains("set<")) {
                continue;
            }
            let body = build(&[]);
            let decoded = p
                .parse_value_from_raw_bytes(&body, &type_str, "col", 0)
                .unwrap_or_else(|e| {
                    panic!("{t} at {label}: the entry must be kept, not dropped: {e:?}")
                });
            let key = match &decoded {
                Value::Map(kv) => {
                    &kv.first()
                        .unwrap_or_else(|| panic!("{t} at {label}: map empty"))
                        .0
                }
                Value::Set(xs) => xs
                    .first()
                    .unwrap_or_else(|| panic!("{t} at {label}: set empty")),
                other => panic!("{t} at {label}: expected a map or set, got {other:?}"),
            };
            assert_ne!(
                *key,
                Value::Null,
                "{t} at {label}: a key-like member must NEVER be null"
            );
            assert_eq!(
                *key,
                Value::blob(Vec::new()),
                "{t} at {label}: preserved opaquely, as the cell-path key is"
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

/// Width assertion at the DIRECT (unnested) bounded position: for each name in
/// [`FIXED_WIDTH_TYPES`], the admissible length is exactly the width the pinned
/// `cassandra-5.0.8` serializer requires — `width` decodes; `width + 1` does
/// not; `width - 1` does not, WHERE `width > 1` (a 1-byte type has no distinct
/// under-width case, so the loop skips it).
///
/// This is the behavioural form of what an earlier revision asserted by
/// comparing a `fixed_width_admissible_width` TABLE against
/// [`FIXED_WIDTH_TYPES`]. #3811 owns the widths inline in each arm, so there is
/// no table to compare; asserting the OBSERVABLE width is strictly stronger —
/// a table can agree with the pinned serializers while the arm it feeds does
/// not.
///
/// **NOT a drift guard, despite the position of this test (roborev r13).** It
/// iterates [`FIXED_WIDTH_TYPES`] and nothing else, and that list is CURATED
/// (see its declaration) — nothing here notices an arm ADDED or RENAMED in
/// `raw_value::reporting`, so such an arm leaves this test GREEN. What it does
/// assert is that every name the list DOES carry still behaves exactly as its
/// pinned serializer requires; the trailing loop asserts only that the names it
/// SPELLS (`text`, `blob`, `varint`) are not width-constrained. `inet` was in that
/// loop and is deliberately NOT — it is width-constrained in Cassandra, and its own
/// case below measures the divergence. The names are spelled rather than counted:
/// this doc said "four" after that removal, which is the drifting-count defect this
/// PR already fixed once (roborev r20 / job 113, and r10 before it).
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

    // Names that are genuinely NOT width-constrained must accept a length no
    // fixed-width arm would — proving they do not silently share one of the arms
    // above. `inet` is deliberately NOT in this list: see the divergence case
    // below, which measures it instead of asserting it is unconstrained.
    for t in ["text", "blob", "varint"] {
        assert!(
            p.parse_value_from_raw_bytes(&[0x31u8; 17], t, "col", 0)
                .is_ok(),
            "{} must not be width-constrained",
            t
        );
    }
}

/// DECLARED DIVERGENCE, characterised not endorsed: `inet` is width-constrained
/// in Cassandra and unconstrained here.
///
/// Cassandra 5.0.8's `InetAddressSerializer.validate` admits a non-empty value of
/// EXACTLY 4 or 16 bytes and throws otherwise. CQLite's `inet` arm applies no
/// width rule at all — MEASURED below at 0, 4, 5, 16 and 17 bytes, every one of
/// which decodes. This is NOT one of the nesting positions #3723 is about and is
/// NOT closed by #3811 or #3631, whose mechanisms are consumption and declared
/// width; an arm that consumes everything at any length is invisible to both.
///
/// It is recorded here rather than fixed because refusing 5 and 17 bytes is a
/// TIGHTENING that turns today-accepted bytes into errors, which needs its own
/// Cassandra oracle and a corpus measurement — the same bar the five-type UDT
/// field carve-out had to clear before #3631 closed it.
///
/// TWO divergences live here, and they are separate: (1) WIDTH — Cassandra admits 4
/// or 16 bytes only, this decoder admits any length; (2) EMPTY — Cassandra reads an
/// empty value as NULL, this decoder returns an empty `Value::Inet`. Neither is
/// closed by #3811 or #3631, whose mechanisms are consumption and declared width.
///
/// Fails in BOTH directions: if `inet` gains a width check, if a currently-admissible
/// width stops decoding, or if the empty case starts returning `Null` (update this
/// case, its doc and the width-unconstrained control above together).
#[test]
fn inet_is_width_unconstrained_here_but_not_in_cassandra_declared_divergence() {
    let p = parser();
    // Zero is a SECOND, separate divergence and is asserted by VALUE, not merely by
    // "it decoded" — Cassandra reads an empty `inet` as NULL, and this decoder returns
    // an empty `Value::Inet` instead. Both are tolerant rather than throwing, so this
    // is not a width widening, but they are not the same value and the earlier wording
    // ("the empty/null disposition") glossed that. Pinned exactly so a change to either
    // side is caught (roborev r19 / job 112).
    match p
        .parse_value_from_raw_bytes(&[], "inet", "col", 0)
        .unwrap_or_else(|e| panic!("inet: an EMPTY value must decode, got {e:?}"))
    {
        Value::Inet(b) => assert!(
            b.is_empty(),
            "inet: an empty value decodes to an EMPTY Inet today, got Inet({b:?})"
        ),
        Value::Null => panic!(
            "inet: an empty value now decodes to Null — that CLOSES the empty-vs-null \
             divergence with Cassandra. Update this case and its doc together"
        ),
        other => panic!("inet: expected an empty Inet (today's behaviour), got {other:?}"),
    }
    // Cassandra-admissible widths must decode — the anti-vacuity half.
    for len in [4usize, 16] {
        p.parse_value_from_raw_bytes(&vec![0x31u8; len], "inet", "col", 0)
            .unwrap_or_else(|e| panic!("inet: {len} bytes is Cassandra-legal, got {e:?}"));
    }
    // The divergence: widths Cassandra REFUSES are accepted here.
    for len in [5usize, 17] {
        let got = p.parse_value_from_raw_bytes(&vec![0x31u8; len], "inet", "col", 0);
        assert!(
            got.is_ok(),
            "inet: {len} bytes is TOLERATED today (Cassandra admits 4 or 16 only); \
             if this now refuses, the divergence is closed — update this case and \
             the width-unconstrained control above together, got {got:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// A UDT FIELD is not one of this module's five nesting positions, but it IS
// width-checked — one layer up, since #3631 (roborev r7 / job 71).
// ---------------------------------------------------------------------------

/// Every fixed-width type whose UDT-field decode reaches the TRAILING arm of
/// `parse_simple_udt_field_value_at` (`row_decoder/typed_value.rs`), with the
/// width the pinned `cassandra-5.0.8` serializer requires.
///
/// That function enumerates explicit arms for `Text`/`Ascii`, `Int`, `BigInt`,
/// `Boolean`, `Float`, `Double`, `Uuid`/`TimeUuid`, `Timestamp` and `Blob`;
/// these five fall through to `other => self.parse_typed_value(..)`, where the
/// declared type's allowed-width set decides. They were the `_ => Value::Blob`
/// fallback's population until #3631 removed it, which is why they are named
/// here rather than any other five.
///
/// **CURATED, NOT DERIVED**, on the same terms as [`FIXED_WIDTH_TYPES`]: an arm
/// moved into or out of `parse_simple_udt_field_value_at` is not detected by
/// anything below.
const DELEGATED_UDT_FIELD_TYPES: &[(&str, CqlType, usize)] = &[
    ("tinyint", CqlType::TinyInt, 1),
    ("smallint", CqlType::SmallInt, 2),
    ("date", CqlType::Date, 4),
    ("time", CqlType::Time, 8),
    ("counter", CqlType::Counter, 8),
];

/// The value a CORRECT-width field of `name` decodes to, for a payload of
/// `width` repeated `0x11` bytes.
///
/// Spelled per type rather than derived, so the case asserts the DECODED TYPE
/// as well as the width rule — `counter` surfacing as `Value::BigInt` (there is
/// no `Value::Counter`) is exactly the sort of thing a derived expectation would
/// hide. The unmatched arm PANICS: adding a row to [`DELEGATED_UDT_FIELD_TYPES`]
/// without an expectation here fails the case rather than silently skipping it.
fn expected_correct_width_decode(name: &str) -> Value {
    match name {
        "tinyint" => Value::TinyInt(0x11),
        "smallint" => Value::SmallInt(0x1111),
        "date" => Value::Date(-1_861_152_495),
        "time" => Value::Time(0x1111_1111_1111_1111),
        // `counter` shares `bigint`'s decoder: there is no `Value::Counter`.
        "counter" => Value::BigInt(0x1111_1111_1111_1111),
        other => panic!(
            "no expected decode declared for UDT field type `{other}` — add one \
             beside its DELEGATED_UDT_FIELD_TYPES row"
        ),
    }
}

/// One inline UDT field of type `ft`, as `parse_inline_udt_value` sees it:
/// a `[i32 BE len][bytes]` component.
fn one_field_udt(ft: CqlType) -> Vec<(String, CqlType)> {
    vec![("f".to_string(), ft)]
}

/// A wrong-width fixed-width value in a **UDT FIELD** of one of five types is
/// REFUSED — the gap an earlier revision of this module characterised as open is
/// CLOSED, by **#3631**.
///
/// ## What changed, and why #3811 alone could not have done it
///
/// `parse_simple_udt_field_value_at` had no arm for `TinyInt`, `SmallInt`,
/// `Date`, `Time` or `Counter`, so each fell to `_ => Ok(Value::Blob(..))`,
/// which consumed the **whole** bounded field slice whatever its length. The
/// enclosing UDT loop then reached `require_fully_consumed` with
/// `current_offset == data.len()` and the assert PASSED: #3811's mechanism is a
/// consumption comparison, so an arm that consumes everything is invisible to
/// it. #3631 replaced that arm with
/// `other => self.parse_typed_value(data, other, "UDT field", depth)`, and the
/// scalar branch there checks the declared type's allowed-width SET before
/// delegating — a width rule, not a consumption one, which is why it sees what
/// #3811 could not.
///
/// ## Cassandra oracle
///
/// Cassandra 5.0.8 refuses every non-empty wrong width for all five —
/// `ByteSerializer` (`size != 1`), `ShortSerializer` (`size != 2`),
/// `SimpleDateSerializer` (`size != 4`), `TimeSerializer` (`size != 8`),
/// `CounterSerializer` (`size != 8 && !isEmpty`) — so CQLite now matches it on
/// the non-empty widths, where it used to be WIDER.
///
/// ZERO length is deliberately asserted as `Value::Null`, NOT as a refusal, and
/// that is not part of what #3631 closed: an empty field carries
/// `ByteBufferUtil.EMPTY_BYTE_BUFFER`, which every one of those serializers
/// reads as null via `accessor.isEmpty(value) ? null : …`. It is the same
/// disposition `empty_is_a_value` encodes, and it is the OPPOSITE of the
/// bounded-path treatment `zero_length_fixed_width_element_is_refused_at_every_nesting_position`
/// characterises under #3847 — the asymmetry is real and is asserted here so it
/// cannot change unnoticed.
///
/// The case fails in BOTH directions: if any of the five stops refusing a
/// non-zero wrong width, and if a correct width stops decoding to its declared
/// type or a zero-length field stops being `Null` (a real behaviour change that
/// must update this test and this module's header in the same commit).
#[test]
fn wrong_width_udt_field_of_five_types_is_refused_since_3631() {
    let p = parser();

    for (name, ft, width) in DELEGATED_UDT_FIELD_TYPES {
        // (a) ANTI-VACUITY CONTROL: the CORRECT width decodes, to the DECLARED
        // type's value — not a Blob, and not Null. Without this the case could
        // pass on a decoder that refuses everything.
        let good = framed(&vec![0x11u8; *width]);
        match p
            .parse_inline_udt_value(&good, "t", &one_field_udt(ft.clone()), 0)
            .unwrap_or_else(|e| panic!("{name}: a correct-width field must decode, got {e:?}"))
        {
            Value::Udt(u) => assert_eq!(
                u.fields[0].value,
                Some(expected_correct_width_decode(name)),
                "{name}: a {width}-byte field must decode to its declared type"
            ),
            other => panic!("{name}: expected a UDT, got {other:?}"),
        }

        // (b) THE PROPERTY: every NON-ZERO wrong width — under and over — is
        // refused, by the field's own declared-width check.
        let mut wrong: Vec<usize> = vec![width + 1];
        if *width > 1 {
            wrong.push(width - 1);
        }
        for w in wrong {
            let body = framed(&vec![0x11u8; w]);
            // `expect_err(&format!(..))` would trip clippy's `expect_fun_call` under
            // the gate's `-D warnings`; match instead, which also lets the message
            // carry the value that was wrongly accepted.
            let err = match p.parse_inline_udt_value(&body, "t", &one_field_udt(ft.clone()), 0) {
                Ok(v) => panic!(
                    "a {w}-byte `{name}` UDT field must be refused (Cassandra admits \
                     {width} only), got {v:?}. If this now decodes, #3631's width check \
                     has been relaxed — update this test and this module's header \
                     together"
                ),
                Err(e) => e,
            };
            assert!(
                is_declared_width_error(&err),
                "{name} ({w} bytes): expected the declared-width refusal from \
                 `parse_typed_value`, got {err:?}"
            );
            let msg = err.to_string();
            assert!(
                msg.contains(&format!("'{name}'"))
                    && msg.contains(&format!("is {width} bytes wide"))
                    && msg.contains(&format!("is {w} bytes")),
                "{name} ({w} bytes): the refusal must name the type, the admissible \
                 width and the actual length: {msg}"
            );
        }

        // (c) ZERO length is NULL, not a width error — the pre-existing empty
        // disposition, asserted so the tightening cannot quietly swallow it.
        let empty = framed(&[]);
        match p
            .parse_inline_udt_value(&empty, "t", &one_field_udt(ft.clone()), 0)
            .unwrap_or_else(|e| panic!("{name}: a zero-length field must decode, got {e:?}"))
        {
            Value::Udt(u) => assert_eq!(
                u.fields[0].value,
                Some(Value::Null),
                "{name}: an empty field is NULL per `accessor.isEmpty(value) ? null : …`"
            ),
            other => panic!("{name}: expected a UDT, got {other:?}"),
        }
    }

    // (d) LIVE CONTROL, a DIFFERENT layer: a type with its OWN explicit arm in
    // `parse_simple_udt_field_value_at` is refused by THAT arm's wording, not by
    // the delegated one. So (b) is evidence about the delegation specifically,
    // rather than about a decoder that refuses every UDT field it is handed.
    for w in [3usize, 5] {
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

    // (e) LIVE CONTROL: #3811's `require_fully_consumed` is ALSO wired into this
    // path and is a THIRD wording — a TRAILING byte after a correct-width `date`
    // field is refused by the enclosing frame's consumption assert, not by the
    // field's declared width. Asserting the two separately is what stops (b)
    // passing on the wrong layer.
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
/// refusal therefore comes from #3811's consumption assert alone — the
/// `require_fully_consumed` that `parse_value_from_raw_bytes` reaches through its
/// reporting twin, named rather than cited by line so this claim cannot decay —
/// which is exactly why the pin matters: this is the one class where the composed
/// rule reduces to a single check.
///
/// SCOPE, so this cannot be read as contradicting #3778: the refusal measured here
/// is on the BOUNDED SCALAR path. #3778 Option A separately DECIDED that a
/// `duration` nested in a frozen collection (`raw_type_value.rs`) and a plain
/// top-level cell (`cell_value_scalar.rs`) are TOLERATED as parity-correct. Those
/// are different call sites; both statements are true of the sites they name.
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
