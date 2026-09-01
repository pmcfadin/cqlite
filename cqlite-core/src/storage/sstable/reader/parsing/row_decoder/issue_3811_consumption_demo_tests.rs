//! Issue #3811 — the ENFORCED consumption/bounds contract on
//! [`V5CompressedLegacyParser::parse_value_from_raw_bytes`].
//!
//! # What this file was, and what it is now
//!
//! It began as a DEMONSTRATION harness whose tests deliberately pinned the
//! WRONG behaviour, so that the defect was a measured fact rather than an
//! argument from reading source, and so that the fix could not be
//! test-invariant. The measured "before" is recorded verbatim in
//! `docs/round-artifacts/issue-3811-defect-demonstration.md`.
//!
//! The fix has landed, so the flip-on-fix checklist that used to live here has
//! been EXECUTED: every case that read `is_accepted_today` now asserts the
//! Cassandra-correct `Err`, and the two `collapse` cases now assert the two
//! inputs yield DIFFERENT outcomes. The four control cases are unchanged and
//! still green — case 4 in particular, which is what a naive "every declared
//! field must be present" fix would have broken.
//!
//! # The oracle (never CQLite's own output — #3042)
//!
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java`,
//! static `split(...)`; `UserType extends TupleType`, so a UDT value is split by
//! exactly this method. Transcribed in
//! `docs/round-artifacts/issue-3811-cassandra-oracle.md`. Its rule ORDER is
//! load-bearing, and is the whole reason cases 3 and 4 below exist:
//!
//! 1. `position == length` before reading component `i` ⇒ **LEGAL** short return;
//!    components `i..n` are absent (implicit null).
//! 2. else `position + 4 > length` ⇒ `MarshalException`
//!    `"Not enough bytes to read %dth component"`. Checked only AFTER rule 1, so
//!    1–3 leftover bytes are a CORRUPTION, not an omitted field.
//! 3. `position + size > length` ⇒ `MarshalException`, same message.
//! 4. after the loop, `position < length` ⇒ `MarshalException`
//!    `"Expected N values for <type> column, but got more"`. Full consumption is
//!    REQUIRED, not optional.
//!
//! # The enforcement point (issue #3811 AC2/AC3, census §5-A/§5-B)
//!
//! `parse_value_from_raw_bytes` is documented as bounded — "The entire `data`
//! slice IS the value" — and is now a thin wrapper over
//! `parse_value_from_raw_bytes_reporting`, which threads a real consumption
//! count out of EVERY arm, plus `require_fully_consumed_raw`. The two UDT arms
//! it used to discard the count on:
//!
//! - the **marshal-form arm** (`other if Self::is_udt_type`), reached with an
//!   `org.apache.cassandra.db.marshal.UserType(...)` type string;
//! - the **registry-resolved bare-name arm**, reached with a bare UDT name that
//!   `UdtRegistry::get_udt_qualified` resolves.
//!
//! Both are exercised here with the SAME byte-vectors, because they are separate
//! code paths reaching the same rule and #3631's history is a fix landing on one
//! arm and not its sibling.
//!
//! # DISCRIMINATION LABELS (AC6)
//!
//! Every case below carries an explicit label:
//!
//! - **DISCRIMINATING** — it FAILS if the defect is reintroduced (i.e. if the
//!   `require_fully_consumed_raw` call in `raw_value.rs` is removed).
//! - **CONTROL / NON-DISCRIMINATING** — it passes before AND after, and exists to
//!   prove the fix is not over-strict. `*_case1_*` and `*_case4_*` are these; they
//!   are the guard against the "all declared fields must be present" mis-fix.
//! - **DISCRIMINATING ELSEWHERE** — it fails if a DIFFERENT guard regresses, and
//!   is labelled with which one, rather than being allowed to imply it covers this
//!   issue's assert. `rule3_overrun_*` is the only one of these.
//!
//! The labels were PROVED, not asserted: the assert was commented out, the suite
//! re-run, and the observed red set recorded in the PR. A test whose claim exceeds
//! what it exercises is the defect class this labelling exists to prevent.
//!
//! These carry NO dataset, reader or feature-flag dependency: the subject is a
//! `pub(super)` method on a plainly-constructed parser, so they run in every
//! build and lane and can never pass vacuously on an empty corpus.

use super::V5CompressedLegacyParser;
use crate::schema::{CqlType, UdtRegistry};
use crate::types::UdtTypeDef;
use crate::{Result, Value};

const KEYSPACE: &str = "issue_3811_ks";

/// Marshal-form type string for `addr(street text, city text)` — reaches the
/// `raw_value.rs:458-459` arm via `Self::is_udt_type`. Field names are hex:
/// `61646472` = "addr", `737472656574` = "street", `63697479` = "city".
const MARSHAL_UDT: &str = "org.apache.cassandra.db.marshal.UserType(issue_3811_ks,61646472,\
737472656574:org.apache.cassandra.db.marshal.UTF8Type,\
63697479:org.apache.cassandra.db.marshal.UTF8Type)";

/// Bare UDT name — reaches the `raw_value.rs:479-480` registry-resolved arm.
const REGISTRY_UDT: &str = "addr";

/// A parser holding a bare-keyed `addr` UDT, so BOTH arms are reachable from one
/// instance (the marshal arm does not consult the registry; the bare-name arm
/// requires it).
fn parser() -> V5CompressedLegacyParser {
    let mut reg = UdtRegistry::new();
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "addr".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true),
    );
    V5CompressedLegacyParser::new(KEYSPACE.to_string(), "t".to_string(), 0, 0, None)
        .with_udt_registry(reg)
}

/// One tuple component: `[i32 BE length][raw bytes]`, Cassandra's `TupleType`
/// component framing.
fn component(bytes: &[u8]) -> Vec<u8> {
    let mut v = (bytes.len() as i32).to_be_bytes().to_vec();
    v.extend_from_slice(bytes);
    v
}

/// Case 1 — every declared field present, buffer ends exactly. 18 bytes:
/// `00000007 "main st" 00000003 "nyc"`.
fn case1_exact() -> Vec<u8> {
    let mut v = component(b"main st");
    v.extend(component(b"nyc"));
    v
}

/// Case 2 — case 1 plus one trailing byte the encoding does not account for.
/// 19 bytes. `TupleType.split` rule 3: `position(18) < length(19)` ⇒ throw.
fn case2_trailing_garbage() -> Vec<u8> {
    let mut v = case1_exact();
    v.push(0xAA);
    v
}

/// Case 3 — case 4 plus a single `0x00`, i.e. a truncated component-length
/// prefix. 12 bytes. `TupleType.split` at `i = 1`: `position(11) != length(12)`
/// so rule 1 does not fire, then `position + 4 (15) > length (12)` ⇒ rule 2
/// throw, `"Not enough bytes to read 1th component"`. NOT a legal omission.
fn case3_partial_prefix() -> Vec<u8> {
    let mut v = case4_legally_short();
    v.push(0x00);
    v
}

/// Case 4 — the trailing declared field is simply absent and the buffer ends
/// exactly. 11 bytes. `TupleType.split` rule 1 at `i = 1`:
/// `position(11) == length(11)` ⇒ legal short return, `city` is null.
fn case4_legally_short() -> Vec<u8> {
    component(b"main st")
}

/// Supplementary — the `case 1 || 0x00` spelling from the plan's vector table.
/// Under the oracle this is rule 3 ("got more"), the same class as case 2, since
/// the component loop is already exhausted when the stray byte is reached.
fn supplementary_exact_plus_zero() -> Vec<u8> {
    let mut v = case1_exact();
    v.push(0x00);
    v
}

/// Rule 3 — a declared component length that OVERRUNS the buffer:
/// `[len = 99]["ab"]`. `TupleType.split`: `position(4) + 99 > length(6)` ⇒
/// `MarshalException("Not enough bytes to read 0th component")`.
fn rule3_overrun() -> Vec<u8> {
    let mut v = 99i32.to_be_bytes().to_vec();
    v.extend_from_slice(b"ab");
    v
}

/// A bounded frozen `list<int>` holding one element: `[count = 1][len = 4][7]`.
/// This is the sub-format census finding A names — the arm used to spell
/// `let (val, _) = self.parse_frozen_list_value_raw(…)`.
fn frozen_list_one_int() -> Vec<u8> {
    let mut v = 1i32.to_be_bytes().to_vec();
    v.extend(component(&7i32.to_be_bytes()));
    v
}

/// Drive the bounded entry point under test.
fn decode(type_str: &str, data: &[u8]) -> Result<Value> {
    parser().parse_value_from_raw_bytes(data, type_str, "col", 0)
}

/// Both fields materialized as text.
fn assert_both_fields(value: &Value, ctx: &str) {
    match value {
        Value::Udt(udt) => {
            assert_eq!(udt.type_name, "addr", "{ctx}: UDT type name");
            assert_eq!(udt.fields.len(), 2, "{ctx}: field count");
            assert_eq!(udt.fields[0].name, "street", "{ctx}: field 0 name");
            assert_eq!(
                udt.fields[0].value,
                Some(Value::Text("main st".into())),
                "{ctx}: field 0 value"
            );
            assert_eq!(udt.fields[1].name, "city", "{ctx}: field 1 name");
            assert_eq!(
                udt.fields[1].value,
                Some(Value::Text("nyc".into())),
                "{ctx}: field 1 value"
            );
        }
        other => panic!("{ctx}: expected Value::Udt, got {other:?}"),
    }
}

/// `street` present, `city` absent (implicit null).
fn assert_city_absent(value: &Value, ctx: &str) {
    match value {
        Value::Udt(udt) => {
            assert_eq!(udt.fields.len(), 2, "{ctx}: field count");
            assert_eq!(
                udt.fields[0].value,
                Some(Value::Text("main st".into())),
                "{ctx}: street"
            );
            assert_eq!(udt.fields[1].name, "city", "{ctx}: field 1 name");
            assert_eq!(udt.fields[1].value, None, "{ctx}: city must be null");
        }
        other => panic!("{ctx}: expected Value::Udt, got {other:?}"),
    }
}

/// Assert a bounded decode was REFUSED, and that the refusal is the consumption
/// contract's and not some unrelated error. `expected_consumed`/`expected_len`
/// pin WHICH boundary fired, so a test cannot pass on a coincidental corruption.
fn assert_refused_short(
    result: Result<Value>,
    expected_consumed: usize,
    expected_len: usize,
    ctx: &str,
) {
    match result {
        Ok(v) => panic!(
            "{ctx}: expected the bounded-consumption refusal (Cassandra TupleType.split), got Ok({v:?})"
        ),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains(&format!("decoded only {expected_consumed} of {expected_len} byte(s)")),
                "{ctx}: expected a short-consumption refusal naming {expected_consumed}/{expected_len}, got: {msg}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// ARM 1 — marshal-form UDT (the `other if Self::is_udt_type(other)` arm)
// ---------------------------------------------------------------------------

/// Case 1. **CONTROL / NON-DISCRIMINATING.** Cassandra accepts (the loop
/// completes with `position == length`), and so must CQLite — before AND after
/// the fix. If this ever fails, the harness is wrong, not the decoder.
#[test]
fn marshal_arm_case1_exact_decodes_ok() {
    let value = decode(MARSHAL_UDT, &case1_exact()).expect("case 1 is a well-formed UDT value");
    assert_both_fields(&value, "marshal/case1");
}

/// Case 2. **DISCRIMINATING.** `TupleType.split` rule 4: `position(18) <
/// length(19)` ⇒ `"Expected 2 values for ... column, but got more"`.
///
/// Before #3811 CQLite ACCEPTED this — the marshal arm returned a short
/// `current_offset` and `raw_value.rs` discarded it with
/// `let (val, _offset) = …`. It is now refused by the wrapper's
/// `require_fully_consumed_raw`.
#[test]
fn marshal_arm_case2_trailing_garbage_is_refused() {
    assert_refused_short(
        decode(MARSHAL_UDT, &case2_trailing_garbage()),
        18,
        19,
        "marshal/case2 (rule 4: trailing 0xAA)",
    );
}

/// Case 3. **DISCRIMINATING.** `TupleType.split` rule 2 at component 1:
/// `position(11) + 4 > length(12)` ⇒ `"Not enough bytes to read 1th component"`.
///
/// The decoder's loop guard `if current_offset + 4 > udt_data.len()` collapses
/// this onto the LEGAL case 4, fills `city` with implicit null and `break`s
/// WITHOUT advancing past the stray byte — so the reported consumption is short
/// by exactly one, which is the same observable as trailing garbage and is what
/// the one comparison at the bounded caller refuses. Note case 4 (ONE BYTE
/// shorter) must keep passing.
#[test]
fn marshal_arm_case3_partial_prefix_is_refused() {
    assert_refused_short(
        decode(MARSHAL_UDT, &case3_partial_prefix()),
        11,
        12,
        "marshal/case3 (rule 2: partial 1-byte component-length prefix)",
    );
}

/// Case 4. **CONTROL / NON-DISCRIMINATING.** `TupleType.split` rule 1:
/// `position(11) == length(11)` before component 1 ⇒ legal short return with
/// `city` null. This is how a UDT that gained a field after the row was written
/// still reads, so it must KEEP passing. An "every declared field must be
/// present" fix would wrongly break exactly this test, which is why the check is
/// a CONSUMPTION COMPARISON and not a field-count assertion.
#[test]
fn marshal_arm_case4_legally_short_decodes_ok_with_null_tail() {
    let value = decode(MARSHAL_UDT, &case4_legally_short())
        .expect("a legally short UDT encoding is ACCEPTED by TupleType.split (position == length)");
    assert_city_absent(&value, "marshal/case4");
}

/// Supplementary — the `case 1 || 0x00` spelling. **DISCRIMINATING**, and it is
/// a **rule 4** case, not a rule 2 one: with every declared field present the
/// component loop is already exhausted when the stray byte is reached, so this
/// merely duplicates case 2's RULE while differing in its bytes. Labelled as
/// what it is rather than as a second partial-prefix case.
#[test]
fn marshal_arm_supplementary_exact_plus_zero_is_refused() {
    assert_refused_short(
        decode(MARSHAL_UDT, &supplementary_exact_plus_zero()),
        18,
        19,
        "marshal/supplementary (rule 4, NOT rule 2)",
    );
}

/// Rule 3 — a declared component length that overruns the buffer.
/// **DISCRIMINATING ELSEWHERE**: this is refused by
/// `checked_component_len` inside the UDT loop, NOT by #3811's consumption
/// assert, so it stays green if the assert is removed. It is included because
/// the demonstration document declared rule 3 UNMEASURED and an unmeasured rule
/// is not a passing one — but it is labelled so its claim does not exceed what
/// it exercises.
#[test]
fn marshal_arm_rule3_overrun_is_refused_by_the_component_length_guard() {
    let result = decode(MARSHAL_UDT, &rule3_overrun());
    assert!(
        result.is_err(),
        "rule 3: a component length overrunning the buffer must be refused, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// ARM 2 — registry-resolved bare UDT name
// ---------------------------------------------------------------------------

/// Case 1 on the registry arm. **CONTROL / NON-DISCRIMINATING.**
#[test]
fn registry_arm_case1_exact_decodes_ok() {
    let value = decode(REGISTRY_UDT, &case1_exact()).expect("case 1 is a well-formed UDT value");
    assert_both_fields(&value, "registry/case1");
}

/// Case 2 on the registry arm. **DISCRIMINATING.** Same oracle rule (4), same
/// defect, DIFFERENT code path — the registry branch publishes its own short
/// offset and had its own `let (val, _offset) = …` discard.
#[test]
fn registry_arm_case2_trailing_garbage_is_refused() {
    assert_refused_short(
        decode(REGISTRY_UDT, &case2_trailing_garbage()),
        18,
        19,
        "registry/case2 (rule 4: trailing 0xAA)",
    );
}

/// Case 3 on the registry arm. **DISCRIMINATING.** The sibling of the marshal
/// partial-prefix guard lives in the registry branch's own loop.
#[test]
fn registry_arm_case3_partial_prefix_is_refused() {
    assert_refused_short(
        decode(REGISTRY_UDT, &case3_partial_prefix()),
        11,
        12,
        "registry/case3 (rule 2: partial 1-byte component-length prefix)",
    );
}

/// Case 4 on the registry arm. **CONTROL / NON-DISCRIMINATING** — must KEEP
/// passing after the fix.
#[test]
fn registry_arm_case4_legally_short_decodes_ok_with_null_tail() {
    let value = decode(REGISTRY_UDT, &case4_legally_short())
        .expect("a legally short UDT encoding is ACCEPTED by TupleType.split (position == length)");
    assert_city_absent(&value, "registry/case4");
}

/// Supplementary on the registry arm. **DISCRIMINATING** (rule 4).
#[test]
fn registry_arm_supplementary_exact_plus_zero_is_refused() {
    assert_refused_short(
        decode(REGISTRY_UDT, &supplementary_exact_plus_zero()),
        18,
        19,
        "registry/supplementary (rule 4, NOT rule 2)",
    );
}

/// Rule 3 on the registry arm. **DISCRIMINATING ELSEWHERE** — see the marshal
/// sibling; the guard is the component-length check, not #3811's assert.
#[test]
fn registry_arm_rule3_overrun_is_refused_by_the_component_length_guard() {
    let result = decode(REGISTRY_UDT, &rule3_overrun());
    assert!(
        result.is_err(),
        "rule 3: a component length overrunning the buffer must be refused, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// AC4 — two DISTINCT serialized inputs must not collapse to ONE `Value`
// ---------------------------------------------------------------------------

/// Case 1 vs case 2: 18 bytes and 19 bytes, one legal and one a corruption
/// Cassandra refuses. **DISCRIMINATING** — before #3811 they yielded the
/// IDENTICAL `Value` on BOTH arms, which is the distinct-inputs-one-`Value`
/// violation AC4 names. Now the legal one decodes and the corrupt one errors, so
/// the two outcomes DIFFER.
#[test]
fn case1_and_case2_no_longer_collapse_to_one_value() {
    for (arm, ty) in [("marshal", MARSHAL_UDT), ("registry", REGISTRY_UDT)] {
        let legal = decode(ty, &case1_exact()).expect("case 1 decodes");
        let corrupt = decode(ty, &case2_trailing_garbage());
        assert!(
            corrupt.is_err(),
            "{arm}: AC4 — the 19-byte trailing-garbage input must not decode"
        );
        // The property AC4 asks for is an inequality of OUTCOMES: one input
        // yields a `Value`, the other yields an error, so no `Value` produced by
        // the corrupt input can equal the legal one.
        assert_both_fields(&legal, &format!("{arm}: AC4 legal half"));
    }
}

/// Case 4 vs case 3: 11 bytes and 12 bytes, ONE BYTE APART, one legal (rule 1)
/// and one a corruption (rule 2). **DISCRIMINATING**, and this is the half of
/// AC4 that a test for case 2 alone does not reach — it is the boundary a
/// field-count-based fix would get wrong in the other direction.
#[test]
fn case4_and_case3_no_longer_collapse_to_one_value() {
    for (arm, ty) in [("marshal", MARSHAL_UDT), ("registry", REGISTRY_UDT)] {
        let legal = decode(ty, &case4_legally_short()).expect("case 4 decodes");
        let corrupt = decode(ty, &case3_partial_prefix());
        assert!(
            corrupt.is_err(),
            "{arm}: AC4 — the 12-byte partial-prefix input must not decode"
        );
        assert_city_absent(&legal, &format!("{arm}: AC4 legal half"));
    }
}

// ---------------------------------------------------------------------------
// Census finding A — the COLLECTION arms of the same function, which discarded
// their callee's count at three further sites
// ---------------------------------------------------------------------------

/// A bounded `list<int>` element whose blob ends exactly. **CONTROL /
/// NON-DISCRIMINATING.**
#[test]
fn bounded_list_exact_decodes_ok() {
    let value = decode("list<int>", &frozen_list_one_int()).expect("a well-formed frozen list");
    assert_eq!(value, Value::List(vec![Value::Integer(7)]));
}

/// The same `list<int>` with one trailing byte. **DISCRIMINATING.**
///
/// Oracle: `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/ListSerializer.java:135`
/// throws `"Unexpected extraneous bytes after list value"`. Before #3811 the arm
/// spelled `let (val, _) = self.parse_frozen_list_value_raw(…)` and this decoded
/// to the SAME `List([Integer(7)])` as the exact encoding.
#[test]
fn bounded_list_with_trailing_byte_is_refused() {
    let mut bytes = frozen_list_one_int();
    let exact = bytes.len();
    bytes.push(0xAA);
    assert_refused_short(
        decode("list<int>", &bytes),
        exact,
        exact + 1,
        "list<int> trailing byte (ListSerializer: extraneous bytes)",
    );
}

/// The set arm's sibling. **DISCRIMINATING** — `SetSerializer.java:127-128`
/// carries the identical guard, and this is a separate discard site.
#[test]
fn bounded_set_with_trailing_byte_is_refused() {
    let mut bytes = frozen_list_one_int();
    let exact = bytes.len();
    bytes.push(0xAA);
    assert_refused_short(
        decode("set<int>", &bytes),
        exact,
        exact + 1,
        "set<int> trailing byte (SetSerializer: extraneous bytes)",
    );
}

// ---------------------------------------------------------------------------
// Census finding F — the frozen TUPLE arm reported the DECLARED extent
// ---------------------------------------------------------------------------

/// A bounded `tuple<text, text>` whose components end exactly. **CONTROL /
/// NON-DISCRIMINATING.**
#[test]
fn bounded_tuple_exact_decodes_ok() {
    let value = decode("tuple<text, text>", &case1_exact()).expect("a well-formed frozen tuple");
    assert_eq!(
        value,
        Value::Tuple(vec![
            Value::Text("main st".into()),
            Value::Text("nyc".into())
        ])
    );
}

/// The same tuple with one trailing byte. **DISCRIMINATING.** `TupleType.split`
/// rule 4 governs tuples directly (it is literally `TupleType`'s own method).
/// Before #3811 the arm discarded `off` entirely.
#[test]
fn bounded_tuple_with_trailing_byte_is_refused() {
    assert_refused_short(
        decode("tuple<text, text>", &case2_trailing_garbage()),
        18,
        19,
        "tuple<text,text> trailing byte (rule 4)",
    );
}

/// A tuple whose trailing component is absent and whose buffer ends exactly.
/// **CONTROL / NON-DISCRIMINATING** — rule 1 is legal for tuples too, so the
/// finding-F fix must not turn a short tuple into an error.
#[test]
fn bounded_tuple_legally_short_decodes_ok() {
    let value = decode("tuple<text, text>", &case4_legally_short())
        .expect("a legally short tuple encoding is accepted (TupleType.split rule 1)");
    assert_eq!(
        value,
        Value::Tuple(vec![Value::Text("main st".into()), Value::Null])
    );
}

/// A tuple with a partial component-length prefix. **DISCRIMINATING** — rule 2,
/// and the one-byte-apart sibling of the case above.
#[test]
fn bounded_tuple_partial_prefix_is_refused() {
    assert_refused_short(
        decode("tuple<text, text>", &case3_partial_prefix()),
        11,
        12,
        "tuple<text,text> partial prefix (rule 2)",
    );
}

// ---------------------------------------------------------------------------
// Census finding D — fixed-width scalars decoded from a PREFIX of an over-wide
// slice
// ---------------------------------------------------------------------------

/// An exactly-4-byte `int`. **CONTROL / NON-DISCRIMINATING.**
#[test]
fn bounded_int_exact_decodes_ok() {
    assert_eq!(
        decode("int", &[0, 0, 0, 7]).expect("4 bytes is an int"),
        Value::Integer(7)
    );
}

/// A 5-byte declared `int`. **DISCRIMINATING.**
///
/// Oracle: `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/Int32Serializer.java:42-43`
/// — `if (accessor.size(value) != 4 && !accessor.isEmpty(value)) throw`. Before
/// #3811 this decoded `Integer(7)` from the first four bytes, i.e. two distinct
/// serialized values collapsed to one `Value`. The refusal comes from the
/// consumption assert (the arm reports its exact width), NOT from a `!= 4`
/// test — because Cassandra's legal widths are `{4, 0}`, so a bare `!= 4` would
/// be a false refusal of a legal EMPTY value.
#[test]
fn bounded_int_over_width_is_refused() {
    assert_refused_short(decode("int", &[0, 0, 0, 7, 0xAA]), 4, 5, "int over-width");
}

/// A 17-byte declared `uuid`. **DISCRIMINATING** — the same rule on a different
/// width, so the fix is not a special case of the 4-byte one.
#[test]
fn bounded_uuid_over_width_is_refused() {
    let bytes = [0u8; 17];
    assert_refused_short(decode("uuid", &bytes), 16, 17, "uuid over-width");
}
