//! Issue #3811 — DEMONSTRATION of the unenforced consumption/bounds contract in
//! [`V5CompressedLegacyParser::parse_value_from_raw_bytes`].
//!
//! # READ THIS BEFORE "FIXING" A FAILURE HERE
//!
//! **These tests pin the CURRENT, WRONG behaviour on purpose.** Four of them
//! (`*_is_accepted_today_*`) assert that CQLite ACCEPTS input that
//! `cassandra-5.0.8` `TupleType.split` REJECTS. They are written that way so the
//! defect is a *demonstrated fact* rather than an argument from reading source,
//! and so that **the moment #3811's fix lands they FAIL** and must be flipped to
//! the Cassandra-correct expectation (an `Err`). A test that passed both before
//! and after the fix would be worthless here, and this repo treats a
//! fix-invariant test as a defect class in its own right.
//!
//! Flip-on-fix checklist, when the consumption assert is wired at the bounded
//! caller: every `#[test]` below whose name contains `is_accepted_today` becomes
//! `assert!(result.is_err())` with the Cassandra message class named, and the two
//! `collapse` tests become "the two inputs yield DIFFERENT outcomes".
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
//! 3. after the loop, `position < length` ⇒ `MarshalException`
//!    `"Expected N values for <type> column, but got more"`. Full consumption is
//!    REQUIRED, not optional.
//!
//! # The claimed defect (issue #3811 AC3, census §5-A/§5-B)
//!
//! `parse_value_from_raw_bytes` (`raw_value.rs:89`) is DOCUMENTED as bounded —
//! "The entire `data` slice IS the value" — but returns a bare `Result<Value>`
//! with no consumption channel, and its two UDT arms discard the count their
//! callee reports:
//!
//! - `raw_value.rs:458-459` — **marshal-form arm** (`other if Self::is_udt_type`),
//!   reached with an `org.apache.cassandra.db.marshal.UserType(...)` type string;
//! - `raw_value.rs:479-480` — **registry-resolved bare-name arm**, reached with a
//!   bare UDT name that `UdtRegistry::get_udt_qualified` resolves.
//!
//! Both are exercised here with the SAME four byte-vectors, because they are
//! separate code paths reaching the same rule and #3631's history is a fix
//! landing on one arm and not its sibling.
//!
//! # The four cases (bytes are one `addr{street text, city text}` UDT)
//!
//! | # | case | bytes | Cassandra | CQLite today |
//! |---|---|---|---|---|
//! | 1 | exact — both fields, no leftover | 18 B | accept | accept (control) |
//! | 2 | trailing garbage — case 1 `\|\| 0xAA` | 19 B | rule 3 throw | **accepts** |
//! | 3 | partial 1-byte prefix — case 4 `\|\| 0x00` | 12 B | rule 2 throw | **accepts** |
//! | 4 | legally short — `city` absent, buffer ends | 11 B | accept, `city` null | accept |
//!
//! **Cases 3 and 4 are ONE BYTE apart and that is the point.** A naive "every
//! declared field must be present" fix would wrongly break case 4, which is how a
//! UDT that gained fields after the row was written still reads. Case 3 is
//! deliberately built as *case 4 plus one stray byte* rather than *case 1 plus one
//! stray byte*: with every declared field present the loop is already exhausted,
//! so a stray byte there is rule 3 ("got more") and merely duplicates case 2. It
//! takes an ABSENT trailing field for rule 2 ("Not enough bytes") to be reachable
//! at all. The `case 1 || 0x00` variant the plan table spells is kept as a
//! supplementary test so the collapse is on record for that spelling too.
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

// ---------------------------------------------------------------------------
// ARM 1 — marshal-form UDT (`raw_value.rs:458-459`)
// ---------------------------------------------------------------------------

/// Case 1, CONTROL. Cassandra accepts (loop completes, `position == length`), and
/// so must CQLite — before AND after the fix. If this ever fails, the harness is
/// wrong, not the decoder.
#[test]
fn marshal_arm_case1_exact_decodes_ok() {
    let value = decode(MARSHAL_UDT, &case1_exact()).expect("case 1 is a well-formed UDT value");
    assert_both_fields(&value, "marshal/case1");
}

/// Case 2. **Cassandra REJECTS** — `TupleType.split` rule 3, `position(18) <
/// length(19)` ⇒ `"Expected 2 values for ... column, but got more"`.
///
/// CQLite ACCEPTS, and this test pins that wrong behaviour (issue #3811 AC3,
/// census §5-B route 2: the marshal arm returns a short `current_offset` and
/// `raw_value.rs:458` discards it with `let (val, _offset) = …`). **FLIP TO
/// `is_err()` WHEN #3811 LANDS.**
#[test]
fn marshal_arm_case2_trailing_garbage_is_accepted_today() {
    let bytes = case2_trailing_garbage();
    let value = decode(MARSHAL_UDT, &bytes)
        .expect("DEFECT #3811: trailing garbage is accepted today; Cassandra throws MarshalException \"but got more\" (TupleType.split, position < length)");
    assert_both_fields(&value, "marshal/case2 (defect: trailing 0xAA ignored)");
}

/// Case 3. **Cassandra REJECTS** — `TupleType.split` rule 2 at component 1,
/// `position(11) + 4 > length(12)` ⇒ `"Not enough bytes to read 1th component"`.
///
/// CQLite ACCEPTS: the loop guard `if current_offset + 4 > udt_data.len()`
/// (`raw_type_value.rs:697`) collapses this onto the LEGAL case 4, fills `city`
/// with implicit null and `break`s WITHOUT advancing past the stray byte — so the
/// reported offset is short by exactly one, and `raw_value.rs:458` discards it.
/// **FLIP TO `is_err()` WHEN #3811 LANDS** — and note case 4 must keep passing.
#[test]
fn marshal_arm_case3_partial_prefix_is_accepted_today() {
    let bytes = case3_partial_prefix();
    let value = decode(MARSHAL_UDT, &bytes)
        .expect("DEFECT #3811: a partial 1-byte component-length prefix is accepted today; Cassandra throws MarshalException \"Not enough bytes to read 1th component\" (TupleType.split, position + 4 > length)");
    assert_city_absent(
        &value,
        "marshal/case3 (defect: stray 0x00 read as an omitted field)",
    );
}

/// Case 4. **Cassandra ACCEPTS** — `TupleType.split` rule 1,
/// `position(11) == length(11)` before component 1 ⇒ legal short return with
/// `city` null. This is how a UDT that gained a field after the row was written
/// still reads, so it must KEEP passing after #3811's fix. A "every declared
/// field must be present" fix would wrongly break exactly this test.
#[test]
fn marshal_arm_case4_legally_short_decodes_ok_with_null_tail() {
    let value = decode(MARSHAL_UDT, &case4_legally_short())
        .expect("a legally short UDT encoding is ACCEPTED by TupleType.split (position == length)");
    assert_city_absent(&value, "marshal/case4");
}

/// Supplementary: the `case 1 || 0x00` spelling. Same oracle verdict as case 2
/// (rule 3, "got more") because the component loop is already exhausted.
/// **FLIP TO `is_err()` WHEN #3811 LANDS.**
#[test]
fn marshal_arm_supplementary_exact_plus_zero_is_accepted_today() {
    let value = decode(MARSHAL_UDT, &supplementary_exact_plus_zero())
        .expect("DEFECT #3811: a trailing 0x00 after a complete UDT is accepted today; Cassandra throws \"but got more\"");
    assert_both_fields(&value, "marshal/supplementary");
}

// ---------------------------------------------------------------------------
// ARM 2 — registry-resolved bare UDT name (`raw_value.rs:479-480`)
// ---------------------------------------------------------------------------

/// Case 1, CONTROL for the registry arm.
#[test]
fn registry_arm_case1_exact_decodes_ok() {
    let value = decode(REGISTRY_UDT, &case1_exact()).expect("case 1 is a well-formed UDT value");
    assert_both_fields(&value, "registry/case1");
}

/// Case 2 on the registry arm. Same oracle (rule 3), same defect, DIFFERENT code
/// path: the short offset is published at `raw_type_value.rs:1087` and discarded
/// at `raw_value.rs:479`. **FLIP TO `is_err()` WHEN #3811 LANDS.**
#[test]
fn registry_arm_case2_trailing_garbage_is_accepted_today() {
    let value = decode(REGISTRY_UDT, &case2_trailing_garbage())
        .expect("DEFECT #3811: trailing garbage is accepted today on the registry-resolved arm too; Cassandra throws \"but got more\"");
    assert_both_fields(&value, "registry/case2 (defect: trailing 0xAA ignored)");
}

/// Case 3 on the registry arm — the sibling of the marshal partial-prefix guard
/// lives at `raw_type_value.rs:934`. **FLIP TO `is_err()` WHEN #3811 LANDS.**
#[test]
fn registry_arm_case3_partial_prefix_is_accepted_today() {
    let value = decode(REGISTRY_UDT, &case3_partial_prefix())
        .expect("DEFECT #3811: a partial 1-byte component-length prefix is accepted today on the registry-resolved arm too; Cassandra throws \"Not enough bytes to read 1th component\"");
    assert_city_absent(
        &value,
        "registry/case3 (defect: stray 0x00 read as an omitted field)",
    );
}

/// Case 4 on the registry arm — must KEEP passing after the fix.
#[test]
fn registry_arm_case4_legally_short_decodes_ok_with_null_tail() {
    let value = decode(REGISTRY_UDT, &case4_legally_short())
        .expect("a legally short UDT encoding is ACCEPTED by TupleType.split (position == length)");
    assert_city_absent(&value, "registry/case4");
}

/// Supplementary on the registry arm. **FLIP TO `is_err()` WHEN #3811 LANDS.**
#[test]
fn registry_arm_supplementary_exact_plus_zero_is_accepted_today() {
    let value = decode(REGISTRY_UDT, &supplementary_exact_plus_zero())
        .expect("DEFECT #3811: a trailing 0x00 after a complete UDT is accepted today; Cassandra throws \"but got more\"");
    assert_both_fields(&value, "registry/supplementary");
}

// ---------------------------------------------------------------------------
// AC4 — two DISTINCT serialized inputs must not collapse to ONE `Value`
// ---------------------------------------------------------------------------

/// Case 1 vs case 2: 18 bytes and 19 bytes, one legal and one a corruption
/// Cassandra refuses, currently yield the IDENTICAL `Value` on BOTH arms. That
/// equality is the distinct-inputs-one-Value violation #3811 names. **FLIP TO
/// `assert_ne!`-style (or an `Err` on the corrupt half) WHEN #3811 LANDS.**
#[test]
fn collapse_case1_vs_case2_yields_one_value_today() {
    for (arm, ty) in [("marshal", MARSHAL_UDT), ("registry", REGISTRY_UDT)] {
        let legal = decode(ty, &case1_exact()).expect("case 1 decodes");
        let corrupt = decode(ty, &case2_trailing_garbage())
            .expect("DEFECT #3811: the corrupt input decodes instead of erroring");
        assert_eq!(
            legal, corrupt,
            "{arm}: DEFECT #3811 — 18-byte legal input and 19-byte trailing-garbage input collapse to one Value"
        );
    }
}

/// Case 4 vs case 3: 11 bytes and 12 bytes, ONE BYTE APART, one legal (rule 1)
/// and one a corruption (rule 2), currently yield the IDENTICAL `Value` on BOTH
/// arms. This is the half of AC4 a test for case 2 alone does not reach. **FLIP
/// WHEN #3811 LANDS.**
#[test]
fn collapse_case4_vs_case3_yields_one_value_today() {
    for (arm, ty) in [("marshal", MARSHAL_UDT), ("registry", REGISTRY_UDT)] {
        let legal = decode(ty, &case4_legally_short()).expect("case 4 decodes");
        let corrupt = decode(ty, &case3_partial_prefix())
            .expect("DEFECT #3811: the corrupt input decodes instead of erroring");
        assert_eq!(
            legal, corrupt,
            "{arm}: DEFECT #3811 — 11-byte legal short input and 12-byte partial-prefix input collapse to one Value"
        );
    }
}
