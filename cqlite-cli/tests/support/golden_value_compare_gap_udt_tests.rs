//! The GOLDEN half of `Divergence::NestedFrozenUdtRendersAsBlobHex`: a golden that
//! is really a DECODE of the declared UDT (issue #3846).
//!
//! Split out of `golden_value_compare_gap_tests.rs` under the campsite rule
//! (CLAUDE.md, epic #1135), which had reached the ~1500-line test target. A child
//! of `golden_value_compare_tests.rs`, so the shared `row`/`schema_of` helpers and
//! the committed `NESTED_UDT_DDL` are reached through `use super::*` and are stated
//! once.
//!
//! # What these cases are about
//!
//! That gap's premise is "the GOLDEN decoded the nested frozen UDT while the egress
//! rendered raw bytes", so the golden side is where its expectation may come from at
//! all (#3042). It used to be read as `matches!(golden, Value::Object(_))`: object-ness
//! stood in for "a decode of the declared UDT", so an object whose FIELD NAMES, field
//! ORDER or leaf KINDS are not the committed `CREATE TYPE`'s satisfied the premise and
//! the gap suppressed the position anyway (issue #3846). That is the same weakness the
//! sibling `NestedFrozenValueLeftUndecodedByGolden` closed on its EGRESS side (roborev
//! jobs 32/38/105) — one variant over, on the golden side.
//!
//! Every expectation below is the committed DDL's or `cassandra-5.0.8`'s, never
//! CQLite's output: `UserType.toJSONString` walks the DECLARED field list and writes
//! every declared field (`null` when its buffer is absent), so a decoded golden
//! carries exactly the declared fields, in declaration order, each spelled as its
//! declared type's own `toJSONString`.
//!
//! Each rule is pinned from BOTH sides — a decode that must still match, and a
//! non-decode that must be REPORTED — so narrowing the rule back reds a case rather
//! than quietly restoring the suppression.

use super::super::super::container::MapKeySpelling;
use super::super::super::schema::CqlType;
use super::super::gap::Position;
use super::*;
use serde_json::json;

/// The blob-hex spelling of the nested UDT's serialized bytes — the EGRESS half of
/// the gap, held fixed by every case here so each one varies only the golden.
/// Synthetic since #3631 (the egress decodes that field now); the exact bytes are not
/// what the gap is keyed on.
const HOME_AS_BLOB_HEX: &str =
    "0x0000000a31204e617679205761790000000941726c696e67746f6e000000053232323031";

/// The declared type of a FIELD of a UDT column, read through the REAL schema reader
/// so every case below is asked about the committed DDL's own type rather than a
/// hand-built one.
fn field_ty(ddl: &str, column: &str, field: &str) -> CqlType {
    let schema = schema_of(ddl, "t");
    let Some(col) = schema.column(column) else {
        panic!("the DDL must declare the column `{column}`");
    };
    let CqlType::Udt(udt) = &col.ty else {
        panic!("`{column}` must be declared a UDT");
    };
    match udt.fields.iter().find(|(name, _)| name == field) {
        Some((_, ty)) => ty.clone(),
        None => panic!("the UDT `{}` must declare the field `{field}`", udt.name),
    }
}

/// Is the pair at `e.home` the declared divergence, with the golden varied?
fn home_gap_matched(golden: &Value) -> bool {
    let home_ty = field_ty(NESTED_UDT_DDL, "e", "home");
    Divergence::NestedFrozenUdtRendersAsBlobHex.matched(
        golden,
        &json!(HOME_AS_BLOB_HEX),
        Position {
            ty: &home_ty,
            egress: Egress::Json,
            // A UDT field's own position, as `compare::At::field` builds it.
            depth: Depth::Inside,
            kinding: Kinding::Natural,
            map_key_spelling: MapKeySpelling::ToJsonString,
        },
    )
}

/// The golden's FIELD SET and field ORDER must be the committed `CREATE TYPE`'s.
///
/// `cassandra-5.0.8 UserType.toJSONString` iterates `for (int i = 0; i < types.size();
/// i++)` over the DECLARED field list and writes every declared field, so an object
/// carrying an undeclared name, missing a declared one, or emitting them in another
/// order is not a rendering that writer can produce — hence not the decoded golden this
/// gap's premise asserts, and the position must be REPORTED rather than suppressed.
#[test]
fn the_nested_udt_gap_requires_a_golden_that_decodes_the_declared_udt() {
    // THE CONTROL FIRST: without it every refusal below could pass because the matcher
    // had stopped matching this position at all.
    assert!(
        home_gap_matched(&json!({"street": "1 Navy Way", "city": "Arlington", "zip": "22201"})),
        "the committed golden's own `e.home` object IS a decode of `address`, so blob \
         hex against it is the declared gap"
    );

    // An UNDECLARED field name (`town`), which also leaves `city` unemitted.
    assert!(
        !home_gap_matched(&json!({"street": "1 Navy Way", "town": "Arlington", "zip": "22201"})),
        "`town` is not declared by the committed CREATE TYPE, so this object is not a \
         decode of `address` and the gap must not suppress the position"
    );
    // A DECLARED field left out. `toJSONString` emits every declared field, `null`
    // included, so an absent field is a missing field and not an agreement.
    assert!(
        !home_gap_matched(&json!({"street": "1 Navy Way", "city": "Arlington"})),
        "an object missing the declared `zip` is not a decode of `address`"
    );
    // The DECLARED ORDER is street, city, zip.
    assert!(
        !home_gap_matched(&json!({"city": "Arlington", "street": "1 Navy Way", "zip": "22201"})),
        "`toJSONString` emits a UDT's fields in declaration order, so another order is \
         not a rendering it can produce"
    );
    // And a golden that is not a field→value object at all.
    for not_a_udt in [
        json!("1 Navy Way, Arlington"),
        json!(HOME_AS_BLOB_HEX),
        json!(null),
        json!(9),
        json!(["1 Navy Way", "Arlington", "22201"]),
    ] {
        assert!(
            !home_gap_matched(&not_a_udt),
            "{not_a_udt} is not the decoded golden object this gap declares"
        );
    }
}

/// A one-off DDL with a NUMERIC and a BLOB nested field, because the committed
/// `address` is text-only and every string is a well-formed `text` value — so the leaf
/// KINDS could not be exercised through it.
const TYPED_NESTED_DDL: &str = "CREATE TYPE geo (lat int, tag blob); \
     CREATE TYPE place (name text, at frozen<geo>); \
     CREATE TABLE t (id int PRIMARY KEY, p frozen<place>);";

fn geo_gap_matched(golden: &Value) -> bool {
    let at_ty = field_ty(TYPED_NESTED_DDL, "p", "at");
    Divergence::NestedFrozenUdtRendersAsBlobHex.matched(
        golden,
        &json!(HOME_AS_BLOB_HEX),
        Position {
            ty: &at_ty,
            egress: Egress::Json,
            depth: Depth::Inside,
            kinding: Kinding::Natural,
            map_key_spelling: MapKeySpelling::ToJsonString,
        },
    )
}

/// The golden's LEAF KINDS must be the ones the declared field types imply.
///
/// The field set, the order and the shape can all be the DDL's while a leaf is not a
/// value of its declared type — `"not-an-int"` at a declared `int`, bare hex at a
/// declared `blob` — and at this position the golden is the ONLY side that could carry
/// the expectation, so reading object-ness as validity suppressed a malformed oracle.
/// Same read-back the sibling gap does on its egress side (roborev jobs 38/105).
///
/// Authorities for the two spellings, both `cassandra-5.0.8`: `Int32Type.toJSONString`
/// writes the value with `writeRawValue`, i.e. a JSON NUMBER; `BytesType.toJSONString`
/// returns `"\"0x" + ByteBufferUtil.bytesToHex(buffer) + '"'`, i.e. `0x` and an even
/// number of hex digits (`BytesSerializer.toString`'s BARE hex is the `getString`
/// spelling, which is not what this position carries).
#[test]
fn the_nested_udt_gap_requires_golden_leaves_of_the_declared_kinds() {
    assert!(
        geo_gap_matched(&json!({"lat": 42, "tag": "0xdeadbeef"})),
        "a JSON number at `int` and a `0x` hex literal at `blob` are what \
         toJSONString writes, so this IS the decoded golden the gap declares"
    );
    assert!(
        !geo_gap_matched(&json!({"lat": "not-an-int", "tag": "0xdeadbeef"})),
        "text at a declared `int` is not a value toJSONString can write there"
    );
    assert!(
        !geo_gap_matched(&json!({"lat": 42, "tag": "deadbeef"})),
        "bare hex is BytesSerializer.toString's getString spelling, not the `0x` \
         literal toJSONString writes at a natural position"
    );
    // AND A NULL FIELD IS STILL A DECODE, which is what keeps this from being a
    // tightening the oracle contradicts: `UserType.toJSONString` writes `null` for a
    // field whose buffer is absent, so every declared field is present and a null one
    // is legal at that position.
    assert!(
        geo_gap_matched(&json!({"lat": null, "tag": "0x"})),
        "a null UDT field and the empty blob are both what toJSONString writes"
    );
}
