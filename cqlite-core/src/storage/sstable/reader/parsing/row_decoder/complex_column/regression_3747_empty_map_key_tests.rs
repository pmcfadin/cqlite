//! Issue #3747 — synthetic-bytes coverage for the EMPTY multicell MAP KEY.
//!
//! # What the fix was, after #3612 landed
//! The map branch guarded its cell-path key decode on `!path_bytes.is_empty()`, so
//! an entry whose key is the empty value produced `decoded_key == None` and was
//! dropped from the reconstructed `Value::Map` entirely — a `SELECT` returned a map
//! SHORT ONE ENTRY, silently. The fix removes that guard. It does **not** decide
//! which empty keys are legal, and that separation is the whole point of this file.
//!
//! # WHY THIS FILE NO LONGER CARRIES A PER-TYPE LADDER OF ITS OWN
//! An earlier revision of this change added one: an explicit `blob` arm, a `varint`
//! arm, and a catch-all that REFUSED a zero-length key for every unmodelled type.
//! Its per-type verdicts were justified against **CQLite's own decoders**
//! (`custom_scalar.rs`, `raw_value.rs`, `partition_key_codec.rs`). That was circular
//! reasoning and it produced WRONG ANSWERS — CLAUDE.md is explicit that a CQLite
//! `file:line` is never format authority.
//!
//! #3612 (PR #3736) then landed [`super::cell_path_key::…::cell_path_key_allowed_widths`],
//! derived from **Cassandra's serializers**, and it disagrees with that earlier
//! revision on most fixed-width families. Cassandra's shape is
//! `size != N && !isEmpty` throws — so an EMPTY buffer is *legal* wherever the
//! serializer spells the check that way:
//!
//! | key type                              | empty legal? |
//! |---------------------------------------|--------------|
//! | `int`, `float`                        | YES (`[0,4]`) |
//! | `bigint`, `counter`, `double`, `timestamp` | YES (`[0,8]`) |
//! | `uuid`, `timeuuid`                    | YES (`[0,16]`) |
//! | `boolean`                             | YES (`[0,1]`) |
//! | `inet`                                | YES (`[0,4,16]`) — `InetAddressSerializer.validate` returns early on empty |
//! | `tinyint`, `smallint`, `date`, `time` | NO — strict `!= N` |
//! | text/ascii/varchar, blob, varint, decimal, composites | YES — variable width |
//!
//! So the correct fix here is **only** to remove the guard and let that authority
//! see the empty case it was previously shielded from. The tests below therefore
//! pin the GUARD's removal and the DELEGATION, and deliberately do not restate the
//! width table — restating it would create a second opinion that can drift from the
//! one #3612 derived, which is the defect this note exists to prevent recurring.

use super::V5CompressedLegacyParser;
use crate::parser::vint::encode_vuint;
use crate::schema::Column;
use crate::types::{EmptyValueType, Value};

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("ks".to_string(), "t".to_string(), 0, 0, None)
}

fn column(cql_type: &str) -> Column {
    Column {
        name: "m".to_string(),
        data_type: cql_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// One multicell complex column holding exactly one cell: `[vuint cell_count]`
/// then `[flags][vuint path_len][path][vuint value_len][value]`. `flags = 0x08` is
/// USE_ROW_TIMESTAMP, which lets the fixture omit the per-cell timestamp delta.
fn one_cell_column(path: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = encode_vuint(1);
    out.push(0x08);
    out.extend_from_slice(&encode_vuint(path.len() as u64));
    out.extend_from_slice(path);
    out.extend_from_slice(&encode_vuint(value.len() as u64));
    out.extend_from_slice(value);
    out
}

fn decode(map_type: &str, path: &[u8], value: &[u8]) -> crate::Result<Value> {
    let p = parser();
    let col = column(map_type);
    let bytes = one_cell_column(path, value);
    p.parse_complex_column_inner(&bytes, 0, &col, map_type, false, 1_000, None, None)
        .map(|(v, _, _)| v)
}

/// THE FIX. `map<text,int>` with a zero-length cell path must decode to `("" -> 7)`.
///
/// RED-VERIFIED by reinstating the guard: this returned `Map([])` — the entry was
/// dropped with no error at all, which is exactly what made the defect silent.
#[test]
fn zero_length_text_key_is_a_legal_empty_key() {
    let decoded = decode("map<text,int>", b"", &7i32.to_be_bytes())
        .expect("a zero-length text key is legal data, not corruption");
    assert_eq!(
        decoded,
        Value::Map(vec![(Value::text(""), Value::Integer(7))]),
        "the empty key must reach the reconstructed map"
    );
}

/// The same for `blob`, reached through a different arm of the value decoder.
#[test]
fn zero_length_blob_key_is_a_legal_empty_key() {
    let decoded = decode("map<blob,int>", b"", &7i32.to_be_bytes())
        .expect("a zero-length blob key is legal data");
    assert_eq!(
        decoded,
        Value::Map(vec![(Value::blob(Vec::new()), Value::Integer(7))])
    );
}

/// DELEGATION, the `empty is LEGAL` direction — MEASURED, and the case that proves
/// an earlier revision of this fix was wrong about `inet`.
///
/// These five decode an empty key to an empty value. An earlier revision of #3747
/// REFUSED `inet`, reasoning from `custom_scalar.rs` (which rejects any inet length
/// but 4/16). roborev flagged that and was right: `InetAddressSerializer.validate`
/// returns early on empty, #3612's width table admits `[0,4,16]`, and the decoder
/// really does produce `Inet(b"")`.
#[test]
fn empty_key_decodes_for_the_families_that_admit_it() {
    let cases: &[(&str, Value)] = &[
        ("text", Value::text("")),
        ("ascii", Value::text("")),
        ("varchar", Value::text("")),
        ("blob", Value::blob(Vec::new())),
        ("varint", Value::varint(Vec::new())),
        ("inet", Value::inet(Vec::new())),
    ];
    for (ty, want) in cases {
        let map_type = format!("map<{ty},int>");
        let decoded = decode(&map_type, b"", &7i32.to_be_bytes())
            .unwrap_or_else(|e| panic!("an empty {ty} key must decode; got {e}"));
        assert_eq!(
            decoded,
            Value::Map(vec![(want.clone(), Value::Integer(7))]),
            "empty {ty} key"
        );
    }
}

/// CASSANDRA-VALID, AND NOW TYPED — the entry survives as the `Value::Empty`
/// SENTINEL carrying its declared family. This is the case roborev pushed on
/// across four rounds and was right about every time.
///
/// Cassandra's `size != N && !isEmpty` shape makes an EMPTY buffer legal for the
/// `N`-or-`0` families, and #3612's width table encodes that. **#3805 slice 2
/// DELIBERATELY SUPERSEDED #3747's opaque-blob placeholder**: when this test was
/// written CQLite had no `Value` that could carry an empty fixed-width scalar, so
/// the module applied its existing policy for an unmodellable key — surface the
/// bytes opaquely and raise `opaque_out`, never drop and never `Err` — and the
/// arm's own comment recorded the seam ("Typed: #3805"). Slice 1 added
/// [`crate::types::Value::Empty`] + [`crate::types::EmptyValueType`], so the key
/// is now PRESENT, TYPED and NOT opaque; `opaque_out` is deliberately left unset,
/// because emitting a `warn!` per entry per row for correct data was the
/// diagnostic defect #3612 closed.
///
/// The two earlier revisions this test was written against are still wrong for the
/// same reasons: one let the error propagate (which `break`s row assembly and
/// takes the column plus every later one), the other dropped the entry (the very
/// data loss #3747 exists to stop).
///
/// ORACLE — the admitted set and its per-family authority is
/// `EmptyValueType`'s membership rule, derived from `cassandra-5.0.8`'s
/// serializers (`Int32Serializer.java:40-44` *"Expected 4 or 0 byte int"*, and
/// its siblings); never from CQLite's prior output, which is exactly the opaque
/// blob this test used to pin.
#[test]
fn a_cassandra_valid_empty_key_survives_as_the_typed_empty_sentinel() {
    let cases: &[(&str, EmptyValueType)] = &[
        ("int", EmptyValueType::Int),
        ("float", EmptyValueType::Float),
        ("bigint", EmptyValueType::BigInt),
        ("double", EmptyValueType::Double),
        ("timestamp", EmptyValueType::Timestamp),
        ("uuid", EmptyValueType::Uuid),
        ("timeuuid", EmptyValueType::TimeUuid),
        ("boolean", EmptyValueType::Boolean),
    ];
    for (ty, tag) in cases {
        let map_type = format!("map<{ty},int>");
        let decoded = decode(&map_type, b"", &7i32.to_be_bytes()).unwrap_or_else(|e| {
            panic!("Cassandra admits an empty {ty}; the entry must survive, not error: {e}")
        });
        assert_eq!(
            decoded,
            Value::Map(vec![(Value::Empty(*tag), Value::Integer(7))]),
            "the empty {ty} key must be PRESERVED as the TYPED sentinel, not dropped, \
             not refused and no longer an opaque blob (#3805 slice 2)"
        );
    }
}

/// The MARSHAL spelling of the same eight families must reach the same tag.
///
/// A `Statistics.db`-sourced key type arrives in marshal form and is normalized by
/// a DIFFERENT branch of the type classifier, so a fix that only handled the CQL
/// short form would pass the test above and leave every no-schema read opaque.
#[test]
fn the_marshal_spelling_of_an_empty_key_reaches_the_same_tag() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    let cases: &[(&str, EmptyValueType)] = &[
        ("Int32Type", EmptyValueType::Int),
        ("FloatType", EmptyValueType::Float),
        ("LongType", EmptyValueType::BigInt),
        ("DoubleType", EmptyValueType::Double),
        ("TimestampType", EmptyValueType::Timestamp),
        ("UUIDType", EmptyValueType::Uuid),
        ("TimeUUIDType", EmptyValueType::TimeUuid),
        ("BooleanType", EmptyValueType::Boolean),
    ];
    for (marshal, tag) in cases {
        let map_type = format!("map<{P}{marshal},{P}Int32Type>");
        let decoded = decode(&map_type, b"", &7i32.to_be_bytes())
            .unwrap_or_else(|e| panic!("an empty {marshal} key must survive: {e}"));
        assert_eq!(
            decoded,
            Value::Map(vec![(Value::Empty(*tag), Value::Integer(7))]),
            "marshal {marshal} must reach the same sentinel tag as its CQL short form"
        );
    }
}

/// The `frozen<…>` SPELLING of a fixed-width key type must also reach the tag.
///
/// `cell_path_key_allowed_widths` PEELS `frozen<…>` before classifying (that peel
/// is load-bearing — see its doc comment), so the sentinel lookup must peel the
/// same way or a frozen-spelled key would keep the opaque fallback while the width
/// table admitted it: a spelling cannot be handled in one helper and assumed
/// impossible in the other.
///
/// # The result is UNWRAPPED, and that is PRE-EXISTING, not a new opinion
/// This arm is the fallback taken when the decoder REFUSED the slice, so no
/// decoder produced a `Value::Frozen` wrapper to preserve — and it already
/// returned a BARE `Blob(b"")` for this spelling before #3805 slice 2. The
/// sentinel keeps that shape exactly. Introducing a `Frozen(…)` wrapper here
/// would be a presentation change with no oracle behind it (`frozen<int>` is not
/// a legal CQL map-key type, so no Cassandra-written bytes can settle it), and it
/// is deliberately NOT bundled into this fix.
#[test]
fn a_frozen_spelled_fixed_width_empty_key_reaches_the_tag() {
    let decoded = decode("map<frozen<int>,int>", b"", &7i32.to_be_bytes())
        .expect("a frozen-spelled empty int key is admitted by the same width table");
    assert_eq!(
        decoded,
        Value::Map(vec![(Value::Empty(EmptyValueType::Int), Value::Integer(7))]),
        "the frozen spelling reaches the same tag, in this arm's pre-existing \
         UNWRAPPED shape"
    );
}

/// CASSANDRA-INVALID — REFUSED, because that is where the module's committed rule
/// draws the line, and this is the one place three review rounds disagreed.
///
/// `cell_path_key`'s error-budget rule is explicit and it decides both halves:
///   * **`Err` only where Cassandra's own `validate`/`split` THROWS.** `tinyint`/
///     `smallint`/`date`/`time` are spelled with a strict `!= N` check and `decimal`
///     needs >= 4 bytes, so an empty buffer is corrupt ON CASSANDRA'S OWN TERMS.
///     Refusing adds no availability risk for data Cassandra would have read.
///   * **NEVER `Err` merely because CQLITE cannot model the type** — that is the
///     opaque case in the test above.
///
/// An earlier revision of this fix swallowed these into a dropped entry, reasoning
/// that a propagated `Err` costs more (row assembly `break`s, so the column and every
/// later one vanish). That reasoning is real but it is NOT this module's call to make:
/// the swallow is a PRE-EXISTING `row_data.rs` defect the module doc tracks
/// separately, and hiding corruption here to compensate would make an empty malformed
/// key behave differently from a non-empty one — the inconsistency roborev flagged.
#[test]
fn a_cassandra_invalid_empty_key_is_refused_like_any_other_corruption() {
    for ty in ["tinyint", "smallint", "date", "time", "decimal"] {
        let map_type = format!("map<{ty},int>");
        match decode(&map_type, b"", &7i32.to_be_bytes()) {
            Err(_) => {}
            Ok(v) => panic!(
                "Cassandra's {ty} serializer throws on an empty buffer, so an empty {ty} \
                 key is corruption and must be refused, not decoded; got {v:?}"
            ),
        }
    }
}

/// A WRONG-LENGTH (3-byte) `map<int,int>` key must still error — the fix must not
/// turn a genuine decode failure into a silent drop or a success. Behaviour is
/// UNCHANGED by the fix (the removed guard never applied to a non-empty path); the
/// case is here so the empty-key success stays distinguishable from a real failure.
#[test]
fn wrong_length_int_key_still_errors() {
    match decode("map<int,int>", &[0x01, 0x02, 0x03], &7i32.to_be_bytes()) {
        Err(_) => {}
        Ok(v) => panic!("a 3-byte int key is corruption and must error; got {v:?}"),
    }
}
