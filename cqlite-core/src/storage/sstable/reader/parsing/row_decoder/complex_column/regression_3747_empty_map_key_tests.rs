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
use crate::types::Value;

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

/// The `empty is NOT REPRESENTABLE` direction — and the assertion that matters is
/// the BLAST RADIUS, not the error.
///
/// roborev job 73 caught what an earlier revision of this fix got wrong. For the
/// families whose empty representation the downstream decoder does not support,
/// that revision let the decode error PROPAGATE. That is strictly worse than the
/// bug #3747 fixes, because row assembly has exactly one `Err` handler and it
/// `break`s the column loop — so the map column **and every later on-disk column**
/// vanish from the row, silently (`row_data.rs`; `cell_path_key.rs`'s own module
/// doc records this, reproduced there with a real `SELECT`).
///
/// I had described that to the lead as "pre-fix silently dropped, post-fix errors
/// visibly". That was wrong in both halves: it is silent either way, and the
/// post-fix loss was LARGER. So the fix now keeps the PRE-fix behaviour for exactly
/// these cases — the one entry is dropped — and the two-layer inconsistency behind
/// it stays #3805's to settle.
///
/// What this pins, therefore, is that an unrepresentable empty key costs ONE ENTRY
/// and not the column: the call must return `Ok`, and the map must simply be short.
#[test]
fn an_unrepresentable_empty_key_drops_one_entry_and_never_truncates_the_row() {
    // Both layers are represented: `tinyint`/`smallint`/`date`/`time` are refused by
    // #3612's width table (strict `!= N`), while `int`/`float`/`bigint`/`double`/
    // `timestamp`/`uuid`/`boolean` pass that table and are refused downstream. After
    // this fix the CALLER cannot tell them apart, which is the point — neither may
    // take the row down.
    for ty in [
        "tinyint",
        "smallint",
        "date",
        "time",
        "int",
        "float",
        "bigint",
        "double",
        "timestamp",
        "uuid",
        "timeuuid",
        "boolean",
        "decimal",
    ] {
        let map_type = format!("map<{ty},int>");
        let decoded = decode(&map_type, b"", &7i32.to_be_bytes()).unwrap_or_else(|e| {
            panic!(
                "an unrepresentable empty {ty} key must NOT propagate — row assembly \
                 would `break` and take the column plus every later one with it; got {e}"
            )
        });
        assert_eq!(
            decoded,
            Value::Map(Vec::new()),
            "the empty {ty} entry is dropped, so the map is short by one and the row \
             is otherwise intact"
        );
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
