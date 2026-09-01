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

/// DELEGATION, the `empty is REFUSED` direction — MEASURED, and it records an
/// INCONSISTENCY inside #3612's landed code rather than asserting a tidy rule.
///
/// Two DIFFERENT layers refuse, and the distinction is the finding:
///
///   * `tinyint`/`smallint`/`date`/`time` are refused by
///     `cell_path_key_allowed_widths` itself ("Map key … requires exactly N bytes").
///     Correct: Cassandra spells those with a strict `!= N` check.
///   * `int`/`float`/`bigint`/`double`/`timestamp`/`uuid`/`timeuuid`/`boolean` are
///     refused by the DOWNSTREAM structural decoder ("Frozen element … need N
///     bytes") — **even though the width table deliberately admits `[0,N]` for
///     them**, because Cassandra's `size != N && !isEmpty` shape makes empty legal.
///     The table's intent is defeated one layer down.
///
/// That second group is a genuine internal inconsistency, it is NOT introduced by
/// #3747 (removing the guard only made the empty case reachable), and fixing it
/// means changing fixed-width handling for every complex column and frozen element
/// — out of scope for a guard removal. Filed separately; this test PINS today's
/// behaviour so the eventual fix is a visible, deliberate change rather than a
/// silent one.
#[test]
fn empty_key_is_refused_for_the_rest_and_the_two_layers_are_distinguishable() {
    // Refused by the WIDTH TABLE (correct per Cassandra's strict `!= N` families).
    for ty in ["tinyint", "smallint", "date", "time"] {
        let map_type = format!("map<{ty},int>");
        let err = match decode(&map_type, b"", &7i32.to_be_bytes()) {
            Err(e) => e.to_string(),
            Ok(v) => panic!("{ty} has a strict width check; empty must error, got {v:?}"),
        };
        assert!(
            err.contains("requires exactly"),
            "{ty} must be refused by cell_path_key_allowed_widths, not downstream; got: {err}"
        );
    }

    // Refused DOWNSTREAM despite the width table admitting empty — the inconsistency.
    for ty in [
        "int",
        "float",
        "bigint",
        "double",
        "timestamp",
        "uuid",
        "boolean",
    ] {
        let map_type = format!("map<{ty},int>");
        let err = match decode(&map_type, b"", &7i32.to_be_bytes()) {
            Err(e) => e.to_string(),
            Ok(v) => panic!("today {ty} is refused downstream; got {v:?}"),
        };
        assert!(
            !err.contains("requires exactly"),
            "{ty} is admitted by the width table, so its refusal must come from the \
             downstream decoder — if this now says 'requires exactly', the two layers \
             have been reconciled and this test should be updated; got: {err}"
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
