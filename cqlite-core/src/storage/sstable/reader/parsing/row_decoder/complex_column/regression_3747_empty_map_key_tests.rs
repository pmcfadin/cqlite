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
/// These four decode an empty key to an empty value of their own family. An
/// earlier revision of #3747 REFUSED `inet`, reasoning from `custom_scalar.rs`
/// (which rejects any inet length but 4/16). roborev flagged that and was right:
/// `InetAddressSerializer.validate` returns early on empty and #3612's width
/// table admits `[0,4,16]`, so an empty `inet` key is LEGAL DATA. That finding
/// stands unchanged.
///
/// # `varint` and `inet` LEFT this list, and the reason is REPRESENTATION only
/// They were here asserting `Varint(b"")` / `Inet(b"")`. The empty buffer is
/// still just as LEGAL for them — nothing about the delegation changed — but the
/// VALUE the reader now produces is the typed sentinel, because they are the two
/// families in this list the tag table ADMITS (roborev job 449 finding C,
/// #4079). Their pin lives in
/// [`varint_and_inet_empty_keys_are_the_typed_sentinel_closing_4079`]; what stays
/// here is the set for which an empty buffer is a MEANINGFUL native value, which
/// is a different claim and needs its own case.
#[test]
fn empty_key_decodes_for_the_families_that_admit_it() {
    let cases: &[(&str, Value)] = &[
        ("text", Value::text("")),
        ("ascii", Value::text("")),
        ("varchar", Value::text("")),
        ("blob", Value::blob(Vec::new())),
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

// #3805/#4017 CROSS-LANE COLLISION, RULED BY THE LEAD ON PR #4033: the pin that
// stood here (`a_frozen_spelled_fixed_width_empty_key_reaches_the_tag`, asserting
// `Empty(Int)`) and #4017's opposite pin (`an_empty_frozen_spelled_fixed_width_key_
// is_also_preserved_opaquely`, asserting `Blob(b"")`) were BOTH DELETED.
//
// The oracle this file's own prose said did not exist DOES, one level up — it is
// Cassandra's GRAMMAR rather than its bytes. `CQL3Type.Raw::freeze()` throws
// "frozen<> is only allowed on collections, tuples, and user-defined types"
// (cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651) and
// only RawCollection/RawTuple/RawUT override it. So no table can carry
// `frozen<int>`, no serialization header can spell `FrozenType(Int32Type)`, and no
// Cassandra-written bytes for this input exist BY CONSTRUCTION.
//
// Under #28, where Cassandra has no behaviour CQLite must not invent one:
// `Empty(Int)` and `Blob(b"")` are both inventions. The correct behaviour is
// REFUSAL, and #4104 SHIPPED it (deliberately not bundled here): `CqlType::parse`
// and the SerializationHeader type parser both reject the spelling, so nothing
// downstream needs — or has — a frozen-scalar branch. The reachability claim that
// replaces these two pins lives in `cell_path_key_tests_frozen`. Also note the
// override set is FOUR, not three: `RawVector` (`CQL3Type.java:916`) returns
// `this`, a vector being implicitly frozen.

/// CASSANDRA-INVALID — REFUSED, because that is where the module's committed rule
/// draws the line, and this is the one place three review rounds disagreed.
///
/// `cell_path_key`'s error-budget rule is explicit and it decides both halves:
///   * **`Err` only where Cassandra's own `validate`/`split` THROWS.** `tinyint`/
///     `smallint`/`date`/`time` are spelled with a strict `!= N` check, so an empty
///     buffer is corrupt ON CASSANDRA'S OWN TERMS. Refusing adds no availability
///     risk for data Cassandra would have read.
///   * **NEVER `Err` merely because CQLITE cannot model the type** — that is the
///     typed case above and the opaque case this module's arm keeps for a family
///     with no sentinel.
///
/// An earlier revision of this fix swallowed these into a dropped entry, reasoning
/// that a propagated `Err` costs more (row assembly `break`s, so the column and every
/// later one vanish). That reasoning is real but it is NOT this module's call to make:
/// the swallow is a PRE-EXISTING `row_data.rs` defect the module doc tracks
/// separately, and hiding corruption here to compensate would make an empty malformed
/// key behave differently from a non-empty one — the inconsistency roborev flagged.
///
/// # `decimal` WAS IN THIS LIST AND IS NOT CORRUPT — the claim was wrong
/// #3747 asserted `decimal` here, and #3805 REQ-3805-02 measured the source and
/// found the claim FALSE: `DecimalSerializer.java:31-34` returns `null` for an
/// empty buffer and `:58-63` throws only `if (!accessor.isEmpty(value) &&
/// accessor.size(value) < 4)`, with the message *"Expected 0 or at least 4
/// bytes"* — zero is named as LEGAL in the message of the very check said to
/// reject it. Corroborated on the write side (`blobAsDecimal(0x)` is ACCEPTED by
/// cqlsh against cassandra:5.0.2) and by the Cassandra-WRITTEN fixture, which
/// carries an empty `m_dec` key. Slice 1's `EmptyValueType::Decimal` variant
/// records the same correction. So it moved to
/// [`an_empty_decimal_key_is_typed_because_cassandra_admits_it`], and this list
/// keeps only the families whose `validate` really is a bare `!= N`.
///
/// `duration` JOINS the list, from a MEASUREMENT rather than an assumption: it is
/// variable-width so the width table admits any length, its empty key fails to
/// decode (`failed to parse duration months … Eof`), and
/// `EmptyValueType::for_cql_type(&CqlType::Duration)` is `None` — so the empty
/// buffer stays a refusal. It is here to pin the direction of the gate move: a
/// gate keyed on "the buffer is empty" ALONE would have turned this `Err` into an
/// opaque blob, i.e. accepted bytes nothing admits.
#[test]
fn a_cassandra_invalid_empty_key_is_refused_like_any_other_corruption() {
    for ty in ["tinyint", "smallint", "date", "time", "duration"] {
        let map_type = format!("map<{ty},int>");
        match decode(&map_type, b"", &7i32.to_be_bytes()) {
            Err(_) => {}
            Ok(v) => panic!(
                "an empty {ty} key is corruption on Cassandra's own terms and must be \
                 refused, not decoded; got {v:?}"
            ),
        }
    }
}

/// The family the gate move exists for: an empty `decimal` key is LEGAL DATA and
/// must reach the read as `Empty(Decimal)`.
///
/// RED-VERIFIED against the previous guard (`allowed.contains(&0)`): this returned
/// `Err(Data corruption: Frozen element 'm': decimal too short (0 bytes))`, because
/// `cql_short_allowed_widths("decimal")` is the EMPTY slice — `decimal` is
/// VARIABLE-width (`{0} ∪ [4, ∞)`, which a width table cannot express), so the
/// width table could not admit the empty buffer and the arm was never entered.
/// The ORACLE is `DecimalSerializer` at `cassandra-5.0.8` (see the note on the
/// refusal test above) plus the Cassandra-WRITTEN fixture's `m_dec` column, never
/// CQLite's own prior output — which was the refusal.
#[test]
fn an_empty_decimal_key_is_typed_because_cassandra_admits_it() {
    let decoded = decode("map<decimal,int>", b"", &7i32.to_be_bytes()).expect(
        "Cassandra's DecimalSerializer accepts the empty buffer: 'Expected 0 or at least 4 bytes'",
    );
    assert_eq!(
        decoded,
        Value::Map(vec![(
            Value::Empty(EmptyValueType::Decimal),
            Value::Integer(7)
        )]),
        "an empty decimal key is legal data and carries the Decimal tag"
    );
}

/// THE BOUND ON THE GATE MOVE. Every OTHER type whose allowed-width slice is
/// EMPTY — so which the widened guard can now reach — must keep EXACTLY the
/// outcome it had before, and in particular must never become a sentinel or an
/// opaque blob.
///
/// This is the assertion that makes "the tag table is the gate" safe rather than
/// permissive, and it is the regression the naive widening
/// (`allowed.contains(&0) || allowed.is_empty()`) would have caused: every
/// composite ALSO has an empty allowed-width slice.
/// `EmptyValueType::for_cql_type` is `None` for `duration`, for every composite
/// (`list`/`set`/`map`/`tuple`/UDT/`frozen<collection>`) and for an unmodelled
/// custom type (MEASURED, all of them), so each falls past the sentinel branch.
///
/// # The families split into TWO groups BY MEASUREMENT, and the split corrects an
/// # expectation worth recording
/// It is natural to expect every composite to `Err` on an empty buffer. FOUR of
/// them do — but `tuple` and UDT (in BOTH spellings) DECODE it, and always did:
///
///   * **REFUSED, and the error must be the DECODER'S OWN, byte-identical to
///     before the gate move** — `duration`, `list<int>`, `set<int>`,
///     `map<int,int>`, `frozen<list<int>>`. Asserted on the MESSAGE, not merely
///     on `is_err()`: an `Err` alone cannot distinguish "the decoder refused it,
///     unchanged" from "some new guard refused it for a different reason", and
///     the whole claim here is that this path is untouched.
///   * **DECODED, and NOT as a sentinel — the `Ok` arm, so the guard is never
///     even reached**: `tuple<int,int>` -> `Tuple([Null, Null])` and
///     `TupleType(Int32Type)` -> `Tuple([Null])`, legal per `TupleType.split` at
///     `cassandra-5.0.8`, where an encoding whose trailing components are simply
///     omitted leaves `position == length`; a REGISTERED UDT (bare name and
///     `UserType(…)` marshal alike) -> a `Udt` whose every field is `None`, by
///     the same rule one layer down; and an UNREGISTERED UDT name -> the
///     pre-existing opaque `Blob(b"")`.
#[test]
fn no_other_empty_width_family_becomes_a_sentinel() {
    // (declared key type, a substring of the decoder's OWN pre-existing error)
    let refused: &[(&str, &str)] = &[
        ("duration", "failed to parse duration months"),
        ("list<int>", "not enough bytes for element count"),
        ("set<int>", "not enough bytes for element count"),
        ("map<int,int>", "not enough bytes for element count"),
        ("frozen<list<int>>", "not enough bytes for element count"),
    ];
    for (ty, needle) in refused {
        let map_type = format!("map<{ty},int>");
        match decode(&map_type, b"", &7i32.to_be_bytes()) {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(needle),
                    "{ty}: the error must still be the DECODER's own ({needle:?}), so the \
                     gate move demonstrably did not reroute this path; got: {msg}"
                );
            }
            Ok(v) => panic!(
                "an empty {ty} key is admitted by NO authority — neither the width table \
                 nor the tag table — so it must stay refused, never a sentinel and never \
                 an opaque blob; got {v:?}"
            ),
        }
    }

    // The `Ok` arm: unchanged by construction, because the guard only sees an
    // `Err`. Pinned anyway, because "the guard cannot see them" is a claim about
    // control flow and this is the measurement of it.
    const UDT_MARSHAL: &str = "org.apache.cassandra.db.marshal.UserType(ks,\
616464726573735f74797065,6669656c64:org.apache.cassandra.db.marshal.Int32Type)";
    let decoded_not_sentinel: &[&str] = &[
        "tuple<int,int>",
        "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type)",
        // The marshal `UserType(…)` spelling needs no registry, so it is the UDT
        // case reachable here — and it is the NO-SCHEMA (`Statistics.db`) route,
        // i.e. the one a real read takes when there is no CQL schema.
        UDT_MARSHAL,
        "unregistered_udt_name",
    ];
    for ty in decoded_not_sentinel {
        let map_type = format!("map<{ty},int>");
        let decoded = decode(&map_type, b"", &7i32.to_be_bytes())
            .unwrap_or_else(|e| panic!("an empty {ty} key decoded before the gate move: {e}"));
        let entries = match decoded {
            Value::Map(entries) => entries,
            other => panic!("{ty}: expected a Map, got {other:?}"),
        };
        assert_eq!(entries.len(), 1, "{ty}: one entry");
        assert!(
            !matches!(entries[0].0, Value::Empty(_)),
            "{ty}: takes the Ok arm, so it must NOT be a sentinel: {:?}",
            entries[0].0
        );
    }
    assert_eq!(
        decode("map<tuple<int,int>,int>", b"", &7i32.to_be_bytes())
            .expect("tuple decodes as before"),
        Value::Map(vec![(
            Value::Tuple(vec![Value::Null, Value::Null]),
            Value::Integer(7)
        )]),
        "tuple<int,int>: the exact pre-existing value, per TupleType.split"
    );
    let udt = decode(&format!("map<{UDT_MARSHAL},int>"), b"", &7i32.to_be_bytes())
        .expect("a marshal UserType decodes an empty cell path as before");
    match udt {
        Value::Map(entries) => match &entries[0].0 {
            Value::Udt(u) => assert!(
                u.fields.iter().all(|f| f.value.is_none()),
                "a UDT key from an empty cell path has every field omitted, per the same \
                 TupleType.split rule one layer down: {u:?}"
            ),
            other => panic!("expected a Udt key, got {other:?}"),
        },
        other => panic!("expected a Map, got {other:?}"),
    }
    assert_eq!(
        decode("map<unregistered_udt_name,int>", b"", &7i32.to_be_bytes())
            .expect("an unresolvable name decodes opaquely as before"),
        Value::Map(vec![(Value::blob(Vec::new()), Value::Integer(7))]),
        "an unregistered UDT name keeps the pre-existing OPAQUE blob — that policy is \
         for a type this reader cannot model and is untouched by the gate move"
    );
}

/// `varint` and `inet` empty keys are the TYPED SENTINEL — issue **#4079 is
/// CLOSED by this test's flip** (roborev job 449, finding C).
///
/// # What this replaces, and why the previous pin was a signpost and not a claim
/// This test used to be
/// `varint_and_inet_keep_their_native_empty_spelling_declared_gap`, pinning
/// `Varint(b"")` / `Inet(b"")` as a DECLARED GAP. The mechanism it recorded is
/// still exactly right and is worth keeping: both families DECODE an empty
/// buffer successfully (`IntegerSerializer.java:31-34` — validate's whole body
/// is the comment `// no invalid integers.`; `InetAddressSerializer.java:52-55`
/// — `if (accessor.isEmpty(value)) return;`), so they took the decoder's `Ok`
/// arm and an admission gate consulted ONLY on a decode FAILURE could never
/// reach them. The gate is now consulted for EVERY empty cell path, before the
/// decode, so it does.
///
/// # Why it became a defect rather than an inconsistency
/// `EmptyValueType::for_cql_type` returns `Some(Varint)`/`Some(Inet)`, so those
/// families already had a canonical empty spelling and the native one was a
/// SECOND spelling of one value. What decided it is a PUBLIC-SURFACE fact:
/// both bindings REJECT a zero-length `Inet` outright —
/// `cqlite_ffi_common::inet::inet_kind` maps any length other than 4 or 16 to
/// `InetError` ("Invalid inet address length: 0 (expected 4 or 16)"), which
/// Python raises as `ParseError` (`bindings/python/src/value.rs::inet_to_py`)
/// and Node as a thrown error (`bindings/node/src/value.rs::inet_to_string_js`),
/// with no passthrough branch by #28 mandate. So on data Cassandra accepts and
/// writes, a `SELECT` through either binding FAILED. `Value::Empty(_)` renders
/// as `""` on both (`value.rs:52` / `value.rs:217`), which is what `sstabledump`
/// and `SELECT JSON` emit.
#[test]
fn varint_and_inet_empty_keys_are_the_typed_sentinel_closing_4079() {
    let cases: &[(&str, EmptyValueType)] = &[
        ("varint", EmptyValueType::Varint),
        ("inet", EmptyValueType::Inet),
    ];
    for (ty, tag) in cases {
        let map_type = format!("map<{ty},int>");
        let decoded = decode(&map_type, b"", &7i32.to_be_bytes())
            .unwrap_or_else(|e| panic!("an empty {ty} key must decode: {e}"));
        assert_eq!(
            decoded,
            Value::Map(vec![(Value::Empty(*tag), Value::Integer(7))]),
            "{ty}: the empty key must be the TYPED sentinel, not a second native \
             spelling of the same empty buffer (#4079)"
        );
    }
    // The NON-empty sibling is untouched: normalization is a property of the
    // EMPTY cell path, never of the family.
    assert_eq!(
        decode("map<inet,int>", &[10, 0, 0, 1], &1i32.to_be_bytes())
            .expect("a 4-byte inet key still decodes natively"),
        Value::Map(vec![(Value::inet(vec![10, 0, 0, 1]), Value::Integer(1))]),
        "a NON-empty inet key keeps its native spelling"
    );
    assert_eq!(
        decode("map<varint,int>", &[0x2a], &1i32.to_be_bytes())
            .expect("a 1-byte varint key still decodes natively"),
        Value::Map(vec![(Value::varint(vec![0x2a]), Value::Integer(1))]),
        "a NON-empty varint key keeps its native spelling"
    );
}

/// THE BOUND on finding C's normalization, in the direction that matters most:
/// `text`/`ascii`/`varchar`/`blob` MUST keep their NATIVE empty spelling.
///
/// An empty buffer is a legal, MEANINGFUL value for those families — Cassandra
/// OVERRIDES `isNull` precisely to say so (`serializers/BytesSerializer.java:57-62`,
/// *"is not \"null\" for bytes types, it is byte[0]"*, and
/// `serializers/AbstractTextSerializer.java:72-77`) — so a sentinel there would
/// be the very "two spellings of one value" defect #4079 is about, inverted.
///
/// The gate that excludes them is `EmptyValueType::for_cql_type` returning
/// `None`, and this test ASSERTS that rather than trusting it: the decoder's
/// behaviour and the table it delegates to are pinned in the same place, so a
/// widening of the table cannot silently widen the decoder.
#[test]
fn text_and_blob_empty_keys_keep_their_native_spelling_and_the_table_says_so() {
    use crate::schema::CqlType;
    let cases: &[(&str, CqlType, Value)] = &[
        ("text", CqlType::Text, Value::text("")),
        ("ascii", CqlType::Ascii, Value::text("")),
        ("varchar", CqlType::Varchar, Value::text("")),
        ("blob", CqlType::Blob, Value::blob(Vec::new())),
    ];
    for (ty, cql, want) in cases {
        assert_eq!(
            EmptyValueType::for_cql_type(cql),
            None,
            "{ty}: the ADMISSION TABLE must not admit a text/blob family — an empty \
             buffer is a MEANINGFUL value there, never a sentinel"
        );
        let map_type = format!("map<{ty},int>");
        assert_eq!(
            decode(&map_type, b"", &7i32.to_be_bytes())
                .unwrap_or_else(|e| panic!("an empty {ty} key must decode: {e}")),
            Value::Map(vec![(want.clone(), Value::Integer(7))]),
            "{ty}: the empty key stays NATIVE, never Value::Empty"
        );
    }
}
