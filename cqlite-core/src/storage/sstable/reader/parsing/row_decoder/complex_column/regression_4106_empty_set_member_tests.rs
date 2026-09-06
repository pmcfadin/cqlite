//! Issue #4106 — synthetic-bytes coverage for the EMPTY multicell SET MEMBER.
//!
//! # The defect
//! The set branch guarded its cell-path decode on `!path_bytes.is_empty()`, so a
//! member whose serialized form is the EMPTY BUFFER produced `set_member == None`
//! and was dropped from the reconstructed `Value::Set` entirely — a `SELECT`
//! returned a set SHORT ONE MEMBER, silently, with no error and no log line.
//!
//! # This is #3747 one collection over, and the design ruling is INHERITED
//! A non-frozen SET stores its element in the cell path *exactly* as a map stores
//! its key there, and Cassandra says so in ONE place for BOTH: `validateCellPath`
//! (`schema/ColumnMetadata.java:457-467` at `cassandra-5.0.8`) validates
//! `((CollectionType)type).nameComparator().validate(path.get(0))`, and
//! `nameComparator()` is the KEYS type for a `MapType` and the ELEMENTS type for a
//! `SetType` (`db/marshal/SetType.java:101-104`). The framing is literally the
//! same object: one `CollectionType.cellPathSerializer`
//! (`db/marshal/CollectionType.java:55`, `:361-382`) writes every collection's
//! cell path as `ByteBufferUtil.writeWithVIntLength(path.get(0))`, so a
//! zero-length path is EXPRESSIBLE and means an EMPTY component. And a set's
//! member really is written there with an empty cell VALUE —
//! `cql3/Sets.java:407`: `params.addCell(column, CellPath.create(bb),
//! ByteBufferUtil.EMPTY_BYTE_BUFFER)`.
//!
//! So the legal/corrupt question for an empty set member is the SAME `validate()`
//! question `EmptyValueType` already answers, and this file therefore inherits
//! `regression_3747_empty_map_key_tests`'s ruling verbatim:
//!
//! **DO NOT restate the per-type width/admission table here.** The admission is
//! [`crate::types::EmptyValueType::for_cql_type`], derived from Cassandra's
//! serializers; a second opinion written at this level can drift from it, and an
//! earlier revision of #3747 that justified per-type verdicts against CQLite's
//! own decoders was circular reasoning that produced WRONG ANSWERS. What the
//! tests below pin is the GUARD's removal and the DELEGATION.
//!
//! # What is deliberately NOT widened (scope, stated so it is not read as a gap)
//! The map cell path additionally validates FIXED WIDTHS and full consumption
//! (`cell_path_key_allowed_widths`, issue #3612). The set path does not, and
//! #3612's own module header records that "widening it to the frozen/set routes
//! is out of scope". This issue does not widen it either: a NON-EMPTY set member
//! keeps the exact decode it had (`parse_value_from_raw_bytes`), and only the
//! EMPTY buffer — which previously reached NO authority at all, because the guard
//! shielded it — is newly routed through the shared admission gate. Pinned by
//! [`a_non_empty_member_is_decoded_exactly_as_before`].

use super::V5CompressedLegacyParser;
use crate::parser::vint::encode_vuint;
use crate::schema::Column;
use crate::types::{EmptyValueType, Value};

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("ks".to_string(), "t".to_string(), 0, 0, None)
}

fn column(cql_type: &str) -> Column {
    Column {
        name: "s".to_string(),
        data_type: cql_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// One multicell SET column holding `paths.len()` cells: `[vuint cell_count]`
/// then, per cell, `[flags][vuint path_len][path]` with NO value bytes.
///
/// `flags = 0x0C` is `HAS_EMPTY_VALUE | USE_ROW_TIMESTAMP` — the exact pair a
/// live set element carries on disk (`cql3/Sets.java:407` writes the empty
/// buffer as the cell value; `USE_ROW_TIMESTAMP` lets the fixture omit the
/// per-cell timestamp delta).
fn set_column_bytes(paths: &[&[u8]]) -> Vec<u8> {
    let mut out = encode_vuint(paths.len() as u64);
    for path in paths {
        out.push(0x0C);
        out.extend_from_slice(&encode_vuint(path.len() as u64));
        out.extend_from_slice(path);
    }
    out
}

fn decode(set_type: &str, paths: &[&[u8]]) -> crate::Result<Value> {
    let p = parser();
    let col = column(set_type);
    let bytes = set_column_bytes(paths);
    p.parse_complex_column_inner(&bytes, 0, &col, set_type, false, 1_000, None, None)
        .map(|(v, _, _)| v)
}

fn members(decoded: Value) -> Vec<Value> {
    match decoded {
        Value::Set(members) => members,
        other => panic!("expected a Set, got {other:?}"),
    }
}

/// THE FIX, in the shape the defect actually bites: a set with an empty member
/// AND a non-empty one must come back with BOTH.
///
/// A single-member fixture would have shown `Set([])` versus `Set([Empty(Int)])`,
/// which is a visible difference; the two-member case is the one that was
/// SILENT — `Set([Integer(7)])` looks like a perfectly ordinary set and nothing
/// in it says a member is missing. Cassandra sorts the empty member FIRST
/// (`db/marshal/Int32Type.java` compares `Boolean.compare(right.isEmpty,
/// left.isEmpty)` when either side is empty), so that is the on-disk order the
/// fixture uses and the order the decode must preserve.
#[test]
fn an_empty_member_beside_a_non_empty_one_is_not_dropped() {
    let decoded = decode("set<int>", &[b"", &7i32.to_be_bytes()])
        .expect("an empty set member is legal data for `int`, not corruption");
    assert_eq!(
        members(decoded),
        vec![Value::Empty(EmptyValueType::Int), Value::Integer(7)],
        "BOTH members must reach the reconstructed set, in on-disk order — the old \
         `!path_bytes.is_empty()` guard returned a one-member set and left no trace"
    );
}

/// DELEGATION, the `empty is LEGAL` direction. Every family the shared admission
/// table names must survive as the TYPED sentinel.
///
/// The expectation is DERIVED from [`EmptyValueType::for_cql_type`] rather than
/// restated: the per-family verdict is that table's to make (see the module
/// header), and this test's job is to prove the SET path asks it.
#[test]
fn every_admitted_family_survives_as_the_typed_sentinel() {
    use crate::schema::CqlType;
    // CQL forbids `counter` inside a collection (`cql3/CQL3Type.java:827-828`),
    // so it is not declarable here; every other admitted family is.
    let cases: &[(&str, CqlType)] = &[
        ("int", CqlType::Int),
        ("bigint", CqlType::BigInt),
        ("float", CqlType::Float),
        ("double", CqlType::Double),
        ("timestamp", CqlType::Timestamp),
        ("uuid", CqlType::Uuid),
        ("timeuuid", CqlType::TimeUuid),
        ("boolean", CqlType::Boolean),
        ("inet", CqlType::Inet),
        ("decimal", CqlType::Decimal),
        ("varint", CqlType::Varint),
    ];
    for (ty, cql) in cases {
        let tag = EmptyValueType::for_cql_type(cql).unwrap_or_else(|| {
            panic!("{ty} is expected to be an ADMITTED family; the shared table disagrees")
        });
        let decoded = decode(&format!("set<{ty}>"), &[b""])
            .unwrap_or_else(|e| panic!("Cassandra admits an empty {ty}; it must survive: {e}"));
        assert_eq!(
            members(decoded),
            vec![Value::Empty(tag)],
            "{ty}: the empty member must be PRESERVED as the TYPED sentinel"
        );
    }
}

/// The MARSHAL spelling reaches the same tag — the NO-SCHEMA route.
///
/// A set element type sourced from `Statistics.db` arrives in marshal form and is
/// normalized by a different branch of the type classifier, so a fix that only
/// handled the CQL short form would leave every schema-less read dropping the
/// member.
#[test]
fn the_marshal_spelling_of_an_empty_member_reaches_the_same_tag() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    let cases: &[(&str, EmptyValueType)] = &[
        ("Int32Type", EmptyValueType::Int),
        ("LongType", EmptyValueType::BigInt),
        ("UUIDType", EmptyValueType::Uuid),
        ("BooleanType", EmptyValueType::Boolean),
        ("DecimalType", EmptyValueType::Decimal),
    ];
    for (marshal, tag) in cases {
        let set_type = format!("{P}SetType({P}{marshal})");
        let decoded = decode(&set_type, &[b""])
            .unwrap_or_else(|e| panic!("an empty {marshal} member must survive: {e}"));
        assert_eq!(
            members(decoded),
            vec![Value::Empty(*tag)],
            "marshal {marshal} must reach the same sentinel tag as its CQL short form"
        );
    }
}

/// The families for which an empty buffer is a legal, MEANINGFUL value keep
/// their NATIVE spelling — never a sentinel.
///
/// Cassandra OVERRIDES `isNull` precisely to say so
/// (`serializers/BytesSerializer.java:57-62`,
/// `serializers/AbstractTextSerializer.java:72-77`), and the gate that excludes
/// them is `for_cql_type` returning `None`. Both halves are asserted, so a
/// widening of the table cannot silently widen the decoder.
#[test]
fn text_and_blob_empty_members_keep_their_native_spelling() {
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
        let decoded = decode(&format!("set<{ty}>"), &[b""])
            .unwrap_or_else(|e| panic!("an empty {ty} member must decode: {e}"));
        assert_eq!(
            members(decoded),
            vec![want.clone()],
            "{ty}: the empty member stays NATIVE, never Value::Empty"
        );
    }
}

/// CASSANDRA-INVALID — REFUSED, so the gate is an ADMISSION and not a blanket
/// "the buffer is empty, therefore fine".
///
/// Two DIFFERENT refusal routes, asserted on the MESSAGE so the test measures
/// WHICH one each family takes rather than merely that something failed:
///
///  * `tinyint`/`smallint`/`date`/`time` are spelled with a bare `!= N` validate
///    (`serializers/ByteSerializer.java:40-44` and siblings), so an empty cell
///    path is corruption ON CASSANDRA'S OWN TERMS and `validateCellPath`
///    (`schema/ColumnMetadata.java:457-467`) throws on it. Their SHARED value
///    decoder nonetheless answers `Value::Null` — correctly, because its oracle
///    is `deserialize()`, which maps empty to null for all twelve fixed-width
///    scalars (#3847) — and a cell-path component can never be null, so
///    `decode_set_cell_path_member` refuses. This is the SAME outcome the map
///    route already has for the same declared type (there the width table
///    refuses it first), so the two spellings of one empty component agree.
///  * `duration` never reaches that branch: it is variable-width, `for_cql_type`
///    names no tag for it, and its DECODE of the empty buffer FAILS — so the
///    error is the decoder's OWN, byte-unchanged. It is here to pin the
///    direction of the gate: a gate keyed on "the buffer is empty" ALONE would
///    have turned this `Err` into an accepted value. (CQL forbids `duration` in
///    a set at all, `cql3/CQL3Type.java:830-831`.)
///
/// Errors PROPAGATE. Mapping them to a dropped member is what #3811 (roborev F1)
/// removed for the non-empty case, and re-introducing it for the empty one would
/// make an empty malformed member behave differently from a non-empty one.
#[test]
fn a_cassandra_invalid_empty_member_is_refused_like_any_other_corruption() {
    // (declared element type, a substring identifying WHICH refusal fired)
    let refused: &[(&str, &str)] = &[
        ("tinyint", "issue #4106"),
        ("smallint", "issue #4106"),
        ("date", "issue #4106"),
        ("time", "issue #4106"),
        ("duration", "failed to parse duration months"),
    ];
    for (ty, needle) in refused {
        match decode(&format!("set<{ty}>"), &[b""]) {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(needle),
                    "{ty}: expected the {needle:?} refusal route; got: {msg}"
                );
            }
            Ok(v) => panic!(
                "an empty {ty} member is corruption on Cassandra's own terms and must be \
                 refused, not decoded and not dropped; got {v:?}"
            ),
        }
    }
}

/// THE INVARIANT, over every element type a set can declare: an empty cell path
/// is EITHER the typed sentinel (exactly when the shared table admits it), OR a
/// meaningful native value, OR a refusal — and NEVER `Value::Null`, never
/// silently absent.
///
/// This is the census the other tests are instances of, and it is what makes
/// "the tag table is the gate" safe rather than permissive. Two failure modes it
/// exists to catch, both of which a per-family test can miss:
///
///  * a member that VANISHES (`Set([])`) — the #4106 defect itself, and the one
///    that leaves no trace;
///  * a member decoded as `Value::Null` — which no CQL set can hold and which
///    Cassandra cannot write (`cql3/Sets.java:407` puts the element IN the path).
///    This is the state the naive fix produced: removing the guard alone made
///    `set<tinyint>` decode to `Set([Null])`, MEASURED in this lane before the
///    `Null`-refusal branch was added.
///
/// The per-family expectation is DERIVED from [`EmptyValueType::for_cql_type`],
/// never restated (see the module header).
#[test]
fn an_empty_member_is_never_null_and_never_silently_absent() {
    use crate::schema::CqlType;
    const CANDIDATES: &[&str] = &[
        "int",
        "bigint",
        "float",
        "double",
        "timestamp",
        "uuid",
        "timeuuid",
        "boolean",
        "inet",
        "decimal",
        "varint",
        "tinyint",
        "smallint",
        "date",
        "time",
        "text",
        "ascii",
        "varchar",
        "blob",
        "duration",
        "frozen<list<int>>",
        "frozen<set<int>>",
        "frozen<map<int,int>>",
        "frozen<tuple<int,int>>",
        "unregistered_udt_name",
    ];
    let mut admitted = 0usize;
    let mut refused = 0usize;
    let mut native = 0usize;
    for ty in CANDIDATES {
        let expected_tag = CqlType::parse(ty)
            .ok()
            .as_ref()
            .and_then(EmptyValueType::for_cql_type);
        match decode(&format!("set<{ty}>"), &[b""]) {
            Ok(v) => {
                let got = members(v);
                assert_eq!(
                    got.len(),
                    1,
                    "{ty}: the member must be PRESENT — a set one member short is the \
                     #4106 defect and it leaves no other trace"
                );
                assert!(
                    !matches!(got[0], Value::Null),
                    "{ty}: a set member can never be Value::Null — CQL forbids it and \
                     Cassandra cannot write it"
                );
                match expected_tag {
                    Some(tag) => {
                        assert_eq!(
                            got[0],
                            Value::Empty(tag),
                            "{ty}: the shared table admits this family, so the member must \
                             be the TYPED sentinel"
                        );
                        admitted += 1;
                    }
                    None => native += 1,
                }
            }
            Err(_) => {
                assert_eq!(
                    expected_tag, None,
                    "{ty}: the shared table admits this family, so the member must NOT be \
                     refused"
                );
                refused += 1;
            }
        }
    }
    // AFFIRMATIVE COUNTS: a census whose three buckets are not all populated is
    // measuring less than it claims to.
    println!(
        "#4106 empty-set-member census over {} candidate element types: \
         {admitted} TYPED SENTINEL, {native} NATIVE value, {refused} REFUSED",
        CANDIDATES.len()
    );
    assert_eq!(
        admitted + native + refused,
        CANDIDATES.len(),
        "every candidate must land in exactly one bucket"
    );
    assert!(
        admitted >= 11 && native >= 5 && refused >= 5,
        "each bucket must be populated, or this census proves less than it claims: \
         {admitted} sentinel / {native} native / {refused} refused"
    );
}

/// THE BOUND: a composite element type whose empty buffer NO authority admits
/// keeps exactly the outcome it had before.
///
/// `for_cql_type` is `None` for every composite, so each falls past the gate to
/// the same decode it always reached. This is the regression a gate keyed on "the
/// buffer is empty" alone would have caused — accepting bytes nothing admits.
#[test]
fn no_composite_element_type_becomes_a_sentinel() {
    // (declared element type, a substring of the decoder's OWN pre-existing error)
    let refused: &[(&str, &str)] = &[
        ("frozen<list<int>>", "not enough bytes for element count"),
        ("frozen<set<int>>", "not enough bytes for element count"),
        ("frozen<map<int,int>>", "not enough bytes for element count"),
    ];
    for (ty, needle) in refused {
        match decode(&format!("set<{ty}>"), &[b""]) {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(needle),
                    "{ty}: the error must still be the DECODER's own ({needle:?}), so the \
                     gate demonstrably did not reroute this path; got: {msg}"
                );
            }
            Ok(v) => panic!(
                "an empty {ty} member is admitted by no authority, so it must stay \
                 refused — never a sentinel; got {v:?}"
            ),
        }
    }
    // A `tuple` element DECODES an empty buffer (per `TupleType.split`, where an
    // encoding whose trailing components are omitted leaves `position == length`)
    // and must NOT be rewritten into a sentinel.
    let tuple = members(
        decode("set<frozen<tuple<int,int>>>", &[b""]).expect("a tuple element decodes as before"),
    );
    assert_eq!(tuple.len(), 1, "one member");
    assert!(
        !matches!(tuple[0], Value::Empty(_)),
        "a tuple element takes the decoder's Ok arm, so it must not be a sentinel: {:?}",
        tuple[0]
    );
}

/// A NON-EMPTY member is decoded EXACTLY as before — the scope bound from the
/// module header, asserted rather than asserted-in-prose.
///
/// In particular the set path still does NOT width-check its cell path (#3612's
/// declared scope), so a 4-byte `int` member decodes and nothing about this
/// change touches it.
#[test]
fn a_non_empty_member_is_decoded_exactly_as_before() {
    assert_eq!(
        members(decode("set<int>", &[&7i32.to_be_bytes()]).expect("an ordinary member decodes")),
        vec![Value::Integer(7)]
    );
    assert_eq!(
        members(decode("set<text>", &[b"k"]).expect("an ordinary text member decodes")),
        vec![Value::text("k")]
    );
    assert_eq!(
        members(decode("set<inet>", &[&[10u8, 0, 0, 1]]).expect("a 4-byte inet member decodes")),
        vec![Value::inet(vec![10, 0, 0, 1])],
        "a NON-empty inet member keeps its native spelling: normalization is a property \
         of the EMPTY cell path, never of the family"
    );
}
