//! WRITE-SIDE admission of the empty-buffer sentinel on a multicell SET's CELL
//! PATH (issue #4106) — the sibling of `empty_sentinel_write` one collection
//! over.
//!
//! # The defect
//! `write_set_complex_cells` routed EVERY element through the type-blind
//! `serialize_value_into`, whose `Value::Empty` refusal is CORRECT and
//! deliberate: it sees no declared type, so it cannot tell a legal empty `text`
//! from the corruption Cassandra's `validate` throws on for `tinyint`. The map
//! case had a schema-aware entry point; sets had none. So a set CQLite had
//! legitimately DECODED from Cassandra-written bytes could not be written back —
//! a compaction of that SSTable failed outright.
//!
//! # THE ORACLE IS CASSANDRA SOURCE, NOT A ROUND TRIP (CLAUDE.md, #3042)
//! A CQLite-written + CQLite-read round trip is INVARIANT to a uniform framing
//! error — both sides make the identical mistake and the test stays green — so
//! the expectation below is BYTES derived from `cassandra-5.0.8`:
//!
//!  * ONE `CollectionType.cellPathSerializer` serializes every collection's cell
//!    path (`db/marshal/CollectionType.java:55`, `:361-382`) and its whole body
//!    is `ByteBufferUtil.writeWithVIntLength(path.get(0))`, which is
//!    `out.writeUnsignedVInt32(bytes.remaining()); out.write(bytes);`
//!    (`utils/ByteBufferUtil.java:356-360`). For the EMPTY buffer
//!    `remaining() == 0`, so the whole cell path on disk is the unsigned VInt
//!    `0` — the single byte `0x00` — and NOTHING after it. That is the byte
//!    [`the_empty_set_element_is_a_zero_length_cell_path`] asserts.
//!  * a set element is written INTO the cell path with an EMPTY cell VALUE —
//!    `cql3/Sets.java:407`: `params.addCell(column, CellPath.create(bb),
//!    ByteBufferUtil.EMPTY_BYTE_BUFFER)`. Hence the `HAS_EMPTY_VALUE` flag and
//!    no value bytes.
//!  * the empty element sorts FIRST. `db/marshal/Int32Type.java:61-71`:
//!    `if (accessorL.isEmpty(left) || accessorR.isEmpty(right)) return
//!    Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));` — so
//!    an empty component precedes every non-empty one.
//!  * whether an empty component is LEGAL for the declared element type is
//!    `validateCellPath` (`schema/ColumnMetadata.java:457-467`), which validates
//!    `((CollectionType)type).nameComparator().validate(path.get(0))`, and
//!    `SetType.nameComparator()` is the ELEMENTS type
//!    (`db/marshal/SetType.java:101-104`). That is the same question the map key
//!    asks, so the admission is the SAME shared
//!    [`crate::types::EmptyValueType::check_admits`] and is not restated here.

use super::super::*;
use super::support::*;
use crate::types::{EmptyValueType, Value};

fn set_of(element_type: &str) -> Column {
    Column {
        name: "tags".to_string(),
        data_type: element_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

// ───────────────────────────────────────────────────────────────────────────
// THE BYTES
// ───────────────────────────────────────────────────────────────────────────

/// THE FIX, asserted on the EMITTED BYTES: a `set<int>` holding the empty
/// element and `7` writes TWO cells, the empty one FIRST, and its cell path is
/// the single byte `0x00` with nothing after it.
///
/// Every byte of the expectation is derived in the module header from
/// `cassandra-5.0.8`; none of it is read back through CQLite's own reader, which
/// is what makes this an oracle rather than a round trip (#3042).
#[test]
fn the_empty_set_element_is_a_zero_length_cell_path() {
    let writer = DataWriter::new(create_test_stats());
    let value = Value::Set(vec![Value::Integer(7), Value::Empty(EmptyValueType::Int)]);
    let mut buf = Vec::new();
    writer
        .write_complex_column(
            &mut buf,
            &set_of("set<int>"),
            &value,
            1_700_000_000_000_000,
            None,
            TEST_NOW_SECONDS,
        )
        .expect("a set<int> carrying an empty element must be writable (issue #4106)");

    // flags = HAS_EMPTY_VALUE | USE_ROW_TIMESTAMP: a live set element carries an
    // empty cell VALUE (`cql3/Sets.java:407`) and this write has no per-cell TTL,
    // so no timestamp/LDT/TTL delta follows the flags byte.
    const FLAGS: u8 = CELL_HAS_EMPTY_VALUE | CELL_USE_ROW_TIMESTAMP;
    let expected_cells: Vec<u8> = [
        vec![0x02u8],      // cell count = unsigned VInt 2
        vec![FLAGS, 0x00], // the EMPTY element: writeWithVIntLength(EMPTY) == 0x00
        vec![FLAGS, 0x04], // the `7` element: writeWithVIntLength(4 bytes)
        7i32.to_be_bytes().to_vec(),
    ]
    .concat();
    assert!(
        buf.ends_with(&expected_cells),
        "the cell section must be byte-exactly {expected_cells:02x?} (cell count, then the \
         EMPTY element FIRST with a zero-length path, then 7); got {buf:02x?}"
    );

    // And the framing prefix is the ordinary complex-deletion header, so the
    // whole column is well-formed rather than merely ending correctly.
    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 2, "two cells");
    assert!(
        cells[0].cell_path.is_empty() && cells[0].value.is_none(),
        "cell 0 is the EMPTY element: an empty path and no value: {:?}",
        cells[0]
    );
    assert_eq!(
        cells[1].cell_path,
        7i32.to_be_bytes().to_vec(),
        "cell 1 is `7`, unchanged"
    );
}

/// The single-element case, so the zero-length path is asserted with no sibling
/// bytes adjacent to it — a `0x00` followed by another cell's flags could in
/// principle be mis-framed, and this removes that possibility.
#[test]
fn a_lone_empty_element_writes_exactly_one_zero_length_path() {
    let writer = DataWriter::new(create_test_stats());
    let mut buf = Vec::new();
    writer
        .write_complex_column(
            &mut buf,
            &set_of("set<int>"),
            &Value::Set(vec![Value::Empty(EmptyValueType::Int)]),
            1_700_000_000_000_000,
            None,
            TEST_NOW_SECONDS,
        )
        .expect("a one-element set holding only the empty element must be writable");
    assert!(
        buf.ends_with(&[0x01, CELL_HAS_EMPTY_VALUE | CELL_USE_ROW_TIMESTAMP, 0x00]),
        "cell count 1, then flags, then a zero-length cell path and NOTHING else; got {buf:02x?}"
    );
}

/// Every family the shared tag table admits and CQL permits as a set element is
/// writable, and each writes ZERO cell-path bytes.
///
/// The set is DERIVED from [`EmptyValueType::for_cql_type`] rather than restated:
/// a second hand-written list is a second opinion able to drift from the table.
/// `counter` is absent because CQL forbids a `counter` collection element
/// (`cql3/CQL3Type.java:827-828`), so `set<counter>` is not declarable.
#[test]
fn every_admitted_element_family_writes_a_zero_length_path() {
    let element_types = [
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
    ];
    let writer = DataWriter::new(create_test_stats());
    for element_type in element_types {
        let cql = crate::schema::CqlType::parse(element_type)
            .unwrap_or_else(|e| panic!("{element_type} must parse: {e}"));
        let tag = EmptyValueType::for_cql_type(&cql)
            .unwrap_or_else(|| panic!("{element_type} is expected to be an ADMITTED family"));
        let mut buf = Vec::new();
        writer
            .write_complex_column(
                &mut buf,
                &set_of(&format!("set<{element_type}>")),
                &Value::Set(vec![Value::Empty(tag)]),
                1_700_000_000_000_000,
                None,
                TEST_NOW_SECONDS,
            )
            .unwrap_or_else(|e| panic!("set<{element_type}> must admit Empty({tag:?}): {e}"));
        assert!(
            buf.ends_with(&[0x01, CELL_HAS_EMPTY_VALUE | CELL_USE_ROW_TIMESTAMP, 0x00]),
            "set<{element_type}>: the admitted sentinel's cell path is a zero-length \
             VInt and nothing else; got {buf:02x?}"
        );
    }
}

/// The MARSHAL spelling of the set declaration admits identically — the
/// schema-less route.
///
/// This is roborev job 453's finding transposed: a column whose declared type
/// came from a `SerializationHeader` arrives as
/// `org.apache.cassandra.db.marshal.SetType(Int32Type)`, and
/// `write_complex_column` dispatches a multicell set on exactly that prefix, so
/// a gate understanding only the CQL spelling would refuse a sentinel this crate
/// had legitimately decoded from the very SSTable being rewritten.
#[test]
fn the_marshal_spelled_set_declaration_admits_the_same_sentinel() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    let writer = DataWriter::new(create_test_stats());
    for inner in [format!("{P}Int32Type"), "Int32Type".to_string()] {
        let declared = format!("{P}SetType({inner})");
        let mut buf = Vec::new();
        writer
            .write_complex_column(
                &mut buf,
                &set_of(&declared),
                &Value::Set(vec![Value::Empty(EmptyValueType::Int)]),
                1_700_000_000_000_000,
                None,
                TEST_NOW_SECONDS,
            )
            .unwrap_or_else(|e| panic!("{declared} must admit an Empty(int) element: {e}"));
        assert!(
            buf.ends_with(&[0x01, CELL_HAS_EMPTY_VALUE | CELL_USE_ROW_TIMESTAMP, 0x00]),
            "{declared}: a zero-length cell path; got {buf:02x?}"
        );
    }
}

// ───────────────────────────────────────────────────────────────────────────
// REFUSALS — the admission is a GATE, not a blanket acceptance
// ───────────────────────────────────────────────────────────────────────────

/// A declared ELEMENT type that does not ADMIT an empty buffer is refused: the
/// four strict families, for which an empty component is corruption on
/// Cassandra's own terms (bare `!= N` validate,
/// `serializers/ByteSerializer.java:40-44` and siblings, which
/// `validateCellPath` would throw on), and the text/blob families, for which an
/// empty buffer is a legal MEANINGFUL value that must never be spelled as a
/// sentinel.
///
/// The TAG is deliberately a valid one throughout, so nothing here can pass for
/// the wrong reason.
#[test]
fn the_set_cell_path_refuses_an_element_type_that_admits_no_empty_buffer() {
    for element_type in [
        "tinyint", "smallint", "date", "time", "text", "ascii", "varchar", "blob",
    ] {
        let declared = format!("set<{element_type}>");
        let mut out = Vec::new();
        let err = serialize_set_cell_path_element_into(
            &Value::Empty(EmptyValueType::Int),
            &declared,
            &mut out,
        )
        .expect_err("a non-admitting element type must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("does not admit an empty buffer") && msg.contains(&declared),
            "{declared}: the refusal must name the declared type and say why: {msg}"
        );
        assert!(out.is_empty(), "a refusal writes nothing: {out:?}");
    }
}

/// A tag that DISAGREES with the declared element type is refused, naming both —
/// writing it would put bytes on disk that read back as another type.
#[test]
fn the_set_cell_path_refuses_a_tag_that_disagrees_with_the_declared_element_type() {
    let mut out = Vec::new();
    let err = serialize_set_cell_path_element_into(
        &Value::Empty(EmptyValueType::Int),
        "set<bigint>",
        &mut out,
    )
    .expect_err("an Empty(int) element in a set<bigint> must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("`int`") && msg.contains("`bigint`") && msg.contains("#3805"),
        "the refusal must name BOTH types and the issue: {msg}"
    );
    assert!(out.is_empty(), "a refusal writes nothing: {out:?}");
}

/// A declared type that resolves to no SET type at all is a REFUSAL, not a guess
/// (#28) — there is then no declared element type to validate the tag against.
///
/// `FrozenType(SetType(T))` is here on purpose: a frozen set is ONE inline
/// length-prefixed cell with NO CellPath at all, so its empty element is the
/// inline-element case `require_fixed_width` owns (#3847/#4071), never this
/// position. `com.acme.SetType(...)` is the marshal PACKAGE rule: reading an
/// unknown class as Cassandra's `SetType` because the simple name matches is the
/// heuristic that rule exists to refuse.
#[test]
fn the_set_cell_path_refuses_when_the_declared_element_type_is_unavailable() {
    for declared in [
        "int",
        "list<int>",
        "map<int, int>",
        "",
        "org.apache.cassandra.db.marshal.Int32Type",
        "org.apache.cassandra.db.marshal.ListType(Int32Type)",
        "com.acme.SetType(Int32Type)",
        "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(Int32Type))",
        "org.apache.cassandra.db.marshal.SetType(Int32Type",
    ] {
        let mut out = Vec::new();
        let err = serialize_set_cell_path_element_into(
            &Value::Empty(EmptyValueType::Int),
            declared,
            &mut out,
        )
        .expect_err("an unresolvable declared element type must be refused, never guessed");
        let msg = err.to_string();
        assert!(
            msg.contains("#3805") && msg.contains("#28"),
            "{declared:?}: the refusal must cite the sentinel issue and the \
             no-heuristics mandate: {msg}"
        );
        assert!(
            msg.contains("`set<T>`"),
            "{declared:?}: the refusal must say what WOULD have resolved: {msg}"
        );
        assert!(out.is_empty(), "a refusal writes nothing: {out:?}");
    }
}

// ───────────────────────────────────────────────────────────────────────────
// THE BOUND — nothing about a NON-sentinel element changed
// ───────────────────────────────────────────────────────────────────────────

/// An ordinary element still emits its ordinary bytes, and a `Null` element is
/// still refused with the SAME message, because the new position delegates every
/// non-sentinel value to `serialize_collection_element_into` — the exact function
/// the set writer used before.
///
/// The `Null` half matters: routing through the raw `serialize_value_into`
/// instead would have silently turned a rejected null into ZERO bytes, i.e. a
/// dropped member on the WRITE side — the mirror image of the read defect this
/// issue is about.
#[test]
fn a_non_sentinel_set_element_is_serialized_exactly_as_before() {
    let mut out = Vec::new();
    serialize_set_cell_path_element_into(&Value::Integer(7), "set<int>", &mut out)
        .expect("an ordinary element still serializes");
    assert_eq!(out, 7i32.to_be_bytes().to_vec());

    // The declared type is IRRELEVANT unless a sentinel is being written, so an
    // unresolvable one costs nothing on any path that carries none.
    let mut out = Vec::new();
    serialize_set_cell_path_element_into(&Value::Integer(7), "com.acme.NotAType(x)", &mut out)
        .expect("an ordinary element does not need the declared element type");
    assert_eq!(out, 7i32.to_be_bytes().to_vec());

    let mut out = Vec::new();
    let err = serialize_set_cell_path_element_into(&Value::Null, "set<int>", &mut out)
        .expect_err("CQL forbids a null set element");
    assert!(
        err.to_string().contains("SET elements cannot be null"),
        "the pre-existing null refusal must be unchanged: {err}"
    );
    assert!(out.is_empty(), "a refusal writes nothing: {out:?}");
}
