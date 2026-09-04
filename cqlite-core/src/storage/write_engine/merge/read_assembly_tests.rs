//! Unit tests for [`super::read_assembly`] — merged-read reassembly of a row's
//! per-column cells (issues #2324/#2339).
//!
//! Split out of `read_assembly.rs` under the campsite rule (epic #1116/#1135):
//! the composite decode work of #2339 pushed the combined file well past the
//! ~800-line source target.

use super::*;
use crate::schema::{udt_registry_from_cql, Column, KeyColumn, TableSchema};
use crate::types::{TombstoneInfo, TombstoneType, UdtField, UdtValue};
use std::collections::HashMap;
use std::sync::OnceLock;

/// The table's UDT registry, MANDATORY for a composite UDT element/key
/// (issue #2339): `CqlType::parse("set<frozen<key_part>>")` yields
/// `Set(Frozen(Custom("key_part")))` — an all-lowercase UDT name parses to a
/// bare `Custom` with NO field list — so without a registry there is no
/// structure to decode into.
///
/// `key_part (label text, rank int)` is the REAL type of the committed
/// `test_nested_udt_keys.nested_udt_keys` fixture, so the cell-path bytes the
/// UDT cases use can be real Cassandra-written bytes lifted verbatim from its
/// sstabledump golden (#3042). `addr_type` is deliberately NOT registered — it
/// is the unresolved-UDT fail-closed case.
fn registry() -> Option<UdtScope<'static>> {
    static REG: OnceLock<UdtRegistry> = OnceLock::new();
    Some(UdtScope {
        registry: REG.get_or_init(|| {
            udt_registry_from_cql("CREATE TYPE key_part (label text, rank int);", "ks")
        }),
        keyspace: "ks",
    })
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: Vec::new(),
        columns: vec![
            col("id", "int"),
            col("s", "int"),
            col("nums", "set<int>"),
            col("items", "list<int>"),
            col("m", "map<text, bigint>"),
            // Non-scalar element/key columns (roborev 1629 F2): the scalar
            // codec cannot decode these, so they exercise the opaque-composite
            // Blob + raw-byte-order path.
            col("fset", "set<frozen<addr_type>>"),
            col("ftk", "map<frozen<tuple<int, text>>, bigint>"),
            // set<frozen<map<text,int>>> — the frozen-collection element
            // framing (i32-BE count + i32-BE element lengths, Cassandra
            // `CollectionSerializer.pack`/`writeValue`), issue #2339.
            col("smap", "set<frozen<map<text,int>>>"),
            // set<frozen<key_part>> — a composite UDT element, resolvable
            // through `registry()` (issue #2339).
            col("kset", "set<frozen<key_part>>"),
            // roborev job 52 / G2: the NESTED unresolved-UDT case. `key_part` IS in
            // `registry()`; `ghost_part` deliberately is NOT, and it sits nested
            // inside the tuple rather than at the top level.
            col("nset", "set<frozen<tuple<frozen<ghost_part>, int>>>"),
            // The SAME element type by a keyspace-QUALIFIED reference, which is
            // how Cassandra emits a UDT column type and what the CQL parser
            // retains (roborev F2): `CqlType::parse` yields
            // `Custom("udt:ks.key_part")`.
            col("qset", "set<frozen<ks.key_part>>"),
            // The same two types as COMPONENTS of a composite (roborev F3): the
            // composite path decodes to a `Value` and orders component-wise, so
            // it cannot borrow the scalar path's raw-cell_path workaround and
            // needs its own byte-order arm.
            col("ituple", "set<frozen<tuple<inet, int>>>"),
            col("ttuple", "set<frozen<tuple<time, int>>>"),
            // inet/time element ordering (roborev 1631/1632): InetAddressType /
            // TimeType order by raw serialized bytes, which the scalar comparator
            // contradicted with a formatted-string order until #3790.
            col("iset", "set<inet>"),
            col("tset", "set<time>"),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// One complex ELEMENT cell (a set/list member, or a map entry keyed by
/// `cell_path`).
fn elem(column: &str, value: Value, cell_path: Vec<u8>) -> CellData {
    CellData {
        column: column.into(),
        value,
        timestamp: 1,
        ttl: None,
        cell_path: Some(cell_path),
        local_deletion_time: None,
        is_complex_element: true,
        is_deleted: false,
        has_empty_value: false,
    }
}

/// `(int 1, text "a")` as Cassandra serializes a `frozen<tuple<int, text>>`:
/// 4-byte i32-BE length per field, `-1` == null
/// (`TupleType.buildValue`'s `accessor.putInt`, pinned `cassandra-5.0.8`).
const TUPLE_KEY_1A: &[u8] = &[
    0, 0, 0, 4, 0, 0, 0, 1, // int 1
    0, 0, 0, 1, b'a', // text "a"
];

/// Decode an even-length hex string to bytes (test surface for the
/// sstabledump-golden cell paths, issue #2339).
fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
        .collect()
}

fn get<'a>(cells: &'a RowCells, name: &str) -> Option<&'a Value> {
    cells
        .iter()
        .find(|(n, _)| n.as_ref() == name)
        .map(|(_, v)| v)
}

#[test]
fn simple_column_passes_through() {
    let cells = vec![CellData::new("s".into(), Value::Integer(7), 1)];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(get(&out, "s"), Some(&Value::Integer(7)));
}

#[test]
fn simple_tombstone_reads_absent() {
    let tomb = Value::Tombstone(Box::new(TombstoneInfo {
        deletion_time: 1,
        tombstone_type: TombstoneType::CellTombstone,
        local_deletion_time: 0,
        ttl: None,
        range_start: None,
        range_end: None,
    }));
    let cells = vec![CellData::new("s".into(), tomb, 1)];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "s"),
        None,
        "a tombstoned simple cell is absent (null)"
    );
}

#[test]
fn set_and_list_reassemble_all_members_not_last_cell() {
    // Two set members + three list members, each its own cell.
    let cells = vec![
        elem("nums", Value::Integer(10), vec![0, 0, 0, 10]),
        elem("nums", Value::Integer(20), vec![0, 0, 0, 20]),
        elem("items", Value::Integer(1), vec![0]),
        elem("items", Value::Integer(2), vec![1]),
        elem("items", Value::Integer(3), vec![2]),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "nums"),
        Some(&Value::Set(vec![Value::Integer(10), Value::Integer(20)])),
        "SET must keep ALL members, not last-cell-wins"
    );
    assert_eq!(
        get(&out, "items"),
        Some(&Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3)
        ])),
        "LIST must keep all members in on-disk order"
    );
}

#[test]
fn map_reassembles_entries_decoding_key_from_cell_path() {
    // MAP<TEXT,BIGINT>: cell_path is the raw utf8 key; value is the bigint.
    let cells = vec![
        elem("m", Value::BigInt(100), b"alpha".to_vec()),
        elem("m", Value::BigInt(200), b"beta".to_vec()),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "m"),
        Some(&Value::Map(vec![
            (Value::Text("alpha".into()), Value::BigInt(100)),
            (Value::Text("beta".into()), Value::BigInt(200)),
        ])),
        "MAP must reassemble every entry with the key decoded from cell_path"
    );
}

#[test]
fn set_of_inet_orders_by_raw_bytes_not_string() {
    // set<inet>: cell_path (and value) is the raw 4-byte address. Cassandra's
    // InetAddressType orders by UNSIGNED address bytes, so 9.0.0.1 (bytes
    // [9,0,0,1]) precedes 10.0.0.1 (bytes [10,0,0,1]). The formatted-string
    // order the scalar `Custom("inet")` comparator would use is the REVERSE
    // ("10.0.0.1" < "9.0.0.1"), which would mis-order a multi-SSTable set.
    // Arrive out of order to prove the sort actually runs (roborev 1631).
    let ip_9 = vec![9u8, 0, 0, 1];
    let ip_10 = vec![10u8, 0, 0, 1];
    let cells = vec![
        elem("iset", Value::inet(ip_10.clone()), ip_10.clone()),
        elem("iset", Value::inet(ip_9.clone()), ip_9.clone()),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "iset"),
        Some(&Value::Set(vec![
            Value::Inet(ip_9.into()),
            Value::Inet(ip_10.into()),
        ])),
        "set<inet> must order by unsigned address bytes (9.0.0.1 before 10.0.0.1), \
         not the reversed formatted-string order"
    );
}

#[test]
fn set_of_time_orders_by_raw_bytes_not_formatted_string() {
    // set<time>: cell_path is the 8-byte big-endian nanoseconds-of-day; Cassandra's
    // TimeType orders by that raw long (non-negative → byte order == numeric order).
    // Until #3790 the scalar Custom("time") comparator instead used a
    // FORMATTED-string order ("TIME(HH:MM:SS.nnn)"), which — because the hours
    // field is only zero-padded to two digits — diverges from numeric order once
    // the hours magnitude changes digit-width. 10h vs 100h: the string
    // "TIME(100:..." sorts BEFORE "TIME(10:..." ('0' < ':'), the REVERSE of numeric
    // order, so a multi-SSTable set would mis-order pre-fix. (Valid times-of-day
    // happen to coincide under the current Display; ordering by the raw cell_path
    // bytes is the robust, parity-correct rule and closes that class here,
    // roborev 1632.) Arrive out of order to prove the sort runs.
    let t_small = 36_000_000_000_000i64; // 10h in ns
    let t_big = 360_000_000_000_000i64; // 100h in ns
    let cells = vec![
        elem("tset", Value::Time(t_big), t_big.to_be_bytes().to_vec()),
        elem("tset", Value::Time(t_small), t_small.to_be_bytes().to_vec()),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "tset"),
        Some(&Value::Set(vec![Value::Time(t_small), Value::Time(t_big)])),
        "set<time> must order by the raw big-endian long (10h before 100h), \
         not the reversed formatted-string order"
    );
}

#[test]
fn deleted_elements_are_dropped() {
    let mut deleted = elem("nums", Value::Integer(99), vec![0, 0, 0, 99]);
    deleted.is_deleted = true;
    let cells = vec![elem("nums", Value::Integer(10), vec![0, 0, 0, 10]), deleted];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "nums"),
        Some(&Value::Set(vec![Value::Integer(10)])),
        "a deleted set member must be dropped from the reassembled collection"
    );
}

#[test]
fn elements_reassemble_in_cell_path_order_not_arrival_order() {
    // Simulate multi-SSTable merge arrival: elements registered OUT of
    // cell_path order (a newer run's members encountered first). The
    // reassembled collection must land in authoritative cell_path order, not
    // arrival order (issue #2324, roborev 1628).
    let cells = vec![
        // set<int>: cell_path is the 4-byte big-endian member; arrive 30,10,20.
        elem("nums", Value::Integer(30), vec![0, 0, 0, 30]),
        elem("nums", Value::Integer(10), vec![0, 0, 0, 10]),
        elem("nums", Value::Integer(20), vec![0, 0, 0, 20]),
        // list<int>: cell_path is the position (single byte here); arrive 2,0,1.
        elem("items", Value::Integer(200), vec![2]),
        elem("items", Value::Integer(0), vec![0]),
        elem("items", Value::Integer(100), vec![1]),
        // map<text,bigint>: cell_path is the key; arrive gamma,alpha,beta.
        elem("m", Value::BigInt(3), b"gamma".to_vec()),
        elem("m", Value::BigInt(1), b"alpha".to_vec()),
        elem("m", Value::BigInt(2), b"beta".to_vec()),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "nums"),
        Some(&Value::Set(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ])),
        "SET must reassemble in element (cell_path) order, not arrival order"
    );
    assert_eq!(
        get(&out, "items"),
        Some(&Value::List(vec![
            Value::Integer(0),
            Value::Integer(100),
            Value::Integer(200),
        ])),
        "LIST must reassemble in position (cell_path) order, not arrival order"
    );
    assert_eq!(
        get(&out, "m"),
        Some(&Value::Map(vec![
            (Value::Text("alpha".into()), Value::BigInt(1)),
            (Value::Text("beta".into()), Value::BigInt(2)),
            (Value::Text("gamma".into()), Value::BigInt(3)),
        ])),
        "MAP must reassemble in key (cell_path) order, not arrival order"
    );
}

#[test]
fn map_deleted_entry_omitted_per_cassandra_select_semantics() {
    // A deleted MAP entry is OMITTED from the reassembled map — matching real
    // Cassandra SELECT output (the read-path authority, issue #1742). This
    // intentionally diverges from the single-generation reader's physical
    // collapsed_value, which keeps a (key, Null) pair (issue #2324, roborev
    // 1628 adjudication; see the module doc + drop-site comment).
    let mut deleted = elem("m", Value::BigInt(999), b"gone".to_vec());
    deleted.is_deleted = true;
    let cells = vec![
        elem("m", Value::BigInt(1), b"alpha".to_vec()),
        deleted,
        elem("m", Value::BigInt(2), b"beta".to_vec()),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "m"),
        Some(&Value::Map(vec![
            (Value::Text("alpha".into()), Value::BigInt(1)),
            (Value::Text("beta".into()), Value::BigInt(2)),
        ])),
        "a deleted map entry must be omitted (no (key, Null)) per Cassandra SELECT semantics"
    );
}

#[test]
fn simple_then_complex_same_column_fails_closed() {
    // A simple (whole-value) cell then a per-element (complex) cell for the
    // SAME column: impossible for a consistent na+ schema (roborev 1629 F1).
    // Pre-fix the element was SILENTLY DROPPED (register_complex's `if let`
    // fell through); now it fails closed naming the column.
    let simple = CellData::new("nums".into(), Value::Integer(5), 1);
    let complex = elem("nums", Value::Integer(10), vec![0, 0, 0, 10]);
    let err = assemble_read_cells_with_udts(vec![simple, complex], &schema(), None, registry())
        .unwrap_err();
    assert!(
        err.to_string().contains("nums"),
        "mixed-shape error must name the column, got: {err}"
    );
}

#[test]
fn complex_then_simple_same_column_fails_closed() {
    // The reverse arrival order: a complex element then a simple cell for the
    // SAME column. Pre-fix the simple cell OVERWROTE (dropped) the whole
    // collection; now it fails closed naming the column (roborev 1629 F1).
    let complex = elem("nums", Value::Integer(10), vec![0, 0, 0, 10]);
    let simple = CellData::new("nums".into(), Value::Integer(5), 1);
    let err = assemble_read_cells_with_udts(vec![complex, simple], &schema(), None, registry())
        .unwrap_err();
    assert!(
        err.to_string().contains("nums"),
        "mixed-shape error must name the column, got: {err}"
    );
}

/// A `frozen<udt>` SET ELEMENT must reconstruct as a typed `Value::Udt`
/// (issue #2339). The element identity IS the `cell_path`; the cell VALUE is
/// empty for a set, as the sstabledump golden confirms (`"value":""`).
///
/// The three cell paths are VERBATIM element bodies lifted from the
/// sstabledump golden of the committed `test_nested_udt_keys.nested_udt_keys`
/// fixture (`s_set_udt`), i.e. real CASSANDRA-WRITTEN bytes for
/// `key_part (label text, rank int)`, never CQLite's own output (#3042).
/// `ffffffff` (i32 `-1`) is Cassandra's NULL field marker
/// (`TupleType.buildValue`'s `putInt(-1)`), exercised by the all-null element.
///
/// ORDER is Cassandra's `TupleType.compareCustom` (component-wise, a NULL
/// component sorting BEFORE a non-null one), NOT raw `cell_path` byte order —
/// which would put the `ffff…` element LAST. Confirmed by Cassandra-written
/// bytes: in the same fixture's partition `2`, the `s_set_udt` frozen set holds
/// `key_part{null,null}` BEFORE `key_part{"nullrank2",null}`.
#[test]
fn set_of_frozen_udt_decodes_structurally() {
    let beta = hex("00000004626574610000000400000002");
    let gamma = hex("0000000567616d6d610000000400000003");
    let nulls = hex("ffffffffffffffff");
    let cells = vec![
        elem("kset", Value::blob(Vec::new()), gamma.clone()),
        elem("kset", Value::blob(Vec::new()), nulls.clone()),
        elem("kset", Value::blob(Vec::new()), beta.clone()),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    let field = |name: &str, v: Value| UdtField {
        name: name.to_string(),
        value: Some(v),
    };
    let udt = |fields: Vec<UdtField>| {
        Value::Frozen(Box::new(Value::Udt(Box::new(UdtValue {
            keyspace: "ks".to_string(),
            type_name: "key_part".to_string(),
            fields,
        }))))
    };
    assert_eq!(
        get(&out, "kset"),
        Some(&Value::Set(vec![
            // Component-wise `UserType`/`TupleType` order: a NULL first component
            // sorts before any non-null one, then "beta" < "gamma".
            udt(vec![
                UdtField {
                    name: "label".to_string(),
                    value: None,
                },
                UdtField {
                    name: "rank".to_string(),
                    value: None,
                },
            ]),
            udt(vec![
                field("label", Value::Text("beta".into())),
                field("rank", Value::Integer(2)),
            ]),
            udt(vec![
                field("label", Value::Text("gamma".into())),
                field("rank", Value::Integer(3)),
            ]),
        ])),
        "a frozen<UDT> set element must decode structurally (issue #2339)"
    );
}

/// Roborev F2 (issue #2339): a keyspace-QUALIFIED UDT reference must decode on
/// the merged-read arm, because the Flight bypass predicate already treats it as
/// resolvable.
///
/// `ComparatorType::from_cql_type_with_registry`'s `Custom` arm looks a reference
/// up with `registry.get_udt(keyspace, name)`, which is NOT qualifier-aware, so
/// `Custom("udt:ks.key_part")` missed a registry keyed by BARE name and stayed an
/// opaque `Custom` — the merged read then failed closed. The bypass predicate
/// answers the same question with `UdtRegistry::resolve_type`, which IS
/// qualifier-aware, so it selected the single-generation arm: one generation
/// decoded, two errored. `element_comparator` now resolves through that SAME
/// resolver, so the two arms agree by construction.
///
/// The expectation is IDENTICAL to `set_of_frozen_udt_decodes_structurally`'s
/// (same fixture-derived cell paths, same declared UDT) because a qualifier is a
/// resolution detail, not a different type — and the decoded `UdtValue.type_name`
/// stays the BARE name, which `resolve_udt_reference` guarantees.
#[test]
fn set_of_qualified_frozen_udt_decodes_structurally() {
    let beta = hex("00000004626574610000000400000002");
    let gamma = hex("0000000567616d6d610000000400000003");
    let bare = assemble_read_cells_with_udts(
        vec![
            elem("kset", Value::blob(Vec::new()), gamma.clone()),
            elem("kset", Value::blob(Vec::new()), beta.clone()),
        ],
        &schema(),
        None,
        registry(),
    )
    .expect("the BARE reference decodes (the pre-existing path)");
    let qualified = assemble_read_cells_with_udts(
        vec![
            elem("qset", Value::blob(Vec::new()), gamma),
            elem("qset", Value::blob(Vec::new()), beta),
        ],
        &schema(),
        None,
        registry(),
    )
    .expect(
        "a QUALIFIED `set<frozen<ks.key_part>>` element must decode on the merged-read \
         arm — the bypass predicate already calls it resolvable (roborev F2, #2339)",
    );
    assert_eq!(
        get(&qualified, "qset"),
        get(&bare, "kset"),
        "a keyspace qualifier is a resolution detail: the decoded value must be \
         identical to the bare reference's"
    );
    assert!(
        matches!(get(&qualified, "qset"), Some(Value::Set(items)) if items.len() == 2),
        "sanity: the qualified column really did reassemble both elements"
    );
}

/// Roborev F3 (issue #2339): an `inet` COMPONENT of a composite orders by the
/// serialized address bytes, not by the formatted dotted quad.
///
/// `InetAddressType() {super(ComparisonType.BYTE_ORDER);}` at the pinned
/// `cassandra-5.0.8` tag, so `9.0.0.1` = `[9,0,0,1]` precedes `10.0.0.1` =
/// `[10,0,0,1]` — the REVERSE of their dotted-quad TEXT order (`"10.0.0.1" <
/// "9.0.0.1"`, since `'1' < '9'`). A `Display`-string comparison therefore orders a
/// `tuple`/UDT carrying an inet the wrong way round, and that is what
/// `ComparatorType::Custom("inet")` used to do before #3790.
///
/// This case is NOT a pin of this module's own arm — there is no such arm. The
/// composite scalar leaves delegate to the central `ComparatorType::compare`, whose
/// `custom::compare_inet` is an unsigned `[u8]` compare of the raw address
/// (fixture-backed against the Cassandra-written `test_comparator_order` corpus).
/// What this pins is that the COMPOSITE path reaches that authority for a component
/// type, which the scalar `set<inet>` path cannot demonstrate: the scalar path sorts
/// on raw `cell_path` bytes (`comparator_orders_by_raw_cell_path_bytes`), whereas a
/// composite's cell_path bytes are dominated by the components' i32-BE length
/// prefixes, so only the value comparator can order it.
#[test]
fn composite_with_an_inet_component_orders_by_address_bytes_not_text() {
    // frozen<tuple<inet, int>>: i32-BE length + value per component.
    let nine = hex("0000000409000001" /* 9.0.0.1 */)
        .into_iter()
        .chain(hex("0000000400000001")) // int 1
        .collect::<Vec<u8>>();
    let ten = hex("000000040a000001" /* 10.0.0.1 */)
        .into_iter()
        .chain(hex("0000000400000001")) // int 1
        .collect::<Vec<u8>>();
    // Arrival order deliberately the REVERSE of the expected order, so a
    // no-op sort cannot pass this.
    let cells = vec![
        elem("ituple", Value::blob(Vec::new()), ten),
        elem("ituple", Value::blob(Vec::new()), nine),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    let tuple = |addr: Vec<u8>| {
        Value::Frozen(Box::new(Value::Tuple(vec![
            Value::inet(addr),
            Value::Integer(1),
        ])))
    };
    assert_eq!(
        get(&out, "ituple"),
        Some(&Value::Set(vec![
            tuple(vec![9, 0, 0, 1]),
            tuple(vec![10, 0, 0, 1]),
        ])),
        "9.0.0.1 must precede 10.0.0.1 (InetAddressType is BYTE_ORDER); the \
         formatted-string order the central comparator falls through to is the reverse"
    );
}

/// Roborev F3 (issue #2339): a `time` COMPONENT of a composite orders by its
/// 8-byte big-endian serialized form.
///
/// `private TimeType() {super(ComparisonType.BYTE_ORDER);}` at the pinned
/// `cassandra-5.0.8` tag. Unlike `inet` there is no OBSERVABLE divergence for an
/// in-range value — `Value`'s `TIME(hh:mm:ss.nnnnnnnnn)` rendering zero-pads every
/// field, so text order happens to agree — which is precisely why this is pinned:
/// the ordering must come from the declared type's authority, not from a `Display`
/// impl that no format authority governs and that a future change could reflow.
///
/// As with the `inet` case, the order is delivered by the central
/// `ComparatorType::compare` (`custom::compare_time`, which compares
/// `i64::to_be_bytes` — BYTE_ORDER verbatim, and correct for a NEGATIVE nanos too,
/// #3935), not by any arm local to this module. What is pinned here is that the
/// composite path routes a `time` COMPONENT to that authority.
#[test]
fn composite_with_a_time_component_orders_by_serialized_bytes() {
    // frozen<tuple<time, int>>: time is an 8-byte i64-BE nanoseconds-of-day.
    let at = |nanos: i64| {
        let mut path = vec![0, 0, 0, 8];
        path.extend_from_slice(&nanos.to_be_bytes());
        path.extend_from_slice(&hex("0000000400000001")); // int 1
        path
    };
    let (early, late) = (1_i64, 36_000_000_000_000_i64); // 00:00:00.000000001, 10:00
    let cells = vec![
        elem("ttuple", Value::blob(Vec::new()), at(late)),
        elem("ttuple", Value::blob(Vec::new()), at(early)),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    let tuple = |nanos: i64| {
        Value::Frozen(Box::new(Value::Tuple(vec![
            Value::Time(nanos),
            Value::Integer(1),
        ])))
    };
    assert_eq!(
        get(&out, "ttuple"),
        Some(&Value::Set(vec![tuple(early), tuple(late)])),
        "the earlier nanoseconds-of-day must sort first (TimeType is BYTE_ORDER \
         over an 8-byte big-endian i64)"
    );
}

/// Issue #2339 (roborev F4, REJECTED finding — this pins the CORRECT behaviour so
/// it cannot be re-litigated): a tuple/UDT whose trailing components are OMITTED
/// compares EQUAL to one that encodes them EXPLICITLY AS NULL.
///
/// A review round claimed the two must differ ("Cassandra's shorter-prefix
/// ordering") and proposed comparing component counts. That contradicts the pinned
/// `cassandra-5.0.8` `TupleType.compareCustom`, verbatim:
///
/// ```text
/// if (allRemainingComponentsAreNull(left, accessorL, offsetL)
///  && allRemainingComponentsAreNull(right, accessorR, offsetR))
///     return 0;
/// ...
/// private <T> boolean allRemainingComponentsAreNull(T v, ValueAccessor<T> accessor, int offset) {
///     while (!accessor.isEmptyFromOffset(v, offset)) {
///         int size = accessor.getInt(v, offset);
///         offset += TypeSizes.INT_SIZE;
///         if (size >= 0) return false;
///     }
///     return true;            // EXHAUSTED BUFFER => vacuously TRUE
/// }
/// ```
///
/// An exhausted buffer is vacuously "all remaining components are null", so the
/// omitted and explicit encodings return 0. Cassandra's shorter-prefix rule lives
/// in `ClusteringComparator` (clustering-key PREFIXES) — a different ordering on a
/// different type, not tuple component comparison.
///
/// Byte encodings, not hand-built values: the decoder is what turns an omitted
/// suffix into a SHORTER `Value::Tuple`, and that is half of the property.
#[test]
fn an_omitted_tuple_suffix_compares_equal_to_an_explicit_all_null_suffix() {
    let cmp = ComparatorType::Tuple(vec![ComparatorType::Text, ComparatorType::Int]);
    let decode =
        |bytes: &[u8]| composite::decode_composite("c", "element", bytes, &cmp).expect("decodes");
    // text "a", then: nothing / an explicit null int (i32-BE -1).
    let omitted = decode(&hex("0000000161"));
    let explicit = decode(&hex("0000000161ffffffff"));
    assert_eq!(
        omitted,
        Value::Tuple(vec![Value::Text("a".into())]),
        "precondition: an omitted suffix decodes to a SHORTER tuple, which is what \
         makes the comparison non-trivial"
    );
    assert_eq!(
        explicit,
        Value::Tuple(vec![Value::Text("a".into()), Value::Null]),
        "precondition: the explicit encoding decodes to a null component"
    );
    assert_eq!(
        composite::compare_composite(&omitted, &explicit, &cmp).unwrap(),
        std::cmp::Ordering::Equal,
        "TupleType.compareCustom: both sides' remaining components are all null \
         (vacuously so for the exhausted one), so it returns 0"
    );
    assert_eq!(
        composite::compare_composite(&explicit, &omitted, &cmp).unwrap(),
        std::cmp::Ordering::Equal,
        "and the relation is symmetric"
    );
    // The CONTROL that makes the assertion meaningful: a NON-null suffix is
    // GREATER than an omitted one, so "equal" is not just "short-circuits on
    // length".
    let present = decode(&hex("00000001610000000400000007"));
    assert_eq!(
        composite::compare_composite(&omitted, &present, &cmp).unwrap(),
        std::cmp::Ordering::Less,
        "a side that runs out is LESS than one carrying a non-null component"
    );
}

/// The RETAINED fail-closed path: a composite element whose declared UDT name
/// resolves to NOTHING (no registry, or a name absent from it) has no field
/// list to decode into, so it still fails closed naming the column and the
/// unresolved type — never opaque bytes, never a guess (issues #28/#2339).
/// `fset set<frozen<addr_type>>` is deliberately absent from `registry()`.
#[test]
fn set_of_unresolved_udt_still_fails_closed() {
    let cells = vec![elem(
        "fset",
        Value::blob(Vec::new()),
        hex("00000004626574610000000400000002"),
    )];
    let err = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("fset") && msg.contains("addr_type") && msg.contains("#2339"),
        "an unresolved composite element must fail closed naming the column and \
         the declared type, got: {msg}"
    );
}

/// **roborev job 60 — a composite cell path with TRAILING BYTES must fail closed.**
///
/// `ftk` is `map<frozen<tuple<int, text>>, bigint>`. A well-formed key for it is
/// `i32 len=4 | int | i32 len=n | text`. Appending ONE extra byte leaves framing that
/// does not consume the slice — and the shared decoder used to ignore the remainder,
/// so this path decoded to the SAME logical key as the well-formed one. In a map that
/// is a duplicate-key hazard.
///
/// Cassandra's `TupleType.validate` throws on trailing bytes after a composite's
/// components, so refusing it adds no availability risk for data Cassandra would read.
///
/// RED BEFORE THE FIX: this returned `Ok` and the trailing byte was silently dropped.
#[test]
fn composite_cell_path_with_trailing_bytes_fails_closed() {
    // int 7, then text "a", then ONE byte too many.
    let mut path = Vec::new();
    path.extend_from_slice(&4i32.to_be_bytes());
    path.extend_from_slice(&7i32.to_be_bytes());
    path.extend_from_slice(&1i32.to_be_bytes());
    path.push(b'a');
    let well_formed_len = path.len();
    path.push(0xAA); // trailing garbage

    let cells = vec![elem("ftk", Value::BigInt(1), path)];
    let err = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ftk") && msg.contains("trailing"),
        "a composite cell path with trailing bytes must fail closed naming the column, \
         got: {msg}"
    );

    // Control: the SAME path without the extra byte decodes fine, so the refusal is
    // caused by the trailing byte and not by the fixture being malformed.
    let mut ok_path = Vec::new();
    ok_path.extend_from_slice(&4i32.to_be_bytes());
    ok_path.extend_from_slice(&7i32.to_be_bytes());
    ok_path.extend_from_slice(&1i32.to_be_bytes());
    ok_path.push(b'a');
    assert_eq!(ok_path.len(), well_formed_len);
    let ok_cells = vec![elem("ftk", Value::BigInt(1), ok_path)];
    assert!(
        assemble_read_cells_with_udts(ok_cells, &schema(), None, registry()).is_ok(),
        "the same cell path WITHOUT the trailing byte must still decode"
    );
}

/// **GAP (issue #2339) — a `varint` COMPONENT of a composite does NOT yet order as
/// Cassandra does. `#[ignore]`d, and DELIBERATELY still asserting the CORRECT order.**
///
/// Cassandra's `IntegerType.compare` orders `varint` by SIGNED two's-complement
/// magnitude, so `-1` (body `0xFF`) sorts BELOW `0` (body `0x00`). The central
/// `ComparatorType::compare`'s `varint` arm is `Bytes::cmp` — raw unsigned bytes —
/// which puts `0` first because `0x00 < 0xFF`. `decimal` (unequal scales compared as
/// `format!("{:?}.{}")` strings, self-described in source as "For now, simple string
/// comparison") and `uuid` (raw `Uuid::cmp` rather than `UUIDType`'s
/// version-then-v1-timestamp-then-tail order) are wrong in the same way.
///
/// Since #2339 the composite scalar leaves delegate to that central comparator and to
/// nothing else, so this arm INHERITS the defect rather than papering over it. That is
/// the deliberate trade: the alternative was to keep the write path's
/// `collection_order::compare_collection_elements` as a SECOND ordering authority for
/// the same types, which is the divergence class #2339 exists to remove — and a second
/// path would also HIDE this defect from exactly the test that reports it.
///
/// The fix is a CONVERGENCE, not new code: `collection_order::scalar` already
/// implements all three correctly under its `IntegerType`/`DecimalType`/`UUIDType`
/// citations (#1275). When the central comparator adopts them, DELETE the `#[ignore]`
/// and this test passes as written — which is why the expectation is NOT inverted to
/// pin the current wrong answer. Pinning the defect would green-wash it and would red
/// the moment somebody fixed it.
///
/// The expectation is derived from Cassandra's semantics (`IntegerType.compare`,
/// signed two's-complement), never from CQLite's own prior behaviour (#3041).
#[test]
#[ignore = "GAP #4063 (from #2339): central ComparatorType::compare orders varint by raw bytes, not signed IntegerType order; un-ignore when the central varint/decimal/uuid arms converge on collection_order::scalar"]
fn varint_component_of_a_composite_orders_signed_not_by_raw_bytes() {
    let cmp = ComparatorType::Tuple(vec![ComparatorType::Varint]);
    let minus_one = Value::Tuple(vec![Value::Varint(vec![0xFF].into())]);
    let zero = Value::Tuple(vec![Value::Varint(vec![0x00].into())]);

    assert_eq!(
        compare_composite(&minus_one, &zero, &cmp).unwrap(),
        std::cmp::Ordering::Less,
        "a varint component must order -1 BEFORE 0 (signed, per Cassandra IntegerType); \
         raw-byte order would put 0 first because 0xFF > 0x00"
    );
    assert_eq!(
        compare_composite(&zero, &minus_one, &cmp).unwrap(),
        std::cmp::Ordering::Greater,
        "and the comparison must be antisymmetric"
    );
}

/// **roborev job 52 / G2 — the NESTED unresolved UDT must fail closed too.**
///
/// The guard used to test only the comparator `unwrap_frozen_comparator` returns,
/// i.e. the TOP level. `nset set<frozen<tuple<frozen<ghost_part>, int>>>` keeps
/// `Custom("ghost_part")` at a NESTED position, so that check passed and the
/// decoder's `_ =>` arm turned the field into an opaque `Value::Blob` — a
/// plausible-looking wrong value that a multi-generation read then emitted AND
/// SORTED, instead of failing closed. That is the class #28 forbids, and it also
/// contradicted this PR's own claim that an unresolvable UDT still fails closed.
///
/// RED BEFORE THE FIX: this returned `Ok` with a `Blob` nested in the tuple.
#[test]
fn nested_unresolved_udt_fails_closed() {
    let cells = vec![elem(
        "nset",
        Value::blob(Vec::new()),
        hex("00000004626574610000000400000002"),
    )];
    let err = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("nset") && msg.contains("ghost_part") && msg.contains("#2339"),
        "an unresolved UDT NESTED inside a tuple must fail closed naming the column \
         and the nested type — never an opaque Blob, got: {msg}"
    );
}
/// **The three-argument [`assemble_read_cells`] is PUBLIC API and must keep
/// behaving as `assemble_read_cells_with_udts(.., None)` (roborev job 110 F2).**
///
/// #2339 needed a UDT registry on this path, and adding a required parameter to a
/// `pub` re-exported function would break every external consumer at compile
/// time. So the original arity is preserved as a delegation. This pins the
/// delegation itself, which is the part that can silently rot: a future edit that
/// gave the three-arg form its OWN body, or defaulted it to something other than
/// `None`, would still compile and would still pass every other test in this file
/// (they all call the four-arg form).
///
/// Asserted on BOTH sides of the registry's observable effect, so neither arm is
/// a tautology: a scalar collection where both must SUCCEED identically, and a
/// composite UDT element where both must FAIL CLOSED with the same message.
#[test]
fn three_arg_entry_point_is_exactly_the_four_arg_form_with_no_registry() {
    // (1) Registry-independent input: identical Ok values.
    let scalar = || {
        vec![
            elem("nums", Value::Integer(20), vec![0, 0, 0, 20]),
            elem("nums", Value::Integer(10), vec![0, 0, 0, 10]),
        ]
    };
    let three = assemble_read_cells(scalar(), &schema(), None).unwrap();
    let four = assemble_read_cells_with_udts(scalar(), &schema(), None, None).unwrap();
    assert_eq!(
        get(&three, "nums"),
        get(&four, "nums"),
        "the three-arg form must produce the same row as the four-arg form with None"
    );
    assert_eq!(
        get(&three, "nums"),
        Some(&Value::Set(vec![Value::Integer(10), Value::Integer(20)])),
        "sanity: that shared value is the real reassembled set, not two empty rows"
    );

    // (2) Registry-DEPENDENT input: both must fail closed, identically. This is
    // what would break if the three-arg form silently supplied a registry.
    let composite = || {
        vec![elem(
            "kset",
            Value::blob(Vec::new()),
            hex("00000004626574610000000400000002"),
        )]
    };
    let three_err = assemble_read_cells(composite(), &schema(), None)
        .expect_err("no registry => a composite UDT element must fail closed");
    let four_err = assemble_read_cells_with_udts(composite(), &schema(), None, None)
        .expect_err("explicit None registry => same fail-closed");
    assert_eq!(
        three_err.to_string(),
        four_err.to_string(),
        "both entry points must fail closed with the SAME error"
    );
    assert!(
        three_err.to_string().contains("kset") && three_err.to_string().contains("#2339"),
        "sanity: that shared error is the #2339 composite refusal, got: {three_err}"
    );

    // (3) And the registry genuinely CHANGES this outcome, which is what makes
    // (2) meaningful rather than a restatement of "both return Err".
    assert!(
        assemble_read_cells_with_udts(composite(), &schema(), None, registry()).is_ok(),
        "control: WITH the registry the same composite element decodes, so (2) \
         pins the absence of a registry rather than an unconditional failure"
    );
}

/// The same shape with NO registry at all: a composite element/key is
/// undecodable without one, so the path fails closed rather than guessing
/// (issue #2339).
#[test]
fn composite_element_without_registry_fails_closed() {
    let cells = vec![elem(
        "kset",
        Value::blob(Vec::new()),
        hex("00000004626574610000000400000002"),
    )];
    let err = assemble_read_cells_with_udts(cells, &schema(), None, None).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("kset") && msg.contains("key_part") && msg.contains("#2339"),
        "without a UDT registry a composite element must fail closed, got: {msg}"
    );
}

#[test]
fn all_deleted_collection_is_absent() {
    let mut deleted = elem("nums", Value::Integer(99), vec![0, 0, 0, 99]);
    deleted.is_deleted = true;
    let out = assemble_read_cells_with_udts(vec![deleted], &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "nums"),
        None,
        "an all-deleted collection reads absent (empty non-frozen collection == null)"
    );
}

#[test]
fn unprojected_composite_collection_column_is_dropped_not_errored() {
    // Projection-aware assembly (issue #2324, roborev 1633): a row carrying a
    // scalar column `s` AND an unsupported composite-keyed collection column
    // `ftk` (frozen<tuple> map key). A query that projects ONLY `s` (so `ftk`
    // is NOT in `needed`) must SUCCEED, dropping `ftk` entirely — matching the
    // observable pre-#2324 behaviour where an unrelated SELECT never touched
    // this column. Since #2339 the column would also ASSEMBLE fine when
    // projected, so this case now pins the projection DROP itself (its
    // complement, `composite_column_is_assembled_without_projection_filter`,
    // pins the decode).
    let cells = vec![
        CellData::new("s".into(), Value::Integer(7), 1),
        elem("ftk", Value::BigInt(1), TUPLE_KEY_1A.to_vec()),
    ];
    let needed: HashSet<String> = ["s".to_string()].into_iter().collect();
    let out = assemble_read_cells_with_udts(cells, &schema(), Some(&needed), registry()).unwrap();
    assert_eq!(get(&out, "s"), Some(&Value::Integer(7)));
    assert_eq!(
        get(&out, "ftk"),
        None,
        "an unprojected composite-keyed collection column is dropped, not assembled/errored"
    );
}

#[test]
fn composite_column_is_assembled_without_projection_filter() {
    // The complement of the case above: the SAME row with `needed = None` (a
    // plain `SELECT *`). Every column is read, so the composite-keyed `ftk` IS
    // assembled — and since #2339 that assembly SUCCEEDS structurally instead
    // of failing the whole request. This pins that the projection filter is
    // what makes the unprojected case a DROP, not an incidental change
    // (roborev 1633), and that the composite path itself is now decodable.
    let cells = vec![
        CellData::new("s".into(), Value::Integer(7), 1),
        elem("ftk", Value::BigInt(1), TUPLE_KEY_1A.to_vec()),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(get(&out, "s"), Some(&Value::Integer(7)));
    assert_eq!(
        get(&out, "ftk"),
        Some(&Value::Map(vec![(
            Value::Frozen(Box::new(Value::Tuple(vec![
                Value::Integer(1),
                Value::Text("a".into())
            ]))),
            Value::BigInt(1)
        )])),
        "with no projection filter the composite column is assembled structurally"
    );
}

#[test]
fn projected_composite_collection_column_is_decoded() {
    // When the composite column IS projected/referenced it is assembled — the
    // #2339 decode, where the pre-fix behaviour was a clean fail-closed error
    // (roborev 1632/1633).
    let cells = vec![elem("ftk", Value::BigInt(1), TUPLE_KEY_1A.to_vec())];
    let needed: HashSet<String> = ["ftk".to_string()].into_iter().collect();
    let out = assemble_read_cells_with_udts(cells, &schema(), Some(&needed), registry()).unwrap();
    assert_eq!(
        get(&out, "ftk"),
        Some(&Value::Map(vec![(
            Value::Frozen(Box::new(Value::Tuple(vec![
                Value::Integer(1),
                Value::Text("a".into())
            ]))),
            Value::BigInt(1)
        )])),
        "a projected composite map-key column decodes structurally (issue #2339)"
    );
}

// ---- issue #2339: composite collection key/element decode (RED first) ----

/// A `frozen<tuple<int, text>>` MAP KEY must reconstruct as a typed
/// `Value::Tuple`, not fail closed (issue #2339).
///
/// Cell-path bytes are Cassandra's tuple serialization — 4-byte i32-BE per
/// field, `-1` == null — per `TupleType.buildValue` (`accessor.putInt`) at
/// the pinned `cassandra-5.0.8` tag. `(1, "a")` and `(2, "b")`.
#[test]
fn map_with_frozen_tuple_key_decodes_structurally() {
    let k1 = TUPLE_KEY_1A.to_vec();
    let k2 = vec![
        0, 0, 0, 4, 0, 0, 0, 2, // int 2
        0, 0, 0, 1, b'b', // text "b"
    ];
    let cells = vec![
        elem("ftk", Value::BigInt(2), k2),
        elem("ftk", Value::BigInt(1), k1),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "ftk"),
        Some(&Value::Map(vec![
            (
                Value::Frozen(Box::new(Value::Tuple(vec![
                    Value::Integer(1),
                    Value::Text("a".into())
                ]))),
                Value::BigInt(1)
            ),
            (
                Value::Frozen(Box::new(Value::Tuple(vec![
                    Value::Integer(2),
                    Value::Text("b".into())
                ]))),
                Value::BigInt(2)
            ),
        ])),
        "a frozen<tuple> map key must decode structurally (issue #2339)"
    );
}

/// A `set<frozen<map<text,int>>>` element must reconstruct as a typed
/// nested map, exercising the FROZEN element framing (i32-BE), not the
/// non-frozen VInt framing (issue #2339).
///
/// The two cell paths are the VERBATIM hex sstabledump prints for
/// `test_types.cx_nested_frozen_collections.s_map_vals` — i.e. real
/// CASSANDRA-WRITTEN bytes, never CQLite's own output (#3042):
///   `00000001 00000002 6b31 00000004 00000001`            => {"k1": 1}
///   `00000002 00000002 6b32 00000004 00000002
///             00000002 6b33 00000004 00000003`            => {"k2": 2, "k3": 3}
#[test]
fn set_of_frozen_map_decodes_with_i32_element_framing() {
    let one = hex("00000001000000026b310000000400000001");
    let two = hex("00000002000000026b320000000400000002000000026b330000000400000003");
    let cells = vec![
        elem("smap", Value::blob(Vec::new()), two),
        elem("smap", Value::blob(Vec::new()), one),
    ];
    let out = assemble_read_cells_with_udts(cells, &schema(), None, registry()).unwrap();
    assert_eq!(
        get(&out, "smap"),
        Some(&Value::Set(vec![
            Value::Frozen(Box::new(Value::Map(vec![(
                Value::Text("k1".into()),
                Value::Integer(1)
            )]))),
            Value::Frozen(Box::new(Value::Map(vec![
                (Value::Text("k2".into()), Value::Integer(2)),
                (Value::Text("k3".into()), Value::Integer(3)),
            ]))),
        ])),
        "a frozen<map> set element must decode with i32-BE element framing (issue #2339)"
    );
}
