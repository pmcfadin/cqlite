//! Unit tests for [`super`] — the write path's UDT / marshal-type schema helpers.
//!
//! Split out of `schema_helpers.rs` under the campsite rule (#1116/#1135): that
//! source file was already over the 800-line target, so #4158's changes could not
//! land there without tripping the `file-size` ratchet.

use super::*;

const KS: &str = "test_ks";

fn address_def() -> UdtTypeDef {
    UdtTypeDef::new(KS.to_string(), "address".to_string())
        .with_field("street".to_string(), CqlType::Text, true)
        .with_field("city".to_string(), CqlType::Text, true)
}

fn registry_with_address() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(address_def());
    reg
}

/// Render at Cassandra's `toString(false)` — the TOP-LEVEL COLUMN entry point,
/// where a `FrozenType(...)` wrapper IS printed (#4158).
fn as_column(ty: &CqlType, reg: &UdtRegistry) -> String {
    render_field_marshal(ty, KS, reg, false)
}

/// Render as a FIELD of a frozen (never-multicell) UserType, where Cassandra
/// passes `ignoreFreezing || !isMultiCell` = true to every subtype, so a nested
/// frozen type is spelled bare (`UserType.java:444`).
fn as_frozen_udt_field(ty: &CqlType, reg: &UdtRegistry) -> String {
    render_field_marshal(ty, KS, reg, true)
}

/// roborev #1020 Finding 2: a `frozen<address>` DIRECT UDT field drops the
/// FrozenType wrapper and renders as a bare `UserType(...)`.
#[test]
fn direct_frozen_udt_field_renders_bare_user_type() {
    let reg = registry_with_address();
    let ty = CqlType::Frozen(Box::new(CqlType::Custom("address".to_string())));
    let m = as_frozen_udt_field(&ty, &reg);
    assert!(
        m.starts_with("org.apache.cassandra.db.marshal.UserType("),
        "direct frozen<udt> must be bare UserType, got {m}"
    );
    assert!(!m.contains("FrozenType("), "wrapper must be elided: {m}");
    assert!(m.contains("UTF8Type"), "address fields must expand: {m}");
}

/// roborev #1020 Finding 2: a `frozen<list<frozen<address>>>` COLUMN keeps its
/// FrozenType wrapper and expands the nested UDT element to `UserType(...)` —
/// it must NOT collapse to a non-frozen list nor fall back to `BytesType`.
/// Asserts the EXACT marshal byte shape.
#[test]
fn frozen_list_of_frozen_udt_keeps_wrapper_and_expands_udt() {
    let reg = registry_with_address();
    let ty = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
        Box::new(CqlType::Custom("address".to_string())),
    )))));
    let m = as_column(&ty, &reg);
    // street=737472656574, city=63697479, address=61646472657373.
    let expected = "org.apache.cassandra.db.marshal.FrozenType(\
         org.apache.cassandra.db.marshal.ListType(\
         org.apache.cassandra.db.marshal.UserType(test_ks,61646472657373,\
         737472656574:org.apache.cassandra.db.marshal.UTF8Type,\
         63697479:org.apache.cassandra.db.marshal.UTF8Type)))";
    assert_eq!(m, expected);
}

/// roborev #1020 Finding 2 (precise byte shape), COLUMN context: the nested UDT inside a
/// frozen collection expands to a full `UserType(...)` (never `BytesType`),
/// and the outer `FrozenType`/`ListType` wrappers are preserved.
#[test]
fn frozen_map_to_frozen_udt_expands_value_udt() {
    let reg = registry_with_address();
    // frozen<map<text, frozen<address>>>
    let ty = CqlType::Frozen(Box::new(CqlType::Map(
        Box::new(CqlType::Text),
        Box::new(CqlType::Frozen(Box::new(CqlType::Custom(
            "address".to_string(),
        )))),
    )));
    let m = as_column(&ty, &reg);
    assert!(
        m.starts_with(
            "org.apache.cassandra.db.marshal.FrozenType(\
             org.apache.cassandra.db.marshal.MapType("
        ),
        "outer Frozen+Map wrappers must be preserved: {m}"
    );
    assert!(
        m.contains("org.apache.cassandra.db.marshal.UserType(test_ks,61646472657373,"),
        "nested address UDT must expand to UserType, not BytesType: {m}"
    );
    assert!(
        !m.contains("BytesType"),
        "nested UDT must never collapse to BytesType: {m}"
    );
}

/// roborev #1020 Finding 1: a KEYSPACE-QUALIFIED `frozen<test_ks.address>`
/// direct UDT field resolves through the registry split and renders byte-for-
/// byte identical to the unqualified `frozen<address>` form (bare
/// `UserType(...)`, no `FrozenType` wrapper, no `BytesType` fallback).
#[test]
fn qualified_frozen_udt_field_resolves_identically_to_bare() {
    let reg = registry_with_address();
    let bare = CqlType::Frozen(Box::new(CqlType::Custom("address".to_string())));
    let qualified = CqlType::Frozen(Box::new(CqlType::Custom("test_ks.address".to_string())));
    let m_bare = as_frozen_udt_field(&bare, &reg);
    let m_qual = as_frozen_udt_field(&qualified, &reg);
    assert!(
        m_qual.starts_with("org.apache.cassandra.db.marshal.UserType("),
        "qualified frozen<ks.udt> must be bare UserType, got {m_qual}"
    );
    assert!(
        !m_qual.contains("BytesType"),
        "qualified UDT must resolve, not collapse to BytesType: {m_qual}"
    );
    assert_eq!(
        m_qual, m_bare,
        "qualified frozen<ks.udt> must render identically to the bare form"
    );
}

/// roborev #1020 Finding 1: a KEYSPACE-QUALIFIED
/// `frozen<list<frozen<test_ks.address>>>` resolves the nested qualified UDT
/// element through the registry split — identical marshal to the unqualified
/// `frozen<list<frozen<address>>>` (FrozenType+ListType wrappers preserved,
/// nested UDT expanded to `UserType(...)`, never `BytesType`).
#[test]
fn qualified_frozen_list_of_frozen_udt_resolves_identically_to_bare() {
    let reg = registry_with_address();
    let bare = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
        Box::new(CqlType::Custom("address".to_string())),
    )))));
    let qualified = CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Frozen(
        Box::new(CqlType::Custom("test_ks.address".to_string())),
    )))));
    let m_bare = as_column(&bare, &reg);
    let m_qual = as_column(&qualified, &reg);
    assert!(
        m_qual.starts_with(
            "org.apache.cassandra.db.marshal.FrozenType(\
             org.apache.cassandra.db.marshal.ListType("
        ),
        "outer Frozen+List wrappers must be preserved: {m_qual}"
    );
    assert!(
        m_qual.contains("org.apache.cassandra.db.marshal.UserType(test_ks,61646472657373,"),
        "nested qualified address UDT must expand to UserType: {m_qual}"
    );
    assert!(
        !m_qual.contains("BytesType"),
        "nested qualified UDT must never collapse to BytesType: {m_qual}"
    );
    assert_eq!(
        m_qual, m_bare,
        "qualified nested frozen<list<frozen<ks.udt>>> must match the bare form"
    );
}

/// roborev #1020 Finding 1: `cql_type_references_udt` must detect a UDT behind
/// a KEYSPACE-QUALIFIED name (so the column-level dispatch rewrites the header
/// instead of leaving it `BytesType`).
#[test]
fn cql_type_references_udt_detects_qualified_name() {
    let reg = registry_with_address();
    let qualified = CqlType::Frozen(Box::new(CqlType::Custom("test_ks.address".to_string())));
    assert!(
        cql_type_references_udt(&qualified, KS, &reg),
        "qualified frozen<ks.udt> must be detected as referencing a UDT"
    );
}

/// Issue #4158: a `frozen<collection>` FIELD of a frozen UDT loses its own
/// `FrozenType(...)` wrapper, because `UserType.toString(boolean)` stringifies
/// its fields with `ignoreFreezing || !isMultiCell` and a frozen UserType is
/// never multicell (pinned `cassandra-5.0.8`, `UserType.java:444`). The previous
/// hand-rolled rule dropped the wrapper ONLY for a DIRECT UDT field and kept it
/// for a frozen collection — a spelling Cassandra does not write, and one that
/// had begun to disagree with `cql_type_to_marshal_type` after #4158 corrected
/// the string converter.
#[test]
fn frozen_collection_field_of_a_frozen_udt_drops_its_wrapper() {
    let reg = registry_with_address();
    let q = "org.apache.cassandra.db.marshal.";
    for (field, expected) in [
        (
            CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int)))),
            format!("{q}ListType({q}Int32Type)"),
        ),
        (
            CqlType::Frozen(Box::new(CqlType::Set(Box::new(CqlType::Text)))),
            format!("{q}SetType({q}UTF8Type)"),
        ),
        (
            CqlType::Frozen(Box::new(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Int),
            ))),
            format!("{q}MapType({q}UTF8Type,{q}Int32Type)"),
        ),
    ] {
        assert_eq!(as_frozen_udt_field(&field, &reg), expected);
    }
}

/// Issue #4158: `TupleType.toString()` is
/// `getClass().getName() + stringifyTypeParameters(types, true)`
/// (`TupleType.java:557-560`) — it never wraps ITSELF and always renders its
/// components with `ignoreFreezing = true`. So a `frozen<tuple<...>>` COLUMN has
/// no wrapper at all, and a frozen component inside a tuple has none either.
#[test]
fn tuple_never_wraps_itself_and_ignores_freezing_in_components() {
    let reg = registry_with_address();
    let q = "org.apache.cassandra.db.marshal.";

    // frozen<tuple<int, frozen<address>>> as a COLUMN.
    let frozen_tuple = CqlType::Frozen(Box::new(CqlType::Tuple(vec![
        CqlType::Int,
        CqlType::Frozen(Box::new(CqlType::Custom("address".to_string()))),
    ])));
    let m = as_column(&frozen_tuple, &reg);
    assert!(
        !m.contains("FrozenType("),
        "a tuple is already frozen and is never wrapped: {m}"
    );
    assert!(
        m.starts_with(&format!("{q}TupleType({q}Int32Type,{q}UserType(test_ks,")),
        "the tuple's UDT component must expand to a bare UserType: {m}"
    );

    // tuple<int, frozen<list<int>>> as a COLUMN: the component's own wrapper
    // is suppressed by the tuple's `ignoreFreezing = true`.
    let tuple = CqlType::Tuple(vec![
        CqlType::Int,
        CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int)))),
    ]);
    assert_eq!(
        as_column(&tuple, &reg),
        format!("{q}TupleType({q}Int32Type,{q}ListType({q}Int32Type))")
    );
}

/// A TOP-LEVEL `frozen<UDT>` COLUMN keeps its wrapper — the shape Apache
/// Cassandra wrote for `test_oa.udt_table`'s `address frozen<address_type>`
/// column. The FIELD context (asserted by
/// [`direct_frozen_udt_field_renders_bare_user_type`]) is the one that drops it;
/// the two must not be conflated (#4158).
#[test]
fn frozen_udt_column_keeps_its_wrapper() {
    let reg = registry_with_address();
    let ty = CqlType::Frozen(Box::new(CqlType::Custom("address".to_string())));
    let m = as_column(&ty, &reg);
    assert_eq!(
        m,
        "org.apache.cassandra.db.marshal.FrozenType(\
org.apache.cassandra.db.marshal.UserType(test_ks,61646472657373,\
737472656574:org.apache.cassandra.db.marshal.UTF8Type,\
63697479:org.apache.cassandra.db.marshal.UTF8Type))"
    );
}
