//! Issue #2807: keyspace-qualified UDT type names in CQL schemas.
//!
//! Cassandra's `CREATE TABLE` / `describe` output ALWAYS emits UDT column types
//! keyspace-qualified (e.g. `list<frozen<cassandra_easy_stress.addr>>`). Before
//! this fix the nom schema parser rejected the `.` in the qualified type name
//! (`identifier` stops at `.`), so every table with a UDT column failed to parse
//! and was unreadable via Flight `do_get`.
//!
//! These tests exercise the SAME public schema-parse surface the Flight
//! `get_schema` path uses — [`cqlite_core::schema::parse_cql_schema`] — plus the
//! authoritative DDL→registry resolver
//! [`cqlite_core::schema::udt_registry_from_cql`] / [`UdtRegistry::resolve_type`],
//! proving both the grammar fix AND that the parser and registry key qualified
//! UDT references IDENTICALLY (bare name + separate keyspace) so the column
//! actually resolves rather than silently missing (the #2349 degradation class).

use cqlite_core::schema::{parse_cql_schema, udt_registry_from_cql, CqlType};

fn data_type_of<'a>(schema: &'a cqlite_core::schema::TableSchema, col: &str) -> &'a str {
    schema
        .columns
        .iter()
        .find(|c| c.name == col)
        .unwrap_or_else(|| panic!("column {col} missing from parsed schema"))
        .data_type
        .as_str()
}

#[test]
fn qualified_udt_in_frozen_collection_parses() {
    let cql = "CREATE TABLE t (key text PRIMARY KEY, \
        u1 list<frozen<cassandra_easy_stress.addr>>)";
    let schema = parse_cql_schema(cql).expect("qualified-UDT DDL must parse (#2807)");
    assert_eq!(
        data_type_of(&schema, "u1"),
        "list<frozen<cassandra_easy_stress.addr>>"
    );
}

#[test]
fn qualified_scalar_udt_column_parses() {
    let cql = "CREATE TABLE t (key text PRIMARY KEY, a frozen<cassandra_easy_stress.addr>)";
    let schema = parse_cql_schema(cql).expect("qualified-UDT DDL must parse (#2807)");
    assert_eq!(
        data_type_of(&schema, "a"),
        "frozen<cassandra_easy_stress.addr>"
    );
}

/// The exact field-failure shape: a realistic `CREATE TABLE` as Cassandra's
/// `describe` emits it, with qualified UDTs in every collection position.
#[test]
fn full_create_table_with_qualified_udts_parses() {
    let cql = "CREATE TABLE cassandra_easy_stress.t (\
        key text, \
        u1 list<frozen<cassandra_easy_stress.addr>>, \
        u2 frozen<cassandra_easy_stress.addr>, \
        m map<text, frozen<cassandra_easy_stress.addr>>, \
        s set<frozen<cassandra_easy_stress.addr>>, \
        PRIMARY KEY (key));";
    let schema = parse_cql_schema(cql).expect("qualified-UDT DDL must parse (#2807)");
    assert_eq!(
        data_type_of(&schema, "u1"),
        "list<frozen<cassandra_easy_stress.addr>>"
    );
    assert_eq!(
        data_type_of(&schema, "u2"),
        "frozen<cassandra_easy_stress.addr>"
    );
    assert_eq!(
        data_type_of(&schema, "m"),
        "map<text, frozen<cassandra_easy_stress.addr>>"
    );
    assert_eq!(
        data_type_of(&schema, "s"),
        "set<frozen<cassandra_easy_stress.addr>>"
    );
}

/// Regression: the UNqualified UDT form must still parse unchanged.
#[test]
fn unqualified_udt_still_parses() {
    let cql = "CREATE TABLE t (key text PRIMARY KEY, a frozen<addr>, b addr)";
    let schema = parse_cql_schema(cql).expect("unqualified UDT must still parse");
    assert_eq!(data_type_of(&schema, "a"), "frozen<addr>");
    assert_eq!(data_type_of(&schema, "b"), "addr");
}

/// CONNECTION test: BOTH a keyspace-qualified `CREATE TYPE` AND a table whose UDT
/// columns reference it keyspace-qualified in every collection position — proving
/// the parser fix (which RETAINS the `keyspace.` prefix) and the registry (keyed
/// by bare name + separate keyspace) key types IDENTICALLY, so the column
/// actually resolves against the registry entry rather than silently missing.
#[test]
fn qualified_udt_column_resolves_against_registry_from_field_ddl() {
    let ks = "cassandra_easy_stress";
    let create_type = "CREATE TYPE cassandra_easy_stress.addr (street text, city text, zip int);";
    let create_table = "CREATE TABLE cassandra_easy_stress.t (\
        key text, \
        u1 list<frozen<cassandra_easy_stress.addr>>, \
        u2 frozen<cassandra_easy_stress.addr>, \
        m map<text, frozen<cassandra_easy_stress.addr>>, \
        s set<frozen<cassandra_easy_stress.addr>>, \
        PRIMARY KEY (key));";

    // Registry built from the CREATE TYPE (the Flight read path resolves a
    // ticket's DDL through this exact function).
    let registry = udt_registry_from_cql(create_type, ks);
    assert!(
        registry.contains_udt(ks, "addr"),
        "registry keyed by bare `addr` in keyspace `{ks}`"
    );

    // Table parsed through the PUBLIC schema-parse surface Flight uses.
    let schema = parse_cql_schema(create_table).expect("qualified-UDT DDL must parse (#2807)");

    // Every UDT column's declared type must resolve to a full struct (street,
    // city, zip) via the registry — not stay an opaque unresolved reference.
    for col_name in ["u1", "u2", "m", "s"] {
        let parsed = CqlType::parse(data_type_of(&schema, col_name))
            .unwrap_or_else(|e| panic!("column {col_name} type must parse: {e:?}"));
        let resolved = registry.resolve_type(&parsed, ks);
        let fields = find_resolved_addr(&resolved).unwrap_or_else(|| {
            panic!("{col_name}: `addr` did not resolve to a populated struct: {resolved:?}")
        });
        assert_eq!(fields.len(), 3, "{col_name}: addr has street+city+zip");
        assert_eq!(fields[0].0, "street", "{col_name}");
    }
}

/// Cassandra emits case-sensitive UDT names quoted (`frozen<ks."MyType">`). The
/// grammar supports quoted identifiers in the UDT type position; CREATE TYPE and
/// the column reference both normalize through the same quote-stripping
/// identifier, so they key identically and the column resolves.
#[test]
fn quoted_case_sensitive_qualified_udt_resolves_against_registry() {
    let ks = "ks";
    let registry = udt_registry_from_cql(r#"CREATE TYPE ks."MyAddr" (street text);"#, ks);
    assert!(
        registry.contains_udt(ks, "MyAddr"),
        "quoted CREATE TYPE keys the bare, case-preserved `MyAddr`"
    );

    let schema = parse_cql_schema(
        r#"CREATE TABLE ks.t (key text, a frozen<ks."MyAddr">, PRIMARY KEY (key));"#,
    )
    .expect("quoted qualified-UDT DDL must parse (#2807)");
    let resolved = registry.resolve_type(
        &CqlType::parse(data_type_of(&schema, "a")).expect("type must parse"),
        ks,
    );
    match &resolved {
        CqlType::Frozen(inner) => match inner.as_ref() {
            CqlType::Udt(name, fields) => {
                assert_eq!(name, "MyAddr", "case-preserved bare node name");
                assert_eq!(fields.len(), 1);
            }
            other => panic!("expected resolved Udt, got {other:?}"),
        },
        other => panic!("expected Frozen, got {other:?}"),
    }
}

/// Newly reachable via the grammar fix (#2807 addendum case a): a `CREATE TYPE`
/// whose FIELD type is itself keyspace-qualified previously FAILED to parse (so it
/// never registered); now it registers with a qualified field type that the NESTED
/// registry lookups must resolve through the shared splitter. This drives the
/// authoritative DDL→registry resolver over a two-level qualified UDT and asserts
/// the inner UDT fully materializes.
#[test]
fn nested_qualified_udt_field_type_resolves() {
    let ks = "cassandra_easy_stress";
    let ddl = "CREATE TYPE cassandra_easy_stress.inner (v text); \
        CREATE TYPE cassandra_easy_stress.outer (n frozen<cassandra_easy_stress.inner>);";
    let registry = udt_registry_from_cql(ddl, ks);
    assert!(registry.contains_udt(ks, "inner"));
    assert!(registry.contains_udt(ks, "outer"));

    let outer = CqlType::parse("frozen<cassandra_easy_stress.outer>").expect("outer type parses");
    let resolved = registry.resolve_type(&outer, ks);
    // frozen<outer> → outer{ n: frozen<inner> } → inner{ v: text }
    let outer_fields = match &resolved {
        CqlType::Frozen(i) => match i.as_ref() {
            CqlType::Udt(name, fields) => {
                assert_eq!(name, "outer");
                fields
            }
            other => panic!("expected outer Udt, got {other:?}"),
        },
        other => panic!("expected Frozen, got {other:?}"),
    };
    let (_, n_type) = outer_fields
        .iter()
        .find(|(n, _)| n == "n")
        .expect("field n");
    // The nested qualified field type must have resolved to the full inner struct.
    let inner_fields = match n_type {
        CqlType::Frozen(i) => match i.as_ref() {
            CqlType::Udt(name, fields) => {
                assert_eq!(name, "inner");
                fields
            }
            other => panic!("nested field must resolve to inner Udt, got {other:?}"),
        },
        CqlType::Udt(name, fields) => {
            assert_eq!(name, "inner");
            fields
        }
        other => panic!("nested field must resolve to inner Udt, got {other:?}"),
    };
    assert_eq!(inner_fields.len(), 1);
    assert_eq!(inner_fields[0].0, "v");
}

/// Newly reachable (#2807 addendum case b): a qualified frozen UDT in a
/// partition-KEY position. `parse_create_table` copies the column `data_type` into
/// the `KeyColumn`, so the qualifier must survive there too and resolve.
#[test]
fn qualified_udt_in_partition_key_position_parses_and_resolves() {
    let ks = "cassandra_easy_stress";
    let schema = parse_cql_schema(
        "CREATE TABLE cassandra_easy_stress.t (\
            id frozen<cassandra_easy_stress.addr>, v text, PRIMARY KEY (id));",
    )
    .expect("qualified UDT in key position must parse (#2807)");

    let pk = &schema.partition_keys[0];
    assert_eq!(pk.name, "id");
    assert_eq!(pk.data_type, "frozen<cassandra_easy_stress.addr>");

    let registry =
        udt_registry_from_cql("CREATE TYPE cassandra_easy_stress.addr (street text);", ks);
    let resolved =
        registry.resolve_type(&CqlType::parse(&pk.data_type).expect("key type parses"), ks);
    assert!(
        find_resolved_addr(&resolved).is_some(),
        "partition-key UDT must resolve to a struct, got {resolved:?}"
    );
}

/// Walk a (possibly collection-wrapped) resolved type and return the `addr` UDT's
/// populated fields, or `None` if it never resolved to a struct.
fn find_resolved_addr(ty: &CqlType) -> Option<&Vec<(String, CqlType)>> {
    match ty {
        CqlType::Udt(name, fields) if name == "addr" && !fields.is_empty() => Some(fields),
        CqlType::List(i) | CqlType::Set(i) | CqlType::Frozen(i) => find_resolved_addr(i),
        CqlType::Map(k, v) => find_resolved_addr(k).or_else(|| find_resolved_addr(v)),
        CqlType::Tuple(ts) => ts.iter().find_map(find_resolved_addr),
        _ => None,
    }
}
