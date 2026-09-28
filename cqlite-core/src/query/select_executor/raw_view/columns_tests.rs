//! Unit pins for the raw SSTable view's column contract — the
//! `#[cfg(test)] mod tests` of `columns.rs`, extracted VERBATIM to a sibling
//! file so the parent stays under the campsite-rule size threshold (epic
//! #1116 / #1135). Included via
//! `#[cfg(test)] #[path = "columns_tests.rs"] mod tests;`, so `super` is
//! still the `columns` module and every path resolves exactly as before.

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};

fn dropped_regular_col_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_tomb".to_string(),
        table: "dropped_regular_col".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "keep_col".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "drop_col".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

#[test]
fn strip_raw_view_suffix_recognizes_and_rejects() {
    assert_eq!(
        strip_raw_view_suffix("dropped_regular_col_raw_sstable_data"),
        Some("dropped_regular_col")
    );
    assert_eq!(strip_raw_view_suffix("dropped_regular_col"), None);
    // Bare suffix with no base name is refused, not treated as a
    // zero-length base table.
    assert_eq!(strip_raw_view_suffix("_raw_sstable_data"), None);
}

/// Pinned column-contract snapshot (issue #4222, spec's public-surface
/// requirement) for `test_tomb.dropped_regular_col_raw_sstable_data`. A
/// column rename/add/remove in [`raw_view_columns`] must show up here as a
/// diff, not be discovered downstream.
#[test]
fn dropped_regular_col_column_contract_snapshot() {
    let schema = dropped_regular_col_schema();
    let (columns, _metadata_names) =
        raw_view_columns(&schema).expect("no collision in this fixture");
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "pk",
            "ck",
            "keep_col",
            "keep_col_timestamp",
            "keep_col_ttl",
            "keep_col_local_deletion_time",
            "keep_col_tombstone",
            "drop_col",
            "drop_col_timestamp",
            "drop_col_ttl",
            "drop_col_local_deletion_time",
            "drop_col_tombstone",
            "row_timestamp",
            "row_ttl",
            "row_local_deletion_time",
            "row_tombstone",
            "row_deletion_timestamp",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "row_kind",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
            "sstable",
            "generation",
            "format",
            "position",
        ],
        "raw-view column contract changed unexpectedly — update this snapshot \
         deliberately if the change is intended (issue #4222 D7)"
    );
    // Positions must be dense/ordered (metadata.columns is positional).
    for (idx, col) in columns.iter().enumerate() {
        assert_eq!(col.position, idx);
    }
}

#[test]
fn key_columns_are_never_duplicated_as_metadata_quads() {
    let schema = dropped_regular_col_schema();
    let (columns, _metadata_names) =
        raw_view_columns(&schema).expect("no collision in this fixture");
    let pk_timestamp_present = columns.iter().any(|c| c.name == "pk_timestamp");
    assert!(
        !pk_timestamp_present,
        "a partition-key column must not get a per-cell metadata quad"
    );
}

/// Roborev finding (issue #4222, round 9): the returned `metadata_names`
/// set must contain EXACTLY the synthesized per-cell quad + row-level
/// quintet names — never a structural column (`pk`/`ck`/`keep_col`/
/// `drop_col` themselves), and never an ALWAYS-applicable column
/// (`generation`/`sstable`/`row_kind`/`partition_deletion_time`/etc,
/// `predicates.rs`'s separate `always_applicable_column_names` set).
#[test]
fn metadata_names_contains_exactly_the_synthesized_derivatives() {
    let schema = dropped_regular_col_schema();
    let (_columns, metadata_names) =
        raw_view_columns(&schema).expect("no collision in this fixture");
    let expected: std::collections::HashSet<String> = [
        "keep_col_timestamp",
        "keep_col_ttl",
        "keep_col_local_deletion_time",
        "keep_col_tombstone",
        "drop_col_timestamp",
        "drop_col_ttl",
        "drop_col_local_deletion_time",
        "drop_col_tombstone",
        "row_timestamp",
        "row_ttl",
        "row_local_deletion_time",
        "row_tombstone",
        "row_deletion_timestamp",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(metadata_names, expected);
    // Structural + always-applicable columns must NEVER appear here.
    for never in [
        "pk",
        "ck",
        "keep_col",
        "drop_col",
        "generation",
        "sstable",
        "format",
        "position",
        "row_kind",
        "partition_deletion_time",
        "partition_deletion_timestamp",
        "bound_inclusive",
        "range_deletion_time",
        "range_deletion_timestamp",
    ] {
        assert!(
            !metadata_names.contains(never),
            "'{never}' must never be classified as a metadata derivative"
        );
    }
}

/// Roborev finding (issue #4222, round 9) — the EXACT failure scenario
/// the finding cited: a REAL base column named `event_timestamp` with
/// NO sibling `event` column (so the collision guard never fires) must
/// NOT end up in `metadata_names` — it is a genuine structural data
/// column, never a synthesized derivative, no matter what its name
/// LOOKS like.
#[test]
fn a_real_column_that_merely_looks_like_a_metadata_derivative_is_never_classified_as_one() {
    let mut schema = dropped_regular_col_schema();
    schema.columns.push(Column {
        name: "event_timestamp".to_string(),
        data_type: "text".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    });
    let (columns, metadata_names) =
        raw_view_columns(&schema).expect("no collision — 'event_timestamp' has no sibling");
    assert!(
        columns.iter().any(|c| c.name == "event_timestamp"),
        "the real base column must still be present in the contract"
    );
    assert!(
        !metadata_names.contains("event_timestamp"),
        "REGRESSION: a real base column merely named like a metadata derivative must \
         NEVER be classified as one — it is a genuine structural data column"
    );
}

/// A base table whose real column name collides with a synthesized
/// metadata-column name must fail closed (design.md D8), never silently
/// clobber one of the two (roborev finding, issue #4222).
#[test]
fn colliding_base_column_name_fails_closed() {
    let mut schema = dropped_regular_col_schema();
    schema.columns.push(Column {
        name: "generation".to_string(),
        data_type: "int".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    });
    let err = raw_view_columns(&schema)
        .expect_err("a base column literally named 'generation' must be refused");
    assert!(
        matches!(err, Error::Schema(_)),
        "collision must surface as Error::Schema, got: {err:?}"
    );
}

/// A base column named `<other>_timestamp` alongside a plain `<other>`
/// column collides with that OTHER column's synthesized metadata quad.
#[test]
fn colliding_quad_suffix_fails_closed() {
    let mut schema = dropped_regular_col_schema();
    schema.columns.push(Column {
        name: "keep_col_timestamp".to_string(),
        data_type: "bigint".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    });
    let err = raw_view_columns(&schema)
        .expect_err("a base column literally named 'keep_col_timestamp' must be refused");
    assert!(matches!(err, Error::Schema(_)));
}

/// Roborev finding (issue #4222, round 5, correcting round 4's own
/// gap): a BARE lowercase UDT name — e.g. `address` — parses to
/// `CqlType::Custom("address")` with NO `udt:` prefix
/// (`cql_type_parser.rs`'s all-lowercase fallback arm), indistinguishable
/// at this call site from a genuinely unrecognized custom type. This
/// must fail closed (never silently default to "simple"), or a real
/// UDT column gets the WRONG metadata shape (the single-cell quad
/// instead of the `_complex_deletion` trio).
#[test]
fn bare_lowercase_udt_type_name_fails_closed_rather_than_simple() {
    let mut schema = dropped_regular_col_schema();
    schema.columns.push(Column {
        name: "addr".to_string(),
        data_type: "address".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    });
    let err = raw_view_columns(&schema).expect_err(
        "a bare lowercase UDT-shaped type name must be refused, never silently \
         classified as a simple type",
    );
    assert!(
        matches!(err, Error::Schema(_)),
        "must surface as Error::Schema, got: {err:?}"
    );
}

/// Sanity: the explicitly `udt:`-prefixed shape (a MIXED-case or
/// dotted UDT name, `cql_type_parser.rs`'s earlier branch) also fails
/// closed here — this call site has no UDT registry to consult either
/// way, so both `Custom(_)` shapes are refused identically.
#[test]
fn udt_prefixed_custom_type_also_fails_closed() {
    assert_eq!(
        try_is_complex_cql_type(&CqlType::Custom("udt:Address".to_string())),
        None
    );
    assert_eq!(
        try_is_complex_cql_type(&CqlType::Custom("address".to_string())),
        None
    );
    assert_eq!(try_is_complex_cql_type(&CqlType::Text), Some(false));
    assert_eq!(
        try_is_complex_cql_type(&CqlType::List(Box::new(CqlType::Int))),
        Some(true)
    );
}

/// Roborev finding (issue #4222, round 11 — F1): the declared-type
/// classifier must be [`CqlType::parse`] (the parser the doc comments
/// above actually cite), NOT `row_build.rs`'s `parse_cql_type_str`
/// (`parser::complex_types::ComplexTypeParser`). That parser has NO
/// `varint` arm at all and orders its `time` alternative BEFORE
/// `timeuuid`, so `"timeuuid"` parsed as `Time` with a trailing
/// `"uuid"` and was REJECTED, and `"varint"` fell through to
/// `Custom("varint")` — both of which this function turns into a hard
/// `Error::Schema`. The raw view was therefore entirely unusable for
/// any table carrying a `timeuuid`, `varint`, `vector<..>` or UDT
/// column (real committed fixtures do: `basic-types.cql` declares
/// `session_id TIMEUUID`; `issue-4114-vector-float.cql` declares
/// `vector<float, n>`), and the emitted message blamed UDT ambiguity
/// for what was really a primitive-parser gap.
#[test]
fn primitive_and_vector_types_classify_as_simple_never_fail_closed() {
    let mut schema = dropped_regular_col_schema();
    for (name, data_type) in [
        ("session_id", "timeuuid"),
        ("big_number", "varint"),
        ("embedding", "vector<float, 3>"),
        ("money", "decimal"),
        ("when", "time"),
    ] {
        schema.columns.push(Column {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
    }
    let (columns, metadata_names) = raw_view_columns(&schema).expect(
        "REGRESSION (F1): every one of these is an unambiguous SINGLE-CELL type — \
         the raw view must not refuse a table that declares one",
    );
    for name in ["session_id", "big_number", "embedding", "money", "when"] {
        // Each gets the single-cell QUAD, never the complex trio.
        for suffix in ["timestamp", "ttl", "local_deletion_time", "tombstone"] {
            let synthesized = format!("{name}_{suffix}");
            assert!(
                columns.iter().any(|c| c.name == synthesized),
                "'{name}' must be classified SIMPLE and get '{synthesized}'"
            );
            assert!(metadata_names.contains(&synthesized));
        }
        assert!(
            !columns
                .iter()
                .any(|c| c.name == format!("{name}_complex_deletion")),
            "'{name}' is single-cell — it must NOT get the complex-deletion trio"
        );
    }
}

/// The classifier's own unit-level contract, pinned directly on
/// [`CqlType::parse`] output (issue #4222, round 11 — F1): `timeuuid`
/// and `varint` are real primitives, not `Custom(_)`.
#[test]
fn cql_type_parse_resolves_the_types_the_weaker_parser_missed() {
    assert_eq!(CqlType::parse("timeuuid").ok(), Some(CqlType::TimeUuid));
    assert_eq!(CqlType::parse("varint").ok(), Some(CqlType::Varint));
    assert_eq!(
        try_is_complex_cql_type(&CqlType::parse("timeuuid").expect("primitive")),
        Some(false)
    );
    assert_eq!(
        try_is_complex_cql_type(&CqlType::parse("varint").expect("primitive")),
        Some(false)
    );
}
