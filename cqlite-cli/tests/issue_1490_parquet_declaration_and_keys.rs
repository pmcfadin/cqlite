//! Two ground-truth properties of the Parquet↔JSONL parity harness — issue
//! #1490 (AD1) round 6, epic #1469.
//!
//! Both were FALSE PASSES / FALSE FAILS at the ROOT of the comparison chain, and
//! both are about the same thing: the harness derives every expectation from a
//! case's DECLARED CQL type, so the declaration and the way a declared type is
//! applied are load-bearing in a way no value comparison can check.
//!
//! # 1. The declaration is verified against the committed schema
//!
//! `arrow_expect` deliberately derives the expected Arrow type from the case's
//! declared CQL type rather than from CQLite's mapping (reading the expectation
//! out of the code under test proves nothing — #3041). But the declaration was
//! HAND-COPIED into the case and never checked against `test-data/schemas/*.cql`:
//! a declaration that drifted to match a wrong export mapping made the Arrow type
//! check AND the value comparison both pass. `schema_fixture` closes that, and
//! the controls below prove it REDS — a check that has never been shown to fail
//! is not evidence of anything.
//!
//! # 2. A primary-KEY component is typed from its full declared type
//!
//! `sstabledump` writes every key component as a quoted STRING (Cassandra's
//! `AbstractType.getString`), and only the INTEGRAL family was ever coerced back.
//! A declared `boolean`/`float`/`double`/`decimal` key therefore stayed `Text`
//! against the Arrow side's typed value — a false primary-key difference on every
//! row of such a table — and the numeric pass ran over `cells` but not over
//! `keys`, so the two halves of one component could disagree with each other.
//!
//! No corpus table declares such a key today (the highest-value expansion targets
//! that do are enumerated in `issue_1490_parquet_jsonl_parity.rs`), so these tests
//! drive `golden_rows::project_golden` over a synthetic sstabledump-shaped golden.
//! That is legitimate here and NOT a #3042 self-consistency round trip: the
//! expectation is never CQLite's output — it is either Cassandra's documented
//! rendering (`Boolean.toString`, `Float.toString`, `BigDecimal.toString`) or the
//! ARROW-side constructor the export's own values pass through
//! (`decimal::exact_from_decimal128`, `f32 as f64`).

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use std::path::Path;

use parquet_parity::canonical_jsonl::{
    parse_document_str_with_keys, CanonicalValue, KeySpec, NormalizedFloat,
};
use parquet_parity::cql_type::{parse_column, ColumnType};
use parquet_parity::decimal::{exact_from_decimal128, exact_from_text, EXPORT_DECIMAL_SCALE};
use parquet_parity::golden_rows::{canonicalize_key_component, project_golden};
use parquet_parity::schema_fixture;
use parquet_parity::{ParityCase, SchemaCheck};

// ===========================================================================
// 1. The declared columns / types / keys vs the COMMITTED schema
// ===========================================================================

/// `test_da.simple_table` exactly as `da-test.cql` declares it — the baseline
/// every control below perturbs by ONE thing.
const TRUE_COLUMNS: &[(&str, &str)] = &[
    ("id", "uuid"),
    ("name", "text"),
    ("age", "int"),
    ("salary", "bigint"),
    ("active", "boolean"),
    ("created", "timestamp"),
];

const fn checked_case(
    columns: &'static [(&'static str, &'static str)],
    partition_key: &'static [&'static str],
    clustering: &'static [&'static str],
    udts: &'static [&'static str],
) -> ParityCase {
    ParityCase {
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test.cql",
        udts,
        columns,
        partition_key,
        clustering,
        schema_check: SchemaCheck::Committed,
        must_run: true,
        covers: "CONTROL for the committed-schema declaration check",
        known_gap: None,
        known_type_gaps: &[],
    }
}

/// The baseline must PASS, or every control below would red for the wrong
/// reason.
#[test]
fn the_true_declaration_validates_against_the_committed_schema() {
    const CASE: ParityCase = checked_case(TRUE_COLUMNS, &["id"], &[], &[]);
    let verified = schema_fixture::validate_declaration(&CASE)
        .expect("test_da.simple_table's real declaration must agree with da-test.cql");
    assert_eq!(
        verified,
        TRUE_COLUMNS.len(),
        "every declared column's type must have been compared, not skipped"
    );
}

/// THE round-6 BLOCKER: a declared TYPE that drifts from the schema must red,
/// naming the column and both types.
///
/// `age` is really `INT`; declaring it `bigint` is exactly the drift that would
/// otherwise make a wrong `int64` export mapping compare as CORRECT.
#[test]
fn a_declared_type_that_drifts_from_the_committed_schema_reds() {
    const DRIFTED: &[(&str, &str)] = &[
        ("id", "uuid"),
        ("name", "text"),
        ("age", "bigint"),
        ("salary", "bigint"),
        ("active", "boolean"),
        ("created", "timestamp"),
    ];
    const CASE: ParityCase = checked_case(DRIFTED, &["id"], &[], &[]);
    let err = schema_fixture::validate_declaration(&CASE)
        .expect_err("a declared type that disagrees with the committed schema MUST red");
    assert!(
        err.contains("column 'age'")
            && err.contains("declares 'bigint'")
            && err.contains("schema declares 'INT'"),
        "the refusal must name the column and BOTH types: {err}"
    );
}

/// A case that OMITS a schema column must red: an undeclared column is an
/// uncompared column, and the Parquet column-set check cannot see it (the export
/// derives its columns from the same schema, so both sides would agree on a
/// shorter list only if the case is the one that shrank).
#[test]
fn an_omitted_schema_column_reds() {
    const SHORT: &[(&str, &str)] = &[("id", "uuid"), ("name", "text")];
    const CASE: ParityCase = checked_case(SHORT, &["id"], &[], &[]);
    let err =
        schema_fixture::validate_declaration(&CASE).expect_err("an omitted schema column MUST red");
    assert!(
        err.contains("column 'age' is declared by the schema but NOT by the case")
            && err.contains("uncompared column"),
        "{err}"
    );
}

/// A column the schema does not declare must red too — the other direction of
/// the same drift.
#[test]
fn a_column_the_schema_does_not_declare_reds() {
    const EXTRA: &[(&str, &str)] = &[
        ("id", "uuid"),
        ("name", "text"),
        ("age", "int"),
        ("salary", "bigint"),
        ("active", "boolean"),
        ("created", "timestamp"),
        ("invented", "text"),
    ];
    const CASE: ParityCase = checked_case(EXTRA, &["id"], &[], &[]);
    let err = schema_fixture::validate_declaration(&CASE)
        .expect_err("a column the schema does not declare MUST red");
    assert!(
        err.contains("column 'invented' is declared by the case but NOT by the schema"),
        "{err}"
    );
}

/// A drifted KEY definition must red. Key definitions are the other half of the
/// round-6 finding: they decide which golden array a component is read from and
/// in which ORDER, so a wrong one silently re-pairs every row.
#[test]
fn a_drifted_partition_key_reds() {
    const CASE: ParityCase = checked_case(TRUE_COLUMNS, &["name"], &[], &[]);
    let err = schema_fixture::validate_declaration(&CASE)
        .expect_err("a partition key the schema does not declare MUST red");
    assert!(
        err.contains("partition key")
            && err.contains("[\"name\"]")
            && err.contains("[\"id\"]")
            && err.contains("ORDER is significant"),
        "{err}"
    );
}

/// An INVENTED clustering key must red: `test_da.simple_table` has none, and a
/// case claiming one would read a clustering component that is not there.
#[test]
fn a_drifted_clustering_key_reds() {
    const CASE: ParityCase = checked_case(TRUE_COLUMNS, &["id"], &["name"], &[]);
    let err = schema_fixture::validate_declaration(&CASE)
        .expect_err("a clustering key the schema does not declare MUST red");
    assert!(
        err.contains("clustering key") && err.contains("[\"name\"]"),
        "{err}"
    );
}

/// A declared UDT the schema never declares must red: `cql_type::parse_column`
/// accepts any bare identifier the case lists as a UDT, so an invented name
/// silently turns a typo into a "UDT column".
#[test]
fn a_declared_udt_the_schema_does_not_declare_reds() {
    const CASE: ParityCase = checked_case(TRUE_COLUMNS, &["id"], &[], &["no_such_udt"]);
    let err = schema_fixture::validate_declaration(&CASE)
        .expect_err("a UDT name the committed schema does not declare MUST red");
    assert!(
        err.contains("declared UDT 'no_such_udt' is not a CREATE TYPE"),
        "{err}"
    );
}

/// A case naming a table its schema does not declare must red — never a silently
/// skipped check.
#[test]
fn a_table_the_committed_schema_does_not_declare_reds() {
    const CASE: ParityCase = ParityCase {
        keyspace: "test_da",
        table: "no_such_table",
        schema: "da-test.cql",
        udts: &[],
        columns: &[("id", "uuid")],
        partition_key: &["id"],
        clustering: &[],
        schema_check: SchemaCheck::Committed,
        must_run: true,
        covers: "CONTROL: a table the committed schema does not declare",
        known_gap: None,
        known_type_gaps: &[],
    };
    let err = schema_fixture::validate_declaration(&CASE)
        .expect_err("a table the schema does not declare MUST red");
    assert!(
        err.contains("does not declare this table") && err.contains("simple_table"),
        "the refusal must name what the schema DOES declare: {err}"
    );
}

/// The check is wired into the pipeline every case runs through, not merely
/// available: a drifted declaration must fail `run_case`, before any stage.
#[test]
fn the_schema_check_is_wired_into_the_case_pipeline() {
    const DRIFTED: &[(&str, &str)] = &[
        ("id", "uuid"),
        ("name", "text"),
        ("age", "bigint"),
        ("salary", "bigint"),
        ("active", "boolean"),
        ("created", "timestamp"),
    ];
    const CASE: ParityCase = checked_case(DRIFTED, &["id"], &[], &[]);
    let err = parquet_parity::run_case(&CASE)
        .err()
        .map(|f| f.to_string())
        .expect("a drifted declaration must fail the CASE, not only the helper");
    assert!(
        err.contains("DISAGREES with its committed schema") && err.contains("column 'age'"),
        "{err}"
    );
}

/// The opt-out works AND is scoped: a `Synthetic` control's mis-declaration
/// passes the schema check and still reds on the ARROW TYPE check it exists for.
///
/// Both halves matter. Without the first, the deliberate-misdeclaration controls
/// could not run at all; without the second, `Synthetic` would be a way to
/// disable the harness rather than one check of it.
#[test]
fn a_synthetic_control_opts_out_of_the_schema_check_only() {
    const DRIFTED: &[(&str, &str)] = &[
        ("id", "uuid"),
        ("name", "text"),
        ("age", "bigint"),
        ("salary", "bigint"),
        ("active", "boolean"),
        ("created", "timestamp"),
    ];
    const CASE: ParityCase = ParityCase {
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test.cql",
        udts: &[],
        columns: DRIFTED,
        partition_key: &["id"],
        clustering: &[],
        schema_check: SchemaCheck::Synthetic {
            why: "CONTROL: mis-declares `age` on purpose, exactly as the Arrow-type controls do",
        },
        must_run: true,
        covers: "CONTROL: the Synthetic opt-out is scoped to the schema check",
        known_gap: None,
        known_type_gaps: &[],
    };
    let err = parquet_parity::prepare(&CASE)
        .err()
        .map(|f| f.to_string())
        .expect("the mis-declared Arrow type must still red");
    assert!(
        err.contains("Arrow type mismatch for column 'age'"),
        "the opt-out must let the case reach the TYPE check: {err}"
    );
    assert!(
        !err.contains("DISAGREES with its committed schema"),
        "an explicit Synthetic opt-out must suppress the SCHEMA check: {err}"
    );
}

// ---------------------------------------------------------------------------
// The CENSUS: every real case's declaration is verified, and none opts out
// ---------------------------------------------------------------------------

/// Every real case's hand-copied declaration agrees with its committed CQL
/// schema — columns, TYPES and key definitions.
///
/// The per-case pipeline enforces this on every run (`run_stages` stage zero);
/// this census additionally REPORTS the count and asserts that NO real case has
/// opted out — a `SchemaCheck::Synthetic` opt-out belongs to the
/// deliberate-misdeclaration controls only, and must never spread to a real
/// table, which is how an opt-out becomes the new default.
///
/// It needs no fixture: the schemas are committed source.
#[test]
fn every_real_case_declaration_matches_its_committed_schema() {
    assert_eq!(
        parquet_parity::cases::REAL_CASES.len(),
        10,
        "the census must cover every real case this lane declares"
    );
    let mut columns_verified = 0usize;
    for case in parquet_parity::cases::REAL_CASES {
        assert_eq!(
            case.schema_check,
            SchemaCheck::Committed,
            "{}: a REAL case must never opt out of the schema check",
            case.id()
        );
        let verified = schema_fixture::validate_declaration(case).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            verified,
            case.columns.len(),
            "{}: every declared column's type must have been compared",
            case.id()
        );
        columns_verified += verified;
    }
    eprintln!(
        "[declaration census] {}/{} real cases validated against their committed schema \
         ({columns_verified} column types + their key definitions); 0 opted out",
        parquet_parity::cases::REAL_CASES.len(),
        parquet_parity::cases::REAL_CASES.len()
    );
}

// ---------------------------------------------------------------------------
// The schema PARSER itself — the grammar the corpus actually uses
// ---------------------------------------------------------------------------

/// A composite partition key, a multi-component clustering key, `STATIC`, an
/// inline `PRIMARY KEY`, keyspace qualification and case/whitespace folding, all
/// on one synthetic fixture. Each of these appears in a committed schema.
#[test]
fn the_schema_parser_reads_the_grammar_the_corpus_uses() {
    const TEXT: &str = r#"
-- a comment mentioning a ';' and a 'quoted -- dash'
CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy'};
USE ks;
CREATE TYPE IF NOT EXISTS person (first_name text, age int);
CREATE TABLE IF NOT EXISTS inline (
    id UUID PRIMARY KEY,
    props MAP<TEXT, FROZEN<SET<INT>>>
) WITH compression = {'class': 'LZ4Compressor'};
CREATE TABLE composite (
    tenant TEXT,
    user_id UUID,
    bucket INT,
    item TIMEUUID,
    label TEXT STATIC,
    PRIMARY KEY ((tenant, user_id), bucket, item)
) WITH clustering ORDER BY (bucket DESC, item ASC);
CREATE TABLE other_ks.elsewhere (k INT PRIMARY KEY, v TEXT);
"#;
    let parsed = schema_fixture::parse(TEXT).expect("the grammar above must parse");

    assert!(parsed.udts.contains("person"), "{:?}", parsed.udts);

    let inline = parsed.table("ks", "inline").expect("inline table");
    assert_eq!(inline.partition_key, vec!["id".to_string()]);
    assert!(inline.clustering.is_empty());
    assert_eq!(
        schema_fixture::normalize_type(&inline.columns[1].cql_type),
        "map<text,frozen<set<int>>>",
        "a collection type's internal commas must not split the column list"
    );

    let composite = parsed
        .table("KS", "COMPOSITE")
        .expect("case-insensitive lookup");
    assert_eq!(
        composite.partition_key,
        vec!["tenant".to_string(), "user_id".to_string()],
        "a (( … )) partition key must keep BOTH components, in order"
    );
    assert_eq!(
        composite.clustering,
        vec!["bucket".to_string(), "item".to_string()],
        "clustering order comes from the PRIMARY KEY clause, not CLUSTERING ORDER BY"
    );
    assert!(
        composite
            .columns
            .iter()
            .any(|c| c.name == "label" && c.is_static),
        "a STATIC column must parse, with its type text intact: {:?}",
        composite.columns
    );

    assert!(
        parsed.table("other_ks", "elsewhere").is_some(),
        "a keyspace-qualified CREATE TABLE must not be filed under the USE keyspace"
    );
    assert!(parsed.table("ks", "elsewhere").is_none());
}

/// A `CREATE TABLE` the parser cannot make sense of is an ERROR, never a table
/// that quietly does not exist — otherwise the whole check degrades into "the
/// schema does not declare this table", which reads like a case defect.
#[test]
fn the_schema_parser_refuses_a_table_it_cannot_parse() {
    for (text, expect) in [
        ("USE ks; CREATE TABLE t (id INT, v TEXT);", "no PRIMARY KEY"),
        (
            "USE ks; CREATE TABLE t (id INT PRIMARY KEY, PRIMARY KEY (id));",
            "both a PRIMARY KEY clause and an inline PRIMARY KEY",
        ),
        (
            "USE ks; CREATE TABLE t (id INT, PRIMARY KEY (ghost));",
            "which is not one of its columns",
        ),
        (
            "CREATE TABLE t (id INT PRIMARY KEY);",
            "is not keyspace-qualified",
        ),
        ("USE ks; CREATE TABLE t (id);", "has no type"),
    ] {
        let err = schema_fixture::parse(text)
            .err()
            .unwrap_or_else(|| panic!("must refuse: {text}"));
        assert!(err.contains(expect), "expected {expect:?} in: {err}");
    }
}

// ===========================================================================
// 2. Primary-KEY components are typed from their full declared type
// ===========================================================================

fn col(name: &str, declared: &str) -> ColumnType {
    parse_column(name, declared, &[]).expect("declared type must parse")
}

/// A declared `boolean` key is `Bool`, not the string sstabledump wrote.
#[test]
fn a_boolean_key_component_becomes_a_bool() {
    for (raw, expected) in [("true", true), ("false", false)] {
        let got = canonicalize_key_component(
            &CanonicalValue::Text(raw.to_string()),
            &col("flag", "boolean"),
        )
        .expect("a legal boolean rendering must convert");
        assert_eq!(
            got,
            CanonicalValue::Bool(expected),
            "sstabledump writes a boolean key through Boolean.toString, and the Arrow side \
             holds Bool — a Text here is a false primary-key difference on every row"
        );
    }
}

/// A declared `float` key lands on the SAME double the Arrow `Float32` column
/// decodes to — the narrowing rule a `float` CELL already went through.
#[test]
fn a_float_key_component_narrows_to_32_bits() {
    let got = canonicalize_key_component(
        &CanonicalValue::Text("1.84".to_string()),
        &col("px", "float"),
    )
    .expect("a float key must convert");
    // The right-hand side is the ARROW side's own value: `arrow_rows` reads a
    // Float32 array element and widens it, which is exactly `1.84f32 as f64`.
    assert_eq!(got, CanonicalValue::Float(NormalizedFloat(1.84f32 as f64)));
    assert_ne!(
        got,
        CanonicalValue::Float(NormalizedFloat(1.84f64)),
        "the un-narrowed double is a DIFFERENT value, which is why the narrowing is not cosmetic"
    );
}

/// A declared `double` key keeps full 64-bit precision (no narrowing).
#[test]
fn a_double_key_component_keeps_64_bits() {
    let got = canonicalize_key_component(
        &CanonicalValue::Text("1014.5449131979983".to_string()),
        &col("v", "double"),
    )
    .expect("a double key must convert");
    assert_eq!(
        got,
        CanonicalValue::Float(NormalizedFloat(1014.5449131979983f64))
    );
}

/// A declared `decimal` key becomes the EXACT decimal — and specifically the same
/// canonical form the exported `Decimal128(38, 9)` cell lands on.
#[test]
fn a_decimal_key_component_becomes_the_exact_arrow_side_decimal() {
    let got = canonicalize_key_component(
        &CanonicalValue::Text("10576.6".to_string()),
        &col("amount", "decimal"),
    )
    .expect("a decimal key must convert");
    // Built the ARROW way: unscaled 10576.6 × 10^9 at the export's fixed scale.
    let arrow_side = exact_from_decimal128(10_576_600_000_000, 9, "arrow decimal")
        .expect("the export's own scale must be representable")
        .canonical();
    assert_eq!(
        got, arrow_side,
        "the golden key and the exported cell must land on ONE canonical decimal"
    );
    assert_ne!(
        got,
        CanonicalValue::Text("10576.6".to_string()),
        "an untyped decimal key stays a bare string and can never equal the exported value"
    );
}

/// An integral key stays `Int` whether or not the loader's `KeySpec` already
/// coerced it — so the two paths cannot disagree.
#[test]
fn an_integral_key_component_is_an_int_either_way() {
    for raw in [
        CanonicalValue::Text("-42".to_string()),
        CanonicalValue::Int(-42),
    ] {
        let got = canonicalize_key_component(&raw, &col("pk", "int"))
            .expect("an integral key must convert");
        assert_eq!(got, CanonicalValue::Int(-42));
    }
}

/// A declared `text` key that SPELLS a timestamp stays `Text` — the round-5
/// no-heuristics rule, re-asserted through the single key entry point so a
/// refactor cannot drop it.
#[test]
fn a_text_key_component_that_spells_a_timestamp_stays_text() {
    let raw = "2025-10-06 01:12:07.265Z";
    let got = canonicalize_key_component(
        &CanonicalValue::Timestamp {
            micros: 1_759_713_127_265_000,
            raw: raw.to_string(),
        },
        &col("k", "text"),
    )
    .expect("a text key must convert");
    assert_eq!(got, CanonicalValue::Text(raw.to_string()));
}

/// A value that does not DENOTE its declared key type is a REFUSAL, never a
/// silent fallback to text: comparing it as a string would hide both a broken
/// fixture and a broken declaration.
#[test]
fn a_key_component_that_contradicts_its_declared_type_is_refused() {
    for (declared, raw, needle) in [
        ("boolean", "maybe", "'true' or 'false'"),
        ("float", "not-a-number", "a finite decimal number"),
        ("int", "12x", "an integer"),
        ("decimal", "1e9", "not a plain decimal literal"),
    ] {
        let err =
            canonicalize_key_component(&CanonicalValue::Text(raw.to_string()), &col("k", declared))
                .err()
                .unwrap_or_else(|| panic!("a {declared} key rendered {raw:?} must be REFUSED"));
        assert!(
            err.contains(needle) && err.contains(raw),
            "the refusal must name the value and what it is not: {err}"
        );
    }
}

/// A `decimal` key literal with more fractional digits than the export's fixed
/// scale is refused, not truncated.
#[test]
fn a_decimal_key_beyond_the_export_scale_is_refused() {
    let err = exact_from_text("0.1234567891", EXPORT_DECIMAL_SCALE, "ctx")
        .expect_err("10 fractional digits exceed the export's scale 9");
    assert!(
        err.contains("refuses to compare rather than truncate"),
        "{err}"
    );
}

/// END TO END: a golden whose PRIMARY KEY is `boolean` + `float` + `decimal`
/// projects typed key components, and `keys` and `cells` hold the IDENTICAL
/// value for each of them.
///
/// The second half is the round-6 finding's other leg: the numeric pass used to
/// run over `cells` only, so a valid key could produce a false primary-key
/// difference (the sort key disagreeing with the cell) while its cell compared
/// equal.
#[test]
fn project_golden_types_key_components_and_keys_agree_with_cells() {
    // sstabledump's shape, with every key component a quoted string — Cassandra
    // serializes them through `AbstractType.getString` (measured in the committed
    // corpus: `test_comp.lz4_table`'s golden carries `"key":["1"]` for `pk int`).
    // ONE JSON object per LINE, as sstabledump writes it — the loader is a JSONL
    // reader, so this must not be pretty-printed across lines.
    const GOLDEN: &str = concat!(
        r#"{"partition":{"key":["true"]},"rows":[{"type":"row","#,
        r#""clustering":["1.84","10576.6"],"#,
        r#""liveness_info":{"tstamp":"2021-01-01T00:00:00Z"},"#,
        r#""cells":[{"name":"v","value":"payload"}]}]}"#
    );

    let columns = vec![
        col("flag", "boolean"),
        col("px", "float"),
        col("amount", "decimal"),
        col("v", "text"),
    ];
    let keys = KeySpec::from_cql_types(&["boolean"], &["float", "decimal"]);
    let doc = parse_document_str_with_keys(GOLDEN, Path::new("<synthetic>"), true, &keys)
        .expect("the synthetic golden must parse");

    let rows = project_golden(&doc, &columns, &["flag"], &["px", "amount"])
        .expect("the golden must project");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    let expected_decimal = exact_from_decimal128(10_576_600_000_000, 9, "arrow decimal")
        .expect("scale 9 is the export's own")
        .canonical();
    assert_eq!(
        row.keys,
        vec![
            CanonicalValue::Bool(true),
            CanonicalValue::Float(NormalizedFloat(1.84f32 as f64)),
            expected_decimal.clone(),
        ],
        "every key component must be typed by its declared type"
    );

    for (name, expected) in [
        ("flag", CanonicalValue::Bool(true)),
        ("px", CanonicalValue::Float(NormalizedFloat(1.84f32 as f64))),
        ("amount", expected_decimal),
    ] {
        assert_eq!(
            row.cells.get(name),
            Some(&expected),
            "the CELL for key column '{name}' must hold the same canonicalized value the sort \
             key does, or a valid key produces a false primary-key difference"
        );
    }
    assert_eq!(
        row.cells.get("v"),
        Some(&CanonicalValue::Text("payload".to_string()))
    );
}

/// A key component that contradicts its declared type fails the PROJECTION,
/// naming the row and the key column — so the refusal is reachable through the
/// path a real case uses, not only through the helper.
#[test]
fn project_golden_refuses_a_key_component_that_contradicts_its_type() {
    const GOLDEN: &str = concat!(
        r#"{"partition":{"key":["maybe"]},"rows":[{"type":"row","#,
        r#""clustering":[],"cells":[{"name":"v","value":"x"}]}]}"#
    );
    let columns = vec![col("flag", "boolean"), col("v", "text")];
    let doc = parse_document_str_with_keys(
        GOLDEN,
        Path::new("<synthetic>"),
        true,
        &KeySpec::from_cql_types(&["boolean"], &[]),
    )
    .expect("the synthetic golden must parse");

    let err = project_golden(&doc, &columns, &["flag"], &[])
        .expect_err("a boolean key rendered \"maybe\" MUST fail the projection");
    assert!(
        err.contains("key column 'flag'") && err.contains("'true' or 'false'"),
        "{err}"
    );
}
