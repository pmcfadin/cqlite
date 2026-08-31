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
use parquet_parity::declared::{canonicalize_arrow_decimal, canonicalize_golden, Declared};
use parquet_parity::golden_rows::project_golden;
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

/// ONE primary-key component, through THE declared-type-guided entry point
/// (`declared.rs`) at the `PrimaryKey` position — the same call
/// `golden_rows::project_golden` makes, so these tests cannot pass against a
/// path the harness does not use.
fn key_component(raw: &CanonicalValue, col: &ColumnType) -> Result<CanonicalValue, String> {
    canonicalize_golden(
        raw.clone(),
        &Declared::primary_key(&col.spec, format!("key column '{}'", col.name)),
    )
}

/// ONE multicell collection PATH component (a set element, or a map entry's
/// key), at the `CollectionPath` position — stringified by Cassandra exactly as
/// a key component is, and therefore converted by the same declared-scalar
/// rules (issue #1490 round 7).
/// The raw value is built the way the loader builds it — `CanonicalValue::from_json`
/// over the JSON STRING sstabledump writes in `path` — so these tests exercise
/// the same input the harness sees, including the shared parser's one
/// value-level guess (a `Z`-suffixed spelling becomes a `Timestamp`).
fn path_component(raw: &str, elem: &ColumnType) -> Result<CanonicalValue, String> {
    canonicalize_golden(
        CanonicalValue::from_json(&serde_json::Value::String(raw.to_string())),
        &Declared::collection_path(&elem.spec, format!("collection '{}' path", elem.name)),
    )
}

/// A declared `boolean` key is `Bool`, not the string sstabledump wrote.
#[test]
fn a_boolean_key_component_becomes_a_bool() {
    for (raw, expected) in [("true", true), ("false", false)] {
        let got = key_component(
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
    let got = key_component(
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
    let got = key_component(
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
    let got = key_component(
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
        let got = key_component(&raw, &col("pk", "int")).expect("an integral key must convert");
        assert_eq!(got, CanonicalValue::Int(-42));
    }
}

/// A declared `text` key that SPELLS a timestamp stays `Text` — the round-5
/// no-heuristics rule, re-asserted through the single key entry point so a
/// refactor cannot drop it.
#[test]
fn a_text_key_component_that_spells_a_timestamp_stays_text() {
    let raw = "2025-10-06 01:12:07.265Z";
    let got = key_component(
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
        let err = key_component(&CanonicalValue::Text(raw.to_string()), &col("k", declared))
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

// ===========================================================================
// 3. A multicell collection PATH component is typed the same way (round 7)
//
// sstabledump emits one cell PER ELEMENT of a non-frozen collection, and the
// element (a set) or the entry KEY (a map) arrives in the cell's `path` as a
// STRINGIFIED value — Cassandra's `AbstractType.getString`, exactly as for a
// primary-key component. Only the INTEGRAL family was ever converted back, so a
// declared `set<float>`, `set<double>`, `set<decimal>` or boolean-keyed map
// compared Cassandra's stringified text against the Arrow side's typed value: a
// FALSE parity failure on documented expansion targets (`signed_special_collections`,
// #3578). Both positions now route through the ONE declared-type-guided entry
// point (`declared.rs`), so they cannot diverge again.
//
// The expectations are Cassandra's documented renderings or the ARROW side's own
// constructor, never CQLite's output (#3041/#3042).
// ===========================================================================

/// A declared `boolean` collection element/key is `Bool`, not `Text`.
#[test]
fn a_boolean_path_component_becomes_a_bool() {
    for (raw, expected) in [("true", true), ("false", false)] {
        assert_eq!(
            path_component(raw, &col("flag", "boolean")).expect("a legal boolean must convert"),
            CanonicalValue::Bool(expected),
            "a boolean-keyed collection's path is written through Boolean.toString, and the \
             Arrow side holds Bool"
        );
    }
}

/// A declared `float` element narrows to 32 bits — the SAME rule a `float` cell
/// and a `float` key component go through, so all three land on one value.
#[test]
fn a_float_path_component_narrows_to_32_bits() {
    let got = path_component("1.84", &col("px", "float")).expect("a float element must convert");
    // The right-hand side is the ARROW side's own value: `Float32` widened.
    assert_eq!(got, CanonicalValue::Float(NormalizedFloat(1.84f32 as f64)));
    assert_ne!(
        got,
        CanonicalValue::Float(NormalizedFloat(1.84f64)),
        "the un-narrowed double is a DIFFERENT value"
    );
    // `double` keeps full precision.
    assert_eq!(
        path_component("1014.5449131979983", &col("v", "double")).expect("a double element"),
        CanonicalValue::Float(NormalizedFloat(1014.5449131979983f64))
    );
}

/// A declared `decimal` element becomes the EXACT decimal the exported
/// `Decimal128(38, 9)` cell lands on.
#[test]
fn a_decimal_path_component_becomes_the_exact_arrow_side_decimal() {
    let got =
        path_component("10576.6", &col("amount", "decimal")).expect("a decimal element converts");
    let arrow_side = exact_from_decimal128(10_576_600_000_000, 9, "arrow decimal")
        .expect("the export's own scale must be representable")
        .canonical();
    assert_eq!(
        got, arrow_side,
        "the golden element and the exported cell must land on ONE canonical decimal"
    );
    assert_ne!(
        got,
        CanonicalValue::Text("10576.6".to_string()),
        "an untyped decimal element stays a bare string and can never equal the exported value"
    );
}

/// The integral conversion that ALREADY worked must keep working, and a declared
/// `text` element must NOT be coerced — a `set<text>` holding `"5"` can never
/// compare equal to a `set<int>` holding `5`.
#[test]
fn an_integral_path_component_converts_and_a_text_one_does_not() {
    assert_eq!(
        path_component("-2", &col("e", "int")).expect("integral element"),
        CanonicalValue::Int(-2)
    );
    assert_eq!(
        path_component("5", &col("e", "text")).expect("text element"),
        CanonicalValue::Text("5".to_string()),
        "coercing a text element would let a set<text> compare equal to a set<int>"
    );
}

/// A declared `text` element that SPELLS a timestamp stays `Text` — the round-5
/// no-heuristics rule, now reaching the PATH position too (it did not before:
/// a non-integral path was returned exactly as the shared parser had guessed it).
#[test]
fn a_text_path_component_that_spells_a_timestamp_stays_text() {
    let raw = "2025-10-06 01:12:07.265Z";
    assert_eq!(
        path_component(raw, &col("e", "text")).expect("text element"),
        CanonicalValue::Text(raw.to_string())
    );
    // …and a declared `timestamp` element still compares as an INSTANT.
    let got = path_component(raw, &col("e", "timestamp")).expect("timestamp element");
    assert!(
        matches!(got, CanonicalValue::Timestamp { micros, .. } if micros == 1_759_713_127_265_000),
        "a declared timestamp element must compare as an instant, got {got:?}"
    );
}

/// A path component that does not DENOTE its declared element type is a
/// REFUSAL, never a silent fallback to text — the same rule as for a key
/// component, because it is now the same code.
#[test]
fn a_path_component_that_contradicts_its_declared_type_is_refused() {
    for (declared, raw, needle) in [
        ("boolean", "maybe", "'true' or 'false'"),
        ("float", "not-a-number", "a finite decimal number"),
        ("int", "12x", "an integer"),
        ("decimal", "1e9", "not a plain decimal literal"),
    ] {
        let err = path_component(raw, &col("e", declared))
            .err()
            .unwrap_or_else(|| panic!("a {declared} element rendered {raw:?} must be REFUSED"));
        assert!(
            err.contains(needle) && err.contains(raw),
            "the refusal must name the value and what it is not: {err}"
        );
    }
}

/// END TO END: a golden carrying a non-frozen `set<float>` and a
/// `map<boolean,decimal>` projects TYPED elements and TYPED entry keys.
#[test]
fn project_golden_types_multicell_collection_paths() {
    // sstabledump's multicell shape: one cell per element, the element (or the
    // map entry's key) in `path`, stringified.
    const GOLDEN: &str = concat!(
        r#"{"partition":{"key":["1"]},"rows":[{"type":"row","clustering":[],"#,
        r#""liveness_info":{"tstamp":"2021-01-01T00:00:00Z"},"cells":["#,
        r#"{"name":"s","path":["1.84"],"value":""},"#,
        r#"{"name":"m","path":["true"],"value":10576.6}"#,
        r#"]}]}"#
    );

    let columns = vec![
        col("id", "int"),
        col("s", "set<float>"),
        col("m", "map<boolean, decimal>"),
    ];
    // Through `preserve_exact_lexemes` first, exactly as `mod.rs::load_golden`
    // does: the `map<boolean,decimal>` entry VALUE is a bare JSON number, and
    // the harness reads a decimal from its LITERAL — a decimal that reaches the
    // comparison as a double is refused (round 10, section 5).
    let golden = preserve_exact_lexemes(GOLDEN, &columns).expect("the rewrite must succeed");
    let doc = parse_document_str_with_keys(
        &golden,
        Path::new("<synthetic>"),
        true,
        &KeySpec::from_cql_types(&["int"], &[]),
    )
    .expect("the synthetic golden must parse");

    let rows = project_golden(&doc, &columns, &["id"], &[]).expect("the golden must project");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    assert_eq!(
        row.cells.get("s"),
        Some(&CanonicalValue::List(vec![CanonicalValue::Float(
            NormalizedFloat(1.84f32 as f64)
        )])),
        "a set<float> element arrives as the stringified path '1.84' and must land on the \
         SAME double the Arrow Float32 column decodes to"
    );
    let expected_decimal = exact_from_decimal128(10_576_600_000_000, 9, "arrow decimal")
        .expect("scale 9 is the export's own")
        .canonical();
    assert_eq!(
        row.cells.get("m"),
        Some(&CanonicalValue::Map(vec![(
            CanonicalValue::Bool(true),
            expected_decimal
        )])),
        "a map<boolean,decimal> entry's KEY arrives as the stringified path 'true' and its \
         VALUE as a typed JSON number"
    );
}

// ===========================================================================
// 4b. A `Decimal128` cell is decoded from its DECLARED type (round 7)
//
// Every scale-zero `Decimal128` used to canonicalize as an `Int`, on the grounds
// that scale 0 is `varint`'s mapping. But `arrow_expect` accepts
// `Decimal128(p, s)` for ANY `s >= 0` for a declared `decimal` — deliberately,
// since Arrow carries ONE scale per COLUMN — so a valid `decimal` column
// exported at scale 0 passed the TYPE check and then compared `Int(n)` against
// the golden's exact `decimal(n)`: a false VALUE failure on correct data.
// ===========================================================================

/// A scale-ZERO `Decimal128` cell of a declared `decimal` column canonicalizes
/// as an exact DECIMAL, and equals the golden's whole-valued literal.
#[test]
fn a_scale_zero_decimal_cell_compares_as_a_decimal() {
    let decimal = col("d", "decimal");
    for whole in [0i128, 1, -1, 42, -31_595, 1i128 << 60] {
        let exported = canonicalize_arrow_decimal(
            whole,
            0,
            &Declared::cell(&decimal.spec, "scale-zero decimal cell"),
        )
        .expect("a scale-zero decimal must canonicalize");
        // The GOLDEN side: sstabledump writes a whole decimal as a JSON integer.
        let golden = canonicalize_golden(
            CanonicalValue::Int(whole),
            &Declared::cell(&decimal.spec, "golden decimal cell"),
        )
        .expect("a whole golden decimal is exact");
        assert_eq!(
            exported, golden,
            "a decimal exported at scale 0 must compare EQUAL to the golden's whole literal \
             {whole}, not as Int({whole}) against decimal({whole})"
        );
        assert_eq!(exported, CanonicalValue::Text(format!("decimal({whole})")));
        // And it is NOT the integer form — the mapping the old rule produced.
        assert_ne!(exported, CanonicalValue::Int(whole));
    }
}

/// A declared `varint` stays an INTEGER domain on both sides at scale 0 — the
/// distinction the declared type exists to make.
#[test]
fn a_scale_zero_varint_cell_stays_an_integer() {
    let varint = col("v", "varint");
    let exported = canonicalize_arrow_decimal(7, 0, &Declared::cell(&varint.spec, "varint cell"))
        .expect("varint");
    assert_eq!(exported, CanonicalValue::Int(7));
    assert_eq!(
        exported,
        canonicalize_golden(
            CanonicalValue::Int(7),
            &Declared::cell(&varint.spec, "golden varint cell")
        )
        .expect("a varint golden literal is already exact"),
        "a varint must NOT be converted to a decimal on either side"
    );
    // A `varint` is pinned to scale 0 by `arrow_expect`; a scaled one is a
    // refusal rather than a rescale.
    let err = canonicalize_arrow_decimal(7, 2, &Declared::cell(&varint.spec, "varint cell"))
        .expect_err("a scaled varint must be refused");
    assert!(err.contains("INTEGER domain"), "{err}");
}

/// Without a declared SCALAR the `Decimal128` decode REFUSES: scale zero is both
/// a `varint` and a whole-valued `decimal`, so there is nothing to decide it by
/// and guessing is what round 7 found.
#[test]
fn a_decimal_cell_without_a_declared_scalar_is_refused() {
    let collection = col("s", "set<int>");
    let err = canonicalize_arrow_decimal(
        7,
        0,
        &Declared::cell(&collection.spec, "decimal under a collection declaration"),
    )
    .expect_err("an ambiguous Decimal128 must be refused, never guessed");
    assert!(
        err.contains("ambiguous without the declared type") && err.contains("refuses"),
        "{err}"
    );
}

// ===========================================================================
// 5. A decimal CELL's LITERAL survives the parse (round 10)
//
// The FALSE PASS this section exists for: rounds 4–9 canonicalized a decimal
// CELL by RECOVERING it from the `f64` the shared comparator produced (render at
// the export scale, then check neither one-unit neighbour parses to the same
// double, and treat a unique answer as exact). That cannot work in principle:
// `0.100000000000000001` and `0.1` are the SAME `f64`, so the recovery rendered
// `0.1`, found both neighbours rounding elsewhere, declared itself EXACT — and
// canonicalized an eighteen-digit golden literal as `0.1`, so a lossy export
// writing `0.1` compared EQUAL. Once a value is an `f64` the distinguishing
// digits are gone and no probing brings them back.
//
// So the literal's TEXT is kept at deserialization time, before the shared
// parser sees it (`golden_text::preserve_exact_lexemes`, called by
// `mod.rs::load_golden`), every golden decimal is read by
// `decimal::exact_from_text`, and a declared `decimal` that still arrives as a
// double is REFUSED. The two halves are coupled deliberately:
// `an_unrewritten_decimal_cell_is_refused_end_to_end` pins that dropping the
// rewrite REDS every decimal table instead of quietly restoring the recovery.
// `project_golden_types_multicell_collection_paths` (section 3) goes through the
// same rewrite, for the same reason.
// ===========================================================================

use parquet_parity::golden_text::preserve_exact_lexemes;

/// A golden decimal CELL, through THE declared-type door at the `Cell` position.
fn decimal_cell_golden(raw: CanonicalValue) -> Result<CanonicalValue, String> {
    let decimal = col("d", "decimal");
    canonicalize_golden(raw, &Declared::cell(&decimal.spec, "decimal cell"))
}

/// The same cell as the EXPORT writes it: `Decimal128(38, scale)`.
fn decimal_cell_arrow(unscaled: i128, scale: i8) -> Result<CanonicalValue, String> {
    let decimal = col("d", "decimal");
    canonicalize_arrow_decimal(
        unscaled,
        scale,
        &Declared::cell(&decimal.spec, "exported decimal cell"),
    )
}

/// A decimal cell's literal as the harness receives it once preserved.
fn decimal_literal(literal: &str) -> CanonicalValue {
    CanonicalValue::Text(literal.to_string())
}

/// THE ROUND-10 CONTROL: two decimal literals that are the SAME `f64` must stay
/// DISTINCT, and neither may be canonicalized as the other.
///
/// The collision is ASSERTED, not assumed, so the test cannot silently stop
/// being about a collision — which is what let the hole through: the previous
/// control used a literal whose extra digit still survived float conversion.
#[test]
fn f64_colliding_decimal_literals_stay_distinct() {
    // Premise: these two literals ARE one double, so a double-mediated golden
    // side cannot distinguish them however it probes.
    let long = "0.100000000000000001";
    let short = "0.1";
    assert_eq!(
        long.parse::<f64>().expect("f64").to_bits(),
        short.parse::<f64>().expect("f64").to_bits(),
        "premise of this control: the two literals collide under f64"
    );

    assert_eq!(
        decimal_cell_golden(decimal_literal(short)).expect("0.1 is representable at scale 9"),
        decimal_cell_arrow(100_000_000, 9).expect("scale-9 decimal"),
        "0.1 must compare exactly against the exported cell for 0.1"
    );

    // The eighteen-digit literal carries more fractional digits than the
    // export's fixed scale can hold, so it is REFUSED — comparing it would mean
    // comparing two different numbers. A refusal is a loud non-answer; the false
    // PASS (canonicalizing it as 0.1) is what is gone.
    let err = decimal_cell_golden(decimal_literal(long)).expect_err(
        "a literal beyond the export's fixed scale must be refused, never canonicalized as 0.1",
    );
    assert!(
        err.contains("fractional digits"),
        "the refusal must name the precision it cannot carry, got: {err}"
    );
    assert!(
        !err.contains("0.1)"),
        "the refusal must not have canonicalized the literal as 0.1, got: {err}"
    );

    // A collision the export CAN carry: both literals are inside the fixed
    // scale-9 precision, they are one unit apart, and they are the same double —
    // so these are COMPARED, and compared distinct, rather than refused.
    let (a, b) = ("9007199.254740001", "9007199.254740002");
    assert_eq!(
        a.parse::<f64>().expect("f64").to_bits(),
        b.parse::<f64>().expect("f64").to_bits(),
        "premise: two scale-9 literals inside the export scale that collide under f64"
    );
    let a_exact = decimal_cell_golden(decimal_literal(a)).expect("scale-9 literal");
    let b_exact = decimal_cell_golden(decimal_literal(b)).expect("scale-9 literal");
    assert_ne!(
        a_exact, b_exact,
        "two distinct decimals sharing one double must stay distinct"
    );
    assert_eq!(
        a_exact,
        decimal_cell_arrow(9_007_199_254_740_001, 9).expect("scale-9 decimal")
    );
    assert_eq!(
        b_exact,
        decimal_cell_arrow(9_007_199_254_740_002, 9).expect("scale-9 decimal"),
        "and each must equal the exported cell for its OWN value"
    );
}

/// A golden decimal that arrives as a DOUBLE — its literal lost — is REFUSED by
/// name, whatever the double is.
#[test]
fn a_golden_decimal_that_lost_its_literal_is_refused() {
    for double in [0.1f64, 31_595.67, 0.0, -0.0, f64::NAN, f64::INFINITY] {
        let err = decimal_cell_golden(CanonicalValue::Float(NormalizedFloat(double)))
            .expect_err("a decimal that arrives as a double must be refused");
        assert!(
            err.contains("LITERAL TEXT was lost"),
            "the refusal must name the cause, got: {err}"
        );
    }

    // A declared `double` column is NOT affected: the exact-bit float comparison
    // is a different contract and stays exactly as it was.
    let double = col("f", "double");
    assert_eq!(
        canonicalize_golden(
            CanonicalValue::Float(NormalizedFloat(31_595.67)),
            &Declared::cell(&double.spec, "double cell"),
        )
        .expect("a double column compares as a double"),
        CanonicalValue::Float(NormalizedFloat(31_595.67))
    );
}

/// The rewrite quotes ONLY the positions declared `decimal`/`varint`, and only
/// their numbers.
#[test]
fn the_rewrite_preserves_exact_lexemes_and_nothing_else() {
    let columns = vec![
        col("balance", "decimal"),
        col("big", "varint"),
        col("rate", "double"),
        col("tags", "frozen<set<decimal>>"),
        col("note", "text"),
    ];

    let line = concat!(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","clustering":[],"cells":["#,
        r#"{"name":"balance","value":0.100000000000000001},"#,
        r#"{"name":"big","value":123456789012345678901234567890},"#,
        r#"{"name":"rate","value":1014.5449131979983},"#,
        r#"{"name":"tags","value":[1.5,2.25]},"#,
        r#"{"name":"note","value":"0.1"}]}]}"#,
        "\n"
    );
    let rewritten = preserve_exact_lexemes(line, &columns).expect("the rewrite must succeed");

    assert!(
        rewritten.contains(r#"{"name":"balance","value":"0.100000000000000001"}"#),
        "the decimal literal must survive verbatim, quoted: {rewritten}"
    );
    assert!(
        rewritten.contains(r#"{"name":"big","value":"123456789012345678901234567890"}"#),
        "a varint literal beyond u64::MAX must survive verbatim too: {rewritten}"
    );
    assert!(
        rewritten.contains(r#"{"name":"tags","value":["1.5","2.25"]}"#),
        "a collection of decimals is quoted element-wise: {rewritten}"
    );
    assert!(
        rewritten.contains(r#"{"name":"rate","value":1014.5449131979983}"#),
        "a `double` column's literal must reach serde_json's exact parser untouched, so the \
         exact-bit float comparison is unchanged: {rewritten}"
    );
    assert!(
        rewritten.contains(r#"{"name":"note","value":"0.1"}"#),
        "a text cell is untouched: {rewritten}"
    );

    // With NO declared column the document is JSON-equivalent to the original —
    // the rewrite cannot change anything else about a golden.
    let untouched = preserve_exact_lexemes(line, &[]).expect("no-op rewrite");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(untouched.trim()).expect("valid JSON"),
        serde_json::from_str::<serde_json::Value>(line.trim()).expect("valid JSON"),
        "a golden with no declared decimal must survive the rewrite unchanged"
    );
}

/// ROUND-11 FINDING 1: the preservation is per POSITION, not per COLUMN.
///
/// A column whose declared type MENTIONS a `decimal` used to have every number
/// in its value quoted, so a `map<decimal,int>` turned its `int` VALUES into
/// strings — a false parity failure on ordinary correct data. The decision is
/// now taken at each position from that position's declared type, by the same
/// `declared.rs` recursion that canonicalizes the value. Both map shapes are
/// covered, because they reach the harness differently: a NON-frozen
/// `map<decimal,int>` arrives as one cell per entry (key in the stringified
/// `path`, value a bare JSON number), a FROZEN one as a single JSON object (keys
/// already strings). And a `list<int>` in the same row must be untouched.
#[test]
fn the_rewrite_leaves_non_decimal_positions_of_a_decimal_bearing_column_alone() {
    const GOLDEN: &str = concat!(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","clustering":[],"#,
        r#""liveness_info":{"tstamp":"2021-01-01T00:00:00Z"},"cells":["#,
        r#"{"name":"m","path":["1.5"],"value":3},"#,
        r#"{"name":"fm","value":{"1.5":3}},"#,
        r#"{"name":"l","value":[1,2]},"#,
        r#"{"name":"t","value":[7,2.25]},"#,
        r#"{"name":"d","value":2.25}"#,
        r#"]}]}"#,
        "\n"
    );
    let columns = vec![
        col("id", "text"),
        col("m", "map<decimal, int>"),
        col("fm", "frozen<map<decimal, int>>"),
        col("l", "frozen<list<int>>"),
        col("t", "frozen<tuple<int, decimal>>"),
        col("d", "decimal"),
    ];

    let rewritten = preserve_exact_lexemes(GOLDEN, &columns).expect("the rewrite must succeed");

    // The exact defect: the map's declared-`int` VALUES stay JSON numbers.
    assert!(
        rewritten.contains(r#"{"name":"m","path":["1.5"],"value":3}"#),
        "a map<decimal,int> entry VALUE is declared `int` and must NOT be quoted: {rewritten}"
    );
    assert!(
        rewritten.contains(r#"{"name":"fm","value":{"1.5":3}}"#),
        "a frozen map<decimal,int>'s value is declared `int` and its key is already a JSON \
         string, so NOTHING here changes: {rewritten}"
    );
    assert!(
        rewritten.contains(r#"{"name":"l","value":[1,2]}"#),
        "a list<int> in a decimal-bearing table must be untouched: {rewritten}"
    );
    // A tuple is positional: member 0 is `int`, member 1 is `decimal`.
    assert!(
        rewritten.contains(r#"{"name":"t","value":[7,"2.25"]}"#),
        "only the tuple member DECLARED decimal is quoted: {rewritten}"
    );
    assert!(
        rewritten.contains(r#"{"name":"d","value":"2.25"}"#),
        "the scalar decimal cell is quoted: {rewritten}"
    );

    // And end to end: the values compare as the declared types say they should.
    let doc = parse_document_str_with_keys(
        &rewritten,
        Path::new("<synthetic>"),
        true,
        &KeySpec::from_cql_types(&["text"], &[]),
    )
    .expect("the rewritten golden must parse");
    let rows = project_golden(&doc, &columns, &["id"], &[]).expect("the golden must project");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    let d_1_5 = exact_from_text("1.5", EXPORT_DECIMAL_SCALE, "expected")
        .expect("1.5 parses")
        .canonical();
    let d_2_25 = exact_from_text("2.25", EXPORT_DECIMAL_SCALE, "expected")
        .expect("2.25 parses")
        .canonical();

    for name in ["m", "fm"] {
        assert_eq!(
            row.cells.get(name),
            Some(&CanonicalValue::Map(vec![(
                d_1_5.clone(),
                CanonicalValue::Int(3)
            )])),
            "column '{name}': the decimal KEY is exact and the int VALUE stays an integer — \
             quoting it would compare Text(\"3\") against the Arrow Int32 3"
        );
    }
    assert_eq!(
        row.cells.get("l"),
        Some(&CanonicalValue::List(vec![
            CanonicalValue::Int(1),
            CanonicalValue::Int(2)
        ]))
    );
    assert_eq!(
        row.cells.get("t"),
        Some(&CanonicalValue::List(vec![
            CanonicalValue::Int(7),
            d_2_25.clone()
        ]))
    );
    assert_eq!(row.cells.get("d"), Some(&d_2_25));
}

/// ROUND-11 FINDING 2: a `varint` above `u64::MAX` survives EXACTLY.
///
/// `serde_json` parses such a literal as an `f64` (both `as_i64` and `as_u64`
/// fail), while the export writes the column as an exact `Decimal128(38, 0)`
/// that reads back as an `Int` — a `Float`-vs-`Int` false mismatch with the
/// distinguishing digits already lost. Kept by the same position-precise
/// mechanism as a `decimal`.
#[test]
fn a_varint_above_u64_max_is_preserved_and_compares_exactly() {
    // 30 digits: far above u64::MAX, and inside the 38-digit unscaled range of
    // the Decimal128 the export writes a `varint` to.
    const LITERAL: &str = "123456789012345678901234567890";
    const VALUE: i128 = 123_456_789_012_345_678_901_234_567_890;

    // Premise, ASSERTED so this control cannot silently stop being about the
    // case it exists for: serde_json really does lose this literal to an f64.
    let bare: serde_json::Value = serde_json::from_str(LITERAL).expect("valid JSON number");
    let n = bare.as_number().expect("a number");
    assert!(
        n.as_i64().is_none() && n.as_u64().is_none() && n.as_f64().is_some(),
        "premise: a varint above u64::MAX is parsed by serde_json as an f64"
    );

    let golden = format!(
        concat!(
            r#"{{"partition":{{"key":["k"]}},"rows":[{{"type":"row","clustering":[],"#,
            r#""cells":[{{"name":"v","value":{}}}]}}]}}"#,
            "\n"
        ),
        LITERAL
    );
    let columns = vec![col("id", "text"), col("v", "varint")];
    let keys = KeySpec::from_cql_types(&["text"], &[]);

    // WITH the rewrite: the literal is read exactly and equals the exported cell.
    let rewritten = preserve_exact_lexemes(&golden, &columns).expect("the rewrite must succeed");
    assert!(
        rewritten.contains(&format!(r#""value":"{LITERAL}""#)),
        "the varint literal must survive verbatim, quoted: {rewritten}"
    );
    let doc = parse_document_str_with_keys(&rewritten, Path::new("<synthetic>"), true, &keys)
        .expect("the rewritten golden must parse");
    let rows = project_golden(&doc, &columns, &["id"], &[]).expect("the golden must project");
    assert_eq!(rows.len(), 1);

    let varint = col("v", "varint");
    let exported = canonicalize_arrow_decimal(
        VALUE,
        0,
        &Declared::cell(&varint.spec, "exported varint cell"),
    )
    .expect("a varint exports as Decimal128 scale 0");
    assert_eq!(exported, CanonicalValue::Int(VALUE));
    assert_eq!(
        rows[0].cells.get("v"),
        Some(&exported),
        "the preserved varint literal must compare exactly against the exported cell"
    );

    // WITHOUT it: the cell reaches the harness as a double and the projection
    // REFUSES — the coupling that makes the preservation non-optional.
    let doc = parse_document_str_with_keys(&golden, Path::new("<synthetic>"), true, &keys)
        .expect("the raw golden must parse");
    let err = project_golden(&doc, &columns, &["id"], &[])
        .expect_err("a varint whose literal was lost must be refused");
    assert!(
        err.contains("LITERAL TEXT was lost"),
        "the refusal must name the cause, got: {err}"
    );

    // A varint that fits an i64 is unaffected: same `Int`, before and after.
    let small = concat!(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","clustering":[],"#,
        r#""cells":[{"name":"v","value":-42}]}]}"#,
        "\n"
    );
    let rewritten = preserve_exact_lexemes(small, &columns).expect("the rewrite must succeed");
    let doc = parse_document_str_with_keys(&rewritten, Path::new("<synthetic>"), true, &keys)
        .expect("parse");
    let rows = project_golden(&doc, &columns, &["id"], &[]).expect("project");
    assert_eq!(rows[0].cells.get("v"), Some(&CanonicalValue::Int(-42)));
}

/// ROUND-12 FINDING: a NESTED object spelled like a cell is NOT a cell.
///
/// The walker this replaced decided "is this an sstabledump cell?" from an
/// object's SHAPE — any object, at any depth, carrying a `"name"` string that
/// matched a declared column. So a frozen map or a UDT field spelled
/// `{"name":"amount","value":…}` was rewritten per the unrelated `amount`
/// COLUMN's declaration, and the oracle itself was corrupted: the value the
/// golden carried is not the value the comparison then saw. Structure is now
/// resolved by `serde_json` against the document's own shape, so a cell is a
/// member of a row's `cells` array and nothing else.
#[test]
fn a_nested_object_named_like_a_decimal_column_is_not_a_cell() {
    // `amount` is a declared `decimal` column, so the walker's shape test fired
    // on every one of these — all ORDINARY VALUES of other columns: a
    // `map<text,int>` holding the keys "name" and "value", a UDT whose FIELDS
    // are called `name` and `value`, and the same object one level deeper.
    const GOLDEN: &str = concat!(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","clustering":[],"cells":["#,
        r#"{"name":"amount","value":2.5},"#,
        r#"{"name":"tally","value":{"name":"amount","value":7}},"#,
        r#"{"name":"who","value":{"name":"amount","value":7}},"#,
        r#"{"name":"nested","value":[{"name":"amount","value":7}]}"#,
        r#"]}]}"#,
        "\n"
    );
    let columns = vec![
        col("id", "text"),
        col("amount", "decimal"),
        col("tally", "frozen<map<text, int>>"),
        parse_column("who", "frozen<person>", &["person"]).expect("declared UDT"),
        col("nested", "frozen<list<frozen<map<text, int>>>>"),
    ];

    let rewritten = preserve_exact_lexemes(GOLDEN, &columns).expect("the rewrite must succeed");

    // The ONLY textual change a golden may undergo is the quoting of a
    // declared-decimal/varint literal — so the whole document, byte for byte,
    // must be the original with the one real decimal CELL quoted and NOTHING
    // else. A byte census rather than a set of `contains` probes: a probe can
    // only find the look-alikes someone already thought of, and the walker's
    // defect was reaching spellings nobody had.
    assert!(
        rewritten.contains(r#"{"name":"amount","value":"2.5"}"#),
        "the declared-decimal CELL must still be preserved: {rewritten}"
    );
    assert_eq!(
        rewritten.replace(r#""value":"2.5""#, r#""value":2.5"#),
        GOLDEN,
        "a nested object spelled like the `amount` cell must NOT be rewritten per the \
         `amount` column — that corrupts the oracle"
    );
}

/// END TO END through the REAL load path: the rewrite, the shared parser and
/// `project_golden` together must keep a decimal cell's literal exact — and
/// WITHOUT the rewrite the same golden must be REFUSED, never compared.
#[test]
fn an_unrewritten_decimal_cell_is_refused_end_to_end() {
    const GOLDEN: &str = concat!(
        r#"{"partition":{"key":["k"]},"rows":[{"type":"row","clustering":[],"#,
        r#""cells":[{"name":"balance","value":0.1}]}]}"#,
        "\n"
    );
    let columns = vec![col("id", "text"), col("balance", "decimal")];
    let keys = KeySpec::from_cql_types(&["text"], &[]);

    // WITH the rewrite: the literal is read exactly and equals the exported cell.
    let rewritten = preserve_exact_lexemes(GOLDEN, &columns).expect("the rewrite must succeed");
    let doc = parse_document_str_with_keys(&rewritten, Path::new("<synthetic>"), true, &keys)
        .expect("the rewritten golden must parse");
    let rows = project_golden(&doc, &columns, &["id"], &[]).expect("the golden must project");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cells.get("balance"),
        Some(&decimal_cell_arrow(100_000_000, 9).expect("scale-9 decimal")),
        "the preserved literal must compare exactly against the exported cell"
    );

    // WITHOUT it: the cell reaches the harness as a double and the projection
    // REFUSES. This is the coupling that makes the preservation non-optional —
    // a future edit that drops it REDS every decimal table.
    let doc = parse_document_str_with_keys(GOLDEN, Path::new("<synthetic>"), true, &keys)
        .expect("the raw golden must parse");
    let err = project_golden(&doc, &columns, &["id"], &[])
        .expect_err("a decimal cell whose literal was lost must be refused");
    assert!(
        err.contains("LITERAL TEXT was lost"),
        "the refusal must name the cause, got: {err}"
    );
}
