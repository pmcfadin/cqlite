//! Vortex export, per CQL type family (issue #4237, R2.2 / R2.3).
//!
//! R2.2 requires EVERY type family to export. The 33-table sweep in
//! `issue_4237_vortex_parquet_differential.rs` carries no tuple, UDT-as-struct or varint column, so
//! this file runs the SAME `diff_pair` logic (column set both directions, column order, per-field
//! `logical_field` incl. extension metadata, order-sensitive values with an explicit null marker —
//! see `support/vortex_differential.rs`) on real Cassandra-written fixtures that carry them:
//!
//! | family                     | fixture                                                    | provenance |
//! |----------------------------|------------------------------------------------------------|------------|
//! | top-level tuple            | `test_types.cx_tuple_field_order` (`cql-type-parity.cql`)  | fetched bundle |
//! | UDT as struct (+ nested tuple, varint, decimal, duration, timeuuid fields) | `test_udt_wide_fields.udt_wide_fields` (`issue-3722-udt-wide-fields.cql`) | committed `test-data/fixtures/issue_3722` |
//! | varint                     | `test_varint.varint_regular` (`issue-4360-varint-column.cql`; ids 1..=13 in range, 40-digit rows refused identically) | committed `test-data/fixtures/issue_4360` |
//! | UDTs nested in tuple/collection keys | `test_nested_udt_keys.nested_udt_keys` (`nested-udt-keys.cql`) | fetched bundle |
//! | duration (R2.3)            | `test_basic.simple_table.duration_val`                     | fetched bundle |
//!
//! The tuple fixture already existed (written by Cassandra 5.0 for #1007), so none was generated.
//!
//! Fixture roots are resolved per TABLE (`sstables_root_for_table`, #3220). A committed fixture is
//! `must_run` (a missing one FAILS regardless of env); a bundle-only table fails closed under
//! `CQLITE_REQUIRE_FIXTURES=1` and otherwise skips cleanly. Assertions are per case — there is no
//! suite-wide `ran > 0`.

#![cfg(all(feature = "state_machine", feature = "vortex"))]

#[path = "../../cqlite-core/tests/support/datasets_root.rs"]
mod datasets_root;
#[path = "support/vortex_differential.rs"]
mod vortex_differential;

use arrow::datatypes::DataType;
use std::path::PathBuf;
use vortex_differential::*;

fn strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn schema_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
        .join(name)
}

/// Where a case's SSTables live.
#[derive(Clone, Copy)]
enum Fixture {
    /// Committed under `test-data/fixtures/<dir>` (itself an sstables root): `must_run`, a missing
    /// Data.db FAILS regardless of env.
    Committed(&'static str),
    /// In the fetched dataset bundle, resolved per table by `sstables_root_for_table` (#3220):
    /// fails closed under `CQLITE_REQUIRE_FIXTURES=1`, else a clean skip.
    Bundle,
}

/// Resolve `<ks>.<table>`'s `sstables/` root, or `None` for a clean skip. Panics (fail closed)
/// when a `must_run` / strict case cannot find its Data.db.
fn root_or_skip(ks: &str, table: &str, fixture: Fixture) -> Option<PathBuf> {
    let (root, committed) = match fixture {
        Fixture::Committed(dir) => {
            let root = datasets_root::repo_root()
                .join("test-data/fixtures")
                .join(dir);
            (
                datasets_root::table_has_data(&root, ks, table).then_some(root),
                true,
            )
        }
        Fixture::Bundle => (datasets_root::sstables_root_for_table(ks, table), false),
    };
    if root.is_none() {
        assert!(
            !(committed || strict()),
            "{ks}.{table}: no Data.db found ({}) and the case is {} — \
             fetch with bash test-data/scripts/fetch-datasets.sh",
            datasets_root::describe_search(ks, table),
            if committed {
                "a committed must_run fixture"
            } else {
                "required by CQLITE_REQUIRE_FIXTURES=1"
            }
        );
        eprintln!("{ks}.{table}: bundle fixture absent, skipping (non-strict)");
    }
    root
}

/// Export `<ks>.<table>` to both formats, assert full agreement, return the pair for the caller's
/// family-specific assertions.
fn export_and_diff(
    ks: &str,
    table: &str,
    schema: &str,
    fixture: Fixture,
    where_clause: Option<&str>,
) -> Option<ExportedPair> {
    let root = root_or_skip(ks, table, fixture)?;
    let pair = export_both(&root, ks, table, &schema_file(schema), where_clause)
        .unwrap_or_else(|e| panic!("{e}"));
    let rows = diff_pair(&pair).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        rows > 0,
        "{ks}.{table}: 0 rows compared — a 0-row export of a fixture that carries data is a failure"
    );
    Some(pair)
}

fn field<'a>(
    schema: &'a arrow::datatypes::Schema,
    name: &str,
    which: &str,
) -> &'a arrow::datatypes::Field {
    schema
        .field_with_name(name)
        .unwrap_or_else(|_| panic!("{which}: column {name} missing from {schema:?}"))
}

fn assert_struct_in_both(pair: &ExportedPair, column: &str, expected_fields: &[&str]) {
    for (which, schema) in [
        ("parquet", pair.parquet_schema.as_ref()),
        ("vortex", pair.vortex_schema.as_ref()),
    ] {
        let f = field(schema, column, which);
        let DataType::Struct(children) = f.data_type() else {
            panic!(
                "{which}: {column} must be a Struct, got {:?}",
                f.data_type()
            );
        };
        let names: Vec<&str> = children.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(
            names, expected_fields,
            "{which}: {column} struct field names/order"
        );
    }
}

/// R2.2 — top-level `tuple<int, text, boolean>` exports as a positional Struct in both formats,
/// null middle element included (golden: `[9,null,false]`).
#[test]
fn r2_2_tuple_exports_as_struct_and_matches_parquet() {
    let Some(pair) = export_and_diff(
        "test_types",
        "cx_tuple_field_order",
        "cql-type-parity.cql",
        Fixture::Bundle,
        None,
    ) else {
        return;
    };
    assert_struct_in_both(&pair, "t", &["field_0", "field_1", "field_2"]);
    // The golden has two rows (ck 1: [7,"seven",true], ck 2: [9,null,false]).
    assert_eq!(
        total_rows(&pair.vortex_batches),
        2,
        "cx_tuple_field_order row count"
    );
    let values = column_values_as_strings(&pair.vortex_batches, "t");
    assert_eq!(values.len(), 2);
    assert!(
        values.iter().any(|v| v.contains("seven")) && values.iter().any(|v| v.contains('9')),
        "tuple values must carry the golden elements, got {values:?}"
    );
}

/// R2.2 — a frozen UDT column exports as a Struct (field order = the UDT declaration), and its
/// nested tuple / varint / decimal / duration / timeuuid fields survive; the table also carries
/// UDTs inside maps / sets (`mw`, `fmw`, `fsw`, `sw`).
#[test]
#[ignore = "BLOCKED, not Vortex-specific: SELECT * query metadata types a top-level UDT column as \
            Custom(\"wide\") (no UdtRegistry reaches ColumnInfo.cql_type), so BOTH the Parquet and \
            the Vortex export fail with `column 'w': expected Blob value, got Udt(..)`"]
fn r2_2_udt_exports_as_struct_and_matches_parquet() {
    let Some(pair) = export_and_diff(
        "test_udt_wide_fields",
        "udt_wide_fields",
        "issue-3722-udt-wide-fields.cql",
        Fixture::Committed("issue_3722"),
        None,
    ) else {
        return;
    };
    assert_struct_in_both(
        &pair,
        "w",
        &[
            "s", "t", "d", "vi", "tm", "tu", "du", "dt", "ip", "fl", "fs", "fm", "tp", "nu", "fu",
            "bl", "i",
        ],
    );
    // 3 partitions in the sstabledump golden.
    assert_eq!(
        total_rows(&pair.vortex_batches),
        3,
        "udt_wide_fields row count"
    );
}

/// Varint ids 1..=13 of `varint_regular`: every value that fits `Decimal128(38,0)` (ids 14/15 are
/// the +/- 40-digit values, id 100 the null tombstone).
const VARINT_IN_RANGE: &str = "id IN (1,2,3,4,5,6,7,8,9,10,11,12,13)";

/// R2.2 — `varint` exports identically in both formats, down to the logical type
/// (`Decimal128(38,0)`, `arrow_schema.rs`) and the exact digits of 2^64+1.
#[test]
fn r2_2_varint_regular_matches_parquet() {
    let Some(pair) = export_and_diff(
        "test_varint",
        "varint_regular",
        "issue-4360-varint-column.cql",
        Fixture::Committed("issue_4360"),
        Some(VARINT_IN_RANGE),
    ) else {
        return;
    };
    assert_eq!(total_rows(&pair.vortex_batches), 13, "varint rows 1..=13");
    for (which, schema) in [
        ("parquet", pair.parquet_schema.as_ref()),
        ("vortex", pair.vortex_schema.as_ref()),
    ] {
        assert_eq!(
            field(schema, "v", which).data_type(),
            &DataType::Decimal128(38, 0),
            "{which}: varint logical type"
        );
    }
    let values = column_values_as_strings(&pair.vortex_batches, "v");
    assert!(
        values.iter().any(|v| v == "18446744073709551617"),
        "2^64+1 must round-trip exactly, got {values:?}"
    );
}

/// R2.2 / R4 — a varint beyond `Decimal128(38,0)` (the fixture's 40-digit rows) is REFUSED, not
/// truncated, by BOTH formats with the same column-naming message — the documented mapping limit
/// (`arrow_schema.rs`: varint -> Decimal128(38,0)) fails closed identically everywhere.
#[test]
fn r2_2_varint_beyond_decimal128_fails_closed_in_both_formats() {
    let Some(root) = root_or_skip(
        "test_varint",
        "varint_regular",
        Fixture::Committed("issue_4360"),
    ) else {
        return;
    };
    let schema = schema_file("issue-4360-varint-column.cql");
    let tmp = tempfile::TempDir::new().expect("tmp");
    for format in ["parquet", "vortex"] {
        let out = tmp.path().join(format!("out.{format}"));
        let (_, stderr, ok) = run_cli(&[
            "--schema",
            schema.to_str().unwrap(),
            "--data-dir",
            root.to_str().unwrap(),
            "export",
            out.to_str().unwrap(),
            "--format",
            format,
            "--table",
            "test_varint.varint_regular",
        ]);
        assert!(!ok, "{format}: a 40-digit varint must not export silently");
        assert!(
            stderr.contains("Column 'v': varint value exceeds Decimal128(38, 0) range"),
            "{format}: the refusal must name column v and the range, got: {stderr}"
        );
        if format == "vortex" {
            // R4.1 (Parquet's own partial-file behavior is out of scope here).
            assert!(!out.exists(), "vortex: a refused export must leave no file");
        }
    }
}

/// R2.2 — UDTs nested inside tuples / sets / maps (as keys and values), with null UDT fields.
#[test]
fn r2_2_nested_udt_keys_match_parquet() {
    let _ = export_and_diff(
        "test_nested_udt_keys",
        "nested_udt_keys",
        "nested-udt-keys.cql",
        Fixture::Bundle,
        None,
    );
}

/// Every `duration_val` in the sstabledump golden of `test_basic.simple_table` as nanoseconds
/// (the golden spells them `12h58m22s`; this fixture's values are h/m/s only).
fn golden_duration_nanos(root: &std::path::Path) -> Vec<i64> {
    let dir = datasets_root::table_generation_dirs(root, "test_basic", "simple_table")
        .into_iter()
        .next()
        .expect("simple_table generation dir");
    let jsonl = std::fs::read_dir(&dir)
        .expect("read table dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db.jsonl"))
        .unwrap_or_else(|| panic!("no Data.db.jsonl golden in {}", dir.display()));
    let text = std::fs::read_to_string(&jsonl).expect("read golden");
    let mut out = Vec::new();
    for chunk in text.split("\"name\":\"duration_val\",\"value\":\"").skip(1) {
        let lit = chunk.split('"').next().expect("literal");
        let mut nanos = 0i64;
        let mut num = String::new();
        for c in lit.chars() {
            if c.is_ascii_digit() {
                num.push(c);
                continue;
            }
            let n: i64 = num
                .parse()
                .unwrap_or_else(|_| panic!("duration literal {lit}"));
            num.clear();
            nanos += n * match c {
                'h' => 3_600_000_000_000,
                'm' => 60_000_000_000,
                's' => 1_000_000_000,
                other => panic!("unexpected unit {other} in golden duration {lit}"),
            };
        }
        assert!(num.is_empty(), "trailing digits in golden duration {lit}");
        out.push(nanos);
    }
    assert!(!out.is_empty(), "golden carries no duration_val cells");
    out
}

/// R2.3 — `duration` has no Parquet/Arrow `MonthDayNano` path, so BOTH formats carry it as
/// `Utf8` text in the same CQL duration form (`<n>ns` for a nanos-only value, as `1mo2d3ns` in the
/// spec's example). The sample values come from the sstabledump golden
/// (`test_basic.simple_table.duration_val`, e.g. `12h58m22s` = `46702000000000ns`).
#[test]
fn r2_3_duration_is_utf8_in_both_formats_with_golden_text() {
    let Some(pair) = export_and_diff(
        "test_basic",
        "simple_table",
        "basic-types.cql",
        Fixture::Bundle,
        None,
    ) else {
        return;
    };
    for (which, schema) in [
        ("parquet", pair.parquet_schema.as_ref()),
        ("vortex", pair.vortex_schema.as_ref()),
    ] {
        assert_eq!(
            logical_type(field(schema, "duration_val", which).data_type()),
            "Utf8",
            "{which}: duration logical type"
        );
    }
    let root = datasets_root::sstables_root_for_table("test_basic", "simple_table")
        .expect("resolved above");
    let golden = golden_duration_nanos(&root);
    // 12h58m22s is the first golden literal.
    assert!(golden.contains(&46_702_000_000_000));
    for (which, batches) in [
        ("parquet", &pair.parquet_batches),
        ("vortex", &pair.vortex_batches),
    ] {
        let values = column_values_as_strings(batches, "duration_val");
        for n in &golden {
            let expect = format!("{n}ns");
            assert!(
                values.iter().any(|v| *v == expect),
                "{which}: golden duration {expect} missing (first values: {:?})",
                &values[..values.len().min(5)]
            );
        }
    }
}
