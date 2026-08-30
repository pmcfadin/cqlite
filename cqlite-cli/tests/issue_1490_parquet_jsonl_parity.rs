//! Parquet round-trip VALUE parity vs the sstabledump JSONL goldens — issue
//! #1490 (AD1), epic #1469.
//!
//! See `tests/support/parquet_parity/mod.rs` for the harness contract. One
//! `#[test]` per corpus table, so a case that cannot run is visible on its own
//! line instead of hiding behind a sibling that did (#3220).

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use parquet_parity::canonical_jsonl::CanonicalValue;
use parquet_parity::{assert_case, ExpectedFailure, KnownGap, KnownTypeGap, ParityCase};

// ---------------------------------------------------------------------------
// test_da — BTI (`da`) fixtures, binaries COMMITTED to git
// ---------------------------------------------------------------------------

const DA_SIMPLE: ParityCase = ParityCase {
    keyspace: "test_da",
    table: "simple_table",
    schema: "da-test.cql",
    udts: &[],
    columns: &[
        ("id", "uuid"),
        ("name", "text"),
        ("age", "int"),
        ("salary", "bigint"),
        ("active", "boolean"),
        ("created", "timestamp"),
    ],
    partition_key: &["id"],
    clustering: &[],
    must_run: true,
    covers: "BTI da: uuid/text/int/bigint/boolean/timestamp scalars",
    known_gap: None,
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_da_simple_table() {
    assert_case(&DA_SIMPLE);
}

const DA_COLLECTIONS: ParityCase = ParityCase {
    keyspace: "test_da",
    table: "collection_table",
    schema: "da-test.cql",
    udts: &[],
    columns: &[
        ("id", "uuid"),
        ("tags", "set<text>"),
        ("scores", "list<int>"),
        ("properties", "map<text, text>"),
    ],
    partition_key: &["id"],
    clustering: &[],
    must_run: true,
    covers: "BTI da: non-frozen set/list/map assembled from per-element cells",
    known_gap: None,
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_da_collection_table() {
    assert_case(&DA_COLLECTIONS);
}

// ---------------------------------------------------------------------------
// test_signed_coll — signed integers inside collections, binaries COMMITTED
// ---------------------------------------------------------------------------

const SIGNED_INT_COLLECTIONS: ParityCase = ParityCase {
    keyspace: "test_signed_coll",
    table: "signed_int_collections",
    schema: "signed-collection-parity.cql",
    udts: &[],
    columns: &[("id", "int"), ("s", "set<int>"), ("m", "map<int, text>")],
    partition_key: &["id"],
    clustering: &[],
    must_run: true,
    covers: "negative integers as set elements and map keys (stringified paths)",
    known_gap: None,
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_signed_coll_signed_int_collections() {
    assert_case(&SIGNED_INT_COLLECTIONS);
}

// ---------------------------------------------------------------------------
// test_comp — compressed BIG (`nb`) with a clustering key, binaries COMMITTED
// ---------------------------------------------------------------------------

const COMP_LZ4: ParityCase = ParityCase {
    keyspace: "test_comp",
    table: "lz4_table",
    schema: "compression-parity.cql",
    udts: &[],
    columns: &[("pk", "int"), ("ck", "int"), ("body", "text")],
    partition_key: &["pk"],
    clustering: &["ck"],
    must_run: true,
    covers: "LZ4-compressed BIG nb, 600 clustering rows in one partition",
    known_gap: None,
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_comp_lz4_table() {
    assert_case(&COMP_LZ4);
}

// ---------------------------------------------------------------------------
// test_compactionparityudt — frozen UDTs and frozen nesting, binaries COMMITTED
// ---------------------------------------------------------------------------

const UDT_FROZEN_PERSON: ParityCase = ParityCase {
    keyspace: "test_compactionparityudt",
    table: "udt_frozen_person",
    schema: "compaction-parity-udt.cql",
    udts: &["person", "address", "employee"],
    columns: &[("id", "int"), ("p", "frozen<person>")],
    partition_key: &["id"],
    clustering: &[],
    must_run: true,
    covers: "frozen UDT with a NULL inner field",
    known_gap: Some(KnownGap {
        issue: "#3556",
        // The gap is an ABORT of the export itself, and it is the ONLY failure
        // the case exhibits. Recorded as structured data and compared by SET
        // EQUALITY, so a parity difference, an unreadable Parquet file or an
        // Arrow type mismatch appearing ALONGSIDE it is an unrecorded extra and
        // fails the case.
        expect: &[ExpectedFailure::ExportAborted {
            detail: "expected Blob value, got Udt",
        }],
        what: "a frozen UDT column reaches the Arrow converter with no CqlType, so the \
               export aborts instead of writing a Struct",
    }),
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_compactionparityudt_udt_frozen_person() {
    assert_case(&UDT_FROZEN_PERSON);
}

const UDT_COLLECTIONS: ParityCase = ParityCase {
    keyspace: "test_compactionparityudt",
    table: "udt_collections",
    schema: "compaction-parity-udt.cql",
    udts: &["person", "address", "employee"],
    columns: &[
        ("id", "int"),
        ("fl", "frozen<list<int>>"),
        ("fm", "frozen<map<text,int>>"),
        ("lp", "frozen<list<frozen<person>>>"),
        ("ma", "frozen<map<text, frozen<address>>>"),
    ],
    partition_key: &["id"],
    clustering: &[],
    must_run: true,
    covers: "frozen collections of frozen UDTs (single-cell nested values)",
    known_gap: Some(KnownGap {
        issue: "#3556",
        // TWO columns carry the SAME #3556 defect, and recording the failure
        // SET is what surfaced the second one: while the gap was matched by a
        // conjunction of substrings pinning `lp`, `ma`'s mismatch was
        // aggregated into the same message and rode along completely unnoticed.
        // Set EQUALITY forced it to be recorded (or fixed) — which is the whole
        // argument for structured failure data.
        //
        // Both are compared by equality on (column, expected, actual), so a
        // THIRD column joining them, or either of these two changing its wrong
        // type, still FAILS.
        expect: &[
            ExpectedFailure::ArrowType {
                column: "lp",
                expected: "list<struct(udt 'person')>",
                actual: "list<utf8>",
            },
            ExpectedFailure::ArrowType {
                column: "ma",
                expected: "map<utf8 | large_utf8,struct(udt 'address')>",
                actual: "map<utf8,utf8>",
            },
        ],
        what: "a UDT nested inside a frozen collection (list element 'lp', map value \
               'ma') is exported as a Utf8 ValueFormatter rendering instead of an Arrow \
               Struct",
    }),
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_compactionparityudt_udt_collections() {
    assert_case(&UDT_COLLECTIONS);
}

// ---------------------------------------------------------------------------
// Fetched corpus (`bash test-data/scripts/fetch-datasets.sh`) — these tables'
// binaries are gitignored, so a clean SKIP is legitimate; CQLITE_REQUIRE_FIXTURES=1
// promotes them to must-run.
// ---------------------------------------------------------------------------

const BASIC_SIMPLE: ParityCase = ParityCase {
    keyspace: "test_basic",
    table: "simple_table",
    schema: "basic-types.cql",
    udts: &[],
    columns: &[
        ("id", "uuid"),
        ("name", "text"),
        ("age", "int"),
        ("salary", "bigint"),
        ("height", "float"),
        ("weight", "double"),
        ("active", "boolean"),
        ("created", "timestamp"),
        ("birth_date", "date"),
        ("work_time", "time"),
        ("description", "blob"),
        ("account_balance", "decimal"),
        ("session_id", "timeuuid"),
        ("ip_address", "inet"),
        ("small_number", "tinyint"),
        ("medium_number", "smallint"),
        ("duration_val", "duration"),
        ("varchar_field", "varchar"),
        ("ascii_field", "ascii"),
    ],
    partition_key: &["id"],
    clustering: &[],
    must_run: false,
    covers: "the full scalar zoo: float/double/decimal/date/time/blob/inet/duration/timeuuid",
    known_gap: None,
    // FOUND BY THIS CHECK on its first run: `session_id timeuuid` is exported as
    // `Utf8` while `id uuid` — the identical 128-bit domain — is exported as
    // `FixedSizeBinary(16)`. The VALUES compare equal (both sides render the
    // UUID text), which is precisely why only a type assertion can see it.
    //
    // Recorded per COLUMN rather than as a whole-case `known_gap`: this table's
    // other 18 columns and all 19,000 cell comparisons — session_id's included —
    // still run.
    known_type_gaps: &[KnownTypeGap {
        column: "session_id",
        issue: "#3563",
        actual: "utf8",
        what: "'timeuuid' never parses (the scalar `alt` matches `time` first), so the \
               column's declared type is dropped and it degrades to Text",
    }],
};

#[test]
fn parquet_values_match_golden_test_basic_simple_table() {
    assert_case(&BASIC_SIMPLE);
}

const BASIC_COMPOSITE_KEY: ParityCase = ParityCase {
    keyspace: "test_basic",
    table: "composite_key_table",
    schema: "basic-types.cql",
    udts: &[],
    columns: &[
        ("partition_key", "uuid"),
        ("clustering_key1", "timestamp"),
        ("clustering_key2", "text"),
        ("data", "text"),
        ("value", "int"),
    ],
    partition_key: &["partition_key"],
    clustering: &["clustering_key1", "clustering_key2"],
    must_run: false,
    covers: "two-component clustering key (timestamp DESC, text ASC)",
    known_gap: None,
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_basic_composite_key_table() {
    assert_case(&BASIC_COMPOSITE_KEY);
}

const COLLECTIONS_TABLE: ParityCase = ParityCase {
    keyspace: "test_collections",
    table: "collection_table",
    schema: "collections.cql",
    udts: &[],
    columns: &[
        ("id", "uuid"),
        ("tags", "set<text>"),
        ("scores", "list<int>"),
        ("properties", "map<text, text>"),
        ("numbers_set", "set<int>"),
        ("ordered_values", "list<timestamp>"),
        ("metadata_map", "map<text, bigint>"),
    ],
    partition_key: &["id"],
    clustering: &[],
    must_run: false,
    covers: "six non-frozen collections incl. list<timestamp> and map<text,bigint>",
    known_gap: None,
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_collections_collection_table() {
    assert_case(&COLLECTIONS_TABLE);
}

const TIMESERIES_SENSOR_DATA: ParityCase = ParityCase {
    keyspace: "test_timeseries",
    table: "sensor_data",
    schema: "time-series.cql",
    udts: &[],
    columns: &[
        ("sensor_id", "uuid"),
        ("timestamp", "timestamp"),
        ("temperature", "float"),
        ("humidity", "float"),
        ("pressure", "double"),
        ("battery_level", "tinyint"),
        ("location", "text"),
        ("status", "text"),
    ],
    partition_key: &["sensor_id"],
    clustering: &["timestamp"],
    must_run: false,
    covers: "2000 clustering rows across 10 partitions, float/double/tinyint",
    known_gap: None,
    known_type_gaps: &[],
};

#[test]
fn parquet_values_match_golden_test_timeseries_sensor_data() {
    assert_case(&TIMESERIES_SENSOR_DATA);
}

// ---------------------------------------------------------------------------
// Oracle-fidelity guard
// ---------------------------------------------------------------------------

/// The goldens' float literals must parse EXACTLY, or the oracle is wrong.
///
/// serde_json's DEFAULT float parser is approximate. Measured on a real
/// `test_timeseries.sensor_data` `double`: `1014.5449131979983` parses ONE ULP
/// HIGH (`…647f` instead of `…647e`), so the harness reported a divergence for
/// an export that was byte-correct — the oracle, not the code under test, was
/// wrong. `cqlite-cli`'s dev-dependency on `serde_json` therefore enables
/// `float_roundtrip`; this test fails if that is ever dropped, instead of
/// letting every float-bearing case turn red for a reason nobody can see.
///
/// (The same lossy parse affects `cqlite-core`'s shared `canonical_jsonl`
/// comparator wherever it is compiled WITHOUT that feature — filed as #3557,
/// whose fix is to widen the WORKSPACE dependency; this guard covers the lane it
/// can speak for.)
#[test]
fn golden_float_literals_parse_exactly() {
    for literal in [
        "1014.5449131979983",
        "1009.0486357959104",
        "991.5986069073092",
        "31595.67",
    ] {
        let via_serde: f64 = serde_json::from_str::<serde_json::Value>(literal)
            .expect("literal must be valid JSON")
            .as_f64()
            .expect("literal must be a JSON number");
        let via_rust: f64 = literal.parse().expect("literal must parse as f64");
        assert_eq!(
            via_serde.to_bits(),
            via_rust.to_bits(),
            "serde_json parsed {literal} as {via_serde:?} but the exact value is \
             {via_rust:?} — re-enable serde_json's `float_roundtrip` feature in \
             cqlite-cli's dev-dependencies (issue #1490)"
        );
    }
}

// ---------------------------------------------------------------------------
// Harness sensitivity — a comparison that has never been shown to FAIL is not
// evidence of anything
// ---------------------------------------------------------------------------

/// Perturb ONE cell of the exported Parquet rows and require the comparison to
/// report it, naming the row, the column and both values.
///
/// Uses `test_da.simple_table` (committed binaries, so this always runs) and the
/// real export output — not a synthetic table — so it exercises exactly the code
/// path the parity cases use.
#[test]
fn harness_detects_a_single_changed_cell() {
    let prepared = prepared_or_panic(&DA_SIMPLE);
    let mut rows = prepared.parquet;
    let original = rows[0]
        .cell("name")
        .cloned()
        .expect("simple_table has a 'name' column");
    let mutated = match &original {
        CanonicalValue::Text(s) => CanonicalValue::Text(format!("{s} (perturbed)")),
        other => panic!("expected 'name' to be Text, got {other:?}"),
    };
    rows[0].overwrite_cell("name", mutated);

    let err = parquet_parity::compare(&DA_SIMPLE, &prepared.columns, prepared.golden, rows)
        .err()
        .map(|f| f.to_string())
        .expect("a perturbed cell MUST be reported as a parity difference");
    assert!(
        err.contains("column 'name'") && err.contains("perturbed"),
        "the diff must name the column and show both values: {err}"
    );
}

/// A cell that is NULLed out — the exact shape of the silent-NULL bug #1485
/// closed — must be reported, not treated as "no value to compare".
#[test]
fn harness_detects_a_nulled_cell() {
    let prepared = prepared_or_panic(&DA_SIMPLE);
    let mut rows = prepared.parquet;
    rows[0].overwrite_cell("age", CanonicalValue::Absent);

    let err = parquet_parity::compare(&DA_SIMPLE, &prepared.columns, prepared.golden, rows)
        .err()
        .map(|f| f.to_string())
        .expect("a NULLed cell MUST be reported as a parity difference");
    assert!(
        err.contains("column 'age'") && err.contains("<absent>"),
        "the diff must name the column and show the absence: {err}"
    );
}

/// A dropped row must be reported as a row-count difference, never absorbed.
#[test]
fn harness_detects_a_dropped_row() {
    let prepared = prepared_or_panic(&DA_SIMPLE);
    let mut rows = prepared.parquet;
    rows.pop();

    let err = parquet_parity::compare(&DA_SIMPLE, &prepared.columns, prepared.golden, rows)
        .err()
        .map(|f| f.to_string())
        .expect("a dropped row MUST be reported");
    assert!(err.contains("row count differs"), "{err}");
}

/// A rewritten primary key must be reported: the harness sorts both sides by
/// primary key, so a key that no longer matches would otherwise pair two
/// unrelated rows and could compare equal by luck.
#[test]
fn harness_detects_a_rewritten_primary_key() {
    let prepared = prepared_or_panic(&DA_SIMPLE);
    let mut rows = prepared.parquet;
    rows[0].overwrite_key(0, CanonicalValue::Text("not-a-real-uuid".to_string()));
    rows[0].overwrite_cell("id", CanonicalValue::Text("not-a-real-uuid".to_string()));

    let err = parquet_parity::compare(&DA_SIMPLE, &prepared.columns, prepared.golden, rows)
        .err()
        .map(|f| f.to_string())
        .expect("a rewritten primary key MUST be reported");
    assert!(
        err.contains("primary key differs") || err.contains("column 'id'"),
        "{err}"
    );
}

/// A wholly EMPTY oracle must fail rather than pass vacuously — the
/// 0-rows-when-present failure mode.
#[test]
fn harness_refuses_an_empty_oracle() {
    let prepared = prepared_or_panic(&DA_SIMPLE);
    let err = parquet_parity::compare(&DA_SIMPLE, &prepared.columns, Vec::new(), prepared.parquet)
        .err()
        .map(|f| f.to_string())
        .expect("an empty golden MUST fail");
    assert!(err.contains("ZERO rows"), "{err}");
}

fn prepared_or_panic(case: &ParityCase) -> parquet_parity::Prepared {
    match parquet_parity::prepare(case) {
        Ok(Some(p)) => p,
        Ok(None) => panic!(
            "{}: its SSTable binaries are committed to git, so it must always resolve",
            case.id()
        ),
        Err(e) => panic!("{e}"),
    }
}

// ---------------------------------------------------------------------------
// Fail-closed scope refusals — a physical dump is not a reconciled result set
// ---------------------------------------------------------------------------

/// A fixture carrying a ROW DELETION must be REFUSED, not silently compared: the
/// dump keeps the shadowed row while the export (correctly) drops it, so a
/// comparison would report a difference that is a property of the two oracles,
/// not of the code (#1742).
#[test]
fn harness_refuses_a_fixture_with_a_row_deletion() {
    const SHADOW_ROW_DELETE: ParityCase = ParityCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "shadow_row_delete",
        schema: "compaction-tombstone-ttl-parity.cql",
        udts: &[],
        columns: &[("id", "int"), ("ck", "int"), ("v", "text")],
        partition_key: &["id"],
        clustering: &["ck"],
        must_run: true,
        covers: "NEGATIVE control: a committed fixture with a row tombstone",
        known_gap: None,
        known_type_gaps: &[],
    };
    let err = parquet_parity::run_case(&SHADOW_ROW_DELETE)
        .err()
        .map(|f| f.to_string())
        .expect("a row-tombstone fixture must be refused, not compared");
    assert!(
        err.contains("row-level deletion") && err.contains("#1742"),
        "the refusal must name the construct and the reason: {err}"
    );
}

/// Same for a TTL: it can expire between fixture generation and test time, so a
/// dump-vs-export comparison is not stable.
#[test]
fn harness_refuses_a_fixture_with_a_ttl() {
    const TTL_TABLE: ParityCase = ParityCase {
        keyspace: "test_da",
        table: "ttl_table",
        schema: "da-test.cql",
        udts: &[],
        columns: &[("id", "uuid"), ("data", "text"), ("expiring_value", "int")],
        partition_key: &["id"],
        clustering: &[],
        must_run: true,
        covers: "NEGATIVE control: a committed fixture carrying a TTL",
        known_gap: None,
        known_type_gaps: &[],
    };
    let err = parquet_parity::run_case(&TTL_TABLE)
        .err()
        .map(|f| f.to_string())
        .expect("a TTL-bearing fixture must be refused, not compared");
    assert!(err.contains("TTL"), "the refusal must name the TTL: {err}");
}

// ---------------------------------------------------------------------------
// Unit coverage for the two normalization pieces
//
// These run in EVERY checkout, including one with no fetched corpus, where the
// only `duration` column in the case list (`test_basic.simple_table`) skips —
// otherwise the parser that decides duration equality would be untested exactly
// where the corpus is thinnest.
// ---------------------------------------------------------------------------

/// The two writers' spellings of the SAME duration must normalize to the same
/// (months, days, nanos) triple.
#[test]
fn duration_spellings_normalize_to_the_same_value() {
    use parquet_parity::spelling::parse_duration;

    // sstabledump's decomposed spelling vs the ValueFormatter's nanos spelling,
    // taken verbatim from a `test_basic.simple_table` row.
    assert_eq!(
        parse_duration("50m33s", "test").expect("cassandra spelling"),
        (0, 0, 3_033_000_000_000)
    );
    assert_eq!(
        parse_duration("3033000000000ns", "test").expect("cqlite spelling"),
        (0, 0, 3_033_000_000_000)
    );
    // Month/day components, and the units only one writer emits.
    assert_eq!(
        parse_duration("1y2mo3w4d5h6m7s8ms9us10ns", "test").expect("full grammar"),
        (
            14,
            25,
            5 * 3_600_000_000_000
                + 6 * 60_000_000_000
                + 7 * 1_000_000_000
                + 8 * 1_000_000
                + 9 * 1_000
                + 10
        )
    );
    // Both negative spellings: Cassandra's single leading sign vs the
    // ValueFormatter's per-component signs.
    assert_eq!(
        parse_duration("-1mo2d", "test").expect("cassandra negative"),
        (-1, -2, 0)
    );
    assert_eq!(
        parse_duration("-1mo-2d", "test").expect("cqlite negative"),
        (-1, -2, 0)
    );
    assert_eq!(parse_duration("0ns", "test").expect("zero"), (0, 0, 0));
}

/// A malformed or unknown-unit duration must ERROR — never normalize to
/// something that quietly compares unequal for an unexplained reason.
#[test]
fn duration_parser_rejects_malformed_spellings() {
    use parquet_parity::spelling::parse_duration;

    for bad in ["", "33", "ns", "1x", "1mo?", "-", "1 mo"] {
        assert!(
            parse_duration(bad, "test").is_err(),
            "{bad:?} must be rejected, not normalized"
        );
    }
}

/// The declared-type parser must REFUSE an unrecognized type rather than fall
/// back to comparing by JSON shape, which would silently weaken the oracle.
#[test]
fn declared_type_parser_refuses_unknown_types() {
    use parquet_parity::cql_type::parse_column;

    assert!(parse_column("c", "int", &[]).is_ok());
    assert!(
        parse_column("c", "SET<Text>", &[]).is_ok(),
        "case-insensitive"
    );
    assert!(parse_column("c", "frozen<list<frozen<person>>>", &["person"]).is_ok());
    // A UDT that the case did not declare, and a type that does not exist.
    let err =
        parse_column("c", "frozen<person>", &[]).expect_err("an undeclared UDT must be refused");
    assert!(err.contains("person"), "{err}");
    assert!(parse_column("c", "quaternion", &[]).is_err());
    assert!(
        parse_column("c", "map<int>", &[]).is_err(),
        "map needs 2 params"
    );
    assert!(parse_column("c", "set<int", &[]).is_err(), "unbalanced");
}

// ---------------------------------------------------------------------------
// Unit coverage for the Arrow TYPE expectation
//
// Value canonicalization folds every integer width into one `Int`, so the type
// check is the ONLY thing standing between a wrong CQL→Arrow mapping and a green
// suite. These cases prove it both accepts the faithful mapping and REJECTS a
// mis-width — the guard has to have been seen to red.
// ---------------------------------------------------------------------------

/// Every declared scalar in the corpus maps to exactly one faithful Arrow type.
#[test]
fn expected_arrow_type_pins_each_scalar() {
    use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
    use parquet_parity::arrow_expect::expected_shape;
    use parquet_parity::cql_type::parse_column;

    let accepts = |declared: &str, actual: &DataType| -> bool {
        let col = parse_column("c", declared, &[]).expect("declared type must parse");
        expected_shape(&col.spec)
            .expect("every corpus scalar has a declared expectation")
            .accepts(actual)
    };

    for (declared, expected) in [
        ("boolean", DataType::Boolean),
        ("tinyint", DataType::Int8),
        ("smallint", DataType::Int16),
        ("int", DataType::Int32),
        ("bigint", DataType::Int64),
        ("counter", DataType::Int64),
        ("float", DataType::Float32),
        ("double", DataType::Float64),
        ("text", DataType::Utf8),
        ("varchar", DataType::Utf8),
        ("ascii", DataType::Utf8),
        ("inet", DataType::Utf8),
        ("blob", DataType::Binary),
        ("uuid", DataType::FixedSizeBinary(16)),
        ("timeuuid", DataType::FixedSizeBinary(16)),
        ("date", DataType::Date32),
        ("time", DataType::Time64(TimeUnit::Nanosecond)),
        ("decimal", DataType::Decimal128(38, 9)),
        ("varint", DataType::Decimal128(38, 0)),
        ("duration", DataType::Utf8),
        ("duration", DataType::Interval(IntervalUnit::MonthDayNano)),
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ),
    ] {
        assert!(
            accepts(declared, &expected),
            "'{declared}' must accept {expected:?}"
        );
    }

    // The mis-width family this check exists for: a value round-trips
    // unchanged through any of these, so ONLY the type check can see it.
    for (declared, wrong) in [
        ("tinyint", DataType::Int64),
        ("tinyint", DataType::Int16),
        ("smallint", DataType::Int32),
        ("int", DataType::Int64),
        ("bigint", DataType::Int32),
        ("float", DataType::Float64),
        ("double", DataType::Float32),
        ("varint", DataType::Decimal128(38, 9)),
        ("varint", DataType::Int64),
        ("uuid", DataType::Utf8),
        ("blob", DataType::Utf8),
        ("date", DataType::Utf8),
        ("time", DataType::Utf8),
        ("boolean", DataType::Int8),
        ("inet", DataType::Binary),
        // A timestamp must be UTC epoch MILLIS, not a zone-less local one.
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ] {
        assert!(
            !accepts(declared, &wrong),
            "'{declared}' must REJECT {wrong:?} — a wrong width round-trips its values \
             unchanged, so nothing else in this harness can catch it"
        );
    }
}

/// Nested types are matched structurally: element/key/value types recurse, and a
/// UDT must be a `Struct` (#3556's `Utf8` flattening is exactly this check).
#[test]
fn expected_arrow_type_recurses_into_nested_types() {
    use arrow::datatypes::{DataType, Field, Fields};
    use parquet_parity::arrow_expect::{expected_shape, validate_field};
    use parquet_parity::cql_type::parse_column;
    use std::sync::Arc;

    let list_of = |t: DataType| DataType::List(Arc::new(Field::new("item", t, true)));
    let map_of = |k: DataType, v: DataType| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", k, false),
                    Field::new("value", v, true),
                ])),
                false,
            )),
            false,
        )
    };
    let shape = |declared: &str, udts: &[&str]| {
        let col = parse_column("c", declared, udts).expect("declared type must parse");
        expected_shape(&col.spec).expect("expectation must be derivable")
    };

    assert!(shape("set<int>", &[]).accepts(&list_of(DataType::Int32)));
    assert!(shape("list<int>", &[]).accepts(&list_of(DataType::Int32)));
    // …but not a list of the wrong element width.
    assert!(!shape("set<int>", &[]).accepts(&list_of(DataType::Int64)));
    assert!(shape("map<int, text>", &[]).accepts(&map_of(DataType::Int32, DataType::Utf8)));
    assert!(!shape("map<int, text>", &[]).accepts(&map_of(DataType::Utf8, DataType::Utf8)));
    // A UDT is a Struct with fields the case does not declare…
    let person = DataType::Struct(Fields::from(vec![Field::new("nm", DataType::Utf8, true)]));
    assert!(shape("frozen<person>", &["person"]).accepts(&person));
    // …and a Utf8 rendering of one is NOT.
    assert!(!shape("frozen<person>", &["person"]).accepts(&DataType::Utf8));
    assert!(!shape("frozen<list<frozen<person>>>", &["person"]).accepts(&list_of(DataType::Utf8)));

    // The mismatch message must name the column, the declared CQL type and both
    // Arrow types — it is what the #3556 known-gap signature pins.
    let col = parse_column("lp", "frozen<list<frozen<person>>>", &["person"]).expect("parses");
    let mismatch = validate_field(&col, &list_of(DataType::Utf8))
        .expect_err("a Utf8-flattened UDT must be rejected")
        .expect("it is a type mismatch, not a refusal to answer");
    // The rendered ACTUAL type is a FIELD, so the known-type-gap record can
    // compare it by equality rather than by substring.
    assert_eq!(mismatch.actual, "list<utf8>");
    assert_eq!(mismatch.expected, "list<struct(udt 'person')>");
    let err = mismatch.to_string();
    assert!(
        err.contains("Arrow type mismatch for column 'lp' declared 'frozen<list<frozen<person>>>'")
            && err.contains("expected list<struct(udt 'person')>")
            && err.contains("got list<utf8>"),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
// The Arrow TYPE check, and the per-column KnownTypeGap record, on REAL export
// output
//
// These are NEGATIVE CONTROLS built on `test_da.simple_table` (committed
// binaries, so they always run): the case deliberately MIS-DECLARES `age` as
// `bigint` where the fixture's CQL schema says `int`, which is the same
// observable situation as an export that widened the column. The values still
// compare (both sides canonicalize to `Int`), so ONLY the type check can red —
// which is the property under test.
// ---------------------------------------------------------------------------

/// `test_da.simple_table`'s real columns, with `age` mis-declared as `bigint`.
const AGE_AS_BIGINT: &[(&str, &str)] = &[
    ("id", "uuid"),
    ("name", "text"),
    ("age", "bigint"),
    ("salary", "bigint"),
    ("active", "boolean"),
    ("created", "timestamp"),
];

const fn da_simple_variant(
    columns: &'static [(&'static str, &'static str)],
    known_type_gaps: &'static [KnownTypeGap],
) -> ParityCase {
    ParityCase {
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test.cql",
        udts: &[],
        columns,
        partition_key: &["id"],
        clustering: &[],
        must_run: true,
        covers: "NEGATIVE CONTROL for the Arrow type check",
        known_gap: None,
        known_type_gaps,
    }
}

/// A wrong Arrow type must be REPORTED, naming the column, the declared CQL
/// type, the expected Arrow type and the actual one.
#[test]
fn type_check_reds_on_a_wrong_arrow_type() {
    const CASE: ParityCase = da_simple_variant(AGE_AS_BIGINT, &[]);
    let err = parquet_parity::prepare(&CASE)
        .err()
        .map(|f| f.to_string())
        .expect("an int32 column declared bigint MUST be reported");
    assert!(
        err.contains("Arrow type mismatch for column 'age' declared 'bigint'")
            && err.contains("expected int64")
            && err.contains("got int32"),
        "{err}"
    );
}

/// A recorded per-column gap whose `actual` matches excuses THAT column's type —
/// and nothing else: the case still prepares, so its values are still compared.
#[test]
fn a_matching_known_type_gap_excuses_only_that_column() {
    const CASE: ParityCase = da_simple_variant(
        AGE_AS_BIGINT,
        &[KnownTypeGap {
            column: "age",
            issue: "#0000",
            actual: "int32",
            what: "NEGATIVE CONTROL: the mis-declaration above",
        }],
    );
    let prepared = parquet_parity::prepare(&CASE)
        .expect("a matching type gap must not block the value comparison")
        .expect("test_da.simple_table's binaries are committed");
    assert!(
        !prepared.parquet.is_empty() && !prepared.golden.is_empty(),
        "the excused case must still project both sides"
    );
}

/// A gap recorded for a DIFFERENT actual type must not absorb this one — the
/// comparison is an equality, never a substring.
#[test]
fn a_known_type_gap_cannot_absorb_a_different_type_defect() {
    const CASE: ParityCase = da_simple_variant(
        AGE_AS_BIGINT,
        &[KnownTypeGap {
            column: "age",
            issue: "#0000",
            actual: "utf8",
            what: "NEGATIVE CONTROL: a gap recorded for another type",
        }],
    );
    let err = parquet_parity::prepare(&CASE)
        .err()
        .map(|f| f.to_string())
        .expect("a gap for utf8 must not excuse an int32 mismatch");
    assert!(
        err.contains("DIFFERENT type defect") && err.contains("got int32"),
        "{err}"
    );
}

/// A gap whose column's type is CORRECT must fail, demanding the record be
/// deleted — the same self-retiring rule the whole-case `known_gap` follows.
#[test]
fn a_known_type_gap_that_no_longer_reproduces_fails() {
    const CASE: ParityCase = da_simple_variant(
        DA_SIMPLE.columns,
        &[KnownTypeGap {
            column: "name",
            issue: "#0000",
            actual: "int32",
            what: "NEGATIVE CONTROL: a gap on a column whose type is correct",
        }],
    );
    let err = parquet_parity::prepare(&CASE)
        .err()
        .map(|f| f.to_string())
        .expect("a gap that no longer reproduces MUST fail");
    assert!(
        err.contains("now CORRECT") && err.contains("delete the KnownTypeGap"),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
// The whole-case `known_gap` is EXCLUSIVE, on real export output
//
// Round-2 roborev finding: a gap matched by a conjunction of precise substrings
// proves the recorded failure is PRESENT but says nothing about whether anything
// ELSE is. The harness aggregates EVERY Arrow type mismatch into one report, so
// a second, unrecorded mismatch rode along inside the same string and was
// excused. These controls pin the property that replaced containment: the
// observed failure set must EQUAL the recorded one.
//
// They are built on `test_da.simple_table` (committed binaries, so they always
// run) by MIS-DECLARING columns, which is the same observable situation as an
// export that produced the wrong Arrow type.
// ---------------------------------------------------------------------------

/// `test_da.simple_table` with TWO columns mis-declared: `age` as `bigint`
/// (really `int32`) and `name` as `int` (really `utf8`).
const TWO_WRONG_TYPES: &[(&str, &str)] = &[
    ("id", "uuid"),
    ("name", "int"),
    ("age", "bigint"),
    ("salary", "bigint"),
    ("active", "boolean"),
    ("created", "timestamp"),
];

const fn da_simple_gap_variant(
    columns: &'static [(&'static str, &'static str)],
    known_gap: Option<KnownGap>,
) -> ParityCase {
    ParityCase {
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test.cql",
        udts: &[],
        columns,
        partition_key: &["id"],
        clustering: &[],
        must_run: true,
        covers: "NEGATIVE CONTROL for known_gap exclusivity",
        known_gap,
        known_type_gaps: &[],
    }
}

/// The recorded `age` mismatch, as `arrow_expect` renders it.
const AGE_GAP: ExpectedFailure = ExpectedFailure::ArrowType {
    column: "age",
    expected: "int64",
    actual: "int32",
};

/// The `name` mismatch that accompanies it — deliberately NOT recorded below.
const NAME_GAP: ExpectedFailure = ExpectedFailure::ArrowType {
    column: "name",
    expected: "int32",
    actual: "utf8",
};

/// A SECOND, UNRECORDED failure occurring alongside a recorded gap must FAIL the
/// case — the exact shape of the round-2 defect.
///
/// The assertion is deliberately two-part: it first shows the recorded failure
/// IS present and precisely rendered (so a containment match, however many
/// precise substrings it conjoined, would have PASSED here), then shows set
/// equality refuses it anyway and NAMES the intruder.
#[test]
fn a_known_gap_cannot_hide_a_second_unrecorded_failure() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        expect: &[AGE_GAP],
        what: "NEGATIVE CONTROL: records only ONE of the two mismatches present",
    };
    const CASE: ParityCase = da_simple_gap_variant(TWO_WRONG_TYPES, Some(GAP));

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("two wrong Arrow types must fail");
    let rendered = failures.to_string();
    // Every substring a conjunction-style signature would have pinned about the
    // RECORDED failure is present — containment would have excused this case.
    assert!(
        rendered.contains("Arrow type mismatch for column 'age' declared 'bigint'")
            && rendered.contains("expected int64")
            && rendered.contains("got int32"),
        "{rendered}"
    );
    let problem = GAP
        .mismatch(&CASE.id(), failures.items())
        .expect("an UNRECORDED second failure must NOT be excused by a matching gap");
    assert!(
        problem.contains("OBSERVED BUT NOT RECORDED")
            && problem.contains("arrow-type[name] expected=int32 actual=utf8"),
        "the refusal must NAME the failure the gap would have hidden: {problem}"
    );
}

/// …and recording BOTH failures excuses the case, so the mechanism is an
/// equality and not simply "more than one failure always fails".
#[test]
fn a_known_gap_recording_the_exact_failure_set_is_excused() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        expect: &[AGE_GAP, NAME_GAP],
        what: "NEGATIVE CONTROL: records BOTH mismatches present",
    };
    const CASE: ParityCase = da_simple_gap_variant(TWO_WRONG_TYPES, Some(GAP));

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("two wrong Arrow types must fail");
    assert_eq!(
        GAP.mismatch(&CASE.id(), failures.items()),
        None,
        "the EXACT recorded set must be excused: {failures}"
    );
}

/// A recorded failure that stopped happening must FAIL — the self-retiring half,
/// asserted on the same real export output rather than only through the
/// "no longer reproduces" path (which only fires when NOTHING fails).
#[test]
fn a_known_gap_recording_a_failure_that_no_longer_happens_fails() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        expect: &[
            AGE_GAP,
            NAME_GAP,
            ExpectedFailure::ArrowType {
                column: "salary",
                expected: "int64",
                actual: "utf8",
            },
        ],
        what: "NEGATIVE CONTROL: records a third failure that does not happen",
    };
    const CASE: ParityCase = da_simple_gap_variant(TWO_WRONG_TYPES, Some(GAP));

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("two wrong Arrow types must fail");
    let problem = GAP
        .mismatch(&CASE.id(), failures.items())
        .expect("a recorded failure that stopped happening MUST fail");
    assert!(
        problem.contains("RECORDED BUT NOT OBSERVED")
            && problem.contains("arrow-type[salary] expected=int64 actual=utf8"),
        "{problem}"
    );
}

/// A gap recording a DIFFERENT wrong type for the right column must not absorb
/// this one: the (column, expected, actual) triple is compared by EQUALITY.
#[test]
fn a_known_gap_cannot_absorb_a_different_wrong_type_on_the_same_column() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        expect: &[
            ExpectedFailure::ArrowType {
                column: "age",
                expected: "int64",
                actual: "utf8",
            },
            NAME_GAP,
        ],
        what: "NEGATIVE CONTROL: records the wrong ACTUAL type for 'age'",
    };
    const CASE: ParityCase = da_simple_gap_variant(TWO_WRONG_TYPES, Some(GAP));

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("two wrong Arrow types must fail");
    let problem = GAP
        .mismatch(&CASE.id(), failures.items())
        .expect("a gap recording actual=utf8 must not excuse actual=int32");
    assert!(
        problem.contains("arrow-type[age] expected=int64 actual=int32")
            && problem.contains("arrow-type[age] expected=int64 actual=utf8"),
        "both the observed and the recorded triple must be named: {problem}"
    );
}

/// A gap recording NOTHING would match nothing-in-particular, so it is refused
/// rather than treated as "no expectations, therefore satisfied" — the permissive
/// branch a two-valued containment test would have taken.
#[test]
fn a_known_gap_recording_no_failures_is_refused() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        expect: &[],
        what: "NEGATIVE CONTROL: an empty recorded set",
    };
    const CASE: ParityCase = da_simple_gap_variant(TWO_WRONG_TYPES, Some(GAP));

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("two wrong Arrow types must fail");
    let problem = GAP
        .mismatch(&CASE.id(), failures.items())
        .expect("an empty recorded set MUST be refused");
    assert!(problem.contains("NO expected failures"), "{problem}");
}

/// A gap naming a column the case does not declare could never retire, so it is
/// refused outright.
#[test]
fn a_known_type_gap_must_name_a_declared_column() {
    const CASE: ParityCase = da_simple_variant(
        DA_SIMPLE.columns,
        &[KnownTypeGap {
            column: "no_such_column",
            issue: "#0000",
            actual: "utf8",
            what: "NEGATIVE CONTROL: a gap on a column that does not exist",
        }],
    );
    let err = parquet_parity::prepare(&CASE)
        .err()
        .map(|f| f.to_string())
        .expect("a gap naming an undeclared column MUST be refused");
    assert!(
        err.contains("KnownTypeGap is recorded for column 'no_such_column'")
            && err.contains("which the case does not declare"),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
// Whole-valued decimals: the golden renders `1`, the export `Decimal128(38,9)`
//
// No corpus table currently carries a scale-0 decimal (measured: 2650 decimal
// cells across every golden, none integer-shaped), so this path is covered by a
// unit test over the two normalizers rather than by inventing a fixture.
// ---------------------------------------------------------------------------

/// A decimal whose golden literal has no fractional part must canonicalize to
/// the SAME canonical value as the exported `Decimal128(38, 9)` cell.
#[test]
fn whole_valued_decimal_canonicalizes_on_both_sides() {
    use parquet_parity::arrow_rows::decimal_to_canonical;
    use parquet_parity::canonical_jsonl::{CanonicalValue, NormalizedFloat};
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_rows::normalize_declared_numbers;

    let decimal = parse_column("d", "decimal", &[]).expect("decimal parses");
    let varint = parse_column("v", "varint", &[]).expect("varint parses");

    for whole in [0i128, 1, -1, 42, -31_595] {
        // Golden side: sstabledump writes a whole decimal as a JSON integer.
        let golden = normalize_declared_numbers(CanonicalValue::Int(whole), &decimal.spec);
        // Export side: Decimal128(38, 9) holds whole * 10^9.
        let exported = decimal_to_canonical(whole * 1_000_000_000, 9, "test")
            .expect("scale-9 decimal must canonicalize");
        assert_eq!(
            golden, exported,
            "a whole decimal {whole} must compare equal across the two sides"
        );
        assert_eq!(
            golden,
            CanonicalValue::Float(NormalizedFloat(whole as f64)),
            "the canonical form of a whole decimal is the exact double it denotes"
        );
    }

    // A fractional decimal is untouched by the rule, and still compares exactly.
    assert_eq!(
        normalize_declared_numbers(
            CanonicalValue::Float(NormalizedFloat(31_595.67)),
            &decimal.spec
        ),
        decimal_to_canonical(31_595_670_000_000, 9, "test").expect("fractional decimal")
    );

    // varint is an integer domain on BOTH sides: it must stay an `Int`, or the
    // rule would turn a type confusion into a silent pass.
    assert_eq!(
        normalize_declared_numbers(CanonicalValue::Int(7), &varint.spec),
        CanonicalValue::Int(7)
    );
    assert_eq!(
        decimal_to_canonical(7, 0, "test").expect("varint"),
        CanonicalValue::Int(7)
    );

    // Beyond 2^53 the conversion would be lossy, so the golden value is LEFT as
    // an Int (and the export side refuses outright) — a loud failure, never a
    // rounded comparison.
    let huge = 1i128 << 60;
    assert_eq!(
        normalize_declared_numbers(CanonicalValue::Int(huge), &decimal.spec),
        CanonicalValue::Int(huge)
    );
    assert!(decimal_to_canonical(huge, 9, "test").is_err());
}

// ---------------------------------------------------------------------------
// A FROZEN map: sstabledump writes a JSON object, Arrow reads back a Map
//
// The corpus's only frozen maps (`fm`, `ma` on
// `test_compactionparityudt.udt_collections`) sit behind the #3556 whole-case
// gap and never reach the value comparison, so the conversion is covered by unit
// tests over the normalizer rather than by inventing a fixture. Without it those
// two columns would report a FALSE value difference the day #3556 is fixed.
// ---------------------------------------------------------------------------

/// A frozen map's golden JSON object must canonicalize to the SAME canonical
/// value the Arrow `Map` side produces — including the KEY coercion, which is
/// driven by the declared key type and never applied blindly.
#[test]
fn frozen_map_golden_object_canonicalizes_to_a_map() {
    use parquet_parity::canonical_jsonl::CanonicalValue;
    use parquet_parity::cql_type::parse_column;
    use parquet_parity::golden_rows::coerce_declared_shape;

    let text = |s: &str| CanonicalValue::Text(s.to_string());

    // `frozen<map<text,int>>` — the shape of `udt_collections.fm`.
    let fm = parse_column("fm", "frozen<map<text,int>>", &[]).expect("parses");
    let golden = CanonicalValue::Tuple(vec![
        ("a".to_string(), CanonicalValue::Int(1)),
        ("b".to_string(), CanonicalValue::Int(2)),
    ]);
    // What `arrow_rows::canonical_from_arrow` builds from an Arrow Map.
    let exported = CanonicalValue::Map(vec![
        (text("a"), CanonicalValue::Int(1)),
        (text("b"), CanonicalValue::Int(2)),
    ]);
    assert_eq!(coerce_declared_shape(golden, &fm.spec), exported);

    // `frozen<map<text, frozen<address>>>` — the shape of `udt_collections.ma`:
    // the VALUE stays a Tuple (a UDT really is a struct), the OUTER object
    // becomes a Map, and a null inner field folds to Absent as everywhere else.
    let ma =
        parse_column("ma", "frozen<map<text, frozen<address>>>", &["address"]).expect("parses");
    let golden = CanonicalValue::Tuple(vec![(
        "home".to_string(),
        CanonicalValue::Tuple(vec![
            ("city".to_string(), text("Austin")),
            ("zip".to_string(), CanonicalValue::Null),
        ]),
    )]);
    assert_eq!(
        parquet_parity::golden_rows::fold_null(coerce_declared_shape(golden, &ma.spec)),
        CanonicalValue::Map(vec![(
            text("home"),
            CanonicalValue::Tuple(vec![
                ("city".to_string(), text("Austin")),
                ("zip".to_string(), CanonicalValue::Absent),
            ]),
        )])
    );

    // An INTEGRAL key arrives as the JSON object key STRING `"1"`; the declared
    // key type is what coerces it back, matching the Arrow Int32 key.
    let mi = parse_column("mi", "frozen<map<int,text>>", &[]).expect("parses");
    assert_eq!(
        coerce_declared_shape(
            CanonicalValue::Tuple(vec![("-2".to_string(), text("x"))]),
            &mi.spec
        ),
        CanonicalValue::Map(vec![(CanonicalValue::Int(-2), text("x"))])
    );

    // …and a TEXT key that merely LOOKS numeric must stay Text, or a
    // `map<text,int>` holding "5" would false-match a `map<int,int>` holding 5.
    assert_eq!(
        coerce_declared_shape(
            CanonicalValue::Tuple(vec![("5".to_string(), CanonicalValue::Int(9))]),
            &fm.spec
        ),
        CanonicalValue::Map(vec![(text("5"), CanonicalValue::Int(9))])
    );

    // A Tuple stays a Tuple for every declared type that is NOT a map: a UDT
    // and a frozen list of UDTs must be untouched by the reshape.
    let person = parse_column("p", "frozen<person>", &["person"]).expect("parses");
    let as_tuple = CanonicalValue::Tuple(vec![("nm".to_string(), text("A"))]);
    assert_eq!(
        coerce_declared_shape(as_tuple.clone(), &person.spec),
        as_tuple
    );
    let lp = parse_column("lp", "frozen<list<frozen<person>>>", &["person"]).expect("parses");
    assert_eq!(
        coerce_declared_shape(CanonicalValue::List(vec![as_tuple.clone()]), &lp.spec),
        CanonicalValue::List(vec![as_tuple])
    );

    // A frozen map NESTED inside a frozen list is reached too.
    let lm = parse_column("lm", "frozen<list<frozen<map<text,int>>>>", &[]).expect("parses");
    assert_eq!(
        coerce_declared_shape(
            CanonicalValue::List(vec![CanonicalValue::Tuple(vec![(
                "k".to_string(),
                CanonicalValue::Int(3)
            )])]),
            &lm.spec
        ),
        CanonicalValue::List(vec![CanonicalValue::Map(vec![(
            text("k"),
            CanonicalValue::Int(3)
        )])])
    );
}

/// `CanonicalValue::Map` compares as an ORDERED sequence, so the golden's JSON
/// object order has to be sstabledump's (i.e. Cassandra's key-comparator order),
/// which is the order the Arrow map carries.
///
/// That holds only because the workspace pins `serde_json`'s `preserve_order`
/// feature. Asserted directly: if the feature is ever dropped, `serde_json`
/// falls back to a `BTreeMap` and object keys come out in STRING order, which
/// diverges from Cassandra's for every non-text key type — and a frozen map
/// would start comparing in the wrong order with no explanation.
#[test]
fn golden_json_object_order_is_preserved() {
    let parsed: serde_json::Value =
        serde_json::from_str(r#"{"b": 1, "a": 2, "10": 3, "2": 4}"#).expect("valid JSON");
    let keys: Vec<&str> = parsed
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec!["b", "a", "10", "2"],
        "serde_json must preserve JSON object order (workspace feature \
         `preserve_order`); without it a frozen map's golden entries would be \
         re-sorted into STRING order and stop matching the Arrow map's \
         Cassandra-comparator order"
    );
}

/// A non-frozen collection is multicell (one sstabledump cell per element); a
/// frozen one is not. That distinction drives the whole golden projection, so it
/// is asserted directly rather than only through a corpus case.
#[test]
fn frozen_wrapper_decides_multicell() {
    use parquet_parity::cql_type::parse_column;

    assert!(parse_column("s", "set<int>", &[])
        .expect("set<int>")
        .is_multicell_collection());
    assert!(!parse_column("s", "frozen<set<int>>", &[])
        .expect("frozen<set<int>>")
        .is_multicell_collection());
    assert!(!parse_column("p", "frozen<person>", &["person"])
        .expect("frozen<person>")
        .is_multicell_collection());
    assert!(!parse_column("n", "int", &[])
        .expect("int")
        .is_multicell_collection());
}
