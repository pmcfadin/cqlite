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
use parquet_parity::{assert_case, KnownGap, ParityCase};

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
        expect: "expected Blob value, got Udt",
        what: "a frozen UDT column reaches the Arrow converter with no CqlType, so the \
               export aborts instead of writing a Struct",
    }),
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
        expect: "column 'lp'",
        what: "a UDT nested inside a frozen collection is exported as a Utf8 \
               ValueFormatter rendering instead of an Arrow Struct",
    }),
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
    };
    let err = parquet_parity::run_case(&SHADOW_ROW_DELETE)
        .err()
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
    };
    let err = parquet_parity::run_case(&TTL_TABLE)
        .err()
        .expect("a TTL-bearing fixture must be refused, not compared");
    assert!(err.contains("TTL"), "the refusal must name the TTL: {err}");
}
