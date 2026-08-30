//! Parquet round-trip VALUE parity vs the sstabledump JSONL goldens — issue
//! #1490 (AD1), epic #1469.
//!
//! See `tests/support/parquet_parity/mod.rs` for the harness contract. One
//! `#[test]` per corpus table, so a case that cannot run is visible on its own
//! line instead of hiding behind a sibling that did (#3220).

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use parquet_parity::{assert_case, ParityCase};

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
/// comparator wherever it is compiled WITHOUT that feature — reported
/// separately; this guard covers the lane it can speak for.)
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
