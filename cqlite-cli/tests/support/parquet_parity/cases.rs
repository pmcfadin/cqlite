//! The lane's REAL `ParityCase` declarations — one per corpus table under
//! Parquet↔sstabledump value parity (#1490).
//!
//! Split out of `issue_1490_parquet_jsonl_parity.rs` (campsite rule, #1135) so
//! two test binaries can share ONE declaration: the per-table `#[test]`s live in
//! that file, and the declaration CENSUS — every case's columns, types and key
//! definitions validated against its committed CQL schema — lives in
//! `issue_1490_parquet_declaration_and_keys.rs`. Duplicating the declarations
//! instead would defeat the whole point of the check, since a copy is exactly
//! what round 6 found unverified.
//!
//! Every type text here is copied from the committed `test-data/schemas/*.cql`,
//! i.e. from the Cassandra schema the fixture was written with, never from
//! CQLite's Arrow mapping (#3041) — and that copy is VERIFIED on every run
//! (`schema_fixture`), so a drift reds instead of silently becoming the oracle.

#![allow(dead_code)]

use super::failure::Stage;
use super::{ExpectedFailure, KnownGap, KnownTypeGap, ParityCase, SchemaCheck};

pub const DA_SIMPLE: ParityCase = ParityCase {
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
    schema_check: SchemaCheck::Committed,
    must_run: true,
    covers: "BTI da: uuid/text/int/bigint/boolean/timestamp scalars",
    known_gap: None,
    known_type_gaps: &[],
};

pub const DA_COLLECTIONS: ParityCase = ParityCase {
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
    schema_check: SchemaCheck::Committed,
    must_run: true,
    covers: "BTI da: non-frozen set/list/map assembled from per-element cells",
    known_gap: None,
    known_type_gaps: &[],
};

pub const SIGNED_INT_COLLECTIONS: ParityCase = ParityCase {
    keyspace: "test_signed_coll",
    table: "signed_int_collections",
    schema: "signed-collection-parity.cql",
    udts: &[],
    columns: &[("id", "int"), ("s", "set<int>"), ("m", "map<int, text>")],
    partition_key: &["id"],
    clustering: &[],
    schema_check: SchemaCheck::Committed,
    must_run: true,
    covers: "negative integers as set elements and map keys (stringified paths)",
    known_gap: None,
    known_type_gaps: &[],
};

pub const COMP_LZ4: ParityCase = ParityCase {
    keyspace: "test_comp",
    table: "lz4_table",
    schema: "compression-parity.cql",
    udts: &[],
    columns: &[("pk", "int"), ("ck", "int"), ("body", "text")],
    partition_key: &["pk"],
    clustering: &["ck"],
    schema_check: SchemaCheck::Committed,
    must_run: true,
    covers: "LZ4-compressed BIG nb, 600 clustering rows in one partition",
    known_gap: None,
    known_type_gaps: &[],
};

pub const UDT_FROZEN_PERSON: ParityCase = ParityCase {
    keyspace: "test_compactionparityudt",
    table: "udt_frozen_person",
    schema: "compaction-parity-udt.cql",
    udts: &["person", "address", "employee"],
    columns: &[("id", "int"), ("p", "frozen<person>")],
    partition_key: &["id"],
    clustering: &[],
    schema_check: SchemaCheck::Committed,
    must_run: true,
    covers: "frozen UDT with a NULL inner field",
    known_gap: Some(KnownGap {
        issue: "#3556",
        // The gap is an ABORT of the export itself, and it is the ONLY failure
        // the case exhibits. Recorded as structured data and compared by SET
        // EQUALITY, so a parity difference, an unreadable Parquet file or an
        // Arrow type mismatch appearing ALONGSIDE it is an unrecorded extra and
        // fails the case.
        //
        // The three UNRUNNABLE stages are recorded too, by name: the abort is
        // what PREVENTS them, and a deferral that does not say how much it
        // defers is exactly what let an earlier failure shrink the "exact set"
        // (round-3 roborev finding). The golden stage is NOT in this list
        // because it runs INDEPENDENTLY of the export and PASSES — an ineligible
        // golden here would be an unrecorded extra and would fail the case.
        expect: &[
            ExpectedFailure::ExportAborted {
                detail: "expected Blob value, got Udt",
            },
            ExpectedFailure::Unrunnable {
                stage: Stage::ParquetRead,
                column: None,
                blocked_by: Stage::Export,
            },
            ExpectedFailure::Unrunnable {
                stage: Stage::ArrowTypes,
                column: None,
                blocked_by: Stage::Export,
            },
            ExpectedFailure::Unrunnable {
                stage: Stage::ValueComparison,
                column: None,
                blocked_by: Stage::Export,
            },
        ],
        what: "a frozen UDT column reaches the Arrow converter with no CqlType, so the \
               export aborts instead of writing a Struct",
    }),
    known_type_gaps: &[],
};

pub const UDT_COLLECTIONS: ParityCase = ParityCase {
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
    schema_check: SchemaCheck::Committed,
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
            // The wrong TYPE on these two columns blocks THEIR values and
            // nothing else: `id`, `fl` and `fm` are still compared per cell on
            // every run, and a regression in any of them is an unrecorded extra
            // that fails this case. Before the aggregate, the first type
            // mismatch cancelled the whole comparison and those three columns
            // were silently uncovered.
            ExpectedFailure::Unrunnable {
                stage: Stage::ValueComparison,
                column: Some("lp"),
                blocked_by: Stage::ArrowTypes,
            },
            ExpectedFailure::Unrunnable {
                stage: Stage::ValueComparison,
                column: Some("ma"),
                blocked_by: Stage::ArrowTypes,
            },
        ],
        what: "a UDT nested inside a frozen collection (list element 'lp', map value \
               'ma') is exported as a Utf8 ValueFormatter rendering instead of an Arrow \
               Struct",
    }),
    known_type_gaps: &[],
};

pub const BASIC_SIMPLE: ParityCase = ParityCase {
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
    schema_check: SchemaCheck::Committed,
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

pub const BASIC_COMPOSITE_KEY: ParityCase = ParityCase {
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
    schema_check: SchemaCheck::Committed,
    must_run: false,
    covers: "two-component clustering key (timestamp DESC, text ASC)",
    known_gap: None,
    known_type_gaps: &[],
};

pub const COLLECTIONS_TABLE: ParityCase = ParityCase {
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
    schema_check: SchemaCheck::Committed,
    must_run: false,
    covers: "six non-frozen collections incl. list<timestamp> and map<text,bigint>",
    known_gap: None,
    known_type_gaps: &[],
};

pub const TIMESERIES_SENSOR_DATA: ParityCase = ParityCase {
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
    schema_check: SchemaCheck::Committed,
    must_run: false,
    covers: "2000 clustering rows across 10 partitions, float/double/tinyint",
    known_gap: None,
    known_type_gaps: &[],
};

/// Every REAL case this lane declares, for the declaration census. Each case
/// also has its own `#[test]` in `issue_1490_parquet_jsonl_parity.rs`, so this
/// slice can never hide a skipped table.
pub const REAL_CASES: &[&ParityCase] = &[
    &DA_SIMPLE,
    &DA_COLLECTIONS,
    &SIGNED_INT_COLLECTIONS,
    &COMP_LZ4,
    &UDT_FROZEN_PERSON,
    &UDT_COLLECTIONS,
    &BASIC_SIMPLE,
    &BASIC_COMPOSITE_KEY,
    &COLLECTIONS_TABLE,
    &TIMESERIES_SENSOR_DATA,
];
