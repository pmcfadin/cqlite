//! Parquet round-trip VALUE parity vs the sstabledump JSONL goldens — issue
//! #1490 (AD1), epic #1469.
//!
//! See `tests/support/parquet_parity/mod.rs` for the harness contract. One
//! `#[test]` per corpus table, so a case that cannot run is visible on its own
//! line instead of hiding behind a sibling that did (#3220).
//!
//! # Corpus coverage: 8 tables compared, and every exclusion NAMED
//!
//! A blanket sentence ("the remaining tables are not yet covered") is how a gap
//! becomes invisible; an enumerated list is a work item someone can pick up. So
//! the corpus considered by this lane is enumerated below, per table, with the
//! reason each excluded table is excluded.
//!
//! Corpus measured: **21 keyspaces / 172 table directories / 155 `*-Data.db`
//! generations** (16 test keyspaces + 5 Cassandra-internal `system*` ones).
//! **8 tables compare today**, over **40,829 cells**.
//!
//! Reason codes, and what each one MEANS for whoever picks the table up:
//!
//! * `INCLUDED` — compared on every run.
//! * `#1742` — the committed dump is not physical-dump-eligible (a TTL, a row or
//!   partition deletion, a range tombstone bound, or a static row): the dump
//!   keeps what a reconciled read drops, so a comparison would report a
//!   difference between the two ORACLES, not a defect. Needs the
//!   query-semantics oracle, not this one. **Not fixable here.**
//! * `refused` — the harness declines the representation (`unsupported.rs`): a
//!   UDT's Arrow Struct field types are undeclarable, or a declared CQL tuple's
//!   golden and export land in different canonical representations. Fixable by
//!   TEACHING the harness the representation.
//! * `counter` — counter columns; the export side is unverified and a counter's
//!   dump rendering needs its own decision before a case can be declared.
//! * `redundant` — its type surface is a subset of an INCLUDED table's; adding
//!   it buys little (still cheap, so listed rather than dismissed).
//! * `undeclared` — **nothing blocks it.** It needs a hand-written `ParityCase`
//!   (columns copied from the committed schema). This is the expansion work.
//!
//! ## `test_basic` (8 tables, `basic-types.cql`)
//!
//! * `simple_table` — INCLUDED (the full scalar zoo; carries the #3563 timeuuid type gap).
//! * `composite_key_table` — INCLUDED (two-component clustering key).
//! * `multi_partition_table` — undeclared: composite (2-component) partition key + timeuuid
//!   clustering. High value; expect the #3563 timeuuid type gap on `item_id`.
//! * `compression_test_table` — redundant: text/blob only, a subset of `simple_table`'s scalars.
//! * `uncompressed_table` — redundant: text/int/timestamp subset; the uncompressed read path is
//!   covered by `test_comp.uncompressed_table`.
//! * `ttl_test_table` — #1742: `default_time_to_live = 86400`, so every row carries a TTL.
//! * `static_columns_table` — #1742: the dump emits `static_block` entries, which are not rows;
//!   the harness refuses a non-`row` entry rather than guessing how one projects.
//! * `counters` — counter: four COUNTER columns.
//!
//! ## `test_collections` (8 tables, `collections.cql`)
//!
//! * `collection_table` — INCLUDED (six non-frozen collections).
//! * `frozen_collections_table` — undeclared: frozen set/list/map beside a non-frozen set — the
//!   single-cell-vs-multicell distinction in one table. HIGH value, cheap.
//! * `typed_collections_table` — undeclared: `set<uuid>`, `set<decimal>`, `list<blob>`,
//!   `map<text,inet>`. High value (exercises the exact-decimal path inside a collection).
//! * `nested_collections_table` — undeclared: `map<text, frozen<set<text>>>` and friends —
//!   nested frozen collection VALUES, a representation with no coverage at all today.
//! * `empty_collections_table` — undeclared: empty-vs-null collections (the #1485 silent-NULL
//!   family). High value.
//! * `large_collections_table` — undeclared: large collections + a clustering key.
//! * `collection_clustering_table` — undeclared, and needs work first: its CLUSTERING KEY is
//!   `frozen<list<text>>`, which the harness's key coercion does not handle.
//! * `collections_with_udts` — refused: `list<frozen<address_type>>` etc.; a UDT's Struct field
//!   types are undeclarable, so the type claim is unmeasurable.
//!
//! ## `test_timeseries` (9 tables, `time-series.cql`)
//!
//! * `sensor_data` — INCLUDED (2000 clustering rows, float/double/tinyint).
//! * `stock_prices` — undeclared: five DECIMAL columns + composite partition key. HIGH value —
//!   the exact-decimal comparison landed in this lane and has one table behind it.
//! * `tick_data` — undeclared: decimals + a 3-component partition key + timeuuid clustering.
//! * `app_metrics` — undeclared: composite partition key + `map<text,text>`.
//! * `user_activity` — undeclared: `(uuid, date)` partition key + map.
//! * `user_sessions` — undeclared: inet + map + boolean, no clustering.
//! * `log_entries` — undeclared: 3-component partition key, timeuuid clustering.
//! * `event_store` — undeclared: bigint clustering + map + timeuuid.
//! * `time_bucketed_counters` — counter: three COUNTER columns.
//!
//! ## `test_wide_rows` (8 tables, `wide-rows.cql`)
//!
//! * `wide_partition_table` — undeclared: FIVE clustering components (mixed ASC/DESC).
//!   HIGH value; nothing else covers a clustering key that wide.
//! * `many_columns_table` — undeclared: 100 columns spanning every scalar plus five
//!   collections. Highest single-table value in the corpus; expect the #3563 timeuuid gap.
//! * `product_catalog` — undeclared: `map<text,float>`, `map<text,double>`,
//!   `map<text,frozen<set<text>>>`, decimal.
//! * `chat_messages` — undeclared: `map<text,frozen<set<uuid>>>` + timeuuid clustering.
//! * `multi_metric_timeseries` — undeclared: 30 numeric columns + map + set + blob.
//! * `document_versions` — undeclared: int clustering DESC + set + map.
//! * `sparse_data_table` — undeclared: sparse/NULL-heavy rows across text/double/bool/blob and
//!   three collections. High value for the absent-vs-null family.
//! * `large_blob_table` — undeclared: large blobs + int clustering.
//!
//! ## `test_da` (4 tables, BTI `da`, binaries COMMITTED)
//!
//! * `simple_table` — INCLUDED. * `collection_table` — INCLUDED.
//! * `ttl_table` — #1742 (and used here as a NEGATIVE control that the refusal fires).
//! * `wide_table` — undeclared (schema `wide-table-bti.cql`): BTI wide partition.
//!
//! ## `test_signed_coll` (4 tables, `signed-collection-parity.cql`, COMMITTED)
//!
//! * `signed_int_collections` — INCLUDED.
//! * `frozen_int_collections` — undeclared: the FROZEN counterparts of the included table.
//!   Cheap and high value.
//! * `signed_width_collections` — undeclared: `set<bigint|smallint|tinyint>` — element WIDTH
//!   surface, which is exactly what the Arrow type check exists for.
//! * `signed_special_collections` — undeclared: `set<decimal>`, `set<double>`.
//!
//! ## `test_comp` (7 tables, `compression-parity.cql`, COMMITTED)
//!
//! * `lz4_table` — INCLUDED.
//! * `snappy_table`, `deflate_table`, `zstd_table`, `uncompressed_table`, `short_final_chunk` —
//!   redundant: byte-identical 3-column type surface (`pk int, ck int, body text`); only the
//!   codec/chunk length differs. Cheap to add, and each is a distinct decompression path.
//! * `incompressible_uncompressed_chunk` — redundant/undeclared: same shape with a `blob`
//!   payload and an uncompressed chunk.
//!
//! ## `test_compactionparityudt` (4 tables, `compaction-parity-udt.cql`, COMMITTED)
//!
//! * `udt_frozen_person` — INCLUDED, behind the #3556 whole-case gap (the export aborts).
//! * `udt_collections` — INCLUDED, with the #3556 per-column type gaps on `lp`/`ma`.
//! * `udt_nested` — refused + #3556: `frozen<employee>` nests `frozen<address>`, so the Struct
//!   field types are doubly undeclarable.
//! * `udt_null_inner` — refused + #3556: the same `lp`/`ma` columns as `udt_collections`,
//!   with NULL inner fields.
//!
//! ## `test_compaction_tombstone_ttl` (3 tables, COMMITTED)
//!
//! * `shadow_row_delete` — #1742 (row tombstone); used here as a NEGATIVE control.
//! * `ttl_expired_live` — #1742 (TTL).
//! * `rt_cross_gen` — #1742 (range tombstone bounds/boundaries).
//!
//! ## Remaining keyspaces — grouped, with counts, NOT per-table
//!
//! These are counted so nothing is invisible, but not enumerated per table: no case was
//! considered for any of them, and a per-table enumeration is part of the expansion issue's
//! own survey rather than a claim this lane can make.
//!
//! * `test_types` (20 dirs / 28 generations, `cql-type-parity.cql`) — the richest remaining
//!   type surface, and the **highest-value expansion target**: its `cx_*` tables are
//!   deliberately about tuple field order, UDT field order/NULLs, multicell UDT collection
//!   paths and nested frozen collections — i.e. several of them are exactly the
//!   representations this harness now REFUSES, so they gate on teaching it those.
//! * `test_oa` (6) — BIG `oa` fixtures. The harness covers `da` and `nb` today, so `oa` is a
//!   FORMAT gap, not a type gap. High value, low effort.
//! * `test_tomb` (9 dirs / 14 generations) — tombstone/GC surface: `#1742` by construction.
//! * `test_deltas` (27 dirs, 9 with binaries) — delta/CDC fixtures, multi-generation by design;
//!   the harness refuses a multi-generation table (a per-generation dump is not a reconciled
//!   read).
//! * `test_compactionparity` (2), `test_writeparity` (3), `test_big` (1) — compaction/write
//!   parity and one large table; undeclared.
//! * `system`, `system_auth`, `system_distributed`, `system_schema`, `system_traces`
//!   (49 dirs total) — Cassandra's own internal keyspaces: not user data, no committed CQL
//!   schema fixture, and several carry multiple generations. Out of scope for value parity.

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use parquet_parity::canonical_jsonl::CanonicalValue;
use parquet_parity::failure::Stage;
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

/// A wrong Arrow type on a column also makes the VALUE comparison unrunnable for
/// THAT column — an explicit part of the failure set, so a gap has to state how
/// much it defers.
const fn values_deferred(column: &'static str) -> ExpectedFailure {
    ExpectedFailure::Unrunnable {
        stage: Stage::ValueComparison,
        column: Some(column),
        blocked_by: Stage::ArrowTypes,
    }
}

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
        expect: &[
            AGE_GAP,
            NAME_GAP,
            values_deferred("age"),
            values_deferred("name"),
        ],
        what: "NEGATIVE CONTROL: records BOTH mismatches and BOTH deferrals present",
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
            values_deferred("age"),
            values_deferred("name"),
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

// ---------------------------------------------------------------------------
// The AGGREGATE is EXCLUSIVE too, on real export output
//
// Round-3 roborev finding: the pipeline stopped at the first failing stage, so
// the "exact failure set" a gap is compared against was only the set of what
// went wrong BEFORE the first abort. Two things could therefore hide behind a
// recorded gap — a malformed/ineligible GOLDEN (never loaded, because the
// expected export abort came first) and a VALUE regression in a column the
// deferred TYPE does not cover (never compared, because the first type mismatch
// cancelled the whole comparison). These two controls pin both.
// ---------------------------------------------------------------------------

/// (a) A gap recording an aborting export — and every stage that abort prevents
/// — must NOT hide the GOLDEN's own validation.
///
/// The golden stage depends on nothing the export does, so it runs FIRST and
/// unconditionally. Built on `test_da.ttl_table`, whose committed dump carries a
/// TTL and is therefore INELIGIBLE for physical-dump parity (#1742), with the
/// case naming a committed schema that does not declare the table so the REAL
/// export aborts. Before the aggregate this case would have been fully excused:
/// the abort happened first and the golden was never even opened.
#[test]
fn a_known_gap_cannot_hide_an_ineligible_golden_behind_an_aborting_export() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        expect: &[
            ExpectedFailure::ExportAborted {
                detail: "Could not determine column names for export",
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
        what: "NEGATIVE CONTROL: records the export abort and every stage it blocks, and \
               NOTHING about the golden",
    };
    const CASE: ParityCase = ParityCase {
        keyspace: "test_da",
        table: "ttl_table",
        // Deliberately a schema that does not declare `test_da.ttl_table`, so the
        // real `cqlite export` aborts.
        schema: "basic-types.cql",
        udts: &[],
        columns: &[("id", "uuid"), ("data", "text"), ("expiring_value", "int")],
        partition_key: &["id"],
        clustering: &[],
        must_run: true,
        covers: "NEGATIVE CONTROL: an ineligible golden alongside an aborting export",
        known_gap: Some(GAP),
        known_type_gaps: &[],
    };

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("the export must abort");
    let rendered = failures.to_string();
    // The whole export-side set the gap records IS present — so a mechanism that
    // only ever saw the first failing stage would have excused this case.
    assert!(
        rendered.contains("cqlite export failed") && rendered.contains("COULD NOT RUN"),
        "{rendered}"
    );
    let problem = GAP.mismatch(&CASE.id(), failures.items()).expect(
        "an INELIGIBLE golden must NOT be excused by a gap that records only the export abort",
    );
    assert!(
        problem.contains("OBSERVED BUT NOT RECORDED") && problem.contains("TTL"),
        "the refusal must name the golden defect the gap would have hidden: {problem}"
    );
}

/// (b) A gap deferring the TYPE of two columns must NOT defer any OTHER column's
/// VALUES.
///
/// The perturbation is applied to REAL export output through the staging seam,
/// on `salary` — a column with no type failure — and the aggregate must report
/// it as an unrecorded extra. Before the aggregate, the first type mismatch
/// cancelled the value comparison outright and every other column of the table
/// was silently uncovered while the gap read as "still present".
#[test]
fn a_known_gap_cannot_hide_a_value_regression_in_an_unaffected_column() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        expect: &[
            AGE_GAP,
            NAME_GAP,
            values_deferred("age"),
            values_deferred("name"),
        ],
        what: "NEGATIVE CONTROL: records the two type mismatches and the two deferrals, \
               EXACTLY — and nothing about any other column's values",
    };
    const CASE: ParityCase = da_simple_gap_variant(TWO_WRONG_TYPES, Some(GAP));

    let mut stages = parquet_parity::stage_case(&CASE)
        .expect("staging must not refuse a resolvable fixture")
        .expect("test_da.simple_table's binaries are committed");
    stages.overwrite_parquet_cell(0, "salary", CanonicalValue::Int(-4_242_424_242));

    let failures = parquet_parity::finish_case(&CASE, stages)
        .err()
        .expect("a perturbed cell in a column the gap does not defer MUST be reported");
    let problem = GAP
        .mismatch(&CASE.id(), failures.items())
        .expect("a value regression outside the deferred columns must NOT be excused");
    assert!(
        problem.contains("OBSERVED BUT NOT RECORDED") && problem.contains("column 'salary'"),
        "the refusal must NAME the value regression the gap would have hidden: {problem}"
    );
    // …and only that: the two type mismatches and the two per-column deferrals
    // all still reproduce, so the extra is the regression and nothing else.
    assert!(
        !problem.contains("RECORDED BUT NOT OBSERVED"),
        "every recorded failure must still reproduce: {problem}"
    );
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
// The THIRD outcome: `unsupported-representation`, on real export output
//
// A positive verdict requires an AFFIRMATIVE MEASUREMENT, so a stage that could
// not measure must be distinguishable from one that passed. These controls pin
// that the refusal (a) happens, (b) names the column and the representation,
// (c) FAILS the case, and (d) cannot be recorded into a `known_gap`.
//
// Built on `test_da.simple_table` (committed binaries, so they always run) by
// DECLARING a column as a CQL `tuple` — the same observable situation as a case
// that genuinely covers a tuple column.
// ---------------------------------------------------------------------------

/// `test_da.simple_table` with `name` declared as a CQL `tuple` — a
/// representation the harness refuses to compare.
const NAME_AS_TUPLE: &[(&str, &str)] = &[
    ("id", "uuid"),
    ("name", "tuple<text, text>"),
    ("age", "int"),
    ("salary", "bigint"),
    ("active", "boolean"),
    ("created", "timestamp"),
];

/// A declared CQL tuple is REFUSED by name, not compared across two
/// representations — and the refusal fails the case.
#[test]
fn a_declared_cql_tuple_column_is_refused_not_compared() {
    const CASE: ParityCase = da_simple_variant(NAME_AS_TUPLE, &[]);

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("a column the harness cannot compare MUST fail the case, never pass it");
    let rendered = failures.to_string();
    assert!(
        rendered.contains("UNSUPPORTED REPRESENTATION")
            && rendered.contains("column 'name'")
            && rendered.contains("cql-tuple-values"),
        "the refusal must name the stage, the column and the representation: {rendered}"
    );
    // And it is a REFUSAL, not a value difference: nothing was compared, so
    // nothing may be reported as unequal for that column.
    assert!(
        !rendered.contains("golden text:"),
        "a refused column must not also be reported as a value divergence: {rendered}"
    );
}

/// A refusal can NOT be recorded into a `known_gap`.
///
/// A known gap says "a recorded PRODUCT defect still reproduces"; a refusal says
/// "this HARNESS cannot represent this shape". There is deliberately no
/// `ExpectedFailure::UnsupportedRepresentation`, so the refusal is always an
/// unrecorded extra — this control pins that a gap recording everything ELSE the
/// case exhibits still fails, naming the refusal it would have hidden.
#[test]
fn a_refusal_cannot_be_recorded_as_a_known_gap() {
    const GAP: KnownGap = KnownGap {
        issue: "#0000",
        // Everything the case exhibits EXCEPT the refusal. Note there is no
        // `values_deferred("name")` entry: a REFUSED column carries the refusal
        // record INSTEAD of an `Unrunnable` one — the two are exclusive, because
        // reporting a column's values as twice-uncovered would break the very
        // multiset equality this control is about.
        expect: &[ExpectedFailure::ArrowType {
            column: "name",
            expected: "struct<utf8 | large_utf8,utf8 | large_utf8>",
            actual: "utf8",
        }],
        what: "NEGATIVE CONTROL: records every failure EXCEPT the representation refusal, \
               which has no ExpectedFailure counterpart by design",
    };
    const CASE: ParityCase = ParityCase {
        keyspace: "test_da",
        table: "simple_table",
        schema: "da-test.cql",
        udts: &[],
        columns: NAME_AS_TUPLE,
        partition_key: &["id"],
        clustering: &[],
        must_run: true,
        covers: "NEGATIVE CONTROL: a refusal is not a known gap",
        known_gap: Some(GAP),
        known_type_gaps: &[],
    };

    let failures = parquet_parity::run_case(&CASE)
        .err()
        .expect("the case must fail");
    let problem = GAP
        .mismatch(&CASE.id(), failures.items())
        .expect("a HARNESS refusal must never be excused by a PRODUCT-defect record");
    assert!(
        problem.contains("OBSERVED BUT NOT RECORDED")
            && problem.contains("unsupported-representation[value-comparison:column 'name']")
            && problem.contains("representation=cql-tuple-values"),
        "the refusal must be named as the unrecorded extra: {problem}"
    );
    // …and the refusal is the ONLY thing the gap failed to account for: every
    // other failure it records still reproduces, so this control is about the
    // refusal and not about a stale record.
    assert!(
        !problem.contains("RECORDED BUT NOT OBSERVED"),
        "every OTHER recorded failure must still reproduce: {problem}"
    );
}

// ---------------------------------------------------------------------------
// Fail closed on a table that SHOULD have been covered (#3220)
// ---------------------------------------------------------------------------

/// A `must_run` case whose fixture is ABSENT must RED, not quietly reduce the
/// compared-table count.
///
/// The rule it pins: `must_run` is set from `git ls-files`, so a committed
/// fixture that goes missing (a bad merge, a SIGKILLed fetch that left tracked
/// files deleted, a wrong `CQLITE_DATASETS_ROOT`) makes THAT case panic on its
/// own line. Without this control the rule was only ever exercised in the
/// direction where the fixture is present — i.e. never exercised at all.
///
/// Driven through `assert_case`, the entry point every parity case uses, and
/// through a table name no root can carry, so the assertion is about the
/// fail-closed rule and not about this machine's corpus.
#[test]
fn a_missing_committed_fixture_reds_the_suite() {
    const ABSENT: ParityCase = ParityCase {
        keyspace: "test_da",
        // A table no corpus root carries — the observable shape of a committed
        // fixture that went missing.
        table: "no_such_table_control",
        schema: "da-test.cql",
        udts: &[],
        columns: &[("id", "uuid")],
        partition_key: &["id"],
        clustering: &[],
        must_run: true,
        covers: "NEGATIVE CONTROL: a must_run case whose fixture is absent",
        known_gap: None,
        known_type_gaps: &[],
    };

    // The panic is the expected outcome, so the default hook's backtrace noise
    // is suppressed for the duration and restored immediately after.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parquet_parity::assert_case(&ABSENT)
    }));
    std::panic::set_hook(hook);

    let payload = result.expect_err(
        "a must_run case whose fixture is ABSENT must PANIC — a silent skip would shrink the \
         compared-table count with nothing red (#3220)",
    );
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_default();
    assert!(
        message.contains("test_da.no_such_table_control") && message.contains("MUST run"),
        "the failure must name the table and the rule: {message}"
    );
}
