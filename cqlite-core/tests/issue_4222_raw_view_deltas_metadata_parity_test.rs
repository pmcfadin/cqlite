//! Issue #4222 — raw SSTable view, per-cell/per-collection/range metadata
//! parity against the `sstabledump` goldens (C intent-audit items R2, R4,
//! R11's collection caveat and R12's fixture spread).
//!
//! # What this lane adds that the point-read lane could not
//!
//! `test_tomb.resurrection_gc_positive` (the point-read lane's fixture)
//! carries no TTL'd cell, no collection column and no range tombstone, so
//! three whole families of the raw view's column contract (design.md D7)
//! had **no public-surface coverage at all**: `<col>_ttl`,
//! `<col>_complex_deletion{,_time,_timestamp}`, and the
//! `range_tombstone_start`/`range_tombstone_end` `row_kind`s with their
//! `bound_inclusive`/`range_deletion_time`/`range_deletion_timestamp`
//! columns. The range-marker mapping was unit-proven against a
//! `CompactionRowData::RangeMarker` the TEST built by hand — CQLite
//! asserting against input CQLite constructed, the wrong oracle class per
//! `docs/development/test-oracles.md` / #3042. Everything here runs through
//! `Database::execute` over real Cassandra 5.0 bytes instead.
//!
//! # Oracle (#1742/#3041/#3042)
//!
//! Every expected value is **parsed at runtime** from the committed
//! `*-Data.db.jsonl` sstabledump golden that sits beside the very `Data.db`
//! the query reads — never transcribed into a Rust literal, so a fixture
//! regeneration can never silently drift away from the assertions. The
//! goldens are read for the facts they state (`liveness_info.ttl`,
//! `liveness_info.expires_at`, a complex cell's `deletion_info.marked_deleted`,
//! a `range_tombstone_bound`'s `type`/`clustering`), and each loader panics
//! rather than defaulting when the golden does not carry the fact a case is
//! about — a fixture that stopped exercising the case must FAIL, never pass
//! vacuously.
//!
//! # Fixtures: `test_deltas` (`test-data/schemas/deltas.cql`, `nb` BIG, LZ4)
//!
//! * `ttl_cells` — `INSERT … USING TTL 3600` on pk=1/2/3; pk=10 written with
//!   NO TTL, the negative control that proves `_ttl` is reported from the
//!   bytes and never fabricated.
//! * `collection_ops` — `SET`/`LIST`/`MAP` columns; pk=2's `tags` was
//!   OVERWRITTEN (`tags = {…}`), which writes a complex deletion with its own
//!   timestamp, distinct from the same row's `props`/`vals` markers.
//! * `range_tombstones` — multi-column clustering `(ck1, ck2)`; pk=1 is a
//!   PREFIX bound (`ck1 = 2`, `ck2` unspecified — sstabledump renders it
//!   `"*"`), pk=3 is MIXED inclusivity (`ck1 > 1 AND ck1 <= 3`).
//!
//! # Fixture discipline (#3220/#3121, roborev finding #4222)
//!
//! These three fixtures' `Data.db` binaries are **fetch-only** — the repo
//! commits only their JSONL/`.txt`/`.crc32` sidecars — so this lane SKIPs
//! cleanly (never panics) when no candidate root carries the real bytes,
//! exactly as `issue_4222_raw_view_point_read_test.rs` does and for the same
//! reason: `scripts/agent-gate.sh` exports `CQLITE_DATASETS_ROOT`
//! UNCONDITIONALLY, so "the env var is set" is not a signal that a fetch
//! happened, and #3121's two-level SKIP/PANIC rule cannot apply.
//! `CQLITE_REQUIRE_FIXTURES=1` (#972 strict mode) turns every would-be SKIP
//! into a panic for a CI lane that must not pass having run nothing.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use chrono::DateTime;
use cqlite_core::query::result::QueryRow;
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};
use serde_json::Value as Json;
use std::path::{Path, PathBuf};

const KEYSPACE: &str = "test_deltas";
/// Every fixture here was written by the same Cassandra 5.0 run as a single
/// `nb` BIG generation, so the golden beside the data is always this one.
const GENERATION_PREFIX: &str = "nb-1-big";

// ---------------------------------------------------------------------------
// Fixture resolution (fetch-only ⇒ clean SKIP, or PANIC under strict mode)
// ---------------------------------------------------------------------------

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// The resolved `sstables/` root carrying `test_deltas.<table>`'s real bytes,
/// or `None` — the ONLY sanctioned skip for these fetch-only fixtures.
fn deltas_root_or_skip(table: &str) -> Option<PathBuf> {
    if let Some(root) = sstables_root_for_table(KEYSPACE, table) {
        return Some(root);
    }
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but '{KEYSPACE}.{table}' was not found under any \
             candidate root — fetch the corpus first \
             (bash test-data/scripts/fetch-datasets.sh): {}",
            describe_search(KEYSPACE, table)
        );
    }
    eprintln!(
        "SKIP: '{KEYSPACE}.{table}' (fetch-only fixture) was not found under any candidate \
         root — {}",
        describe_search(KEYSPACE, table)
    );
    None
}

/// Open a `Database` over JUST this one table's generation directories, and
/// the golden partitions that sit beside its `Data.db`.
///
/// The ingestion filter is TABLE-granular (`/test_deltas/<table>-`), not
/// keyspace-granular: `test_deltas` holds nine tables and this lane needs
/// exactly one of them per case.
async fn open_table(table: &str) -> Option<(Database, Vec<Json>)> {
    let root = deltas_root_or_skip(table)?;
    let schema =
        schema_path("deltas.cql").expect("committed schema deltas.cql must be readable (#3148)");
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root.clone(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/{table}-")),
    };
    let result = ingest(cfg).await.expect("ingestion of the fixture");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the committed schema must load, else the raw view would refuse with Error::Schema"
    );
    let golden = golden_partitions(&root, table);
    Some((result.database, golden))
}

// ---------------------------------------------------------------------------
// sstabledump golden (the oracle) — parsed, never transcribed
// ---------------------------------------------------------------------------

/// Every partition object of `<root>/<keyspace>/<table>-*/<gen>-Data.db.jsonl`,
/// read from the SAME generation directory whose `Data.db` the query reads.
fn golden_partitions(root: &Path, table: &str) -> Vec<Json> {
    let dirs = datasets_root::table_generation_dirs(root, KEYSPACE, table);
    let dir = dirs.first().unwrap_or_else(|| {
        panic!(
            "no *-Data.db-bearing {table}-* directory under {}/{KEYSPACE} even though that \
             root was selected as carrying the table",
            root.display()
        )
    });
    let path = dir.join(format!("{GENERATION_PREFIX}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "the sstabledump golden {} must be readable — it is THE oracle for this lane, \
             so its absence is a failure, never a skip: {e}",
            path.display()
        )
    });
    let parts: Vec<Json> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each golden line must be a JSON object"))
        .collect();
    assert!(
        !parts.is_empty(),
        "golden {} carried no partitions — a fixture that stopped exercising this lane must \
         FAIL, not pass vacuously",
        path.display()
    );
    parts
}

/// The golden partition whose key is `pk` (sstabledump renders keys as strings).
fn golden_partition<'a>(parts: &'a [Json], pk: i32) -> &'a Json {
    let wanted = pk.to_string();
    parts
        .iter()
        .find(|p| p["partition"]["key"][0].as_str() == Some(wanted.as_str()))
        .unwrap_or_else(|| panic!("golden must carry partition pk={pk}"))
}

/// The golden `"type": "row"` entry whose first clustering component is `ck`.
fn golden_row<'a>(partition: &'a Json, ck: i64) -> &'a Json {
    partition["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("golden partition must carry a 'rows' array"))
        .iter()
        .find(|r| r["type"] == "row" && r["clustering"][0].as_i64() == Some(ck))
        .unwrap_or_else(|| panic!("golden must carry a row with clustering[0] = {ck}"))
}

/// The golden cell named `name` under `row` that carries a `deletion_info`
/// but NO `path` — i.e. a COMPLEX (collection-level) deletion marker, not a
/// per-element tombstone.
fn golden_complex_deletion<'a>(row: &'a Json, name: &str) -> &'a Json {
    row["cells"]
        .as_array()
        .unwrap_or_else(|| panic!("golden row must carry a 'cells' array"))
        .iter()
        .find(|c| c["name"] == name && c.get("path").is_none() && c.get("deletion_info").is_some())
        .unwrap_or_else(|| {
            panic!("golden row must carry a pathless complex-deletion marker for '{name}'")
        })
}

/// The golden `range_tombstone_bound` entry carrying a `start` (or `end`) key.
fn golden_range_bound<'a>(partition: &'a Json, side: &str) -> &'a Json {
    let bound = partition["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("golden partition must carry a 'rows' array"))
        .iter()
        .find(|r| r["type"] == "range_tombstone_bound" && r.get(side).is_some())
        .unwrap_or_else(|| panic!("golden must carry a range_tombstone_bound with a '{side}'"));
    &bound[side]
}

/// Parse an sstabledump RFC3339 timestamp into epoch MICROSECONDS — the unit
/// `<col>_timestamp` / `row_timestamp` / `*_deletion_timestamp` report.
fn iso_to_micros(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp_micros()
}

/// Parse an sstabledump RFC3339 timestamp into epoch SECONDS — the unit
/// `*_local_deletion_time` / `range_deletion_time` / `expires_at` report.
fn iso_to_secs(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp()
}

/// A required golden STRING field, or a panic naming what was missing —
/// never a silent default that would turn a drifted fixture into a pass.
fn golden_str<'a>(node: &'a Json, field: &str) -> &'a str {
    node[field]
        .as_str()
        .unwrap_or_else(|| panic!("golden node must carry a string '{field}': {node}"))
}

// ---------------------------------------------------------------------------
// Raw-view row accessors
// ---------------------------------------------------------------------------

fn get<'a>(row: &'a QueryRow, col: &str) -> Option<&'a Value> {
    row.values.get(col)
}

fn text_of(row: &QueryRow, col: &str) -> Option<String> {
    match get(row, col) {
        Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).to_string()),
        _ => None,
    }
}

fn int_of(row: &QueryRow, col: &str) -> Option<i32> {
    match get(row, col) {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }
}

fn bigint_of(row: &QueryRow, col: &str) -> Option<i64> {
    match get(row, col) {
        Some(Value::BigInt(i)) => Some(*i),
        _ => None,
    }
}

fn bool_of(row: &QueryRow, col: &str) -> Option<bool> {
    match get(row, col) {
        Some(Value::Boolean(b)) => Some(*b),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// R2 scenario B — a TTL'd live cell carries its declared TTL and expiry
// ---------------------------------------------------------------------------

/// Spec R2: "a TTL'd live cell's `<col>_ttl` carries the declared TTL and
/// `<col>_local_deletion_time` its computed expiry" — asserted per cell AND
/// at row level, every expectation parsed from `ttl_cells`' golden.
///
/// Before this case, `grep "_ttl"` over the whole #4222 test suite returned
/// ZERO hits: the mapping (`row_map.rs`'s `cell.ttl` → `<col>_ttl`) was
/// working, untested code, while TTL parity is explicit in the issue's AC1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_cells_report_the_declared_ttl_and_computed_expiry_from_the_golden() {
    let Some((db, golden)) = open_table("ttl_cells").await else {
        return;
    };
    let result = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.ttl_cells_raw_sstable_data WHERE pk = 1"
        ))
        .await
        .expect("raw view point-key query must succeed");

    let partition = golden_partition(&golden, 1);
    let golden_rows = partition["rows"].as_array().expect("golden rows array");
    assert_eq!(
        result.rows.len(),
        golden_rows.len(),
        "the raw view must return exactly the golden's physical row count for pk=1"
    );

    for ck in 1..=5i64 {
        let g = golden_row(partition, ck);
        let liveness = &g["liveness_info"];
        // Fail closed if the FIXTURE stopped carrying a TTL: this case is
        // about the TTL columns, so a golden without one means the lane
        // measures nothing (#3220's "never pass on an empty measurement").
        let ttl = liveness["ttl"].as_i64().unwrap_or_else(|| {
            panic!("ttl_cells pk=1 ck={ck} golden must carry liveness_info.ttl — got {liveness}")
        });
        let tstamp = iso_to_micros(golden_str(liveness, "tstamp"));
        let expires_at = iso_to_secs(golden_str(liveness, "expires_at"));
        // The golden's own two fields must agree with each other, so an
        // `expires_at` assertion below cannot be satisfied by a value CQLite
        // merely echoed from the wrong field.
        assert_eq!(
            expires_at,
            tstamp / 1_000_000 + ttl,
            "golden self-consistency: expires_at must equal write-time + TTL"
        );

        let row = result
            .rows
            .iter()
            .find(|r| int_of(r, "ck") == Some(ck as i32))
            .unwrap_or_else(|| panic!("the raw view must return ck={ck}"));

        for col in ["val", "extra"] {
            assert_eq!(
                int_of(row, &format!("{col}_ttl")),
                Some(ttl as i32),
                "{col}_ttl must equal the golden's declared TTL for ck={ck}"
            );
            assert_eq!(
                bigint_of(row, &format!("{col}_timestamp")),
                Some(tstamp),
                "{col}_timestamp must match the golden's write time byte-exact for ck={ck}"
            );
            assert_eq!(
                bigint_of(row, &format!("{col}_local_deletion_time")),
                Some(expires_at),
                "{col}_local_deletion_time must be the golden's COMPUTED EXPIRY (write-time \
                 + TTL), not a tombstone time, for ck={ck}"
            );
            assert_eq!(
                get(row, &format!("{col}_tombstone")),
                None,
                "a LIVE expiring cell must carry no tombstone kind for ck={ck}"
            );
        }

        // The row-level liveness quintet reports the same three facts
        // (R3's `row_timestamp`/`row_ttl`, also previously unasserted).
        assert_eq!(int_of(row, "row_ttl"), Some(ttl as i32));
        assert_eq!(bigint_of(row, "row_timestamp"), Some(tstamp));
        assert_eq!(bigint_of(row, "row_local_deletion_time"), Some(expires_at));
        assert_eq!(
            text_of(row, "row_kind").as_deref(),
            Some("row"),
            "a live TTL'd row is an ordinary row, never a tombstone kind"
        );
    }
}

/// Negative control for the case above: `ttl_cells` pk=10 was written with
/// NO TTL, so every `_ttl` column must be ABSENT — proving the TTL columns
/// are read from the bytes rather than fabricated whenever the reader has a
/// local-deletion-time field available (#28's no-heuristics mandate).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_written_without_a_ttl_reports_no_ttl_at_all() {
    let Some((db, golden)) = open_table("ttl_cells").await else {
        return;
    };
    let partition = golden_partition(&golden, 10);
    // The control is only a control if the golden really has no TTL here.
    for g in partition["rows"].as_array().expect("golden rows array") {
        assert!(
            g["liveness_info"].get("ttl").is_none(),
            "ttl_cells pk=10 is the NO-TTL control — a golden that grew a TTL invalidates \
             this case and must FAIL: {g}"
        );
    }

    let result = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.ttl_cells_raw_sstable_data WHERE pk = 10"
        ))
        .await
        .expect("raw view point-key query must succeed");
    assert!(!result.rows.is_empty(), "pk=10 must return its rows");

    for row in &result.rows {
        for col in ["val_ttl", "extra_ttl", "row_ttl"] {
            assert_eq!(
                get(row, col),
                None,
                "{col} must be ABSENT for a cell written without a TTL, never fabricated"
            );
        }
        for col in [
            "val_local_deletion_time",
            "extra_local_deletion_time",
            "row_local_deletion_time",
        ] {
            assert_eq!(
                get(row, col),
                None,
                "{col} must be ABSENT for a live non-expiring cell"
            );
        }
        // The write time IS present — so the absences above are real
        // absences, not the whole row failing to decode.
        assert!(
            bigint_of(row, "val_timestamp").is_some(),
            "val_timestamp must still be reported for a live non-expiring cell"
        );
    }
}

// ---------------------------------------------------------------------------
// R2 scenario C — complex (collection) deletion markers
// ---------------------------------------------------------------------------

/// Spec R2: "for a collection/UDT column, `<col>_complex_deletion` and — when
/// true — `_complex_deletion_time` / `_complex_deletion_timestamp`". Golden:
/// `collection_ops` pk=2, whose `tags` was OVERWRITTEN (`tags = {'only_this'}`)
/// and therefore carries a complex deletion with its OWN timestamp, distinct
/// from the `props`/`vals` markers written by the same statement batch.
///
/// Before this case the only coverage was a hand-built `ComplexColumn` in
/// `row_map.rs`'s unit tests — CQLite asserting against input CQLite
/// constructed (#3042), and never reaching the public surface at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_complex_deletion_markers_match_the_golden() {
    let Some((db, golden)) = open_table("collection_ops").await else {
        return;
    };
    let result = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.collection_ops_raw_sstable_data WHERE pk = 2"
        ))
        .await
        .expect("raw view point-key query must succeed");
    assert_eq!(result.rows.len(), 1, "collection_ops pk=2 holds one row");
    let row = &result.rows[0];

    let g_row = golden_row(golden_partition(&golden, 2), 1);
    let mut seen_timestamps = Vec::new();
    for col in ["tags", "vals", "props"] {
        let deletion = &golden_complex_deletion(g_row, col)["deletion_info"];
        let marked = iso_to_micros(golden_str(deletion, "marked_deleted"));
        let local = iso_to_secs(golden_str(deletion, "local_delete_time"));
        seen_timestamps.push(marked);

        assert_eq!(
            bool_of(row, &format!("{col}_complex_deletion")),
            Some(true),
            "{col} carries a complex deletion in the golden, so its marker must be true"
        );
        assert_eq!(
            bigint_of(row, &format!("{col}_complex_deletion_timestamp")),
            Some(marked),
            "{col}_complex_deletion_timestamp must match the golden's marked_deleted byte-exact"
        );
        assert_eq!(
            bigint_of(row, &format!("{col}_complex_deletion_time")),
            Some(local),
            "{col}_complex_deletion_time must match the golden's local_delete_time byte-exact"
        );
    }

    // `tags` was overwritten by a LATER statement than `props`/`vals`, so its
    // marker timestamp differs. Without this, three assertions against one
    // shared value could all pass on an implementation that copied a single
    // row-level deletion onto every collection column.
    assert_ne!(
        seen_timestamps[0], seen_timestamps[2],
        "golden precondition: pk=2's `tags` overwrite must carry a DIFFERENT marked_deleted \
         than `props`, else this case cannot distinguish per-column markers"
    );
    assert_ne!(
        bigint_of(row, "tags_complex_deletion_timestamp"),
        bigint_of(row, "props_complex_deletion_timestamp"),
        "each collection column's complex deletion must be reported from its OWN marker, \
         never a single value copied across the row"
    );

    // Complex columns take the `_complex_deletion*` triple INSTEAD of the
    // per-cell quad — pinned here on the value axis, and by
    // `collection_table_column_contract_is_pinned_from_the_public_surface`
    // (below) on the name axis.
    for absent in ["tags_timestamp", "tags_ttl", "tags_local_deletion_time"] {
        assert_eq!(
            get(row, absent),
            None,
            "a collection column gets the complex-deletion triple, never the per-cell quad"
        );
    }
}

/// C-audit R11 caveat: the R11 column-contract snapshot uses
/// `test_tomb.dropped_regular_col`, which has NO collection or UDT column —
/// so the `<col>_complex_deletion{,_time,_timestamp}` NAMES were pinned by
/// nothing. This pins them from the PUBLIC surface (`metadata.columns`, the
/// positional contract every writer renders from), on a fixture that really
/// has three collection columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_table_column_contract_is_pinned_from_the_public_surface() {
    let Some((db, _golden)) = open_table("collection_ops").await else {
        return;
    };
    let result = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.collection_ops_raw_sstable_data WHERE pk = 2"
        ))
        .await
        .expect("raw view point-key query must succeed");

    let names: Vec<&str> = result
        .metadata
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec![
            "pk",
            "ck",
            "tags",
            "tags_complex_deletion",
            "tags_complex_deletion_time",
            "tags_complex_deletion_timestamp",
            "vals",
            "vals_complex_deletion",
            "vals_complex_deletion_time",
            "vals_complex_deletion_timestamp",
            "props",
            "props_complex_deletion",
            "props_complex_deletion_time",
            "props_complex_deletion_timestamp",
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
        "the raw-view column contract for a COLLECTION-bearing table changed — update this \
         snapshot deliberately if the change is intended (issue #4222 D7)"
    );
    // metadata.columns is positional; every writer indexes it.
    for (idx, col) in result.metadata.columns.iter().enumerate() {
        assert_eq!(col.position, idx);
    }
}

// ---------------------------------------------------------------------------
// R4 — range tombstones are their own rows, observed END-TO-END
// ---------------------------------------------------------------------------

/// Assert the shared facts of one golden bound against one raw-view row.
fn assert_bound_matches_golden(row: &QueryRow, golden_bound: &Json, row_kind: &str) {
    assert_eq!(
        text_of(row, "row_kind").as_deref(),
        Some(row_kind),
        "each range-tombstone bound is its own row, discriminated by row_kind"
    );
    let expected_inclusive = match golden_str(golden_bound, "type") {
        "inclusive" => true,
        "exclusive" => false,
        other => panic!("unexpected sstabledump bound type '{other}'"),
    };
    assert_eq!(
        bool_of(row, "bound_inclusive"),
        Some(expected_inclusive),
        "bound_inclusive must match sstabledump's reported inclusivity for THIS bound"
    );

    let deletion = &golden_bound["deletion_info"];
    assert_eq!(
        bigint_of(row, "range_deletion_timestamp"),
        Some(iso_to_micros(golden_str(deletion, "marked_deleted"))),
        "range_deletion_timestamp must match the golden's marked_deleted byte-exact"
    );
    assert_eq!(
        bigint_of(row, "range_deletion_time"),
        Some(iso_to_secs(golden_str(deletion, "local_delete_time"))),
        "range_deletion_time must match the golden's local_delete_time byte-exact"
    );

    // The bound carries the bound's clustering components and NOTHING else:
    // every non-key, non-range column is absent, never a fabricated NULL.
    assert_eq!(
        get(row, "val"),
        None,
        "a range-tombstone bound row carries no data-column value"
    );
    assert_eq!(get(row, "val_timestamp"), None);
    assert_eq!(get(row, "row_timestamp"), None);
    assert_eq!(get(row, "row_tombstone"), None);
    assert_eq!(get(row, "partition_deletion_timestamp"), None);

    // The bound's own clustering prefix, taken from the golden.
    let clustering = golden_bound["clustering"]
        .as_array()
        .expect("a golden bound must carry a clustering array");
    assert_eq!(
        int_of(row, "ck1"),
        clustering[0].as_i64().map(|v| v as i32),
        "the bound's first clustering component must be reported"
    );
    // sstabledump renders an UNSPECIFIED trailing component as "*". CQLite
    // must report it ABSENT — never a fabricated NULL or a zero value.
    assert_eq!(
        clustering[1].as_str(),
        Some("*"),
        "golden precondition: these fixtures' bounds are ck1-only PREFIXES"
    );
    assert_eq!(
        get(row, "ck2"),
        None,
        "an unspecified clustering component must be ABSENT on the bound row, never \
         fabricated (the prefix-bound clause of spec R4)"
    );
}

/// Spec R4, scenario 1 — PREFIX bound: `range_tombstones` pk=1 was deleted
/// with `ck1 = 2` only, so both bounds carry `ck1 = 2` with `ck2`
/// unspecified. This is the first time ANY test observes a
/// `range_tombstone_start`/`range_tombstone_end` row from a query: the prior
/// coverage called `map_compaction_row` on a hand-built marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_tombstone_prefix_bound_yields_two_bound_rows_end_to_end() {
    let Some((db, golden)) = open_table("range_tombstones").await else {
        return;
    };
    let result = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.range_tombstones_raw_sstable_data WHERE pk = 1"
        ))
        .await
        .expect("raw view point-key query must succeed");

    let partition = golden_partition(&golden, 1);
    assert_eq!(
        result.rows.len(),
        partition["rows"].as_array().expect("rows array").len(),
        "the raw view must emit one row per golden entry — including BOTH range-tombstone \
         bounds, which sstabledump also lists as two separate entries"
    );

    let start = result
        .rows
        .iter()
        .find(|r| text_of(r, "row_kind").as_deref() == Some("range_tombstone_start"))
        .expect("a range_tombstone_start row must be returned by the QUERY, not just mapped");
    let end = result
        .rows
        .iter()
        .find(|r| text_of(r, "row_kind").as_deref() == Some("range_tombstone_end"))
        .expect("a range_tombstone_end row must be returned by the QUERY");

    assert_bound_matches_golden(
        start,
        golden_range_bound(partition, "start"),
        "range_tombstone_start",
    );
    assert_bound_matches_golden(
        end,
        golden_range_bound(partition, "end"),
        "range_tombstone_end",
    );

    // The prefix case: both bounds are INCLUSIVE on the same ck1 (the golden
    // says so; the helper asserted it per bound — restated here so the
    // scenario's own shape is visible at the call site).
    assert_eq!(bool_of(start, "bound_inclusive"), Some(true));
    assert_eq!(bool_of(end, "bound_inclusive"), Some(true));
    assert_eq!(int_of(start, "ck1"), int_of(end, "ck1"));

    // Source identity is attached to a synthetic bound row like any other.
    for row in [start, end] {
        assert_eq!(bigint_of(row, "generation"), Some(1));
        assert_eq!(text_of(row, "format").as_deref(), Some("big"));
        assert!(text_of(row, "sstable")
            .unwrap_or_default()
            .starts_with("nb-1-"));
    }
}

/// Spec R4, scenario 2 — MIXED open/closed inclusivity:
/// `range_tombstones` pk=3 was deleted with `ck1 > 1 AND ck1 <= 3`, so the
/// START bound is EXCLUSIVE and the END bound INCLUSIVE. Reported per bound,
/// never collapsed to one inclusivity for the whole marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_tombstone_mixed_inclusivity_is_reported_per_bound() {
    let Some((db, golden)) = open_table("range_tombstones").await else {
        return;
    };
    let result = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.range_tombstones_raw_sstable_data WHERE pk = 3"
        ))
        .await
        .expect("raw view point-key query must succeed");

    let partition = golden_partition(&golden, 3);
    let g_start = golden_range_bound(partition, "start");
    let g_end = golden_range_bound(partition, "end");
    // The whole point of pk=3 is that the two bounds DISAGREE; if a
    // regenerated fixture made them agree this case would silently become a
    // duplicate of the prefix case.
    assert_ne!(
        g_start["type"], g_end["type"],
        "golden precondition: pk=3's bounds must have DIFFERENT inclusivity — that is the \
         scenario"
    );

    let start = result
        .rows
        .iter()
        .find(|r| text_of(r, "row_kind").as_deref() == Some("range_tombstone_start"))
        .expect("a range_tombstone_start row must be returned");
    let end = result
        .rows
        .iter()
        .find(|r| text_of(r, "row_kind").as_deref() == Some("range_tombstone_end"))
        .expect("a range_tombstone_end row must be returned");

    assert_bound_matches_golden(start, g_start, "range_tombstone_start");
    assert_bound_matches_golden(end, g_end, "range_tombstone_end");

    assert_eq!(
        bool_of(start, "bound_inclusive"),
        Some(false),
        "pk=3's start bound is EXCLUSIVE (`ck1 > 1`)"
    );
    assert_eq!(
        bool_of(end, "bound_inclusive"),
        Some(true),
        "pk=3's end bound is INCLUSIVE (`ck1 <= 3`)"
    );
    assert_ne!(
        int_of(start, "ck1"),
        int_of(end, "ck1"),
        "the two bounds of pk=3's range sit at different clustering positions"
    );
}
