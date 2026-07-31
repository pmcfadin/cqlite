//! Issue #3095 — STATIC-column `SELECT` semantics on the Flight `do_get` row
//! route, asserted against Apache Cassandra 5.0.8's own rules over
//! CASSANDRA-WRITTEN bytes.
//!
//! # Oracle (authority, not CQLite's prior behaviour)
//!
//! `cassandra-5.0.8:src/java/org/apache/cassandra/cql3/statements/SelectStatement.java`,
//! `processPartition()`:
//!
//! * the static row is fetched OUT OF BAND — `Row staticRow = partition.staticRow()`
//!   (`db/rows/BaseRowIterator.java`) — so it is never an element of the row
//!   iteration and `partition.hasNext()` counts CLUSTERING rows only;
//! * with N > 0 clustering rows the per-row loop emits exactly N result rows and
//!   fills every `case STATIC:` slot from that partition-level `staticRow`, so the
//!   static value appears on EVERY row and there is NO extra `ck = null` row;
//! * with ZERO clustering rows it emits exactly ONE row — clustering and REGULAR
//!   columns null (`default: result.add((ByteBuffer) null)`), statics populated —
//!   and only when `restrictions.returnStaticContentOnPartitionWithNoRows()` holds
//!   (`restrictions/StatementRestrictions.java`: `queriesFullPartitions()` =
//!   `!hasClusteringColumnsRestrictions() && !hasRegularColumnsRestrictions()`).
//!   That branch `return`s, so the two shapes are mutually exclusive.
//!
//! # Fixtures are CASSANDRA-WRITTEN (hard requirement, #3042)
//!
//! A CQLite-written + CQLite-read round trip is INVARIANT to a uniform
//! serialization/assembly error and would additionally be confounded by the
//! write-side #1074 (statics emitted into the clustering row), so it cannot be the
//! oracle for this defect. Both fixtures here are real Apache Cassandra 5.0
//! container flushes with committed `Data.db` binaries and committed sstabledump
//! goldens:
//!
//! * `test_writeparity.static_clustering_shape` — ONE partition, a static row plus
//!   exactly one clustering row (`generate-write-load-parity.sh`, Cassandra 5.0.2);
//! * `test_deltas.static_with_rows` — a STATIC-ONLY partition (`pk = 99`, a
//!   `static_block` and no `row`) alongside three partitions of four clustering
//!   rows each (`generate-deltas.sh`, Cassandra 5.0.2). Every expectation below is
//!   read off that committed golden, then shaped by the Cassandra rules above.
//!
//! # Isolation
//!
//! `CQLITE_FLIGHT_MERGE_PATH` and `CQLITE_TTL_NOW_OVERRIDE_SECS` are
//! PROCESS-GLOBAL, so this file holds exactly ONE `#[test]` that runs every case
//! sequentially (the discipline `issue_3058_forced_path_differential.rs` uses).
//! Add a case to the list, never a second `#[test]`.
//!
//! Fixture contract: cases SKIP cleanly when the committed corpus is absent,
//! UNLESS `CQLITE_REQUIRE_FIXTURES=1` (which the gate sets), where absence is a
//! hard failure. A case returning ZERO rows where rows are expected always fails.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use arrow::array::Int64Array;
use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_flight::bypass::MERGE_PATH_ENV;
use cqlite_flight::filter::ScanSpec;
use cqlite_flight::producer::MergeProducer;
use cqlite_flight::service::CqliteFlightService;

/// Debug-only reader seam pinning the read-time reconciliation clock
/// (`now_clock.rs`). Neither fixture carries a TTL, so the pin only removes
/// wall-clock nondeterminism — it never selects between rows (#2642).
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// 2027-01-15T08:00:00Z — after every write in both fixtures.
const PINNED_NOW: i64 = 1_800_000_000;

/// One row rendered as an ordered `column -> value` map (nulls as `<null>`), so a
/// mismatch prints a readable diff and every column participates.
type Row = BTreeMap<String, String>;

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .to_path_buf()
}

fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("test-data/datasets"))
}

/// Resolve `<sstables>/<keyspace>/<table>-<uuid>/` whose `Data.db` binaries are
/// actually present, choosing the lexicographically FIRST such directory so a
/// keyspace holding several generations of the same table name (as `test_deltas`
/// does) resolves deterministically instead of by `read_dir` order.
fn fixture_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let ks_dir = datasets_root().join("sstables").join(keyspace);
    let prefix = format!("{table}-");
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&ks_dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
                && has_data_db(p)
        })
        .collect();
    candidates.sort();
    candidates.into_iter().next()
}

fn has_data_db(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
}

/// Stage exactly ONE fixture directory into a private `<tmp>/<keyspace>/<dir>/`
/// data root, so the service's `DirSource::resolve` cannot pick a DIFFERENT
/// same-named table directory than the one this test derived its expectations
/// from.
fn stage(keyspace: &str, dir: &Path) -> std::io::Result<tempfile::TempDir> {
    let temp = tempfile::tempdir()?;
    let leaf = dir
        .file_name()
        .ok_or_else(|| std::io::Error::other("fixture dir has no file name"))?;
    let dest = temp.path().join(keyspace).join(leaf);
    std::fs::create_dir_all(&dest)?;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            std::fs::copy(entry.path(), dest.join(entry.file_name()))?;
        }
    }
    Ok(temp)
}

fn ticket_json(keyspace: &str, table: &str, ddl: &str) -> serde_json::Value {
    serde_json::json!({ "keyspace": keyspace, "table": table, "ddl": ddl })
}

/// Render every column of every row through Arrow's own formatter, so the
/// comparison covers values AND nullness for every CQL type.
fn push_rows(batch: &RecordBatch, out: &mut Vec<Row>) {
    let schema = batch.schema();
    let formatters: Vec<_> = batch
        .columns()
        .iter()
        .map(|c| {
            arrow::util::display::ArrayFormatter::try_new(
                c.as_ref(),
                &arrow::util::display::FormatOptions::default(),
            )
            .expect("array formatter")
        })
        .collect();
    for r in 0..batch.num_rows() {
        let mut row = Row::new();
        for (c, field) in schema.fields().iter().enumerate() {
            let rendered = if batch.column(c).is_null(r) {
                "<null>".to_string()
            } else {
                formatters[c].value(r).to_string()
            };
            row.insert(field.name().clone(), rendered);
        }
        out.push(row);
    }
}

// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_rows(svc: &CqliteFlightService, ticket: &serde_json::Value) -> Vec<Row> {
    let bytes = serde_json::to_vec(ticket).expect("ticket json");
    let resp = svc
        .do_get(Request::new(Ticket::new(bytes)))
        .await
        .expect("do_get")
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        push_rows(&batch.expect("record batch"), &mut rows);
    }
    rows
}

/// Run `ticket` with a forced arm, returning its rows and the probe delta that
/// proves which arm actually ran.
async fn run_forced(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    arm: &str,
) -> (Vec<Row>, ReadPathProbe) {
    std::env::set_var(MERGE_PATH_ENV, arm);
    let before = ReadPathProbe::snapshot();
    let rows = do_get_rows(svc, ticket).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);
    std::env::remove_var(MERGE_PATH_ENV);
    (rows, delta)
}

/// Drain an AGGREGATING `do_get` into its partial-aggregate `cnt` values.
// arrow-flight's `FlightError` Err type has a framework-fixed large size (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_counts(svc: &CqliteFlightService, ticket: &serde_json::Value) -> Vec<i64> {
    let bytes = serde_json::to_vec(ticket).expect("ticket json");
    let resp = svc
        .do_get(Request::new(Ticket::new(bytes)))
        .await
        .expect("do_get")
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("record batch");
        let col = batch
            .column_by_name("cnt")
            .expect("the partial-aggregate schema carries `cnt`");
        let counts = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Count partials are Int64");
        for i in 0..counts.len() {
            out.push(counts.value(i));
        }
    }
    out
}

/// Drive the BUFFERED collect route (`MergeProducer::produce_from_paths`, behind the
/// public `produce` family) directly over the fixture's `Data.db` files.
fn produce_from_paths_rows(dir: &Path, ddl: &str) -> Vec<Row> {
    let schema = cqlite_core::schema::parse_cql_schema(ddl).expect("ddl parses");
    let producer = MergeProducer::with_spec(schema, 8192, ScanSpec::default()).expect("producer");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("fixture dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "the fixture must carry a Data.db");
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());
    let batches = producer.produce_from_paths(paths);
    std::env::remove_var(TTL_NOW_ENV);
    let batches = batches.expect("the buffered collect route runs");
    let mut rows = Vec::new();
    for batch in &batches {
        push_rows(batch, &mut rows);
    }
    rows
}

/// Build one expected row from `(column, value)` pairs; `None` is a null column.
fn row(pairs: &[(&str, Option<&str>)]) -> Row {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.unwrap_or("<null>").to_string()))
        .collect()
}

fn sorted(rows: &[Row]) -> Vec<Row> {
    let mut out = rows.to_vec();
    out.sort();
    out
}

/// How strongly `expected` pins the shape of the result: the exact EMISSION ORDER,
/// or only its content.
///
/// Cassandra's own order is only a TOTAL order over a result set when that set
/// comes from a single partition — `UnfilteredRowIterator` walks a partition in
/// CLUSTERING order (the table comparator) and `processPartition()` emits in
/// iteration order, but the order BETWEEN partitions is murmur3 TOKEN order, which
/// these expectations deliberately do not encode. So the order-sensitive
/// assertion is scoped to the cases where Cassandra genuinely guarantees one.
#[derive(Clone, Copy)]
enum ExpectedOrder {
    /// `expected` IS the sequence Cassandra emits — a single-partition result (in
    /// clustering order) or a single-row result. Asserted position-by-position.
    Exact,
    /// A MULTI-partition result: only the multiset is asserted, because a total
    /// order across partitions is not guaranteed by clustering alone. The
    /// arm-vs-arm check still pins BOTH arms to the same order.
    Multiset,
}

/// Assert `ticket` returns exactly `expected` on BOTH arms at the pinned `now` —
/// in EMISSION ORDER for `ExpectedOrder::Exact`, as a multiset otherwise — that the
/// two arms genuinely DIFFERED, and that their row order is identical (issue #3095
/// AC1/AC2/AC3).
async fn assert_static_semantics(
    label: &str,
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    expected: &[Row],
    order: ExpectedOrder,
    failures: &mut Vec<String>,
) {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());
    let (merge_rows, merge_delta) = run_forced(svc, ticket, "merge").await;
    let (bypass_rows, bypass_delta) = run_forced(svc, ticket, "bypass").await;
    std::env::remove_var(TTL_NOW_ENV);

    let want = sorted(expected);
    for (arm, got) in [("merge", &merge_rows), ("bypass", &bypass_rows)] {
        let matches = match order {
            ExpectedOrder::Exact => got.as_slice() == expected,
            ExpectedOrder::Multiset => sorted(got) == want,
        };
        if matches {
            continue;
        }
        // An order-only divergence gets its own headline: the content is
        // Cassandra's, the SEQUENCE is not, and that distinction is what makes the
        // failure actionable.
        let kind = if matches!(order, ExpectedOrder::Exact) && sorted(got) == want {
            "CLUSTERING-ORDER MISMATCH vs Cassandra 5.0.8 (content matches; a \
             single-partition result is emitted in clustering order)"
        } else {
            "STATIC SEMANTICS MISMATCH vs Cassandra 5.0.8 processPartition()"
        };
        // Show the compared forms, so the printed diff is the one that failed.
        let (want_shown, got_shown) = match order {
            ExpectedOrder::Exact => (expected.to_vec(), got.to_vec()),
            ExpectedOrder::Multiset => (want.clone(), sorted(got)),
        };
        failures.push(format!(
            "case {label} [{arm} arm]: {kind}\n  expected ({} rows): {:#?}\n  \
             got      ({} rows): {:#?}",
            want_shown.len(),
            want_shown,
            got_shown.len(),
            got_shown,
        ));
    }
    // AC3: the two arms agree over the SAME bytes at a PINNED `now`, in ORDER too.
    if merge_rows != bypass_rows {
        failures.push(format!(
            "case {label}: the two arms DISAGREE (order-sensitive)\n  merge ({} rows): \
             {:#?}\n  bypass ({} rows): {:#?}",
            merge_rows.len(),
            merge_rows,
            bypass_rows.len(),
            bypass_rows,
        ));
    }
    // Anti-vacuity: the comparison above is only a differential if the arms really
    // differed. The merge run must have merged; the bypass run must NOT have.
    if merge_delta.mergers_built == 0 {
        failures.push(format!(
            "case {label}: the forced-merge run built no merger — the arm comparison \
             would be vacuous"
        ));
    }
    if bypass_delta.mergers_built != 0 {
        failures.push(format!(
            "case {label}: the forced-bypass run still built {} merger(s) — a \
             static-bearing schema must now be servable by the single-generation arm \
             (issue #3095 AC5)",
            bypass_delta.mergers_built
        ));
    }
}

// ---------------------------------------------------------------------------
// Fixture 1: test_writeparity.static_clustering_shape (Cassandra 5.0.2)
// ---------------------------------------------------------------------------

const WP_KS: &str = "test_writeparity";
const WP_TBL: &str = "static_clustering_shape";
/// Verbatim from `test-data/schemas/write-load-parity.cql`.
const WP_DDL: &str = "CREATE TABLE test_writeparity.static_clustering_shape \
     (id int, ck int, sdata text static, rdata text, PRIMARY KEY (id, ck))";

// ---------------------------------------------------------------------------
// Fixture 2: test_deltas.static_with_rows (Cassandra 5.0.2)
// ---------------------------------------------------------------------------

const DL_KS: &str = "test_deltas";
const DL_TBL: &str = "static_with_rows";
/// Verbatim from `test-data/schemas/deltas.cql`.
const DL_DDL: &str = "CREATE TABLE test_deltas.static_with_rows \
     (pk int, ck int, static_col text static, row_col text, PRIMARY KEY (pk, ck))";

// ---------------------------------------------------------------------------
// Fixture 3: test_tomb.static_with_tombstones (Cassandra 5.0, COMMITTED)
// ---------------------------------------------------------------------------

const TB_KS: &str = "test_tomb";
const TB_TBL: &str = "static_with_tombstones";
/// Verbatim from `test-data/schemas/tombstone-parity.cql`.
const TB_DDL: &str = "CREATE TABLE test_tomb.static_with_tombstones \
     (pk int, ck int, stat_col text static, row_col text, PRIMARY KEY (pk, ck))";

// ---------------------------------------------------------------------------
// Fixture 4: test_oa.static_table (Cassandra 5.0, `oa` FORMAT; fetch-only)
// ---------------------------------------------------------------------------

const OA_KS: &str = "test_oa";
const OA_TBL: &str = "static_table";
/// Verbatim from `test-data/schemas/oa-test.cql`.
const OA_DDL: &str = "CREATE TABLE test_oa.static_table \
     (partition_key uuid, clustering_key int, static_col text static, row_data text, \
      PRIMARY KEY (partition_key, clustering_key))";

/// Every row Cassandra returns for `SELECT * FROM test_deltas.static_with_rows`,
/// derived from the committed sstabledump golden + `processPartition()`: each of
/// the 12 clustering rows carries its partition's static value, and the
/// STATIC-ONLY partition `pk = 99` contributes exactly ONE row with null
/// clustering and null regular columns.
fn deltas_expected_select_star() -> Vec<Row> {
    let mut out = Vec::new();
    for (pk, statik) in [
        (1, "static_val_1"),
        (2, "static_val_2"),
        (3, "static_val_3"),
    ] {
        for ck in 1..=4 {
            out.push(row(&[
                ("pk", Some(&pk.to_string())),
                ("ck", Some(&ck.to_string())),
                ("static_col", Some(statik)),
                ("row_col", Some(&format!("row_{pk}_{ck}"))),
            ]));
        }
    }
    out.push(row(&[
        ("pk", Some("99")),
        ("ck", None),
        ("static_col", Some("static_only_val")),
        ("row_col", None),
    ]));
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn flight_static_column_semantics_match_cassandra() {
    let mut failures: Vec<String> = Vec::new();
    // Per-fixture flags, NOT a count (both reviewers): a `ran >= 2` floor is satisfied
    // by ANY two of the three cases, so with the fetch-only `test_writeparity` fixture
    // present a COMMITTED fixture could be missing and the lane would still pass.
    let mut ran_deltas = false;
    let mut ran_tomb = false;
    let mut ran_writeparity = false;

    // -- AC1: static + N clustering rows → exactly N rows, statics on each ------
    match fixture_dir(WP_KS, WP_TBL) {
        Some(dir) => {
            let temp = stage(WP_KS, &dir).expect("stage fixture");
            let svc = CqliteFlightService::new(temp.path().to_path_buf(), 8192);
            assert_static_semantics(
                "static_clustering_shape/select-star",
                &svc,
                &ticket_json(WP_KS, WP_TBL, WP_DDL),
                &[row(&[
                    ("id", Some("1")),
                    ("ck", Some("7")),
                    ("sdata", Some("static-val")),
                    ("rdata", Some("row-val")),
                ])],
                // ONE partition (`id = 1`), one row — Cassandra's emission order.
                ExpectedOrder::Exact,
                &mut failures,
            )
            .await;
            ran_writeparity = true;
        }
        None => {
            let msg = format!("{WP_KS}.{WP_TBL}: fixture absent");
            if require_fixtures() {
                failures.push(format!("REQUIRE_FIXTURES: {msg}"));
            } else {
                eprintln!("SKIP {msg}");
            }
        }
    }

    // -- AC1 + AC2: statics on every row AND the static-only partition ---------
    match fixture_dir(DL_KS, DL_TBL) {
        Some(dir) => {
            let temp = stage(DL_KS, &dir).expect("stage fixture");
            let svc = CqliteFlightService::new(temp.path().to_path_buf(), 8192);

            assert_static_semantics(
                "static_with_rows/select-star",
                &svc,
                &ticket_json(DL_KS, DL_TBL, DL_DDL),
                &deltas_expected_select_star(),
                // FOUR partitions (pk = 1, 2, 3, 99): the inter-partition order is
                // token order, which this expectation does not encode.
                ExpectedOrder::Multiset,
                &mut failures,
            )
            .await;

            // AC2, second half: with a CLUSTERING-column restriction Cassandra
            // ignores partitions that are empty outside their static content
            // (`returnStaticContentOnPartitionWithNoRows()` is false), so `pk = 99`
            // contributes ZERO rows while the three row-bearing partitions each
            // return their `ck = 1` row WITH the static value.
            let mut ck_restricted = ticket_json(DL_KS, DL_TBL, DL_DDL);
            ck_restricted["predicates"] =
                serde_json::json!([{ "column": "ck", "op": "Equal", "value": 1 }]);
            assert_static_semantics(
                "static_with_rows/clustering-restriction",
                &svc,
                &ck_restricted,
                &[
                    row(&[
                        ("pk", Some("1")),
                        ("ck", Some("1")),
                        ("static_col", Some("static_val_1")),
                        ("row_col", Some("row_1_1")),
                    ]),
                    row(&[
                        ("pk", Some("2")),
                        ("ck", Some("1")),
                        ("static_col", Some("static_val_2")),
                        ("row_col", Some("row_2_1")),
                    ]),
                    row(&[
                        ("pk", Some("3")),
                        ("ck", Some("1")),
                        ("static_col", Some("static_val_3")),
                        ("row_col", Some("row_3_1")),
                    ]),
                ],
                // One row from EACH of three partitions — token order across them.
                ExpectedOrder::Multiset,
                &mut failures,
            )
            .await;

            // A STATIC-column restriction does NOT disable the static-only row.
            // Cassandra authority (`StatementRestrictions.java`, cassandra-5.0.8):
            // `hasRegularColumnsRestrictions =
            //  nonPrimaryKeyRestrictions.hasRestrictionFor(ColumnMetadata.Kind.REGULAR)`
            // — a STATIC restriction lands in `nonPrimaryKeyRestrictions` but its kind
            // is STATIC, so it is NOT counted, `queriesFullPartitions()` stays true and
            // `returnStaticContentOnPartitionWithNoRows()` remains TRUE. So the
            // static-only partition's row IS produced and then filtered by the
            // restriction itself — here it MATCHES, so `pk = 99` is the only row, while
            // the three row-bearing partitions (whose static values differ) contribute
            // none.
            let mut static_restricted = ticket_json(DL_KS, DL_TBL, DL_DDL);
            static_restricted["predicates"] = serde_json::json!([
                { "column": "static_col", "op": "Equal", "value": "static_only_val" }
            ]);
            assert_static_semantics(
                "static_with_rows/static-column-restriction",
                &svc,
                &static_restricted,
                &[row(&[
                    ("pk", Some("99")),
                    ("ck", None),
                    ("static_col", Some("static_only_val")),
                    ("row_col", None),
                ])],
                // The restriction leaves exactly one surviving partition's row.
                ExpectedOrder::Exact,
                &mut failures,
            )
            .await;

            // AC2, second half again, this time a REGULAR-column restriction —
            // the other disjunct of `queriesFullPartitions()`.
            let mut regular_restricted = ticket_json(DL_KS, DL_TBL, DL_DDL);
            regular_restricted["predicates"] =
                serde_json::json!([{ "column": "row_col", "op": "Equal", "value": "row_2_3" }]);
            assert_static_semantics(
                "static_with_rows/regular-restriction",
                &svc,
                &regular_restricted,
                &[row(&[
                    ("pk", Some("2")),
                    ("ck", Some("3")),
                    ("static_col", Some("static_val_2")),
                    ("row_col", Some("row_2_3")),
                ])],
                // The restriction leaves exactly one row, from one partition.
                ExpectedOrder::Exact,
                &mut failures,
            )
            .await;

            ran_deltas = true;
        }
        None => {
            let msg = format!("{DL_KS}.{DL_TBL}: fixture absent");
            if require_fixtures() {
                failures.push(format!("REQUIRE_FIXTURES: {msg}"));
            } else {
                eprintln!("SKIP {msg}");
            }
        }
    }

    // -- B1: a live static row ALONGSIDE row / cell / range tombstones -------
    // Cassandra's `partition.hasNext()` is evaluated over the ALREADY-FILTERED
    // `RowIterator`, and the static row is a PARTITION-level object that can never
    // revive a deleted clustering row. So the row-tombstoned `ck = 2` and the
    // range-deleted `ck = 4..5` are HIDDEN, while `ck = 3` (whose `row_col` cell was
    // tombstoned but whose row liveness survives) is returned with a NULL `row_col`.
    // Every surviving row carries the static value. Read off the committed golden
    // (`nb-1-big-Data.db.jsonl`) + the schema's documented write pattern.
    match fixture_dir(TB_KS, TB_TBL) {
        Some(dir) => {
            let temp = stage(TB_KS, &dir).expect("stage fixture");
            let svc = CqliteFlightService::new(temp.path().to_path_buf(), 8192);
            // FULL `SELECT *`, asserted on BOTH arms (issue #3140, the fail-closed
            // guard now RETIRED). This fixture's `ck = 3` carries a simple CELL
            // tombstone on `row_col`, which the arms used to handle differently: the
            // merge arm dropped it (the column reads null — Cassandra's answer) while
            // the single-generation fast arm surfaced a raw `Value::Tombstone` that the
            // Arrow encoder rejected, aborting `do_get` with zero rows. PR #3122 fixed
            // the fast arm at its source (`row_decoder`'s
            // `PartitionShadow::cell_tombstone_dropped`), so this is now an ordinary
            // both-arms differential — and `assert_static_semantics` is what makes that
            // load-bearing: it requires the forced-bypass leg to build ZERO mergers, so
            // the case cannot pass by silently routing back to the merge arm. `row_col`
            // is deliberately PROJECTED IN — the whole point is that Cassandra's
            // `row_col` values, including the null on `ck = 3`, are asserted on the FAST
            // arm's own decode.
            assert_static_semantics(
                "static_with_tombstones/select-star(both arms, #3140)",
                &svc,
                &ticket_json(TB_KS, TB_TBL, TB_DDL),
                &[
                    row(&[
                        ("pk", Some("1")),
                        ("ck", Some("1")),
                        ("stat_col", Some("surviving_static")),
                        ("row_col", Some("row_1")),
                    ]),
                    row(&[
                        ("pk", Some("1")),
                        ("ck", Some("3")),
                        ("stat_col", Some("surviving_static")),
                        ("row_col", None),
                    ]),
                    row(&[
                        ("pk", Some("1")),
                        ("ck", Some("6")),
                        ("stat_col", Some("surviving_static")),
                        ("row_col", Some("row_6")),
                    ]),
                ],
                // ORDER-SENSITIVE, and load-bearing for this PR: a SINGLE partition
                // (`pk = 1`), so Cassandra's answer is these three rows in ASCENDING
                // CLUSTERING order (`ck` 1, 3, 6 — the default `ASC` comparator, with
                // the row-tombstoned `ck = 2` and the range-deleted `ck = 4..5`
                // absent). Retiring the `StaticColumnsWithDeletions` guard is
                // justified only if the fast arm's results are CORRECT, and correct
                // includes Cassandra's clustering order — a both-arms inversion would
                // slip past a multiset comparison and past the arm-vs-arm check.
                ExpectedOrder::Exact,
                &mut failures,
            )
            .await;
            ran_tomb = true;
        }
        None => {
            let msg = format!("{TB_KS}.{TB_TBL}: fixture absent");
            if require_fixtures() {
                failures.push(format!("REQUIRE_FIXTURES: {msg}"));
            } else {
                eprintln!("SKIP {msg}");
            }
        }
    }

    // -- NB2: the AGGREGATE route (every `SELECT count(*)` ticket) ------------
    // `bypass_reason` returns `Aggregating` for every aggregating ticket, so this
    // ALWAYS lands on `drive_aggregate`. Cassandra's `count(*)` counts exactly the
    // rows `SELECT *` returns — 13 here, including the pk=99 static-only partition's
    // row and EXCLUDING the phantom `ck = null` rows the pre-fix route counted (it
    // reported 16). This is the route whose answer disagreed with `SELECT *` over the
    // same bytes until the static choke point was hoisted.
    match fixture_dir(DL_KS, DL_TBL) {
        Some(dir) => {
            let temp = stage(DL_KS, &dir).expect("stage fixture");
            let svc = CqliteFlightService::new(temp.path().to_path_buf(), 8192);
            let mut ticket = ticket_json(DL_KS, DL_TBL, DL_DDL);
            ticket["aggregation"] = serde_json::json!({
                "group_by": [],
                "aggregates": [{ "func": "Count", "column": null, "output": "cnt" }]
            });
            std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());
            let counted = do_get_counts(&svc, &ticket).await;
            std::env::remove_var(TTL_NOW_ENV);
            let expected = deltas_expected_select_star().len() as i64;
            if counted != vec![expected] {
                failures.push(format!(
                    "case static_with_rows/count-star: the AGGREGATE route must count \
                     exactly the rows SELECT * returns ({expected}); got {counted:?}"
                ));
            }
        }
        None => {
            let msg = format!("{DL_KS}.{DL_TBL}: fixture absent (count-star case)");
            if require_fixtures() {
                failures.push(format!("REQUIRE_FIXTURES: {msg}"));
            } else {
                eprintln!("SKIP {msg}");
            }
        }
    }

    // -- NB2: the BUFFERED collect route (`produce_from_paths`) --------------
    // The public non-streaming surface behind `produce` / `produce_from_paths` /
    // `produce_from_resolved` drives `drive_merge`, which had the same gap. Asserted
    // on the row SHAPE, not just a count.
    match fixture_dir(DL_KS, DL_TBL) {
        Some(dir) => {
            let rows = produce_from_paths_rows(&dir, DL_DDL);
            let want = sorted(&deltas_expected_select_star());
            if sorted(&rows) != want {
                failures.push(format!(
                    "case static_with_rows/produce_from_paths: the BUFFERED collect \
                     route diverges from Cassandra\n  expected ({} rows): {:#?}\n  \
                     got      ({} rows): {:#?}",
                    want.len(),
                    want,
                    rows.len(),
                    sorted(&rows)
                ));
            }
        }
        None => {
            let msg = format!("{DL_KS}.{DL_TBL}: fixture absent (produce_from_paths case)");
            if require_fixtures() {
                failures.push(format!("REQUIRE_FIXTURES: {msg}"));
            } else {
                eprintln!("SKIP {msg}");
            }
        }
    }

    // -- AC1 on the `oa` FORMAT --------------------------------------------
    // The same static rule on a different on-disk format version: `oa` (BTI-era BIG),
    // vs `nb` for every other fixture here. Statics on each of two partitions' two
    // clustering rows, no phantom `ck = null` row. Fetch-only (not committed), so it
    // SKIPs on a bare checkout and is never part of the committed-fixture floor.
    match fixture_dir(OA_KS, OA_TBL) {
        Some(dir) => {
            let temp = stage(OA_KS, &dir).expect("stage fixture");
            let svc = CqliteFlightService::new(temp.path().to_path_buf(), 8192);
            // PROJECTION excludes `partition_key`, and not to weaken the case: CQLite
            // renders a `uuid` WITHOUT hyphens
            // (`11111111111111111111000000000001` where Cassandra and sstabledump
            // render `11111111-1111-1111-1111-000000000001`). Both arms agree on it and
            // it has nothing to do with static columns, so it is REPORTED as a separate
            // defect rather than asserted-as-correct here or fixed in this diff. The
            // partition association is still pinned: the two partitions carry disjoint
            // `row_data` values, so a static value landing on the wrong partition's rows
            // fails this case.
            let mut ticket = ticket_json(OA_KS, OA_TBL, OA_DDL);
            ticket["columns"] = serde_json::json!(["clustering_key", "static_col", "row_data"]);
            let mut want = Vec::new();
            for (statik, rows) in [
                ("shared static value A", ["row 1 data", "row 2 data"]),
                (
                    "shared static value B",
                    ["row 1 in partition 2", "row 2 in partition 2"],
                ),
            ] {
                for (i, row_data) in rows.iter().enumerate() {
                    want.push(row(&[
                        ("clustering_key", Some(&(i + 1).to_string())),
                        ("static_col", Some(statik)),
                        ("row_data", Some(row_data)),
                    ]));
                }
            }
            assert_static_semantics(
                "oa/static_table/select-star",
                &svc,
                &ticket,
                &want,
                // TWO partitions — token order across them is not encoded here (and
                // `partition_key` is projected out, so it could not be checked).
                ExpectedOrder::Multiset,
                &mut failures,
            )
            .await;
        }
        None => eprintln!(
            "SKIP {OA_KS}.{OA_TBL}: fixture absent (fetch-only `oa` corpus; additive \
             format coverage, never the floor)"
        ),
    }

    assert!(
        failures.is_empty(),
        "issue #3095 static-column parity failures:\n{}",
        failures.join("\n\n")
    );
    // Fail-closed floor, asserted PER FIXTURE and honest about what ran. Both
    // COMMITTED Cassandra fixtures ship in the repo (binaries included), so a skip on
    // either is a hard failure regardless of `CQLITE_REQUIRE_FIXTURES` — that is what
    // makes AC1/AC2/AC3 non-vacuous on any machine.
    // `test_writeparity.static_clustering_shape` is deliberately EXCLUDED from the
    // fetched corpus (`test-data/validation-matrix.md`), so it may legitimately skip;
    // it is additive coverage and never part of the floor.
    assert!(
        ran_deltas,
        "the COMMITTED fixture {DL_KS}.{DL_TBL} did not run — it ships in the repo, so \
         a skip means the corpus or the directory resolution broke, and AC1/AC2/AC3 \
         would be unverified"
    );
    assert!(
        ran_tomb,
        "the COMMITTED fixture {TB_KS}.{TB_TBL} did not run — it ships in the repo, so \
         a skip means the corpus or the directory resolution broke, and the B1 \
         tombstone-vs-static case would be unverified"
    );
    if !ran_writeparity {
        eprintln!(
            "NOTE {WP_KS}.{WP_TBL} skipped (fetch-only fixture, excluded from the \
             published corpus) — the COMMITTED fixtures above still ran"
        );
    }
}
