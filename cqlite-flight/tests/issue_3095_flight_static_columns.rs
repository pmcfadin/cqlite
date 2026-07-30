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

use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_flight::bypass::MERGE_PATH_ENV;
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

/// Assert `ticket` returns exactly `expected` (as a multiset) on BOTH arms at the
/// pinned `now`, that the two arms genuinely DIFFERED, and that their row ORDER is
/// identical (issue #3095 AC1/AC2/AC3).
async fn assert_static_semantics(
    label: &str,
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    expected: &[Row],
    failures: &mut Vec<String>,
) {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());
    let (merge_rows, merge_delta) = run_forced(svc, ticket, "merge").await;
    let (bypass_rows, bypass_delta) = run_forced(svc, ticket, "bypass").await;
    std::env::remove_var(TTL_NOW_ENV);

    let want = sorted(expected);
    for (arm, got) in [("merge", &merge_rows), ("bypass", &bypass_rows)] {
        if sorted(got) != want {
            failures.push(format!(
                "case {label} [{arm} arm]: STATIC SEMANTICS MISMATCH vs Cassandra 5.0.8 \
                 processPartition()\n  expected ({} rows): {:#?}\n  got      ({} rows): {:#?}",
                want.len(),
                want,
                got.len(),
                sorted(got),
            ));
        }
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
    let mut ran = 0usize;

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
                &mut failures,
            )
            .await;
            ran += 1;
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
                &mut failures,
            )
            .await;

            ran += 1;
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

    assert!(
        failures.is_empty(),
        "issue #3095 static-column parity failures:\n{}",
        failures.join("\n\n")
    );
    if require_fixtures() {
        assert_eq!(
            ran, 2,
            "CQLITE_REQUIRE_FIXTURES=1: both committed Cassandra static fixtures must run"
        );
    } else if ran == 0 {
        eprintln!(
            "SKIP issue_3095_flight_static_columns: no fixtures present \
             (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)"
        );
    }
}
