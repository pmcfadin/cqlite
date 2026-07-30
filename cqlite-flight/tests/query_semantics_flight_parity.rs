//! Issue #2374 — the QUERY-SEMANTICS parity oracle, over the FLIGHT `do_get`
//! path (the Trino/Flight read surface), mirroring the in-core lane
//! `cqlite-core/tests/query_semantics_oracle_parity.rs`.
//!
//! The physical sstabledump JSONL goldens (`*-Data.db.jsonl`) enumerate every
//! on-disk cell — including tombstones, deleted rows, and expired-but-
//! uncompacted TTL cells — so a row-count/value comparison against them
//! structurally CANNOT catch a read-time-reconciliation bug: when the Flight
//! producer fails to reconcile, both sides still contain the shadowed/expired
//! rows and parity passes while a real Cassandra `SELECT` diverges. This lane
//! guards the Flight path against exactly that (the #2789 read-time TTL bug this
//! test was written to expose, before the fix threaded a reconciliation `now`
//! into the Flight k-way merger).
//!
//! This test compares Flight `do_get` output to the POST-RECONCILIATION result
//! set a real Cassandra returns, recorded per-fixture in
//! `test-data/query-semantics-oracle.json`. TTL expiry is evaluated at a PINNED
//! `now` (per case) via the debug-only `CQLITE_TTL_NOW_OVERRIDE_SECS` reader
//! seam, so it is deterministic and never wall-clock-flaky.
//!
//! DEBUG-ONLY PIN: the `CQLITE_TTL_NOW_OVERRIDE_SECS` seam is
//! `#[cfg(debug_assertions)]`, so the pin only takes effect in a debug build.
//! The agent gate runs debug, so the lane always exercises the pinned path
//! there. The anti-empty-pass contract below guarantees the test can never
//! green-pass vacuously.
//!
//! Anti-empty-pass / SKIP contract (IDENTICAL to the in-core lane):
//!   * Each case carries a non-empty `expected_rows`; a `0-rows-when-rows-
//!     expected` result is a HARD mismatch failure, never a vacuous pass.
//!   * `physical_row_count` is re-derived from the committed golden JSONL and
//!     asserted, proving the fixture's rows are physically present on disk.
//!   * When the committed fixture (or its `*.db` binaries) is absent, the case
//!     SKIPs cleanly — UNLESS `CQLITE_REQUIRE_FIXTURES=1` (the agent-gate
//!     `flight-query-semantics-oracle` component sets it), in which case an
//!     absent/empty fixture is a hard FAIL (fail-closed).
//!
//! Cases run SEQUENTIALLY inside ONE test so the process-global TTL-now env seam
//! is never mutated concurrently.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use serde::Deserialize;
use tonic::Request;

use cqlite_flight::service::CqliteFlightService;

/// Debug-only reader seam (see `now_clock.rs`): pins the read-time TTL "now"
/// clock (epoch seconds) so a long-expired fixture is read deterministically
/// "as of" the oracle's pinned evaluation time.
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// Fail-closed switch: when set, an absent/empty committed fixture is a hard
/// failure instead of a clean skip (the agent-gate `flight-query-semantics-
/// oracle` component sets it, matching the in-core `query-semantics-oracle`).
fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Oracle model (test-data/query-semantics-oracle.json)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Oracle {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    id: String,
    keyspace: String,
    table: String,
    fixture_dir_prefix: String,
    sstable_prefix: String,
    schema: String,
    #[allow(dead_code)]
    query: String,
    pinned_now_secs: i64,
    physical_row_count: usize,
    /// STATIC-ONLY partitions in the committed golden — partitions carrying a
    /// `static_block` and ZERO `row` entries (issue #3095). Cassandra returns
    /// exactly ONE result row for each, and that row has no physical `row` entry
    /// behind it, so this is the amount by which a correct semantic result may
    /// LEGITIMATELY exceed `physical_row_count`. Re-derived from the golden and
    /// asserted below; defaults to 0, keeping the strict guard for every other case.
    #[serde(default)]
    physical_static_only_partitions: usize,
    /// The keyspace-qualified `CREATE TABLE` the Flight ticket must carry.
    ///
    /// Required for any case whose table is NOT the
    /// `(id int, ck int, v text, PRIMARY KEY (id, ck))` shape every table in
    /// `compaction-tombstone-ttl-parity.cql` shares — e.g. the static-column case
    /// (issue #3095), whose DDL must declare the column `static`. Authored in the
    /// oracle next to the query it belongs to; still cross-checked against the
    /// schema file by [`ddl_for`].
    #[serde(default)]
    ddl: Option<String>,
    /// Ordered map per row so JSON author-order is preserved for readable diffs.
    expected_rows: Vec<serde_json::Map<String, serde_json::Value>>,
    /// Explicit opt-in for a case whose correct semantic result is ZERO rows.
    /// Required whenever `expected_rows` is empty — see the module doc's anti-
    /// empty-pass contract. Defaults to `false`.
    #[serde(default)]
    expect_empty: bool,
    /// Whether this case is expressible on the FLIGHT lane. A Flight ticket is a
    /// whole-table projection scan with NO `WHERE` clause, so an oracle case whose
    /// `query` carries a partition/clustering predicate (e.g. the BTI
    /// clustering-slice case, issue #3002) cannot be run here at all and is
    /// declared `"flight_lane": false` in the oracle. Defaults to `true`, so a case
    /// that omits the field is still asserted on this lane (never a silent skip).
    #[serde(default = "default_true")]
    flight_lane: bool,
}

/// `serde` default for [`Case::flight_lane`] — a case is on the Flight lane unless
/// it explicitly opts out.
fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Path resolution (mirrors the in-core lane)
// ---------------------------------------------------------------------------

/// Repo root = the parent of this crate's manifest dir (`<repo>/cqlite-flight`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-flight has a parent repo dir")
        .to_path_buf()
}

/// The `sstables/` root. Prefer `CQLITE_DATASETS_ROOT` when it actually holds the
/// committed keyspace; otherwise fall back to the in-repo committed corpus.
fn sstables_root(keyspace: &str) -> Option<PathBuf> {
    let candidates = [
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(|r| PathBuf::from(r).join("sstables")),
        Some(
            repo_root()
                .join("test-data")
                .join("datasets")
                .join("sstables"),
        ),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|root| root.join(keyspace).is_dir())
}

fn schema_path(file: &str) -> Option<PathBuf> {
    let candidates = [
        std::env::var("CQLITE_DATASETS_ROOT").ok().and_then(|r| {
            PathBuf::from(r)
                .parent()
                .map(|p| p.join("schemas").join(file))
        }),
        Some(repo_root().join("test-data").join("schemas").join(file)),
    ];
    candidates.into_iter().flatten().find(|p| p.exists())
}

/// Resolve `<sstables>/<keyspace>/<prefix><uuid>/` for a case.
///
/// A keyspace can hold SEVERAL directories for the same table name (each flush
/// campaign mints a new CFID — `test_deltas` ships three `static_with_rows-*`),
/// and only some of them carry the committed/fetched `Data.db`. So candidates are
/// SORTED (deterministic, never `read_dir` order) and one holding this case's
/// `<sstable_prefix>-Data.db` is preferred; with none, the first match is returned
/// so the caller's "not fetched" SKIP / fail-closed path still reports the miss.
fn fixture_dir(
    sstables_root: &Path,
    keyspace: &str,
    prefix: &str,
    sstable_prefix: &str,
) -> Option<PathBuf> {
    let ks_dir = sstables_root.join(keyspace);
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&ks_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(prefix))
                    .unwrap_or(false)
        })
        .collect();
    candidates.sort();
    let data_db = format!("{sstable_prefix}-Data.db");
    candidates
        .iter()
        .find(|p| p.join(&data_db).exists())
        .cloned()
        .or_else(|| candidates.into_iter().next())
}

/// Count `type == "row"` entries in the committed sstabledump JSONL golden
/// (anti-empty-pass: proves the physical rows are really on disk).
fn golden_row_count(dir: &Path, sstable_prefix: &str) -> Option<usize> {
    let jsonl = dir.join(format!("{sstable_prefix}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&jsonl).ok()?;
    let mut total = 0usize;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).ok()?;
        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
            total += rows
                .iter()
                .filter(|r| r.get("type").and_then(|t| t.as_str()) == Some("row"))
                .count();
        }
    }
    Some(total)
}

/// Stage exactly ONE fixture directory into a private `<tmp>/<keyspace>/<dir>/`
/// data root for the service to resolve.
///
/// Load-bearing, not hygiene: a keyspace can hold SEVERAL directories for the same
/// table name (`test_deltas` ships three `static_with_rows-*`, only one with the
/// committed binaries), and `DirSource::resolve` picks one by directory listing. Without
/// staging, the service could read a DIFFERENT generation than the one this lane
/// derived `physical_row_count` from — comparing a golden against bytes it does not
/// describe (and, when the resolved dir has no `Data.db`, silently returning 0 rows).
fn stage_case_dir(keyspace: &str, dir: &Path) -> Result<tempfile::TempDir, String> {
    let temp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let leaf = dir
        .file_name()
        .ok_or_else(|| format!("fixture dir has no file name: {}", dir.display()))?;
    let dest = temp.path().join(keyspace).join(leaf);
    std::fs::create_dir_all(&dest).map_err(|e| format!("create {}: {e}", dest.display()))?;
    for entry in std::fs::read_dir(dir).map_err(|e| format!("read {}: {e}", dir.display()))? {
        let entry = entry.map_err(|e| format!("read dir entry: {e}"))?;
        if entry
            .file_type()
            .map_err(|e| format!("file type: {e}"))?
            .is_file()
        {
            std::fs::copy(entry.path(), dest.join(entry.file_name()))
                .map_err(|e| format!("copy {}: {e}", entry.path().display()))?;
        }
    }
    Ok(temp)
}

/// Count partitions in the committed golden holding a `static_block` and NO
/// `type == "row"` entry — the STATIC-ONLY partitions Cassandra returns exactly one
/// result row for (issue #3095). Derived from the golden, never authored.
fn golden_static_only_partitions(dir: &Path, sstable_prefix: &str) -> Option<usize> {
    let jsonl = dir.join(format!("{sstable_prefix}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&jsonl).ok()?;
    let mut total = 0usize;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).ok()?;
        let Some(rows) = v.get("rows").and_then(|r| r.as_array()) else {
            continue;
        };
        let has = |k: &str| {
            rows.iter()
                .any(|r| r.get("type").and_then(|t| t.as_str()) == Some(k))
        };
        if has("static_block") && !has("row") {
            total += 1;
        }
    }
    Some(total)
}

// ---------------------------------------------------------------------------
// DDL derivation (the schema file holds ALL tables; the Flight ticket carries
// ONE `CREATE TABLE`, so build the per-table DDL matching the fixture)
// ---------------------------------------------------------------------------

/// Extract the single `CREATE TABLE ... <table> (...)` statement for `table`
/// from the multi-table `.cql` schema file, and rewrite it as a keyspace-
/// qualified DDL string the Flight ticket needs. Every table in
/// `compaction-tombstone-ttl-parity.cql` has the identical
/// `(id INT, ck INT, v TEXT, PRIMARY KEY (id, ck))` shape, so the DDL is derived
/// authoritatively from the schema's declared column set rather than guessed.
fn ddl_for(
    schema_file: &Path,
    keyspace: &str,
    table: &str,
    declared_ddl: Option<&str>,
) -> Result<String, String> {
    // The parity schema declares every table with the SAME column set; assert
    // the target table is actually declared in the file so a typo can never
    // silently synthesize a DDL for a non-existent table.
    let text = std::fs::read_to_string(schema_file)
        .map_err(|e| format!("read schema {}: {e}", schema_file.display()))?;
    let declared = text.contains(&format!("TABLE IF NOT EXISTS {table} "))
        || text.contains(&format!("TABLE {table} "));
    if !declared {
        return Err(format!(
            "table {table} is not declared in {}",
            schema_file.display()
        ));
    }
    // A case that declares its own DDL wins (it is the only way to express a table
    // shape the parity schema does not share — e.g. a STATIC column, issue #3095).
    // It must still name THIS table, so a copy/paste from another case cannot
    // silently decode the wrong shape.
    if let Some(ddl) = declared_ddl {
        if !ddl.contains(table) {
            return Err(format!("case DDL does not name table {table}: {ddl}"));
        }
        return Ok(ddl.to_string());
    }
    Ok(format!(
        "CREATE TABLE {keyspace}.{table} \
         (id int, ck int, v text, PRIMARY KEY (id, ck))"
    ))
}

/// Build the on-the-wire Flight ticket JSON: keyspace + table + per-table DDL +
/// the oracle's `SELECT id, ck, v` projection.
fn ticket_bytes(keyspace: &str, table: &str, ddl: &str, columns: &[String]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": keyspace,
        "table": table,
        "ddl": ddl,
        "columns": columns,
    }))
    .expect("serialize flight ticket")
}

// ---------------------------------------------------------------------------
// Arrow batch → comparable rows
// ---------------------------------------------------------------------------

/// Decode an `id`/`ck`/`v` cell out of a batch column at row `i` into plain
/// JSON, comparing directly against the oracle's authored scalars. `id`/`ck`
/// are CQL `int` (Arrow Int32), `v` is `text` (Arrow Utf8). An `int` may also
/// surface as Int64 depending on the converter; both are accepted. Any other
/// shape is a hard error, never a silent mis-compare.
fn cell_to_json(batch: &RecordBatch, col: &str, i: usize) -> Result<serde_json::Value, String> {
    use serde_json::json;
    let array = batch
        .column_by_name(col)
        .ok_or_else(|| format!("batch missing projected column {col}"))?;
    if array.is_null(i) {
        return Ok(serde_json::Value::Null);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(json!(a.value(i)));
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(json!(a.value(i)));
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(json!(a.value(i)));
    }
    Err(format!(
        "column {col} has unsupported Arrow type {:?}",
        array.data_type()
    ))
}

/// Drive one `do_get` in-process and decode every returned batch into rows of
/// the oracle's `id`/`ck`/`v` projection.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_rows(
    svc: &CqliteFlightService,
    ticket: Vec<u8>,
    cols: &[String],
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .map_err(|s| format!("do_get status: {s}"))?;
    let stream = resp
        .into_inner()
        .map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
    let mut out = Vec::new();
    while let Some(batch) = rb.next().await {
        let batch: RecordBatch = batch.map_err(|e| format!("decode flight batch: {e}"))?;
        for i in 0..batch.num_rows() {
            let mut m = serde_json::Map::new();
            for col in cols {
                m.insert(col.clone(), cell_to_json(&batch, col, i)?);
            }
            out.push(m);
        }
    }
    Ok(out)
}

/// Normalize a result set into a sorted, comparable multiset (row order is not
/// asserted; the oracle compares as a multiset keyed by the projected columns).
fn normalize(rows: Vec<serde_json::Map<String, serde_json::Value>>) -> Vec<String> {
    let mut out: Vec<String> = rows
        .into_iter()
        .map(|m| {
            let sorted: BTreeMap<_, _> = m.into_iter().collect();
            serde_json::to_string(&sorted).unwrap_or_default()
        })
        .collect();
    out.sort();
    out
}

/// One case: returns Ok(true) if it ran a comparison, Ok(false) if it SKIPped.
async fn run_case(case: &Case) -> Result<bool, String> {
    // Anti-empty-pass config validation runs FIRST, for EVERY case — including a
    // `flight_lane: false` one (whose config is still validated here even though this
    // lane cannot execute it), so an opt-out can never smuggle a malformed case past
    // the guards below.
    if case.expected_rows.is_empty() {
        if !case.expect_empty {
            return Err(format!(
                "case {}: expected_rows is empty but expect_empty is not set — a \
                 case expecting zero semantic rows must opt in explicitly",
                case.id
            ));
        }
        if case.physical_row_count == 0 {
            return Err(format!(
                "case {}: expect_empty is set but physical_row_count is 0 — \
                 expect_empty must prove rows existed on disk and were reconciled away",
                case.id
            ));
        }
    }

    // Not expressible on this lane (a whole-table projection `do_get` cannot carry
    // the case's WHERE clause). This is a DECLARED per-case property in the oracle,
    // not a fixture-presence skip, so it is honoured even under
    // CQLITE_REQUIRE_FIXTURES — the in-core lane
    // (cqlite-core/tests/query_semantics_oracle_parity.rs) asserts these cases.
    if !case.flight_lane {
        // The opt-out is only legitimate for a query this lane genuinely cannot
        // express — i.e. one carrying a `WHERE` clause. Without this check the flag
        // could later be flipped on an expressible case to MUTE a real failure.
        if !case.query.to_ascii_uppercase().contains(" WHERE ") {
            return Err(format!(
                "case {}: declares flight_lane: false but its query carries no WHERE \
                 clause, so it IS expressible as a whole-table projection scan — the \
                 opt-out may not be used to mute an expressible case: {}",
                case.id, case.query
            ));
        }
        eprintln!(
            "SKIP case {} — declared flight_lane: false (query carries a WHERE clause \
             the Flight ticket surface cannot express)",
            case.id
        );
        return Ok(false);
    }

    let Some(root) = sstables_root(&case.keyspace) else {
        let msg = format!("case {}: keyspace {} absent", case.id, case.keyspace);
        if require_fixtures() {
            return Err(format!("REQUIRE_FIXTURES: {msg}"));
        }
        eprintln!("SKIP {msg}");
        return Ok(false);
    };
    let Some(dir) = fixture_dir(
        &root,
        &case.keyspace,
        &case.fixture_dir_prefix,
        &case.sstable_prefix,
    ) else {
        let msg = format!(
            "case {}: fixture dir {}* absent",
            case.id, case.fixture_dir_prefix
        );
        if require_fixtures() {
            return Err(format!("REQUIRE_FIXTURES: {msg}"));
        }
        eprintln!("SKIP {msg}");
        return Ok(false);
    };
    let data_db = dir.join(format!("{}-Data.db", case.sstable_prefix));
    if !data_db.exists() {
        let msg = format!("case {}: {} not fetched", case.id, data_db.display());
        if require_fixtures() {
            return Err(format!("REQUIRE_FIXTURES: {msg}"));
        }
        eprintln!("SKIP {msg}");
        return Ok(false);
    }
    let Some(schema) = schema_path(&case.schema) else {
        let msg = format!("case {}: schema {} absent", case.id, case.schema);
        if require_fixtures() {
            return Err(format!("REQUIRE_FIXTURES: {msg}"));
        }
        eprintln!("SKIP {msg}");
        return Ok(false);
    };

    // Anti-empty-pass: the physical rows really exist on disk, and the oracle's
    // declared physical count matches the committed golden.
    let golden = golden_row_count(&dir, &case.sstable_prefix)
        .ok_or_else(|| format!("case {}: golden JSONL missing/unreadable", case.id))?;
    if golden != case.physical_row_count {
        return Err(format!(
            "case {}: golden physical row count {golden} != oracle physical_row_count {}",
            case.id, case.physical_row_count
        ));
    }
    // Issue #3095: the ONE way a correct semantic result may EXCEED the physical row
    // count is a STATIC-ONLY partition (a `static_block` with no `row`), which
    // Cassandra returns one row for. Re-derived from the golden and cross-checked
    // against the oracle's declaration, so the allowance cannot be inflated to mask
    // an over-emitting read path.
    let static_only = golden_static_only_partitions(&dir, &case.sstable_prefix)
        .ok_or_else(|| format!("case {}: golden JSONL missing/unreadable", case.id))?;
    if static_only != case.physical_static_only_partitions {
        return Err(format!(
            "case {}: golden static-only partitions {static_only} != oracle \
             physical_static_only_partitions {}",
            case.id, case.physical_static_only_partitions
        ));
    }
    // Otherwise a shadowing/expiry oracle only ever HIDES rows a physical dump shows.
    if golden + static_only < case.expected_rows.len() {
        return Err(format!(
            "case {}: physical rows {golden} + static-only partitions {static_only} \
             < expected semantic rows {}",
            case.id,
            case.expected_rows.len()
        ));
    }

    let ddl = ddl_for(&schema, &case.keyspace, &case.table, case.ddl.as_deref())
        .map_err(|e| format!("case {}: {e}", case.id))?;
    // Project exactly the columns the oracle asserts (author order preserved).
    let cols: Vec<String> = case
        .expected_rows
        .first()
        .map(|r| r.keys().cloned().collect())
        .unwrap_or_default();
    if cols.is_empty() {
        return Err(format!(
            "case {}: expected_rows carries no columns, so the Flight projection \
             would be empty and the comparison vacuous",
            case.id
        ));
    }

    // Pin the read-time TTL clock for this case (removed after do_get, before
    // the next case). The Flight producer threads this SAME `now` (via
    // `read_time_now_secs`, issue #2789) into its k-way merger's TTL expiry.
    // The service resolves `<data_dir>/<keyspace>/<table>-<uuid>/`, so point it
    // at the sstables root.
    // Stage the RESOLVED fixture dir, so the service reads exactly the bytes this
    // lane derived its physical counts from (see `stage_case_dir`).
    let staged =
        stage_case_dir(&case.keyspace, &dir).map_err(|e| format!("case {}: {e}", case.id))?;
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, case.pinned_now_secs.to_string());
    let svc = CqliteFlightService::new(staged.path().to_path_buf(), 8192);
    let ticket = ticket_bytes(&case.keyspace, &case.table, &ddl, &cols);
    let got_rows = do_get_rows(&svc, ticket, &cols).await;
    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
    let actual = got_rows.map_err(|e| format!("case {}: {e}", case.id))?;

    let expected = normalize(case.expected_rows.clone());
    let got = normalize(actual);
    if expected != got {
        return Err(format!(
            "case {} ({}.{}): FLIGHT query-semantics MISMATCH\n  pinned_now_secs: {}\n  physical rows on disk: {golden}\n  expected ({} rows): {:#?}\n  got      ({} rows): {:#?}",
            case.id,
            case.keyspace,
            case.table,
            case.pinned_now_secs,
            expected.len(),
            expected,
            got.len(),
            got,
        ));
    }
    eprintln!(
        "PASS case {} (flight) — semantic {} rows (physical dump had {golden}); reconciliation applied",
        case.id,
        got.len()
    );
    Ok(true)
}

/// A synthetic `Case` for the guard-enforcement tests below.
fn synthetic_case(
    id: &str,
    expected_rows_len_zero: bool,
    expect_empty: bool,
    physical_row_count: usize,
) -> Case {
    Case {
        id: id.to_string(),
        keyspace: "does_not_matter".to_string(),
        table: "does_not_matter".to_string(),
        fixture_dir_prefix: "does_not_matter".to_string(),
        sstable_prefix: "does_not_matter".to_string(),
        schema: "does_not_matter.cql".to_string(),
        query: "SELECT 1".to_string(),
        pinned_now_secs: 0,
        physical_row_count,
        physical_static_only_partitions: 0,
        ddl: None,
        expected_rows: if expected_rows_len_zero {
            Vec::new()
        } else {
            vec![serde_json::Map::new()]
        },
        expect_empty,
        flight_lane: true,
    }
}

/// An empty `expected_rows` without the explicit `expect_empty` opt-in must FAIL
/// LOUDLY, never silently collapse to `[]` and pass vacuously.
#[tokio::test]
async fn empty_expected_rows_without_opt_in_fails_loudly() {
    let case = synthetic_case("synthetic_empty_no_optin", true, false, 5);
    let err = run_case(&case).await.expect_err(
        "an empty expected_rows without expect_empty must fail loudly, not vacuously pass",
    );
    assert!(
        err.contains("expect_empty"),
        "error must name the missing opt-in, got: {err}"
    );
}

/// `expect_empty: true` still requires proof that physical rows existed and were
/// reconciled away — `physical_row_count == 0` is an empty/misconfigured fixture.
#[tokio::test]
async fn expect_empty_requires_nonzero_physical_row_count() {
    let case = synthetic_case("synthetic_empty_zero_physical", true, true, 0);
    let err = run_case(&case)
        .await
        .expect_err("expect_empty with physical_row_count == 0 must fail loudly");
    assert!(
        err.contains("physical_row_count"),
        "error must name the missing proof, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn query_semantics_oracle_matches_flight_do_get() {
    let oracle_path = repo_root()
        .join("test-data")
        .join("query-semantics-oracle.json");
    let text = std::fs::read_to_string(&oracle_path)
        .unwrap_or_else(|e| panic!("read oracle {}: {e}", oracle_path.display()));
    let oracle: Oracle = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("parse oracle {}: {e}", oracle_path.display()));
    assert!(
        !oracle.cases.is_empty(),
        "oracle must define at least one case"
    );

    // Sequential: the TTL-now env seam is process-global.
    let mut ran = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for case in &oracle.cases {
        match run_case(case).await {
            Ok(true) => ran += 1,
            Ok(false) => {}
            Err(e) => failures.push(e),
        }
    }

    assert!(
        failures.is_empty(),
        "flight query-semantics oracle failures:\n{}",
        failures.join("\n\n")
    );

    if require_fixtures() {
        // Fail closed per CASE, not merely suite-wide: EVERY case that did not
        // declare the `flight_lane: false` opt-out must actually have run, else a
        // silently-skipped case would leave this lane green without asserting it.
        let expected_on_lane: Vec<&str> = oracle
            .cases
            .iter()
            .filter(|c| c.flight_lane)
            .map(|c| c.id.as_str())
            .collect();
        assert_eq!(
            ran,
            expected_on_lane.len(),
            "CQLITE_REQUIRE_FIXTURES=1: {} of {} flight-lane oracle cases ran — every case \
             without a `flight_lane: false` opt-out must run (expected on lane: {:?})",
            ran,
            expected_on_lane.len(),
            expected_on_lane
        );
        assert!(
            ran > 0,
            "CQLITE_REQUIRE_FIXTURES=1 but no oracle case ran (fixtures absent) — fail-closed"
        );
    } else if ran == 0 {
        eprintln!("SKIP flight query_semantics_oracle: no fixtures present (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)");
    }
}
