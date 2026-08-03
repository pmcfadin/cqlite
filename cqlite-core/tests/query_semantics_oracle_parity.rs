//! Issue #1742: the QUERY-SEMANTICS parity oracle.
//!
//! A parity lane DISTINCT from the physical sstabledump JSONL goldens. The
//! physical goldens (`*-Data.db.jsonl`) enumerate every on-disk cell — including
//! tombstones, deleted rows, and expired-but-uncompacted TTL cells — so a
//! row-count/value comparison against them structurally CANNOT catch a
//! read-time-reconciliation bug: when CQLite fails to reconcile, both sides
//! still contain the shadowed/expired rows and parity passes while a real
//! Cassandra `SELECT` diverges. That is exactly the P0 (#1741) this oracle guards
//! against regressing.
//!
//! This test compares CQLite `SELECT` output to the POST-RECONCILIATION result
//! set a real Cassandra returns, recorded per-fixture in
//! `test-data/query-semantics-oracle.json`. TTL expiry is evaluated at a PINNED
//! `now` (per case) via the debug-only `CQLITE_TTL_NOW_OVERRIDE_SECS` reader seam,
//! so it is deterministic and never wall-clock-flaky.
//!
//! Anti-empty-pass / SKIP contract:
//!   * Each case carries a non-empty `expected_rows`, OR explicitly opts into a
//!     legitimate zero-row outcome via `expect_empty: true` (e.g. "every row was
//!     shadowed"). A case with empty `expected_rows` and no `expect_empty` opt-in
//!     is a hard FAIL — it is never allowed to silently collapse the compared
//!     column set to `[]` and pass vacuously regardless of what the reader
//!     returns. `expect_empty: true` additionally requires `physical_row_count >
//!     0`, proving rows really existed on disk and were reconciled away rather
//!     than an empty/misconfigured fixture masquerading as "reconciled to zero".
//!   * The compared column set is the query's DECLARED projection (from the result
//!     metadata), never inferred from the keys an expected row happens to carry: an
//!     absent key is how the core result model spells NULL, so inference would drop
//!     exactly the column a deleted-cell case asserts (#3094). Each expected row
//!     must therefore enumerate that projection exactly — a NULL authored as an
//!     explicit `null`.
//!   * `physical_row_count` is re-derived from the committed golden JSONL and
//!     asserted, proving the fixture's rows are physically present on disk.
//!   * When the committed fixture (or its `*.db` binaries) is absent, the case
//!     SKIPs cleanly — UNLESS `CQLITE_REQUIRE_FIXTURES=1` (the agent-gate
//!     component sets it), in which case an absent/empty fixture is a hard FAIL
//!     (fail-closed) so the gate lane can never green-pass without running.
//!
//! Cases run sequentially inside ONE async test so the process-global TTL-now env
//! seam is never mutated concurrently by a sibling test in this binary.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

// TABLE-granular fixture-root resolution, shared with the sibling dataset lanes
// (issue #3220).
#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::Database;
use serde::Deserialize;

/// Debug-only reader seam (see `now_clock.rs`): pins the read-time TTL "now"
/// clock (epoch seconds) so a long-expired fixture is read deterministically
/// "as of" the oracle's pinned evaluation time.
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// Fail-closed switch: when set, an absent/empty committed fixture is a hard
/// failure instead of a clean skip (the agent-gate `query-semantics-oracle`
/// component sets it, matching `compaction-byte-parity`).
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
    #[allow(dead_code)]
    table: String,
    fixture_dir_prefix: String,
    sstable_prefix: String,
    schema: String,
    query: String,
    pinned_now_secs: i64,
    physical_row_count: usize,
    /// STATIC-ONLY partitions in the committed golden — partitions carrying a
    /// `static_block` and ZERO `row` entries (issue #3095).
    ///
    /// Cassandra returns exactly ONE result row for each such partition
    /// (`SelectStatement.processPartition()`: clustering + REGULAR columns null,
    /// statics populated), and that row has NO physical `row` entry behind it. So
    /// this is the amount by which a correct semantic result may LEGITIMATELY
    /// exceed `physical_row_count` — the one exception to "an oracle only ever
    /// HIDES rows a physical dump shows". Re-derived from the golden and asserted
    /// below, so it can never be inflated to mask an over-emitting read path.
    /// Defaults to 0, so every pre-existing case keeps the strict guard.
    #[serde(default)]
    physical_static_only_partitions: usize,
    /// Ordered map per row so JSON author-order is preserved for readable diffs.
    expected_rows: Vec<serde_json::Map<String, serde_json::Value>>,
    /// Explicit opt-in for a case whose correct semantic result is ZERO rows
    /// (e.g. every physical row was shadowed/expired). Required whenever
    /// `expected_rows` is empty — see the module doc's anti-empty-pass contract.
    /// Defaults to `false` so an author who forgets to fill in `expected_rows`
    /// gets a loud failure, never a silent vacuous pass.
    #[serde(default)]
    expect_empty: bool,
}

// ---------------------------------------------------------------------------
// Path resolution
// ---------------------------------------------------------------------------

// Repo root, candidate `sstables/` roots and committed-schema resolution come from
// the shared, TABLE-granular resolver (issue #3220). This file used to carry a
// private copy of a KEYSPACE-granular `sstables_root`, byte-identical to the copies
// in `point_vs_full_differential.rs` and `read_path_forcing_e2e.rs` — so the same
// absence (a root holding the keyspace but not the table) surfaced as a confusing
// hard FAIL here and a silent SKIP there.
use datasets_root::{repo_root, schema_path};

/// The candidate root that actually carries THIS case's fixture.
///
/// Table-granular (issue #3220): every candidate root is probed with the case's own
/// `fixture_dir_prefix`/`sstable_prefix` and the first one holding the `Data.db`
/// wins, so a `CQLITE_DATASETS_ROOT` corpus lacking a git-committed table falls
/// through to the checkout instead of being committed to. When NO root resolves the
/// fixture, a root merely holding the keyspace is returned so the caller's
/// "dir absent" / "not fetched" messages still name a real path.
fn sstables_root_for_case(case: &Case) -> Option<PathBuf> {
    let candidates = datasets_root::sstables_root_candidates();
    let data_db = format!("{}-Data.db", case.sstable_prefix);
    candidates
        .iter()
        .find(|root| {
            fixture_dir(
                root,
                &case.keyspace,
                &case.fixture_dir_prefix,
                &case.sstable_prefix,
            )
            .map(|dir| dir.join(&data_db).exists())
            .unwrap_or(false)
        })
        .or_else(|| candidates.iter().find(|r| r.join(&case.keyspace).is_dir()))
        .cloned()
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

/// Count partitions in the committed golden that hold a `static_block` and NO
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
// Value normalization (authoritative, never a byte-pattern guess)
// ---------------------------------------------------------------------------

/// Map a decoded scalar `Value` to plain JSON so it compares directly against the
/// oracle's authored scalars. Unsupported types are a hard error (never silently
/// mis-compared) — the oracle only uses int/text today; extend explicitly.
fn value_to_json(v: &Value) -> Result<serde_json::Value, String> {
    use serde_json::json;
    Ok(match v {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => json!(b),
        Value::Integer(i) => json!(i),
        Value::BigInt(i) | Value::Counter(i) => json!(i),
        Value::SmallInt(i) => json!(i),
        Value::TinyInt(i) => json!(i),
        Value::Float(f) => json!(f),
        Value::Text(s) => json!(std::str::from_utf8(s).unwrap_or_default()),
        other => return Err(format!("unsupported oracle scalar {other:?}")),
    })
}

/// Cross-check a case's authored `expected_rows` against `cols`, the query's
/// DECLARED projection (issue #3094, round-3 review).
///
/// Every expected row must enumerate that projection EXACTLY: an unknown key is an
/// oracle typo (never silently ignored), and a MISSING key is a column left
/// un-asserted — a NULL must be authored as an explicit `null`, so the assertion is
/// visible in the oracle rather than implied by omission. Without this, a case that
/// simply omitted a column would compare fewer columns than it projects and pass
/// vacuously on exactly the NULL it exists to assert.
fn validate_projection(
    case_id: &str,
    cols: &[String],
    expected_rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), String> {
    if cols.is_empty() {
        return Err(format!(
            "case {case_id}: result metadata declares no columns — the projection \
             cannot be derived, and comparing zero columns would pass vacuously"
        ));
    }
    for (i, row) in expected_rows.iter().enumerate() {
        for key in row.keys() {
            if !cols.contains(key) {
                return Err(format!(
                    "case {case_id}: expected_rows[{i}] names column {key}, which the \
                     query's declared projection ({cols:?}) does not contain — an \
                     oracle typo, not a NULL"
                ));
            }
        }
        for col in cols {
            if !row.contains_key(col) {
                return Err(format!(
                    "case {case_id}: expected_rows[{i}] omits projected column {col} — \
                     every projected column must be authored (a NULL as an explicit \
                     `null`), otherwise it is silently left out of the comparison"
                ));
            }
        }
    }
    Ok(())
}

async fn open_db(sstables_dir: &Path, schema: &Path, keyspace: &str) -> Result<Database, String> {
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: sstables_dir.to_path_buf(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.map_err(|e| format!("ingestion: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".into());
    }
    Ok(result.database)
}

/// Normalize a result set into a sorted, comparable form (row order is not
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
    // Anti-empty-pass config validation (independent of fixture presence): an
    // empty `expected_rows` must be an explicit, provable opt-in — never an
    // accident that silently collapses the compared column set to `[]` and
    // passes vacuously regardless of what the reader returns.
    if case.expected_rows.is_empty() {
        if !case.expect_empty {
            return Err(format!(
                "case {}: expected_rows is empty but expect_empty is not set — a \
                 case expecting zero semantic rows must opt in explicitly via \
                 expect_empty: true (an unopted-in empty expected_rows would \
                 silently pass vacuously regardless of what the reader returns)",
                case.id
            ));
        }
        if case.physical_row_count == 0 {
            return Err(format!(
                "case {}: expect_empty is set but physical_row_count is 0 — \
                 expect_empty must prove rows existed on disk and were \
                 reconciled away, not just an empty/misconfigured fixture",
                case.id
            ));
        }
    }

    let Some(root) = sstables_root_for_case(case) else {
        // Names the table AND every root searched: "keyspace absent" was actively
        // misleading when the keyspace existed and only the table did not (#3220).
        let msg = format!(
            "case {}: no candidate sstables root holds {}.{}* — {}",
            case.id,
            case.keyspace,
            case.fixture_dir_prefix,
            datasets_root::describe_roots()
        );
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
    // Issue #3095: the ONE way a correct semantic result may exceed the physical
    // row count is a STATIC-ONLY partition (a `static_block` with no `row`), which
    // Cassandra returns one row for. Re-derive that count from the golden and
    // assert the oracle's declaration, so the allowance cannot be inflated to mask
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
    // Otherwise the oracle never asserts fewer physical rows than the semantic
    // result — a shadowing/expiry oracle only ever HIDES rows a physical dump shows.
    if golden + static_only < case.expected_rows.len() {
        return Err(format!(
            "case {}: physical rows {golden} + static-only partitions {static_only} \
             < expected semantic rows {}",
            case.id,
            case.expected_rows.len()
        ));
    }

    // Pin the read-time TTL clock for this case (removed after execute, before the
    // next case, so no case leaks its pin — single-threaded within this test).
    // Ingest points at the whole `sstables/` root, filtered to the keyspace.
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, case.pinned_now_secs.to_string());
    let db = match open_db(&root, &schema, &case.keyspace).await {
        Ok(db) => db,
        Err(e) => {
            std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
            return Err(format!("case {}: {e}", case.id));
        }
    };

    let exec = db.execute(&case.query).await;
    // Issue #3109: run the SAME query through the STREAMING executor as well, INSIDE
    // the same pin. That executor is the one that consumes the BATCHED reader surface
    // (`select_executor/streaming.rs` -> `StorageEngine::scan_stream_batched` ->
    // `SSTableReader::scan_stream_batched`), which the materializing `execute` above
    // does not reach — so without this second drive a per-surface decode-posture
    // divergence (the #1577 class: the batched surface lacking the BTI dispatch its
    // siblings have, and so decoding `da` readers UNSHADOWED) is INVISIBLE to this
    // oracle. Both lanes must produce the recorded post-reconciliation result set.
    let streamed = collect_streaming(&db, &case.query).await;
    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
    let result = exec.map_err(|e| format!("case {}: SELECT failed: {e}", case.id))?;
    let (streamed_cols, streamed_rows) =
        streamed.map_err(|e| format!("case {}: streaming SELECT failed: {e}", case.id))?;

    // The compared column set is the query's DECLARED projection, taken from the
    // result metadata (the engine's rendering of the case's own `SELECT` list) —
    // NOT the keys the first expected row happens to carry.
    //
    // Why that distinction is load-bearing (issue #3094, round-3 review): within a
    // row, an ABSENT entry IS the core result model's representation of NULL — the
    // decoder only inserts cells the row actually carries, and every consumer
    // (`output/csv.rs`, `output/json.rs`, `export/arrow_convert.rs`) renders a
    // missing column as null. A cell deleted by a cell tombstone is exactly that.
    // So a column set INFERRED from present keys would silently drop precisely the
    // column a deleted-cell case exists to assert, and the case would pass
    // vacuously. Deriving it from the declared projection instead keeps the
    // comparison authoritative rather than shaped by observed data (#28).
    let cols: Vec<String> = result
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    //
    // `validate_projection` SUBSUMES the earlier `origin/main` guard (issue #3095),
    // which derived `cols` from the first expected row and then checked each was
    // projected: it asserts BOTH directions — every oracle column is projected (an
    // unprojected one is an oracle typo, not a NULL) AND every projected column is
    // authored in every expected row (so no projected column is silently left out
    // of the comparison).
    validate_projection(&case.id, &cols, &case.expected_rows)?;
    let actual = project_rows(&case.id, &cols, &result.rows)?;

    // The streaming lane's projection must be the SAME declared projection, else the
    // two lanes would be compared on different column sets and a divergence could
    // hide in the difference.
    if streamed_cols != cols {
        return Err(format!(
            "case {}: the streaming executor declared projection {streamed_cols:?} but \
             the materializing executor declared {cols:?} — the two lanes must compare \
             the same column set",
            case.id
        ));
    }
    let streamed_actual = project_rows(&case.id, &cols, &streamed_rows)?;

    let expected = normalize(case.expected_rows.clone());
    for (lane, rows) in [
        ("materializing (`execute`)", actual),
        (
            "streaming (`execute_streaming`, batched reader surface)",
            streamed_actual,
        ),
    ] {
        let got = normalize(rows);
        if expected != got {
            return Err(format!(
                "case {} ({}): query-semantics MISMATCH on the {lane} lane\n  query: {}\n  pinned_now_secs: {}\n  physical rows on disk: {golden}\n  expected ({} rows): {:#?}\n  got      ({} rows): {:#?}",
                case.id,
                case.keyspace,
                case.query,
                case.pinned_now_secs,
                expected.len(),
                expected,
                got.len(),
                got,
            ));
        }
    }
    eprintln!(
        "PASS case {} — semantic {} rows on BOTH the materializing and streaming lanes \
         (physical dump had {golden}); reconciliation applied",
        case.id,
        expected.len()
    );
    Ok(true)
}

/// Execute `query` through the STREAMING executor and drain it, returning the
/// declared projection and the rows (issue #3109).
///
/// This is the lane that reaches the reader's BATCHED scan surface; `db.execute`
/// does not, so a posture divergence between the two reader surfaces is only
/// observable from here.
async fn collect_streaming(
    db: &Database,
    query: &str,
) -> Result<(Vec<String>, Vec<cqlite_core::query::result::QueryRow>), String> {
    let mut iter = db
        .execute_streaming(
            query,
            cqlite_core::query::result::StreamingConfig::default(),
        )
        .await
        .map_err(|e| format!("{e}"))?;
    let cols: Vec<String> = iter
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let mut rows = Vec::new();
    while let Some(row) = iter.next_async().await {
        rows.push(row.map_err(|e| format!("streamed row: {e}"))?);
    }
    Ok((cols, rows))
}

/// Render `rows` down to the declared projection `cols` as comparable JSON.
///
/// A PROJECTED column with no cell in a row reads NULL — CQL semantics, and exactly
/// what Cassandra returns for the clustering and regular columns of a static-only
/// partition's row (issue #3095; `processPartition()`'s
/// `default: result.add((ByteBuffer) null)`), and equally what a cell TOMBSTONE
/// leaves behind: an absent entry IS the core result model's NULL (issue #3094).
/// `validate_projection` is what keeps this from masking an oracle typo.
fn project_rows(
    case_id: &str,
    cols: &[String],
    rows: &[cqlite_core::query::result::QueryRow],
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
    let mut actual = Vec::with_capacity(rows.len());
    for row in rows {
        let mut m = serde_json::Map::new();
        for col in cols {
            let json = match row.values.get(col.as_str()) {
                Some(v) => value_to_json(v).map_err(|e| format!("case {case_id}: {e}"))?,
                None => serde_json::Value::Null,
            };
            m.insert(col.clone(), json);
        }
        actual.push(m);
    }
    Ok(actual)
}

/// A synthetic `Case` for the guard-enforcement tests below. Uses placeholder
/// paths/queries that are never reached — `run_case` must reject these cases
/// during the config-validation check at the top, before any fixture I/O.
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
        expected_rows: if expected_rows_len_zero {
            Vec::new()
        } else {
            vec![serde_json::Map::new()]
        },
        expect_empty,
    }
}

/// Issue #1742 review finding: an empty `expected_rows` without the explicit
/// `expect_empty` opt-in must FAIL LOUDLY, never silently collapse the compared
/// column set to `[]` and pass vacuously.
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

/// `expect_empty: true` still requires proof that physical rows existed and
/// were reconciled away — `physical_row_count == 0` is an empty/misconfigured
/// fixture, not a legitimate "reconciled to zero" case.
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

/// Issue #3094 round-3 review: the compared column set comes from the query's
/// DECLARED projection, so a case that OMITS a projected column must fail loudly
/// instead of quietly leaving that column out of the comparison. (Omission was the
/// vacuity vector: an absent key is how the core result model spells NULL, so the
/// dropped column would be exactly the one a deleted-cell case asserts.)
#[test]
fn expected_row_omitting_a_projected_column_fails_loudly() {
    let cols = vec!["pk".to_string(), "ck".to_string(), "row_col".to_string()];
    let row: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(r#"{"pk":1,"ck":3}"#).expect("row json");
    let err = validate_projection("synthetic_omits_column", &cols, &[row])
        .expect_err("an omitted projected column must fail loudly, not pass vacuously");
    assert!(
        err.contains("omits projected column row_col"),
        "error must name the omitted column, got: {err}"
    );
}

/// The mirror guard: a key the projection does not declare is an oracle typo and
/// must never be silently ignored.
#[test]
fn expected_row_naming_an_undeclared_column_fails_loudly() {
    let cols = vec!["pk".to_string(), "ck".to_string()];
    let row: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(r#"{"pk":1,"ck":3,"typo_col":null}"#).expect("row json");
    let err = validate_projection("synthetic_typo_column", &cols, &[row])
        .expect_err("an undeclared column name must fail loudly");
    assert!(
        err.contains("typo_col"),
        "error must name the undeclared column, got: {err}"
    );
}

/// An empty declared projection can only compare zero columns, which would pass
/// regardless of what the reader returned.
#[test]
fn empty_declared_projection_fails_loudly() {
    let err = validate_projection("synthetic_no_cols", &[], &[])
        .expect_err("an empty declared projection must fail loudly");
    assert!(
        err.contains("declares no columns"),
        "error must name the empty projection, got: {err}"
    );
}

#[tokio::test]
async fn query_semantics_oracle_matches_cassandra_select() {
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

    let mut ran = 0usize;
    let mut skipped: Vec<&str> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for case in &oracle.cases {
        match run_case(case).await {
            Ok(true) => ran += 1,
            Ok(false) => skipped.push(case.id.as_str()),
            Err(e) => failures.push(e),
        }
    }

    assert!(
        failures.is_empty(),
        "query-semantics oracle failures:\n{}",
        failures.join("\n\n")
    );

    if require_fixtures() {
        // Fail closed per CASE, not merely suite-wide: this lane has NO per-case
        // opt-out (the oracle's `flight_lane` flag scopes ONLY the Flight lane, which
        // cannot express a `WHERE` clause — this in-core lane executes every case), so
        // EVERY case must have run. A suite-wide `ran > 0` would let a newly added
        // case skip silently while the gate component still passed.
        assert!(
            skipped.is_empty(),
            "CQLITE_REQUIRE_FIXTURES=1 but {} of {} oracle cases SKIPped ({:?}) — every \
             case must run on this lane; it has no per-case opt-out",
            skipped.len(),
            oracle.cases.len(),
            skipped
        );
        assert_eq!(
            ran,
            oracle.cases.len(),
            "CQLITE_REQUIRE_FIXTURES=1: {ran} of {} oracle cases ran — fail-closed",
            oracle.cases.len()
        );
    } else if ran == 0 {
        eprintln!("SKIP query_semantics_oracle: no fixtures present (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)");
    }
}
