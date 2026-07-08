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

/// Repo root = the parent of this crate's manifest dir (`<repo>/cqlite-core`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent repo dir")
        .to_path_buf()
}

/// The `sstables/` root. Prefer `CQLITE_DATASETS_ROOT` when it actually holds the
/// committed keyspace; otherwise fall back to the in-repo committed corpus (these
/// fixtures are committed, not gitignored, so the repo copy is always present).
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

fn fixture_dir(sstables_root: &Path, keyspace: &str, prefix: &str) -> Option<PathBuf> {
    let ks_dir = sstables_root.join(keyspace);
    std::fs::read_dir(&ks_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(prefix))
                    .unwrap_or(false)
        })
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
        Value::Text(s) => json!(s),
        other => return Err(format!("unsupported oracle scalar {other:?}")),
    })
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

    let Some(root) = sstables_root(&case.keyspace) else {
        let msg = format!("case {}: keyspace {} absent", case.id, case.keyspace);
        if require_fixtures() {
            return Err(format!("REQUIRE_FIXTURES: {msg}"));
        }
        eprintln!("SKIP {msg}");
        return Ok(false);
    };
    let Some(dir) = fixture_dir(&root, &case.keyspace, &case.fixture_dir_prefix) else {
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
    // The oracle never asserts fewer physical rows than the semantic result —
    // a shadowing/expiry oracle only ever HIDES rows a physical dump shows.
    if golden < case.expected_rows.len() {
        return Err(format!(
            "case {}: physical rows {golden} < expected semantic rows {}",
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
    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
    let result = exec.map_err(|e| format!("case {}: SELECT failed: {e}", case.id))?;

    // Project each result row to the oracle's declared columns.
    let cols: Vec<String> = case
        .expected_rows
        .first()
        .map(|r| r.keys().cloned().collect())
        .unwrap_or_default();
    let mut actual: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for row in &result.rows {
        let mut m = serde_json::Map::new();
        for col in &cols {
            let v = row
                .values
                .get(col.as_str())
                .ok_or_else(|| format!("case {}: result row missing column {col}", case.id))?;
            m.insert(
                col.clone(),
                value_to_json(v).map_err(|e| format!("case {}: {e}", case.id))?,
            );
        }
        actual.push(m);
    }

    let expected = normalize(case.expected_rows.clone());
    let got = normalize(actual);
    if expected != got {
        return Err(format!(
            "case {} ({}): query-semantics MISMATCH\n  query: {}\n  pinned_now_secs: {}\n  physical rows on disk: {golden}\n  expected ({} rows): {:#?}\n  got      ({} rows): {:#?}",
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
    eprintln!(
        "PASS case {} — semantic {} rows (physical dump had {golden}); reconciliation applied",
        case.id,
        got.len()
    );
    Ok(true)
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
        "query-semantics oracle failures:\n{}",
        failures.join("\n\n")
    );

    if require_fixtures() {
        assert!(
            ran > 0,
            "CQLITE_REQUIRE_FIXTURES=1 but no oracle case ran (fixtures absent) — fail-closed"
        );
    } else if ran == 0 {
        eprintln!("SKIP query_semantics_oracle: no fixtures present (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)");
    }
}
