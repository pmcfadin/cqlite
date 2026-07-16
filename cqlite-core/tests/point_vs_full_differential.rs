//! Issue #1918: the POINT-vs-FULL differential-equality lane.
//!
//! The CQLite-vs-CQLite complement to the CQLite-vs-Cassandra query-semantics
//! oracle (`query_semantics_oracle_parity.rs`, #1742). It runs the same
//! point-read-eligible query through BOTH forced access paths —
//! `CQLITE_READ_PATH=point` (a partition-targeted lookup) and
//! `CQLITE_READ_PATH=full` (a full scan + reconciliation) — via the
//! `QueryConfig::forced_read_path` knob and asserts the two paths return
//! byte-identical result sets (rows, values, AND order).
//!
//! Why this catches bugs a physical dump cannot: the `*-Data.db.jsonl` goldens
//! enumerate every on-disk cell (tombstones/expired included), so a
//! read-time-reconciliation divergence between the point and full paths is
//! invisible to them (both retain the shadowed rows). This lane compares the
//! POST-reconciliation `SELECT` result of the two paths directly — precisely the
//! divergence class #1741 hid behind green physical goldens.
//!
//! It is a **query-semantics-class** oracle: TTL expiry is evaluated at a PINNED
//! `now` via the debug-only `CQLITE_TTL_NOW_OVERRIDE_SECS` reader seam (never
//! wall-clock), so a long-expired fixture reads deterministically and the point
//! and full runs see identical expiry. The corpus deliberately includes
//! multi-generation, tombstone, and TTL fixtures (`test_tomb`,
//! `test_compaction_tombstone_ttl`) — the reconciliation classes the lane exists
//! to guard.
//!
//! Anti-empty-pass / SKIP contract (matches the query-semantics oracle):
//!   * When the committed corpus (or its `*.db` binaries) is absent, each case
//!     SKIPs cleanly — UNLESS `CQLITE_REQUIRE_FIXTURES=1` (the agent-gate
//!     integration-tests tier sets it), in which case an absent/empty fixture is
//!     a hard FAIL so the lane can never green-pass without running.
//!   * A case that discovers ZERO partition keys in a present fixture is a hard
//!     FAIL (a fixture with rows must yield at least one point query), never a
//!     silent vacuous pass.
//!
//! The harness's divergence detection is itself regression-tested by
//! `comparison_detects_a_seeded_divergence` below (feeding the compare helper two
//! different row sets must report a mismatch), complementing the manual
//! seed-a-real-divergence verification recorded in the PR.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::{Config, Database};

/// Debug-only reader seam (see `now_clock.rs`): pins read-time TTL "now" so TTL
/// expiry is deterministic and IDENTICAL across the point and full runs.
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// A fixed pin well past every fixture's TTL boundary. The exact value is
/// immaterial to point-vs-full EQUALITY (both runs use the same pin); a fixed
/// value simply removes wall-clock flakiness. 2027-01-15T08:00:00Z.
const PINNED_NOW_SECS: i64 = 1_800_000_000;

/// The most point-query keys probed per table (bounds worst-case fan-out on a
/// wide corpus while still covering every distinct partition in the small
/// tombstone/TTL fixtures, which have only a handful of partitions).
const MAX_KEYS_PER_TABLE: usize = 32;

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// One corpus table: a single-column INT partition key (so a `WHERE pk = <int>`
/// literal is trivial and unambiguous to build). `divergence_classes` documents
/// which reconciliation class the fixture exercises (multi-generation /
/// tombstone / TTL), asserting the corpus stays exhaustive over #1741's classes.
struct TableCase {
    keyspace: &'static str,
    table: &'static str,
    schema: &'static str,
    /// The single INT partition-key column name.
    pk_column: &'static str,
    /// Partition keys ALWAYS probed in addition to the ones discovered by a live
    /// scan. Needed for fixtures whose partitions reconcile to ZERO live rows
    /// (e.g. a partition-tombstone-only table): discovery finds nothing, yet the
    /// point-vs-full equality of an empty-on-both-paths partition is a genuine
    /// #1741 shadowing check, not a vacuous pass.
    probe_keys: &'static [i64],
    /// Documented reconciliation classes this fixture covers (for the corpus
    /// coverage assertion; not used at query time).
    divergence_classes: &'static [&'static str],
}

/// The corpus. Every table has a single INT partition key. Collectively they
/// cover multi-generation reconciliation (`test_tomb` 2-gen tables), tombstones
/// (row/cell/range/partition deletes), and TTL expiry (`gc_before_boundary`,
/// `ttl_expired_live`).
const CORPUS: &[TableCase] = &[
    // Multi-generation (2 flushes) + cross-gen deletes shadowing older live rows.
    TableCase {
        keyspace: "test_tomb",
        table: "resurrection_gc0",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["multi_generation", "tombstone"],
    },
    TableCase {
        keyspace: "test_tomb",
        table: "resurrection_gc_positive",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["multi_generation", "tombstone"],
    },
    // Cross-generation partition tombstone + a tombstone-only partition.
    TableCase {
        keyspace: "test_tomb",
        table: "skipped_partition_delete",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[1, 2],
        divergence_classes: &["multi_generation", "tombstone"],
    },
    // TTL localDeletionTime boundary (expired vs live cells).
    TableCase {
        keyspace: "test_tomb",
        table: "gc_before_boundary",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["ttl"],
    },
    // Live static cell surviving adjacent row/cell/range tombstones.
    TableCase {
        keyspace: "test_tomb",
        table: "static_with_tombstones",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["tombstone"],
    },
    // Post-major-compaction tombstone/TTL fixtures (single output SSTable).
    TableCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "shadow_row_delete",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        probe_keys: &[],
        divergence_classes: &["tombstone"],
    },
    TableCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "ttl_expired_live",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        probe_keys: &[],
        divergence_classes: &["ttl"],
    },
];

// ---------------------------------------------------------------------------
// Path resolution (mirrors query_semantics_oracle_parity.rs)
// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent repo dir")
        .to_path_buf()
}

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

/// True when the keyspace dir holds at least one `*-Data.db` for `table` — i.e.
/// the (gitignored) binaries have actually been fetched, not just the JSONL.
fn table_has_data(root: &Path, keyspace: &str, table: &str) -> bool {
    let ks_dir = root.join(keyspace);
    let Ok(entries) = std::fs::read_dir(&ks_dir) else {
        return false;
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&format!("{table}-")))
                    .unwrap_or(false)
        })
        .any(|dir| {
            std::fs::read_dir(&dir)
                .map(|rd| {
                    rd.filter_map(|e| e.ok()).any(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.ends_with("-Data.db"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
        })
}

// ---------------------------------------------------------------------------
// Result normalization (authoritative; never a byte-pattern guess)
// ---------------------------------------------------------------------------

/// Normalize a result set to an ORDERED list of per-row strings, each a
/// sorted-by-column-name `Debug` rendering of the row's values. `Debug` on
/// `Value` is stable and total across every CQL type (scalars, collections,
/// UDTs), so the comparison covers all values without a hand-maintained matcher;
/// sorting columns within a row removes `HashMap` iteration nondeterminism while
/// preserving ROW order (asserted per spec: rows, values, AND order).
fn normalize(rows: &[QueryRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            let sorted: BTreeMap<&str, String> = row
                .values
                .iter()
                .map(|(k, v)| (k.as_ref(), format!("{v:?}")))
                .collect();
            format!("{sorted:?}")
        })
        .collect()
}

/// Build a `Database` over the fixture with a fixed read-path forcing mode.
async fn open_db(
    root: &Path,
    schema: &Path,
    keyspace: &str,
    mode: ReadPathMode,
) -> Result<Database, String> {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.map_err(|e| format!("ingestion: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".into());
    }
    Ok(result.database)
}

/// Discover the DISTINCT integer partition-key values present in `table` by
/// running a full-scan `SELECT` (on the `full`-mode DB, so a full-table read is
/// legal). Returns them sorted + deduplicated so the probe set is deterministic.
async fn discover_pk_ints(db: &Database, case: &TableCase) -> Result<Vec<i64>, String> {
    let query = format!(
        "SELECT {} FROM {}.{}",
        case.pk_column, case.keyspace, case.table
    );
    let result = db
        .execute(&query)
        .await
        .map_err(|e| format!("discovery SELECT failed: {e}"))?;
    let mut seen: BTreeMap<i64, ()> = BTreeMap::new();
    for row in &result.rows {
        if let Some(v) = row.values.get(case.pk_column) {
            let as_int = value_as_i64(v).ok_or_else(|| {
                format!(
                    "partition key {} decoded as a non-integer value {v:?}; this lane \
                     only handles INT partition keys",
                    case.pk_column
                )
            })?;
            seen.insert(as_int, ());
        }
    }
    Ok(seen.into_keys().take(MAX_KEYS_PER_TABLE).collect())
}

/// Extract an `i64` from any integer-family `Value`.
fn value_as_i64(v: &cqlite_core::types::Value) -> Option<i64> {
    use cqlite_core::types::Value;
    match v {
        Value::TinyInt(i) => Some(*i as i64),
        Value::SmallInt(i) => Some(*i as i64),
        Value::Integer(i) => Some(*i as i64),
        Value::BigInt(i) | Value::Counter(i) | Value::Timestamp(i) => Some(*i),
        _ => None,
    }
}

/// Run `query` under both forced modes and assert byte-identical (rows, values,
/// order) result sets. Returns the diff description on mismatch.
async fn assert_point_full_equal(
    point_db: &Database,
    full_db: &Database,
    query: &str,
) -> Result<(), String> {
    let point = point_db
        .execute(query)
        .await
        .map_err(|e| format!("point path failed for `{query}`: {e}"))?;
    let full = full_db
        .execute(query)
        .await
        .map_err(|e| format!("full path failed for `{query}`: {e}"))?;

    let point_rows = normalize(&point.rows);
    let full_rows = normalize(&full.rows);
    if point_rows != full_rows {
        return Err(format!(
            "point-vs-full DIVERGENCE for `{query}`:\n  point ({} rows): {:#?}\n  full  ({} rows): {:#?}",
            point_rows.len(),
            point_rows,
            full_rows.len(),
            full_rows
        ));
    }
    Ok(())
}

/// Run every eligible query for one table under `point` and `full`, asserting
/// equality. `Ok(true)` = ran a comparison, `Ok(false)` = SKIPped (absent
/// fixture, non-fail-closed).
async fn run_case(case: &TableCase) -> Result<bool, String> {
    let Some(root) = sstables_root(case.keyspace) else {
        return skip_or_fail(&format!("keyspace {} absent", case.keyspace));
    };
    if !table_has_data(&root, case.keyspace, case.table) {
        return skip_or_fail(&format!(
            "table {}.{} has no fetched *-Data.db",
            case.keyspace, case.table
        ));
    }
    let Some(schema) = schema_path(case.schema) else {
        return skip_or_fail(&format!("schema {} absent", case.schema));
    };

    let full_db = open_db(&root, &schema, case.keyspace, ReadPathMode::Full).await?;
    let point_db = open_db(&root, &schema, case.keyspace, ReadPathMode::Point).await?;

    let discovered = discover_pk_ints(&full_db, case).await?;
    // Merge discovered (live) keys with the always-probe keys, deduplicated and
    // sorted so the probe set is deterministic.
    let mut key_set: BTreeMap<i64, ()> = BTreeMap::new();
    for k in discovered.iter().chain(case.probe_keys.iter()) {
        key_set.insert(*k, ());
    }
    let keys: Vec<i64> = key_set.into_keys().take(MAX_KEYS_PER_TABLE).collect();
    // Anti-empty-pass: a present fixture MUST yield at least one partition key to
    // probe (discovered or explicit), else the lane would run zero comparisons
    // and pass vacuously. A table that reconciles to zero LIVE rows must declare
    // explicit `probe_keys` (so the empty-on-both-paths equality is still checked).
    if keys.is_empty() {
        return Err(format!(
            "case {}.{}: no partition keys to probe (discovered none and no \
             explicit probe_keys) — a present fixture must yield at least one \
             point query; declare probe_keys for a fully-reconciled-away table",
            case.keyspace, case.table
        ));
    }

    // Single-key `=` equality for every discovered partition.
    for k in &keys {
        let query = format!(
            "SELECT * FROM {}.{} WHERE {} = {}",
            case.keyspace, case.table, case.pk_column, k
        );
        assert_point_full_equal(&point_db, &full_db, &query).await?;
    }

    // `IN (...)` over the complete partition key (when ≥2 keys exist): the union
    // of targeted lookups (point) must equal the full-scan + in-memory IN filter.
    if keys.len() >= 2 {
        let list = keys
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT * FROM {}.{} WHERE {} IN ({})",
            case.keyspace, case.table, case.pk_column, list
        );
        assert_point_full_equal(&point_db, &full_db, &query).await?;
    }

    eprintln!(
        "PASS {}.{} — {} point queries + IN, point == full (classes: {:?})",
        case.keyspace,
        case.table,
        keys.len(),
        case.divergence_classes
    );
    Ok(true)
}

/// Either SKIP cleanly (returning `Ok(false)`) or, under `CQLITE_REQUIRE_FIXTURES`,
/// fail closed.
fn skip_or_fail(msg: &str) -> Result<bool, String> {
    if require_fixtures() {
        return Err(format!("REQUIRE_FIXTURES: {msg}"));
    }
    eprintln!("SKIP {msg}");
    Ok(false)
}

#[tokio::test]
async fn point_vs_full_differential_equality() {
    // Corpus coverage assertion: the lane must exercise every #1741 divergence
    // class (multi-generation, tombstone, TTL) — never silently narrow to a
    // trivial live-only corpus.
    let covered: std::collections::BTreeSet<&str> = CORPUS
        .iter()
        .flat_map(|c| c.divergence_classes.iter().copied())
        .collect();
    for required in ["multi_generation", "tombstone", "ttl"] {
        assert!(
            covered.contains(required),
            "corpus must cover the {required:?} reconciliation class (issue #1741 divergence set)"
        );
    }

    // Pin the read-time TTL clock for the whole run (single-threaded here, so no
    // sibling test in this binary races the process-global env seam).
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, PINNED_NOW_SECS.to_string());

    let mut ran = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for case in CORPUS {
        match run_case(case).await {
            Ok(true) => ran += 1,
            Ok(false) => {}
            Err(e) => failures.push(format!("{}.{}: {e}", case.keyspace, case.table)),
        }
    }

    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);

    assert!(
        failures.is_empty(),
        "point-vs-full differential failures:\n{}",
        failures.join("\n\n")
    );

    if require_fixtures() {
        assert!(
            ran > 0,
            "CQLITE_REQUIRE_FIXTURES=1 but no differential case ran (fixtures absent) — fail-closed"
        );
    } else if ran == 0 {
        eprintln!(
            "SKIP point_vs_full_differential: no fixtures present \
             (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)"
        );
    }
}

/// Regression-test the harness itself: the compare logic MUST flag a divergence
/// (a different or reordered row set) rather than silently passing — the
/// "demonstrably fail if either path is broken" contract, at the harness level.
#[test]
fn comparison_detects_a_seeded_divergence() {
    use cqlite_core::query::result::RowMetadata;
    use cqlite_core::types::{RowKey, Value};

    fn row(id: i64) -> QueryRow {
        let mut values = std::collections::HashMap::new();
        values.insert("id".into(), Value::Integer(id as i32));
        QueryRow {
            values,
            key: RowKey::from(id.to_be_bytes().to_vec()),
            metadata: RowMetadata::default(),
            cell_metadata: None,
        }
    }

    let base = vec![row(1), row(2), row(3)];

    // Identical sets compare equal.
    assert_eq!(normalize(&base), normalize(&base));

    // A DIFFERENT value set diverges.
    let altered_value = vec![row(1), row(9), row(3)];
    assert_ne!(
        normalize(&base),
        normalize(&altered_value),
        "a differing row value must be detected as a divergence"
    );

    // A REORDERED set diverges (order is asserted, not just the multiset).
    let reordered = vec![row(3), row(2), row(1)];
    assert_ne!(
        normalize(&base),
        normalize(&reordered),
        "a reordered row set must be detected as a divergence (order matters)"
    );
}
