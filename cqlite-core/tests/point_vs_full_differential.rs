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
//!
//! ## Second axis: 1 generation vs N generations (issue #3129)
//!
//! `one_vs_n_generation` (submodule, same target) adds the orthogonal axis this
//! file's point-vs-full comparison structurally CANNOT see: both of the point/full
//! arms read the same fixture at the same generation count, so a divergence
//! between single-generation reconciliation and the cross-generation merge kernel
//! reproduces identically on both arms and stays green. That submodule reads the
//! SAME bytes at 1 generation and at N ≥ 2 generations and requires identical
//! result sets, reusing this file's corpus conventions, pinned `now`, SKIP
//! contract and `normalize`.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

// `#[path]` because this file IS the integration target's crate root: a bare
// `mod` would resolve to `tests/one_vs_n_generation.rs`, which cargo would then
// ALSO auto-discover as its own (helper-less, non-compiling) test target. Keeping
// the submodule under `tests/point_vs_full_differential/` — a directory without a
// `main.rs`, so cargo ignores it for target discovery — makes the ownership
// obvious and keeps this file inside the campsite file-size target.
#[path = "point_vs_full_differential/one_vs_n_generation.rs"]
mod one_vs_n_generation;

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
    /// Extra WITHIN-partition clustering predicates to run against EVERY probed
    /// partition key, as `(predicate, expected_row_count)` pairs evaluated as
    /// `WHERE <pk> = <k> AND <predicate>` (issue #3002). These exercise the
    /// clustering-slice read path — for a BTI (`da`) wide partition the point run
    /// resolves its byte window from the `Rows.db` row-index trie
    /// (`bti_clustering_row_window`) while the full run decodes the whole partition
    /// and filters, so a wrong row-index window diverges here.
    ///
    /// The expected count is REQUIRED (anti-vacuous-pass): `point == full` alone is
    /// satisfied by both-empty (`0 == 0`, a window that dropped every row) and by
    /// both-unfiltered (`300 == 300`, a predicate that never narrowed), so each
    /// predicate is anchored to the row count its slice must yield. Empty =
    /// partition-key equality only.
    clustering_slice_predicates: &'static [(&'static str, usize)],
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
        clustering_slice_predicates: &[],
    },
    TableCase {
        keyspace: "test_tomb",
        table: "resurrection_gc_positive",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["multi_generation", "tombstone"],
        clustering_slice_predicates: &[],
    },
    // Cross-generation partition tombstone + a tombstone-only partition.
    TableCase {
        keyspace: "test_tomb",
        table: "skipped_partition_delete",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[1, 2],
        divergence_classes: &["multi_generation", "tombstone"],
        clustering_slice_predicates: &[],
    },
    // TTL localDeletionTime boundary (expired vs live cells).
    TableCase {
        keyspace: "test_tomb",
        table: "gc_before_boundary",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["ttl"],
        clustering_slice_predicates: &[],
    },
    // Live static cell surviving adjacent row/cell/range tombstones.
    TableCase {
        keyspace: "test_tomb",
        table: "static_with_tombstones",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["tombstone"],
        clustering_slice_predicates: &[],
    },
    // Post-major-compaction tombstone/TTL fixtures (single output SSTable).
    TableCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "shadow_row_delete",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        probe_keys: &[],
        divergence_classes: &["tombstone"],
        clustering_slice_predicates: &[],
    },
    TableCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "ttl_expired_live",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        probe_keys: &[],
        divergence_classes: &["ttl"],
        clustering_slice_predicates: &[],
    },
    // BTI (`da`) WIDE partition with a per-partition `Rows.db` row index (issue
    // #3002): the ONLY corpus table whose point path narrows its decode to a
    // clustering-slice byte window resolved from the row-index trie. All rows are
    // live (no tombstone/TTL class), so the divergence this case guards is a wrong
    // row-index window — a point run that drops or over-collects rows the full-scan
    // run returns. The slices deliberately span block 0 (`ck < 8`, whose floor is the
    // empty separator the #3002 root fix restored), a mid-partition point read, an
    // interior range, and the last block.
    TableCase {
        keyspace: "test_da",
        table: "wide_table",
        schema: "wide-table-bti.cql",
        pk_column: "pk",
        probe_keys: &[1, 2, 3],
        divergence_classes: &["bti_clustering_slice"],
        // Every partition holds ck=0..=299, so each slice's row count is exact and
        // identical for pk=1/2/3 — and every one of them is strictly between 0 and the
        // partition's 300 rows, so neither an empty nor an unnarrowed result can pass.
        clustering_slice_predicates: &[
            ("ck < 8", 8),
            ("ck = 150", 1),
            ("ck >= 100 AND ck < 110", 10),
            ("ck >= 296", 4),
            ("ck > 0 AND ck <= 3", 3),
        ],
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
/// Parameterized (rather than taking a `TableCase`) so the `one_vs_n_generation`
/// axis reuses the exact same discovery.
async fn discover_pk_ints(
    db: &Database,
    keyspace: &str,
    table: &str,
    pk_column: &str,
) -> Result<Vec<i64>, String> {
    let query = format!("SELECT {pk_column} FROM {keyspace}.{table}");
    let result = db
        .execute(&query)
        .await
        .map_err(|e| format!("discovery SELECT failed: {e}"))?;
    let mut seen: BTreeMap<i64, ()> = BTreeMap::new();
    for row in &result.rows {
        if let Some(v) = row.values.get(pk_column) {
            let as_int = value_as_i64(v).ok_or_else(|| {
                format!(
                    "partition key {pk_column} decoded as a non-integer value {v:?}; this lane \
                     only handles INT partition keys"
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
/// order) result sets. Returns the agreed row count on success (so a caller can
/// anchor it to an expected count), or the diff description on mismatch.
async fn assert_point_full_equal(
    point_db: &Database,
    full_db: &Database,
    query: &str,
) -> Result<usize, String> {
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
    Ok(point_rows.len())
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

    let discovered = discover_pk_ints(&full_db, case.keyspace, case.table, case.pk_column).await?;
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

    // Single-key `=` equality for every discovered partition. The agreed row count is
    // RETAINED per key: it is exactly the reference the clustering-slice block below
    // needs, and re-running the same full-partition query there would decode every
    // wide partition twice more per run (2 paths × ~600 KiB for `test_da.wide_table`).
    let mut partition_rows_by_key: BTreeMap<i64, usize> = BTreeMap::new();
    for k in &keys {
        let query = format!(
            "SELECT * FROM {}.{} WHERE {} = {}",
            case.keyspace, case.table, case.pk_column, k
        );
        let rows = assert_point_full_equal(&point_db, &full_db, &query).await?;
        partition_rows_by_key.insert(*k, rows);
    }

    // Within-partition clustering slices (issue #3002): for a BTI wide partition the
    // point path resolves its decode window from the `Rows.db` row index while the
    // full path decodes the whole partition and filters, so the two paths must still
    // agree row-for-row, value-for-value, in order. Each slice is ALSO anchored to its
    // expected row count, so neither a both-empty nor a both-unnarrowed result can
    // pass vacuously. The predicate set is a per-CASE property, so it is checked ONCE
    // outside the per-key loop (it is not a per-key condition).
    if !case.clustering_slice_predicates.is_empty() {
        for k in &keys {
            // This partition's full row count, the reference every slice must be
            // strictly smaller than — the count the `=` equality loop above already
            // agreed on for this key (per-key, never assumed uniform).
            let partition_rows = *partition_rows_by_key.get(k).ok_or_else(|| {
                format!(
                    "case {}.{}: no agreed full-partition row count for key {k} \
                     (the equality loop must record one per probed key)",
                    case.keyspace, case.table
                )
            })?;
            for (predicate, expected_rows) in case.clustering_slice_predicates {
                let query = format!(
                    "SELECT * FROM {}.{} WHERE {} = {} AND {}",
                    case.keyspace, case.table, case.pk_column, k, predicate
                );
                let got = assert_point_full_equal(&point_db, &full_db, &query).await?;
                if got != *expected_rows {
                    return Err(format!(
                        "case {}.{}: `{query}` returned {got} rows on BOTH paths but the slice \
                         must yield exactly {expected_rows} — equal-but-wrong is still wrong",
                        case.keyspace, case.table
                    ));
                }
                if got == 0 || got >= partition_rows {
                    return Err(format!(
                        "case {}.{}: `{query}` returned {got} rows against a {partition_rows}-row \
                         partition — a clustering slice must be non-empty AND strictly smaller \
                         than the whole partition (else the comparison is vacuous)",
                        case.keyspace, case.table
                    ));
                }
            }
        }
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
        "PASS {}.{} — {} point queries + {} clustering slices/key + IN, point == full \
         (classes: {:?})",
        case.keyspace,
        case.table,
        keys.len(),
        case.clustering_slice_predicates.len(),
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
