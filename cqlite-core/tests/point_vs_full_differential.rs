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
//!   * Every case whose SSTable binaries are COMMITTED to git carries
//!     `must_run: true` and is fail-closed UNCONDITIONALLY — a SKIP is a hard
//!     FAILURE with or without `CQLITE_REQUIRE_FIXTURES` (issue #3220). Those
//!     fixtures exist in every checkout, so a SKIP can only mean the lane failed to
//!     RESOLVE them.
//!   * A case whose binaries are FETCHED (gitignored) SKIPs cleanly when the corpus
//!     is absent — UNLESS `CQLITE_REQUIRE_FIXTURES=1` (the agent-gate
//!     integration-tests tier sets it), under which EVERY case must run.
//!   * A case that discovers ZERO partition keys in a present fixture is a hard
//!     FAIL (a fixture with rows must yield at least one point query), never a
//!     silent vacuous pass; every clustering slice is additionally anchored to an
//!     exact expected row count, so a present-but-empty fixture FAILs rather than
//!     comparing `0 == 0`.
//!
//! Fixture roots resolve TABLE-granularly via `support/datasets_root.rs` (#3220):
//! a `CQLITE_DATASETS_ROOT` corpus holding the keyspace but not the table falls
//! through to the checkout's committed copy instead of being committed to. The
//! agent-gate `bti-multiclustering` component runs this target as defense in depth.
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
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    // `issue_3782_corrupt_agreement` (submodule, unconditionally compiled into
    // this target) stages the LZ4-compressed #3782 fixture through
    // `support/corrupt_byte_fixture.rs`, whose control leg cannot decode
    // without the production `lz4` decoder — see the note on
    // `issue_3782_corrupt_row_refusal.rs` (roborev job 59 finding 2, #3950).
    // Kept in step with `Cargo.toml`'s `required-features`.
    feature = "lz4"
))]

// `#[path]` because this file IS the integration target's crate root: a bare
// `mod` would resolve to `tests/one_vs_n_generation.rs`, which cargo would then
// ALSO auto-discover as its own (helper-less, non-compiling) test target. Keeping
// the submodule under `tests/point_vs_full_differential/` — a directory without a
// `main.rs`, so cargo ignores it for target discovery — makes the ownership
// obvious and keeps this file inside the campsite file-size target.
#[path = "point_vs_full_differential/one_vs_n_generation.rs"]
mod one_vs_n_generation;

// Issue #3782: the CORRUPT-fixture half of the differential. The two arms agreed
// by both TRUNCATING before the fix, so agreement alone never revealed the loss —
// same directory, same reason (a submodule keeps this file inside the campsite
// size target and out of cargo's target auto-discovery).
#[path = "point_vs_full_differential/issue_3782_corrupt_agreement.rs"]
mod issue_3782_corrupt_agreement;

// Issue #3890: the same point-vs-full comparison over NON-INTEGER (UUID)
// partition keys, which this file's `CORPUS` is structurally unable to express
// (`probe_keys: &[i64]` / `discover_pk_ints`). Same `#[path]` rationale as above.
#[path = "point_vs_full_differential/uuid_keyed_axis.rs"]
mod uuid_keyed_axis;

// TABLE-granular fixture-root resolution, shared with the sibling dataset lanes
// (issue #3220). Declared BEFORE first use so both this file and the submodule
// (`use super::…`) resolve fixtures the same way.
#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::collections::BTreeMap;
use std::path::Path;

use serial_test::serial;

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

/// RAII guard for a PROCESS-GLOBAL env var: sets it on construction and restores
/// the PREVIOUS state on drop — the earlier value if there was one, else unset.
/// An unconditional `remove_var` would instead DISCARD a value the surrounding
/// environment had set (e.g. a developer pinning the clock for a whole run), and a
/// trailing `remove_var` statement never runs at all when an assertion panics.
///
/// The guard bounds the WINDOW; it does NOT bound concurrency. Because the seam is
/// process-global and every test in this binary shares one process, each test that
/// writes it is ALSO `#[serial]` — without that, one test's restore-on-drop would
/// unpin a sibling's clock mid-run and its remaining scans would silently fall back
/// to `SystemTime::now()` (`now_clock.rs`), producing a spurious divergence or
/// masking a real one. Both parts are required; neither alone is sufficient.
struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => std::env::set_var(self.key, previous),
            None => std::env::remove_var(self.key),
        }
    }
}

/// Pin the read-time TTL clock at `PINNED_NOW_SECS` (never wall-clock) for as long
/// as the returned guard lives. Bind it to a NAMED local (`let _clock = …`); a bare
/// `let _ =` would drop it immediately and unpin the clock.
#[must_use = "the clock stays pinned only while the returned guard is alive"]
fn pin_read_clock() -> EnvVarGuard {
    EnvVarGuard::set(TTL_NOW_OVERRIDE_ENV, &PINNED_NOW_SECS.to_string())
}

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
    /// This case MUST execute — a SKIP is a hard FAILURE, unconditionally (issue
    /// #3220).
    ///
    /// Set for every case whose SSTable binaries are **committed to git** rather than
    /// fetched: they are present in EVERY checkout, so there is no legitimate
    /// absence and no reason to require `CQLITE_REQUIRE_FIXTURES=1` before saying so.
    /// A declarative flag (rather than a table name hardcoded in the terminal
    /// assertion) so a future committed fixture opts in where it is defined.
    ///
    /// AUTHORITY for the value is `git ls-files`, NEVER directory presence in the
    /// working tree: `fetch-datasets.sh` unpacks the fetched corpus into
    /// `test-data/datasets/` by default, so a GITIGNORED fixture is routinely present
    /// on disk in a checkout where another machine has nothing. Re-derive with
    ///
    /// ```text
    /// git ls-files 'test-data/datasets/sstables/**-Data.db'
    /// ```
    ///
    /// which (as of #3220) covers exactly four of this corpus's tables —
    /// `test_tomb/static_with_tombstones`, both `test_compaction_tombstone_ttl`
    /// tables and both `test_da` tables, i.e. FIVE of the nine cases. Every other
    /// `test_tomb` table here ships only its `*-Data.db.jsonl` / `*-Statistics.db.txt`
    /// sidecars, so its binaries are fetched and its absence is legitimate.
    ///
    /// Why it is load-bearing: `CQLITE_REQUIRE_FIXTURES` is NOT set by the
    /// `core-tests` component that runs this target, and the terminal check used to
    /// be a suite-wide `ran > 0` — so a single case that resolved to no fixture
    /// skipped silently behind seven siblings that ran. That is exactly how the
    /// #3032 `test_da.multiclustering_table` case never executed on a machine whose
    /// `CQLITE_DATASETS_ROOT` held `test_da` without that table.
    must_run: bool,
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
        // FETCHED (gitignored binaries; only the JSONL/.db.txt sidecars are
        // committed) — a clean SKIP on a minimal checkout is legitimate.
        must_run: false,
        clustering_slice_predicates: &[],
    },
    TableCase {
        keyspace: "test_tomb",
        table: "resurrection_gc_positive",
        schema: "tombstone-parity.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["multi_generation", "tombstone"],
        // FETCHED (gitignored binaries; only the JSONL/.db.txt sidecars are
        // committed) — a clean SKIP on a minimal checkout is legitimate.
        must_run: false,
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
        // FETCHED (gitignored binaries; only the JSONL/.db.txt sidecars are
        // committed) — a clean SKIP on a minimal checkout is legitimate.
        must_run: false,
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
        // FETCHED (gitignored binaries; only the JSONL/.db.txt sidecars are
        // committed) — a clean SKIP on a minimal checkout is legitimate.
        must_run: false,
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
        // COMMITTED: `git ls-files` lists nb-1-big-{Data,Index,Filter,Summary,
        // Statistics,CompressionInfo}.db under
        // test-data/datasets/sstables/test_tomb/static_with_tombstones-4cdb9780…/ —
        // the ONLY test_tomb table in this corpus whose binaries are in git. Present in
        // every checkout, so a SKIP can only mean the lane failed to RESOLVE it.
        must_run: true,
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
        must_run: true,
        clustering_slice_predicates: &[],
    },
    TableCase {
        keyspace: "test_compaction_tombstone_ttl",
        table: "ttl_expired_live",
        schema: "compaction-tombstone-ttl-parity.cql",
        pk_column: "id",
        probe_keys: &[],
        divergence_classes: &["ttl"],
        must_run: true,
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
        must_run: true,
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
    // BTI (`da`) wide partition with a COMPOUND clustering key (issue #3032):
    // `PRIMARY KEY (pk, bucket, seq)`, i.e. two components of DIFFERING types
    // (`text` then `int`). `test_da.wide_table` above has a SINGLE `int` clustering
    // column, so its slices can only ever bound the whole clustering key — it
    // structurally cannot exercise a bound on a NON-first clustering component, nor
    // a bound that is a PROPER PREFIX of the clustering key, nor the OSS50
    // variable-length (text) component encoding. All three are covered here.
    //
    // Every row is live (no tombstone/TTL), so the divergence this case guards is a
    // wrong within-partition window or a wrong post-scan bound evaluation on a
    // multi-component clustering key.
    TableCase {
        keyspace: "test_da",
        table: "multiclustering_table",
        schema: "multiclustering-table-bti.cql",
        pk_column: "pk",
        probe_keys: &[1, 2, 3],
        divergence_classes: &["bti_clustering_slice", "compound_clustering"],
        must_run: true,
        // The fixture's partitions are DELIBERATELY non-uniform (pk=1: 3 buckets x
        // 60 rows = 180; pk=2: 5 x 32 = 160; pk=3: 8 x 16 = 128), which is what makes
        // their row-index tries differ structurally. This lane applies one expected
        // count to EVERY probed key, so each predicate below is chosen to yield the
        // SAME count in all three partitions: every bucket in every partition holds
        // at least 16 rows (`seq` = 0..15), so a slice confined to `seq < 16` is
        // partition-independent. Each count is strictly between 0 and the smallest
        // partition's 128 rows, so neither an empty nor an unnarrowed result passes.
        //
        // A FIRST-component-only slice (e.g. `bucket = 'bo'`, the shape that actually
        // drives the `Rows.db` prefix-bound narrowing) cannot appear here: its row
        // count is the bucket size, which differs per partition by construction. That
        // shape is covered instead by the query-semantics oracle case
        // `multiclustering_bti__first_component_prefix_slice` and by
        // `issue_3032_multiclustering_clustering_slice_select.rs`, which compare the
        // point and full paths per partition against the committed JSONL golden.
        clustering_slice_predicates: &[
            // Bounds on the SECOND clustering component, under an equality on the
            // first — the multi-component shape `wide_table` cannot express.
            ("bucket = 'alpha' AND seq >= 2 AND seq < 8", 6),
            ("bucket = 'bo' AND seq < 4", 4),
            (
                "bucket = 'charlie-extended-bucket' AND seq > 9 AND seq <= 14",
                5,
            ),
            // A full two-component point read.
            ("bucket = 'bo' AND seq = 5", 1),
        ],
    },
    // BIG (`nb`) COMPRESSED wide partition (issue #3890): 114 LZ4 chunks over a
    // 1,837,037-byte UNCOMPRESSED data section (27,823 bytes on disk), read from
    // CompressionInfo.db's header — the largest committed multi-chunk compressed
    // BIG fixture, and second overall behind `test_da.wide_table` (115 chunks,
    // BTI). `wide_table` above covers the BTI half; this covers the BIG half,
    // where the seek's chunk-rounded window overruns furthest into the SUCCESSOR
    // partition when the PARSE input is unbounded and the walker then re-reads a
    // row body as a partition header. All rows are live, so the only divergence
    // this case can report is a point path that dropped or over-collected rows.
    // Re-derivation command: test-data/schemas/wide-partition-big.cql.
    TableCase {
        keyspace: "test_big",
        table: "wide_partition",
        schema: "wide-partition-big.cql",
        pk_column: "pk",
        probe_keys: &[],
        divergence_classes: &["big_compressed_multichunk"],
        // COMMITTED: `git ls-files` lists
        // test-data/datasets/sstables/test_big/wide_partition-ffe2ee50…/nb-2-big-Data.db.
        must_run: true,
        // A single `int` clustering column, like `wide_table`. The partition sizes
        // are NOT uniform across pk values here, so every predicate is confined to
        // the low `ck` range every partition is known to hold (the fixture's
        // partitions each start at ck=0 and run contiguously), keeping one expected
        // count valid for every probed key.
        clustering_slice_predicates: &[("ck < 8", 8), ("ck >= 2 AND ck < 5", 3)],
    },
];

/// Stable identity of a corpus case, used by the per-case must-run bookkeeping.
fn case_id(case: &TableCase) -> String {
    format!("{}.{}", case.keyspace, case.table)
}

/// PURE decision behind the must-run assertion: every `must_run` case absent from
/// `ran`, by table name.
///
/// Factored out so the assertion has a proof it CAN fail
/// (`must_run_violations_flags_a_committed_case_that_did_not_run`). A fail-closed
/// guard whose failing branch is never exercised is indistinguishable from a guard
/// that cannot fire — the exact shape of the #3220 defect it replaces.
fn must_run_violations<'a>(cases: &'a [TableCase], ran: &[String]) -> Vec<&'a str> {
    cases
        .iter()
        .filter(|c| c.must_run && !ran.iter().any(|id| id == &case_id(c)))
        .map(|c| c.table)
        .collect()
}

// ---------------------------------------------------------------------------
// Path resolution — the shared, TABLE-granular resolver (issue #3220)
// ---------------------------------------------------------------------------

// Re-exported for the `one_vs_n_generation` submodule (`use super::…`) and shared,
// byte-for-byte, with `query_semantics_oracle_parity.rs` + `read_path_forcing_e2e.rs`.
// A private per-file copy is what let the same absence read as a hard FAIL in one
// lane and a silent SKIP in another.
use datasets_root::{describe_search, schema_path, sstables_root_for_table};

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
    // TABLE-granular: every candidate root is searched for THIS table's `*-Data.db`,
    // so a root holding the keyspace without the table falls through to the next one
    // instead of being committed to (issue #3220).
    let Some(root) = sstables_root_for_table(case.keyspace, case.table) else {
        return skip_or_fail(&describe_search(case.keyspace, case.table));
    };
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

/// `#[serial]`: this test writes the process-global `CQLITE_TTL_NOW_OVERRIDE_SECS`
/// read seam that the `one_vs_n_generation` sibling in this SAME binary also writes
/// (libtest runs them on a multi-threaded pool by default), so the two must never
/// overlap. The pure comparator tests below neither read nor write the seam.
#[tokio::test]
#[serial]
async fn point_vs_full_differential_equality() {
    // Corpus coverage assertion: the lane must exercise every #1741 divergence
    // class (multi-generation, tombstone, TTL) — never silently narrow to a
    // trivial live-only corpus.
    let covered: std::collections::BTreeSet<&str> = CORPUS
        .iter()
        .flat_map(|c| c.divergence_classes.iter().copied())
        .collect();
    for required in [
        "multi_generation",
        "tombstone",
        "ttl",
        // The BTI (`da`) clustering-slice classes (#3002 / #3032): the point path's
        // `Rows.db` row-index window, and its multi-component (`text` then `int`)
        // clustering form. Listed so dropping either fixture reds this lane instead
        // of quietly narrowing the corpus.
        "bti_clustering_slice",
        "compound_clustering",
        // Issue #3890: the BIG (`nb`) COMPRESSED multi-chunk class. Listed so
        // dropping `test_big.wide_partition` reds this lane rather than quietly
        // narrowing the corpus back to fixtures whose chunk overrun is negligible.
        "big_compressed_multichunk",
    ] {
        assert!(
            covered.contains(required),
            "corpus must cover the {required:?} reconciliation class (issue #1741 divergence set)"
        );
    }

    // Pin the read-time TTL clock for the whole run. The seam is process-global, so
    // the pin is held by an RAII guard (restores the previous value, even on panic)
    // AND this test is `#[serial]` against every sibling that writes the same var.
    let _clock = pin_read_clock();

    let mut ran: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for case in CORPUS {
        match run_case(case).await {
            Ok(true) => ran.push(case_id(case)),
            Ok(false) => skipped.push(case_id(case)),
            Err(e) => failures.push(format!("{}: {e}", case_id(case))),
        }
    }

    assert!(
        failures.is_empty(),
        "point-vs-full differential failures:\n{}",
        failures.join("\n\n")
    );

    // PER-CASE must-run, UNCONDITIONALLY (issue #3220). Every `must_run` fixture is
    // COMMITTED to git, so it is present in every checkout and a SKIP can only mean
    // the case failed to RESOLVE its fixture — the silent-skip defect this assertion
    // exists to make impossible. Deliberately NOT gated on
    // `CQLITE_REQUIRE_FIXTURES`: this target runs under `core-tests`, which does not
    // set it, and that is exactly where the #3032 multiclustering case skipped
    // unnoticed. Suite-wide `ran > 0` cannot see it — seven siblings ran.
    let must_run_missing = must_run_violations(CORPUS, &ran);
    assert!(
        must_run_missing.is_empty(),
        "{} committed-fixture case(s) did NOT run: {:?} — these fixtures are COMMITTED to git, \
         so absence means the lane failed to RESOLVE them, never that they are legitimately \
         missing.\n  ran    : {:?}\n  skipped: {:?}\n  remedy : git restore --source=HEAD -- \
         test-data/datasets/sstables (or fix root resolution — see \
         tests/support/datasets_root.rs)",
        must_run_missing.len(),
        must_run_missing,
        ran,
        skipped
    );

    let ran = ran.len();
    if require_fixtures() {
        // Fail closed per CASE, not merely suite-wide (matching the query-semantics
        // oracle): this lane has no per-case opt-out, so under REQUIRE_FIXTURES every
        // corpus case must have run. A suite-wide `ran > 0` would let a newly added
        // case skip silently behind its siblings.
        assert!(
            skipped.is_empty(),
            "CQLITE_REQUIRE_FIXTURES=1 but {} of {} differential cases SKIPped ({:?}) — \
             fail-closed",
            skipped.len(),
            CORPUS.len(),
            skipped
        );
        assert_eq!(
            ran,
            CORPUS.len(),
            "CQLITE_REQUIRE_FIXTURES=1: {ran} of {} differential cases ran — fail-closed",
            CORPUS.len()
        );
    } else if ran == 0 {
        eprintln!(
            "SKIP point_vs_full_differential: no fixtures present \
             (set CQLITE_REQUIRE_FIXTURES=1 to fail-close)"
        );
    }
}

/// Regression-test the FAIL-CLOSED guard itself (issue #3220): the must-run decision
/// must FIRE for a committed-fixture case that did not run, and must stay silent for
/// a fetched-only case that skipped. Without this, the guard's failing branch is
/// never exercised and "green" would not distinguish a guard that works from a guard
/// that cannot fire.
#[test]
fn must_run_violations_flags_a_committed_case_that_did_not_run() {
    // The real corpus, minus the #3032 case's id: exactly the state observed on a
    // machine whose CQLITE_DATASETS_ROOT lacked the committed fixture.
    let ran_without_multiclustering: Vec<String> = CORPUS
        .iter()
        .filter(|c| c.table != "multiclustering_table")
        .map(case_id)
        .collect();
    assert_eq!(
        must_run_violations(CORPUS, &ran_without_multiclustering),
        vec!["multiclustering_table"],
        "a committed-fixture case that did not run must be reported, even though every \
         other case ran (the suite-wide `ran > 0` blind spot)"
    );

    // All cases ran: no violation.
    let all: Vec<String> = CORPUS.iter().map(case_id).collect();
    assert!(
        must_run_violations(CORPUS, &all).is_empty(),
        "no violation when every case ran"
    );

    // A FETCHED-only (must_run == false) case that skipped is NOT a violation: those
    // binaries are gitignored, so their absence is legitimate on a minimal checkout.
    let without_fetched: Vec<String> = CORPUS.iter().filter(|c| c.must_run).map(case_id).collect();
    assert!(
        must_run_violations(CORPUS, &without_fetched).is_empty(),
        "a gitignored-fixture case that skipped must not trip the committed-fixture guard"
    );

    // The corpus must actually DECLARE some committed cases, else the guard above is
    // vacuous (an all-`false` corpus can never produce a violation).
    assert!(
        CORPUS.iter().any(|c| c.must_run),
        "at least one corpus case must be declared must_run, else the guard is vacuous"
    );
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
