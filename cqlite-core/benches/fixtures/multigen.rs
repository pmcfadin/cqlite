//! k-parameterized multi-generation overlap fixtures (issue #2043 / M9).
//!
//! Generalizes the fixed-`L0_SSTABLES` same-`(pk, ck)`-across-generations pattern
//! of `benches/compaction.rs::build_tombstone_heavy` into a builder parameterized
//! by **k** (the number of SSTable generations a row cluster spans) crossed with a
//! collision-mix selector, so a bench can measure how per-row merge cost grows
//! with cluster depth (`docs/research/issue-2043-reconcile-overlap-multiplier.md`).
//!
//! Built on [`crate::fixtures::seeded_rng`] and the public `WriteEngine` flush
//! API, so it is deterministic and needs **no** vendored dataset corpus
//! (`CQLITE_DATASETS_ROOT` is never consulted): controlled `k` is the independent
//! variable and the vendored corpus is single-generation, so it cannot supply
//! `k > 1` at all.
//!
//! ## Every generation's contribution is k-INVARIANT (issue #2043 roborev)
//!
//! [`generation_mutations`] deliberately takes **no `k` parameter**: generation
//! `g` writes byte-identically whether the arm is k = 1 or k = 20 (same RNG
//! sequence, same column sets, same tombstone kinds). A composition that varied
//! with k would confound *cluster depth* with *cell/tombstone population* in the
//! `cost(k)/cost(1)` ratio the record derives, which is exactly the confound an
//! earlier `(generation + k) % 2` alternation introduced. The bench asserts this
//! invariance across arms (per-generation census, `reconcile_overlap.rs`).
//!
//! ## Why in-generation tombstones sit BELOW the live cells
//!
//! The flush writer reconciles *within* a generation
//! (`writer/data_writer/rows.rs::merge_row_group`): the newest `DeleteRow` wins
//! and shadows every cell with `timestamp <= deletion_ts`, and a pure row
//! tombstone carries no liveness. A row tombstone written ABOVE its generation's
//! live cells therefore collapses that generation to a **cell-less** row
//! tombstone before the merge ever sees it — so the "live cells vs row
//! tombstone" shape would never reach `KWayMerger` and the arm would silently
//! measure tombstone-vs-tombstone. Row tombstones are consequently stamped at
//! [`TS_ROW_TOMBSTONE_OFFSET`] (strictly below the live cells), which is the real
//! Cassandra coexistence shape (issue #932: a row deletion older than the
//! surviving cells is kept alongside them) and is present at **every k,
//! including k = 1**. Cell tombstones stay ABOVE their column's live cell
//! ([`TS_CELL_TOMBSTONE_OFFSET`]) because a surviving cell tombstone is the point
//! of that half of the arm and per-column LWW keeps it without shadowing the row.
//!
//! ## `now` is pinned through the API, never the env var
//!
//! The TTL-bearing mixes are measured at [`PINNED_NOW_SECS`], threaded into the
//! merge via `KWayMerger::with_now_secs`. The read-path TTL-`now` override env
//! seam (`reader/parsing/row_decoder/now_clock.rs:61`) is
//! `#[cfg(debug_assertions)]` and compiles OUT of the release profile
//! `cargo bench` uses, where it silently falls back to the wall clock — a bench
//! pinned that way would drift run to run. Neither this module nor the bench
//! reads that variable, and by contract neither even names it (see
//! `benches/README.md` for the operator-facing explanation).

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::WriteEngine;

/// Keyspace of the overlap fixture table.
pub const OVERLAP_KEYSPACE: &str = "test_bench";
/// Table of the overlap fixture table.
pub const OVERLAP_TABLE: &str = "overlap_table";

/// CQL for the overlap fixture: a `(pk, ck)` composite primary key so one
/// partition holds many clustering rows (a *cluster* per `(pk, ck)`), plus three
/// value columns so the `field_blend` mix can collide on DIFFERENT columns in
/// different generations (per-cell reconciliation, not merely per-row LWW).
/// A single `CREATE TABLE` keeps the no-heuristics mandate (issue #28) supplied
/// with an unambiguous, authoritative schema.
pub const OVERLAP_TABLE_CQL: &str = "\
CREATE TABLE test_bench.overlap_table (
    pk INT,
    ck INT,
    v0 TEXT,
    v1 TEXT,
    v2 INT,
    PRIMARY KEY (pk, ck)
);";

/// The reconcile-time `now` (epoch seconds) every TTL-bearing arm is measured at,
/// pinned through `KWayMerger::with_now_secs`. 2023-11-14T22:13:20Z — a FIXED
/// instant in the past, deliberately far from any run's wall clock so that a
/// silent wall-clock fallback would produce a *different* expired-cell count and
/// be caught (see [`EXPIRED_LDT_SECS`] / [`LIVE_LDT_SECS`]).
pub const PINNED_NOW_SECS: i64 = 1_700_000_000;

/// `localDeletionTime` of the expiring cell that IS expired at
/// [`PINNED_NOW_SECS`] (one hour before it). Expired at the pin AND at any later
/// wall clock.
pub const EXPIRED_LDT_SECS: i32 = (PINNED_NOW_SECS - 3_600) as i32;

/// `localDeletionTime` of the expiring cell that is NOT expired at
/// [`PINNED_NOW_SECS`] (one hour after it) but WOULD be expired under a
/// present-day wall clock. This is the cell that makes the pin observable: at the
/// pin exactly ONE expiring cell per row converts to a cell tombstone; under a
/// wall-clock fallback, two would.
pub const LIVE_LDT_SECS: i32 = (PINNED_NOW_SECS + 3_600) as i32;

/// TTL stamped on the expiring cells. Only its presence matters for expiry here:
/// the authoritative decision uses the explicit `local_deletion_time` above
/// ([`EXPIRED_LDT_SECS`] / [`LIVE_LDT_SECS`]) against the pinned `now`, so this
/// value changes no count this bench asserts. It is kept far below both LDTs so
/// `ldt - ttl` (the LDT a parity consumer derives for an expiring cell) stays
/// positive and never saturates.
///
/// **Un-shaped relationship, deliberately left as measured (issue #2043 roborev):**
/// `ldt - ttl` is NOT this cell's own creation instant. [`TS_BASE`] puts every
/// cell's WRITE timestamp at ~1.6e9 s while the LDTs are pinned relative to
/// [`PINNED_NOW_SECS`] (~1.7e9 s), so `ldt - ttl` lands ~3 years AFTER the cell was
/// written. Nothing in the current fixture or bench compares the two, which is why
/// the constants are left untouched (re-deriving them changes on-disk bytes and
/// would void the banked k-curve). A consumer that DOES relate them — issue #848's
/// tombstone-vs-expiring tie-break, which weighs an expiring cell's derived
/// tombstone LDT against write timestamps — must first re-derive the fixture so
/// `ldt - ttl == base_ts / 1_000_000` for each generation, i.e. pin the LDTs
/// relative to [`TS_BASE`] (or move [`TS_BASE`] up to [`PINNED_NOW_SECS`]'s decade)
/// instead of only to [`PINNED_NOW_SECS`], and re-measure.
pub const FIXTURE_TTL_SECS: u32 = 600;

/// Partitions per generation. `OVERLAP_PARTITIONS * OVERLAP_CK` = **4096 clusters
/// per generation**.
///
/// **This — not [`OVERLAP_CK`] — is the knob the arm width was quadrupled on** (owner
/// decision 2026-07-26, issue #2043; 16 → 64).
///
/// Why the width had to grow at all: `KWayMerger::new_from_readers` spawns one OS
/// producer thread + opens one adapter PER GENERATION, all inside the timed region, so
/// per-drain setup is a fixed cost that GROWS with k. Against a ~1024-row denominator
/// it lands in the numerator of `cost(k)/cost(1)` and biases the multiplier UPWARD
/// with k — while a real compaction over millions of rows amortizes it to nothing. At
/// the original 16 × 64 = 1024 clusters/generation the bench's own `SetupCensus`
/// (`benches/reconcile_overlap.rs`) measured that share at **0.65–0.86 % at k = 1 but
/// 2.4–4.8 % at k = 20**: a ~2.3 % upward bias on the k = 20 multiplier.
///
/// Why the PARTITION count and not the clustering width: `MergeStep::Partition`
/// materializes **one whole partition's reconciled rows at a time**, so rows-per-
/// partition is itself a first-order determinant of per-row merge cost. Growing
/// `OVERLAP_CK` therefore changes the quantity being measured at the same time as it
/// amortizes setup — measured, not assumed: a 16 × 256 variant of this matrix moved the
/// saturated `disjoint` anchor from 2.82 to **3.02 µs/row (+7 %, and +12.6 % at
/// k = 20 alone, where a partition batch reaches 20 × 256 rows)**, which would have
/// confounded the amortization fix with a partition-width change and pushed the anchor
/// out of the record's ±50 % band. Scaling PARTITIONS instead leaves rows-per-
/// partition-per-generation at 64 — byte-for-byte the shape the k-curve was banked on —
/// and quadruples the denominator anyway.
///
/// Result: setup share is now **0.2–0.3 % at k = 1 and ≤1.0 % at k = 20** (measured and
/// printed per arm, never assumed), so the record's raw and setup-corrected multipliers
/// agree to well under 1 %. Still bounded: a k = 20 iteration is ~0.25 s, a generation
/// is 4096 rows of a 5-column table, and the 27-arm matrix stays a single-digit-minute
/// run.
pub const OVERLAP_PARTITIONS: usize = 64;

/// Clustering rows per partition, per generation. Deliberately UNCHANGED at 64 while
/// the arm width grew 4× — see [`OVERLAP_PARTITIONS`] for why this is the wrong knob
/// to scale (it changes the `MergeStep::Partition` batch width, i.e. the quantity
/// being measured).
pub const OVERLAP_CK: usize = 64;

/// Clusters (`(pk, ck)` pairs) each generation writes.
pub const CLUSTERS_PER_GEN: usize = OVERLAP_PARTITIONS * OVERLAP_CK;

/// Clustering rows per partition per generation for the **producer-count control**
/// arm (issue #2043 §3): DOUBLE the matrix width, so a ONE-generation fixture
/// holds the same `2 × CLUSTERS_PER_GEN` clusters — and the same cell count — as the TWO-generation
/// `disjoint/k2` arm. Comparing those two arms changes only the number of
/// producer/adapter streams the drain fans in, which is the measured mechanism
/// behind the k=1 anchor deviation.
pub const PRODUCER_CONTROL_CK: usize = OVERLAP_CK * 2;

/// The collision shape a fixture's generations present to the merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverlapMix {
    /// Every generation writes a DISJOINT `ck` range, so no cluster ever spans
    /// more than one generation. The control arm: per-row cost should stay ~flat
    /// as k grows (only heap depth changes), and `disjoint`/k=1 is the anchor
    /// against the published ~2.0 µs/row narrow-disjoint-singleton figure.
    Disjoint,
    /// Every generation writes the SAME `(pk, ck)` clusters with ascending
    /// timestamps — pure last-write-wins overwrite. The cluster depth IS k.
    LwwOverwrite,
    /// Same `(pk, ck)` in every generation, and each generation additionally
    /// tombstones the lower half of its own clustering range (row tombstone,
    /// stamped BELOW that generation's live cells so both survive the flush —
    /// see the module docs) and cell-tombstones one column of the upper half —
    /// so both tombstone kinds collide with live cells at every k, including
    /// k=1. Across generations the newer generation's row deletion also shadows
    /// the older generation's cells, so the merge does real deletion-vs-cell
    /// comparison work at every depth.
    Tombstone,
    /// Same `(pk, ck)` in every generation, where each row carries one
    /// already-expired expiring cell and one not-yet-expired expiring cell at
    /// [`PINNED_NOW_SECS`] — the arm that exercises `expire_ttl_cells`.
    TtlExpiring,
    /// A field-shaped blend over the same `(pk, ck)` space: a quarter of the
    /// clusters are singletons (written by ONE generation only), and the rest
    /// mix per-column LWW blending, row/cell tombstones, and expiring cells —
    /// the closest local approximation of a real compaction-state cluster
    /// population.
    FieldBlend,
}

impl OverlapMix {
    /// Every mix, in the order the bench matrix walks them.
    pub const ALL: [OverlapMix; 5] = [
        OverlapMix::Disjoint,
        OverlapMix::LwwOverwrite,
        OverlapMix::Tombstone,
        OverlapMix::TtlExpiring,
        OverlapMix::FieldBlend,
    ];

    /// Stable Criterion/bench id segment.
    pub fn id(self) -> &'static str {
        match self {
            OverlapMix::Disjoint => "disjoint",
            OverlapMix::LwwOverwrite => "lww_overwrite",
            OverlapMix::Tombstone => "tombstone",
            OverlapMix::TtlExpiring => "ttl_expiring",
            OverlapMix::FieldBlend => "field_blend",
        }
    }

    /// The `now_secs` this mix must be reconciled at: `Some(PINNED_NOW_SECS)` for
    /// the mixes that contain expiring cells, `None` for the rest.
    ///
    /// `None` is a STRICT no-op in `ReconcileState::expire_ttl_cells`, so the
    /// no-TTL arms keep the expiry machinery entirely out of the measurement
    /// rather than pinning a far-future `now` that would still walk every cell.
    pub fn now_secs(self) -> Option<i64> {
        match self {
            OverlapMix::TtlExpiring | OverlapMix::FieldBlend => Some(PINNED_NOW_SECS),
            OverlapMix::Disjoint | OverlapMix::LwwOverwrite | OverlapMix::Tombstone => None,
        }
    }
}

/// The k values the bench matrix walks.
pub const K_VALUES: [usize; 5] = [1, 2, 5, 10, 20];

/// A built multi-generation fixture: `k` flushed, uncompacted SSTable
/// generations over one table, plus the schema that decodes them.
pub struct MultigenFixture {
    /// `Data.db` paths ordered NEWEST-to-OLDEST — exactly the run-index order
    /// `KWayMerger::new_from_readers` requires (run 0 = newest = LWW winner).
    pub data_paths: Vec<PathBuf>,
    /// Authoritative schema parsed from [`OVERLAP_TABLE_CQL`].
    pub schema: TableSchema,
    /// Generations spanned (= `data_paths.len()`).
    pub k: usize,
    /// The mix this fixture was built for.
    pub mix: OverlapMix,
    /// Clustering rows per partition each generation writes (= [`OVERLAP_CK`]
    /// for the matrix arms; doubled for the producer-count control arm).
    pub ck_per_gen: usize,
    /// Total **mutations** handed to `WriteEngine::write` across all generations.
    ///
    /// This is a MUTATION count, NOT the merge's input row count: a generation's
    /// live write and its in-generation tombstone are two mutations that the
    /// flush writer reconciles into ONE on-disk row. For the merge's actual input
    /// row count use the bench's observed per-generation drain
    /// (`reconcile_overlap.rs::observed_input_rows`), which reads what the
    /// readers really emit.
    pub mutations_written: u64,
    // Holds the temp dir alive: an `Arc<SSTableReader>` handed to
    // `new_from_readers` maps/reads these files for its whole lifetime, so the
    // fixture must outlive every merger built from it.
    _tmp: tempfile::TempDir,
}

/// Build a `k`-generation fixture for `mix` with `ck_per_gen` clustering rows per
/// partition per generation ([`OVERLAP_CK`] for every matrix arm).
///
/// Writes each generation into its own memtable and flushes it with compaction
/// DISABLED (`WriteEngineConfig::auto_compaction = false`), so exactly `k`
/// `Data.db` files remain uncompacted. Asserts that count before returning: a
/// fixture that silently compacted (or failed to flush) would measure the wrong k.
///
/// The non-default width exists for the producer-count control arm (issue #2043
/// §3): holding the row and cell count fixed while changing only the number of
/// producers requires a 1-generation fixture as wide as a 2-generation one.
pub fn build_multigen_sized(k: usize, mix: OverlapMix, ck_per_gen: usize) -> MultigenFixture {
    use cqlite_core::schema::parse_cql_schema;

    assert!(k >= 1, "k must be >= 1");
    assert!(
        ck_per_gen >= 2 && ck_per_gen % 4 == 0,
        "ck_per_gen must be a positive multiple of 4 (the mixes split by halves and by slot % 4)"
    );
    let schema = parse_cql_schema(OVERLAP_TABLE_CQL).expect("parse overlap-fixture schema");
    let tmp = tempfile::TempDir::new().expect("temp dir for overlap fixture");
    let data_dir = tmp.path().join("data");
    let mut engine = open_fixture_engine(tmp.path(), &schema);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for overlap fixture flush");

    let mut oldest_first = Vec::with_capacity(k);
    let mut mutations_written = 0u64;
    let mut rng = crate::fixtures::seeded_rng();
    for generation in 0..k {
        mutations_written += write_generation(&mut engine, mix, generation, ck_per_gen, &mut rng);
        let info = rt
            .block_on(engine.flush())
            .expect("overlap fixture flush must not error")
            .expect("overlap fixture flush produced no SSTable");
        oldest_first.push(info.data_path);
    }
    rt.block_on(engine.close())
        .expect("close overlap fixture engine");

    let sstable_dir = data_dir.join(OVERLAP_KEYSPACE).join(OVERLAP_TABLE);
    assert_eq!(
        count_data_files(&sstable_dir),
        k,
        "overlap fixture (k={k}, mix={}) must leave exactly {k} uncompacted Data.db files in {}",
        mix.id(),
        sstable_dir.display()
    );
    assert!(
        mutations_written > 0,
        "overlap fixture (k={k}, mix={}) wrote no rows",
        mix.id()
    );

    // Newest-to-oldest: run index 0 must be the LAST generation flushed.
    oldest_first.reverse();
    MultigenFixture {
        data_paths: oldest_first,
        schema,
        k,
        mix,
        ck_per_gen,
        mutations_written,
        _tmp: tmp,
    }
}

/// `WriteEngine` over [`OVERLAP_TABLE_CQL`] with a huge flush threshold (the
/// builder flushes explicitly, one flush per generation), durability DISABLED and
/// **compaction disabled**.
///
/// `auto_compaction = false` is explicit rather than incidental: `WriteEngineConfig::new`
/// defaults it to `true`, which installs the default STCS merge policy. The
/// generations happen to survive anyway because the builder never calls
/// `maintenance_step()`, but relying on that is a latent trap — turning the policy
/// off means "exactly k uncompacted generations" is guaranteed by configuration,
/// not by an omission.
///
/// Durability off is a *setup-cost* choice only: the fixture is untimed scaffolding
/// for a read-side merge measurement, and `SyncEachWrite` would fsync once per
/// mutation (~20k fsyncs at k=20). Nothing about the resulting SSTables differs.
fn open_fixture_engine(dir: &Path, schema: &TableSchema) -> WriteEngine {
    use cqlite_core::storage::write_engine::{Durability, WriteEngineConfig};

    let mut cfg = WriteEngineConfig::new(dir.join("data"), dir.join("wal"), schema.clone())
        .with_flush_threshold(usize::MAX)
        .with_durability(Durability::Disabled);
    cfg.auto_compaction = false;
    WriteEngine::new(cfg).expect("build overlap-fixture write engine")
}

/// `-Data.db` files directly under `dir` (mirrors
/// `tests/issue_1579_streaming_multigen_order.rs::count_data_files`).
fn count_data_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read sstable dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        .count()
}

/// Base write timestamp (micros). Well below [`PINNED_NOW_SECS`] in seconds so
/// every cell's write time precedes the pinned reconcile instant.
const TS_BASE: i64 = 1_600_000_000_000_000;

/// Per-generation timestamp stride (micros): ascending across generations so the
/// NEWEST generation always wins LWW, and wide enough that a generation's
/// in-generation tombstone offsets never reach the next generation.
const TS_GEN_STRIDE: i64 = 1_000_000;

/// Offset (micros) of a generation's ROW tombstone relative to that generation's
/// live cells: strictly **below** them, so the flush writer keeps both (the
/// issue-#932 coexistence shape) instead of shadowing the cells away. See the
/// module docs. Magnitude stays far under [`TS_GEN_STRIDE`], so a generation's
/// tombstone never reaches into the previous generation's window.
const TS_ROW_TOMBSTONE_OFFSET: i64 = -1;

/// Offset (micros) of a generation's CELL tombstone relative to that generation's
/// live cells: strictly **above** them, so per-column last-write-wins keeps the
/// tombstone as the surviving value of that one column.
const TS_CELL_TOMBSTONE_OFFSET: i64 = 1;

/// Write one generation's mutations into `engine`'s memtable, returning the
/// number of mutations written (NOT rows — see
/// [`MultigenFixture::mutations_written`]).
fn write_generation(
    engine: &mut WriteEngine,
    mix: OverlapMix,
    generation: usize,
    ck_per_gen: usize,
    rng: &mut rand::rngs::StdRng,
) -> u64 {
    use rand::Rng;

    let base_ts = TS_BASE + (generation as i64) * TS_GEN_STRIDE;
    let mut written = 0u64;
    for pk in 0..OVERLAP_PARTITIONS as i32 {
        for slot in 0..ck_per_gen as i32 {
            // `Disjoint` shifts each generation into its own ck window so no
            // cluster is ever shared; every other mix reuses the same window.
            let ck = match mix {
                OverlapMix::Disjoint => (generation as i32) * ck_per_gen as i32 + slot,
                _ => slot,
            };
            let tag: u32 = rng.gen();
            for m in generation_mutations(mix, pk, ck, slot, generation, ck_per_gen, base_ts, tag) {
                engine.write(m).expect("overlap fixture write");
                written += 1;
            }
        }
    }
    written
}

/// The mutations one `(pk, ck)` cluster receives from `generation` under `mix`.
///
/// **Takes no `k`.** Generation `g`'s contribution is identical at every k, so
/// `cost(k)/cost(1)` varies only cluster DEPTH, never the cell/tombstone
/// population (see the module docs on k-invariance).
fn generation_mutations(
    mix: OverlapMix,
    pk: i32,
    ck: i32,
    slot: i32,
    generation: usize,
    ck_per_gen: usize,
    base_ts: i64,
    tag: u32,
) -> Vec<cqlite_core::storage::write_engine::Mutation> {
    let half = (ck_per_gen / 2) as i32;
    match mix {
        // Disjoint / pure LWW: one full-row write per generation.
        OverlapMix::Disjoint | OverlapMix::LwwOverwrite => {
            vec![live_row(pk, ck, generation, base_ts, tag)]
        }
        // Live row + a same-generation tombstone: a row tombstone on the lower
        // clustering half (stamped BELOW the live cells so the flush keeps both —
        // otherwise the generation collapses to a cell-less row tombstone and the
        // live-vs-row-tombstone collision never reaches the merge), a cell
        // tombstone on `v1` of the upper half (stamped ABOVE its column's live
        // cell so the tombstone is the surviving value).
        OverlapMix::Tombstone => {
            let mut out = vec![live_row(pk, ck, generation, base_ts, tag)];
            if slot < half {
                out.push(row_tombstone(pk, ck, base_ts + TS_ROW_TOMBSTONE_OFFSET));
            } else {
                out.push(cell_tombstone(
                    pk,
                    ck,
                    "v1",
                    base_ts + TS_CELL_TOMBSTONE_OFFSET,
                ));
            }
            out
        }
        // Two expiring cells per row: one already expired at the pin, one not.
        OverlapMix::TtlExpiring => vec![ttl_row(pk, ck, generation, base_ts, tag)],
        // Field-shaped blend, by `slot` residue class:
        //   0 → singleton (written by generation 0 ONLY, so 1/4 of clusters have
        //       depth 1 regardless of k — the L3 fast-path-eligible population);
        //   1 → per-column LWW blending (alternating columns per generation);
        //   2 → live + tombstone collision;
        //   3 → expiring cells.
        OverlapMix::FieldBlend => match slot % 4 {
            0 => {
                if generation == 0 {
                    vec![live_row(pk, ck, generation, base_ts, tag)]
                } else {
                    Vec::new()
                }
            }
            1 => vec![blended_row(pk, ck, generation, base_ts, tag)],
            2 => {
                let mut out = vec![live_row(pk, ck, generation, base_ts, tag)];
                // Alternate the tombstone kind by GENERATION PARITY ONLY so both
                // shapes appear even at small k. Never by `k`: keying the kind off
                // k made generation g's composition depend on the arm's depth, so
                // the k=1 anchor was measured on a different tombstone population
                // than the k>1 arms (roborev, issue #2043).
                if generation % 2 == 0 {
                    out.push(cell_tombstone(
                        pk,
                        ck,
                        "v0",
                        base_ts + TS_CELL_TOMBSTONE_OFFSET,
                    ));
                } else {
                    out.push(row_tombstone(pk, ck, base_ts + TS_ROW_TOMBSTONE_OFFSET));
                }
                out
            }
            _ => vec![ttl_row(pk, ck, generation, base_ts, tag)],
        },
    }
}

fn table_id() -> cqlite_core::storage::write_engine::TableId {
    cqlite_core::storage::write_engine::TableId::new(OVERLAP_KEYSPACE, OVERLAP_TABLE)
}

fn keys(
    pk: i32,
    ck: i32,
) -> (
    cqlite_core::storage::write_engine::PartitionKey,
    Option<cqlite_core::storage::write_engine::ClusteringKey>,
) {
    use cqlite_core::storage::write_engine::{ClusteringKey, PartitionKey};
    use cqlite_core::types::Value;
    (
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
    )
}

/// A full live row (`v0`, `v1`, `v2` all written).
fn live_row(
    pk: i32,
    ck: i32,
    generation: usize,
    base_ts: i64,
    tag: u32,
) -> cqlite_core::storage::write_engine::Mutation {
    use cqlite_core::storage::write_engine::{CellOperation, Mutation};
    use cqlite_core::types::Value;

    let (partition, clustering) = keys(pk, ck);
    Mutation::new(
        table_id(),
        partition,
        clustering,
        vec![
            CellOperation::Write {
                column: "v0".to_string(),
                value: Value::text(format!("g{generation}-{tag}")),
            },
            CellOperation::Write {
                column: "v1".to_string(),
                value: Value::text(format!("v1-g{generation}")),
            },
            CellOperation::Write {
                column: "v2".to_string(),
                value: Value::Integer(tag as i32),
            },
        ],
        base_ts,
        None,
    )
}

/// A row whose written COLUMN SET alternates per generation, so reconciliation
/// must union per-column winners from different generations rather than pick one
/// whole-row winner.
fn blended_row(
    pk: i32,
    ck: i32,
    generation: usize,
    base_ts: i64,
    tag: u32,
) -> cqlite_core::storage::write_engine::Mutation {
    use cqlite_core::storage::write_engine::{CellOperation, Mutation};
    use cqlite_core::types::Value;

    let (partition, clustering) = keys(pk, ck);
    let ops = if generation % 2 == 0 {
        vec![
            CellOperation::Write {
                column: "v0".to_string(),
                value: Value::text(format!("blend-v0-g{generation}-{tag}")),
            },
            CellOperation::Write {
                column: "v2".to_string(),
                value: Value::Integer(tag as i32),
            },
        ]
    } else {
        vec![CellOperation::Write {
            column: "v1".to_string(),
            value: Value::text(format!("blend-v1-g{generation}-{tag}")),
        }]
    };
    Mutation::new(table_id(), partition, clustering, ops, base_ts, None)
}

/// A row carrying one already-expired and one not-yet-expired expiring cell at
/// [`PINNED_NOW_SECS`], plus a live-forever `v2`.
fn ttl_row(
    pk: i32,
    ck: i32,
    generation: usize,
    base_ts: i64,
    tag: u32,
) -> cqlite_core::storage::write_engine::Mutation {
    use cqlite_core::storage::write_engine::{CellOperation, Mutation};
    use cqlite_core::types::Value;

    let (partition, clustering) = keys(pk, ck);
    Mutation::new(
        table_id(),
        partition,
        clustering,
        vec![
            // Expired at the pin (and at any later wall clock).
            CellOperation::WriteWithTtl {
                column: "v0".to_string(),
                value: Value::text(format!("expired-g{generation}-{tag}")),
                ttl_seconds: FIXTURE_TTL_SECS,
                local_deletion_time: Some(EXPIRED_LDT_SECS),
            },
            // Live at the pin, expired under a wall-clock fallback — the cell
            // that makes the pin observable.
            CellOperation::WriteWithTtl {
                column: "v1".to_string(),
                value: Value::text(format!("live-g{generation}-{tag}")),
                ttl_seconds: FIXTURE_TTL_SECS,
                local_deletion_time: Some(LIVE_LDT_SECS),
            },
            CellOperation::Write {
                column: "v2".to_string(),
                value: Value::Integer(tag as i32),
            },
        ],
        base_ts,
        None,
    )
}

/// A row tombstone at `ts`.
fn row_tombstone(pk: i32, ck: i32, ts: i64) -> cqlite_core::storage::write_engine::Mutation {
    use cqlite_core::storage::write_engine::{CellOperation, Mutation};

    let (partition, clustering) = keys(pk, ck);
    Mutation::new(
        table_id(),
        partition,
        clustering,
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

/// A cell tombstone on `column` at `ts`.
fn cell_tombstone(
    pk: i32,
    ck: i32,
    column: &str,
    ts: i64,
) -> cqlite_core::storage::write_engine::Mutation {
    use cqlite_core::storage::write_engine::{CellOperation, Mutation};

    let (partition, clustering) = keys(pk, ck);
    Mutation::new(
        table_id(),
        partition,
        clustering,
        vec![CellOperation::Delete {
            column: column.to_string(),
            local_deletion_time: None,
        }],
        ts,
        None,
    )
}
