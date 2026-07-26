//! k-parameterized multi-generation overlap fixtures (issue #2043 / M9).
//!
//! Generalizes the fixed-`L0_SSTABLES` same-`(pk, ck)`-across-generations pattern
//! of `benches/compaction.rs::build_tombstone_heavy` into a builder parameterized
//! by **k** (the number of SSTable generations a row cluster spans) crossed with a
//! collision-mix selector, so a bench can measure how per-row merge cost grows
//! with cluster depth (`docs/research/issue-2043-reconcile-overlap-multiplier.md`).
//!
//! Built on [`super::seeded_rng`] and the public `WriteEngine` flush API, so it is
//! deterministic and needs **no** vendored dataset corpus (`CQLITE_DATASETS_ROOT`
//! is never consulted): controlled `k` is the independent variable and the
//! vendored corpus is single-generation, so it cannot supply `k > 1` at all.
//!
//! ## `now` is pinned through the API, never the env var
//!
//! The TTL-bearing mixes are measured at [`PINNED_NOW_SECS`], threaded into the
//! merge via `KWayMerger::with_now_secs`. The read-path env seam
//! `CQLITE_TTL_NOW_OVERRIDE_SECS` is `#[cfg(debug_assertions)]` and compiles OUT
//! of the release profile `cargo bench` uses, where it silently falls back to the
//! wall clock — a bench pinned that way would drift run to run. This module must
//! therefore never mention that variable.

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

/// TTL stamped on the expiring cells. Only its presence matters for expiry (the
/// authoritative decision uses the explicit `local_deletion_time` above); it is
/// kept well below both LDTs so `ldt - ttl` (the parity tombstone LDT) is a
/// sane, non-saturating creation instant.
pub const FIXTURE_TTL_SECS: u32 = 600;

/// Partitions per generation.
pub const OVERLAP_PARTITIONS: usize = 16;

/// Clustering rows per partition, per generation. `OVERLAP_PARTITIONS *
/// OVERLAP_CK` = 1024 clusters per generation — enough rows that per-row cost
/// dominates per-scan fixed cost, small enough that k=20 stays a few-ms iteration.
pub const OVERLAP_CK: usize = 64;

/// Clusters (`(pk, ck)` pairs) each generation writes.
pub const CLUSTERS_PER_GEN: usize = OVERLAP_PARTITIONS * OVERLAP_CK;

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
    /// tombstones the lower half of its own clustering range (row tombstone) and
    /// cell-tombstones one column of the upper half — so both tombstone kinds
    /// collide with live cells at every k, including k=1.
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
    /// Total rows written across all generations — the merge's INPUT row count
    /// (the reconcile work), used to derive collisions-per-output-row.
    pub input_rows: u64,
    // Holds the temp dir alive: an `Arc<SSTableReader>` handed to
    // `new_from_readers` maps/reads these files for its whole lifetime, so the
    // fixture must outlive every merger built from it.
    _tmp: tempfile::TempDir,
}

/// Build a `k`-generation fixture for `mix`.
///
/// Writes each generation into its own memtable and flushes it, with NO merge
/// policy installed, so exactly `k` `Data.db` files remain uncompacted. Asserts
/// that count before returning: a fixture that silently compacted (or failed to
/// flush) would measure the wrong k.
pub fn build_multigen(k: usize, mix: OverlapMix) -> MultigenFixture {
    use cqlite_core::schema::parse_cql_schema;

    assert!(k >= 1, "k must be >= 1");
    let schema = parse_cql_schema(OVERLAP_TABLE_CQL).expect("parse overlap-fixture schema");
    let tmp = tempfile::TempDir::new().expect("temp dir for overlap fixture");
    let data_dir = tmp.path().join("data");
    let mut engine = open_fixture_engine(tmp.path(), &schema);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for overlap fixture flush");

    let mut oldest_first = Vec::with_capacity(k);
    let mut input_rows = 0u64;
    let mut rng = super::seeded_rng();
    for generation in 0..k {
        input_rows += write_generation(&mut engine, mix, generation, k, &mut rng);
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
        input_rows > 0,
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
        input_rows,
        _tmp: tmp,
    }
}

/// `WriteEngine` over [`OVERLAP_TABLE_CQL`] with a huge flush threshold (the
/// builder flushes explicitly, one flush per generation) and durability DISABLED.
///
/// Durability off is a *setup-cost* choice only: the fixture is untimed scaffolding
/// for a read-side merge measurement, and `SyncEachWrite` would fsync once per
/// mutation (~20k fsyncs at k=20). Nothing about the resulting SSTables differs.
fn open_fixture_engine(dir: &Path, schema: &TableSchema) -> WriteEngine {
    use cqlite_core::storage::write_engine::{Durability, WriteEngineConfig};

    let cfg = WriteEngineConfig::new(dir.join("data"), dir.join("wal"), schema.clone())
        .with_flush_threshold(usize::MAX)
        .with_durability(Durability::Disabled);
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

/// Write one generation's mutations into `engine`'s memtable, returning the
/// number of rows (mutations) written.
fn write_generation(
    engine: &mut WriteEngine,
    mix: OverlapMix,
    generation: usize,
    k: usize,
    rng: &mut rand::rngs::StdRng,
) -> u64 {
    use rand::Rng;

    let base_ts = TS_BASE + (generation as i64) * TS_GEN_STRIDE;
    let mut written = 0u64;
    for pk in 0..OVERLAP_PARTITIONS as i32 {
        for slot in 0..OVERLAP_CK as i32 {
            // `Disjoint` shifts each generation into its own ck window so no
            // cluster is ever shared; every other mix reuses the same window.
            let ck = match mix {
                OverlapMix::Disjoint => (generation as i32) * OVERLAP_CK as i32 + slot,
                _ => slot,
            };
            let tag: u32 = rng.gen();
            for m in generation_mutations(mix, pk, ck, slot, generation, k, base_ts, tag) {
                engine.write(m).expect("overlap fixture write");
                written += 1;
            }
        }
    }
    written
}

/// The mutations one `(pk, ck)` cluster receives from `generation` under `mix`.
fn generation_mutations(
    mix: OverlapMix,
    pk: i32,
    ck: i32,
    slot: i32,
    generation: usize,
    k: usize,
    base_ts: i64,
    tag: u32,
) -> Vec<cqlite_core::storage::write_engine::Mutation> {
    let half = (OVERLAP_CK / 2) as i32;
    match mix {
        // Disjoint / pure LWW: one full-row write per generation.
        OverlapMix::Disjoint | OverlapMix::LwwOverwrite => {
            vec![live_row(pk, ck, generation, base_ts, tag)]
        }
        // Live row + a same-generation tombstone: a row tombstone on the lower
        // clustering half, a cell tombstone on `v1` of the upper half. Both sit
        // one micro above the live write so they collide with it inside the
        // generation AND across generations.
        OverlapMix::Tombstone => {
            let mut out = vec![live_row(pk, ck, generation, base_ts, tag)];
            if slot < half {
                out.push(row_tombstone(pk, ck, base_ts + 1));
            } else {
                out.push(cell_tombstone(pk, ck, "v1", base_ts + 1));
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
                // Alternate the tombstone kind by generation so both shapes
                // appear even at small k; `k` keeps the pattern k-aware.
                if (generation + k) % 2 == 0 {
                    out.push(cell_tombstone(pk, ck, "v0", base_ts + 1));
                } else {
                    out.push(row_tombstone(pk, ck, base_ts + 1));
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
