//! Single-partition lookup latency vs SSTable count (Issue #958, Epic #951).
//!
//! #949 made a fully-constrained `WHERE pk = ?` prune SSTables via the bloom
//! filter / BTI trie before parsing, so a single-partition read should scale
//! **sub-linearly** with the number of SSTables backing the table. The
//! `issue_958_partition_lookup_work_bound` integration test is the hard CI gate
//! (it fails if the read parses O(N) SSTables); this benchmark is the
//! complementary *trend* signal: it measures `SSTableManager::scan_partition`
//! latency for a fixed single-partition key against a table backed by an
//! increasing number of SSTable generations (4, 8, 16, 32).
//!
//! Intended observation: latency grows much slower than the generation count
//! (ideally flat, modulo per-SSTable bloom-check overhead). A regression to a
//! full scan would show latency growing roughly linearly with the generation
//! count.
//!
//! How to run / read it:
//! - `scripts/profile.sh bench` picks it up automatically (it is in
//!   `BENCH_TARGETS`), saving a criterion baseline under `target/criterion/`.
//! - Or directly:
//!   `cargo bench --package cqlite-core \
//!      --features write-support,state_machine \
//!      --bench partition_lookup_scaling`
//! - The criterion group is `partition_lookup_scaling`; each data point is named
//!   `sstables_<K>` so the report shows latency per generation count side by
//!   side. Compare the slope across K — sub-linear is the pass/observation.
//!
//! Requires `write-support` (to flush generations) and `state_machine` (the
//! `SSTableManager` reader stack). Built deterministically in a temp dir; no
//! fetched datasets needed. When those features are off, this target compiles to
//! an empty `main` so `cargo bench --no-run` still succeeds.

#[cfg(all(feature = "write-support", feature = "state_machine"))]
#[path = "profiling/mod.rs"]
mod profiling;

#[cfg(all(feature = "write-support", feature = "state_machine"))]
mod scaling {
    use cqlite_core::platform::Platform;
    use cqlite_core::schema::parse_cql_schema;
    use cqlite_core::storage::sstable::SSTableManager;
    use cqlite_core::storage::write_engine::{
        CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
    };
    use cqlite_core::types::{TableId as CqlTableId, Value};
    use cqlite_core::Config;
    use criterion::{black_box, BenchmarkId, Criterion, Throughput};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    const KS: &str = "scale_ks";
    const TBL: &str = "items";

    /// SSTable generation counts to sweep. The point is the *slope* across these,
    /// not any single value.
    const GENERATION_COUNTS: &[usize] = &[4, 8, 16, 32];

    fn schema_cql() -> String {
        format!("CREATE TABLE {KS}.{TBL} (id int PRIMARY KEY, name text, score int);")
    }

    fn write_row(id: i32, ts: i64) -> Mutation {
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(format!("name-{id}")),
            },
            CellOperation::Write {
                column: "score".to_string(),
                value: Value::Integer(id),
            },
        ];
        Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
    }

    /// A built fixture: a temp dir holding `n` SSTable generations, an open
    /// manager over it, and the partition key bytes for a key in one generation.
    struct Fixture {
        _temp: TempDir,
        manager: SSTableManager,
        table_id: CqlTableId,
        pk_bytes: Vec<u8>,
        schema: cqlite_core::schema::TableSchema,
    }

    /// Build `n` generations (one row each, disjoint keys), then open a manager.
    /// The target key (id = (n/2)*100 + 1) lives in exactly one generation.
    fn build_fixture(rt: &Runtime, n: usize) -> Fixture {
        let temp = TempDir::new().expect("temp dir");
        let data_dir = temp.path().join("data");
        let wal_dir = temp.path().join("wal");
        let schema = parse_cql_schema(&schema_cql()).expect("parse schema");

        let cfg = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
        let mut engine = WriteEngine::new(cfg).expect("engine");
        for g in 0..n {
            let id = (g as i32) * 100 + 1;
            engine.write(write_row(id, 100 + g as i64)).expect("write");
            rt.block_on(engine.flush())
                .expect("flush")
                .unwrap_or_else(|| panic!("generation {g} produced no SSTable"));
        }
        rt.block_on(engine.close()).expect("close");

        let target_id = (n as i32 / 2) * 100 + 1;
        let pk_bytes = cqlite_core::storage::partition_key_codec::encode_partition_key_columns(
            &[Value::Integer(target_id)],
            &schema,
        )
        .expect("encode partition key");

        let config = Config::default();
        let manager = rt.block_on(async {
            let platform = Arc::new(Platform::new(&config).await.expect("platform"));
            SSTableManager::new(&data_dir, &config, platform, None)
                .await
                .expect("open manager")
        });

        let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
        Fixture {
            _temp: temp,
            manager,
            table_id,
            pk_bytes,
            schema,
        }
    }

    pub fn bench(c: &mut Criterion) {
        let rt = Runtime::new().expect("tokio runtime");

        let mut group = c.benchmark_group("partition_lookup_scaling");
        // One logical lookup per iteration; reporting elements lets the report
        // show per-lookup latency directly.
        group.throughput(Throughput::Elements(1));

        for &n in GENERATION_COUNTS {
            let fx = build_fixture(&rt, n);

            // Sanity: the targeted lookup must return exactly the one partition,
            // so the bench measures real successful work, not an empty fast-exit.
            let probe = rt.block_on(fx.manager.scan_partition(
                &fx.table_id,
                &fx.pk_bytes,
                Some(&fx.schema),
            ));
            assert_eq!(
                probe.expect("probe lookup").len(),
                1,
                "fixture with {n} generations must resolve the target partition to 1 row"
            );

            group.bench_with_input(
                BenchmarkId::from_parameter(format!("sstables_{n}")),
                &n,
                |b, _| {
                    b.iter(|| {
                        let rows = rt
                            .block_on(fx.manager.scan_partition(
                                &fx.table_id,
                                black_box(&fx.pk_bytes),
                                Some(&fx.schema),
                            ))
                            .expect("scan_partition");
                        black_box(rows)
                    });
                },
            );
        }

        group.finish();
    }
}

#[cfg(all(feature = "write-support", feature = "state_machine"))]
criterion::criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = scaling::bench,
);

#[cfg(all(feature = "write-support", feature = "state_machine"))]
criterion::criterion_main!(benches);

// Without the required features there is nothing to measure; provide an empty
// entry point so `cargo bench --no-run` still builds this target.
#[cfg(not(all(feature = "write-support", feature = "state_machine")))]
fn main() {}
