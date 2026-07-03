//! Deterministic fixture loaders shared by the cqlite-core micro-benchmarks
//! (Issue #537, Epic #541 Phase 1).
//!
//! Phase 1 benches run against small, fixed slices of the real Cassandra 5.0
//! SSTables already vendored under `test-data/datasets`. This module is the one
//! place that knows how to:
//!
//! - locate the dataset root (`CQLITE_DATASETS_ROOT` or a workspace-relative
//!   fallback) — [`datasets_root`];
//! - open a queryable [`cqlite_core::Database`] over a single fixture table,
//!   isolated in a temp dir so a bench run never mutates the shared corpus —
//!   [`open_read_db`] (requires the `cli-helpers` feature);
//! - build a [`WriteEngine`](cqlite_core::storage::write_engine::WriteEngine)
//!   against a temp dir for the write benches —
//!   [`open_write_engine`] (requires the `write-support` feature);
//! - hand out a fixed-seed RNG so key/partition selection is identical on
//!   every run and every machine — [`seeded_rng`].
//!
//! No network, no Docker, no live Cassandra. The fixtures are the binary
//! SSTables fetched by `test-data/scripts/fetch-datasets.sh`; if they are
//! missing the loaders panic with a pointer to that script.
//!
//! This file is included into each bench target via
//! `#[path = "fixtures/mod.rs"] mod fixtures;`, so every bench compiles its own
//! copy. Each bench uses only a subset of the helpers, hence the module-wide
//! `dead_code` allowance below — these are shared support functions, not
//! product code, so an unused helper in one bench is expected, not a smell.

#![allow(dead_code)]

use std::path::PathBuf;

/// Fixed RNG seed shared by every bench. Any key/partition/value selection that
/// is "random" must draw from [`seeded_rng`] so the selected set is byte-for-byte
/// identical across runs and machines — the core determinism guarantee of #537.
pub const BENCH_SEED: u64 = 0x00C0_FFEE_5EED_5EED;

/// A deterministic RNG seeded from [`BENCH_SEED`]. Same seed in, same sequence
/// out, so "pick N random keys" yields the same N keys every run.
pub fn seeded_rng() -> rand::rngs::StdRng {
    use rand::SeedableRng;
    rand::rngs::StdRng::seed_from_u64(BENCH_SEED)
}

/// Locate the `test-data/datasets` root.
///
/// Prefers `CQLITE_DATASETS_ROOT`; otherwise falls back to the workspace-relative
/// path derived from `CARGO_MANIFEST_DIR` (the crate dir is
/// `<workspace>/cqlite-core`, so the datasets live at
/// `<workspace>/test-data/datasets`). The fallback lets the benches run from a
/// plain checkout with fetched datasets and no environment setup.
pub fn datasets_root() -> PathBuf {
    match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets"),
    }
}

/// The `sstables/` subtree holding per-keyspace fixture data.
pub fn sstables_root() -> PathBuf {
    datasets_root().join("sstables")
}

/// The `test-data/schemas` directory (sibling of `datasets`).
pub fn schemas_root() -> PathBuf {
    datasets_root().join("../schemas")
}

/// Resolve the on-disk SSTable directory for `<keyspace>/<table>-<hash>`.
///
/// SSTable table directories carry a CFID hash suffix, so we glob on the
/// `<table>-` prefix and take the single match. Panics with an actionable
/// message if the fixture is absent (datasets not fetched).
pub fn table_dir(keyspace: &str, table: &str) -> PathBuf {
    let parent = sstables_root().join(keyspace);
    let prefix = format!("{table}-");
    let entry = std::fs::read_dir(&parent)
        .unwrap_or_else(|e| {
            panic!(
                "cannot read fixture keyspace dir {}: {e}.\n\
                 Fetch the SSTable fixtures first: bash test-data/scripts/fetch-datasets.sh",
                parent.display()
            )
        })
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with(&prefix));
    match entry {
        Some(e) => e.path(),
        None => panic!(
            "fixture table {keyspace}/{table} not found under {}.\n\
             Fetch the SSTable fixtures first: bash test-data/scripts/fetch-datasets.sh",
            parent.display()
        ),
    }
}

/// True if a fixture's on-disk SSTable directory is present under the datasets
/// root. Unlike [`table_dir`] this never panics — it returns `false` when the
/// keyspace dir or the `<table>-<hash>` directory is missing, so an *optional*
/// fixture (e.g. the BTI `test_da` corpus, absent in some checkouts) can be
/// skip-registered by a bench rather than aborting the run.
pub fn fixture_present(fx: &ReadFixture) -> bool {
    let parent = sstables_root().join(fx.keyspace);
    let prefix = format!("{}-", fx.table);
    std::fs::read_dir(&parent)
        .ok()
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .any(|e| e.file_name().to_string_lossy().starts_with(&prefix))
        })
        .unwrap_or(false)
}

/// A small, fixed read fixture: a keyspace/table in the vendored corpus plus the
/// schema file that decodes it. Descriptors are `const` so benches reference a
/// stable, named set rather than hard-coding paths.
#[derive(Clone, Copy, Debug)]
pub struct ReadFixture {
    /// Keyspace directory name under `sstables/`.
    pub keyspace: &'static str,
    /// Table name (without the CFID hash suffix).
    pub table: &'static str,
    /// Schema file under `test-data/schemas/` that decodes this table.
    pub schema_file: &'static str,
}

impl ReadFixture {
    /// `test_basic.simple_table` — 999 rows, `UUID` partition key, no clustering.
    /// The point-lookup and full-scan fixture.
    pub const SIMPLE: ReadFixture = ReadFixture {
        keyspace: "test_basic",
        table: "simple_table",
        schema_file: "basic-types.cql",
    };

    /// `test_da.simple_table` — the BTI (`da` format) analogue of [`Self::SIMPLE`]:
    /// `UUID PRIMARY KEY`, no clustering. The `read/get_partition_bti` point-read
    /// fixture. **Optional** — the `test_da` corpus is not present in every
    /// checkout, so benches must guard on [`fixture_present`] and skip-register
    /// when absent. Uses the BTI-specific `da-test.cql` schema (the table is
    /// declared under the `test_da` keyspace there, not `test_basic`).
    pub const SIMPLE_BTI: ReadFixture = ReadFixture {
        keyspace: "test_da",
        table: "simple_table",
        schema_file: "da-test.cql",
    };

    /// `test_timeseries.sensor_data` — partition + clustering layout. The
    /// clustering-slice fixture.
    pub const CLUSTERING: ReadFixture = ReadFixture {
        keyspace: "test_timeseries",
        table: "sensor_data",
        schema_file: "time-series.cql",
    };

    /// `test_collections.collection_table` — lists/sets/maps. The type-heavy
    /// decode fixture (isolates deserialization cost).
    pub const TYPE_HEAVY: ReadFixture = ReadFixture {
        keyspace: "test_collections",
        table: "collection_table",
        schema_file: "collections.cql",
    };

    /// Fully-qualified `keyspace.table` for use in CQL queries.
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }
}

/// A queryable database opened over a single fixture table, kept isolated in a
/// temp dir. Hold this for the lifetime of the bench: dropping it removes the
/// temp copy and the database's runtime files.
#[cfg(feature = "cli-helpers")]
pub struct ReadDb {
    /// The queryable handle. Run `db.execute("SELECT ...")` against it.
    pub db: cqlite_core::Database,
    // Kept alive so the isolated copy is not reaped while the db is in use.
    _tmp: tempfile::TempDir,
}

/// Open a queryable [`cqlite_core::Database`] over one fixture table.
///
/// The fixture's SSTable directory is copied into a fresh temp dir before
/// opening, so the database's runtime files (WAL, manifest) land in the temp
/// copy and never touch the shared `test-data` corpus — keeping repeated bench
/// runs deterministic and side-effect free. Uses only the public one-shot
/// ingestion API ([`cqlite_core::ingestion::ingest`]).
#[cfg(feature = "cli-helpers")]
pub fn open_read_db(fx: &ReadFixture) -> ReadDb {
    use cqlite_core::ingestion::{ingest, IngestionConfig};

    let src = table_dir(fx.keyspace, fx.table);
    let tmp = tempfile::TempDir::new().expect("create temp dir for read fixture");
    // Recreate the `<keyspace>/<table-hash>/` layout discovery expects.
    let dst = tmp
        .path()
        .join(fx.keyspace)
        .join(src.file_name().expect("fixture dir has a final component"));
    copy_dir_recursive(&src, &dst);

    let schema_path = schemas_root().join(fx.schema_file);
    let cfg = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: tmp.path().to_path_buf(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        // Substring match on the full table-dir path; narrows discovery to the
        // single fixture table even though only one was copied.
        table_directory_filter: Some(format!("/{}/{}", fx.keyspace, fx.table)),
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let db = rt
        .block_on(ingest(cfg))
        .expect("ingest read fixture")
        .database;

    ReadDb { db, _tmp: tmp }
}

/// CQL for the write-bench target table — a single `CREATE TABLE` so the
/// no-heuristics mandate (Issue #28) has an unambiguous write target. Mirrors
/// `test_basic.simple_table` (UUID PK) used by the read fixtures.
#[cfg(feature = "write-support")]
pub const WRITE_TABLE_CQL: &str = "\
CREATE TABLE test_basic.simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    active BOOLEAN
);";

/// Build a [`WriteEngine`](cqlite_core::storage::write_engine::WriteEngine) whose
/// data and WAL directories live under `dir` (typically a per-iteration
/// [`tempfile::TempDir`]). `flush_threshold` bytes controls when the memtable is
/// eligible to flush; pass a large value to bench pure ingest, a small value to
/// force flushes.
///
/// The engine is built with the default [`Durability::SyncEachWrite`] policy —
/// every `write()` call performs `wal.append()` + `wal.sync()` (fsync).  Use
/// [`open_write_engine_wal_off`] to benchmark the WAL-disabled path.
#[cfg(feature = "write-support")]
pub fn open_write_engine(
    dir: &std::path::Path,
    flush_threshold: usize,
) -> cqlite_core::storage::write_engine::WriteEngine {
    use cqlite_core::schema::parse_cql_schema;
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    let schema = parse_cql_schema(WRITE_TABLE_CQL).expect("parse write-bench schema");
    let cfg = WriteEngineConfig::new(dir.join("data"), dir.join("wal"), schema)
        .with_flush_threshold(flush_threshold);
    WriteEngine::new(cfg).expect("build write engine")
}

/// Build a [`WriteEngine`](cqlite_core::storage::write_engine::WriteEngine) with
/// [`Durability::Disabled`] — WAL append and fsync are skipped on every
/// `write()` call.  Mutations land only in the memtable; data is durable only
/// after an explicit [`WriteEngine::flush`].
///
/// Use this for the `write/ingest_wal_off` bench (Issue #574) to measure the
/// pure CPU/memtable ingest cost without I/O noise from fsync.
#[cfg(feature = "write-support")]
pub fn open_write_engine_wal_off(
    dir: &std::path::Path,
    flush_threshold: usize,
) -> cqlite_core::storage::write_engine::WriteEngine {
    use cqlite_core::schema::parse_cql_schema;
    use cqlite_core::storage::write_engine::{Durability, WriteEngine, WriteEngineConfig};

    let schema = parse_cql_schema(WRITE_TABLE_CQL).expect("parse write-bench schema");
    let cfg = WriteEngineConfig::new(dir.join("data"), dir.join("wal"), schema)
        .with_flush_threshold(flush_threshold)
        .with_durability(Durability::Disabled);
    WriteEngine::new(cfg).expect("build write engine (WAL off)")
}

/// Recursively copy a directory tree (files only; SSTable dirs are flat).
#[cfg(feature = "cli-helpers")]
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap_or_else(|e| panic!("create {}: {e}", dst.display()));
    for entry in std::fs::read_dir(src).unwrap_or_else(|e| panic!("read {}: {e}", src.display())) {
        let entry = entry.expect("dir entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir_recursive(&from, &to);
        } else {
            std::fs::copy(&from, &to)
                .unwrap_or_else(|e| panic!("copy {} -> {}: {e}", from.display(), to.display()));
        }
    }
}
