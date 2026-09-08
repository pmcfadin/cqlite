//! Issue #4159 follow-on — an entry that merely *has* the `*-Data.db` NAME is not
//! an SSTable, and must not make a HEALTHY table unreadable.
//!
//! # The defect this pins
//!
//! #4159 made a generation that cannot be READ refuse instead of returning
//! `Ok(vec![])`. Correct — but candidate enumeration accepted anything matching the
//! `*-Data.db` name pattern, including entries that are not SSTables at all. A
//! DIRECTORY named `da-9-bti-Data.db`, or a SYMLINK named `da-8-bti-Data.db`
//! pointing at a sibling generation's `Data.db`, was handed to
//! `SSTableReader::open`, the open failed, and the refusal ledger then made the
//! ENTIRE table — every healthy generation in it — return
//! `Error::UnreadableSSTable`.
//!
//! That the data was fully readable is not an inference:
//! `scripts/tests/test_bti_perf_scan.sh` records that on the pre-#4159 binary the
//! same two planted entries made the harness report `generations: 3` while
//! `rows_scanned` stayed 468 and the exit code stayed 0. Only the generation COUNT
//! was ever wrong; #4159 turned that miscount into total loss of the table.
//!
//! # What is asserted, and what must NOT regress
//!
//! * a directory named `*-Data.db` beside a valid generation ⇒ the scan returns its
//!   rows, and the phantom generation is not counted;
//! * a symlink named `*-Data.db` pointing at a sibling generation's `Data.db` ⇒ the
//!   same;
//! * **the guard against over-correcting**: a REGULAR-FILE `*-Data.db` that is
//!   genuinely unreadable still refuses. The fix changes only what counts as a
//!   candidate; it must never become a swallow. This case is asserted explicitly so
//!   a future narrowing of the candidate test cannot silently remove it.
//!
//! # Fail-closed, per case
//!
//! Nothing here is dataset-dependent: every fixture is written by the test itself
//! into a `TempDir`, so every case is `must_run` unconditionally, with no corpus
//! loop and therefore no suite-wide `ran > 0` (#3220).

#![cfg(all(feature = "write-support", feature = "state_machine"))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, SchemaRegistry, SchemaRegistryConfig, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId as WriteTableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{TableId, Value};
use cqlite_core::{Config, Error};
use tempfile::TempDir;
use tokio::sync::RwLock;

const KEYSPACE: &str = "ks_4159_nonreg";
const TABLE: &str = "healthy";

fn table_id() -> TableId {
    TableId::from(format!("{KEYSPACE}.{TABLE}").as_str())
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Flush ONE generation of `<KEYSPACE>.<TABLE>` holding a single row `id = pk`
/// into `<root>/data`, returning its `Data.db` path.
async fn write_generation(root: &Path, pk: i32) -> PathBuf {
    let schema = schema();
    let config = WriteEngineConfig::new(
        root.join("data"),
        root.join("wal").join(format!("gen-{pk}")),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("WriteEngine::new");
    let mutation = Mutation::new(
        WriteTableId::new(KEYSPACE, TABLE),
        PartitionKey::single("id", Value::Integer(pk)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text(format!("row-{pk}")),
        }],
        1_700_000_000_000_000i64 + i64::from(pk),
        None,
    );
    engine.write_async(mutation).await.expect("write_async");
    let info = engine
        .flush()
        .await
        .expect("flush")
        .expect("flush must produce an SSTable");
    let data = info.data_path.clone();
    assert!(
        data.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with("-Data.db")),
        "the fixture generation must itself be a `*-Data.db` candidate, or the \
         planted strays below are not being compared against anything: {}",
        data.display()
    );
    assert!(
        std::fs::symlink_metadata(&data)
            .expect("lstat the written generation")
            .is_file(),
        "the fixture generation must be a REGULAR FILE, or this lane's accept case \
         is vacuous: {}",
        data.display()
    );
    data
}

async fn platform() -> Arc<Platform> {
    let config = Config::default();
    Arc::new(Platform::new(&config).await.expect("Platform::new"))
}

async fn empty_registry(platform: Arc<Platform>) -> Arc<RwLock<SchemaRegistry>> {
    let config = Config::default();
    Arc::new(RwLock::new(
        SchemaRegistry::new(SchemaRegistryConfig::default(), platform, config)
            .await
            .expect("SchemaRegistry::new"),
    ))
}

/// Which candidate-enumeration site a case exercises.
///
/// There are THREE in the read path and they are separate code:
/// `discovery_walk::find_data_files` (the recursive base-path walk),
/// `manager_open::load_from_table_directories` (the pre-discovered-paths
/// constructor) and `discovery_walk::discover_data_file_paths` (the refresh's
/// re-discovery). A fix at one says nothing about the others, so every case runs
/// all three.
#[derive(Clone, Copy, Debug)]
enum Site {
    /// `SSTableManager::new` ⇒ `load_existing_sstables` ⇒ `find_data_files`.
    BasePathWalk,
    /// `SSTableManager::new_from_discovered_paths` ⇒ `load_from_table_directories`.
    DiscoveredPaths,
    /// `SSTableManager::refresh_tables` ⇒ `discover_data_file_paths`, with the
    /// strays planted BEFORE the refresh so the refresh is what observes them.
    Refresh,
}

impl Site {
    const ALL: [Site; 3] = [Site::BasePathWalk, Site::DiscoveredPaths, Site::Refresh];

    /// Open a manager over `<root>/data` with `plant` applied at the moment that
    /// makes THIS site the observer of the planted entry: before construction for
    /// the two constructors, and AFTER construction (so the re-discovery is what
    /// sees it) for the refresh.
    /// `Err` can only come from the refresh: both constructors are best-effort by
    /// design (#4159) and are asserted to SUCCEED here, so a caller of `open` is
    /// deciding one thing only — whether a re-discovery may fail over the planted
    /// entry.
    async fn open(
        self,
        root: &Path,
        real: &Path,
        plant: &dyn Fn(&Path, &Path),
    ) -> Result<SSTableManager, Error> {
        let config = Config::default();
        let platform = platform().await;
        let registry = empty_registry(platform.clone()).await;
        let data = root.join("data");
        if !matches!(self, Site::Refresh) {
            plant(&table_dir(root), real);
        }
        let manager = match self {
            Site::BasePathWalk | Site::Refresh => {
                SSTableManager::new(&data, &config, platform, Some(registry)).await
            }
            Site::DiscoveredPaths => {
                let dir = data.join(KEYSPACE).join(TABLE);
                SSTableManager::new_from_discovered_paths(
                    &data,
                    vec![dir],
                    &config,
                    platform,
                    Some(registry),
                )
                .await
            }
        }
        .expect(
            "the constructors stay best-effort: they must SUCCEED with a stray entry \
             present (#4159 design decision)",
        );
        if matches!(self, Site::Refresh) {
            plant(&table_dir(root), real);
            manager.refresh_tables().await?;
        }
        Ok(manager)
    }
}

/// The table directory holding the flushed generations.
fn table_dir(root: &Path) -> PathBuf {
    root.join("data").join(KEYSPACE).join(TABLE)
}

/// Scan the table and return the rows, or the refusal.
async fn scan(manager: &SSTableManager) -> Result<usize, Error> {
    let schema = schema();
    manager
        .scan(&table_id(), None, None, None, Some(&schema))
        .await
        .map(|rows| rows.len())
}

/// A stray entry named `*-Data.db` must leave the table fully readable, and must
/// not be counted as a generation.
async fn assert_stray_is_ignored(what: &str, plant: impl Fn(&Path, &Path)) {
    for site in Site::ALL {
        let root = TempDir::new().expect("TempDir");
        let real = write_generation(root.path(), 1).await;

        let manager = site.open(root.path(), &real, &plant).await.unwrap_or_else(|e| {
            panic!(
                "{what} @ {site:?}: re-discovery FAILED over an entry that is not an \
                 SSTable at all ({e})"
            )
        });
        let rows = scan(&manager).await.unwrap_or_else(|e| {
            panic!(
                "{what} @ {site:?}: the scan REFUSED a healthy table because of an entry \
                 that is not an SSTable at all ({e}). A stray `*-Data.db` entry must not \
                 be a candidate."
            )
        });
        assert_eq!(
            rows, 1,
            "{what} @ {site:?}: the healthy generation's row must still be returned \
             (0-rows-when-present is a failure)"
        );
        let stats = manager.stats().await.expect("stats");
        assert_eq!(
            stats.sstable_count, 1,
            "{what} @ {site:?}: the stray must not be counted as a generation — a \
             phantom duplicate generation either double-counts rows or refuses on its \
             missing companions"
        );
    }
}

/// Case 1: a DIRECTORY named `*-Data.db`.
///
/// Pre-fix this made every read of the table return `Error::UnreadableSSTable`
/// naming the directory.
#[tokio::test]
async fn a_directory_named_data_db_does_not_make_the_table_unreadable() {
    assert_stray_is_ignored("directory named *-Data.db", |dir, _real| {
        std::fs::create_dir(dir.join("nb-9-big-Data.db")).expect("plant a directory");
    })
    .await;
}

/// Case 2: a SYMLINK named `*-Data.db` pointing at a sibling generation's
/// `Data.db` — a phantom duplicate generation: the same bytes under a second
/// generation number, with no `nb-8-big-Statistics.db` companion. Following it
/// either duplicates rows or refuses on the missing companion; neither is right.
///
/// Pre-fix this made every read of the table return `Error::UnreadableSSTable`.
#[cfg(unix)]
#[tokio::test]
async fn a_symlink_named_data_db_does_not_make_the_table_unreadable() {
    assert_stray_is_ignored("symlink named *-Data.db", |dir, real| {
        std::os::unix::fs::symlink(real, dir.join("nb-8-big-Data.db")).expect("plant a symlink");
    })
    .await;
}

/// Case 3 — THE GUARD AGAINST OVER-CORRECTING.
///
/// A REGULAR FILE named `*-Data.db` that cannot be read must STILL refuse. The fix
/// narrows what counts as a candidate; it must never narrow what a candidate's
/// failure means. Asserted explicitly so a future tightening of the candidate test
/// (say, one that also required a sibling `Statistics.db`) cannot silently turn
/// this refusal back into an empty success.
#[tokio::test]
async fn a_regular_file_data_db_that_cannot_be_read_still_refuses() {
    for site in Site::ALL {
        let root = TempDir::new().expect("TempDir");
        let real = write_generation(root.path(), 1).await;
        let stray = table_dir(root.path()).join("nb-9-big-Data.db");

        let manager = match site
            .open(root.path(), &real, &|dir, real| {
                plant_unreadable_generation(dir, real)
            })
            .await
        {
            Ok(manager) => manager,
            // The refresh site is allowed to refuse EARLIER: `refresh_tables`
            // propagates the open failure of a genuinely unreadable generation to
            // its caller. That is loud, which is the property under test — the only
            // forbidden answer is a silent success.
            Err(e) => {
                assert!(
                    matches!(site, Site::Refresh),
                    "{site:?}: only the refresh may surface the refusal before the \
                     read; a constructor is best-effort by design (#4159). Got {e:?}"
                );
                assert!(
                    e.to_string().contains("nb-9-big"),
                    "{site:?}: the refresh's refusal must name the generation it could \
                     not read; got {e}"
                );
                continue;
            }
        };
        match scan(&manager).await {
            Ok(n) => panic!(
                "{site:?}: the scan answered Ok with {n} row(s) while a regular-file \
                 `*-Data.db` generation was UNREADABLE. That is the #4159 swallow \
                 returning: the caller cannot tell an incomplete answer from a complete \
                 one."
            ),
            Err(Error::UnreadableSSTable { path, refused, .. }) => {
                assert_eq!(
                    path, stray,
                    "{site:?}: the refusal must name the generation that was refused"
                );
                assert!(
                    refused >= 1,
                    "{site:?}: a refusal reports at least one refused generation"
                );
            }
            Err(other) => panic!(
                "{site:?}: expected the dedicated Error::UnreadableSSTable so a caller can \
                 MATCH on \"unreadable table\" (never on message text, #28); got {other:?}"
            ),
        }
    }
}

/// Plant generation 9 as a REGULAR-FILE copy of the healthy generation whose
/// `Statistics.db` is cut inside its 32-byte outer header — the one damage class
/// that refuses at `SSTableReader::open` itself, so the refusal travels the
/// candidate → open → refusal-ledger path this fix touches.
///
/// The copy's validity is MEASURED, not assumed: the `Data.db` really is a regular
/// file and the damaged `Statistics.db` really is shorter than the header it
/// declares.
fn plant_unreadable_generation(dir: &Path, real: &Path) {
    let real_name = real
        .file_name()
        .and_then(|n| n.to_str())
        .expect("the healthy generation has a name");
    let prefix = real_name
        .strip_suffix("-Data.db")
        .expect("checked by write_generation");
    let mut copied = 0usize;
    for entry in std::fs::read_dir(dir).expect("read the table directory") {
        let entry = entry.expect("a table-directory entry");
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(component) = name.strip_prefix(prefix) else {
            continue;
        };
        std::fs::copy(entry.path(), dir.join(format!("nb-9-big{component}")))
            .expect("copy a component into generation 9");
        copied += 1;
    }
    assert!(
        copied >= 2,
        "expected at least a Data.db and a Statistics.db to copy, copied {copied}"
    );
    let stats = dir.join("nb-9-big-Statistics.db");
    let pristine = std::fs::read(&stats).expect("read the copied Statistics.db");
    assert!(
        pristine.len() > 20,
        "the copied Statistics.db must be longer than the truncation, or the damage \
         is a no-op"
    );
    // 20 < the 32-byte fixed header `parse_nb_format_header` reads, so the file's
    // own framing cannot parse and no downstream fallback can reach past it.
    std::fs::write(&stats, &pristine[..20]).expect("truncate the copied Statistics.db");
    assert!(
        std::fs::symlink_metadata(dir.join("nb-9-big-Data.db"))
            .expect("lstat the planted generation")
            .is_file(),
        "control: the planted generation's Data.db must be a REGULAR FILE, or this \
         case is testing case 1"
    );
}
