//! Issue #4159 — a scan over an SSTable that cannot be READ must return `Err`
//! naming the cause, never `Ok(vec![])`.
//!
//! # The defect
//!
//! Both `SSTableManager` constructors load discovered generations best-effort. A
//! generation whose `SSTableReader::open` failed was logged at `warn!` and then
//! merely absent from the reader map, so `SSTableManager::new` returned `Ok`, the
//! table's reader list came out EMPTY, and each read surface's
//! `reader_list.is_empty()` guard answered `Ok(Vec::new())`. A caller could not tell
//! "this table is empty" from "this table is unreadable and I gave you nothing" —
//! the #3721 swallow class at SSTable granularity, and the failure mode nothing
//! downstream can detect.
//!
//! # Why a DELIBERATELY-MALFORMED CQLite-WRITTEN fixture is the right instrument
//!
//! Issue #3042's rule — a CQLite-written + CQLite-read round-trip is invariant to a
//! uniform framing error and can never be the oracle for an ON-DISK
//! FRAMING/ENCODING property — does **not** bind here, and this note exists so a
//! later reviewer does not mistake this lane for a violation of it. The property
//! under test is **CQLite's own error propagation**: does a refusal raised inside
//! the metadata parse reach the caller of `scan`? That is a property of CQLite's
//! control flow, not of the Cassandra file format, so no Cassandra-written oracle
//! can express it — what is needed is a file whose metadata parse REFUSES, and the
//! cheapest honest way to get one is to write a well-formed SSTable and then damage
//! its `Statistics.db`.
//!
//! Each staged corruption is **MEASURED, not assumed**: `stage` asserts that
//! `parse_statistics_with_fallback` really does refuse the mutated bytes before any
//! read-surface expectation is stated, and every case pairs the mutated leg with a
//! PRISTINE control leg that must return its row. So "the read refused" can never
//! be a lane that refuses healthy data, and "the fixture is corrupt" is never taken
//! on trust.
//!
//! # Fail-closed, per case (AC5)
//!
//! Nothing here is dataset-dependent: every fixture is written by the test itself
//! into a `TempDir`, so every case is `must_run` unconditionally. There is
//! deliberately no corpus loop and therefore no suite-wide `ran > 0`.

#![cfg(all(feature = "write-support", feature = "state_machine"))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, SchemaRegistry, SchemaRegistryConfig, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId as WriteTableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{TableId, Value};
use cqlite_core::{Config, Error, ScanRow};
use tempfile::TempDir;
use tokio::sync::RwLock;

const KEYSPACE: &str = "ks_4159";
const TABLE: &str = "refusal";
const OTHER_TABLE: &str = "bystander";

/// The `table_readers` key the manager derives for `KEYSPACE.TABLE`.
fn table_id(table: &str) -> TableId {
    TableId::from(format!("{KEYSPACE}.{table}").as_str())
}

fn schema_for(table: &str) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: table.to_string(),
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

/// The on-disk components of one flushed generation.
struct Generation {
    data: PathBuf,
    statistics: PathBuf,
}

/// Flush ONE generation of `<KEYSPACE>.<table>` holding a single row `id = pk`
/// into `<root>/data`.
async fn write_generation(root: &Path, table: &str, pk: i32) -> Generation {
    let schema = schema_for(table);
    let config = WriteEngineConfig::new(
        root.join("data"),
        root.join("wal").join(format!("{table}-{pk}")),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("WriteEngine::new");
    let mutation = Mutation::new(
        WriteTableId::new(KEYSPACE, table),
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
    Generation {
        data: info.data_path.clone(),
        statistics: info.stats_path.clone(),
    }
}

/// How a staged fixture's `Statistics.db` is damaged.
///
/// Two INDEPENDENT refusal classes, because covering one says nothing about the
/// other: the first refuses in the file's outer framing, the second inside the
/// SerializationHeader's declared TYPE — the trigger issue #4159 was found through.
#[derive(Clone, Copy, Debug)]
enum Damage {
    /// The file is cut short, so the enhanced parser cannot complete its walk.
    /// This is the generic "metadata refusal" the issue insists is not specific to
    /// any one trigger.
    TruncatedStatistics,
    /// The partition-key marshal TYPE string in the SERIALIZATION_HEADER is made
    /// invalid UTF-8, which `parse_serialization_header_schema` refuses
    /// ("Invalid UTF-8 in partition key type"). This is the #4158-shaped trigger:
    /// a serialization-header type the reader legitimately will not decode.
    RefusedHeaderType,
    /// The file is cut inside its 32-byte OUTER header, so `parse_nb_format_header`
    /// itself cannot complete.
    ///
    /// # Why this class exists, and why it is the load-bearing RED control
    ///
    /// It is the ONE class that refuses at `f22ce842b` too. The other two are
    /// swallowed *below* the manager by the SerializationHeader MARKER SEARCH this
    /// issue also removes: pre-fix, both still parsed SUCCESSFULLY, so pre-fix they
    /// cannot reach the read surfaces at all and cannot demonstrate the
    /// SSTable-granularity swallow. This class reaches it, which is what makes the
    /// pre-fix measurement `scan` → `Ok` with 0 rows rather than a fixture-staging
    /// failure. Keep all three: they refuse at three different depths (outer
    /// framing, TOC-positioned content, declared type) and a fix at one says
    /// nothing about the others.
    TruncatedToOuterHeader,
}

impl Damage {
    fn apply(self, bytes: &[u8]) -> Vec<u8> {
        match self {
            // Keep the 32-byte header and the TOC so the file still looks like a
            // Statistics.db and the refusal comes from the CONTENT walk, not from
            // "this is not a Statistics.db at all".
            Damage::TruncatedStatistics => bytes[..bytes.len() / 2].to_vec(),
            Damage::RefusedHeaderType => {
                const MARSHAL: &[u8] = b"org.apache.cassandra.db.marshal.";
                let at = bytes
                    .windows(MARSHAL.len())
                    .position(|w| w == MARSHAL)
                    .expect("a written Statistics.db carries a marshal type name");
                let mut out = bytes.to_vec();
                // 0xFF can never begin a valid UTF-8 sequence, so
                // `std::str::from_utf8` on the type string fails and the schema
                // parser returns its `Verify` refusal. The LENGTH is untouched, so
                // the failure is specifically "the declared type does not decode".
                out[at] = 0xFF;
                out
            }
            // 20 < the 32-byte `parse_nb_format_header` fixed header, so the walk
            // cannot even read the file's own framing. No downstream fallback can
            // reach past a header that did not parse.
            Damage::TruncatedToOuterHeader => bytes[..20.min(bytes.len())].to_vec(),
        }
    }

    /// A fragment the SURFACED error must contain, distinct per class.
    ///
    /// This is AC1's "naming the cause" half, and it is checked per class ON
    /// PURPOSE: a single shared fragment would pass even if all three refusals
    /// collapsed back to one indistinguishable message, which is exactly the defect
    /// the `nom::error::Error`-shaped path had (`code: Verify` plus a hex dump, the
    /// same text for a corrupt outer header and for an undecodable declared type).
    fn expected_cause_fragment(self) -> &'static str {
        match self {
            Damage::TruncatedStatistics => "SERIALIZATION_HEADER",
            Damage::RefusedHeaderType => "the partition key type is not valid UTF-8",
            Damage::TruncatedToOuterHeader => "outer header did not parse",
        }
    }
}

/// Damage `generation`'s `Statistics.db` and MEASURE that the statistics parse now
/// refuses it — the fixture's validity is asserted, never assumed.
fn stage(generation: &Generation, damage: Damage) {
    let pristine = std::fs::read(&generation.statistics).expect("read Statistics.db");
    assert!(
        parse_statistics_with_fallback(&pristine, None).is_ok(),
        "control: the PRISTINE Statistics.db must parse, or the mutated-leg \
         expectations below are meaningless"
    );
    let damaged = damage.apply(&pristine);
    assert_ne!(
        damaged, pristine,
        "{damage:?} must actually change the bytes"
    );
    std::fs::write(&generation.statistics, &damaged).expect("write damaged Statistics.db");
    assert!(
        parse_statistics_with_fallback(&damaged, None).is_err(),
        "{damage:?} must make the Statistics.db parse REFUSE — otherwise this lane \
         is not staging a metadata refusal at all"
    );
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

/// Open a manager over `<root>/data` — the `SSTableManager::new` (base-path
/// discovery) constructor, the one the reproducer used.
async fn manager(root: &Path) -> SSTableManager {
    let config = Config::default();
    let platform = platform().await;
    let registry = empty_registry(platform.clone()).await;
    SSTableManager::new(&root.join("data"), &config, platform, Some(registry))
        .await
        .expect(
            "the constructor stays best-effort: it must still SUCCEED with an unreadable \
             generation present, because failing it would make one corrupt file render \
             every other table unreadable (#4159 design decision)",
        )
}

/// Open a manager over the pre-discovered table directories — the
/// `new_from_discovered_paths` sibling constructor, which `DiscoveryService`-driven
/// callers use. Fixing only `new` would leave the swallow reachable through here.
async fn manager_from_discovered(root: &Path, table: &str) -> SSTableManager {
    let config = Config::default();
    let platform = platform().await;
    let registry = empty_registry(platform.clone()).await;
    let dir = root.join("data").join(KEYSPACE).join(table);
    SSTableManager::new_from_discovered_paths(
        &root.join("data"),
        vec![dir],
        &config,
        platform,
        Some(registry),
    )
    .await
    .expect("the sibling constructor is best-effort too")
}

/// Assert `e` is the dedicated, MATCHABLE refusal and that it carries the original
/// cause — never a message check, which would stay green through a refactor that
/// re-wrapped the cause in a different variant while forwarding its text.
fn assert_unreadable(surface: &str, e: &Error, expected_path: &Path, damage: Damage) {
    match e {
        Error::UnreadableSSTable {
            table,
            path,
            refused,
            source,
        } => {
            assert!(
                table.contains(TABLE),
                "{surface}: the refusal must name the table being read, got {table:?}"
            );
            assert_eq!(
                path, expected_path,
                "{surface}: the refusal must name the generation that was refused"
            );
            assert!(
                *refused >= 1,
                "{surface}: a refusal reports at least one refused generation"
            );
            // The ORIGINAL open failure, by reference count — this is the AC1
            // "naming the cause" half. `source` is walked through the std trait so
            // the assertion is about the error CHAIN, not about our own field.
            let source: &Error = source;
            let rendered = source.to_string();
            assert!(
                rendered.contains(damage.expected_cause_fragment()),
                "{surface}: the carried cause must NAME why the file was refused \
                 (expected to contain {:?} for {damage:?}); got: {rendered}",
                damage.expected_cause_fragment()
            );
            assert!(
                !rendered.contains("code: Verify"),
                "{surface}: the cause must not be the opaque nom `ErrorKind` the old \
                 path surfaced for every distinct refusal alike; got: {rendered}"
            );
            assert!(
                std::error::Error::source(e).is_some(),
                "{surface}: the refusal must expose the open failure as its \
                 std::error::Error::source, so a caller can walk the chain"
            );
        }
        other => panic!(
            "{surface}: expected the dedicated Error::UnreadableSSTable so a caller can \
             MATCH on \"unreadable table\" (never on message text, #28); got {other:?}"
        ),
    }
}

/// What one read surface answered.
struct SurfaceOutcome {
    name: &'static str,
    outcome: Result<usize, Error>,
}

/// Every ROW-RETURNING read surface `SSTableManager` exposes, evaluated over
/// `manager` for `table`.
///
/// The set is deliberately the whole public row-returning surface and not just
/// `scan`: PR #3814 (the #3721 fix) was green on six surfaces and FALSE on the BTI
/// point-read path, because "an AC satisfied on the surfaces you tested is not an AC
/// satisfied". `Ok(n)` carries the row count so a surface that answered a SILENT
/// EMPTY is distinguishable from one that answered rows.
async fn observe(manager: &SSTableManager, table: &str) -> Vec<SurfaceOutcome> {
    let tid = table_id(table);
    let schema = schema_for(table);
    // The partition key bytes never matter to the refusal — it is raised during
    // reader resolution, before any key is consulted — so a fixed encoding of
    // `id = 1` (int is 4 bytes big-endian in Cassandra) is enough for the
    // partition-targeted surfaces.
    let pk_bytes = 1i32.to_be_bytes();
    let mut out = Vec::new();

    out.push(SurfaceOutcome {
        name: "scan",
        outcome: manager
            .scan(&tid, None, None, None, Some(&schema))
            .await
            .map(|r| r.len()),
    });
    out.push(SurfaceOutcome {
        name: "scan_with_cell_metadata",
        outcome: manager
            .scan_with_cell_metadata(&tid, None, None, None, Some(&schema))
            .await
            .map(|r| r.len()),
    });
    out.push(SurfaceOutcome {
        name: "scan_partition",
        outcome: manager
            .scan_partition(&tid, &pk_bytes, Some(&schema))
            .await
            .map(|(r, _engaged)| r.len()),
    });
    // `scan_partition_clustering` and the reverse iterator exist only on the default
    // build: the `tombstones` build's partition surface is
    // `manager_tombstones_partition_scan.rs` (scan-and-filter), and `reverse_scan.rs`
    // carries `#![cfg(not(feature = "tombstones"))]`. Naming the cfg here keeps the
    // surface census honest per build instead of silently covering fewer surfaces
    // under `--all-features`.
    #[cfg(not(feature = "tombstones"))]
    out.push(SurfaceOutcome {
        name: "scan_partition_clustering",
        outcome: manager
            .scan_partition_clustering(&tid, &pk_bytes, None, Some(&schema))
            .await
            .map(|(r, _engaged)| r.len()),
    });
    out.push(SurfaceOutcome {
        name: "scan_partition_with_cell_metadata",
        outcome: manager
            .scan_partition_with_cell_metadata(&tid, &pk_bytes, Some(&schema))
            .await
            .map(|(r, _engaged)| r.len()),
    });
    out.push(SurfaceOutcome {
        name: "get",
        outcome: manager
            .get(&tid, &cqlite_core::RowKey::new(pk_bytes.to_vec()))
            .await
            .map(|r| usize::from(r.is_some())),
    });
    out.push(SurfaceOutcome {
        name: "scan_stream",
        outcome: drain_stream(manager, &tid, &schema).await,
    });
    out.push(SurfaceOutcome {
        name: "scan_stream_batched",
        outcome: drain_batched(manager, &tid, &schema).await,
    });
    out
}

async fn drain_stream(
    manager: &SSTableManager,
    tid: &TableId,
    schema: &TableSchema,
) -> Result<usize, Error> {
    let mut stream = manager
        .scan_stream(tid, None, None, Some(schema), 16)
        .await?;
    let mut n = 0usize;
    while let Some(item) = stream.recv().await {
        item?;
        n += 1;
    }
    Ok(n)
}

async fn drain_batched(
    manager: &SSTableManager,
    tid: &TableId,
    schema: &TableSchema,
) -> Result<usize, Error> {
    let mut stream = manager
        .scan_stream_batched(tid, None, None, Some(schema), 16)
        .await?;
    let mut n = 0usize;
    while let Some(item) = stream.recv().await {
        n += item?.len();
    }
    Ok(n)
}

/// AC3's RED CONTROL, and the core of AC1/AC2.
///
/// Before the fix EVERY surface below answered `Ok` with ZERO rows on a staged
/// fixture whose `Statistics.db` refuses; the pre-fix measurement is recorded in
/// the PR. Now every one of them must return `Error::UnreadableSSTable`.
async fn assert_every_surface_refuses(damage: Damage) {
    let root = TempDir::new().expect("TempDir");
    let generation = write_generation(root.path(), TABLE, 1).await;

    // Control leg FIRST, over the pristine bytes: prove the fixture reads before it
    // is damaged, so a refusal below is attributable to the damage alone.
    {
        let manager = manager(root.path()).await;
        for surface in observe(&manager, TABLE).await {
            let n = surface.outcome.unwrap_or_else(|e| {
                panic!(
                    "control leg: surface `{}` REFUSED a PRISTINE fixture ({e}) — the \
                     mutated-leg expectations would be meaningless",
                    surface.name
                )
            });
            assert_eq!(
                n, 1,
                "control leg: surface `{}` must see the one written row (0-rows-when-present \
                 is a failure)",
                surface.name
            );
        }
    }

    stage(&generation, damage);

    let manager = manager(root.path()).await;
    for surface in observe(&manager, TABLE).await {
        match &surface.outcome {
            Ok(n) => panic!(
                "{damage:?}: surface `{}` answered Ok with {n} row(s) over an UNREADABLE \
                 SSTable. That is the #4159 swallow: the caller cannot tell an empty table \
                 from an unreadable one.",
                surface.name
            ),
            Err(e) => assert_unreadable(surface.name, e, &generation.data, damage),
        }
    }
}

#[tokio::test]
async fn truncated_statistics_makes_every_read_surface_refuse() {
    assert_every_surface_refuses(Damage::TruncatedStatistics).await;
}

#[tokio::test]
async fn refused_serialization_header_type_makes_every_read_surface_refuse() {
    assert_every_surface_refuses(Damage::RefusedHeaderType).await;
}

/// AC3's RED CONTROL PROPER — the class that refuses pre-fix too, so the pre-fix
/// run reaches the read surfaces and MEASURES the swallow instead of failing while
/// staging the fixture. See [`Damage::TruncatedToOuterHeader`].
#[tokio::test]
async fn a_statistics_db_cut_inside_its_outer_header_makes_every_read_surface_refuse() {
    assert_every_surface_refuses(Damage::TruncatedToOuterHeader).await;
}

/// AC2's OTHER half: `Ok(empty)` must still mean "this table genuinely has no
/// data". A guard that answered `Err` on every empty reader list would satisfy AC1
/// and destroy the API.
#[tokio::test]
async fn a_genuinely_absent_table_still_answers_ok_empty() {
    let root = TempDir::new().expect("TempDir");
    // One healthy generation of a DIFFERENT table, so the manager is non-trivial.
    let _healthy = write_generation(root.path(), OTHER_TABLE, 7).await;

    let manager = manager(root.path()).await;
    let rows = manager
        .scan(&table_id("never_written"), None, None, None, None)
        .await
        .expect("a table with no SSTables is EMPTY, not unreadable");
    assert!(rows.is_empty(), "no SSTables ⇒ no rows");
}

/// The PARTIAL case: some generations opened and one did not. Still `Err`.
///
/// A partial answer presented as complete is the same silent-data-loss defect one
/// degree weaker, so this is deliberately NOT special-cased into a success. Note the
/// reader list here is NON-EMPTY, so the `reader_list.is_empty()` guard never fires
/// — which is exactly why the refusal check cannot live at that guard.
#[tokio::test]
async fn a_partially_readable_table_refuses_rather_than_answering_partially() {
    let root = TempDir::new().expect("TempDir");
    let first = write_generation(root.path(), TABLE, 1).await;
    let second = write_generation(root.path(), TABLE, 2).await;
    assert_ne!(
        first.data, second.data,
        "the two flushes must produce two distinct generations"
    );

    // Control: both generations readable ⇒ both rows.
    {
        let manager = manager(root.path()).await;
        let rows = manager
            .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
            .await
            .expect("control: two healthy generations must read");
        assert_eq!(rows.len(), 2, "control: both written rows must be visible");
    }

    stage(&second, Damage::TruncatedStatistics);

    let manager = manager(root.path()).await;
    let e = manager
        .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
        .await
        .expect_err(
            "one readable generation plus one refused one is a PARTIAL answer; returning it \
             under Ok is silent data loss",
        );
    assert_unreadable(
        "scan (partial)",
        &e,
        &second.data,
        Damage::TruncatedStatistics,
    );
}

/// The refusal must be SCOPED: one unreadable table must not render an unrelated
/// table under the same base path unreadable. This is why the fix records the
/// refusal per table instead of failing the constructor.
#[tokio::test]
async fn an_unrelated_table_under_the_same_base_path_still_reads() {
    let root = TempDir::new().expect("TempDir");
    let broken = write_generation(root.path(), TABLE, 1).await;
    let _bystander = write_generation(root.path(), OTHER_TABLE, 7).await;
    stage(&broken, Damage::TruncatedStatistics);

    let manager = manager(root.path()).await;

    manager
        .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
        .await
        .expect_err("the damaged table must refuse");

    let rows = manager
        .scan(
            &table_id(OTHER_TABLE),
            None,
            None,
            None,
            Some(&schema_for(OTHER_TABLE)),
        )
        .await
        .expect("an unrelated healthy table must still read");
    assert_eq!(
        rows.len(),
        1,
        "the bystander table's row must survive the sibling's refusal"
    );
    assert!(
        matches!(rows[0].1, ScanRow::Row(_)),
        "the bystander row must decode normally"
    );
}

/// The SIBLING CONSTRUCTOR. `new_from_discovered_paths` carried its own copy of the
/// discard, so fixing only `new` would leave the swallow reachable through every
/// `DiscoveryService`-driven caller.
#[tokio::test]
async fn the_discovered_paths_constructor_refuses_too() {
    let root = TempDir::new().expect("TempDir");
    let generation = write_generation(root.path(), TABLE, 1).await;

    {
        let manager = manager_from_discovered(root.path(), TABLE).await;
        let rows = manager
            .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
            .await
            .expect("control: the sibling constructor must read a healthy generation");
        assert_eq!(rows.len(), 1, "control: the written row must be visible");
    }

    stage(&generation, Damage::RefusedHeaderType);

    let manager = manager_from_discovered(root.path(), TABLE).await;
    let e = manager
        .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
        .await
        .expect_err("new_from_discovered_paths must record the refusal too");
    assert_unreadable(
        "scan (discovered paths)",
        &e,
        &generation.data,
        Damage::RefusedHeaderType,
    );
}

/// A refusal must not be PERMANENT: once the offending generation is gone from
/// disk, `refresh_tables` clears it and the table reads again. Without this a table
/// stays unreadable forever after the operator removed the bad file.
#[tokio::test]
async fn removing_the_refused_generation_and_refreshing_restores_readability() {
    let root = TempDir::new().expect("TempDir");
    let good = write_generation(root.path(), TABLE, 1).await;
    let bad = write_generation(root.path(), TABLE, 2).await;
    stage(&bad, Damage::TruncatedStatistics);

    let manager = manager(root.path()).await;
    manager
        .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
        .await
        .expect_err("while the bad generation is present the table is unreadable");

    // Remove every component of the refused generation.
    let dir = bad
        .data
        .parent()
        .expect("generation directory")
        .to_path_buf();
    let stem = bad
        .data
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix("-Data.db"))
        .expect("<version>-<gen>-<format>-Data.db")
        .to_string();
    for entry in std::fs::read_dir(&dir).expect("read generation directory") {
        let p = entry.expect("dir entry").path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if name.starts_with(&stem) {
            std::fs::remove_file(&p).expect("remove refused component");
        }
    }
    assert!(!bad.data.exists(), "the refused generation must be gone");
    assert!(good.data.exists(), "the healthy generation must remain");

    manager.refresh_tables().await.expect("refresh_tables");
    let rows = manager
        .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
        .await
        .expect("with the refused generation removed the table is readable again");
    assert_eq!(
        rows.len(),
        1,
        "the surviving generation's row must be returned"
    );
}

/// SIBLING (audit S25/S26): a `CompressionInfo.db` that is PRESENT but unreadable
/// must refuse, while an ABSENT one still means "uncompressed".
///
/// The two `create_minimal_*_header` builders answered BOTH with `"NONE"` under one
/// `Err` arm commented "Assuming no compression". Absence is legitimate — every
/// SSTable CQLite's own write surface emits is uncompressed and has no such
/// component (#1406) — but a CORRUPT one taking the same branch means reading
/// COMPRESSED chunk data as raw bytes, which decodes to nothing: a silently empty
/// read of a healthy file, the same class as the swallow this issue is about.
///
/// Both directions are asserted, because the fix would be worthless (and would
/// break every uncompressed read) if it refused on absence too.
///
/// NOTE, stated so this is not read as more than it is: the corrupt direction was
/// ALREADY refusing before this change, via `load_compression_info_metadata`'s
/// #1001 fail-fast on a present-but-malformed component. The
/// `create_minimal_*_header` change is therefore a LOCAL honesty fix — absence and
/// corruption are now distinguished at the probe that asks the question, instead of
/// one `"NONE"` arm relying on a different reader to refuse later — not a newly
/// closed data-loss path. Keeping the case anyway pins the end-to-end contract.
#[tokio::test]
async fn a_present_but_corrupt_compression_info_refuses_while_an_absent_one_does_not() {
    let root = TempDir::new().expect("TempDir");
    let generation = write_generation(root.path(), TABLE, 1).await;

    let compression_info = generation
        .data
        .to_string_lossy()
        .replace("-Data.db", "-CompressionInfo.db");
    let compression_info = PathBuf::from(compression_info);

    // Direction 1 — ABSENT. CQLite writes uncompressed SSTables, so the component
    // is genuinely not there and the table must read normally.
    assert!(
        !compression_info.exists(),
        "the write surface emits UNCOMPRESSED SSTables (#1406), so there should be no \
         CompressionInfo.db to begin with — this case's premise"
    );
    {
        let manager = manager(root.path()).await;
        let rows = manager
            .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
            .await
            .expect("an ABSENT CompressionInfo.db means UNCOMPRESSED, not unreadable");
        assert_eq!(rows.len(), 1, "the uncompressed table must still read");
    }

    // Direction 2 — PRESENT and unparseable.
    std::fs::write(&compression_info, b"not a CompressionInfo.db").expect("stage the component");
    let manager = manager(root.path()).await;
    let e = manager
        .scan(&table_id(TABLE), None, None, None, Some(&schema_for(TABLE)))
        .await
        .expect_err(
            "a PRESENT but unparseable CompressionInfo.db must refuse: answering \
             `algorithm = \"NONE\"` reads compressed chunks as raw bytes and decodes to \
             nothing",
        );
    // The OUTCOME is what this case pins, not the SITE: several component readers
    // touch `CompressionInfo.db` during one open (`create_minimal_*_header`'s
    // algorithm probe and, for a chunk-compressed Data.db,
    // `load_compression_info_metadata`'s #1001 fail-fast), and which one reaches the
    // damaged component first depends on whether that generation's Data.db is
    // headerless. Asserting a particular message here would pin the dispatch order
    // rather than the contract. The absence/corruption split at the probe itself is
    // pinned by `load_nb_compression_info`'s own unit tests in
    // `reader/header.rs`, which call it directly.
    assert!(
        matches!(e, Error::UnreadableSSTable { .. }),
        "the refusal must reach the caller as the dedicated matchable variant; got {e:?}"
    );
}
