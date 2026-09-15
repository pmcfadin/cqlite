//! Issue #4196, roborev round 23 High finding — `salvage_sstable` must
//! normalize the caller's schema against the INPUT generation's own
//! serialization header, exactly as `compact_sstables` does, so a stale
//! `--schema` cannot silently drop a column the damaged file still carries.
//!
//! # The defect
//!
//! `compact_sstables` calls `effective_compaction_schema` +
//! `apply_udt_marshals_from_inputs` unconditionally before decode
//! (`merge/mod.rs`). `salvage_sstable` called NEITHER: the caller's raw
//! `--schema` went straight into `SSTableWriter::with_format` AND into the
//! per-partition decoder. Given a hand-written schema that omits a static
//! column the input's header declares — an ordinary stale-schema operator
//! mistake, close to the median reason anyone reaches for a recovery tool —
//! `compact` self-healed and logged it, while `salvage` wrote a SMALLER
//! `Data.db` whose serialization header did not declare the column at all,
//! reported `recovered=N lost=0`, and exited 0. Salvage is one-shot: once the
//! damaged input is gone, so is that column.
//!
//! # The oracle, and why it is not CQLite-vs-CQLite
//!
//! CLAUDE.md's #3042 blind spot: a CQLite-WRITTEN + CQLite-READ round trip is
//! INVARIANT to a uniform serialization defect, so it cannot be the oracle for
//! an on-disk property. This lane therefore anchors on Cassandra twice over,
//! and neither expectation is derived from CQLite's prior salvage behaviour:
//!
//!   1. **The input's own Cassandra-written `Statistics.db`** — the expected
//!      column set is READ OUT of the real Cassandra 5.0 fixture's
//!      serialization header (`static_data`, `is_static: true`). If that
//!      header ever stops declaring it, this lane fails closed rather than
//!      quietly asserting nothing.
//!   2. **`compact_sstables` over the identical input with the identical
//!      (deliberately incomplete) schema** — the byte-parity-proven-vs-
//!      Cassandra write path (issue #1017). Salvage's DIVERGENCE from it, given
//!      identical inputs, IS the bug; agreement with it is the fix.
//!
//! Fixture: `test_basic.static_columns_table` (real Cassandra 5.0, BIG `nb`,
//! Snappy). Its `-Data.db` is NOT git-tracked (`git ls-files` shows only the
//! `*.jsonl`/`Digest.crc32`/`TOC.txt`/`Statistics.db.txt` sidecars), so per
//! issue #3220's PER-CASE doctrine this case takes the SKIP route when the
//! fetched corpus is absent, and `CQLITE_REQUIRE_FIXTURES=1` turns that skip
//! into a hard failure. It never passes on an empty dataset: every assertion
//! below is reached only after a real `Data.db` is resolved, and the row/
//! partition counts are asserted non-zero.

// `not(tombstones)`: matches `write_engine::salvage`'s own gate — see that
// module's declaration in `write_engine/mod.rs`.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::salvage::{salvage_sstable, SalvageOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_basic";
const TABLE: &str = "static_columns_table";
const SCHEMA_FILE: &str = "basic-types.cql";
/// The static column the fixture's Cassandra-written header declares and the
/// stale schema below deliberately omits.
const STALE_OMITTED_COLUMN: &str = "static_data";

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn table_schema() -> TableSchema {
    let schema_path = datasets_root::schema_path(SCHEMA_FILE).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = KEYSPACE.to_string();
    t
}

fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        if e.file_name().to_string_lossy().ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.pop().expect("exactly one"),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// Every column the SSTable's own serialization header declares, and the
/// subset it marks STATIC — read from `Statistics.db` beside `data_db`.
///
/// This is the Cassandra-authored fact the whole lane hangs on (oracle 1), and
/// the SAME surface `effective_compaction_schema` reads, so an unreadable or
/// unparseable header is a hard failure here rather than an empty set that
/// would make every comparison below vacuously true.
fn header_columns(data_db: &Path, subject: &str) -> (BTreeSet<String>, BTreeSet<String>) {
    let stats_path = {
        let name = data_db
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| panic!("{subject}: Data.db path has no file name"));
        data_db.with_file_name(format!("{}Statistics.db", name.trim_end_matches("Data.db")))
    };
    let bytes = std::fs::read(&stats_path)
        .unwrap_or_else(|e| panic!("{subject}: Statistics.db {stats_path:?} unreadable: {e}"));
    let (_, stats) =
        cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &bytes, None,
        )
        .unwrap_or_else(|e| panic!("{subject}: Statistics.db {stats_path:?} unparseable: {e}"));
    let all: BTreeSet<String> = stats
        .serialization_header_columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let statics: BTreeSet<String> = stats
        .serialization_header_columns
        .iter()
        .filter(|c| c.is_static)
        .map(|c| c.name.clone())
        .collect();
    assert!(
        !all.is_empty(),
        "{subject}: serialization header declares ZERO columns — a present-but-empty header would \
         make every column-set comparison in this lane vacuously true"
    );
    (all, statics)
}

/// `full` minus the static column, i.e. the stale hand-written `--schema` an
/// operator would plausibly type. Fails closed if the committed CQL fixture
/// stops declaring that column, since the removal is what the lane measures.
fn stale_schema(full: &TableSchema) -> TableSchema {
    let mut stale = full.clone();
    let before = stale.columns.len();
    stale.columns.retain(|c| c.name != STALE_OMITTED_COLUMN);
    assert_eq!(
        stale.columns.len() + 1,
        before,
        "the committed {SCHEMA_FILE} definition of {TABLE} no longer declares a \
         `{STALE_OMITTED_COLUMN}` column, so this lane would compare two identical schemas and \
         prove nothing"
    );
    stale
}

/// A salvage run whose caller schema omits a static column the input's own
/// serialization header declares must reach the SAME column set
/// `compact_sstables` reaches from the same inputs — never a smaller one.
#[tokio::test]
async fn salvage_with_a_stale_schema_matches_compaction_on_the_effective_column_set() {
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{TABLE} is absent; {}",
                datasets_root::describe_search(KEYSPACE, TABLE)
            );
        }
        eprintln!(
            "[issue_4196] {KEYSPACE}.{TABLE} fixture absent (dataset not fetched; its Data.db is \
             not git-tracked); skipping — set CQLITE_REQUIRE_FIXTURES=1 to make this a failure"
        );
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE}: no usable generation directory"));
    let data_db = single_data_db(&fixture_dir);

    // ---- Oracle 1: the input's own Cassandra-written serialization header ----
    let (input_columns, input_statics) = header_columns(&data_db, "the Cassandra-written input");
    assert!(
        input_statics.contains(STALE_OMITTED_COLUMN),
        "the fixture's Cassandra-written header must declare `{STALE_OMITTED_COLUMN}` STATIC for \
         this lane to mean anything; header statics: {input_statics:?}, all columns: \
         {input_columns:?}"
    );

    let full = table_schema();
    let stale = stale_schema(&full);

    let temp = TempDir::new().expect("tempdir");
    let compact_root = temp.path().join("compact");
    let salvage_root = temp.path().join("salvage");

    // ---- Oracle 2: compact_sstables over the IDENTICAL input and schema ----
    let compact_report = compact_sstables(
        vec![data_db.clone()],
        &compact_root,
        &stale,
        41961,
        None,  // gc_before: no purging
        None,  // now
        false, // purge_safe: single input, never proven overlap-safe
    )
    .await
    .expect("no-purge single-input compaction must succeed");
    assert!(
        compact_report.stats.output_rows > 0,
        "the compaction oracle wrote zero rows — the fixture is empty, which would make this \
         comparison vacuous"
    );
    let compact_out = compact_root.join(KEYSPACE).join(TABLE);

    let report = salvage_sstable(&data_db, &salvage_root, &stale, SalvageOptions::default())
        .await
        .expect("salvage of a healthy fixture must succeed");
    let salvage_out = salvage_root.join(KEYSPACE).join(TABLE);

    assert!(
        report.refused.is_none() && report.losses.is_empty(),
        "healthy fixture salvage must neither refuse nor lose partitions; report={report:?}"
    );
    assert!(
        report.partitions.recovered > 0 && report.partitions.written > 0,
        "salvage recovered/wrote nothing, so the header comparison below would be vacuous; \
         partitions={:?}",
        report.partitions
    );

    // ---- The assertion the defect fails ----
    let (compact_columns, compact_statics) = header_columns(
        &single_data_db(&compact_out),
        "the compaction oracle's output",
    );
    let (salvage_columns, salvage_statics) =
        header_columns(&single_data_db(&salvage_out), "the salvaged output");

    assert!(
        compact_statics.contains(STALE_OMITTED_COLUMN),
        "the compaction oracle itself dropped `{STALE_OMITTED_COLUMN}`, so it cannot serve as the \
         reference here; oracle statics: {compact_statics:?}"
    );
    assert!(
        salvage_statics.contains(STALE_OMITTED_COLUMN),
        "SALVAGE DROPPED the static column `{STALE_OMITTED_COLUMN}`: the input's own header \
         declares it (statics {input_statics:?}) and the compaction oracle preserved it (statics \
         {compact_statics:?}), but the salvaged output's serialization header declares only \
         {salvage_columns:?} — the caller's stale schema was used verbatim instead of being \
         normalized against the input header"
    );
    assert_eq!(
        salvage_columns, compact_columns,
        "the salvaged output's serialization-header column set differs from the compaction \
         oracle's for the same input and the same caller schema"
    );

    // The manifest must SAY it self-healed — an operator reading the report has
    // to learn that their `--schema` and the recovered generation now differ.
    let normalized: Vec<&str> = report
        .component_findings
        .iter()
        .filter(|f| f.class == "SchemaNormalizedFromHeader")
        .map(|f| f.detail.as_str())
        .collect();
    assert!(
        normalized.iter().any(|d| d.contains(STALE_OMITTED_COLUMN)),
        "the manifest must carry a SchemaNormalizedFromHeader finding NAMING \
         `{STALE_OMITTED_COLUMN}`; component_findings={:?}",
        report.component_findings
    );

    // ---- Content, not just the header: the cells must be there too ----
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::SSTableReader;
    use std::sync::Arc;
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let compact_rows =
        SSTableReader::open(&single_data_db(&compact_out), &config, platform.clone())
            .await
            .expect("open compaction-oracle output")
            // Decoded with the FULL schema (what a correctly-configured reader has),
            // so a dropped static cell shows up as a content difference too.
            .iterate_all_partitions_for_compaction(Some(&full))
            .await
            .expect("decode compaction-oracle rows");
    let salvage_rows = SSTableReader::open(&single_data_db(&salvage_out), &config, platform)
        .await
        .expect("open salvaged output")
        .iterate_all_partitions_for_compaction(Some(&full))
        .await
        .expect("decode salvaged rows");
    assert!(
        !compact_rows.is_empty(),
        "the compaction oracle's output decoded zero rows — vacuous comparison"
    );
    assert_eq!(
        compact_rows, salvage_rows,
        "salvaged rows differ in content from the compaction oracle's for the same input and the \
         same (stale) caller schema"
    );

    eprintln!(
        "[issue_4196] {KEYSPACE}.{TABLE}: a schema omitting `{STALE_OMITTED_COLUMN}` was \
         normalized from the input header; salvage and compact_sstables agree on the output \
         header column set ({} column(s), statics {salvage_statics:?}) and on all {} decoded \
         row(s). {} partition(s) recovered, 0 lost.",
        salvage_columns.len(),
        salvage_rows.len(),
        report.partitions.recovered
    );
}
