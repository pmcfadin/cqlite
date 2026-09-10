//! Issue #4196 (spec R1) — salvage of a HEALTHY SSTable is a no-purge
//! compaction of it: byte-identical output to `compact_sstables` over the
//! same single input with purging disabled, and an affirmative empty loss
//! list (`losses: 0 RECOGNISED`).
//!
//! # Oracle
//!
//! Committed, Cassandra-written fixtures — `test_basic.composite_key_table`
//! (BIG/`nb`, LZ4) and `test_da.multiclustering_table` (BTI/`da`, LZ4).
//! CQLite's `compact_sstables` is itself already byte-parity-proven against
//! Cassandra elsewhere (issue #1017); this test's job is narrower and
//! specific to #4196: that `salvage_sstable`'s independent recovery loop
//! (boundary enumeration -> decode-at-offset -> reconcile -> write) reaches
//! the SAME bytes `compact_sstables` reaches for a healthy input, proving the
//! two share the real reconciliation/conversion code rather than two
//! implementations that happen to agree on one fixture (design D1).
//!
//! Dataset doctrine (issue #719): SKIP when a fixture is genuinely absent;
//! `CQLITE_REQUIRE_FIXTURES=1` turns that into a hard failure.

#![cfg(feature = "write-support")]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::salvage::{salvage_sstable, SalvageOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn table_schema(
    schema_file: &str,
    table: &str,
    keyspace: &str,
) -> cqlite_core::schema::TableSchema {
    let schema_path = datasets_root::schema_path(schema_file).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {table}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = keyspace.to_string();
    t
}

fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir).expect("read fixture dir").flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.pop().unwrap(),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// Component suffixes present under `dir` (strips the `<version>-<gen>-<big|bti>-`
/// descriptor prefix). Drops derived golden sidecars no engine emits.
fn component_suffixes(dir: &Path) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            for marker in ["-big-", "-bti-"] {
                if let Some(idx) = name.find(marker) {
                    set.insert(name[idx + marker.len()..].to_string());
                    break;
                }
            }
        }
    }
    set.retain(|s| !s.ends_with(".jsonl") && !s.ends_with("Statistics.db.txt"));
    set
}

fn descriptor_prefix(data_db: &Path) -> String {
    data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .trim_end_matches("Data.db")
        .to_string()
}

fn read_component(dir: &Path, suffix: &str) -> Vec<u8> {
    let data = single_data_db(dir);
    let prefix = descriptor_prefix(&data);
    let path = dir.join(format!("{prefix}{suffix}"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("component {path:?} unreadable in an output dir: {e}"))
}

fn first_diff(a: &[u8], b: &[u8]) -> Option<usize> {
    let n = a.len().max(b.len());
    (0..n).find(|&i| a.get(i) != b.get(i))
}

/// Run the R1 parity assertion for one fixture. `byte_for_byte` names the
/// components salvage and `compact_sstables` MUST byte-match for a healthy
/// single-input run — both are a no-purge, single-source reconciliation of
/// the SAME partitions through the SAME writer, so every component either
/// side names is expected to match (checked via the component-SET equality
/// below); this list is the ones asserted BYTE-IDENTICAL, chosen to mirror
/// issue #1017's cross-engine byte set for the format family.
async fn assert_healthy_salvage_matches_no_purge_compaction(
    keyspace: &str,
    table: &str,
    schema_file: &str,
    out_generation: u64,
    byte_for_byte: &[&str],
) {
    let Some(root) = datasets_root::sstables_root_for_table(keyspace, table) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} is absent; {}",
                datasets_root::describe_search(keyspace, table)
            );
        }
        eprintln!("[issue_4196] {keyspace}.{table} fixture absent (dataset not fetched); skipping");
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, keyspace, table)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{keyspace}.{table}: no usable generation directory"));

    let schema = table_schema(schema_file, table, keyspace);
    let data_db = single_data_db(&fixture_dir);

    let temp = TempDir::new().expect("tempdir");
    let compact_out_root = temp.path().join("compact");
    let salvage_out_root = temp.path().join("salvage");

    let compact_report = compact_sstables(
        vec![data_db.clone()],
        &compact_out_root,
        &schema,
        out_generation,
        None,  // gc_before: no purging
        None,  // now: irrelevant, no TTL expiry evaluated without a cutoff
        false, // purge_safe: single-input, never proven overlap-safe
    )
    .await
    .expect("no-purge single-input compaction must succeed");
    assert!(
        compact_report.stats.output_rows > 0,
        "compaction oracle wrote zero rows — the fixture itself is empty, which would make \
         this test vacuous"
    );
    let compact_out = compact_out_root.join(&schema.keyspace).join(&schema.table);

    let salvage_report = salvage_sstable(
        &data_db,
        &salvage_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of a healthy fixture must succeed");
    let salvage_out = salvage_out_root.join(&schema.keyspace).join(&schema.table);

    // R1.2 — affirmative empty loss list; totals must be non-degenerate.
    assert!(
        salvage_report.losses.is_empty(),
        "{keyspace}.{table}: healthy fixture salvage reported losses: {:?}",
        salvage_report.losses
    );
    assert!(
        salvage_report.refused.is_none(),
        "{keyspace}.{table}: healthy fixture salvage refused: {:?}",
        salvage_report.refused
    );
    assert_eq!(
        salvage_report.partitions.total, salvage_report.partitions.recovered,
        "{keyspace}.{table}: total must equal recovered when there are zero losses"
    );
    assert!(
        salvage_report.partitions.total > 0,
        "{keyspace}.{table}: salvage reported zero total partitions on a non-empty fixture — \
         the boundary enumeration itself is broken, which would make the empty-loss assertion \
         vacuous"
    );
    let text = salvage_report.render_text();
    assert!(
        text.contains("losses: 0 RECOGNISED"),
        "{keyspace}.{table}: text rendering must carry the affirmative empty-loss line; got:\n{text}"
    );

    // R1.1 — byte-for-byte parity with the no-purge compaction oracle.
    let compact_components = component_suffixes(&compact_out);
    let salvage_components = component_suffixes(&salvage_out);
    assert_eq!(
        compact_components, salvage_components,
        "{keyspace}.{table}: component set differs between compact_sstables output and salvage \
         output"
    );
    for suffix in byte_for_byte {
        assert!(
            compact_components.contains(*suffix),
            "{keyspace}.{table}: compaction oracle missing component {suffix}"
        );
        let a = read_component(&compact_out, suffix);
        let b = read_component(&salvage_out, suffix);
        if a != b {
            let at = first_diff(&a, &b);
            panic!(
                "{keyspace}.{table}: {suffix} byte mismatch between compact_sstables ({} bytes) \
                 and salvage ({} bytes), first diff at {at:?}",
                a.len(),
                b.len()
            );
        }
    }

    eprintln!(
        "[issue_4196] {keyspace}.{table}: salvage of a healthy SSTable byte-matches a no-purge \
         single-input compaction ({byte_for_byte:?}); {} partition(s) recovered, 0 lost.",
        salvage_report.partitions.recovered
    );
}

#[tokio::test]
async fn salvage_of_healthy_big_sstable_matches_no_purge_compaction() {
    assert_healthy_salvage_matches_no_purge_compaction(
        "test_basic",
        "composite_key_table",
        "basic-types.cql",
        4196,
        &["Data.db", "Index.db", "Summary.db", "CRC.db"],
    )
    .await;
}

/// BTI counterpart of the above. `compact_sstables` (design note discovered
/// while implementing #4196) always emits BIG output regardless of the
/// input's on-disk format — `cqlite compact` has no format-preservation
/// knob — so a literal byte-for-byte comparison against it cannot hold for a
/// BTI input (its component set is structurally different: Partitions.db +
/// Rows.db vs Index.db + Summary.db). This is a genuine premise gap in the
/// R1.1 scenario as written for BTI inputs, reported rather than
/// hand-waved; a `compact_sstables` format-preservation option is
/// out-of-scope follow-up work, not a #4196 blocker.
///
/// The oracle here instead is CQLite's own compaction-row decoder applied to
/// BOTH the original input and salvage's output: every [`CompactionRow`]
/// salvage's boundary-source-driven recovery loop wrote must decode back out
/// byte-identically to what a full scan of the ORIGINAL input decodes —
/// proving salvage lost nothing and fabricated nothing for a healthy BTI
/// input, independent of `compact_sstables`'s BIG-only limitation.
#[tokio::test]
async fn salvage_of_healthy_bti_sstable_preserves_every_row() {
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::SSTableReader;
    use std::sync::Arc;

    const KEYSPACE: &str = "test_da";
    const TABLE: &str = "multiclustering_table";
    const SCHEMA_FILE: &str = "multiclustering-table-bti.cql";

    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{TABLE} is absent; {}",
                datasets_root::describe_search(KEYSPACE, TABLE)
            );
        }
        eprintln!("[issue_4196] {KEYSPACE}.{TABLE} fixture absent (dataset not fetched); skipping");
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE}: no usable generation directory"));

    let schema = table_schema(SCHEMA_FILE, TABLE, KEYSPACE);
    let data_db = single_data_db(&fixture_dir);

    let temp = TempDir::new().expect("tempdir");
    let salvage_out_root = temp.path().join("salvage");
    let salvage_report = salvage_sstable(
        &data_db,
        &salvage_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of a healthy BTI fixture must succeed");
    assert!(
        salvage_report.losses.is_empty() && salvage_report.refused.is_none(),
        "{KEYSPACE}.{TABLE}: healthy BTI fixture salvage should have zero losses; report={salvage_report:?}"
    );
    assert!(
        salvage_report.partitions.total > 0
            && salvage_report.partitions.total == salvage_report.partitions.recovered,
        "{KEYSPACE}.{TABLE}: expected total == recovered > 0, got {:?}",
        salvage_report.partitions
    );
    let salvage_out = salvage_out_root.join(&schema.keyspace).join(&schema.table);
    let salvage_data_db = single_data_db(&salvage_out);

    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let input_reader = SSTableReader::open(&data_db, &config, platform.clone())
        .await
        .expect("open input reader");
    let output_reader = SSTableReader::open(&salvage_data_db, &config, platform)
        .await
        .expect("open salvage output reader");

    let input_rows = input_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("decode input rows");
    let output_rows = output_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("decode salvage output rows");

    assert!(
        !input_rows.is_empty(),
        "{KEYSPACE}.{TABLE}: input decoded zero rows — the fixture itself is empty, which \
         would make this test vacuous"
    );
    assert_eq!(
        input_rows.len(),
        output_rows.len(),
        "{KEYSPACE}.{TABLE}: salvage output row count differs from the original input"
    );
    assert_eq!(
        input_rows, output_rows,
        "{KEYSPACE}.{TABLE}: salvage output rows differ in content from the original input"
    );

    eprintln!(
        "[issue_4196] {KEYSPACE}.{TABLE}: healthy BTI salvage preserved all {} row(s) \
         byte-identically (compaction-row decode); 0 losses.",
        input_rows.len()
    );
}
