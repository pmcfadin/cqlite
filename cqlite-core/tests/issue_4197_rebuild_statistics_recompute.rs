//! Issue #4197 (spec R4) — `Statistics.db` rebuild is opt-in and lossy by
//! declaration: aggregates are recomputed from a full structural Data.db
//! decode; `repaired_at`/`pending_repair`/`is_transient` are recovered when
//! the original is readable, else lost; origin-host/compaction-ancestry are
//! unconditionally lost.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::write_engine::rebuild::{rebuild_components, Component, RebuildOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{copy_fixture_dir, require_fixtures_strict, single_data_db, table_schema};

const KEYSPACE: &str = "test_basic";
const TABLE: &str = "composite_key_table";
const SCHEMA_FILE: &str = "basic-types.cql";

fn fixture_dir_or_skip() -> Option<std::path::PathBuf> {
    let root = datasets_root::sstables_root_for_table(KEYSPACE, TABLE)?;
    datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE)
        .into_iter()
        .next()
}

/// Independently-derived partition count from the fixture's committed
/// `sstabledump` golden — the R4.1 oracle, never CQLite's own output.
fn golden_partition_count(data_db: &std::path::Path) -> usize {
    let mut golden_path = data_db.as_os_str().to_owned();
    golden_path.push(".jsonl");
    let text = std::fs::read_to_string(&golden_path)
        .unwrap_or_else(|e| panic!("golden {golden_path:?} must read: {e}"));
    text.lines().filter(|l| !l.trim().is_empty()).count()
}

/// R4.4 — `statistics` is never implied by a bare `--components` omission.
///
/// roborev finding (Medium): the original version of this test asserted
/// only `Component::parse_list("index,digest")` excludes `Statistics` — a
/// tautology over the PARSER, never exercising `rebuild_components` at
/// all. R4.4's actual claim is that a request omitting `statistics`
/// attempts NO `Statistics.db` write and never reports it in
/// `regenerated` — asserted here against the real function.
#[tokio::test]
async fn statistics_opt_in_enforced() {
    let Some(fixture_dir) = fixture_dir_or_skip() else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{TABLE} is absent; {}",
                datasets_root::describe_search(KEYSPACE, TABLE)
            );
        }
        eprintln!("[issue_4197] {KEYSPACE}.{TABLE} fixture absent; skipping");
        return;
    };
    let schema = table_schema(SCHEMA_FILE, TABLE, KEYSPACE);
    let temp = TempDir::new().expect("tempdir");
    let working = copy_fixture_dir(&fixture_dir, temp.path());
    let data_db = single_data_db(&working);
    let out = temp.path().join("out");
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };

    let requested = Component::parse_list("digest").unwrap();
    assert!(!requested.contains(&Component::Statistics));
    let report = rebuild_components(&data_db, &schema, &requested, &options)
        .await
        .expect("rebuild must succeed");
    assert!(report.refused.is_none(), "refused: {:?}", report.refused);
    assert!(
        !report.regenerated.iter().any(|c| c == "statistics"),
        "statistics must never appear in `regenerated` when not requested; report={report:?}"
    );
    // Statistics.db still lands under `--out` (untouched components are
    // always copied verbatim, spec R7.1's "complete component set"), but it
    // must be the ORIGINAL's bytes UNCHANGED — never a freshly-recomputed
    // one, since `statistics` was never requested.
    let original_stats = std::fs::read(fixture_dir.join(format!(
        "{}Statistics.db",
        single_data_db(&fixture_dir)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .trim_end_matches("Data.db")
    )))
    .expect("original fixture must carry a Statistics.db");
    let prefix = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap()
        .trim_end_matches("Data.db")
        .to_string();
    let copied_stats = std::fs::read(out.join(format!("{prefix}Statistics.db")))
        .expect("Statistics.db must still be copied verbatim into --out");
    assert_eq!(
        original_stats, copied_stats,
        "Statistics.db under --out must be an untouched verbatim copy when `statistics` was \
         never requested"
    );
}

/// R4.1 — aggregates recomputed correctly (partition_count against the
/// independent sstabledump-golden oracle).
#[tokio::test]
async fn statistics_recompute_partition_count_matches_golden() {
    let Some(fixture_dir) = fixture_dir_or_skip() else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{TABLE} is absent; {}",
                datasets_root::describe_search(KEYSPACE, TABLE)
            );
        }
        eprintln!("[issue_4197] {KEYSPACE}.{TABLE} fixture absent; skipping");
        return;
    };
    let schema = table_schema(SCHEMA_FILE, TABLE, KEYSPACE);
    let temp = TempDir::new().expect("tempdir");
    let working = copy_fixture_dir(&fixture_dir, temp.path());
    let data_db = single_data_db(&working);
    let expected_partitions = golden_partition_count(
        &fixture_dir.join(single_data_db(&fixture_dir).file_name().expect("file name")),
    );
    assert!(
        expected_partitions > 0,
        "golden yielded zero partitions — this test would be vacuous"
    );

    let out = temp.path().join("out");
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };
    let report = rebuild_components(&data_db, &schema, &[Component::Statistics], &options)
        .await
        .expect("rebuild must succeed");
    assert!(report.refused.is_none(), "refused: {:?}", report.refused);
    assert!(report.regenerated.iter().any(|c| c == "statistics"));

    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::statistics_reader::StatisticsReader;
    use std::sync::Arc;
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let prefix = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap()
        .trim_end_matches("Data.db")
        .to_string();
    let stats_path = out.join(format!("{prefix}Statistics.db"));
    let reader = StatisticsReader::open(&stats_path, platform)
        .await
        .expect("open rebuilt Statistics.db");
    let partition_count = reader.statistics().row_stats.partition_count;
    assert_eq!(
        partition_count as usize, expected_partitions,
        "{KEYSPACE}.{TABLE}: recomputed partition_count differs from the independent \
         sstabledump-golden partition count"
    );

    let classification = report
        .classification
        .get("statistics")
        .unwrap_or_else(|| panic!("no `statistics` classification in report={report:?}"));
    for field in [
        "min_timestamp",
        "max_timestamp",
        "partition_count",
        "row_count",
    ] {
        assert_eq!(
            classification.get(field).map(String::as_str),
            Some("recomputed"),
            "field {field}, report={report:?}"
        );
    }
    for field in ["origin_host", "compaction_ancestry"] {
        assert_eq!(
            classification.get(field).map(String::as_str),
            Some("lost"),
            "field {field}, report={report:?}"
        );
    }

    eprintln!(
        "[issue_4197] {KEYSPACE}.{TABLE}: recomputed partition_count={partition_count} matches \
         the independent sstabledump-golden oracle ({expected_partitions})."
    );
}

/// R4.2/R4.3 — repair-coordination fields are `recovered` when the original
/// `Statistics.db` (or an explicit recovery-source override) parses, `lost`
/// (never guessed) when it is genuinely gone.
#[tokio::test]
async fn statistics_repair_fields_recovered_vs_lost() {
    let Some(fixture_dir) = fixture_dir_or_skip() else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{TABLE} is absent; {}",
                datasets_root::describe_search(KEYSPACE, TABLE)
            );
        }
        eprintln!("[issue_4197] {KEYSPACE}.{TABLE} fixture absent; skipping");
        return;
    };
    let schema = table_schema(SCHEMA_FILE, TABLE, KEYSPACE);

    // R4.2: rename the original Statistics.db aside (readable, but not at
    // its expected sibling path) and pass it as an explicit recovery
    // source.
    {
        let temp = TempDir::new().expect("tempdir");
        let working = copy_fixture_dir(&fixture_dir, temp.path());
        let data_db = single_data_db(&working);
        let prefix = data_db
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .trim_end_matches("Data.db")
            .to_string();
        let stats_path = working.join(format!("{prefix}Statistics.db"));
        // Renamed aside to a DIFFERENT DIRECTORY, same filename —
        // `VersionGates::from_path` derives the format/version gates from
        // the FILENAME's own `<version>-<gen>-<format>-` convention, so an
        // arbitrarily-renamed file (verified the hard way) would make the
        // "renamed aside, but still readable" scenario indistinguishable
        // from a genuinely corrupt one.
        let aside_dir = temp.path().join("aside");
        std::fs::create_dir_all(&aside_dir).expect("create aside dir");
        let renamed_aside = aside_dir.join(format!("{prefix}Statistics.db"));
        std::fs::rename(&stats_path, &renamed_aside).expect("rename Statistics.db aside");

        let out = temp.path().join("out");
        let options = RebuildOptions {
            out_dir: out.clone(),
            statistics_recovery_source: Some(renamed_aside),
        };
        let report = rebuild_components(&data_db, &schema, &[Component::Statistics], &options)
            .await
            .expect("rebuild must succeed");
        assert!(report.refused.is_none(), "refused: {:?}", report.refused);
        let classification = report
            .classification
            .get("statistics")
            .unwrap_or_else(|| panic!("no `statistics` classification in report={report:?}"));
        for field in ["repaired_at", "pending_repair", "is_transient"] {
            assert_eq!(
                classification.get(field).map(String::as_str),
                Some("recovered"),
                "R4.2 field {field}, report={report:?}"
            );
        }
    }

    // R4.3: delete the original outright (genuinely unreadable, no
    // recovery-source override) — must classify `lost`, never `recovered`
    // with a fabricated default.
    {
        let temp = TempDir::new().expect("tempdir");
        let working = copy_fixture_dir(&fixture_dir, temp.path());
        let data_db = single_data_db(&working);
        let prefix = data_db
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .trim_end_matches("Data.db")
            .to_string();
        std::fs::remove_file(working.join(format!("{prefix}Statistics.db")))
            .expect("delete Statistics.db");

        let out = temp.path().join("out");
        let options = RebuildOptions {
            out_dir: out.clone(),
            statistics_recovery_source: None,
        };
        let report = rebuild_components(&data_db, &schema, &[Component::Statistics], &options)
            .await
            .expect("rebuild must succeed");
        assert!(report.refused.is_none(), "refused: {:?}", report.refused);
        let classification = report
            .classification
            .get("statistics")
            .unwrap_or_else(|| panic!("no `statistics` classification in report={report:?}"));
        for field in ["repaired_at", "pending_repair", "is_transient"] {
            assert_eq!(
                classification.get(field).map(String::as_str),
                Some("lost"),
                "R4.3 field {field}, report={report:?}"
            );
        }

        // Never a fabricated `recovered`-looking default — the actual
        // decoded value must be the honest zero/none/false too.
        use cqlite_core::parser::repair_metadata::{parse_repair_metadata, RepairField};
        use cqlite_core::storage::sstable::version_gate::VersionGates;
        let stats_path = out.join(format!("{prefix}Statistics.db"));
        let bytes = std::fs::read(&stats_path).expect("read rebuilt Statistics.db");
        let gates = VersionGates::from_path(&stats_path).expect("version gates");
        let md = parse_repair_metadata(&bytes, Some(&gates)).expect("parse repair metadata");
        assert_eq!(md.repaired_at, 0);
        assert_eq!(md.pending_repair, RepairField::Decoded(None));
        assert_eq!(md.is_transient, RepairField::Decoded(false));
    }

    eprintln!(
        "[issue_4197] {KEYSPACE}.{TABLE}: repair-state recovered-vs-lost classification \
         verified both directions."
    );
}
