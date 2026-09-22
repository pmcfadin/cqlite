//! Issue #4197 (spec R5) — rebuild REFUSES, writing nothing, when a
//! chunk-CRC check over `Data.db` fails or a partition fails to decode
//! structurally while walking it, and names `salvage` (#4196) as the
//! remedy.
//!
//! Oracle: `test_comp_corrupt/data_db_bit_flip` — a Cassandra-verified
//! corrupt fixture (`cassandra_verdict: corrupt`,
//! `corruption-manifest.yml`), captured against the REAL Apache Cassandra
//! 5.0.2 `sstableverify --extended --force` outcome. Not a synthetic
//! CQLite-fabricated corruption.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};

use cqlite_core::storage::write_engine::rebuild::{
    rebuild_components, Component, RebuildOptions, RefusalReason,
};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use rebuild_fixtures::{require_fixtures_strict, single_data_db, table_schema};

/// The corruption corpus lives beside `sstables/` under
/// `corruption/test_comp_corrupt/<fixture>/`, not resolvable through
/// `datasets_root::sstables_root_for_table` (that helper walks
/// `sstables/<keyspace>/<table>-*`, a different tree shape). Resolve
/// directly against each dataset-root candidate instead.
fn corrupt_fixture_dir(name: &str) -> Option<PathBuf> {
    for sstables_root in datasets_root::sstables_root_candidates() {
        // `sstables_root_candidates` names paths ending in `.../sstables` —
        // `corruption/` is its SIBLING under the same dataset root, per
        // `fetch-datasets.sh`'s own layout.
        let Some(dataset_root) = sstables_root.parent() else {
            continue;
        };
        let candidate = dataset_root
            .join("corruption")
            .join("test_comp_corrupt")
            .join(name);
        if candidate.join("nb-1-big-Data.db").is_file() {
            return Some(candidate);
        }
    }
    None
}

#[tokio::test]
async fn rebuild_refuses_on_a_cassandra_verified_corrupt_data_db() {
    const FIXTURE: &str = "data_db_bit_flip";
    let Some(fixture_dir) = corrupt_fixture_dir(FIXTURE) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the corruption fixture {FIXTURE} is absent under \
                 any dataset-root candidate: {:?}",
                datasets_root::sstables_root_candidates()
            );
        }
        eprintln!("[issue_4197] corruption fixture {FIXTURE} absent; skipping");
        return;
    };

    let schema = table_schema("compression-parity.cql", "lz4_table", "test_comp");
    let data_db = single_data_db(&fixture_dir);

    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");

    let input_sha_before = sha256_of(&data_db);
    let options = RebuildOptions {
        out_dir: out.clone(),
        statistics_recovery_source: None,
    };
    let report = rebuild_components(&data_db, &schema, &[Component::Index], &options)
        .await
        .expect("rebuild_components returns Ok even when refusing (design D3)");

    let refusal = report
        .refused
        .unwrap_or_else(|| panic!("expected a refusal for a Cassandra-verified corrupt Data.db"));
    assert_eq!(refusal.reason, RefusalReason::DataCorrupt);
    assert!(
        refusal.remedy.to_lowercase().contains("salvage"),
        "remedy must name salvage (#4196); got: {}",
        refusal.remedy
    );
    assert!(
        report.regenerated.is_empty(),
        "a refused run must regenerate nothing; got {:?}",
        report.regenerated
    );

    // R5.2 — the input is never modified, and nothing lands under --out.
    assert_eq!(
        input_sha_before,
        sha256_of(&data_db),
        "rebuild must never modify its input Data.db, even on refusal"
    );
    assert!(
        !out.exists()
            || std::fs::read_dir(&out)
                .map(|mut d| d.next().is_none())
                .unwrap_or(true),
        "a refused run must write NOTHING to --out"
    );

    eprintln!(
        "[issue_4197] {FIXTURE}: rebuild correctly refused a Cassandra-verified corrupt \
         Data.db, naming salvage as the remedy: {}",
        refusal.remedy
    );
}

fn sha256_of(path: &Path) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    Sha256::digest(bytes).to_vec()
}
