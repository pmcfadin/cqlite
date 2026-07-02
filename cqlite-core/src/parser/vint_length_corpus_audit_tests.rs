//! Issue #1623 — corpus-differential test for `parse_vint_length`.
//!
//! This is the pinned parity test for the unsigned-length fix. It walks every
//! real Cassandra SSTable under `$CQLITE_DATASETS_ROOT/sstables`, runs a full
//! structural scan of each Data.db, and — via the `length_decode_audit` hook in
//! `parse_vint_length` — tallies, at every real length/count decode offset,
//! whether the OLD signed (ZigZag) decode would have AGREED with the fixed
//! unsigned decode. A disagreement is a length field the pre-fix decoder was
//! silently mis-reading.
//!
//! It must be a LIB unit test (not an integration test): the audit hook is
//! `#[cfg(test)]`, which is only compiled for the crate's own unit-test build.
//!
//! Fixture-gating (repo doctrine): SKIPs cleanly when the dataset binaries are
//! absent, but treats "present but zero tables" as a FAILURE. `CQLITE_DATASETS_ROOT`
//! keyed; `CQLITE_REQUIRE_FIXTURES=1` turns the absent-corpus skip into a hard fail.

use crate::parser::vint::length_decode_audit;
use crate::storage::sstable::reader::SSTableReader;
use crate::types::TableId;
use crate::{Config, Platform};
use std::path::PathBuf;
use std::sync::Arc;

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let p = PathBuf::from(root);
    if p.is_dir() {
        Some(p)
    } else {
        None
    }
}

/// Collect `(data_db_path, table_name)` for every SSTable generation with a
/// Data.db under `sstables/<keyspace>/<table>-<uuid>/`.
fn collect_tables(sstables: &std::path::Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let Ok(keyspaces) = std::fs::read_dir(sstables) else {
        return out;
    };
    for ks in keyspaces.flatten() {
        let ks_path = ks.path();
        if !ks_path.is_dir() {
            continue;
        }
        let Ok(tables) = std::fs::read_dir(&ks_path) else {
            continue;
        };
        for table in tables.flatten() {
            let table_dir = table.path();
            if !table_dir.is_dir() {
                continue;
            }
            // dir name is `<table>-<uuid>`; CQL identifiers cannot contain '-',
            // so the table name is everything before the final '-'.
            let dir_name = table_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            let table_name = dir_name
                .rsplit_once('-')
                .map(|(name, _uuid)| name.to_string())
                .unwrap_or_else(|| dir_name.to_string());
            let Ok(files) = std::fs::read_dir(&table_dir) else {
                continue;
            };
            for f in files.flatten() {
                let path = f.path();
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
                {
                    out.push((path, table_name.clone()));
                }
            }
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn corpus_differential_unsigned_length_decode() {
    let Some(root) = datasets_root() else {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is unset/not a dir"
        );
        eprintln!("SKIP: CQLITE_DATASETS_ROOT unset; corpus-differential test skipped.");
        return;
    };
    let sstables = root.join("sstables");
    if !sstables.is_dir() {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but {}/sstables is absent",
            root.display()
        );
        eprintln!(
            "SKIP: {}/sstables absent; corpus-differential test skipped.",
            root.display()
        );
        return;
    }

    let tables = collect_tables(&sstables);
    if tables.is_empty() {
        // Present-but-empty is a failure regardless of require_fixtures: a
        // clean checkout ships JSONL only, but if the sstables/ tree exists it
        // must contain Data.db binaries.
        panic!(
            "corpus present at {} but no Data.db files found — fetch datasets \
             (bash test-data/scripts/fetch-datasets.sh)",
            sstables.display()
        );
    }

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init should succeed"),
    );

    length_decode_audit::arm();
    let mut scanned = 0usize;
    let mut scans_ok = 0usize;
    let mut failures: Vec<String> = Vec::new();

    for (data_db, table_name) in &tables {
        scanned += 1;
        let reader = match SSTableReader::open(data_db, &config, platform.clone()).await {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("open {}: {e}", data_db.display()));
                continue;
            }
        };
        let table_id = TableId::new(table_name.clone());
        match reader.scan(&table_id, None, None, None, None).await {
            Ok(_rows) => scans_ok += 1,
            Err(e) => failures.push(format!("scan {}: {e}", data_db.display())),
        }
    }

    let (agree, disagree) = length_decode_audit::disarm();

    eprintln!(
        "Issue #1623 corpus differential: tables={scanned} scans_ok={scans_ok} \
         length_decodes_agree={agree} length_decodes_disagree={disagree}"
    );
    if !failures.is_empty() {
        eprintln!("Issue #1623 scan failures ({}):", failures.len());
        for f in &failures {
            eprintln!("  - {f}");
        }
    }

    assert!(scanned > 0, "corpus present but no tables were scanned");
    // The fixed unsigned length decoder must let every corpus SSTable open and
    // scan without a structural (length/count) parse error.
    assert!(
        failures.is_empty(),
        "{} corpus SSTable(s) failed to open/scan with the unsigned length decoder",
        failures.len()
    );
}
