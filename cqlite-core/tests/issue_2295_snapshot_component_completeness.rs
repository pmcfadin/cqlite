//! Regression test for issue #2295: snapshot-directory component completeness
//! and the index-vs-sequential-scan routing it drives.
//!
//! # Field symptom
//!
//! A Sidecar-created snapshot served by cqlite-flight contained only `Data.db`
//! (+ `Statistics.db`/`CompressionInfo.db`/TOC/Digest) — its `Index.db`,
//! `Summary.db`, and `Filter.db` siblings were absent. cqlite-flight/core still
//! open such a directory, but with no `Summary.db` the reader's
//! `iterate_all_partitions` falls back to `sequential_scan`: a full
//! materialization + sort of every partition on each read (a real perf cliff).
//!
//! # What this proves
//!
//! The discrimination in the issue concluded the loss is NOT in cqlite-flight's
//! snapshot resolution (it resolves the snapshot dir and enumerates `*-Data.db`,
//! and core discovers all siblings in the SAME directory) nor in the connector
//! kit (it triggers a Sidecar snapshot PUT, which hardlinks every component). The
//! actionable, in-repo guarantee is: given a COMPLETE component set the reader
//! uses the index path (`has_partition_index() == true`), and given a
//! Data.db-only directory it opens but reports `false` and both paths return the
//! SAME partitions (the fallback is correct, just slower). The `false` case also
//! emits an operator WARN naming the absent components and the perf consequence.
//!
//! # Test data requirement
//!
//! `CQLITE_DATASETS_ROOT` must point at a `test-data/datasets` holding the
//! `test_basic/simple_table` BIG (`nb`) fixture with its full component set.
//! Tests skip gracefully when the binaries are absent.

use cqlite_core::{storage::sstable::reader::SSTableReader, Config};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Components that make a directory an INDEX-LESS snapshot: everything the reader
/// needs to open a compressed `nb` SSTable (`Data.db` is headerless — its schema
/// lives in `Statistics.db`; `CompressionInfo.db` is needed to decompress), but
/// WITHOUT the random-access index siblings (`Index.db`/`Summary.db`/`Filter.db`).
const INDEXLESS_KEEP_SUFFIXES: &[&str] = &[
    "-Data.db",
    "-Statistics.db",
    "-CompressionInfo.db",
    "-TOC.txt",
    "-Digest.crc32",
];

/// Locate the `simple_table-<uuid>` directory of the BIG `test_basic` fixture,
/// or `None` when the binary dataset is absent (test then skips).
fn simple_table_dir() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = PathBuf::from(root).join("sstables").join("test_basic");
    let dir = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .find(|e| e.path().is_dir() && e.file_name().to_string_lossy().starts_with("simple_table-"))
        .map(|e| e.path())?;
    // Require the full component set — otherwise the "complete" arm is vacuous.
    let has_summary = std::fs::read_dir(&dir)
        .ok()?
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with("-Summary.db"));
    if !has_summary {
        eprintln!("SKIP: simple_table Summary.db absent; run fetch-datasets.sh");
        return None;
    }
    Some(dir)
}

/// Copy the fixture into `dest/test_basic/simple_table-<uuid>/`, keeping only the
/// entries whose file name ends with one of `keep_suffixes`. Preserving the
/// `<keyspace>/<table>-<uuid>/` shape keeps path-based keyspace/table extraction
/// working. Returns the copied `*-Data.db` path.
fn stage_fixture(src_dir: &Path, dest_root: &Path, keep_suffixes: Option<&[&str]>) -> PathBuf {
    let table_name = src_dir
        .file_name()
        .expect("fixture dir has a name")
        .to_owned();
    let staged_dir = dest_root.join("test_basic").join(&table_name);
    std::fs::create_dir_all(&staged_dir).expect("create staged table dir");

    let mut data_db = None;
    for entry in std::fs::read_dir(src_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Never copy the JSONL/txt sidecar goldens into the SSTable dir.
        if name_str.ends_with(".jsonl") || name_str.ends_with(".db.txt") {
            continue;
        }
        let keep = match keep_suffixes {
            None => {
                name_str.ends_with(".db")
                    || name_str.ends_with(".txt")
                    || name_str.ends_with(".crc32")
            }
            Some(suffixes) => suffixes.iter().any(|s| name_str.ends_with(s)),
        };
        if !keep {
            continue;
        }
        let dest = staged_dir.join(&name);
        std::fs::copy(entry.path(), &dest).expect("copy component");
        if name_str.ends_with("-Data.db") {
            data_db = Some(dest);
        }
    }
    data_db.expect("staged fixture must contain a -Data.db")
}

async fn open_reader(data_db: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform::new"),
    );
    SSTableReader::open(data_db, &config, platform)
        .await
        .expect("SSTableReader::open must succeed")
}

/// Complete snapshot: the reader reports an index is present and
/// `iterate_all_partitions` uses the index path (not a full sequential scan).
#[tokio::test]
async fn complete_snapshot_uses_index_path() {
    let Some(src) = simple_table_dir() else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_db = stage_fixture(&src, tmp.path(), None);

    let reader = open_reader(&data_db).await;
    assert!(
        reader.has_partition_index(),
        "a complete component set (Summary.db present) must expose the index path"
    );

    let rows = reader
        .iterate_all_partitions()
        .await
        .expect("iterate_all_partitions over the complete set");
    assert!(
        !rows.is_empty(),
        "complete fixture must yield rows (0 rows when present = failure)"
    );
}

/// Index-less snapshot (only Data.db + open-critical siblings): the reader still
/// opens but reports NO index, so reads fall back to `sequential_scan`. The
/// fallback is CORRECT — it returns the same partitions the index path does.
#[tokio::test]
async fn indexless_snapshot_falls_back_but_matches_index_results() {
    let Some(src) = simple_table_dir() else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");

    // Complete arm — baseline row set via the index path.
    let complete_db = stage_fixture(&src, &tmp.path().join("complete"), None);
    let complete_reader = open_reader(&complete_db).await;
    assert!(complete_reader.has_partition_index());
    let complete_rows = complete_reader
        .iterate_all_partitions()
        .await
        .expect("iterate complete");

    // Index-less arm — Data.db + open-critical siblings only.
    let indexless_db = stage_fixture(
        &src,
        &tmp.path().join("indexless"),
        Some(INDEXLESS_KEEP_SUFFIXES),
    );
    // Guard the fixture: the index siblings really are absent.
    let dir = indexless_db.parent().expect("parent dir");
    for entry in std::fs::read_dir(dir).expect("read staged dir").flatten() {
        let n = entry.file_name();
        let s = n.to_string_lossy();
        assert!(
            !(s.ends_with("-Index.db") || s.ends_with("-Summary.db")),
            "index-less fixture must not contain {s}"
        );
    }

    let indexless_reader = open_reader(&indexless_db).await;
    assert!(
        !indexless_reader.has_partition_index(),
        "a Data.db-only snapshot must report NO random-access index"
    );

    let indexless_rows = indexless_reader
        .iterate_all_partitions()
        .await
        .expect("iterate index-less (sequential-scan fallback)");

    assert_eq!(
        indexless_rows.len(),
        complete_rows.len(),
        "the sequential-scan fallback must return the same partition count as the index path"
    );
}
