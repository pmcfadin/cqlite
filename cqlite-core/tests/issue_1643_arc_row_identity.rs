//! Issue #1643 (K4): stop cloning the partition key + `TableId` per row.
//!
//! Before this change the partition identity was re-materialised per ROW — the
//! `RowKey` (`Vec<u8>`) and the `TableId` (`String`) were deep-cloned at ~13
//! per-row emit sites, so a 10k-row partition allocated its key 10k times.
//!
//! The fix makes `RowKey` an `Arc<[u8]>` and `TableId` an `Arc<str>`: the
//! identity is materialised ONCE when the partition header is parsed, and every
//! row of that partition holds a pointer-clone (an `Arc` strong-count bump, no
//! byte allocation). This is a pure ownership/allocation change — the key bytes,
//! the emitted values, and the comparison order are byte-identical to before.
//!
//! This test proves BOTH halves against a real multi-row-partition SSTable
//! (`test_timeseries/sensor_data`, 2000 rows across 10 partitions, up to 220
//! rows in one partition):
//!
//! 1. **Parity** — the rows read back are non-empty and every partition key is a
//!    valid 16-byte UUID (the on-disk bytes), so the read path is exercised for
//!    real, not vacuously.
//! 2. **Allocation reduction** — for every partition with N>1 rows, all N
//!    `RowKey`s share ONE backing buffer: identical `buffer_ptr()` AND the shared
//!    `Arc` strong-count is >= N. Pre-change (a per-row `Vec<u8>` clone) each row
//!    owned a DISTINCT buffer, so the pointers would differ and a strong-count
//!    concept would not exist. Likewise all `TableId`s share one `Arc<str>`.

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

fn sensor_data_path() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let path = PathBuf::from(root).join(
        "sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );
    path.exists().then_some(path)
}

#[tokio::test]
async fn partition_key_and_table_id_shared_per_partition_not_cloned_per_row() {
    let Some(path) = sensor_data_path() else {
        // Local-only fixture (gitignored Data.db). Skip on ABSENCE only; when
        // present a 0-row result below is a hard failure (never vacuous).
        eprintln!("sensor_data Data.db absent; set CQLITE_DATASETS_ROOT — skipping");
        return;
    };

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init must succeed"),
    );
    let reader = SSTableReader::open(&path, &config, platform)
        .await
        .expect("open sensor_data SSTable");

    let entries = reader
        .get_all_entries()
        .await
        .expect("get_all_entries must succeed");

    // -- Non-vacuous: the fixture has 2000 rows across 10 partitions. --
    assert!(
        entries.len() >= 1000,
        "expected the multi-row sensor_data fixture (~2000 rows), got {} — \
         the read path was not exercised",
        entries.len()
    );

    // Group rows by their partition-key bytes, keeping ONE representative RowKey
    // per group plus the count. Two RowKeys are the "same partition" iff bytes
    // are equal.
    let mut by_partition: HashMap<Vec<u8>, (cqlite_core::RowKey, usize)> = HashMap::new();
    let mut table_ids: Vec<cqlite_core::TableId> = Vec::new();
    for (tid, key, _row) in &entries {
        // Parity: every partition key is a 16-byte UUID (the raw on-disk bytes).
        assert_eq!(
            key.len(),
            16,
            "sensor_data partition key must be a 16-byte UUID; got {} bytes",
            key.len()
        );
        let e = by_partition
            .entry(key.as_bytes().to_vec())
            .or_insert_with(|| (key.clone(), 0));
        e.1 += 1;
        table_ids.push(tid.clone());
    }

    // The fixture partitions the 2000 rows across ~10 UUID partitions, several
    // with hundreds of rows. Require at least one genuinely multi-row partition,
    // else the sharing assertion below would be vacuous.
    let multi_row_partitions = by_partition.values().filter(|(_, n)| *n > 1).count();
    assert!(
        multi_row_partitions >= 1,
        "expected >=1 multi-row partition to prove per-partition key sharing; \
         found none across {} partitions",
        by_partition.len()
    );

    // -- Allocation reduction, per partition. --
    //
    // Every row of a partition was emitted by pointer-cloning the ONE partition
    // key `Arc` created when the header was parsed. So a second `RowKey` cloned
    // from the same partition must point at the SAME buffer AND the shared
    // strong-count must be >= that partition's row count. We prove the pointer
    // identity by re-cloning a row's key and comparing buffer pointers to the
    // representative we stored: `Arc::clone` never copies the bytes.
    let mut checked_multi = 0usize;
    for (_tid, key, _row) in &entries {
        let (rep_key, count) = by_partition
            .get(key.as_bytes())
            .expect("every row's partition is grouped");
        if *count <= 1 {
            continue;
        }
        // Same partition => same backing buffer pointer (pointer-share, not a
        // per-row byte copy). A pre-#1643 `Vec<u8>` clone would give each row a
        // DISTINCT allocation and thus a different pointer here.
        assert_eq!(
            key.buffer_ptr(),
            rep_key.buffer_ptr(),
            "rows of the same partition must share ONE key buffer (pointer bump), \
             not a per-row allocation"
        );
        checked_multi += 1;
    }
    assert!(
        checked_multi > 1,
        "expected to verify pointer-sharing across multiple rows of a partition, \
         only checked {checked_multi}"
    );

    // The shared strong-count reflects that N rows hold N pointer-clones of ONE
    // Arc (plus the representative + the outer entries Vec): it must exceed 1 for
    // the largest multi-row partition — impossible under per-row deep clones.
    let (biggest_bytes, (_, biggest_count)) = by_partition
        .iter()
        .max_by_key(|(_, (_, n))| *n)
        .expect("at least one partition");
    // Find any live RowKey of the biggest partition still owned by `entries`.
    let live_key = entries
        .iter()
        .find(|(_, k, _)| k.as_bytes() == biggest_bytes.as_slice())
        .map(|(_, k, _)| k)
        .expect("biggest partition has a live row");
    assert!(
        live_key.buffer_strong_count() >= *biggest_count,
        "the biggest partition has {biggest_count} rows; its shared key Arc \
         strong-count is {}, which must be >= the row count (one Arc, N clones)",
        live_key.buffer_strong_count()
    );

    // -- TableId identity is likewise shared (one Arc<str> per parse). --
    assert!(!table_ids.is_empty(), "table ids collected");
    let first_ptr = table_ids[0].name().as_ptr();
    let shared_table_id = table_ids
        .iter()
        .filter(|t| t.name().as_ptr() == first_ptr)
        .count();
    assert!(
        shared_table_id > 1,
        "the TableId must be shared across rows (Arc<str> pointer bump); only {} \
         of {} rows shared the same buffer",
        shared_table_id,
        table_ids.len()
    );

    // Parity: the shared table id names the fixture's keyspace.table.
    assert!(
        table_ids[0].name().ends_with("sensor_data"),
        "table id should name the sensor_data table; got {}",
        table_ids[0].name()
    );
}
