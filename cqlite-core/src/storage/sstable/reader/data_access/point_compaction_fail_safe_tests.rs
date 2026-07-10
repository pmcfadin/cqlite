//! Fail-safe proofs for the single-partition point-read primitive when the
//! index component is UNREADABLE/corrupt, or resolves an offset the
//! materialized window never reaches (issue #2207, roborev IMPORTANT-1 +
//! MEDIUM on PR tip f75dccc2).
//!
//! `read_single_partition_for_compaction`'s spec requires: whenever the index
//! (BTI `Partitions.db` trie / BIG `Index.db`) is absent, unreadable,
//! ambiguous, or resolves an offset the seek cannot reach, the candidate
//! SSTable is READ (scan-fallback) — NEVER a hard-failed query the scan path
//! would still answer, and NEVER a silent empty treated as absence (the
//! false-negative-prune class the presence-oracle spine forbids).
//!
//! ## IMPORTANT-1 — corrupt BTI trie degrades gracefully, never `Err`
//!
//! [`corrupt_bti_trie_degrades_to_index_unavailable_not_err`] corrupts a BTI
//! `Partitions.db` footer AFTER a valid SSTable is written — so the reader
//! OPENS successfully (`has_partition_index()` stays `true`; the file is
//! present and non-empty) but the trie DESCENT for a real key fails to parse
//! — and proves the primitive degrades to
//! `SinglePartitionCompaction::IndexUnavailable` rather than propagating an
//! `Err`. Corruption technique: `lookup_partition_in_bti_slice` reads the
//! trie's `root_offset` from the LAST 8 bytes of `Partitions.db` and errors
//! when `root_offset >= trie_size` (`bti/parser/slice_walk.rs`); overwriting
//! those 8 bytes with `0xFF` guarantees that error deterministically.
//!
//! A full end-to-end "the assembled merger still returns the row via THIS
//! SSTable's scan fallback" proof for a genuinely-BTI-format input is NOT
//! included here: it surfaces a SEPARATE, PRE-EXISTING bug (present on `main`
//! with NO corruption at all) where `stream_all_partitions_for_compaction`
//! routes a `da`-format SSTable to `sequential_scan`'s non-stitching branch
//! (`requires_chunk_stitching()` is `is_nb_format`-gated and is `false` for
//! `da`), which mis-parses even a tiny, uncorrupted BTI partition
//! ("Incomplete value: need N bytes... have M") — the working BTI compaction
//! scan is `stitch_all_chunks` + the compaction parser (used directly by
//! `distinct_partition_keys`), not this path. This is orthogonal to #2207's
//! routing/ordering logic and is NOT masked or silently worked around here;
//! it should be filed as its own issue. It is not a wrong-rows risk (the
//! failure is a loud `Err`, identical to what a full-table scan of the same
//! SSTable already hits on `main` today), so IMPORTANT-1's actual requirement
//! — never propagate the INDEX read's own error as a hard failure — is fully
//! met and proven below for the reachable (BTI) case.
//!
//! ## MEDIUM — a resolved-but-unreachable offset must not read as absence
//!
//! [`stale_index_offset_degrades_to_scan_fallback_and_finds_the_row`] proves
//! the companion fix: `seek_partition_compaction_rows`'s
//! `within >= window.len()` branch (an offset the materialized window never
//! reached — a bad bound / truncated-SSTable shape) now returns `Ok(None)`
//! (→ `IndexUnavailable`), never `Ok(Some(Vec::new()))` (→ a false-negative
//! `Rows(empty)`, indistinguishable from a genuine, fully-decoded absence).
//! This uses a BIG-format fixture with an INTACT Data.db (the target row's
//! bytes are never touched) and directly patches Index.db's `data_offset`
//! VInt for the target key to an implausibly large value — so the seek
//! resolves an offset the real (complete, uncorrupted) file cannot satisfy,
//! and the assembled point-read merger must still find the row via scan
//! fallback.

use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::serialization::vint::encode_unsigned;
use crate::storage::sstable::reader::{SSTableReader, SinglePartitionCompaction};
use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use crate::storage::write_engine::merge::MergeStep;
use crate::storage::write_engine::mutation::{CellOperation, Mutation, TableId};
use crate::storage::write_engine::{
    build_single_partition_merger, KWayMerger, PartitionKey as WEPartitionKey,
};
use crate::types::Value;
use crate::{Config, Platform};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "test_ks";
const TBL: &str = "fs_tbl";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".to_string(),
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

fn mutation(pk: i32, v: &str) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        WEPartitionKey::single("pk", Value::Integer(pk)),
        None,
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::Text(v.to_string()),
        }],
        1_000_000 + pk as i64,
        None,
    )
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

// ---- IMPORTANT-1: corrupt BTI trie ----------------------------------------

/// Write a 2-partition BTI (`da`) SSTable (pk=1 -> "one", pk=2 -> "two"), then
/// overwrite its sibling `Partitions.db` footer's `root_offset` with an
/// out-of-range value so a real trie descent errors deterministically while the
/// file itself stays present/non-empty (the reader still OPENS successfully).
async fn corrupt_bti_fixture() -> (TempDir, std::path::PathBuf) {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let mut writer = SSTableWriter::with_format(
        temp.path().to_path_buf(),
        1,
        &schema,
        16,
        SSTableFormat::Bti,
    )
    .unwrap();

    let mut keyed: Vec<_> = [(1, "one"), (2, "two")]
        .into_iter()
        .map(|(pk, v)| {
            let m = mutation(pk, v);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();
    let data_path = info.data_path.clone();

    // The writer reports the sibling Partitions.db path directly (`SSTableInfo`)
    // — no filename-scheme guessing needed. Stomp its LAST 8 bytes (the
    // root_offset footer) so `root_offset >= trie_size` unconditionally.
    let partitions_path = info
        .partitions_path
        .clone()
        .expect("writer must emit a sibling Partitions.db for a BTI SSTable");

    let mut bytes = std::fs::read(&partitions_path).unwrap();
    assert!(
        bytes.len() >= 8,
        "Partitions.db must carry at least the 8-byte footer"
    );
    let len = bytes.len();
    bytes[len - 8..].fill(0xFF);
    std::fs::write(&partitions_path, &bytes).unwrap();

    (temp, data_path)
}

/// The core primitive degrades a corrupt-trie descent to `IndexUnavailable`,
/// never an `Err` — the fail-safe spine (roborev IMPORTANT-1).
#[tokio::test]
async fn corrupt_bti_trie_degrades_to_index_unavailable_not_err() {
    let (_temp, data_path) = corrupt_bti_fixture().await;
    let reader = open_reader(&data_path).await;
    assert!(
        reader.has_partition_index(),
        "the corrupted-but-present Partitions.db must still report an index (open succeeded)"
    );

    let schema = schema();
    let key_bytes = WEPartitionKey::single("pk", Value::Integer(1))
        .to_bytes(&schema)
        .unwrap();

    let outcome = reader
        .read_single_partition_for_compaction(&key_bytes, Some(&schema))
        .await
        .expect("a corrupt trie must degrade gracefully, never a hard Err");
    assert!(
        matches!(outcome, SinglePartitionCompaction::IndexUnavailable),
        "a corrupt BTI trie descent must report IndexUnavailable (scan-fallback signal), got {outcome:?}"
    );
}

// ---- MEDIUM: offset beyond the materialized window -----------------------

/// Write a 2-partition BIG (`nb`) SSTable, then patch Index.db's on-disk
/// `data_offset` VInt for `pk=2` to an implausibly large value (well beyond
/// the real, INTACT Data.db's length) — a stale/corrupt index entry pointing
/// nowhere real, while `pk=2`'s actual row bytes are untouched and still fully
/// recoverable by a scan. Returns the temp dir (keep alive), the `Data.db`
/// path, and `pk=2`'s raw key bytes.
async fn stale_offset_fixture() -> (TempDir, std::path::PathBuf, Vec<u8>) {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).unwrap();

    let mut keyed: Vec<_> = [(1, "one"), (2, "two")]
        .into_iter()
        .map(|(pk, v)| {
            let m = mutation(pk, v);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();
    let data_path = info.data_path.clone();
    let index_path = info
        .index_path
        .clone()
        .expect("writer must emit a sibling Index.db for the BIG format");

    let target_key_bytes = WEPartitionKey::single("pk", Value::Integer(2))
        .to_bytes(&schema)
        .unwrap();

    // Confirm the REAL (legitimate) resolved offset and the true data-section
    // length via an uncorrupted reader, so the replacement value is provably
    // beyond the actual file (not a guess).
    let real_data_len = {
        let reader = open_reader(&data_path).await;
        let (real_offset, _) = reader
            .lookup_partition_with_index(&target_key_bytes)
            .await
            .unwrap()
            .expect("pk=2 must resolve via the intact Index.db before corruption");
        let header_size = reader.calculate_header_size() as u64;
        let file_len = std::fs::metadata(&data_path).unwrap().len();
        let data_len = file_len.saturating_sub(header_size);
        assert!(
            real_offset < data_len,
            "sanity: the real offset must be within the real data section"
        );
        data_len
    };

    // Locate the Index.db entry's `[key_len: u16 BE][raw key bytes]` prefix
    // (`index_writer.rs::write_entry`'s documented on-disk framing) and splice
    // in a replacement `data_offset` VInt encoding a value STRICTLY beyond
    // `real_data_len` — pk=2 is written last (highest token among the two), so
    // no entry follows it in the file; a length-changing splice is safe.
    let mut index_bytes = std::fs::read(&index_path).unwrap();
    let mut prefix = (target_key_bytes.len() as u16).to_be_bytes().to_vec();
    prefix.extend_from_slice(&target_key_bytes);
    let prefix_start = index_bytes
        .windows(prefix.len())
        .position(|w| w == prefix.as_slice())
        .expect("pk=2's Index.db entry prefix must be found");
    let vint_start = prefix_start + prefix.len();

    // Determine the OLD VInt's byte length by re-encoding the value we already
    // confirmed above, so we splice exactly the old VInt's span.
    let real_offset = {
        let reader = open_reader(&data_path).await;
        reader
            .lookup_partition_with_index(&target_key_bytes)
            .await
            .unwrap()
            .unwrap()
            .0
    };
    let mut old_vint = Vec::new();
    encode_unsigned(real_offset, &mut old_vint);
    assert_eq!(
        &index_bytes[vint_start..vint_start + old_vint.len()],
        old_vint.as_slice(),
        "sanity: the byte-search-located VInt must match the resolved real offset"
    );

    let corrupted_offset = real_data_len + 10_000;
    let mut new_vint = Vec::new();
    encode_unsigned(corrupted_offset, &mut new_vint);
    index_bytes.splice(
        vint_start..vint_start + old_vint.len(),
        new_vint.iter().copied(),
    );
    std::fs::write(&index_path, &index_bytes).unwrap();

    (temp, data_path, target_key_bytes)
}

/// A resolved-but-unreachable offset (stale/corrupt Index.db entry, INTACT
/// Data.db) must degrade to a scan fallback — NEVER a silent `Rows(empty)`
/// treated as absence (roborev MEDIUM). Proven at both layers: the core
/// primitive reports `IndexUnavailable`, and the assembled point-read merger
/// still finds the row via that SSTable's scan.
#[tokio::test]
async fn stale_index_offset_degrades_to_scan_fallback_and_finds_the_row() {
    let (_temp, data_path, target_key_bytes) = stale_offset_fixture().await;
    let schema = schema();

    // Layer 1: the primitive itself.
    let reader = open_reader(&data_path).await;
    let outcome = reader
        .read_single_partition_for_compaction(&target_key_bytes, Some(&schema))
        .await
        .expect("a stale offset must degrade gracefully, never a hard Err");
    assert!(
        matches!(outcome, SinglePartitionCompaction::IndexUnavailable),
        "an offset beyond the materialized window must report IndexUnavailable \
         (scan-fallback signal), NEVER Rows(empty) (false-negative prune), got {outcome:?}"
    );

    // Layer 2: the assembled point-read merger still finds the row.
    let built = build_single_partition_merger(
        vec![data_path],
        &[target_key_bytes],
        &schema,
        ScanCancel::default(),
    )
    .unwrap()
    .expect("pk=2's row is intact in Data.db and must be found via scan fallback");

    let rows = collect_partitions(built);
    assert_eq!(
        rows.len(),
        1,
        "exactly the one target partition must be returned, with its row intact"
    );
    assert_eq!(rows[0], 1, "pk=2's single live row must be present");
}

/// Drain a merger to completion, returning every partition's row count.
fn collect_partitions(mut merger: KWayMerger) -> Vec<usize> {
    let mut out = Vec::new();
    while let MergeStep::Partition { rows, .. } = merger.step().expect("step") {
        out.push(rows.len());
    }
    out
}
