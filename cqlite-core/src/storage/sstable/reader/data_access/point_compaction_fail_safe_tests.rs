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
//!
//! ## MEDIUM — a chunk window truncated BEFORE `end` must not parse as `Rows`
//!
//! [`truncated_chunk_window_degrades_to_scan_fallback_not_partial_rows`] proves
//! the COMPRESSED counterpart (roborev job 1611, High): when
//! `pull_chunk_window` hits EOF (its chunk source exhausted) BEFORE the window
//! covers the full `[offset, end)`, the resulting partial buffer must NOT be
//! parsed with `at_final_chunk = true` — the compaction parser FLUSHES a
//! partially-decoded partition, so the code would surface short/wrong rows as
//! authoritative `Rows(...)` and hide the corruption. The fix tracks whether
//! the window reached `end`; an incomplete materialization degrades to
//! `IndexUnavailable` (scan fallback), NEVER `Rows`. The fixture copies the real
//! LZ4-compressed BTI `test_da/wide_table` (a head partition that spans ~38
//! 16 KiB chunks), then drops the trailing chunk offsets from its
//! `CompressionInfo.db` (reducing `chunk_count`) and truncates `Data.db` to the
//! new last-kept-chunk boundary — so the kept chunks stay CRC-valid while the
//! target partition's `[offset, end)` can no longer be fully materialized. With
//! the fix reverted the primitive returns `Rows(<300)` (a partial decode); with
//! it applied it returns `IndexUnavailable`.

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
use std::path::PathBuf;
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
        .read_single_partition_for_compaction(&key_bytes, Some(&schema), &ScanCancel::default())
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
        .read_single_partition_for_compaction(
            &target_key_bytes,
            Some(&schema),
            &ScanCancel::default(),
        )
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

// ---- HIGH (job 1616): BIG foreign-partition offset must not read as absence --

/// Write a 2-partition BIG (`nb`) SSTable, then redirect Index.db's on-disk
/// `data_offset` VInt for `pk=2` to point at `pk=1`'s (real, valid, DIFFERENT)
/// partition offset — a stale/corrupt exact-offset entry aimed at another
/// EXISTING partition, while pk=2's own row bytes stay intact and
/// scan-recoverable. Returns the temp dir (keep alive), the `Data.db` path, and
/// pk=2's raw key bytes.
///
/// Unlike the "implausibly large offset" fixture, the redirect lands on a
/// perfectly well-formed OTHER partition, so the seek DECODES SUCCESSFULLY and
/// finds only a FOREIGN key — the exact shape a naive authoritative-empty would
/// mistake for absence.
async fn foreign_partition_offset_fixture() -> (TempDir, std::path::PathBuf, Vec<u8>) {
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

    let key1 = WEPartitionKey::single("pk", Value::Integer(1))
        .to_bytes(&schema)
        .unwrap();
    let key2 = WEPartitionKey::single("pk", Value::Integer(2))
        .to_bytes(&schema)
        .unwrap();

    // Resolve BOTH partitions' real offsets from the INTACT index; pk=2's entry
    // will be redirected to pk=1's (valid, different) offset.
    let (off1, off2) = {
        let reader = open_reader(&data_path).await;
        let off1 = reader
            .lookup_partition_with_index(&key1)
            .await
            .unwrap()
            .expect("pk=1 must resolve via the intact Index.db")
            .0;
        let off2 = reader
            .lookup_partition_with_index(&key2)
            .await
            .unwrap()
            .expect("pk=2 must resolve via the intact Index.db")
            .0;
        (off1, off2)
    };
    assert_ne!(
        off1, off2,
        "the two partitions must sit at distinct offsets for a foreign redirect"
    );

    // Splice pk=2's `data_offset` VInt to encode pk=1's offset instead. pk=2 has
    // the higher token (written last) so no entry follows it — a length-changing
    // splice is safe.
    let mut index_bytes = std::fs::read(&index_path).unwrap();
    let mut prefix = (key2.len() as u16).to_be_bytes().to_vec();
    prefix.extend_from_slice(&key2);
    let prefix_start = index_bytes
        .windows(prefix.len())
        .position(|w| w == prefix.as_slice())
        .expect("pk=2's Index.db entry prefix must be found");
    let vint_start = prefix_start + prefix.len();

    let mut old_vint = Vec::new();
    encode_unsigned(off2, &mut old_vint);
    assert_eq!(
        &index_bytes[vint_start..vint_start + old_vint.len()],
        old_vint.as_slice(),
        "sanity: the located VInt must match pk=2's real offset"
    );
    let mut new_vint = Vec::new();
    encode_unsigned(off1, &mut new_vint);
    index_bytes.splice(
        vint_start..vint_start + old_vint.len(),
        new_vint.iter().copied(),
    );
    std::fs::write(&index_path, &index_bytes).unwrap();

    (temp, data_path, key2)
}

/// A BIG exact-offset lookup whose Index.db entry resolves a DIFFERENT valid
/// partition (stale/corrupt entry) must degrade to a scan fallback — the decode
/// lands on a FOREIGN key, and treating an empty match-set there as authoritative
/// absence would SILENTLY DROP the target key (roborev job 1616, High: fail-safe
/// violation). The BTI prefix-collision authoritative-empty rule does NOT apply
/// to a BIG exact offset. Proven at both layers: the primitive reports
/// `IndexUnavailable`, and the assembled point-read merger still finds pk=2's row
/// via scan fallback.
///
/// RED PROOF: with the src fix reverted the `saw_foreign_key && rows.is_empty()`
/// branch returns `Rows(Vec::new())` (a silent drop) and this test FAILS on the
/// `IndexUnavailable` assertion.
#[tokio::test]
async fn big_foreign_partition_offset_degrades_to_scan_fallback_not_empty_rows() {
    let (_temp, data_path, key2) = foreign_partition_offset_fixture().await;
    let schema = schema();

    // Layer 1: the primitive itself.
    let reader = open_reader(&data_path).await;
    let outcome = reader
        .read_single_partition_for_compaction(&key2, Some(&schema), &ScanCancel::default())
        .await
        .expect("a foreign-partition offset must degrade gracefully, never a hard Err");
    assert!(
        matches!(outcome, SinglePartitionCompaction::IndexUnavailable),
        "a BIG Index.db offset resolving a DIFFERENT valid partition must report \
         IndexUnavailable (scan-fallback), NEVER Rows(empty) (a silent drop of the \
         target key), got {outcome:?}"
    );

    // Layer 2: the assembled point-read merger still finds pk=2's intact row.
    let built =
        build_single_partition_merger(vec![data_path], &[key2], &schema, ScanCancel::default())
            .unwrap()
            .expect("pk=2's row is intact in Data.db and must be found via scan fallback");
    let rows = collect_partitions(built);
    assert_eq!(
        rows.len(),
        1,
        "exactly pk=2's partition must be returned, with its row intact"
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

// ---- MEDIUM: a chunk window truncated before `end` (compressed path) --------

/// Schema for the real `test_da/wide_table` fixture
/// (`pk int, ck int, payload text, PRIMARY KEY (pk, ck)`, LZ4). A valid schema
/// lets the compaction parser decode partial rows — so a reverted fix produces
/// `Rows(<300)` (the red proof), not a decode error.
fn wide_table_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_da".to_string(),
        table: "wide_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: crate::schema::ClusteringOrder::default(),
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
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

/// Locate the real `test_da/wide_table-*` SSTable directory (its `Data.db`)
/// under `CQLITE_DATASETS_ROOT`. Returns `None` (skip, never fail) when the
/// env var or the local fixture is absent — it is not in the published CI set.
fn wide_table_data_path() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)?;
    let ks_dir = root.join("sstables").join("test_da");
    for entry in std::fs::read_dir(&ks_dir).ok()?.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with("wide_table-")
        {
            continue;
        }
        for f in std::fs::read_dir(entry.path()).ok()?.flatten() {
            if f.file_name().to_string_lossy().ends_with("-Data.db") {
                return Some(f.path());
            }
        }
    }
    None
}

/// Copy every component of the SSTable directory holding `data_path` into a
/// fresh temp dir, returning the temp dir (keep alive) and the COPIED `Data.db`.
fn copy_sstable_dir(data_path: &std::path::Path) -> (TempDir, PathBuf) {
    let src_dir = data_path.parent().expect("Data.db has a parent dir");
    let temp = TempDir::new().unwrap();
    let mut copied_data = None;
    for f in std::fs::read_dir(src_dir).unwrap().flatten() {
        let name = f.file_name();
        let dst = temp.path().join(&name);
        std::fs::copy(f.path(), &dst).unwrap();
        if name.to_string_lossy().ends_with("-Data.db") {
            copied_data = Some(dst);
        }
    }
    (temp, copied_data.expect("copied a -Data.db"))
}

/// Sibling `*-CompressionInfo.db` for a copied `Data.db`.
fn sibling_compression_info(data_path: &std::path::Path) -> PathBuf {
    let name = data_path.file_name().unwrap().to_string_lossy();
    let base = name.strip_suffix("-Data.db").unwrap();
    data_path
        .parent()
        .unwrap()
        .join(format!("{base}-CompressionInfo.db"))
}

/// A COMPRESSED chunk window that cannot be fully materialized to `end` (EOF
/// before `end`, a truncated/corrupt SSTable) must degrade to
/// `IndexUnavailable` — NEVER a partial `Rows(...)` flushed by the compaction
/// parser at `at_final_chunk = true` (roborev job 1611, High).
///
/// Red proof: with the `!reached_end` guard reverted the primitive returns
/// `Rows(N)` with `N < 300` (a partial, wrong decode surfaced as authoritative);
/// with it applied it returns `IndexUnavailable`.
#[tokio::test]
async fn truncated_chunk_window_degrades_to_scan_fallback_not_partial_rows() {
    let Some(src_data) = wide_table_data_path() else {
        eprintln!("Skipping (truncated compressed window): test_da/wide_table fixture absent");
        return;
    };
    let (_temp, data_path) = copy_sstable_dir(&src_data);
    let schema = wide_table_schema();

    // Resolve the HEAD partition's offset + its authoritative `end` (the successor
    // partition's start) from the INTACT copy, using the SAME index the primitive
    // consults — so the truncation point is provably inside `[offset, end)`.
    let key1 = WEPartitionKey::single("pk", Value::Integer(1))
        .to_bytes(&schema)
        .unwrap();
    let (chunk_len, orig_count, target_off, end, ci_len, offsets) = {
        let reader = open_reader(&data_path).await;
        let ci = reader
            .compression_info
            .clone()
            .expect("wide_table is LZ4-compressed and must carry CompressionInfo");
        let target_off = reader
            .lookup_partition_via_bti_trie(&key1)
            .unwrap()
            .expect("pk=1 must resolve via the intact BTI trie");
        let end = reader
            .successor_partition_offset(target_off)
            .unwrap()
            .expect("pk=1 is a head partition and must have a successor bound");
        let ci_len = std::fs::metadata(sibling_compression_info(&data_path))
            .unwrap()
            .len() as usize;
        (
            ci.chunk_length as usize,
            ci.chunk_offsets.len(),
            target_off as usize,
            end as usize,
            ci_len,
            ci.chunk_offsets.clone(),
        )
    };
    assert_eq!(target_off, 0, "pk=1 must be the head partition (offset 0)");

    // Keep only the FIRST half of the chunks the partition spans, so the window
    // stops well before `end` (the tail chunks become unreachable → EOF).
    let end_chunk = end.div_ceil(chunk_len);
    let new_count = (end_chunk / 2).max(2);
    assert!(new_count < orig_count, "must drop at least one chunk");
    assert!(
        new_count * chunk_len < end,
        "the kept window ({} bytes) must stop strictly before end ({end})",
        new_count * chunk_len
    );

    // Truncate Data.db to the new last-kept-chunk boundary (kept chunks stay
    // CRC-valid) ...
    // `chunk_offsets: Vec<u64>` — pre-existing unrelated `unnecessary_cast`
    // clippy fix (file already touched by issue #2346), no behavior change.
    let cut = offsets[new_count];
    let data_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&data_path)
        .unwrap();
    data_file.set_len(cut).unwrap();

    // ... and rewrite CompressionInfo.db to advertise only `new_count` chunks
    // (drop the trailing 8-byte offset entries; `data_length` stays large). The
    // offset array is the last `orig_count * 8` bytes; `chunk_count` is the u32 BE
    // immediately before it.
    let ci_path = sibling_compression_info(&data_path);
    let ci_bytes = std::fs::read(&ci_path).unwrap();
    assert_eq!(ci_bytes.len(), ci_len);
    let offset_array_start = ci_bytes.len() - orig_count * 8;
    let count_field_pos = offset_array_start - 4;
    assert_eq!(
        u32::from_be_bytes(
            ci_bytes[count_field_pos..count_field_pos + 4]
                .try_into()
                .unwrap()
        ) as usize,
        orig_count,
        "sanity: located chunk_count field must match the parsed offset count"
    );
    let mut new_ci = Vec::with_capacity(count_field_pos + 4 + new_count * 8);
    new_ci.extend_from_slice(&ci_bytes[..count_field_pos]);
    new_ci.extend_from_slice(&(new_count as u32).to_be_bytes());
    new_ci.extend_from_slice(&ci_bytes[offset_array_start..offset_array_start + new_count * 8]);
    std::fs::write(&ci_path, &new_ci).unwrap();

    // Reopen on the truncated copy and probe: the window for `[0, end)` now hits
    // EOF (chunk `new_count` is gone) before reaching `end`.
    let reader = open_reader(&data_path).await;
    let outcome = reader
        .read_single_partition_for_compaction(&key1, Some(&schema), &ScanCancel::default())
        .await
        .expect("a truncated chunk window must degrade gracefully, never a hard Err");
    assert!(
        matches!(outcome, SinglePartitionCompaction::IndexUnavailable),
        "a chunk window that hit EOF before end must report IndexUnavailable \
         (scan-fallback), NEVER a partial Rows(...) flushed as authoritative, got {outcome:?}"
    );
}

// ---- Finding 2 (job 1620, MEDIUM): mid-seek cancellation ------------------
// ---- Finding 3 (job 1620, LOW): BTI miss is an authoritative prune --------

/// Write a clean 2-partition BTI (`da`) SSTable with an INTACT trie (companion
/// to `corrupt_bti_fixture`), returning the temp dir (keep alive) and the
/// `Data.db` path. A present key resolves its offset and reaches the seek; an
/// absent key is a genuine trie miss.
async fn valid_bti_fixture() -> (TempDir, std::path::PathBuf) {
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
    (temp, info.data_path)
}

/// A `scan_cancel` flag tripped BEFORE the seek runs aborts it with
/// `Error::Cancelled` for a PRESENT key — the primitive resolves the offset
/// (steps 1–4) then `seek_partition_compaction_rows`'s entry poll fires, so NO
/// partition is decoded and NO `Rows`/`IndexUnavailable`/`DefinitelyAbsent`
/// outcome is produced. FAILS on pre-fix code (Finding 2): without the seek-path
/// `scan_cancel` poll the seek ignores the token and returns `Ok(...)`.
#[tokio::test]
async fn pre_cancelled_seek_aborts_with_cancelled_not_rows() {
    let (_temp, data_path) = valid_bti_fixture().await;
    let reader = open_reader(&data_path).await;

    // Issue #2346: `scan_cancel` is now a PER-CALL parameter, not a field
    // mutated onto the reader.
    let cancel = ScanCancel::new();
    cancel.cancel();

    let schema = schema();
    let key_bytes = WEPartitionKey::single("pk", Value::Integer(1))
        .to_bytes(&schema)
        .unwrap();

    let result = reader
        .read_single_partition_for_compaction(&key_bytes, Some(&schema), &cancel)
        .await;
    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a pre-cancelled seek of a PRESENT key must abort with Error::Cancelled \
         (decode no rows), got {result:?}"
    );
}

/// A genuinely ABSENT key in a BTI SSTable classifies as `DefinitelyAbsent` —
/// the authoritative-by-construction trie-miss outcome — NEVER
/// `Rows(Vec::new())` (which the enum reserves for a FULLY-DECODED
/// prefix-collision) and never `IndexUnavailable`. Pins Finding 3's three-exit
/// invariant for the BTI-miss path: `bti_trie_resolve` returns `Ok(None)` ONLY
/// for a definitive miss (every degraded/unusable trie state returns `Err` →
/// `IndexUnavailable`), so a miss is a prune, not an empty decode. The reachable
/// path is step 1's presence-oracle prune (`might_contain_partition` funnels
/// through the SAME trie); the corrected step-3 `Ok(None)` arm is its defensive
/// equivalent and now returns the same `DefinitelyAbsent`, so both agree with
/// the contract this test pins.
#[tokio::test]
async fn bti_absent_key_is_definitely_absent_not_empty_rows() {
    let (_temp, data_path) = valid_bti_fixture().await;
    let reader = open_reader(&data_path).await;

    let schema = schema();
    // pk=999 was never written — a genuine trie miss.
    let key_bytes = WEPartitionKey::single("pk", Value::Integer(999))
        .to_bytes(&schema)
        .unwrap();

    let outcome = reader
        .read_single_partition_for_compaction(&key_bytes, Some(&schema), &ScanCancel::default())
        .await
        .expect("an absent-key probe must not error");
    assert!(
        matches!(outcome, SinglePartitionCompaction::DefinitelyAbsent),
        "an absent BTI key must classify as DefinitelyAbsent (authoritative prune), \
         never Rows(empty) or IndexUnavailable, got {outcome:?}"
    );
}
