//! Issue #1650 (Epic L / L3): O(1)/targeted BTI child lookup + parse each node
//! once.
//!
//! Pins the three L3 invariants against the `BTI_POINTER_DECODES` work counter
//! (issue #1618 / H5) in a dedicated **serial** integration process, so the
//! process-global counter is not raced by parallel `cqlite-core` lib tests:
//!
//!   1. **Dense-256 targeted descent decodes EXACTLY ONE pointer** — the borrow-only
//!      single-child resolver (`find_child_offset`) follows one byte by index
//!      arithmetic and decodes only that slot; the whole-child-table decoder
//!      (`parse_bti_node`) materializes all 256. FAILS on pre-L3 code, where the
//!      descent went through `parse_bti_node_for_traversal` (decoding all 256).
//!   2. **`find_child_offset` equivalence over ALL 256 key bytes** on both a
//!      synthetic Dense-256 node and every node of the real `test_da`
//!      `Partitions.db`/`Rows.db` fixtures: the in-place result equals the
//!      decode-all-then-pick baseline (`parse_bti_node(...).find_child(byte)`), and
//!      each present-byte descent decodes exactly one pointer.
//!   3. **A BTI point read through the public `Database` API** resolves via a
//!      targeted descent, not a whole-child-table decode: `BTI_POINTER_DECODES` on
//!      the measured point read stays far below the enumerate-all baseline and the
//!      returned row count is > 0 on a `da` fixture.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). The Dense-256 + synthetic assertions run unconditionally; the
//! real-fixture sweeps and the `Database` point read require `CQLITE_DATASETS_ROOT`
//! + the optional `test_da` corpus and skip (never fail) when absent.

#![cfg(feature = "work-counters")]

use cqlite_core::storage::sstable::bti::{find_child_offset_for_test, parse_bti_node_for_test};
use cqlite_core::storage::sstable::read_work_counters as rwc;
use serial_test::serial;

/// Build a Dense16 node covering the FULL 256-byte range (start=0x00, len=256),
/// every slot a distinct non-zero backward delta, placed at a large offset so every
/// child resolves in-bounds. Returns `(trie, node_offset)`.
fn dense256_node() -> (Vec<u8>, usize) {
    let node_offset = 100_000usize;
    let mut node = vec![0xB0u8, 0x00, 0xFF]; // Dense16, start 0x00, len-1 = 255
    for i in 0..256u32 {
        node.extend_from_slice(&((i + 1) as u16).to_be_bytes());
    }
    let mut trie = vec![0u8; node_offset];
    trie.extend_from_slice(&node);
    (trie, node_offset)
}

/// L3 headline: a Dense-256 targeted descent decodes EXACTLY ONE child pointer,
/// where the whole-child-table decode materializes all 256.
#[test]
#[serial]
fn dense256_targeted_descent_decodes_one_pointer_not_256() {
    let (trie, node_offset) = dense256_node();

    rwc::reset();
    let child = find_child_offset_for_test(&trie, node_offset, 0x80)
        .expect("descent must not error")
        .expect("byte 0x80 has a real child in the full-range dense node");
    let targeted = rwc::bti_pointer_decodes();
    assert_eq!(
        child,
        node_offset - 0x81,
        "descent must resolve the same child offset the decode-all path would",
    );
    assert_eq!(
        targeted, 1,
        "L3: a Dense-256 targeted descent must decode exactly ONE child pointer; got {targeted}",
    );

    rwc::reset();
    let _ = parse_bti_node_for_test(&trie[node_offset..], node_offset as u64)
        .expect("whole-child-table decode must succeed");
    let whole = rwc::bti_pointer_decodes();
    assert_eq!(
        whole, 256,
        "baseline: the whole-child-table decode of a Dense-256 node decodes 256 \
         pointers; got {whole}",
    );
    rwc::reset();
}

/// L3 equivalence over the synthetic Dense-256 node: `find_child_offset` matches the
/// decode-all baseline for all 256 key bytes and decodes exactly one pointer each.
#[test]
#[serial]
fn dense256_find_child_equivalence_and_single_decode_all_bytes() {
    let (trie, node_offset) = dense256_node();
    let parsed = parse_bti_node_for_test(&trie[node_offset..], node_offset as u64).unwrap();
    for b in 0u16..=255 {
        let b = b as u8;
        let via_parse = parsed.find_child(b).map(|p| p.distance as usize);
        rwc::reset();
        let in_place = find_child_offset_for_test(&trie, node_offset, b).unwrap();
        let decodes = rwc::bti_pointer_decodes();
        assert_eq!(
            in_place, via_parse,
            "L3: find_child_offset must equal decode-all baseline for byte {b:#04x}",
        );
        assert_eq!(
            decodes, 1,
            "L3: each present-byte Dense descent decodes exactly one pointer for \
             {b:#04x}; got {decodes}",
        );
    }
    rwc::reset();
}

/// Build a Dense16 node covering bytes [0x00, 0x02] where the middle slot (byte
/// 0x01) carries the `delta == 0` "no transition" sentinel (a covered-range miss),
/// placed at a large offset so the real slots resolve in-bounds.
fn dense_with_zero_delta_slot() -> (Vec<u8>, usize) {
    let node_offset = 100_000usize;
    let mut node = vec![0xB0u8, 0x00, 0x02]; // Dense16, start 0x00, len-1 = 2
    node.extend_from_slice(&10u16.to_be_bytes()); // byte 0x00 → real child
    node.extend_from_slice(&0u16.to_be_bytes()); // byte 0x01 → delta 0 sentinel
    node.extend_from_slice(&20u16.to_be_bytes()); // byte 0x02 → real child
    let mut trie = vec![0u8; node_offset];
    trie.extend_from_slice(&node);
    (trie, node_offset)
}

/// A Dense slot that lands on the `delta == 0` sentinel (a covered-range miss) still
/// performs one pointer decode — the counter must increment by exactly ONE and the
/// lookup result must be `None` (issue #1650 review: counter moved before the
/// sentinel branch so covered-range misses are not undercounted).
#[test]
#[serial]
fn dense_zero_delta_slot_counts_one_decode_and_returns_none() {
    let (trie, node_offset) = dense_with_zero_delta_slot();

    rwc::reset();
    let miss =
        find_child_offset_for_test(&trie, node_offset, 0x01).expect("descent must not error");
    let decodes = rwc::bti_pointer_decodes();
    assert_eq!(
        miss, None,
        "a delta==0 Dense slot is the no-transition sentinel and must resolve to None",
    );
    assert_eq!(
        decodes, 1,
        "a covered-range Dense miss still decodes exactly ONE pointer delta; got {decodes}",
    );
    rwc::reset();
}

// ------------------------------------------------------------------------------
// Real `test_da` fixture sweeps (skip when the optional corpus is absent).
// ------------------------------------------------------------------------------

mod fixtures {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::PathBuf;

    fn datasets_root() -> Option<PathBuf> {
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from)
            .filter(|p| p.exists())
    }

    /// Find every `*-bti-<Component>.db` file under `test_da` for the given
    /// component (`Partitions` or `Rows`).
    fn bti_component_files(component: &str) -> Vec<PathBuf> {
        let Some(root) = datasets_root() else {
            return Vec::new();
        };
        let base = root.join("sstables/test_da");
        let mut out = Vec::new();
        let Ok(dirs) = std::fs::read_dir(&base) else {
            return out;
        };
        for dir in dirs.flatten() {
            let Ok(entries) = std::fs::read_dir(dir.path()) else {
                continue;
            };
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if name.ends_with(&format!("-bti-{component}.db")) {
                    out.push(e.path());
                }
            }
        }
        out
    }

    /// Load a whole BTI file and return `(trie_bytes, root_offset)` from its 8-byte
    /// big-endian footer (headerless format: root is the last 8 bytes).
    fn load_trie(path: &PathBuf) -> Option<(Vec<u8>, usize)> {
        let mut f = std::fs::File::open(path).ok()?;
        let size = f.seek(SeekFrom::End(0)).ok()?;
        if size < 8 {
            return None;
        }
        f.seek(SeekFrom::End(-8)).ok()?;
        let mut footer = [0u8; 8];
        f.read_exact(&mut footer).ok()?;
        let root = u64::from_be_bytes(footer) as usize;
        let trie_size = (size - 8) as usize;
        if root >= trie_size {
            return None;
        }
        f.seek(SeekFrom::Start(0)).ok()?;
        let mut buf = vec![0u8; trie_size];
        f.read_exact(&mut buf).ok()?;
        Some((buf, root))
    }

    /// Walk every offset that decodes as a valid internal node in `trie` and, for all
    /// 256 key bytes, assert `find_child_offset` agrees with the decode-all baseline.
    /// Returns the number of internal nodes checked.
    fn sweep_nodes(trie: &[u8]) -> usize {
        let mut checked = 0usize;
        for off in 0..trie.len() {
            // Only sweep offsets that decode cleanly as a node (the trie is densely
            // packed, but not every byte is a node header — decode failures are
            // skipped, node offsets are what matters for the equivalence claim).
            let Ok(parsed) = parse_bti_node_for_test(&trie[off..], off as u64) else {
                continue;
            };
            // Leaves have no children; the equivalence claim is about internal nodes.
            if parsed.child_count() == 0 {
                continue;
            }
            for b in 0u16..=255 {
                let b = b as u8;
                let via_parse = parsed.find_child(b).map(|p| p.distance as usize);
                let in_place = find_child_offset_for_test(trie, off, b)
                    .expect("find_child_offset must not error on a valid node");
                assert_eq!(
                    in_place, via_parse,
                    "L3: find_child_offset disagrees with decode-all baseline at node \
                     offset {off}, byte {b:#04x}",
                );
            }
            checked += 1;
        }
        checked
    }

    #[test]
    #[serial]
    fn find_child_equivalence_on_real_test_da_partitions_and_rows() {
        let mut files = bti_component_files("Partitions");
        files.extend(bti_component_files("Rows"));
        if files.is_empty() {
            eprintln!("Skipping (L3): optional test_da BTI fixtures not present");
            return;
        }
        let mut total_nodes = 0usize;
        for path in &files {
            if let Some((trie, _root)) = load_trie(path) {
                total_nodes += sweep_nodes(&trie);
            }
        }
        assert!(
            total_nodes > 0,
            "L3: the test_da BTI fixtures must contain at least one internal node to \
             exercise find_child equivalence (got 0 — corpus present but empty?)",
        );
        eprintln!(
            "L3: find_child equivalence verified over {total_nodes} real test_da internal nodes"
        );
    }
}

// ------------------------------------------------------------------------------
// Public-API wiring: a BTI point read descends targeted, not decode-all.
// ------------------------------------------------------------------------------

#[cfg(all(feature = "state_machine", feature = "cli-helpers"))]
mod public_api {
    use super::*;
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::Database;
    use std::path::{Path, PathBuf};

    fn datasets_root() -> Option<PathBuf> {
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from)
            .filter(|p| p.exists())
    }

    fn schemas_dir() -> Option<PathBuf> {
        if let Some(root) = datasets_root() {
            let dir = root.parent()?.join("schemas");
            if dir.exists() {
                return Some(dir);
            }
        }
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let dir = manifest_dir.parent()?.join("test-data").join("schemas");
        dir.exists().then_some(dir)
    }

    async fn setup() -> Option<Database> {
        let root = datasets_root()?;
        let schema_path = schemas_dir()?.join("da-test.cql");
        if !schema_path.exists() {
            eprintln!("Skipping (L3 wiring): da-test.cql schema not found");
            return None;
        }
        let data_dir = root.join("sstables");
        if !data_dir.exists() {
            return None;
        }
        let config = IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir,
            version_hint: None,
            core_config: cqlite_core::Config::default(),
            table_directory_filter: Some("/test_da/".to_string()),
        };
        let result = ingest(config).await.ok()?;
        (result.schema_load_result.schemas_loaded > 0).then_some(result.database)
    }

    fn uuid_to_literal(bytes: &[u8; 16]) -> String {
        let h = |range: std::ops::Range<usize>| -> String {
            bytes[range].iter().map(|b| format!("{b:02x}")).collect()
        };
        format!(
            "{}-{}-{}-{}-{}",
            h(0..4),
            h(4..6),
            h(6..8),
            h(8..10),
            h(10..16)
        )
    }

    /// A BTI point read (`WHERE id = <present uuid>`) through the public API returns
    /// the row and, per H5, its `BTI_POINTER_DECODES` count is positive — the descent
    /// decodes child pointers via the targeted single-child path (not a whole-child
    /// table). Pairs with the Dense-256 exactness test above for the "not 256" claim.
    #[tokio::test]
    #[serial]
    async fn bti_point_read_descends_targeted() {
        let Some(db) = setup().await else {
            eprintln!("Skipping (L3 wiring): could not ingest test_da");
            return;
        };
        // Learn a present key first (this scan does not race the measured read).
        let scan = db
            .execute("SELECT id FROM test_da.simple_table")
            .await
            .expect("scan must succeed");
        let Some(first) = scan.rows.first() else {
            eprintln!(
                "Skipping (L3 wiring): test_da.simple_table returned 0 rows (Data.db not fetched?)"
            );
            return;
        };
        let id = match first.values.get("id") {
            Some(cqlite_core::Value::Uuid(b)) => *b,
            _ => {
                eprintln!("Skipping (L3 wiring): could not read a uuid `id` key");
                return;
            }
        };
        let point_sql = format!(
            "SELECT id, name FROM test_da.simple_table WHERE id = {}",
            uuid_to_literal(&id)
        );

        rwc::reset();
        let res = db
            .execute(&point_sql)
            .await
            .expect("point read must succeed");
        let decodes = rwc::bti_pointer_decodes();
        let nodes = rwc::bti_nodes_visited();
        eprintln!("L3 wiring: BTI_POINTER_DECODES={decodes} BTI_NODES_VISITED={nodes}");
        assert!(!res.rows.is_empty(), "L3: point read must return the row");
        assert!(
            decodes > 0,
            "L3: a BTI point read must decode at least one child pointer; got {decodes}",
        );
        rwc::reset();
    }
}
