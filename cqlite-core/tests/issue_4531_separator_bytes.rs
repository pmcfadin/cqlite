//! Issue #4531 — BYTE-LEVEL oracle for the uuid/timeuuid clustering encoding.
//!
//! The Cassandra-written `Rows.db` row-index trie of the committed fixture
//! (`test-data/fixtures/issue_4531`, cassandra-5.0.8, BTI) stores one SEPARATOR per block
//! boundary. Cassandra builds each as `ByteSource.separatorGt(prevBlockLastKey, blockFirstKey)`
//! (`RowIndexWriter`): the common prefix of the two keys plus one byte (prev byte + 1), in
//! `ClusteringComparator.asByteComparable` form (`40 <component bytes>`).
//!
//! So the property, with Cassandra's bytes as the oracle (not a CQLite round trip):
//!
//! 1. every non-empty separator the REAL `Rows.db` holds is a byte-prefix of
//!    `encode(key)` for some real row `key` of the partition (CQLite's own encoding of the
//!    golden's keys), and sorts strictly above the preceding key's encoding;
//! 2. CQLite's encoding of the partition's keys, taken in the golden's (Cassandra's) clustering
//!    order, is strictly ascending bytewise.
//!
//! With the pre-fix RAW 16-byte encoding both fail on a v1/timeuuid fixture: raw prefixes are
//! not the swizzled prefixes Cassandra stored.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "lz4",
    not(feature = "tombstones")
))]

use std::io::Cursor;
use std::path::PathBuf;

use cqlite_core::storage::sstable::bti::{
    encode_clustering_bound_oss50_typed, iterate_rows_in_bti_trie,
    lookup_raw_key_in_bti_partitions_db, resolve_rows_db_entry, BtiPartitionLocation,
};
use cqlite_core::types::Value;

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_uuid_bti";
/// `(table, clustering DESC, clustering is timeuuid)`.
const TABLES: [(&str, bool, bool); 5] = [
    ("timeuuid_clustering", false, true),
    ("timeuuid_clustering_desc", true, true),
    ("uuid_clustering", false, false),
    // TIE tables: v1 timeuuids sharing a timestamp, differing only in the lsb (0x00..0xFF), so
    // the separators are cut INSIDE the lsb and pin the signed-lsb XOR (CASSANDRA-8730).
    ("timeuuid_ties", false, true),
    ("timeuuid_ties_desc", true, true),
];

fn component(table: &str, suffix: &str) -> Vec<u8> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4531");
    let dirs = datasets_root::table_generation_dirs(&root, KEYSPACE, table);
    assert_eq!(dirs.len(), 1, "{table}: one generation dir");
    let path = std::fs::read_dir(&dirs[0])
        .expect("fixture dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(suffix))
        })
        .unwrap_or_else(|| panic!("{table}: no *{suffix}"));
    let b = std::fs::read(&path).expect("read component");
    assert!(!b.is_empty(), "{path:?} is EMPTY");
    b
}

fn parse_uuid(s: &str) -> [u8; 16] {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    assert_eq!(hex.len(), 32, "uuid {s}");
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex");
    }
    out
}

/// pk=1 clustering keys in the golden's (= Cassandra's) clustering order.
fn golden_keys(table: &str) -> Vec<[u8; 16]> {
    let text = String::from_utf8(component(table, "-Data.db.jsonl")).expect("utf8");
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("json");
        if v["partition"]["key"][0] != "1" {
            continue;
        }
        for r in v["rows"].as_array().expect("rows") {
            if r["type"] == "row" {
                out.push(parse_uuid(r["clustering"][0].as_str().expect("ck")));
            }
        }
    }
    out
}

fn separators(table: &str) -> Vec<Vec<u8>> {
    let mut cur = Cursor::new(component(table, "-Partitions.db"));
    let loc = lookup_raw_key_in_bti_partitions_db(&mut cur, &1i32.to_be_bytes())
        .expect("Partitions.db lookup")
        .expect("pk 1 present");
    let BtiPartitionLocation::RowsOffset(off) = loc else {
        panic!("{table}: pk 1 must be a wide partition with a row index");
    };
    let rdb = component(table, "-Rows.db");
    let header = resolve_rows_db_entry(&rdb, off as usize).expect("Rows.db entry");
    let root = header.require_trie_root().expect("valid trie root");
    iterate_rows_in_bti_trie(&rdb, root)
        .expect("row-index traversal")
        .into_iter()
        .map(|(sep, _)| sep)
        .collect()
}

#[test]
fn cassandra_written_separators_equal_separator_gt_of_cqlite_key_encodings() {
    for (table, desc, timeuuid) in TABLES {
        let keys = golden_keys(table);
        assert_eq!(keys.len(), 480, "{table}: golden pk=1 rows");
        let enc: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| {
                encode_clustering_bound_oss50_typed(&[Value::Uuid(*k)], &[desc], &[timeuuid])
                    .expect("uuid encodes")
            })
            .collect();

        // (2) CQLite's encoding sorts exactly in Cassandra's clustering order.
        for (i, w) in enc.windows(2).enumerate() {
            assert!(
                w[0] < w[1],
                "{table}: encoding of golden rows {i} and {} is not strictly ascending: {:02x?} vs {:02x?}",
                i + 1,
                w[0],
                w[1]
            );
        }

        // (1) every inter-block separator the REAL Rows.db holds equals, byte for byte, Cassandra's
        // `ByteSource.separatorGt(prevBlockLastKey, blockFirstKey)` (cassandra-5.0.8
        // `ByteSource.java:234-243`: "length 1 longer than the common prefix of the two sources,
        // with last byte one higher than the prevMax source") of two CONSECUTIVE rows' CQLite
        // encodings (the last row of one block and the first row of the next are consecutive).
        let sep_gt = |p: &[u8], c: &[u8]| -> Vec<u8> {
            let d = p
                .iter()
                .zip(c)
                .position(|(a, b)| a != b)
                .expect("distinct keys");
            let mut s = p[..d].to_vec();
            s.push(p[d].wrapping_add(1));
            s
        };
        let candidates: Vec<Vec<u8>> = enc.windows(2).map(|w| sep_gt(&w[0], &w[1])).collect();
        let seps = separators(table);
        assert!(
            seps.len() >= 9,
            "{table}: expected several blocks, got {} separators",
            seps.len()
        );
        assert!(
            seps[0].is_empty(),
            "{table}: first separator is ByteComparable.EMPTY"
        );
        // the last entry is complete()'s trailing separator (after the final row), checked apart
        let inter = &seps[1..seps.len() - 1];
        let mut checked = 0;
        for sep in inter {
            assert!(
                candidates.contains(sep),
                "{table}: Cassandra-written separator {sep:02x?} is not separatorGt of ANY two consecutive CQLite key encodings"
            );
            checked += 1;
        }
        // NEGATIVE CONTROL: the pre-fix RAW 16-byte encoding reproduces (almost) none of them, so
        // this oracle genuinely discriminates the swizzle.
        let raw: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| [&[0x40u8][..], &k[..]].concat())
            .collect();
        let raw_candidates: Vec<Vec<u8>> = raw
            .windows(2)
            .filter(|w| w[0] != w[1])
            .map(|w| {
                let d = w[0]
                    .iter()
                    .zip(&w[1])
                    .position(|(a, b)| a != b)
                    .expect("distinct");
                let mut s = w[0][..d].to_vec();
                s.push(w[0][d].wrapping_add(1));
                s
            })
            .collect();
        let raw_hits = inter.iter().filter(|s| raw_candidates.contains(s)).count();
        assert!(
            raw_hits * 2 < inter.len(),
            "{table}: raw bytes matched {raw_hits}/{} separators - the oracle would not detect the bug",
            inter.len()
        );
        // NEGATIVE CONTROL 2 (flag swap): the same keys encoded with the WRONG uuid-vs-timeuuid
        // declaration. On the tie tables the separators are cut inside the lsb, where the two
        // swizzles differ by the 0x80 XOR, so almost none may match; on the other tables the
        // common prefix ends before the lsb and the swap is invisible (so no assertion there).
        if table.starts_with("timeuuid_ties") {
            let swapped: Vec<Vec<u8>> = keys
                .iter()
                .map(|k| {
                    encode_clustering_bound_oss50_typed(&[Value::Uuid(*k)], &[desc], &[!timeuuid])
                        .expect("uuid encodes")
                })
                .collect();
            let swapped_candidates: Vec<Vec<u8>> = swapped
                .windows(2)
                .filter(|w| w[0] != w[1])
                .map(|w| sep_gt(&w[0], &w[1]))
                .collect();
            let swapped_hits = inter
                .iter()
                .filter(|s| swapped_candidates.contains(s))
                .count();
            assert!(
                swapped_hits * 2 < inter.len(),
                "{table}: the WRONG uuid/timeuuid flag still matched {swapped_hits}/{} separators - \
                 the oracle would not detect a flag swap or a dropped lsb XOR",
                inter.len()
            );
        }
        let tail = seps.last().expect("trailing separator");
        assert!(
            tail.as_slice() > enc[enc.len() - 1].as_slice() || enc[enc.len() - 1].starts_with(tail),
            "{table}: trailing separator {tail:02x?} vs last key {:02x?}",
            enc[enc.len() - 1]
        );
        eprintln!("{table}: {checked} Cassandra-written separators matched separatorGt of CQLite key encodings");
        assert!(
            checked >= 8,
            "{table}: too few separators checked ({checked})"
        );
    }
}
