//! Issue #3782 — stage a REAL Cassandra 5.0 fixture twice: pristine (`control`)
//! and with ONE byte of a `text` CLUSTERING-key value flipped (`mutated`).
//!
//! Shared by the `#[ignore]`d measurement probe (`probe_3782.rs`) and the
//! committed regression lane (`issue_3782_corrupt_row_refusal.rs`) so the two
//! can never drift apart.
//!
//! # Why this oracle and not a CQLite-written one (#3042)
//!
//! Every byte here comes from a Cassandra-written SSTable
//! (`test_basic.composite_key_table`, `nb`/BIG, LZ4, `clustering_key2 TEXT`).
//! The corruption is applied to the LZ4 **literal** carrying the value and the
//! chunk's trailing CRC32 is then recomputed, so the change is
//! length-preserving, provably a single DECOMPRESSED byte (asserted here), and
//! invisible to integrity checks. A CQLite-written round-trip fixture could not
//! evidence this at all: both legs would share any framing mistake.
//!
//! The expectations the consuming tests assert are derived from the FORMAT (a
//! `text` clustering value whose length prefix no longer frames a decodable
//! value is a decode error, and at the final chunk no further bytes can arrive
//! to complete it), never from CQLite's prior behaviour.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

pub const FIX_KS: &str = "test_basic";
pub const FIX_TABLE: &str = "composite_key_table";
pub const SCHEMA_FILE: &str = "basic-types.cql";

/// Clustering-key values known to be unique in this fixture; the first one that
/// also appears exactly once as a verbatim LZ4 literal is the mutation target.
const NEEDLES: &[&[u8]] = &[b"necessary", b"purpose", b"artist", b"region", b"glass"];

/// A staged control/mutated pair. Each `*_root` is an ingestion data root — the
/// directory holding `<keyspace>/<table>-<uuid>/`, which is what
/// `IngestionConfig::data_dir` expects.
pub struct Staged {
    pub control_root: PathBuf,
    pub mutated_root: PathBuf,
    pub control_dir: PathBuf,
    pub mutated_dir: PathBuf,
    /// The DECOMPRESSED Data.db offset whose byte was flipped to `0xFF`.
    pub mutated_offset: usize,
}

/// The first component in `dir` whose file name ends with `suffix`.
pub fn comp_file(dir: &Path, suffix: &str) -> PathBuf {
    let mut found: Option<PathBuf> = None;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            if e.file_name().to_string_lossy().ends_with(suffix) {
                found = Some(e.path());
                break;
            }
        }
    }
    match found {
        Some(p) => p,
        None => panic!("no {suffix} component in {dir:?}"),
    }
}

/// `CompressionInfo.db` → (algorithm, chunk_offsets). Parsed independently of the
/// code under test so the chunk layout is an on-disk fact, not a derived one.
fn parse_compression_info(p: &Path) -> (String, Vec<u64>) {
    let b = std::fs::read(p).expect("read CompressionInfo.db");
    let be16 = |o: usize| u16::from_be_bytes([b[o], b[o + 1]]) as usize;
    let be32 = |o: usize| {
        u32::from_be_bytes(b[o..o + 4].try_into().expect("4-byte big-endian field")) as usize
    };
    let nlen = be16(0);
    let mut o = 2usize;
    let alg = String::from_utf8_lossy(&b[o..o + nlen]).to_string();
    o += nlen;
    let opt = be32(o);
    o += 4;
    for _ in 0..opt {
        let kl = be16(o);
        o += 2 + kl;
        let vl = be16(o);
        o += 2 + vl;
    }
    o += 4 + 4 + 8; // chunk_length, max_compressed_length, data_length
    let n = be32(o);
    o += 4;
    let offs = (0..n)
        .map(|i| {
            u64::from_be_bytes(
                b[o + i * 8..o + i * 8 + 8]
                    .try_into()
                    .expect("8-byte chunk offset"),
            )
        })
        .collect();
    (alg, offs)
}

/// Flip ONE byte of the LZ4 literal carrying one of [`NEEDLES`] to `0xFF` in the
/// already-copied fixture at `dir`, and fix the chunk CRC32. Returns the changed
/// DECOMPRESSED offset. Asserts length-preservation and a single-byte change.
fn mutate_clustering_utf8(dir: &Path) -> usize {
    let (alg, offs) = parse_compression_info(&comp_file(dir, "-CompressionInfo.db"));
    assert!(
        alg.to_uppercase().contains("LZ4"),
        "expected an LZ4-compressed fixture, got {alg}"
    );
    let data_path = comp_file(dir, "-Data.db");
    let mut data = std::fs::read(&data_path).expect("read Data.db");
    let file_len = data.len() as u64;

    for needle in NEEDLES {
        for (i, &start) in offs.iter().enumerate() {
            let end = offs.get(i + 1).copied().unwrap_or(file_len);
            let (lo, hi) = (start as usize, (end - 4) as usize);
            let before =
                lz4_flex::decompress_size_prepended(&data[lo..hi]).expect("decompress chunk");
            let dhits: Vec<usize> = (0..before.len().saturating_sub(needle.len()))
                .filter(|&k| &before[k..k + needle.len()] == *needle)
                .collect();
            let chits: Vec<usize> = (0..(hi - lo).saturating_sub(needle.len()))
                .filter(|&k| &data[lo + k..lo + k + needle.len()] == *needle)
                .collect();
            if dhits.len() != 1 || chits.len() != 1 {
                continue;
            }
            let (dpos, flip_at) = (dhits[0], lo + chits[0]);
            let orig = data[flip_at];
            data[flip_at] = 0xFF;
            let after =
                lz4_flex::decompress_size_prepended(&data[lo..hi]).expect("re-decompress chunk");
            assert_eq!(
                before.len(),
                after.len(),
                "the mutation must be length-preserving"
            );
            let diffs: Vec<usize> = (0..before.len())
                .filter(|&k| before[k] != after[k])
                .collect();
            if diffs.as_slice() != [dpos] {
                data[flip_at] = orig; // not a clean single-byte change; try the next needle
                continue;
            }
            assert_eq!(after[dpos], 0xFF);
            let crc = crc32fast::hash(&data[lo..hi]).to_be_bytes();
            data[hi..hi + 4].copy_from_slice(&crc);
            std::fs::write(&data_path, &data).expect("write mutated Data.db");
            return dpos;
        }
    }
    panic!("no needle occurs exactly once as a verbatim LZ4 literal in any chunk");
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create staging dir");
    for e in std::fs::read_dir(src).expect("read fixture dir").flatten() {
        if e.path().is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).expect("copy fixture component");
        }
    }
}

/// Stage a pristine copy and a single-byte-mutated copy of the fixture directory
/// `src` under a `tag`-unique temp root.
pub fn stage_control_and_mutated(src: &Path, tag: &str) -> Staged {
    let name = src
        .file_name()
        .expect("fixture directory has a name")
        .to_owned();
    let root = std::env::temp_dir().join(format!("cqlite-3782-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let control_root = root.join("ctl").join("sstables");
    let mutated_root = root.join("mut").join("sstables");
    let control_dir = control_root.join(FIX_KS).join(&name);
    let mutated_dir = mutated_root.join(FIX_KS).join(&name);
    copy_dir(src, &control_dir);
    copy_dir(src, &mutated_dir);
    let mutated_offset = mutate_clustering_utf8(&mutated_dir);
    Staged {
        control_root,
        mutated_root,
        control_dir,
        mutated_dir,
        mutated_offset,
    }
}
