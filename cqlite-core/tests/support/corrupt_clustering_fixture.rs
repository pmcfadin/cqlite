//! Issue #3782 — stage a REAL Cassandra 5.0 fixture twice: pristine (`control`)
//! and with ONE COMPRESSED byte of a `text` CLUSTERING-key value flipped
//! (`mutated`). Two fixtures are staged by the same code path: BIG (`nb`) and
//! BTI (`da`) — see [`BIG_COMPOSITE`] and [`BTI_MULTICLUSTERING`].
//!
//! Shared by the `#[ignore]`d measurement probe (`probe_3782.rs`) and the
//! committed regression lane (`issue_3782_corrupt_row_refusal.rs`) so the two
//! can never drift apart.
//!
//! # Why this oracle and not a CQLite-written one (#3042)
//!
//! Every byte here comes from a Cassandra-written SSTable. The corruption is
//! applied to the LZ4 **literal** carrying the value and the chunk's trailing
//! CRC32 is then recomputed, so it is length-preserving (asserted here) and
//! invisible to integrity checks. One compressed byte is what real bit-rot
//! changes; LZ4 back-references replicate it into 1..N decompressed positions,
//! every one of which is asserted to hold the flipped value. A CQLite-written
//! round-trip fixture could not evidence this at all: both legs would share any
//! framing mistake.
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

/// Which Cassandra-written fixture to stage, and how to find its mutation site.
///
/// `needles` is an OPTIMISATION, not a requirement: it names values a human
/// already knows to be unique in that fixture. When none of them is a clean
/// single-byte literal (the BTI fixture's clustering values repeat hundreds of
/// times, so LZ4 emits them as back-references), the site is DERIVED from the
/// fixture's own bytes instead — see [`mutate_text_literal`]. Either way the
/// mutation is ACCEPTED only after decompressing and observing that exactly ONE
/// decompressed byte changed, so the site is established by measurement rather
/// than by the needle list being trusted.
pub struct FixtureSpec {
    pub keyspace: &'static str,
    pub table: &'static str,
    pub schema_file: &'static str,
    pub needles: &'static [&'static [u8]],
    /// Which byte OF THE NEEDLE to flip. Stated per fixture rather than derived,
    /// because the two fixtures need different answers and a rule that produced
    /// both would be cleverness in a place that has to stay obvious: BIG's
    /// needles are bare values, so byte 0 is inside the value (and is the site
    /// the pre-fix numbers in `issue_3782_corrupt_row_refusal.rs` were measured
    /// at, so it is pinned); the BTI needle carries a 2-byte length prefix, so
    /// byte 0 would corrupt the LENGTH — a different corruption class, framing
    /// rather than value — and the last byte is used instead.
    pub flip_offset_in_needle: usize,
}

/// `test_basic.composite_key_table` — Cassandra 5.0 `nb`/BIG, LZ4,
/// `clustering_key2 TEXT`.
pub const BIG_COMPOSITE: FixtureSpec = FixtureSpec {
    keyspace: FIX_KS,
    table: FIX_TABLE,
    schema_file: SCHEMA_FILE,
    needles: NEEDLES,
    flip_offset_in_needle: 0,
};

/// `test_da.multiclustering_table` — Cassandra 5.0 `da`/BTI, LZ4,
/// `bucket TEXT` + `seq INT` clustering, `payload TEXT`.
///
/// The BTI counterpart matters because the `da` full scan takes a DIFFERENT
/// route to the same parse: `bti_scan_with_metadata_cancellable` stitches the
/// whole data section and calls `parse_block_with_cell_metadata` (issue #3782,
/// roborev job 48), where BIG goes through `sequential_scan`/`parse_block`.
///
/// The needle carries the clustering value's 2-byte length prefix
/// (`0x0017` = 23 = `len("charlie-extended-bucket")`) because the same text also
/// appears INSIDE every `payload` value of that partition, and a `payload`
/// mutation is NOT a decode error on this reader (measured: the row still
/// decodes, carrying the invalid byte). Pinning the prefix pins the CLUSTERING
/// field, whose decode does validate — the same corruption class the BIG lane
/// uses.
pub const BTI_MULTICLUSTERING: FixtureSpec = FixtureSpec {
    keyspace: "test_da",
    table: "multiclustering_table",
    schema_file: "multiclustering-table-bti.cql",
    needles: &[b"\x00\x17charlie-extended-bucket"],
    // Last byte: inside the VALUE, past the 2-byte length prefix.
    flip_offset_in_needle: b"\x00\x17charlie-extended-bucket".len() - 1,
};

/// A staged control/mutated pair. Each `*_root` is an ingestion data root — the
/// directory holding `<keyspace>/<table>-<uuid>/`, which is what
/// `IngestionConfig::data_dir` expects.
pub struct Staged {
    pub control_root: PathBuf,
    pub mutated_root: PathBuf,
    pub control_dir: PathBuf,
    pub mutated_dir: PathBuf,
    /// The FIRST DECOMPRESSED Data.db offset that reads `0xFF` after the flip.
    pub mutated_offset: usize,
    /// How many decompressed positions the ONE compressed-byte flip changed.
    /// `1` when the literal is referenced nowhere else; more when LZ4
    /// back-references replicate it (every one of them holds the same `0xFF`).
    pub mutated_span: usize,
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

/// `CompressionInfo.db` → (algorithm, chunk_length, chunk_offsets). Parsed
/// independently of the code under test so the chunk layout is an on-disk fact,
/// not a derived one.
fn parse_compression_info(p: &Path) -> (String, usize, Vec<u64>) {
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
    let chunk_length = be32(o);
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
    (alg, chunk_length, offs)
}

/// Flip ONE byte of the COMPRESSED chunk — a byte carried verbatim inside an
/// LZ4 literal — to `0xFF`, fix that chunk's trailing CRC32, and report where
/// the change lands in the DECOMPRESSED data section.
///
/// # Why a COMPRESSED-domain flip
///
/// This is the corruption model real bit-rot produces: what rots is the bytes on
/// disk, which for a compressed SSTable are the compressed ones. A single
/// compressed literal byte is replicated into every decompressed position that
/// LZ4 back-references it from, so the decompressed change is 1..N positions —
/// each holding the SAME flipped value. That is asserted, not assumed: the
/// candidate is accepted only after re-decompressing and observing (a) the
/// decompressed LENGTH is unchanged, so no framing was disturbed, and (b) EVERY
/// changed position reads `0xFF`. A candidate that fails either check is
/// reverted and the next one tried.
///
/// `0xFF` is never a valid UTF-8 byte in any position, so a `text` value
/// carrying it is a decode error by the FORMAT (Cassandra stores UTF8Type as
/// UTF-8), never by CQLite's prior behaviour.
///
/// # Why the needle must pin the FIELD, not just the text
///
/// Measured on `test_da.multiclustering_table`: a `payload` (regular cell) byte
/// flipped to `0xFF` is NOT refused — the row still decodes and carries the
/// invalid byte — while the same flip inside a CLUSTERING value is a decode
/// error. So each spec's needle names the on-disk spelling of a clustering
/// value (its length prefix included where the same text also occurs in a
/// regular cell), and the caller's test asserts the refusal, so a needle that
/// stopped pinning the right field fails loudly rather than passing quietly.
fn mutate_text_literal(dir: &Path, spec: &FixtureSpec) -> (usize, usize) {
    let (alg, chunk_length, offs) = parse_compression_info(&comp_file(dir, "-CompressionInfo.db"));
    assert!(
        alg.to_uppercase().contains("LZ4"),
        "expected an LZ4-compressed fixture, got {alg}"
    );
    let data_path = comp_file(dir, "-Data.db");
    let mut data = std::fs::read(&data_path).expect("read Data.db");
    let file_len = data.len() as u64;

    // Per-needle occurrence tally across EVERY chunk, so an unfound needle is
    // distinguishable from a found-but-unflippable one (roborev job 52).
    let mut needle_hits = vec![0usize; spec.needles.len()];

    for (i, &start) in offs.iter().enumerate() {
        let end = offs.get(i + 1).copied().unwrap_or(file_len);
        let (lo, hi) = (start as usize, (end - 4) as usize);
        let before = lz4_flex::decompress_size_prepended(&data[lo..hi]).expect("decompress chunk");
        if before.is_empty() {
            continue;
        }
        // Chunks are fixed-size in the DECOMPRESSED domain (`chunk_length` from
        // CompressionInfo.db), so the reported offset is the position within
        // the whole stitched data section.
        let chunk_base = i * chunk_length;
        for flip_at in candidate_literal_sites(&data[lo..hi], spec, &mut needle_hits) {
            let abs = lo + flip_at;
            let orig = data[abs];
            if !orig.is_ascii_graphic() {
                continue;
            }
            data[abs] = 0xFF;
            let after = match lz4_flex::decompress_size_prepended(&data[lo..hi]) {
                Ok(a) => a,
                Err(_) => {
                    data[abs] = orig;
                    continue;
                }
            };
            let diffs: Vec<usize> = if after.len() == before.len() {
                (0..before.len())
                    .filter(|&k| before[k] != after[k])
                    .collect()
            } else {
                Vec::new()
            };
            if diffs.is_empty() || !diffs.iter().all(|&k| after[k] == 0xFF) {
                data[abs] = orig; // not a clean replicated flip; try the next site
                continue;
            }
            let crc = crc32fast::hash(&data[lo..hi]).to_be_bytes();
            data[hi..hi + 4].copy_from_slice(&crc);
            std::fs::write(&data_path, &data).expect("write mutated Data.db");
            return (chunk_base + diffs[0], diffs.len());
        }
    }
    // A needle that occurs NOWHERE is a broken fixture/spec, not an
    // unflippable candidate — name it, so the scaffolding cannot weaken a case
    // by silently finding nothing to mutate.
    let missing: Vec<String> = spec
        .needles
        .iter()
        .zip(needle_hits.iter())
        .filter(|(_, &hits)| hits == 0)
        .map(|(needle, _)| format!("{:?}", String::from_utf8_lossy(needle)))
        .collect();
    assert!(
        missing.is_empty(),
        "{}.{}: needle(s) {} occur in NO compressed chunk — the fixture bytes or \
         the needle spelling changed, so there was never a site to flip \
         (per-needle hits across {} chunk(s): {:?})",
        spec.keyspace,
        spec.table,
        missing.join(", "),
        offs.len(),
        needle_hits
    );
    panic!(
        "no needle of {}.{} is carried verbatim as a flippable LZ4 literal \
         (per-needle hits across {} chunk(s): {:?}; every occurrence was either \
         non-unique in its chunk, non-graphic, or not a clean replicated flip)",
        spec.keyspace,
        spec.table,
        offs.len(),
        needle_hits
    );
}

/// Compressed-chunk byte positions worth flipping: for each needle that occurs
/// EXACTLY ONCE in the compressed chunk (hence verbatim in a literal), the byte
/// at the spec's [`FixtureSpec::flip_offset_in_needle`].
///
/// `hits_per_needle[j]` is INCREMENTED by the number of occurrences of
/// `spec.needles[j]` in `comp`, so the caller can tell "no candidate was
/// flippable" from "the needle occurs NOWHERE" — the latter means the fixture or
/// the needle spelling changed, and it must fail loudly rather than skip
/// quietly (roborev job 52).
///
/// The scan bound is INCLUSIVE of the last valid start index
/// (`comp.len() - needle.len()`): an exclusive bound silently missed a needle
/// ending exactly at the end of the chunk, which reads as a false
/// "needle not found".
fn candidate_literal_sites(
    comp: &[u8],
    spec: &FixtureSpec,
    hits_per_needle: &mut [usize],
) -> impl Iterator<Item = usize> {
    assert_eq!(
        hits_per_needle.len(),
        spec.needles.len(),
        "{}.{}: hit counter has {} slots for {} needles",
        spec.keyspace,
        spec.table,
        hits_per_needle.len(),
        spec.needles.len()
    );
    let mut sites: Vec<usize> = Vec::new();
    for (j, needle) in spec.needles.iter().enumerate() {
        assert!(
            spec.flip_offset_in_needle < needle.len(),
            "{}.{}: flip offset {} is outside its {}-byte needle",
            spec.keyspace,
            spec.table,
            spec.flip_offset_in_needle,
            needle.len()
        );
        // A needle longer than the chunk cannot occur in it at all; the
        // inclusive bound below would underflow, so answer it directly.
        if needle.len() > comp.len() {
            continue;
        }
        let hits: Vec<usize> = (0..=comp.len() - needle.len())
            .filter(|&k| &comp[k..k + needle.len()] == *needle)
            .collect();
        hits_per_needle[j] += hits.len();
        if let [k] = hits.as_slice() {
            sites.push(k + spec.flip_offset_in_needle);
        }
    }
    sites.into_iter()
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create staging dir");
    for e in std::fs::read_dir(src).expect("read fixture dir").flatten() {
        if e.path().is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).expect("copy fixture component");
        }
    }
}

/// Stage a pristine copy and a single-byte-mutated copy of the BIG fixture
/// directory `src` under a `tag`-unique temp root.
pub fn stage_control_and_mutated(src: &Path, tag: &str) -> Staged {
    stage_spec(&BIG_COMPOSITE, src, tag)
}

/// [`stage_control_and_mutated`] for an explicit [`FixtureSpec`], so the BTI
/// (`da`) lane stages its own Cassandra-written fixture through exactly the same
/// mutation + measurement path (no second implementation to drift).
pub fn stage_spec(spec: &FixtureSpec, src: &Path, tag: &str) -> Staged {
    let name = src
        .file_name()
        .expect("fixture directory has a name")
        .to_owned();
    let root = std::env::temp_dir().join(format!("cqlite-3782-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let control_root = root.join("ctl").join("sstables");
    let mutated_root = root.join("mut").join("sstables");
    let control_dir = control_root.join(spec.keyspace).join(&name);
    let mutated_dir = mutated_root.join(spec.keyspace).join(&name);
    copy_dir(src, &control_dir);
    copy_dir(src, &mutated_dir);
    let (mutated_offset, mutated_span) = mutate_text_literal(&mutated_dir, spec);
    Staged {
        control_root,
        mutated_root,
        control_dir,
        mutated_dir,
        mutated_offset,
        mutated_span,
    }
}
