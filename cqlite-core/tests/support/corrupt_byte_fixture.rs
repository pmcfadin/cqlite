//! Stage a REAL Cassandra 5.0 fixture twice — pristine (`control`) and with
//! exactly ONE decompressed byte changed (`mutated`) — for the corrupt-byte
//! refusal lanes.
//!
//! Two mutation FAMILIES share the staging, the CRC recomputation and the
//! measurement, because they are the same experiment aimed at two different
//! decode arms:
//!
//! * [`Mutation::ClusteringTextLiteral`] (issue #3782) corrupts a `text`
//!   CLUSTERING-key value — the ROW arm.
//! * [`Mutation::FirstPartitionHeader`] (issue #3928) corrupts one byte of the
//!   FIRST partition HEADER — the header arm, which resynchronised by skipping
//!   a byte and could therefore both DROP the partition and INVENT one out of
//!   misaligned bytes.
//!
//! Each family is staged for BIG (`nb`) and BTI (`da`) — see [`BIG_COMPOSITE`],
//! [`BTI_MULTICLUSTERING`], [`BIG_COMPOSITE_HEADER`] and
//! [`BTI_MULTICLUSTERING_HEADER`].
//!
//! Shared by the `#[ignore]`d measurement probe (`probe_3782.rs`) and the
//! committed regression lanes (`issue_3782_corrupt_row_refusal.rs`,
//! `issue_3928_corrupt_header_refusal.rs`) so they can never drift apart.
//!
//! # Why this oracle and not a CQLite-written one (#3042)
//!
//! Every byte here comes from a Cassandra-written SSTable. The corruption is
//! applied to the COMPRESSED chunk and the chunk's trailing CRC32 is then
//! recomputed, so it is length-preserving (asserted here) and invisible to
//! integrity checks. One compressed byte is what real bit-rot changes. A
//! CQLite-written round-trip fixture could not evidence any of this: both legs
//! would share any framing mistake.
//!
//! The expectations the consuming tests assert are derived from the FORMAT and
//! from the pristine fixture's own Cassandra-written content, never from
//! CQLite's prior behaviour.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use tempfile::TempDir;

pub const FIX_KS: &str = "test_basic";
pub const FIX_TABLE: &str = "composite_key_table";
pub const SCHEMA_FILE: &str = "basic-types.cql";

/// Clustering-key values known to be unique in this fixture; the first one that
/// also appears exactly once as a verbatim LZ4 literal is the mutation target.
const NEEDLES: &[&[u8]] = &[b"necessary", b"purpose", b"artist", b"region", b"glass"];

/// What single-byte corruption to apply, and how to locate it.
///
/// Both families end in the SAME place — one changed byte in the DECOMPRESSED
/// data section, verified by re-decompressing — so they share
/// [`stage_spec`]'s staging, CRC recomputation and acceptance checks. Only the
/// SITE differs, and each variant says how its site is derived.
pub enum Mutation {
    /// Issue #3782 — the ROW arm. Flip one byte of a `text` CLUSTERING value.
    ///
    /// `needles` is an OPTIMISATION, not a requirement: it names values a human
    /// already knows to be unique in that fixture, and the site is ACCEPTED only
    /// after decompressing and observing the intended change, so the site is
    /// established by measurement rather than by the needle list being trusted.
    ClusteringTextLiteral {
        needles: &'static [&'static [u8]],
        /// Which byte OF THE NEEDLE to flip. Stated per fixture rather than
        /// derived, because the two fixtures need different answers and a rule
        /// that produced both would be cleverness in a place that has to stay
        /// obvious: BIG's needles are bare values, so byte 0 is inside the value
        /// (and is the site the pre-fix numbers in
        /// `issue_3782_corrupt_row_refusal.rs` were measured at, so it is
        /// pinned); the BTI needle carries a 2-byte length prefix, so byte 0
        /// would corrupt the LENGTH — a different corruption class, framing
        /// rather than value — and the last byte is used instead.
        flip_offset_in_needle: usize,
    },
    /// Issue #3928 — the partition-HEADER arm. Overwrite one byte of the FIRST
    /// partition header, whose position is a FORMAT FACT and needs no index
    /// component to locate: a Cassandra `Data.db` begins with its first
    /// partition's header (`SortedTableWriter.append`, cassandra-5.0.8:
    /// `ByteBufferUtil.writeWithShortLength(key.getKey(), writer)` followed by
    /// the partition-level `DeletionTime`), so the header starts at DECOMPRESSED
    /// offset 0 and therefore inside chunk 0.
    FirstPartitionHeader(HeaderByte),
}

/// Which byte of the first partition header to overwrite — and, per variant, the
/// value it must currently hold and the value written over it. Both are
/// DERIVED from the variant rather than passed in, so there is no knob a caller
/// can set to a value that corrupts nothing.
#[derive(Clone, Copy, Debug)]
pub enum HeaderByte {
    /// The LOW byte of the 2-byte big-endian key length that
    /// `ByteBufferUtil.writeWithShortLength` writes ahead of the partition key
    /// (cassandra-5.0.8 `ByteBufferUtil.java:362-368`), zeroed.
    ///
    /// The header then declares a 0-byte key while the key bytes Cassandra
    /// actually wrote still follow, so the 12-byte legacy `DeletionTime`
    /// (`DeletionTime.LegacySerializer`, always 4-byte LDT + 8-byte MFDA for
    /// `nb`) is read out of the middle of the KEY and every later structure is
    /// misframed. The declared length no longer matches the content — corruption
    /// by the format, independent of any reader.
    ///
    /// This is the only single-byte route to the header arm on `nb`: that
    /// format's `DeletionTime` has no invalid encodings (any 12 bytes parse), so
    /// a length/content disagreement is what a header-framing corruption looks
    /// like there.
    KeyLengthLowByte,
    /// The partition-level `DeletionTime` discriminator, at `2 + key_len`, set to
    /// `0xFF`.
    ///
    /// `oa`/`da` write a LIVE partition deletion as the single byte
    /// `IS_LIVE_DELETION = 0b1000_0000` (cassandra-5.0.8
    /// `DeletionTime.java:208-213`), and their own reader THROWS on any other
    /// byte with that bit set: `if ((flags & 0xFF) != IS_LIVE_DELETION) throw new
    /// IOException("Corrupted sstable. Invalid flags found deserializing
    /// DeletionTime")` (`DeletionTime.java:222-230`). So `0xFF` here is a header
    /// Cassandra itself refuses — the expectation is the format's, not CQLite's.
    ///
    /// Only applicable to a fixture whose first partition is LIVE, which is
    /// ASSERTED (the pre-flip byte must be exactly `0x80`) rather than assumed.
    DeletionTimeDiscriminator,
}

impl HeaderByte {
    /// The byte written over the site.
    fn replacement(self) -> u8 {
        match self {
            // 0 is the ONLY value that makes the declared key length disagree
            // with the key Cassandra wrote using a single byte (the high byte is
            // already 0 for any key shorter than 256 bytes, so raising the low
            // byte only lengthens the declared key).
            HeaderByte::KeyLengthLowByte => 0x00,
            // Sign bit set (so it is read as the LIVE form) but not the LIVE
            // sentinel — the exact shape `DeletionTime.Serializer.deserialize`
            // rejects. All-ones is also what a rotted byte looks like.
            HeaderByte::DeletionTimeDiscriminator => 0xFF,
        }
    }
}

/// Which Cassandra-written fixture to stage, and how to corrupt it.
pub struct FixtureSpec {
    pub keyspace: &'static str,
    pub table: &'static str,
    pub schema_file: &'static str,
    pub mutation: Mutation,
}

/// `test_basic.composite_key_table` — Cassandra 5.0 `nb`/BIG, LZ4,
/// `clustering_key2 TEXT`.
pub const BIG_COMPOSITE: FixtureSpec = FixtureSpec {
    keyspace: FIX_KS,
    table: FIX_TABLE,
    schema_file: SCHEMA_FILE,
    mutation: Mutation::ClusteringTextLiteral {
        needles: NEEDLES,
        flip_offset_in_needle: 0,
    },
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
/// decodes, carrying the invalid byte).
///
/// That non-refusal is the `row_data.rs` per-column break-swallow family —
/// tracked as **#3778**, the nested-consumption half split out of #3723 — and it
/// is NOT what #3782 changed: a REGULAR-column value whose decode fails is
/// dropped from the row rather than refused, so a `payload` mutation could never
/// evidence this issue's property. Pinning the length prefix pins the CLUSTERING
/// field, whose decode does validate — the same corruption class the BIG lane
/// uses.
pub const BTI_MULTICLUSTERING: FixtureSpec = FixtureSpec {
    keyspace: "test_da",
    table: "multiclustering_table",
    schema_file: "multiclustering-table-bti.cql",
    mutation: Mutation::ClusteringTextLiteral {
        needles: &[b"\x00\x17charlie-extended-bucket"],
        // Last byte: inside the VALUE, past the 2-byte length prefix.
        flip_offset_in_needle: b"\x00\x17charlie-extended-bucket".len() - 1,
    },
};

/// Issue #3928 — the BIG (`nb`) header-arm lane. Same fixture as
/// [`BIG_COMPOSITE`], corrupted in its FIRST partition header instead of in a
/// clustering value.
pub const BIG_COMPOSITE_HEADER: FixtureSpec = FixtureSpec {
    keyspace: FIX_KS,
    table: FIX_TABLE,
    schema_file: SCHEMA_FILE,
    mutation: Mutation::FirstPartitionHeader(HeaderByte::KeyLengthLowByte),
};

/// Issue #3928 — the BTI (`da`) header-arm lane. Same fixture as
/// [`BTI_MULTICLUSTERING`]; `da` has `hasUIntDeletionTime`, so its
/// partition-level `DeletionTime` HAS an invalid encoding and the corruption is
/// one Cassandra's own reader throws on.
pub const BTI_MULTICLUSTERING_HEADER: FixtureSpec = FixtureSpec {
    keyspace: "test_da",
    table: "multiclustering_table",
    schema_file: "multiclustering-table-bti.cql",
    mutation: Mutation::FirstPartitionHeader(HeaderByte::DeletionTimeDiscriminator),
};

/// A staged control/mutated pair. Each `*_root` is an ingestion data root — the
/// directory holding `<keyspace>/<table>-<uuid>/`, which is what
/// `IngestionConfig::data_dir` expects.
///
/// # THIS VALUE OWNS THE STAGED BYTES — HOLD IT FOR THE WHOLE TEST (#3950)
///
/// `Staged` owns a `tempfile::TempDir`, so **dropping it deletes both staged
/// generations**. Every path field points INSIDE that directory, so a helper
/// that copies the paths out and lets the `Staged` go out of scope leaves its
/// caller holding paths to nothing — a mid-test disappearance, which is a worse
/// failure than the leak this ownership fixes. Bind it (`let staged = …;`) for
/// as long as any of its paths is used, and return the whole value from a
/// staging helper rather than the paths alone.
///
/// Before this, the harness staged into a PID-named directory under the system
/// temp dir and never removed it, so every run of every consuming target left
/// two complete SSTable generations behind (roborev job 59 finding 1). The
/// cleanup is pinned by `the_staging_harness_removes_both_generations_on_drop`
/// in `issue_3782_corrupt_row_refusal.rs`: present while the value is alive,
/// gone once it drops.
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
    /// The staging directory both `*_root`s live under. Held so its `Drop`
    /// removes the two copied generations when the test that staged them ends;
    /// read only through `Staged::staging_root`.
    staging: TempDir,
}

impl Staged {
    /// The temp directory both staged generations live under. Exposed so a test
    /// can assert the cleanup itself (present while alive, gone after drop).
    pub fn staging_root(&self) -> &Path {
        self.staging.path()
    }
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
fn mutate_text_literal(
    dir: &Path,
    spec: &FixtureSpec,
    needles: &[&[u8]],
    flip_offset_in_needle: usize,
) -> (usize, usize) {
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
    let mut needle_hits = vec![0usize; needles.len()];

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
        for flip_at in candidate_literal_sites(
            &data[lo..hi],
            spec,
            needles,
            flip_offset_in_needle,
            &mut needle_hits,
        ) {
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
    let missing: Vec<String> = needles
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
/// at `flip_offset_in_needle`.
///
/// `hits_per_needle[j]` is INCREMENTED by the number of occurrences of
/// `needles[j]` in `comp`, so the caller can tell "no candidate was
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
    needles: &[&[u8]],
    flip_offset_in_needle: usize,
    hits_per_needle: &mut [usize],
) -> impl Iterator<Item = usize> {
    assert_eq!(
        hits_per_needle.len(),
        needles.len(),
        "{}.{}: hit counter has {} slots for {} needles",
        spec.keyspace,
        spec.table,
        hits_per_needle.len(),
        needles.len()
    );
    let mut sites: Vec<usize> = Vec::new();
    for (j, needle) in needles.iter().enumerate() {
        assert!(
            flip_offset_in_needle < needle.len(),
            "{}.{}: flip offset {} is outside its {}-byte needle",
            spec.keyspace,
            spec.table,
            flip_offset_in_needle,
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
            sites.push(k + flip_offset_in_needle);
        }
    }
    sites.into_iter()
}

/// Overwrite ONE byte of the FIRST partition header, in the COMPRESSED domain,
/// and report where the change lands in the DECOMPRESSED data section.
///
/// # Why the site needs no index component
///
/// A Cassandra `Data.db` begins with its first partition's header
/// (`SortedTableWriter.append` → `ByteBufferUtil.writeWithShortLength(key, …)`
/// then the partition-level `DeletionTime`, cassandra-5.0.8), and chunks are
/// fixed-size in the DECOMPRESSED domain, so that header lives at decompressed
/// offset 0 — inside chunk 0 — on both BIG and BTI. The byte within it is
/// derived per [`HeaderByte`], reading the key length out of the fixture's own
/// bytes rather than assuming a key width.
///
/// # Why a COMPRESSED-domain write, found by search
///
/// What rots on disk is the compressed bytes, so that is where the change is
/// made. Which compressed byte produces the wanted decompressed change is found
/// by TRYING each position in the chunk and keeping the first whose
/// re-decompression differs from the pristine chunk in EXACTLY the target
/// position and holds EXACTLY the wanted value. That acceptance test is what
/// makes the search sound rather than clever: a candidate that perturbed an LZ4
/// token, match length or match offset would change the decompressed LENGTH or
/// many bytes and is rejected, so every accepted site is behaviourally a
/// single-literal-byte flip. The chunk's trailing CRC32 is then recomputed, so
/// the corruption is length-preserving and invisible to integrity checks.
///
/// Returns `(decompressed_offset, 1)` — the span is always 1 here, by the
/// acceptance test above.
fn mutate_first_partition_header(
    dir: &Path,
    spec: &FixtureSpec,
    which: HeaderByte,
) -> (usize, usize) {
    let (alg, _chunk_length, offs) = parse_compression_info(&comp_file(dir, "-CompressionInfo.db"));
    assert!(
        alg.to_uppercase().contains("LZ4"),
        "expected an LZ4-compressed fixture, got {alg}"
    );
    let data_path = comp_file(dir, "-Data.db");
    let mut data = std::fs::read(&data_path).expect("read Data.db");
    let file_len = data.len() as u64;

    // Chunk 0 holds decompressed offset 0, hence the whole first header.
    let first = *offs.first().expect("CompressionInfo.db lists a chunk");
    let end = offs.get(1).copied().unwrap_or(file_len);
    let (lo, hi) = (first as usize, (end - 4) as usize);
    let before = lz4_flex::decompress_size_prepended(&data[lo..hi]).expect("decompress chunk 0");

    let target = header_byte_offset(spec, which, &before);
    let want = which.replacement();
    assert_ne!(
        before[target], want,
        "{}.{}: {which:?} at decompressed offset {target} already reads 0x{want:02x}, so \
         writing it would corrupt nothing",
        spec.keyspace, spec.table
    );

    for p in lo..hi {
        let orig = data[p];
        if orig == want {
            continue;
        }
        data[p] = want;
        let after = match lz4_flex::decompress_size_prepended(&data[lo..hi]) {
            Ok(a) => a,
            Err(_) => {
                data[p] = orig;
                continue;
            }
        };
        // Cheap discriminator first: the overwhelming majority of positions do
        // not touch the target byte at all.
        if after.len() != before.len() || after[target] != want {
            data[p] = orig;
            continue;
        }
        let changed: Vec<usize> = (0..before.len())
            .filter(|&k| before[k] != after[k])
            .collect();
        if changed != [target] {
            data[p] = orig; // it also disturbed something else; keep looking
            continue;
        }
        let crc = crc32fast::hash(&data[lo..hi]).to_be_bytes();
        data[hi..hi + 4].copy_from_slice(&crc);
        std::fs::write(&data_path, &data).expect("write mutated Data.db");
        return (target, 1);
    }
    panic!(
        "{}.{}: no byte of compressed chunk 0 ({} bytes) writes 0x{want:02x} to decompressed \
         offset {target} ({which:?}) and nothing else — the fixture's compression layout \
         changed, so there is no site to corrupt",
        spec.keyspace,
        spec.table,
        hi - lo,
    );
}

/// The DECOMPRESSED offset of [`HeaderByte`] within the first partition header,
/// plus the format assertion that the byte currently there is the one the format
/// says it is — so a fixture whose shape changed fails loudly instead of
/// corrupting an unrelated byte.
fn header_byte_offset(spec: &FixtureSpec, which: HeaderByte, dec: &[u8]) -> usize {
    let where_ = format!("{}.{} first partition header", spec.keyspace, spec.table);
    assert!(
        dec.len() > 2,
        "{where_}: chunk 0 decompressed to {} bytes, too short to hold a header",
        dec.len()
    );
    // `writeWithShortLength`: 2-byte BIG-ENDIAN unsigned key length.
    let key_len = ((dec[0] as usize) << 8) | dec[1] as usize;
    assert_eq!(
        dec[0], 0x00,
        "{where_}: declared key length {key_len} has a non-zero HIGH byte, so its low byte is \
         not the whole length and this derivation does not apply to this fixture"
    );
    assert!(
        key_len > 0,
        "{where_}: declared key length is already 0 — a Cassandra-written partition key of this \
         fixture is never empty, so the fixture bytes are not what this harness expects"
    );
    match which {
        HeaderByte::KeyLengthLowByte => 1,
        HeaderByte::DeletionTimeDiscriminator => {
            let at = 2 + key_len;
            assert!(
                at < dec.len(),
                "{where_}: the DeletionTime discriminator would sit at {at}, past chunk 0's \
                 {} decompressed bytes",
                dec.len()
            );
            // `DeletionTime.Serializer.serialize` writes exactly this byte for a
            // LIVE partition deletion (cassandra-5.0.8 DeletionTime.java:208-213).
            const OA_IS_LIVE_DELETION: u8 = 0b1000_0000;
            assert_eq!(
                dec[at], OA_IS_LIVE_DELETION,
                "{where_}: the DeletionTime discriminator at {at} reads 0x{:02x}, not the LIVE \
                 sentinel 0x80 — this lane needs a LIVE first partition, because the corruption \
                 it applies is 'the LIVE form with a byte Cassandra's own reader rejects'",
                dec[at]
            );
            at
        }
    }
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
/// directory `src` under a fresh `tag`-prefixed temp root OWNED BY THE RETURNED
/// [`Staged`] — which the caller must hold for as long as it uses the paths.
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
    // A `TempDir`, not a PID-named path under the system temp dir: the old form
    // was never removed, so each run of each consuming target left two whole
    // SSTable generations behind for ever (roborev job 59 finding 1). The `tag`
    // stays in the directory NAME so a human watching the temp dir can still
    // tell the lanes apart while they run. Uniqueness is now the OS's, so the
    // defensive `remove_dir_all` of a possibly-live sibling run is gone too.
    let staging = tempfile::Builder::new()
        .prefix(&format!("cqlite-corrupt-{tag}-"))
        .tempdir()
        .expect("create staging temp dir");
    let root = staging.path();
    let control_root = root.join("ctl").join("sstables");
    let mutated_root = root.join("mut").join("sstables");
    let control_dir = control_root.join(spec.keyspace).join(&name);
    let mutated_dir = mutated_root.join(spec.keyspace).join(&name);
    copy_dir(src, &control_dir);
    copy_dir(src, &mutated_dir);
    let (mutated_offset, mutated_span) = match &spec.mutation {
        Mutation::ClusteringTextLiteral {
            needles,
            flip_offset_in_needle,
        } => mutate_text_literal(&mutated_dir, spec, needles, *flip_offset_in_needle),
        Mutation::FirstPartitionHeader(which) => {
            mutate_first_partition_header(&mutated_dir, spec, *which)
        }
    };
    Staged {
        control_root,
        mutated_root,
        control_dir,
        mutated_dir,
        mutated_offset,
        mutated_span,
        staging,
    }
}
