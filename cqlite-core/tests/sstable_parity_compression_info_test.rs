//! Strict `CompressionInfo.db` metadata + compressed-chunk boundary parity
//! (Epic #968 / issue #986).
//!
//! Proves CQLite decodes the compression metadata Apache Cassandra 5.0 persisted
//! into `CompressionInfo.db` and reads the inline per-chunk CRC trailers Cassandra
//! wrote into `Data.db`, byte-for-byte and field-for-field — with no name/path
//! heuristics (the compressor and every numeric field come from the parsed bytes).
//!
//! Scope owned here (issue #986):
//!   * **CompressionInfo.db byte layout + decoded metadata.** The compressor
//!     simple-name, option key/value pairs, `chunk_length`, `max_compressed_length`,
//!     uncompressed `data_length`, chunk count, and the ordered chunk-offset table
//!     are each re-derived independently from the raw bytes (a second, in-test
//!     reader) and asserted equal to CQLite's `CompressionInfo::parse` decode. The
//!     file must end *exactly* after the offset table — Cassandra appends no
//!     trailing metadata CRC to `CompressionInfo.db` (per-chunk CRCs live inline in
//!     `Data.db`). Offsets are compared as an ordered vector, not just a count.
//!   * **Compressed-chunk boundary parity against Data.db.** For every chunk, the
//!     record size (delta to the next offset, or to EOF for the final partial
//!     chunk) is checked against the `Data.db` length, and the trailing 4-byte
//!     big-endian inline CRC32 is recomputed over the compressed payload and
//!     asserted equal to the stored CRC — exactly as
//!     `CompressedSequentialWriter.java` writes it. Final partial-chunk handling
//!     (last record runs to EOF) is exercised by multi-chunk fixtures.
//!   * **Codec coverage from real fixtures.** The committed corpus carries real
//!     `LZ4Compressor` and `SnappyCompressor` fixtures; both are required to be
//!     exercised. `DeflateCompressor` / `ZstdCompressor` have NO Cassandra fixture
//!     in the corpus — they are classified explicitly as "no fixture available"
//!     and logged, never faked.
//!   * **Negative / fail-closed tests.** Corrupting `CompressionInfo.db` bytes
//!     (mutated in memory) makes `parse` reject the buffer, and corrupting a
//!     compressed chunk's bytes or its inline CRC in a copied `Data.db` buffer
//!     makes the strict CRC gate fail. Truncating the final record below its CRC
//!     trailer also fails closed.
//!
//! Fail-closed contract (matches the established dataset convention — see
//! `sstable_parity_toc_component_test.rs`):
//!   * The binary `*-CompressionInfo.db` and `*-Data.db` are fetched fixtures
//!     (gitignored). When the dataset is *entirely* absent (fresh checkout / CI
//!     without `fetch-datasets.sh`), the lane SKIPS — there is nothing to compare.
//!     Data.db presence is tracked INDEPENDENTLY of the compare count.
//!   * When binaries ARE present, the lane must actually compare fixtures and must
//!     exercise every codec the local corpus contains; a green run with zero
//!     comparisons, or with a present codec left uncovered, is a false pass and
//!     fails closed.
//!
//! Out of scope here (see manifest `planned` entries and issue #986 "Out of
//! scope"): Zstd dictionary compression, compression performance benchmarking,
//! Cassandra upgrade orchestration beyond persisted component bytes, and network
//! streaming compression.

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::compression_info::CompressionInfo;

/// Resolve the committed datasets root (env override first, else workspace tree).
fn datasets_sstables_root() -> PathBuf {
    let root = if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|workspace| workspace.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    };
    root.join("sstables")
}

/// Recursively collect every committed `*-TOC.txt` reference under `dir`,
/// skipping macOS AppleDouble shadow files (`._*`).
///
/// The `TOC.txt` manifest is the committed (git-tracked) anchor: it lists
/// `CompressionInfo.db` whenever the SSTable is compressed, so it lets the lane
/// fail closed when references vanish, independent of whether the gitignored
/// binary `CompressionInfo.db` has been fetched.
fn collect_toc_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("._") {
            continue;
        }
        if path.is_dir() {
            collect_toc_files(&path, out);
        } else if name.ends_with("-TOC.txt") {
            out.push(path);
        }
    }
}

fn all_toc_files() -> Vec<PathBuf> {
    let root = datasets_sstables_root();
    let mut out = Vec::new();
    collect_toc_files(&root, &mut out);
    out.sort();
    // Fail closed: a broken fixture path must turn the strict lane red, not green.
    assert!(
        !out.is_empty(),
        "no committed *-TOC.txt references found under {} — strict CompressionInfo.db parity \
         cannot run (this is a fail-closed guard, not a skip)",
        root.display()
    );
    out
}

/// True iff the committed `TOC.txt` manifest lists a `CompressionInfo.db`
/// component (i.e. the SSTable is compressed). Authoritative — read from the
/// manifest text Cassandra wrote, never inferred from a directory name.
fn toc_lists_compression_info(toc: &Path) -> bool {
    let Ok(content) = std::fs::read_to_string(toc) else {
        return false;
    };
    content
        .lines()
        .map(str::trim)
        .any(|l| l == "CompressionInfo.db")
}

/// Derive the SSTable base path (e.g. `.../nb-1-big`) from its `*-TOC.txt`.
fn base_from_toc(toc: &Path) -> PathBuf {
    let name = toc
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("non-UTF8 TOC filename: {}", toc.display()));
    let base = name
        .strip_suffix("-TOC.txt")
        .unwrap_or_else(|| panic!("TOC not *-TOC.txt: {name}"));
    toc.with_file_name(base)
}

/// The fields of a `CompressionInfo.db` re-derived independently from the raw
/// bytes by this test (a second reader), so the comparison against
/// `CompressionInfo::parse` is a genuine cross-check rather than a tautology.
#[derive(Debug, PartialEq, Eq)]
struct RawCompressionInfo {
    algorithm: String,
    options: Vec<(String, String)>,
    chunk_length: u32,
    max_compressed_length: u32,
    data_length: u64,
    chunk_offsets: Vec<u64>,
    /// Total bytes consumed (must equal the file length: no trailing metadata CRC).
    consumed: usize,
}

/// Independent re-implementation of the Cassandra `CompressionInfo.db` layout
/// (`CompressionMetadata.java:375-392`), used only to cross-check CQLite's parser.
/// Returns `Err` on any structural problem so corruption tests can assert failure.
fn read_raw_compression_info(d: &[u8]) -> Result<RawCompressionInfo, String> {
    let mut o = 0usize;
    let read_u16 = |d: &[u8], o: &mut usize| -> Result<u16, String> {
        if *o + 2 > d.len() {
            return Err(format!("short read u16 at {}", *o));
        }
        let v = u16::from_be_bytes([d[*o], d[*o + 1]]);
        *o += 2;
        Ok(v)
    };
    let read_utf = |d: &[u8], o: &mut usize| -> Result<String, String> {
        let len = read_u16(d, o)? as usize;
        if *o + len > d.len() {
            return Err(format!("short read utf({len}) at {}", *o));
        }
        let s = String::from_utf8(d[*o..*o + len].to_vec()).map_err(|e| e.to_string())?;
        *o += len;
        Ok(s)
    };
    let read_u32 = |d: &[u8], o: &mut usize| -> Result<u32, String> {
        if *o + 4 > d.len() {
            return Err(format!("short read u32 at {}", *o));
        }
        let v = u32::from_be_bytes([d[*o], d[*o + 1], d[*o + 2], d[*o + 3]]);
        *o += 4;
        Ok(v)
    };
    let read_u64 = |d: &[u8], o: &mut usize| -> Result<u64, String> {
        if *o + 8 > d.len() {
            return Err(format!("short read u64 at {}", *o));
        }
        let v = u64::from_be_bytes([
            d[*o],
            d[*o + 1],
            d[*o + 2],
            d[*o + 3],
            d[*o + 4],
            d[*o + 5],
            d[*o + 6],
            d[*o + 7],
        ]);
        *o += 8;
        Ok(v)
    };

    let algorithm = read_utf(d, &mut o)?;
    let option_count = read_u32(d, &mut o)?;
    let mut options = Vec::new();
    for _ in 0..option_count {
        let k = read_utf(d, &mut o)?;
        let v = read_utf(d, &mut o)?;
        options.push((k, v));
    }
    let chunk_length = read_u32(d, &mut o)?;
    let max_compressed_length = read_u32(d, &mut o)?;
    let data_length = read_u64(d, &mut o)?;
    let chunk_count = read_u32(d, &mut o)? as usize;
    let mut chunk_offsets = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        chunk_offsets.push(read_u64(d, &mut o)?);
    }

    Ok(RawCompressionInfo {
        algorithm,
        options,
        chunk_length,
        max_compressed_length,
        data_length,
        chunk_offsets,
        consumed: o,
    })
}

/// Codecs whose decode CQLite supports but for which the committed corpus carries
/// NO real Cassandra fixture. These are classified explicitly (logged), never
/// faked with synthetic reference data.
const CODECS_WITHOUT_FIXTURE: [&str; 2] = ["DeflateCompressor", "ZstdCompressor"];

/// Strict CompressionInfo.db metadata + Data.db chunk-boundary / inline-CRC parity.
#[test]
fn compression_info_strict_metadata_and_chunk_parity() {
    let tocs = all_toc_files();

    // Anchor presence in the committed manifests: how many SSTables are compressed
    // (TOC lists CompressionInfo.db). This is independent of binary fetch and lets
    // the lane fail closed if the committed references ever stop describing
    // compression.
    let compressed_tocs: Vec<&PathBuf> = tocs
        .iter()
        .filter(|t| toc_lists_compression_info(t))
        .collect();
    assert!(
        !compressed_tocs.is_empty(),
        "no committed TOC.txt manifest lists CompressionInfo.db — the compression corpus \
         vanished from the references (fail-closed, not a skip)"
    );

    // Track binary presence (any fetched CompressionInfo.db) INDEPENDENTLY of how
    // many we successfully compared, so a present-but-zero-compared run fails.
    let mut any_ci_present = false;
    let mut any_data_present = false;
    // Tracks Data.db presence INDEPENDENTLY of CompressionInfo.db, so a partial /
    // broken fetch (Data.db fetched but CompressionInfo.db missing) FAILS rather
    // than silently skipping green.
    let mut any_data_db_on_disk = false;

    let mut compared = 0usize;
    let mut skipped_absent = 0usize;

    let mut saw_lz4 = false;
    let mut saw_snappy = false;
    let mut codec_present_lz4 = false; // a fetched LZ4 CompressionInfo.db existed
    let mut codec_present_snappy = false; // a fetched Snappy CompressionInfo.db existed
    let mut saw_multi_chunk = false; // >1 chunk (final partial chunk -> EOF)
    let mut saw_options = false; // a fixture carrying option pairs
    let mut crc_validated_chunks = 0usize;

    for toc in &compressed_tocs {
        let base = base_from_toc(toc);
        let dir = base.parent().unwrap_or_else(|| Path::new("."));
        let base_name = base
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| panic!("non-UTF8 base name from {}", toc.display()));

        let ci_path = dir.join(format!("{base_name}-CompressionInfo.db"));
        let data_path = dir.join(format!("{base_name}-Data.db"));

        // Track Data.db presence independently of CompressionInfo.db so a partial
        // fetch is detectable even for fixtures whose CompressionInfo.db is missing.
        if data_path.exists() {
            any_data_db_on_disk = true;
        }

        // Local-only binaries: skip THIS fixture on absence (never on parse failure).
        if !ci_path.exists() {
            skipped_absent += 1;
            continue;
        }
        any_ci_present = true;

        let ci_bytes = std::fs::read(&ci_path)
            .unwrap_or_else(|e| panic!("read {} failed: {e}", ci_path.display()));

        // (1) CQLite decode.
        let info = CompressionInfo::parse(&ci_bytes).unwrap_or_else(|e| {
            panic!(
                "{}: CQLite failed to parse CompressionInfo.db: {e:?}",
                ci_path.display()
            )
        });

        // (2) Independent re-derivation from the raw bytes (the cross-check).
        let raw = read_raw_compression_info(&ci_bytes).unwrap_or_else(|e| {
            panic!(
                "{}: independent re-derivation of CompressionInfo.db failed: {e}",
                ci_path.display()
            )
        });

        // Compressor name + options come from the bytes (no path heuristic).
        assert_eq!(
            info.algorithm,
            raw.algorithm,
            "{}: compressor name mismatch (cqlite {:?} vs raw {:?})",
            ci_path.display(),
            info.algorithm,
            raw.algorithm,
        );
        assert_eq!(
            info.option_pairs,
            raw.options,
            "{}: option key/value pairs mismatch (cqlite {:?} vs raw {:?})",
            ci_path.display(),
            info.option_pairs,
            raw.options,
        );
        assert_eq!(
            info.chunk_length,
            raw.chunk_length,
            "{}: chunk_length mismatch (cqlite {} vs raw {})",
            ci_path.display(),
            info.chunk_length,
            raw.chunk_length,
        );
        assert_eq!(
            info.max_compressed_length,
            raw.max_compressed_length,
            "{}: max_compressed_length mismatch (cqlite {} vs raw {})",
            ci_path.display(),
            info.max_compressed_length,
            raw.max_compressed_length,
        );
        assert_eq!(
            info.data_length,
            raw.data_length,
            "{}: uncompressed data_length mismatch (cqlite {} vs raw {})",
            ci_path.display(),
            info.data_length,
            raw.data_length,
        );
        // Ordered offset vector — not just the count. Catches any reordering bug.
        assert_eq!(
            info.chunk_offsets,
            raw.chunk_offsets,
            "{}: chunk-offset table mismatch (ordered) (cqlite len {} vs raw len {})",
            ci_path.display(),
            info.chunk_offsets.len(),
            raw.chunk_offsets.len(),
        );

        // No trailing metadata CRC: the file ends exactly after the offset table.
        assert_eq!(
            raw.consumed,
            ci_bytes.len(),
            "{}: CompressionInfo.db has {} trailing byte(s) after the offset table — Cassandra \
             appends no metadata CRC here (per-chunk CRCs are inline in Data.db)",
            ci_path.display(),
            ci_bytes.len() - raw.consumed,
        );

        // Offsets strictly ascending and starting at 0 (Cassandra's first chunk).
        assert_eq!(
            info.chunk_offsets.first().copied(),
            Some(0),
            "{}: first chunk offset must be 0, got {:?}",
            ci_path.display(),
            info.chunk_offsets.first(),
        );
        for w in info.chunk_offsets.windows(2) {
            assert!(
                w[1] > w[0],
                "{}: chunk offsets not strictly ascending: {} <= {}",
                ci_path.display(),
                w[1],
                w[0],
            );
        }

        // Codec bookkeeping (authoritative — from parsed bytes).
        match info.algorithm.as_str() {
            "LZ4Compressor" => codec_present_lz4 = true,
            "SnappyCompressor" => codec_present_snappy = true,
            other if CODECS_WITHOUT_FIXTURE.contains(&other) => {
                // A real Deflate/Zstd fixture would be a corpus addition; if one
                // ever appears, exercise it rather than ignoring it.
            }
            other => panic!(
                "{}: unexpected compressor {:?} — corpus changed; extend the test's codec coverage",
                ci_path.display(),
                other
            ),
        }
        if !info.option_pairs.is_empty() {
            saw_options = true;
        }
        if info.chunk_offsets.len() > 1 {
            saw_multi_chunk = true;
        }

        // (3) Data.db chunk-boundary + inline-CRC parity. Data.db is local-only;
        // skip the chunk pass for this fixture on absence (still counts the
        // metadata comparison above).
        if data_path.exists() {
            any_data_present = true;
            let data_bytes = std::fs::read(&data_path)
                .unwrap_or_else(|e| panic!("read {} failed: {e}", data_path.display()));
            crc_validated_chunks +=
                validate_data_db_chunks(&info, &data_bytes, &data_path).chunks_validated;
            match info.algorithm.as_str() {
                "LZ4Compressor" => saw_lz4 = true,
                "SnappyCompressor" => saw_snappy = true,
                _ => {}
            }
        }

        compared += 1;
    }

    eprintln!(
        "compression_info_strict_metadata_and_chunk_parity: {compared} CompressionInfo.db \
         compared ({skipped_absent} skipped — binary absent), {crc_validated_chunks} compressed \
         chunks CRC-validated | codec_present lz4={codec_present_lz4} snappy={codec_present_snappy} \
         | data_present={any_data_present}"
    );

    // Deflate / Zstd: explicit "no fixture available" classification (never faked).
    for codec in CODECS_WITHOUT_FIXTURE {
        eprintln!(
            "compression_info_strict_metadata_and_chunk_parity: {codec} — no fixture available \
             in the corpus (only LZ4Compressor and SnappyCompressor fixtures exist); classified \
             as no-fixture, not faked with synthetic reference data (issue #986)"
        );
    }

    // Partial/broken fetch guard: if Data.db binaries are present but NOT a single
    // CompressionInfo.db is, the dataset is inconsistent (every compressed table
    // emits a CompressionInfo.db). Fail rather than skip green.
    if !any_ci_present && any_data_db_on_disk {
        panic!(
            "CompressionInfo.db binaries are absent but Data.db binaries ARE present — \
             partial/broken dataset fetch. Compressed fixtures require CompressionInfo.db; \
             re-fetch with bash test-data/scripts/fetch-datasets.sh (issue #986)."
        );
    }

    // Skip-on-total-absence: a fresh checkout has the committed TOC.txt references
    // but no fetched binaries to compare. Nothing to assert -> skip (not red).
    if !any_ci_present {
        debug_assert_eq!(
            skipped_absent,
            compressed_tocs.len(),
            "internal: no CompressionInfo.db present implies every compressed fixture was skipped"
        );
        eprintln!(
            "compression_info_strict_metadata_and_chunk_parity: SKIP — no *-CompressionInfo.db \
             binaries fetched ({} compressed TOC references present without binaries; fetch with \
             bash test-data/scripts/fetch-datasets.sh)",
            compressed_tocs.len(),
        );
        return;
    }

    // Binaries ARE present: the lane must have compared something and must have
    // exercised every codec the local corpus actually contains.
    assert!(
        compared > 0,
        "CompressionInfo.db binaries are present but zero fixtures were compared — strict \
         compression parity proved nothing"
    );
    // Metadata parity for LZ4 is proven by the comparison above; chunk-CRC parity
    // additionally requires Data.db (asserted below only when Data.db present).
    assert!(
        codec_present_lz4,
        "no LZ4Compressor CompressionInfo.db was compared despite present binaries — LZ4 metadata \
         parity unproven (the corpus contains LZ4 fixtures)"
    );
    assert!(
        codec_present_snappy,
        "no SnappyCompressor CompressionInfo.db was compared despite present binaries — Snappy \
         metadata parity unproven (the corpus contains Snappy fixtures)"
    );
    assert!(
        saw_multi_chunk,
        "no multi-chunk CompressionInfo.db was compared — final partial-chunk / chunk-boundary \
         handling unproven (the corpus contains multi-chunk fixtures)"
    );

    // When Data.db is also present, the inline-CRC chunk gate must actually have run
    // for both real codecs (proves the per-chunk CRC trailer parity, not just metadata).
    if any_data_present {
        assert!(
            crc_validated_chunks > 0,
            "Data.db binaries are present but zero compressed chunks were CRC-validated — \
             inline-CRC chunk parity proved nothing"
        );
        assert!(
            saw_lz4,
            "no LZ4 Data.db chunks were CRC-validated despite present Data.db — LZ4 inline-CRC \
             parity unproven"
        );
        assert!(
            saw_snappy,
            "no Snappy Data.db chunks were CRC-validated despite present Data.db — Snappy \
             inline-CRC parity unproven"
        );
    }

    // The committed corpus carries fixtures with compression options on some tables;
    // if none were seen the option-pair decode path went unexercised. This is a soft
    // coverage note (some corpora use all-default options), so log rather than fail.
    if !saw_options {
        eprintln!(
            "compression_info_strict_metadata_and_chunk_parity: note — no fixture carried \
             compression option pairs (all default options); option-pair decode covered by unit \
             tests in compression_info.rs"
        );
    }
}

/// Result of validating every compressed chunk record in a `Data.db`.
struct ChunkValidation {
    chunks_validated: usize,
}

/// Walk every compressed chunk record in `Data.db` using the offset table, verify
/// the record fits within the file (final record runs to EOF — the partial chunk),
/// and recompute + compare the trailing 4-byte big-endian inline CRC32 over the
/// compressed payload. Fails closed on truncation, offset inconsistency, or CRC
/// mismatch. Mirrors `CompressedSequentialWriter.java` (payload + 4-byte CRC).
fn validate_data_db_chunks(
    info: &CompressionInfo,
    data: &[u8],
    data_path: &Path,
) -> ChunkValidation {
    let file_len = data.len() as u64;
    let n = info.chunk_offsets.len();
    let mut validated = 0usize;

    for i in 0..n {
        let start = info.chunk_offsets[i];
        // Record end is the next offset, or EOF for the final (partial) chunk.
        let end = if i + 1 < n {
            info.chunk_offsets[i + 1]
        } else {
            file_len
        };
        assert!(
            end > start,
            "{}: chunk {i} record end {end} <= start {start} — offset inconsistency",
            data_path.display(),
        );
        let record_size = end - start;
        assert!(
            record_size >= 4,
            "{}: chunk {i} record size {record_size} < 4 — too small to hold the inline CRC \
             trailer (truncation)",
            data_path.display(),
        );
        assert!(
            end <= file_len,
            "{}: chunk {i} record end {end} past Data.db EOF {file_len} — truncation / offset \
             inconsistency",
            data_path.display(),
        );

        let start = start as usize;
        let end = end as usize;
        let payload = &data[start..end - 4];
        let crc_bytes = [data[end - 4], data[end - 3], data[end - 2], data[end - 1]];
        let stored_crc = u32::from_be_bytes(crc_bytes);
        let computed_crc = crc32fast::hash(payload);
        assert_eq!(
            stored_crc,
            computed_crc,
            "{}: chunk {i} inline CRC32 mismatch at record [0x{:x}..0x{:x}): stored=0x{:08x} \
             computed=0x{:08x} (payload {} bytes) — corrupt or truncated compressed chunk",
            data_path.display(),
            start,
            end,
            stored_crc,
            computed_crc,
            payload.len(),
        );
        validated += 1;
    }

    ChunkValidation {
        chunks_validated: validated,
    }
}

/// Structurally-invalid `CompressionInfo.db` and corrupted compressed-chunk
/// fixtures fail closed in strict mode — never silently accepted. Uses fetched
/// binaries as the clean baseline (skips when none is fetched), then mutates copies
/// in memory.
///
/// Scope note: this lane asserts that *truncation* and *length-prefix corruption*
/// are rejected (cases a–c) and that chunk-payload / CRC corruption is rejected
/// (cases d–f). It does NOT assert rejection of bytes appended *after* a
/// structurally complete record: `CompressionInfo::parse` does not verify EOF, so
/// trailing garbage is tolerated (and ignored). Case (c2) documents that tolerant
/// behavior explicitly. Making parse reject trailing bytes is a production behavior
/// change deferred to a separate issue.
#[test]
fn compression_info_strict_corruption_fails_closed() {
    let tocs = all_toc_files();

    // Find a fetched CompressionInfo.db (+ its Data.db when present) to corrupt.
    let mut ci_path: Option<PathBuf> = None;
    let mut data_path: Option<PathBuf> = None;
    for toc in &tocs {
        if !toc_lists_compression_info(toc) {
            continue;
        }
        let base = base_from_toc(toc);
        let dir = base.parent().unwrap_or_else(|| Path::new("."));
        let base_name = match base.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        let ci = dir.join(format!("{base_name}-CompressionInfo.db"));
        if ci.exists() {
            let dd = dir.join(format!("{base_name}-Data.db"));
            ci_path = Some(ci);
            if dd.exists() {
                data_path = Some(dd);
            }
            // Prefer a fixture that also has Data.db so the chunk-corruption case runs.
            if data_path.is_some() {
                break;
            }
        }
    }

    let Some(ci_path) = ci_path else {
        eprintln!(
            "compression_info_strict_corruption_fails_closed: SKIP — no *-CompressionInfo.db \
             binary fetched"
        );
        return;
    };

    let clean_ci = std::fs::read(&ci_path)
        .unwrap_or_else(|e| panic!("read {} failed: {e}", ci_path.display()));

    // Baseline: the clean fixture parses.
    let info = CompressionInfo::parse(&clean_ci)
        .unwrap_or_else(|e| panic!("clean {} should parse: {e:?}", ci_path.display()));

    // (a) Corrupt the compressor-name length prefix (bytes 0..2) to an absurd value
    //     -> UTF read overruns the buffer -> parse rejects it.
    {
        let mut corrupt = clean_ci.clone();
        corrupt[0] = 0x7f;
        corrupt[1] = 0xff;
        assert!(
            CompressionInfo::parse(&corrupt).is_err(),
            "parse accepted a CompressionInfo.db with a corrupted compressor-name length — must \
             fail closed"
        );
    }

    // (b) Truncate below the fixed header -> parse rejects it.
    {
        let truncated = &clean_ci[..clean_ci.len().min(6)];
        assert!(
            CompressionInfo::parse(truncated).is_err(),
            "parse accepted a truncated CompressionInfo.db (no chunk table) — must fail closed"
        );
    }

    // (c) Drop the last 8 bytes (one chunk offset) while leaving chunk_count
    //     unchanged -> the final offset read overruns -> parse rejects it.
    if clean_ci.len() > 8 {
        let truncated = &clean_ci[..clean_ci.len() - 8];
        assert!(
            CompressionInfo::parse(truncated).is_err(),
            "parse accepted a CompressionInfo.db missing a declared chunk offset — must fail closed"
        );
    }

    // (c2) Trailing garbage appended after a structurally complete record.
    //      Cassandra never writes trailing bytes after the chunk-offset table, so
    //      `CompressionInfo::parse` does NOT verify EOF: it reads exactly the
    //      declared fixed header + option pairs + chunk-offset table and ignores
    //      anything after it. This case documents that tolerant behavior explicitly
    //      (parse still succeeds and yields the same metadata) rather than asserting
    //      a rejection the parser does not perform. Tightening parse to reject
    //      trailing bytes would be a production behavior change and is deferred to a
    //      separate issue — it is intentionally NOT covered by this strict-fail-closed
    //      lane.
    {
        let mut with_trailer = clean_ci.clone();
        with_trailer.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        let reparsed = CompressionInfo::parse(&with_trailer).unwrap_or_else(|e| {
            panic!(
                "parse rejected valid CompressionInfo.db with appended trailing bytes; \
                 the parser does not (and is not asserted to) verify EOF: {e:?}"
            )
        });
        assert_eq!(
            reparsed.chunk_offsets, info.chunk_offsets,
            "appended trailing bytes changed the decoded chunk-offset table — trailing data must \
             be ignored, never misinterpreted as chunk metadata"
        );
        assert_eq!(
            reparsed.option_pairs, info.option_pairs,
            "appended trailing bytes changed the decoded compression options"
        );
    }

    // Chunk-level corruption requires a fetched Data.db.
    let Some(data_path) = data_path else {
        eprintln!(
            "compression_info_strict_corruption_fails_closed: CompressionInfo.db corruption \
             rejected against {} (Data.db absent — chunk-corruption case skipped)",
            ci_path.display()
        );
        return;
    };

    let clean_data = std::fs::read(&data_path)
        .unwrap_or_else(|e| panic!("read {} failed: {e}", data_path.display()));

    // Sanity: the clean Data.db passes the strict chunk-CRC gate.
    let baseline = validate_data_db_chunks(&info, &clean_data, &data_path);
    assert!(
        baseline.chunks_validated > 0,
        "{}: clean Data.db validated zero chunks — corruption baseline is meaningless",
        data_path.display()
    );

    // (d) Flip a byte inside the first compressed chunk's payload -> inline CRC must
    //     reject it (the stored CRC no longer matches the recomputed CRC).
    {
        let mut corrupt = clean_data.clone();
        let first_payload_byte = info.chunk_offsets.first().copied().unwrap_or(0) as usize;
        assert!(
            first_payload_byte < corrupt.len(),
            "{}: first chunk offset past EOF — cannot corrupt",
            data_path.display()
        );
        corrupt[first_payload_byte] ^= 0xff;
        let result = std::panic::catch_unwind(|| {
            validate_data_db_chunks(&info, &corrupt, Path::new("corrupt-payload"))
        });
        assert!(
            result.is_err(),
            "strict chunk gate accepted a Data.db with a corrupted compressed-chunk payload — \
             must fail closed on CRC mismatch"
        );
    }

    // (e) Flip a byte inside the first chunk's inline CRC trailer -> reject it.
    {
        let mut corrupt = clean_data.clone();
        // End of the first chunk record = second offset, or EOF for a single chunk.
        let first_end = if info.chunk_offsets.len() > 1 {
            info.chunk_offsets[1] as usize
        } else {
            corrupt.len()
        };
        assert!(
            first_end >= 4 && first_end <= corrupt.len(),
            "{}: cannot locate first chunk CRC trailer",
            data_path.display()
        );
        corrupt[first_end - 1] ^= 0xff; // last byte of the 4-byte CRC
        let result = std::panic::catch_unwind(|| {
            validate_data_db_chunks(&info, &corrupt, Path::new("corrupt-crc"))
        });
        assert!(
            result.is_err(),
            "strict chunk gate accepted a Data.db with a corrupted inline CRC trailer — must fail \
             closed"
        );
    }

    // (f) Truncate the final record below its 4-byte CRC trailer -> reject it.
    {
        let last_start = *info.chunk_offsets.last().unwrap_or(&0) as usize;
        // Cut to 3 bytes past the last record start so the final record < 4 bytes.
        let cut = (last_start + 3).min(clean_data.len());
        if cut > last_start && cut < clean_data.len() {
            let truncated = &clean_data[..cut];
            let result = std::panic::catch_unwind(|| {
                validate_data_db_chunks(&info, truncated, Path::new("truncated-data"))
            });
            assert!(
                result.is_err(),
                "strict chunk gate accepted a Data.db whose final record is truncated below its \
                 inline CRC trailer — must fail closed"
            );
        }
    }

    eprintln!(
        "compression_info_strict_corruption_fails_closed: CompressionInfo.db + chunk corruption \
         rejected against {} / {}",
        ci_path.display(),
        data_path.display()
    );
}
