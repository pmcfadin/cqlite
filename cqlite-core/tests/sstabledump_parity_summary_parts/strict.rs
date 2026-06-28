//! Strict Summary.db byte + offset parity (Epic #968 / issue #984).
//!
//! Extracted from `sstabledump_parity_summary.rs` (issue #1024 file-size split,
//! epic #1135). Included as a child module via
//! `#[path = "sstabledump_parity_summary_strict.rs"] mod strict;` so `use super::*`
//! resolves against the parent test module exactly as the inline module did.
//!
//! Proves *byte-for-byte* parity against the on-disk `Summary.db` images Cassandra
//! 5.0 wrote (header fields, LE offset table, per-entry keys/positions, trailing
//! first/last keys, malformation rejection, and BTI Summary.db-absence
//! classification). Fails closed on any discrepancy; each fixture is skipped only
//! when its own Data.db is absent (and panics in CI when datasets are mandated).

use super::*;
use cqlite_core::storage::sstable::directory::{parse_toc_file_detailed, SSTableComponent};
use cqlite_core::storage::sstable::summary_reader::SummaryHeader;
use cqlite_core::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};

/// Raw, independent re-decode of a `Summary.db` image used to cross-check the
/// production `SummaryReader`. Implemented from first principles (no shared
/// parser code) so a bug in either side is caught by the byte comparison.
struct RawSummary {
    header: SummaryHeader,
    /// Offsets exactly as stored (little-endian u32), unmodified.
    raw_offsets: Vec<u32>,
    /// Per-entry decoded data reconstructed from the offset table.
    entries: Vec<RawEntry>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
}

/// One summary entry decoded from raw bytes. Cassandra stores the trailing
/// Index.db position as a **little-endian** u64 (verified byte-for-byte:
/// the LE value lands exactly on the matching `Index.db` partition entry,
/// while the big-endian interpretation produces an out-of-range offset).
/// The authoritative LE offset is retained and asserted against `Index.db`;
/// as of issue #1054 the production `SummaryReader` returns this field
/// little-endian too, so the entry-parity assertion checks the reader's
/// returned position byte-for-byte against this value.
struct RawEntry {
    key: Vec<u8>,
    /// Authoritative Index.db offset (little-endian, on-disk truth).
    position_le: u64,
}

/// Decode a `Summary.db` buffer from scratch. Returns an explicit error on
/// any truncation or offset inconsistency (used both for parity and for the
/// malformation tests below).
fn decode_raw_summary(buf: &[u8]) -> std::result::Result<RawSummary, String> {
    const HEADER_LEN: usize = 24;
    if buf.len() < HEADER_LEN {
        return Err(format!(
            "truncated header: {} bytes < {HEADER_LEN}",
            buf.len()
        ));
    }
    let be_u32 = |o: usize| u32::from_be_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);
    let be_u64 = |o: usize| {
        u64::from_be_bytes([
            buf[o],
            buf[o + 1],
            buf[o + 2],
            buf[o + 3],
            buf[o + 4],
            buf[o + 5],
            buf[o + 6],
            buf[o + 7],
        ])
    };
    let header = SummaryHeader {
        min_index_interval: be_u32(0),
        entries_count: be_u32(4),
        summary_entries_size: be_u64(8),
        sampling_level: be_u32(16),
        size_at_full_sampling: be_u32(20),
    };

    let entries_count = header.entries_count as usize;
    let offset_table_len = entries_count
        .checked_mul(4)
        .ok_or_else(|| "offset table size overflow".to_string())?;
    let summary_block = header.summary_entries_size as usize;
    if summary_block < offset_table_len {
        return Err(format!(
            "summary_entries_size {summary_block} < offset table length {offset_table_len}"
        ));
    }
    let summary_start = HEADER_LEN;
    let summary_end = summary_start
        .checked_add(summary_block)
        .ok_or_else(|| "summary block end overflow".to_string())?;
    if buf.len() < summary_end {
        return Err(format!(
            "truncated summary block: need {summary_end} bytes, have {}",
            buf.len()
        ));
    }

    // Little-endian offset table.
    let mut raw_offsets = Vec::with_capacity(entries_count);
    for i in 0..entries_count {
        let o = summary_start + i * 4;
        raw_offsets.push(u32::from_le_bytes([
            buf[o],
            buf[o + 1],
            buf[o + 2],
            buf[o + 3],
        ]));
    }

    // Cassandra stores absolute offsets into the summary block (offset 0 ==
    // start of the offset table). Normalize to entry-data-relative offsets.
    let entry_data = &buf[summary_start + offset_table_len..summary_end];
    let mut norm: Vec<usize> = Vec::with_capacity(entries_count);
    for (i, &off) in raw_offsets.iter().enumerate() {
        let off = off as usize;
        if off < offset_table_len {
            return Err(format!(
                "offset[{i}] = {off} falls inside the offset table (len {offset_table_len})"
            ));
        }
        if off > summary_block {
            return Err(format!(
                "offset[{i}] = {off} exceeds summary block {summary_block}"
            ));
        }
        norm.push(off - offset_table_len);
    }

    let mut entries = Vec::with_capacity(entries_count);
    for i in 0..entries_count {
        let start = norm[i];
        let end = if i + 1 < entries_count {
            norm[i + 1]
        } else {
            entry_data.len()
        };
        if start >= end {
            return Err(format!("offset[{i}] start {start} >= end {end}"));
        }
        if end > entry_data.len() {
            return Err(format!(
                "offset[{i}] end {end} exceeds entry data len {}",
                entry_data.len()
            ));
        }
        let slice = &entry_data[start..end];
        if slice.len() < 8 {
            return Err(format!("entry {i} too small for 8-byte position"));
        }
        let key_len = slice.len() - 8;
        let key = slice[..key_len].to_vec();
        let pos_bytes: [u8; 8] = [
            slice[key_len],
            slice[key_len + 1],
            slice[key_len + 2],
            slice[key_len + 3],
            slice[key_len + 4],
            slice[key_len + 5],
            slice[key_len + 6],
            slice[key_len + 7],
        ];
        entries.push(RawEntry {
            key,
            position_le: u64::from_le_bytes(pos_bytes),
        });
    }

    // Trailing first/last keys: be_u32 length prefix + bytes.
    let read_key = |start: usize| -> std::result::Result<(Vec<u8>, usize), String> {
        if buf.len() < start + 4 {
            return Err("truncated key length prefix".to_string());
        }
        let len = u32::from_be_bytes([buf[start], buf[start + 1], buf[start + 2], buf[start + 3]])
            as usize;
        let key_start = start + 4;
        let key_end = key_start
            .checked_add(len)
            .ok_or_else(|| "key length overflow".to_string())?;
        if buf.len() < key_end {
            return Err(format!(
                "truncated key body: need {key_end} bytes, have {}",
                buf.len()
            ));
        }
        Ok((buf[key_start..key_end].to_vec(), key_end))
    };
    let (first_key, after_first) = read_key(summary_end)?;
    let (last_key, _after_last) = read_key(after_first)?;

    Ok(RawSummary {
        header,
        raw_offsets,
        entries,
        first_key,
        last_key,
    })
}

/// Datasets root (`CQLITE_DATASETS_ROOT` override, else workspace tree).
fn datasets_sstables_root() -> PathBuf {
    let root = std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(|ws| ws.join("test-data/datasets"))
                .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
        });
    root.join("sstables")
}

fn collect_by_suffix(dir: &Path, suffix: &str, out: &mut Vec<PathBuf>) {
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
            collect_by_suffix(&path, suffix, out);
        } else if name.ends_with(suffix) {
            out.push(path);
        }
    }
}

/// Strict byte + offset parity for every committed BIG `Summary.db` whose
/// sibling `Data.db` is present. Fails closed on any discrepancy.
#[tokio::test]
async fn test_summary_db_strict_byte_parity() -> CqliteResult<()> {
    let root = datasets_sstables_root();
    let mut summaries = Vec::new();
    collect_by_suffix(&root, "-Summary.db", &mut summaries);
    summaries.sort();

    if summaries.is_empty() {
        // Binary fixtures absent in this checkout (only JSONL refs committed).
        // Skip-on-absence per project doctrine; do NOT pass silently with 0 work.
        if parity_datasets_required() {
            ParityFailure::new(scenario::SUMMARY_DB_BIG)
                .lane("summary_db_big")
                .cassandra_source("IndexSummaryTest (Summary.db byte/offset parity)")
                .fixture(root.clone())
                .components(["Summary.db", "Data.db", "Index.db"])
                .repro(
                    "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_summary \
                         strict::test_summary_db_strict_byte_parity -- --nocapture",
                )
                .detail(
                    "CQLITE_PARITY_REQUIRE_DATASETS=1 but no *-Summary.db images were present \
                         — required parity gate must not skip when datasets are mandated",
                )
                .panic();
        }
        eprintln!(
            "skip: no *-Summary.db images under {} (binary fixtures not fetched)",
            root.display()
        );
        return Ok(());
    }

    let platform = Arc::new(Platform::new(&Config::default()).await?);
    let mut validated = 0usize;
    // Count Summary.db images that actually have their sibling Data.db on
    // disk. This distinguishes "binaries unfetched → nothing to validate"
    // (skip) from "fixtures present but none validated" (fail-closed).
    let mut with_data_db = 0usize;

    for summary_path in &summaries {
        let prefix = summary_path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix("-Summary.db"))
            .ok_or_else(|| Error::corruption("bad Summary.db name"))?;
        let dir = summary_path
            .parent()
            .ok_or_else(|| Error::corruption("Summary.db has no parent dir"))?;
        let data_path = dir.join(format!("{prefix}-Data.db"));
        let index_path = dir.join(format!("{prefix}-Index.db"));

        // Skip-on-absence: only validate fixtures whose Data.db is present.
        if !data_path.exists() {
            continue;
        }
        with_data_db += 1;

        // BIG-only: a Summary.db must come from a BIG descriptor.
        let descriptor = SsTableDescriptor::parse(summary_path).map_err(|e| {
            Error::corruption(format!("descriptor parse {}: {e}", summary_path.display()))
        })?;
        assert_eq!(
            descriptor.format,
            SsTableFormat::Big,
            "{}: Summary.db must belong to a BIG SSTable, got {:?}",
            summary_path.display(),
            descriptor.format
        );

        let buf = std::fs::read(summary_path)
            .map_err(|e| Error::corruption(format!("read {}: {e}", summary_path.display())))?;
        let raw = decode_raw_summary(&buf).map_err(|e| {
            Error::corruption(format!(
                "strict decode of {} failed: {e}",
                summary_path.display()
            ))
        })?;

        // ---- Header field parity vs SummaryReader ----
        let reader = SummaryReader::open(summary_path, platform.clone()).await?;
        let header = reader.get_header();
        assert_eq!(
            header.min_index_interval,
            raw.header.min_index_interval,
            "{}: min_index_interval",
            summary_path.display()
        );
        assert_eq!(
            header.entries_count,
            raw.header.entries_count,
            "{}: entries_count",
            summary_path.display()
        );
        assert_eq!(
            header.summary_entries_size,
            raw.header.summary_entries_size,
            "{}: summary_entries_size",
            summary_path.display()
        );
        assert_eq!(
            header.sampling_level,
            raw.header.sampling_level,
            "{}: sampling_level",
            summary_path.display()
        );
        assert_eq!(
            header.size_at_full_sampling,
            raw.header.size_at_full_sampling,
            "{}: size_at_full_sampling",
            summary_path.display()
        );

        // Sampling metadata sanity (authoritative, not heuristic).
        assert!(
            header.min_index_interval > 0,
            "{}: min_index_interval must be > 0",
            summary_path.display()
        );
        assert!(
            header.sampling_level > 0 && header.sampling_level <= 128,
            "{}: sampling_level {} out of (0,128]",
            summary_path.display(),
            header.sampling_level
        );

        // ---- Offset table parity ----
        assert_eq!(
            raw.raw_offsets.len(),
            header.entries_count as usize,
            "{}: offset table length != entries_count",
            summary_path.display()
        );
        let offset_table_len = header.entries_count as usize * 4;
        assert_eq!(
            raw.raw_offsets.first().copied(),
            Some(offset_table_len as u32),
            "{}: first offset must equal offset-table byte length (absolute layout)",
            summary_path.display()
        );
        for w in raw.raw_offsets.windows(2) {
            assert!(
                w[1] > w[0],
                "{}: offset table not strictly increasing: {} then {}",
                summary_path.display(),
                w[0],
                w[1]
            );
        }
        for (i, &off) in raw.raw_offsets.iter().enumerate() {
            assert!(
                (off as u64) < header.summary_entries_size,
                "{}: offset[{i}] = {off} >= summary_entries_size {}",
                summary_path.display(),
                header.summary_entries_size
            );
        }

        // ---- Entry byte parity (keys + Index.db positions) ----
        let entries = reader.get_entries();
        assert_eq!(
            entries.len(),
            raw.entries.len(),
            "{}: entry count mismatch reader={} raw={}",
            summary_path.display(),
            entries.len(),
            raw.entries.len()
        );
        for (i, (entry, raw_entry)) in entries.iter().zip(raw.entries.iter()).enumerate() {
            // Key bytes must byte-match between the production reader and the
            // independent raw decode.
            assert_eq!(
                &entry.partition_key,
                &raw_entry.key,
                "{}: entry[{i}] key bytes mismatch",
                summary_path.display()
            );
            // The on-disk truth is the little-endian position, proven below
            // by resolving it against Index.db. As of issue #1054 the
            // production `SummaryReader` decodes this field little-endian, so
            // its returned position must byte-match the raw LE on-disk value.
            assert_eq!(
                entry.position,
                raw_entry.position_le,
                "{}: entry[{i}] position mismatch reader={} raw_le={} (issue #1054)",
                summary_path.display(),
                entry.position,
                raw_entry.position_le
            );
        }

        // Entry ordering: keys are stored in ascending offset order, so the
        // offset table (already asserted strictly increasing) defines order.
        // The authoritative Index.db positions (little-endian) are also
        // non-decreasing across samples; first sample points at offset 0.
        for w in raw.entries.windows(2) {
            assert!(
                w[1].position_le >= w[0].position_le,
                "{}: Index.db positions not monotonic ({} then {})",
                summary_path.display(),
                w[0].position_le,
                w[1].position_le
            );
        }
        assert_eq!(
            raw.entries[0].position_le,
            0,
            "{}: first summary sample must point at Index.db offset 0",
            summary_path.display()
        );

        // ---- First/last key byte parity ----
        assert_eq!(
            reader.get_first_key(),
            raw.first_key.as_slice(),
            "{}: first_key bytes mismatch",
            summary_path.display()
        );
        assert_eq!(
            reader.get_last_key(),
            raw.last_key.as_slice(),
            "{}: last_key bytes mismatch",
            summary_path.display()
        );
        // First sampled entry key is the SSTable's first decorated key.
        assert_eq!(
            &raw.entries[0].key,
            &raw.first_key,
            "{}: first summary entry key must equal SSTable first key",
            summary_path.display()
        );

        // ---- Index.db offset references are valid (authoritative LE) ----
        // Each sampled position must be a real byte offset inside Index.db,
        // and the entry that lives at that offset must carry the exact same
        // partition key the summary recorded — proving the Index.db
        // reference is byte-correct, not merely in-bounds.
        assert!(
            index_path.exists(),
            "{}: BIG SSTable missing sibling Index.db",
            summary_path.display()
        );
        let index_bytes = std::fs::read(&index_path)
            .map_err(|e| Error::corruption(format!("read Index.db: {e}")))?;
        let index_len = index_bytes.len() as u64;
        for (i, raw_entry) in raw.entries.iter().enumerate() {
            assert!(
                raw_entry.position_le < index_len,
                "{}: entry[{i}] Index.db position {} >= Index.db size {}",
                summary_path.display(),
                raw_entry.position_le,
                index_len
            );
            // Index.db partition entry = be16 key length + key bytes.
            let off = raw_entry.position_le as usize;
            assert!(
                off + 2 <= index_bytes.len(),
                "{}: entry[{i}] Index.db offset {off} has no room for key length",
                summary_path.display()
            );
            let idx_key_len = u16::from_be_bytes([index_bytes[off], index_bytes[off + 1]]) as usize;
            let key_start = off + 2;
            let key_end = key_start + idx_key_len;
            assert!(
                key_end <= index_bytes.len(),
                "{}: entry[{i}] Index.db key at {off} runs past EOF",
                summary_path.display()
            );
            assert_eq!(
                &index_bytes[key_start..key_end],
                raw_entry.key.as_slice(),
                "{}: entry[{i}] Index.db key at offset {off} does not match summary key",
                summary_path.display()
            );
        }

        validated += 1;
        println!(
            "strict OK {} ({} entries, mii={}, sampling={}, first_key_len={})",
            summary_path.display(),
            entries.len(),
            header.min_index_interval,
            header.sampling_level,
            raw.first_key.len(),
        );
    }

    if validated == 0 {
        // Distinguish "nothing fetched to validate" (clean skip) from
        // "fixtures present but none validated" (fail-closed).
        if with_data_db == 0 {
            // Summary.db images were discovered, but NONE had a sibling
            // Data.db on disk — the binary fixtures simply were not fetched
            // in this checkout. There is nothing to validate, so skip
            // cleanly; do NOT claim a fail-closed pass.
            if parity_datasets_required() {
                ParityFailure::new(scenario::SUMMARY_DB_BIG)
                        .lane("summary_db_big")
                        .cassandra_source("IndexSummaryTest (Summary.db byte/offset parity)")
                        .fixture(root.clone())
                        .components(["Summary.db", "Data.db", "Index.db"])
                        .repro(
                            "bash test-data/scripts/fetch-datasets.sh && \
                             CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                             --features write-support --test sstabledump_parity_summary \
                             strict::test_summary_db_strict_byte_parity -- --nocapture",
                        )
                        .detail(format!(
                            "CQLITE_PARITY_REQUIRE_DATASETS=1 but {} Summary.db image(s) had no \
                             sibling Data.db (binaries unfetched) — required parity gate must not \
                             skip when datasets are mandated",
                            summaries.len()
                        ))
                        .panic();
            }
            eprintln!(
                "skip: {} Summary.db image(s) found under {} but none had a sibling Data.db \
                     (binary fixtures not fetched)",
                summaries.len(),
                root.display()
            );
            return Ok(());
        }
        // Summary.db images WERE discovered WITH their sibling Data.db
        // present, yet none reached `validated += 1`. That is a real
        // regression in the lane (every present fixture should validate),
        // not an absent-binary skip — fail closed.
        panic!(
            "{with_data_db} Summary.db image(s) under {} had a sibling Data.db present but \
                 none were validated — strict parity lane proved nothing",
            root.display()
        );
    }

    println!("strict Summary.db byte parity validated {validated} BIG SSTable(s)");
    let _ = write_summary(
        "summary_db_big",
        LaneStatus::Pass,
        scenario::SUMMARY_DB_BIG,
        &[],
    );
    Ok(())
}

/// Truncated / malformed / offset-inconsistent Summary.db images must
/// produce explicit errors, never a panic and never a silent success.
#[test]
fn test_summary_db_malformation_detection() {
    // A minimal valid single-entry image to mutate.
    // Header(24) + offset table(4) + entry(16-byte key + 8-byte pos) +
    // first key(4+16) + last key(4+16).
    let mut img: Vec<u8> = Vec::new();
    img.extend_from_slice(&128u32.to_be_bytes()); // min_index_interval
    img.extend_from_slice(&1u32.to_be_bytes()); // entries_count
    img.extend_from_slice(&28u64.to_be_bytes()); // summary_entries_size = 4 + 24
    img.extend_from_slice(&128u32.to_be_bytes()); // sampling_level
    img.extend_from_slice(&1u32.to_be_bytes()); // size_at_full_sampling
    img.extend_from_slice(&4u32.to_le_bytes()); // offset[0] = 4 (absolute)
    let key = [0x22u8; 16];
    img.extend_from_slice(&key); // entry key
    img.extend_from_slice(&0u64.to_be_bytes()); // entry position
    img.extend_from_slice(&16u32.to_be_bytes()); // first key len
    img.extend_from_slice(&key);
    img.extend_from_slice(&16u32.to_be_bytes()); // last key len
    img.extend_from_slice(&key);

    // Baseline image decodes cleanly.
    assert!(
        decode_raw_summary(&img).is_ok(),
        "baseline image should decode"
    );

    // 1. Truncated header.
    assert!(
        decode_raw_summary(&img[..10]).is_err(),
        "truncated header must error"
    );

    // 2. Truncated summary block (chop the entry data).
    assert!(
        decode_raw_summary(&img[..30]).is_err(),
        "truncated summary block must error"
    );

    // 3. Truncated trailing keys (drop the last key bytes).
    let no_last_key = &img[..img.len() - 20];
    assert!(
        decode_raw_summary(no_last_key).is_err(),
        "truncated trailing keys must error"
    );

    // 4. Offset pointing inside the offset table (inconsistent).
    let mut bad_off = img.clone();
    bad_off[24] = 0x00; // offset[0] LE -> 0, which is inside the table
    assert!(
        decode_raw_summary(&bad_off).is_err(),
        "offset inside offset table must error"
    );

    // 5. summary_entries_size smaller than the offset table.
    let mut bad_size = img.clone();
    bad_size[8..16].copy_from_slice(&2u64.to_be_bytes());
    assert!(
        decode_raw_summary(&bad_size).is_err(),
        "summary_entries_size < offset table must error"
    );
}

/// BTI (`da`) SSTables are classified separately: they MUST NOT carry a
/// `Summary.db` component (the trie `Partitions.db` replaces it). Proven via
/// the authoritative `TOC.txt` manifest plus the on-disk component set.
#[test]
fn test_bti_summary_discovery_classification() {
    let root = datasets_sstables_root();
    let mut tocs = Vec::new();
    collect_by_suffix(&root, "-TOC.txt", &mut tocs);
    tocs.sort();

    if tocs.is_empty() {
        if parity_datasets_required() {
            ParityFailure::new(scenario::COMPONENT_MANIFEST)
                .lane("component_manifest")
                .cassandra_source("BTI/BIG TOC manifest classification (Summary.db presence)")
                .fixture(root.clone())
                .components(["TOC.txt", "Summary.db", "Partitions.db"])
                .repro(
                    "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_summary \
                         strict::test_bti_summary_discovery_classification -- --nocapture",
                )
                .detail(
                    "CQLITE_PARITY_REQUIRE_DATASETS=1 but no *-TOC.txt fixtures were present — \
                         required parity gate must not skip when datasets are mandated",
                )
                .panic();
        }
        eprintln!(
            "skip: no *-TOC.txt fixtures under {} (datasets not present)",
            root.display()
        );
        return;
    }

    let mut big_with_summary = 0usize;
    let mut bti_without_summary = 0usize;

    for toc in &tocs {
        let descriptor = SsTableDescriptor::parse(toc)
            .unwrap_or_else(|e| panic!("descriptor parse {}: {e}", toc.display()));
        let (components, unknown) = parse_toc_file_detailed(toc)
            .unwrap_or_else(|e| panic!("parse {} failed: {e}", toc.display()));
        assert!(
            unknown.is_empty(),
            "{}: unrecognized component(s) {:?}",
            toc.display(),
            unknown
        );
        let has_summary = components.contains(&SSTableComponent::Summary);
        let has_index = components.contains(&SSTableComponent::Index);
        let has_partitions = components.contains(&SSTableComponent::Partitions);

        match descriptor.format {
            SsTableFormat::Big => {
                assert!(
                    has_summary,
                    "{}: BIG SSTable must declare Summary.db",
                    toc.display()
                );
                assert!(
                    has_index,
                    "{}: BIG SSTable must declare Index.db",
                    toc.display()
                );
                assert!(
                    !has_partitions,
                    "{}: BIG SSTable must not declare BTI Partitions.db",
                    toc.display()
                );
                big_with_summary += 1;
            }
            SsTableFormat::Bti => {
                assert!(
                    !has_summary,
                    "{}: BTI SSTable must NOT declare Summary.db (trie Partitions.db replaces it)",
                    toc.display()
                );
                assert!(
                    has_partitions,
                    "{}: BTI SSTable must declare Partitions.db",
                    toc.display()
                );
                // The Summary.db image must also be physically absent.
                let dir = toc.parent().expect("TOC has parent");
                let prefix = toc
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_suffix("-TOC.txt"))
                    .expect("bad TOC name");
                let summary_image = dir.join(format!("{prefix}-Summary.db"));
                assert!(
                    !summary_image.exists(),
                    "{}: BTI SSTable has an unexpected Summary.db image",
                    summary_image.display()
                );
                bti_without_summary += 1;
            }
        }
    }

    println!(
        "BTI/BIG summary classification: {big_with_summary} BIG(+Summary), \
             {bti_without_summary} BTI(no Summary)"
    );
    // Fail closed: a checkout with TOCs but neither BIG nor BTI classified
    // means the discovery path is broken.
    assert!(
        big_with_summary + bti_without_summary > 0,
        "no SSTables classified from {} TOC fixture(s)",
        tocs.len()
    );
}
