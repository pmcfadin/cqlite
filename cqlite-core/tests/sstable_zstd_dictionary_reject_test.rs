//! Zstd **dictionary**-compressed SSTable fail-closed rejection (issue #1399).
//!
//! The parity manifest marks six `cass.zstd_dictionary.*` scenarios
//! `out_of_scope` under `unsupported_compression_dictionary`. CQLite ships plain
//! (no-dictionary) Zstd; "unsupported" must mean the reader **fails closed** on
//! dictionary-compressed input — a typed rejection — and must NEVER return rows,
//! garble bytes, silently produce wrong values, or partially decode. This suite
//! is the boundary-enforcement evidence those manifest entries reference.
//!
//! ## Why there is no stock-Cassandra fixture
//!
//! Apache Cassandra 5.x `org.apache.cassandra.io.compress.ZstdCompressor`
//! (CASSANDRA-14482) exposes only a `compression_level` option; it does **not**
//! train, store, or apply a Zstd dictionary. No stock Apache Cassandra release
//! can flush a dictionary-compressed SSTable, so a real end-to-end fixture must
//! be *commissioned* (see the fixture-commission issue referenced below and
//! `test-data/scripts/generate-zstd-dictionary-fixture.md`). The two
//! reader/verify oracle tests below are therefore fixture-gated: they SKIP in a
//! clean checkout and hard-FAIL under `CQLITE_REQUIRE_FIXTURES=1` when the
//! commissioned fixture is absent (repo fixture-gating doctrine, issue #1094).
//!
//! ## What runs unconditionally
//!
//! `zstd_dictionary_frame_rejected_fail_closed` needs no Cassandra fixture: it
//! trains a *real* Zstd dictionary and compresses a chunk with it (a genuine
//! dictionary-ID-bearing frame, exactly what a dictionary-compressed Cassandra
//! chunk would contain), lays it out in the Cassandra `Data.db` chunk framing
//! (`[compressed_payload][4-byte BE CRC32]`), and drives it through the shipped
//! `ChunkDecompressor` — the same decode path a real SSTable read uses. It
//! proves CQLite rejects the frame (never decodes it to the original bytes) and,
//! since #1414, that the reader detects the dictionary from the AUTHORITATIVE
//! zstd frame header (nonzero `Dictionary_ID`) and fails closed with a typed
//! `Error::UnsupportedFormat` that names the feature — distinct from corruption.
//!
//! ## Feature gate
//!
//! Every test in this file drives the production `ChunkDecompressor` / reader
//! zstd decompress path, which returns `Error::InvalidFormat("Zstd support not
//! compiled in")` when cqlite-core is built WITHOUT its optional `zstd` feature.
//! Under such a build the positive-control decode (and the fail-closed
//! assertions) would be meaningless, so the whole module is gated on the `zstd`
//! feature — it compiles out entirely in a no-zstd build. The unconditional
//! `zstd` dev-dependency (dictionary training) is orthogonal to the runtime
//! `zstd` feature the decompressor is gated on.
#![cfg(feature = "zstd")]

use std::io::Cursor;
use std::path::{Path, PathBuf};

use cqlite_core::parser::header::CassandraVersion;
use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::Error;

/// Documented location of the *commissioned* dictionary-compressed fixture (see
/// the fixture-commission issue). Absent until that issue lands.
const DICT_FIXTURE_REL: &str = "sstables/test_comp/zstd_dictionary_table/nb-1-big";

/// Resolve the datasets root the same way the other parity lanes do.
fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(|w| w.join("test-data/datasets"))
                .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
        })
}

/// True when fixture absence must be a hard failure (nightly / exhaustive tiers).
///
/// Issue #2017 / PR #1996: gate ONLY on `CQLITE_REQUIRE_FIXTURES`, NOT on the
/// shared `require_fixtures_strict()` (which also fires under
/// `CQLITE_PARITY_REQUIRE_DATASETS`, the #1230 main-corpus guard). The
/// dictionary-compressed fixture (`test_comp/zstd_dictionary_table/`, #1399) is
/// UNCOMMISSIONED — no stock Apache Cassandra can flush it, so it is NOT part of
/// the fetched main dataset corpus and is provisioned only in tiers that set
/// `CQLITE_REQUIRE_FIXTURES=1`. Letting `CQLITE_PARITY_REQUIRE_DATASETS=1` (now
/// set on the general `ci.yml` lanes) turn this suite's clean SKIP into a hard
/// panic is exactly the flag conflation that left main CI red.
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

/// The full component set a commissioned `nb`/BIG zstd(+dictionary) fixture MUST
/// carry — matches `test-data/scripts/generate-zstd-dictionary-fixture.md` and
/// the stock `test_comp/zstd_table` generation on disk. Sidecars (`.db.jsonl`,
/// `.db.txt` goldens) are intentionally excluded.
const REQUIRED_FIXTURE_COMPONENTS: &[&str] = &[
    "Data.db",
    "CompressionInfo.db",
    "Statistics.db",
    "Index.db",
    "Summary.db",
    "Filter.db",
    "Digest.crc32",
    "TOC.txt",
];

/// Hard-fail (panic) when the commissioned fixture is present but INCOMPLETE, so
/// a partial fixture can never silently pass a gated oracle on an unrelated
/// missing-component / open / metadata error. Call this only AFTER the Data.db
/// presence gate (which SKIPs a clean checkout and hard-fails under
/// `CQLITE_REQUIRE_FIXTURES=1`) has established the fixture exists.
fn assert_full_fixture_present() {
    let missing: Vec<&str> = REQUIRED_FIXTURE_COMPONENTS
        .iter()
        .copied()
        .filter(|c| {
            !datasets_root()
                .join(format!("{DICT_FIXTURE_REL}-{c}"))
                .exists()
        })
        .collect();
    assert!(
        missing.is_empty(),
        "commissioned zstd-dictionary fixture is INCOMPLETE — missing component(s) {:?} \
         (expected the full set {:?} under {}); a partial fixture must not silently pass a \
         gated oracle",
        missing,
        REQUIRED_FIXTURE_COMPONENTS,
        datasets_root().join(DICT_FIXTURE_REL).display()
    );
}

/// Build a Cassandra-style single-chunk `Data.db` byte image: the compressed
/// payload followed by its 4-byte big-endian inline CRC32 (the exact framing
/// `CompressedSequentialWriter` writes and `ChunkDecompressor` reads).
fn cassandra_chunk_image(compressed_payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(compressed_payload.len() + 4);
    buf.extend_from_slice(compressed_payload);
    buf.extend_from_slice(&crc32fast::hash(compressed_payload).to_be_bytes());
    buf
}

/// A `ZstdCompressor` `CompressionInfo` for one chunk holding exactly
/// `plaintext_len` uncompressed bytes. `max_compressed_length = i32::MAX` keeps
/// the decoder off the incompressible-raw-chunk path so the zstd frame is
/// actually handed to the zstd decoder.
fn zstd_single_chunk_info(plaintext_len: usize) -> CompressionInfo {
    CompressionInfo {
        algorithm: "ZstdCompressor".to_string(),
        option_pairs: vec![],
        chunk_length: plaintext_len as u32,
        max_compressed_length: i32::MAX as u32,
        data_length: plaintext_len as u64,
        chunk_offsets: vec![0],
    }
}

/// Independently recover the nonzero `Dictionary_ID` from a raw zstd frame, so
/// the reader's reported ID can be cross-checked without trusting the reader's
/// own parser. Mirrors the zstd frame spec (RFC 8878 §3.1): 4-byte magic
/// `0xFD2FB528` (LE) + `Frame_Header_Descriptor` (low two bits = 0/1/2/4 ID
/// bytes; bit 5 = Single_Segment_flag; a Window_Descriptor byte follows the
/// descriptor iff that flag is 0) + little-endian `Dictionary_ID`.
fn expected_dictionary_id(frame: &[u8]) -> Option<u32> {
    if frame.len() < 5 || frame[0..4] != [0x28, 0xB5, 0x2F, 0xFD] {
        return None;
    }
    let fhd = frame[4];
    let did_size = match fhd & 0x03 {
        0 => return None,
        1 => 1usize,
        2 => 2usize,
        _ => 4usize,
    };
    let single_segment = (fhd & 0x20) != 0;
    let start = 5 + usize::from(!single_segment);
    let end = start + did_size;
    if frame.len() < end {
        return None;
    }
    let mut id = 0u32;
    for (i, &b) in frame[start..end].iter().enumerate() {
        id |= u32::from(b) << (8 * i);
    }
    (id != 0).then_some(id)
}

/// Deterministic, structured sample corpus for dictionary training. Repeated
/// shared substrings give `zdict` enough signal to train on tiny inputs.
fn training_corpus() -> Vec<Vec<u8>> {
    (0..1024u32)
        .map(|i| {
            format!(
                "cqlite|zstd|dictionary|row={i}|keyspace=test_comp|table=zstd_dictionary_table|value=payload-{}",
                i % 37
            )
            .into_bytes()
        })
        .collect()
}

#[test]
fn zstd_dictionary_frame_rejected_fail_closed() {
    // A chunk of realistic, dictionary-friendly plaintext.
    let plaintext =
        b"cqlite|zstd|dictionary|row=verify|keyspace=test_comp|table=zstd_dictionary_table|value=payload-7"
            .to_vec();

    // Train a REAL zstd dictionary and compress the chunk WITH it. The resulting
    // frame carries the trained dictionary's ID in its header — exactly what a
    // dictionary-compressed Cassandra chunk contains.
    let samples = training_corpus();
    let dict = zstd::dict::from_samples(&samples, 4 * 1024)
        .expect("train a zstd dictionary from the sample corpus");

    let dict_frame = {
        let mut c =
            zstd::bulk::Compressor::with_dictionary(3, &dict).expect("build dictionary compressor");
        c.compress(&plaintext)
            .expect("dictionary-compress the chunk")
    };

    // --- Positive control: the SAME plaintext compressed WITHOUT a dictionary
    // must decode cleanly through the exact same path. This proves the harness
    // (framing, CRC, CompressionInfo, decompressor) is correct, so the failure
    // below is attributable to the DICTIONARY and nothing else.
    let plain_frame = zstd::bulk::compress(&plaintext, 3).expect("plain zstd-compress the chunk");
    {
        let image = cassandra_chunk_image(&plain_frame);
        let mut dec = ChunkDecompressor::new(
            zstd_single_chunk_info(plaintext.len()),
            CassandraVersion::V5_0Release,
        )
        .expect("build decompressor for plain frame");
        let out = dec
            .decompress_chunk_by_index(&mut Cursor::new(image), 0)
            .expect("plain (no-dictionary) zstd chunk must decode");
        assert_eq!(
            out, plaintext,
            "positive control: no-dictionary zstd must round-trip exactly"
        );
    }

    // --- The dictionary frame must FAIL CLOSED through the same path.
    let image = cassandra_chunk_image(&dict_frame);
    let mut dec = ChunkDecompressor::new(
        zstd_single_chunk_info(plaintext.len()),
        CassandraVersion::V5_0Release,
    )
    .expect("build decompressor for dictionary frame");
    let result = dec.decompress_chunk_by_index(&mut Cursor::new(image), 0);

    // 1. REJECTION — never Ok. A dictionary-compressed chunk must never decode
    //    to rows or to any bytes at all.
    let err = match result {
        Ok(bytes) => panic!(
            "FAIL-CLOSED VIOLATION: dictionary-compressed zstd chunk decoded to \
             {} bytes (equals-plaintext={}). CQLite must reject dictionary frames, \
             never decode them.",
            bytes.len(),
            bytes == plaintext
        ),
        Err(e) => e,
    };

    // 2. The rejection must be in the typed-error family (never a panic, never a
    //    partial decode). It must NOT be misclassified as data Corruption.
    assert!(
        !matches!(err, Error::Corruption(_)),
        "dictionary rejection must not be misclassified as Error::Corruption; got: {err}"
    );

    // #1414: the reader now detects the dictionary from the AUTHORITATIVE zstd
    // frame header (a nonzero Dictionary_ID) and fails closed with a typed
    // `Error::UnsupportedFormat` that NAMES the feature — BEFORE attempting a
    // plain decode. It must NOT be the generic `Error::InvalidFormat("Zstd
    // decompression failed …")` a plain-decode failure produced.
    assert!(
        matches!(err, Error::UnsupportedFormat(_)),
        "dictionary rejection must be Error::UnsupportedFormat naming the feature (#1414); got: {err}"
    );
    let msg = err.to_string();
    let lower = msg.to_ascii_lowercase();
    assert!(
        lower.contains("zstd") && lower.contains("dictionary"),
        "rejection message must name the zstd dictionary feature; got: {msg}"
    );
    assert!(
        lower.contains("dictionary_id="),
        "rejection message must name the authoritative Dictionary_ID from the frame header; got: {msg}"
    );
    // The dictionary ID named in the message must be the actual ID encoded in the
    // frame header — proof the reader parsed the real frame, not a placeholder.
    // Independently recover it here from the raw frame bytes (magic 0xFD2FB528 LE
    // + Frame_Header_Descriptor: low two bits = Dictionary_ID size 1/2/4, bit 5 =
    // Single_Segment_flag gating the Window_Descriptor byte).
    let expected_id = expected_dictionary_id(&dict_frame)
        .expect("trained-dictionary frame must carry a nonzero Dictionary_ID");
    assert!(
        msg.contains(&format!("Dictionary_ID={expected_id}")),
        "message must name the frame's real Dictionary_ID {expected_id}; got: {msg}"
    );
    // Not a checksum/CRC finding: the inline chunk CRC over the compressed bytes
    // is valid — the dictionary, not corruption, is why the frame is undecodable.
    assert!(
        !lower.contains("crc") && !lower.contains("checksum") && !lower.contains("digest"),
        "dictionary rejection must not be a CRC/checksum/digest failure; got: {msg}"
    );
}

#[test]
fn zstd_dictionary_sstable_rejected_via_reader_fixture() {
    // AC#1/#2 oracle: a real commissioned Cassandra zstd+dictionary SSTable,
    // opened via the reader/query path, must fail closed (typed rejection, no
    // rows). Fixture-gated: SKIP clean, hard-FAIL under CQLITE_REQUIRE_FIXTURES=1.
    //
    // #1414 landed the typed rejection: the reader detects the dictionary from the
    // authoritative zstd frame header (nonzero Dictionary_ID) and fails closed with
    // `Error::UnsupportedFormat` naming the feature, BEFORE a plain decode. The
    // assertions below encode that end state — a typed UnsupportedFormat rejection
    // naming the zstd/dictionary path, explicitly excluding CRC/checksum, never
    // Corruption, never rows.
    let data_db = datasets_root().join(format!("{DICT_FIXTURE_REL}-Data.db"));
    if !data_db.exists() {
        let msg = format!(
            "zstd-dictionary fixture absent: {} (commission via \
             test-data/scripts/generate-zstd-dictionary-fixture.md; tracked by the \
             fixture-commission issue linked from #1399)",
            data_db.display()
        );
        assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1 but {msg}");
        eprintln!("SKIP: {msg}");
        return;
    }

    // A partial fixture (Data.db present but other components missing) must not
    // let this oracle pass on an unrelated open/metadata error instead of
    // exercising dict decompression — require the FULL commissioned set up front.
    assert_full_fixture_present();

    // Present: the reader must reject (never return rows). We assert the
    // fail-closed safety property here; the exact typed class is tracked as a
    // KNOWN LIMITATION under issue #1414 (see the unit characterization above).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        use cqlite_core::storage::sstable::reader::SSTableReader;
        use cqlite_core::{Config, Platform};
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        // Opening may LEGITIMATELY succeed: metadata parsing (header, Statistics.db,
        // CompressionInfo.db) never decompresses chunk bytes, and `ZstdCompressor`
        // is a known compressor name. The fail-closed property lives on the
        // DECOMPRESSING READ path — so if open() succeeds we MUST drive a full scan
        // (which reads + decompresses every Data.db chunk) and assert THAT rejects.
        match SSTableReader::open(&data_db, &config, platform).await {
            Ok(reader) => {
                // Drive a decompression path: a full scan reads and decompresses
                // every Data.db chunk. This MUST fail closed — never return rows,
                // never surface plaintext.
                match reader.iterate_all_partitions().await {
                    Ok(rows) => panic!(
                        "FAIL-CLOSED VIOLATION: full scan of a dictionary-compressed \
                         SSTable returned {} row(s) instead of rejecting — the \
                         decompression path must fail closed (see #1399 AC#2 / #1414)",
                        rows.len()
                    ),
                    Err(e) => {
                        // Must be the SPECIFIC zstd-dictionary rejection — a typed
                        // `Error::UnsupportedFormat` naming the feature, never data
                        // Corruption, never a panic/partial decode, and never an
                        // unrelated open/metadata error (#1414).
                        assert!(
                            !matches!(e, Error::Corruption(_)),
                            "scan rejection of a dictionary SSTable must not be \
                             misclassified as Corruption; got: {e}"
                        );
                        assert!(
                            matches!(e, Error::UnsupportedFormat(_)),
                            "scan rejection must be the typed UnsupportedFormat \
                             zstd-dictionary rejection (#1414); got: {e}"
                        );
                        let msg = e.to_string().to_ascii_lowercase();
                        // Must name the zstd dictionary feature …
                        assert!(
                            msg.contains("zstd") && msg.contains("dictionary"),
                            "scan rejection message must name the zstd dictionary \
                             feature (#1414); got: {e}"
                        );
                        // … and must NOT be an inline-CRC / chunk-checksum failure:
                        // the compressed-byte CRC is valid; the dictionary, not
                        // corruption, is why the frame cannot be decoded. Excluding
                        // these terms stops a CRC/digest mismatch from satisfying the
                        // oracle by coincidence.
                        assert!(
                            !msg.contains("crc")
                                && !msg.contains("checksum")
                                && !msg.contains("digest"),
                            "scan rejection must be the dict-decompression failure, not a \
                             CRC/checksum/digest mismatch; got: {e}"
                        );
                    }
                }
            }
            Err(e) => {
                // Opening a dictionary-compressed SSTable is EXPECTED to succeed:
                // metadata parsing never decompresses chunk bytes and
                // `ZstdCompressor` is a known compressor name. The fail-closed
                // property lives on the decompressing read path, so an open() error
                // is NOT the intended dict-rejection path — it means the fixture
                // shape changed. Fail loudly rather than accept it as "rejection".
                panic!("unexpected open failure for current fixture shape: {e}");
            }
        }
    });
}

#[test]
fn zstd_dictionary_reader_fails_closed_current_behavior() {
    // CHARACTERIZATION of TODAY's reader behavior (the ACTIVE reader oracle the
    // manifest points at). Distinct from the sibling
    // `zstd_dictionary_sstable_rejected_via_reader_fixture`, which is `#[ignore]`
    // and encodes the #1414 TARGET (a typed `InvalidFormat` rejection that names
    // the zstd/dictionary path and excludes CRC/checksum). This test asserts only
    // the SAFETY PROPERTY that must hold NOW: opening the commissioned
    // dictionary-compressed SSTable and driving a full scan must FAIL CLOSED —
    // never returning rows — while accepting EITHER `Error::Corruption` OR
    // `Error::InvalidFormat`, because the current full-scan stitch path wraps a
    // zstd-dictionary decode failure as `Corruption`. It deliberately does NOT
    // require the typed variant, so it stays green until #1414 flips the class;
    // when #1414 lands, the ignored sibling proves the upgrade and this test keeps
    // guarding against a regression back to returning rows.
    //
    // Fixture-gated identically to the other reader oracle: SKIPs cleanly without
    // the commissioned fixture, hard-FAILs under `CQLITE_REQUIRE_FIXTURES=1`
    // (`require_fixtures()`, #2017), and requires the FULL component set before
    // exercising the decompressing read path.
    let data_db = datasets_root().join(format!("{DICT_FIXTURE_REL}-Data.db"));
    if !data_db.exists() {
        let msg = format!(
            "zstd-dictionary fixture absent: {} (commission via \
             test-data/scripts/generate-zstd-dictionary-fixture.md; tracked by the \
             fixture-commission issue linked from #1399)",
            data_db.display()
        );
        assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1 but {msg}");
        eprintln!("SKIP: {msg}");
        return;
    }

    // Reject a partial fixture up front so this oracle cannot pass on an unrelated
    // missing-component / open / metadata error instead of the dict-decode path.
    assert_full_fixture_present();

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        use cqlite_core::storage::sstable::reader::SSTableReader;
        use cqlite_core::{Config, Platform};
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        // Opening MUST succeed: metadata parsing (header, Statistics.db,
        // CompressionInfo.db) never decompresses chunk bytes and `ZstdCompressor`
        // is a known compressor name. The fail-closed property lives on the
        // DECOMPRESSING read path, so an open() error means the fixture shape
        // changed — fail loudly rather than accept it as "rejection".
        let reader = SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open of a dictionary-compressed SSTable must succeed (metadata only)");

        // Drive the real reader decompress path: a full scan reads + decompresses
        // every Data.db chunk. This MUST fail closed.
        match reader.iterate_all_partitions().await {
            Ok(rows) => panic!(
                "FAIL-CLOSED VIOLATION: full scan of a dictionary-compressed SSTable \
                 returned {} row(s) instead of rejecting — the decompression path must \
                 fail closed (see #1399 AC#2)",
                rows.len()
            ),
            Err(e) => {
                // SAFETY-PROPERTY oracle: the reader must fail closed and return no
                // rows. Accept any of the fail-closed typed variants — #1414 makes
                // the reader-level rejection a typed `UnsupportedFormat` (asserted
                // precisely by the sibling `..._via_reader_fixture`), but this test
                // guards only that the scan hard-errors, so it stays green whether
                // the stitch path surfaces UnsupportedFormat, InvalidFormat, or
                // (legacy) Corruption.
                assert!(
                    matches!(
                        e,
                        Error::Corruption(_) | Error::InvalidFormat(_) | Error::UnsupportedFormat(_)
                    ),
                    "scan of a dictionary SSTable must fail closed (Corruption, \
                     InvalidFormat, or UnsupportedFormat); got: {e}"
                );
            }
        }
    });
}

#[test]
fn zstd_dictionary_verify_reports_unsupported_not_checksum_fixture() {
    // AC#3 oracle: `cqlite verify` over the commissioned fixture must reject and
    // must NOT report the failure as a checksum/digest mismatch (the inline chunk
    // CRC over the compressed bytes is valid — the dictionary, not corruption, is
    // why the bytes cannot be decoded). Fixture-gated like the reader oracle.
    let dir = datasets_root().join(
        Path::new(DICT_FIXTURE_REL)
            .parent()
            .expect("fixture rel path has a parent dir"),
    );
    let data_db = datasets_root().join(format!("{DICT_FIXTURE_REL}-Data.db"));
    if !data_db.exists() {
        let msg = format!(
            "zstd-dictionary fixture absent: {} (see #1399 fixture-commission issue)",
            data_db.display()
        );
        assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1 but {msg}");
        eprintln!("SKIP: {msg}");
        return;
    }

    // Require the FULL commissioned component set so the verify report cannot
    // pass on an unrelated missing-component / metadata finding instead of the
    // decompression-path rejection this oracle exists to prove.
    assert_full_fixture_present();

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyErrorClass, VerifyMode};
        use cqlite_core::{Config, Platform};
        use std::sync::Arc;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let report = verify_sstable(&dir, VerifyMode::Full, &config, platform)
            .await
            .expect("verify should run (env ok) even if it reports findings");

        assert!(
            !report.is_ok(),
            "verify must reject a dictionary-compressed SSTable, not pass it clean"
        );
        // Must NOT be a checksum/digest failure — the compressed-byte CRC is valid.
        assert!(
            report
                .findings
                .iter()
                .all(|f| f.class != VerifyErrorClass::DigestMismatch),
            "dictionary rejection must not be reported as a Digest/checksum mismatch: {:?}",
            report.findings
        );
        // Must report the SPECIFIC unsupported-feature rejection on the data
        // component — not ONLY unrelated setup/metadata findings, and NOT the
        // truncation/bit-flip `ChunkDecompressionError` class. The verify FULL row
        // scan decompresses every Data.db chunk; the dict frame fails closed with
        // `Error::UnsupportedFormat`, and `classify_scan_error` maps it onto the
        // dedicated `VerifyErrorClass::UnsupportedCompressionFeature` (#1414) —
        // distinct from checksum/corruption.
        assert!(
            report
                .findings
                .iter()
                .all(|f| f.class != VerifyErrorClass::ChunkDecompressionError),
            "dictionary rejection must NOT be reported as the truncation/bit-flip \
             ChunkDecompressionError class (#1414): {:?}",
            report.findings
        );
        assert!(
            report.findings.iter().any(|f| {
                if f.class != VerifyErrorClass::UnsupportedCompressionFeature {
                    return false;
                }
                if f.component != "Data.db" && f.component != "CompressionInfo.db" {
                    return false;
                }
                let detail = f.detail.to_ascii_lowercase();
                // Must name the zstd dictionary feature …
                let names_dictionary = detail.contains("zstd") && detail.contains("dictionary");
                // … and must NOT be an inline-CRC / chunk-checksum finding: the
                // compressed-byte CRC is valid, so a CRC-flavored finding must not
                // be allowed to satisfy this oracle.
                let is_crc = detail.contains("crc")
                    || detail.contains("checksum")
                    || detail.contains("digest");
                names_dictionary && !is_crc
            }),
            "verify must report the dictionary rejection as an UnsupportedCompressionFeature on \
             Data.db/CompressionInfo.db whose detail names the zstd dictionary feature \
             (see #1414) — not a CRC/checksum/digest finding and not only unrelated \
             setup/metadata findings: {:?}",
            report.findings
        );
    });
}
