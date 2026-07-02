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
//! proves CQLite rejects the frame (never decodes it to the original bytes) and
//! characterizes the CURRENT error, whose class is a KNOWN LIMITATION tracked in
//! the production-hardening issue referenced below.

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

/// True when fixture absence must be a hard failure (full-dataset CI / nightly).
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
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
    assert!(
        matches!(err, Error::InvalidFormat(_) | Error::UnsupportedFormat(_)),
        "dictionary rejection must be a typed format error; got: {err}"
    );

    // KNOWN LIMITATION: see issue #1414. The *desired* end state (issue #1399
    // acceptance criterion 2) is `Error::UnsupportedFormat` explicitly NAMING the
    // dictionary feature (e.g. "zstd dictionary compression (Dictionary_ID=N) is
    // not supported"). Today CQLite fails closed but surfaces the generic
    // `Error::InvalidFormat("Zstd decompression failed …")` — it does not detect
    // or name the dictionary. This test asserts the CURRENT fail-closed behavior;
    // #1414 tracks upgrading the message/class to a typed dictionary rejection
    // (reader) and a dedicated verify class (see the verify oracle below).
    let msg = err.to_string();
    assert!(
        msg.to_ascii_lowercase().contains("zstd"),
        "rejection message should identify the zstd path; got: {msg}"
    );
}

#[test]
fn zstd_dictionary_sstable_rejected_via_reader_fixture() {
    // AC#1/#2 oracle: a real commissioned Cassandra zstd+dictionary SSTable,
    // opened via the reader/query path, must fail closed (typed rejection, no
    // rows). Fixture-gated: SKIP clean, hard-FAIL under CQLITE_REQUIRE_FIXTURES=1.
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
                        // Must be the SPECIFIC zstd-dictionary decompression
                        // rejection — never data Corruption, never a panic/partial
                        // decode, and never an unrelated open/metadata error.
                        //
                        // KNOWN LIMITATION: see #1414. The target end state is a
                        // clean `Error::UnsupportedFormat` explicitly naming zstd
                        // dictionary compression. Today CQLite fails closed on a
                        // real trained-dict frame as `Error::InvalidFormat` whose
                        // message names the zstd decompression path
                        // (chunk_decompressor.rs:461); this assertion tracks that
                        // CURRENT behavior until #1414 upgrades the class.
                        assert!(
                            !matches!(e, Error::Corruption(_)),
                            "scan rejection of a dictionary SSTable must not be \
                             misclassified as Corruption; got: {e}"
                        );
                        assert!(
                            matches!(e, Error::InvalidFormat(_)),
                            "scan rejection must be the InvalidFormat zstd-dictionary \
                             decompression class (see #1414); got: {e}"
                        );
                        let msg = e.to_string().to_ascii_lowercase();
                        // Must name the zstd/dictionary decompression path
                        // (chunk_decompressor.rs:461) …
                        assert!(
                            msg.contains("zstd") || msg.contains("dictionary"),
                            "scan rejection message must name the zstd/dictionary \
                             decompression path (chunk_decompressor.rs:461); got: {e}"
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
        // Must report the SPECIFIC decompression-path rejection on the data
        // component — not ONLY unrelated setup/metadata findings. The verify
        // FULL row scan decompresses every Data.db chunk; the dict frame fails
        // there and `classify_scan_error` maps the "Zstd decompression failed"
        // message onto `VerifyErrorClass::ChunkDecompressionError` on `Data.db`.
        //
        // KNOWN LIMITATION: see #1414. `ChunkDecompressionError` is shared with
        // truncation/bit-flip; the target end state is an explicit
        // unsupported-compression verify class. This assertion tracks the CURRENT
        // behavior until #1414 upgrades it.
        assert!(
            report.findings.iter().any(|f| {
                if f.class != VerifyErrorClass::ChunkDecompressionError {
                    return false;
                }
                if f.component != "Data.db" && f.component != "CompressionInfo.db" {
                    return false;
                }
                let detail = f.detail.to_ascii_lowercase();
                // Must name the decompression path …
                let names_decompression = detail.contains("zstd")
                    || detail.contains("dictionary")
                    || detail.contains("decompress");
                // … and must NOT be an inline-CRC / chunk-checksum finding: the
                // compressed-byte CRC is valid, so a CRC-flavored decompression
                // finding must not be allowed to satisfy this oracle.
                let is_crc = detail.contains("crc")
                    || detail.contains("checksum")
                    || detail.contains("digest");
                names_decompression && !is_crc
            }),
            "verify must report the dictionary rejection as a ChunkDecompressionError on \
             Data.db/CompressionInfo.db whose detail names the zstd/dictionary/decompress path \
             (see #1414) — not a CRC/checksum/digest finding and not only unrelated \
             setup/metadata findings: {:?}",
            report.findings
        );
    });
}
