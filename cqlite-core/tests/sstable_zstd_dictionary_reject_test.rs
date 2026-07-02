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
                        // Must be a typed format rejection, never data Corruption
                        // and never a panic/partial decode. #1414 tracks upgrading
                        // the message/class to name the dictionary feature; today it
                        // fails closed as an InvalidFormat/UnsupportedFormat-class
                        // error surfaced from the zstd decompression path.
                        assert!(
                            !matches!(e, Error::Corruption(_)),
                            "scan rejection of a dictionary SSTable must not be \
                             misclassified as Corruption; got: {e}"
                        );
                        assert!(
                            matches!(
                                e,
                                Error::InvalidFormat(_)
                                    | Error::UnsupportedFormat(_)
                                    | Error::UnsupportedVersion { .. }
                            ),
                            "scan rejection must be a typed format error; got: {e}"
                        );
                    }
                }
            }
            Err(e) => {
                // Rejecting at open() is also acceptable, as long as it is not
                // misclassified as data corruption.
                assert!(
                    !matches!(e, Error::Corruption(_)),
                    "reader rejection of a dictionary SSTable must not be Corruption; got: {e}"
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
        // KNOWN LIMITATION: see #1414. Today this surfaces as
        // `ChunkDecompressionError` (shared with truncation/bit-flip); #1414 tracks
        // a dedicated unsupported-compression-feature verify class.
    });
}
