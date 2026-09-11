//! Issue #4196 — round-15 fixes from an Opus whole-module `salvage` audit
//! (`openspec/changes/sstable-salvage/tasks.md`, "Round 15 (Opus module
//! audit)"): four independent bounds/overflow findings distinct from the
//! rounds 9-14 entry points already covered by `issue_4196_salvage_oom_bounds.rs`.
//!
//! 1. **Whole-remaining-section materialization for a SHORT boundary
//!    source** (`Medium finding 1`) — `decode_partition_at_offset_for_salvage`'s
//!    `end_bound: None` arm resolved `end` to the WHOLE remaining data
//!    section with no per-partition ceiling; every EXISTING guard bounds
//!    `end` against the real FILE size, which a short/sparse boundary
//!    source's `end` already satisfies by construction.
//! 2. **Pre-flight chunk buffer sized from a short `chunk_offsets` table**
//!    (`Medium finding 2`) — the LAST chunk's declared size is
//!    `total_size - offset[last]` (`compression_info.rs`), so the round-11
//!    plausibility guard (`offset + size <= total_size`) is satisfied BY
//!    CONSTRUCTION for exactly this corruption shape.
//! 3. **`u64` overflow on `entry.data_offset + 1`** (`Medium finding 3`) —
//!    an uncompressed BIG input with NO `CRC.db` reaches this arm with
//!    `data_length == 0`, and a corrupted LAST entry's `data_offset` at
//!    `u64::MAX` panicked the plain `+ 1` in every debug build (every test
//!    lane).
//! 4. **Unbounded `losses` per report** (`Medium finding 4`) — a damaged
//!    input where MOST partitions are lost held every one resident,
//!    multiplied by each `Loss.key_hex`'s independent worst case.
//!
//! Medium finding 5 (a genuine I/O failure exits 3 with no manifest) is
//! CLI-level; its regression test lives in `salvage_cli_tests.rs`.
//!
//! Skip-clean when the corruption corpus is absent; `CQLITE_REQUIRE_FIXTURES=1`
//! (#1094 doctrine) turns that into a hard failure.

// `not(tombstones)`: see the matching note in
// `issue_4196_salvage_healthy_parity.rs`.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::storage::write_engine::salvage::{salvage_sstable, LossClass, SalvageOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/salvage_corpus.rs"]
mod salvage_corpus;

use salvage_corpus::{single_data_db, skip_or_require, table_schema_for};

/// Copy every component of `src` into a fresh temp dir, EXCLUDING `CRC.db`
/// and the bulky `.jsonl`/`.txt` sidecars — the "no CRC.db" precondition
/// Medium findings 1 and 3 both need (an uncompressed input with a MISSING,
/// not merely damaged, `CRC.db` — `ChunkCrcUnavailable` exists for exactly
/// this, a SUPPORTED input shape, not a corruption).
fn copy_without_crc_db(src: &std::path::Path, dst: &std::path::Path) -> std::path::PathBuf {
    std::fs::create_dir_all(dst).expect("create dst dir");
    let mut data_db = None;
    for entry in std::fs::read_dir(src).expect("read src dir").flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-CRC.db")
            || name_str.ends_with(".jsonl")
            || name_str.ends_with("Statistics.db.txt")
        {
            continue;
        }
        std::fs::copy(entry.path(), dst.join(&name)).expect("copy component");
        if name_str.ends_with("-Data.db") {
            data_db = Some(dst.join(&name));
        }
    }
    data_db.expect("fixture must ship a Data.db")
}

/// roborev, issue #4196, round-15 Medium finding 1 (an Opus whole-module
/// audit) — the UNCOMPRESSED arm: a SHORT (here: absent) `CRC.db` reaches
/// `decode_partition_at_offset_for_salvage`'s `end_bound: None` case
/// (`chunk_size == 0` skips the chunk-CRC pre-flight's OWN classification
/// entirely — round 14's fix — so the loop falls through to
/// `recover_one_partition`/this function, exactly the code path under
/// test) with `end` resolving to `section_len` — the file's REAL length.
/// `Data.db` is extended via `File::set_len` (a SPARSE extension — no real
/// bytes are written, so this test allocates nothing large itself) to
/// comfortably exceed `SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES` (128 MiB),
/// while the ONE real partition's `data_offset` (0) is left UNCHANGED —
/// this is NOT an implausible-offset corruption, it is a genuinely-short
/// boundary source's `end` resolving to a huge, real file length.
///
/// The bound itself is the assertion (mirrors this whole module's
/// established `tokio::time::timeout` pattern): pre-fix, this allocates a
/// resident `Vec` sized to the (sparse-but-logically-huge) remaining
/// section before classifying anything; post-fix it classifies `Truncated`
/// immediately, well under the timeout.
#[tokio::test]
async fn short_boundary_source_uncompressed_does_not_materialize_whole_section() {
    const KEYSPACE: &str = "test_comp";
    const TABLE_NAME: &str = "uncompressed_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "uncompressed_table (test_comp) fixture",
            &format!(
                "no candidate root carries {KEYSPACE}.{TABLE_NAME}; {}",
                datasets_root::describe_search(KEYSPACE, TABLE_NAME)
            ),
        );
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE_NAME)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE_NAME}: no usable generation directory"));
    let schema = table_schema_for(TABLE_NAME);

    let temp = TempDir::new().expect("tempdir");
    let corrupt_dir = temp.path().join("corrupt_input");
    let data_db = copy_without_crc_db(&fixture_dir, &corrupt_dir);

    // Sparse extension: logical length grows, real allocated blocks do not
    // (any local filesystem holds a "hole" for the unwritten tail).
    const HUGE_LEN: u64 = 200 * 1024 * 1024; // 200 MiB, comfortably > the 128 MiB ceiling
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_db)
            .expect("open Data.db for set_len");
        let real_len = file.metadata().expect("stat").len();
        assert!(
            real_len < HUGE_LEN,
            "fixture's real Data.db ({real_len} bytes) must already be smaller than the target \
             sparse length, or this test proves nothing"
        );
        file.set_len(HUGE_LEN).expect("sparse-extend Data.db");
    }

    let out_temp = TempDir::new().expect("tempdir");
    let out_root = out_temp.path().join("out");
    let report = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        salvage_sstable(&data_db, &out_root, &schema, SalvageOptions::default()),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "salvage did not complete within 30s against a sparse-extended (200 MiB logical) \
             Data.db with a short (absent) CRC.db — the whole-remaining-section materialization \
             regressed"
        )
    })
    .unwrap_or_else(|e| {
        panic!("salvage must not hard-error on this input (a Loss, not an Err): {e:#}")
    });

    assert_eq!(
        report.partitions.total, 1,
        "expected exactly one partition in this fixture's Index.db; got {}",
        report.partitions.total
    );
    assert_eq!(
        report.losses.len(),
        1,
        "expected exactly the one partition as a loss; got {:?}",
        report.losses
    );
    assert_eq!(
        report.losses[0].class,
        LossClass::Truncated,
        "expected LossClass::Truncated (the span exceeds the plausible-partition ceiling); got \
         {:?}",
        report.losses[0]
    );
    assert!(
        report
            .component_findings
            .iter()
            .any(|f| f.class == "ChunkCrcUnavailable"),
        "expected a ChunkCrcUnavailable finding (CRC.db is absent); got {:?}",
        report.component_findings
    );
}

/// roborev, issue #4196, round-15 Medium finding 2 (an Opus whole-module
/// audit): `chunk_offsets` corrupted SHORT (truncated to ONE entry) while
/// `Data.db` genuinely holds several real chunks — the LAST (only) chunk's
/// declared size becomes `total_size - chunk_offsets[0]`, i.e. the WHOLE
/// FILE, and `offset + size <= total_size` (the round-11 plausibility
/// guard) is satisfied BY CONSTRUCTION for exactly this shape (`size` is
/// DERIVED from `total_size`). The bound itself is the assertion: pre-fix,
/// `ChunkReader::read_chunk` allocates ~the whole Data.db for this one
/// chunk; post-fix the allocation itself is bounded and the chunk is
/// reported as a `CompressionInfo.db` corruption finding.
///
/// Uses `test_comp.short_final_chunk` rather than `lz4_table` (the OTHER
/// tests in this change reuse): `lz4_table`'s real `Data.db` (6,979 bytes)
/// is SMALLER than its own declared `chunk_length` (16,384) — "the whole
/// file" truncated to one chunk therefore NEVER exceeds `chunk_length + 4`
/// for that fixture (measured while developing this test: the truncated
/// chunk reads at its real, small size and surfaces an ordinary CRC32
/// mismatch, never reaching this fix's bound at all). `short_final_chunk`
/// (`chunk_length=4096`, real `Data.db`=12,444 bytes across 62 real
/// chunks) genuinely exceeds `chunk_length + 4` once truncated to one
/// entry, so it is the fixture that actually exercises the new bound.
#[tokio::test]
async fn short_chunk_offsets_table_does_not_materialize_whole_file() {
    const KEYSPACE: &str = "test_comp";
    const TABLE_NAME: &str = "short_final_chunk";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "short_final_chunk (test_comp) fixture",
            &format!(
                "no candidate root carries {KEYSPACE}.{TABLE_NAME}; {}",
                datasets_root::describe_search(KEYSPACE, TABLE_NAME)
            ),
        );
        return;
    };
    let clean_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE_NAME)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE_NAME}: no usable generation directory"));
    let schema = table_schema_for(TABLE_NAME);
    let clean_data_db = single_data_db(&clean_dir);
    let clean_ci_path = clean_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-CompressionInfo.db"),
    );
    let clean_ci_bytes = std::fs::read(&clean_ci_path).expect("read clean CompressionInfo.db");
    let clean_ci = CompressionInfo::parse(&clean_ci_bytes).expect("parse clean CompressionInfo.db");
    assert!(
        clean_ci.chunk_offsets.len() >= 2,
        "need at least 2 real chunks to demonstrate a short-but-plausible-by-construction table"
    );

    // Truncate to the FIRST entry only — now "the last" chunk trivially,
    // whose declared size becomes `total_size - chunk_offsets[0]`.
    let truncated_offsets = vec![clean_ci.chunk_offsets[0]];

    let algorithm =
        cqlite_core::storage::sstable::writer::CompressionAlgorithm::from_cassandra_name(
            &clean_ci.algorithm,
        )
        .unwrap_or_else(|| panic!("unrecognized compressor name: {}", clean_ci.algorithm));
    let corrupt_metadata = cqlite_core::storage::sstable::writer::CompressionMetadata {
        algorithm,
        chunk_length: clean_ci.chunk_length,
        max_compressed_length: clean_ci.max_compressed_length,
        data_length: clean_ci.data_length,
        chunk_offsets: truncated_offsets,
        option_pairs: clean_ci.option_pairs.clone(),
    };
    let corrupt_ci_bytes =
        cqlite_core::storage::sstable::writer::CompressionInfoWriter::new(clean_ci_path.clone())
            .build_to_vec(&corrupt_metadata)
            .expect("re-serialize CompressionInfo.db with a truncated chunk_offsets table");

    let temp = TempDir::new().expect("tempdir");
    let corrupt_dir = temp.path().join("corrupt_input");
    std::fs::create_dir_all(&corrupt_dir).expect("create corrupt input dir");
    for entry in std::fs::read_dir(&clean_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-CompressionInfo.db") {
            std::fs::write(corrupt_dir.join(&name), &corrupt_ci_bytes)
                .expect("write corrupted CompressionInfo.db");
        } else if !name_str.ends_with(".jsonl") && !name_str.ends_with("Statistics.db.txt") {
            std::fs::copy(entry.path(), corrupt_dir.join(&name)).expect("copy fixture component");
        }
    }
    let corrupt_data_db = single_data_db(&corrupt_dir);

    let out_root = temp.path().join("out");
    let report = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        salvage_sstable(
            &corrupt_data_db,
            &out_root,
            &schema,
            SalvageOptions::default(),
        ),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "salvage did not complete within 30s on a chunk_offsets table truncated to ONE \
             entry — the whole-file chunk-buffer allocation regressed"
        )
    })
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on a truncated chunk_offsets table (a Loss or a \
             classified Refusal, not an Err): {e:#}"
        )
    });

    // Every partition's byte range intersects the one (now bogus) chunk —
    // a total loss (or a refusal, if this fixture holds only one
    // partition and it is the whole set) — never a hard error, never a
    // hang.
    if let Some(refusal) = &report.refused {
        eprintln!(
            "[issue_4196] short chunk_offsets table: refused as expected: {:?}",
            refusal
        );
    } else {
        assert!(
            !report.losses.is_empty(),
            "expected at least one loss against a chunk_offsets table truncated to one entry"
        );
        for loss in &report.losses {
            assert_eq!(
                loss.class,
                LossClass::ChunkCrc,
                "expected LossClass::ChunkCrc; got {:?}",
                loss
            );
        }
        eprintln!(
            "[issue_4196] short chunk_offsets table: completed without OOM, {} loss(es) \
             (no chunk handed to an allocation sized from the whole file).",
            report.losses.len()
        );
    }
    assert!(
        report
            .component_findings
            .iter()
            .any(|f| f.class == "ChunkDecompressionError"
                && f.detail.contains("CompressionInfo.db corruption")),
        "expected the finding to name this a CompressionInfo.db corruption, not a Data.db CRC \
         problem; got {:?}",
        report.component_findings
    );
}

/// roborev, issue #4196, round-15 Medium finding 3 (an Opus whole-module
/// audit): an uncompressed BIG input with NO `CRC.db` makes
/// `uncompressed_chunk_preflight` return `data_length == 0`, which skips
/// the `data_length > entry.data_offset` branch in `recover.rs`'s
/// `chunk_range_end` resolution UNCONDITIONALLY (`0` is never greater than
/// anything) — reaching the `entry.data_offset + 1` arm with the corrupted
/// LAST entry's `data_offset` at `u64::MAX` panicked on overflow in every
/// debug build (every test lane) instead of producing a classified
/// `Truncated` loss. This test running to completion AT ALL (under the
/// default debug test profile) is itself half the assertion; the other
/// half is the correct classification.
#[tokio::test]
async fn implausible_offset_with_no_crc_db_does_not_panic() {
    const KEYSPACE: &str = "test_comp";
    const TABLE_NAME: &str = "uncompressed_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "uncompressed_table (test_comp) fixture",
            &format!(
                "no candidate root carries {KEYSPACE}.{TABLE_NAME}; {}",
                datasets_root::describe_search(KEYSPACE, TABLE_NAME)
            ),
        );
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE_NAME)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE_NAME}: no usable generation directory"));
    let schema = table_schema_for(TABLE_NAME);

    let clean_data_db = single_data_db(&fixture_dir);
    let clean_index_db = fixture_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );
    let clean_index_bytes = std::fs::read(&clean_index_db).expect("read clean Index.db");

    // Rebuild the ONE entry with data_offset corrupted to u64::MAX,
    // keeping the key + promoted-index payload byte-for-byte (structural
    // edit, not a hardcoded byte offset — see the sibling tests in
    // `issue_4196_salvage_oom_bounds.rs` for the same pattern).
    let key_len = u16::from_be_bytes([clean_index_bytes[0], clean_index_bytes[1]]) as usize;
    let key_end = 2 + key_len;
    let (_orig_offset, consumed) =
        cqlite_core::parser::vint::decode_unsigned(&clean_index_bytes[key_end..])
            .expect("clean Index.db entry has a well-formed data_offset VInt");
    let rest_after_offset = &clean_index_bytes[key_end + consumed..];
    let mut corrupt_index_bytes = Vec::with_capacity(clean_index_bytes.len() + 8);
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[..key_end]);
    cqlite_core::storage::serialization::vint::encode_unsigned(u64::MAX, &mut corrupt_index_bytes);
    corrupt_index_bytes.extend_from_slice(rest_after_offset);

    let temp = TempDir::new().expect("tempdir");
    let corrupt_dir = temp.path().join("corrupt_input");
    let data_db = copy_without_crc_db(&fixture_dir, &corrupt_dir);
    std::fs::write(
        corrupt_dir.join(clean_index_db.file_name().expect("Index.db has a filename")),
        &corrupt_index_bytes,
    )
    .expect("write corrupted Index.db");

    let out_temp = TempDir::new().expect("tempdir");
    let out_root = out_temp.path().join("out");
    // Bounded-time AND no-panic in one guard: a panic inside the spawned
    // task would surface as the JoinError this `.await` would need to
    // unwrap — but `salvage_sstable` is called in-process (not spawned),
    // so a panic here aborts the WHOLE TEST BINARY; the test completing at
    // all (any outcome) already proves no panic occurred.
    let report = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        salvage_sstable(&data_db, &out_root, &schema, SalvageOptions::default()),
    )
    .await
    .unwrap_or_else(|_| panic!("salvage did not complete within 30s"))
    .unwrap_or_else(|e| {
        panic!("salvage must not hard-error on this input (a Loss, not an Err): {e:#}")
    });

    assert_eq!(report.partitions.total, 1);
    assert_eq!(
        report.losses.len(),
        1,
        "expected exactly the one partition as a loss; got {:?}",
        report.losses
    );
    assert_eq!(
        report.losses[0].class,
        LossClass::Truncated,
        "expected LossClass::Truncated; got {:?}",
        report.losses[0]
    );
    eprintln!(
        "[issue_4196] implausible offset (u64::MAX) with no CRC.db: completed without panicking, \
         classified {:?}.",
        report.losses[0].class
    );
}

/// roborev, issue #4196, round-15 Medium finding 4 (an Opus whole-module
/// audit): a damaged input where MOST partitions are lost held every
/// `Loss` resident — this test constructs a SYNTHETIC `Index.db` naming
/// 600 partitions, every one of them with a `data_offset` far past the
/// clean fixture's real (tiny) data length, so EVERY entry classifies
/// `Truncated` via `recover.rs`'s cheapest short-circuit (no decode
/// attempted for any of them) — exercising the cap
/// (`recover::MAX_RESIDENT_LOSSES == 500`) without needing a genuinely
/// adversarial multi-megabyte corrupt `Index.db`.
#[tokio::test]
async fn losses_beyond_the_cap_are_counted_not_resident() {
    const KEYSPACE: &str = "test_comp";
    const TABLE_NAME: &str = "uncompressed_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "uncompressed_table (test_comp) fixture",
            &format!(
                "no candidate root carries {KEYSPACE}.{TABLE_NAME}; {}",
                datasets_root::describe_search(KEYSPACE, TABLE_NAME)
            ),
        );
        return;
    };
    let fixture_dir = datasets_root::table_generation_dirs(&root, KEYSPACE, TABLE_NAME)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{KEYSPACE}.{TABLE_NAME}: no usable generation directory"));
    let schema = table_schema_for(TABLE_NAME);

    const ENTRY_COUNT: usize = 600;
    const MAX_RESIDENT_LOSSES: usize = 500; // must match recover.rs's own constant
                                            // Comfortably past the real fixture's own (tiny, real) data length —
                                            // EVERY entry below is implausible by construction, never a real
                                            // partition.
    const FAR_BEYOND_REAL_DATA: u64 = 50_000_000;

    let mut synthetic_index_bytes = Vec::new();
    for i in 0..ENTRY_COUNT {
        let key = (i as u32).to_be_bytes(); // 4-byte distinct ascending key
        synthetic_index_bytes.extend_from_slice(&(key.len() as u16).to_be_bytes());
        synthetic_index_bytes.extend_from_slice(&key);
        let data_offset = FAR_BEYOND_REAL_DATA + (i as u64) * 1000; // strictly ascending
        cqlite_core::storage::serialization::vint::encode_unsigned(
            data_offset,
            &mut synthetic_index_bytes,
        );
        cqlite_core::storage::serialization::vint::encode_unsigned(0, &mut synthetic_index_bytes);
        // promoted_len = 0
    }

    let temp = TempDir::new().expect("tempdir");
    let corrupt_dir = temp.path().join("corrupt_input");
    std::fs::create_dir_all(&corrupt_dir).expect("create corrupt input dir");
    for entry in std::fs::read_dir(&fixture_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.ends_with("-Index.db") {
            std::fs::write(corrupt_dir.join(&name), &synthetic_index_bytes)
                .expect("write synthetic Index.db");
        } else if !name_str.ends_with(".jsonl") && !name_str.ends_with("Statistics.db.txt") {
            std::fs::copy(entry.path(), corrupt_dir.join(&name)).expect("copy fixture component");
        }
    }
    let corrupt_data_db = single_data_db(&corrupt_dir);

    let out_temp = TempDir::new().expect("tempdir");
    let out_root = out_temp.path().join("out");
    let report = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        salvage_sstable(
            &corrupt_data_db,
            &out_root,
            &schema,
            SalvageOptions::default(),
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("salvage did not complete within 30s on {ENTRY_COUNT} losses"))
    .unwrap_or_else(|e| {
        panic!("salvage must not hard-error on this input (Losses, not an Err): {e:#}")
    });

    assert_eq!(
        report.partitions.total, ENTRY_COUNT,
        "boundary enumeration must name every synthetic entry"
    );
    // Every partition is provably a loss (every entry.data_offset is far
    // past the real data length) → a total loss → REFUSED, not a
    // partial-recovery report — spec R5.2. The manifest still names the
    // resident/truncated split via `losses`/`losses_truncated`.
    assert!(
        report.refused.is_some(),
        "every partition is implausible by construction — expected a total-loss refusal; got \
         {:?}",
        report
    );
    assert_eq!(
        report.losses.len(),
        MAX_RESIDENT_LOSSES,
        "resident losses must be capped at MAX_RESIDENT_LOSSES; got {}",
        report.losses.len()
    );
    assert_eq!(
        report.losses_truncated,
        ENTRY_COUNT - MAX_RESIDENT_LOSSES,
        "the truncated count must name EXACTLY the remainder — an affirmative count, not \
         silence; got {}",
        report.losses_truncated
    );
    // `partitions.lost` is the TRUE total, not just the resident subset.
    assert_eq!(
        report.partitions.lost, ENTRY_COUNT,
        "partitions.lost must be the TRUE total (resident + truncated), never just \
         losses.len(); got {}",
        report.partitions.lost
    );
    let rendered = report.render_text();
    assert!(
        rendered.contains(&format!(
            "{} more loss(es) truncated",
            ENTRY_COUNT - MAX_RESIDENT_LOSSES
        )),
        "text rendering must disclose the truncation explicitly; got: {rendered}"
    );
    eprintln!(
        "[issue_4196] {ENTRY_COUNT} synthetic losses: {} resident, {} truncated (counted, not \
         silent) — bounded well under an unconstrained {ENTRY_COUNT}-entry resident Vec.",
        report.losses.len(),
        report.losses_truncated
    );
}
