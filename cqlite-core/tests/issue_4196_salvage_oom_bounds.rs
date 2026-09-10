//! Issue #4196 — the unbounded-allocation (OOM/DoS) surface in
//! `write_engine::salvage`'s chunk-range/pre-flight computations, and the
//! `LossClass::Truncated` boundary-past-EOF classification that shares the
//! same code area.
//!
//! Split out of `issue_4196_salvage_corruption_corpus.rs` (round-12,
//! campsite rule / epic #1135) when that file crossed the ~1500-line
//! test-file threshold — a PURE MOVE, no behavior changed by the split.
//! Every value read from an on-disk component that feeds an allocation
//! size, range, or loop bound in `write_engine/salvage/` was audited
//! end-to-end (16-row table, `openspec/changes/sstable-salvage/tasks.md`,
//! round-11 pre-review self-audit); THREE independent entry points into the
//! defect class were found across rounds 9-11, each fixed and
//! regression-guarded here:
//!
//! 1. **Boundary-source side** (round 9) —
//!    `implausible_last_offset_does_not_oom_and_classifies_truncated`: a
//!    corrupted `Index.db` `data_offset` VInt, unbounded against the file's
//!    real length.
//! 2. **`CompressionInfo.data_length`** (round 10) —
//!    `implausible_compression_info_data_length_does_not_oom`: a single
//!    flipped byte in this ONE field bypassed round 9's clamp entirely
//!    (round 9's fix trusted it as already-bounded, which was false).
//! 3. **`CompressionInfo.chunk_offsets` array** (round 11) —
//!    `implausible_chunk_offset_table_does_not_oom`: `ChunkReader::read_chunk`'s
//!    buffer size, bounded only by offset ORDER, never against `Data.db`'s
//!    real length.
//!
//! `index_entry_offset_past_eof_classifies_truncated` (spec R2.3) sits
//! alongside them because it exercises the SAME early-return guard
//! (`decode_partition_at_offset_for_salvage`'s `offset_usize >= end` check)
//! that makes the boundary-source OOM fix's `Truncated` classification
//! reachable at all — the two are one code path's healthy-vs-adversarial
//! faces.
//!
//! # Oracle (design D6)
//!
//! Real Cassandra 5.0 fixtures (`test_comp.lz4_table`, `test_comp.uncompressed_table`,
//! `test_basic.multi_partition_table`) with ONE metadata field (`Index.db`
//! `data_offset`, `CompressionInfo.data_length`, or `CompressionInfo.chunk_offsets`)
//! re-encoded via the sanctioned fixture-synthesis writers
//! (`cqlite_core::storage::serialization::vint::encode_unsigned`,
//! `CompressionInfoWriter::build_to_vec`) — never a CQLite-written round-trip
//! fixture (issue #3042's lesson). `Data.db`/`CRC.db` stay byte-for-byte
//! unchanged in every case; only the ONE targeted metadata field is
//! corrupted, and each test's `#[tokio::test]` is wrapped in a 30s
//! `tokio::time::timeout` (measured to complete in well under a second
//! post-fix) so a regression fails cleanly instead of wedging the suite.
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

use salvage_corpus::{
    candidate_base_roots, no_data_db_anywhere, resolve_root_with_corpus_fixture, single_data_db,
    skip_or_require, split_big_index_entries, table_schema, CLEAN_KEYSPACE, CLEAN_TABLE_DIR,
};

/// Roborev, issue #4196 (round-9, spec R2.3 — `LossClass::Truncated` had no
/// test anywhere in the change: the corpus's ONLY truncation fixture,
/// `data_db_truncation`, is `test_comp.lz4_table` (COMPRESSED) — measured
/// (round 9) to classify `chunk-crc`, NOT `truncated`: `compressed_chunk_preflight`
/// walks every declared chunk BEFORE the per-partition loop ever runs, and a
/// chunk whose bytes no longer exist past the truncation point fails to
/// READ at all, landing in `bad_chunks` — the per-partition loop's
/// `chunk-crc` short-circuit fires before `recover_one_partition` is ever
/// reached, so `LossClass::Truncated`'s own code path is unreachable for a
/// COMPRESSED truncation, and a NAIVE uncompressed byte-truncation has the
/// SAME problem one layer down: truncating mid-row makes the row parser
/// return a hard decode `Err` (measured: "row_size=317 ... exceeds
/// available data"), landing in `PartitionAtOffsetOutcome::DecodeError` →
/// `LossClass::Decode`, not `Truncated` — `Truncated` is reached ONLY via
/// `decode_partition_at_offset_for_salvage`'s EARLY `offset_usize >= end`
/// check (`point_compaction.rs`), before any parsing is attempted at all.
///
/// This test therefore does NOT truncate `Data.db` — it leaves a healthy
/// `test_comp.uncompressed_table` generation's `Data.db`/`CRC.db` byte-for-
/// byte UNCHANGED (so the chunk-CRC pre-flight finds nothing to flag at
/// all: zero `bad_chunks`, zero `component_findings`) and instead
/// re-encodes a COPY of `Index.db` with its ONE entry's `data_offset` VInt
/// field changed from its true value (`0`) to `200000` — comfortably past
/// the real, unmutated `Data.db`'s 195018-byte length — while leaving the
/// entry's key bytes and the promoted-index payload untouched byte-for-byte
/// (`parse_big_index_entry`'s layout,
/// `cqlite-core/src/storage/sstable/index_reader/parse.rs`:
/// `[key_len: u16][key][data_offset: vint][promoted_len: vint][promoted]`).
/// `enumerate_boundaries`'s `check_strictly_ascending` guard trivially
/// passes (a single-entry list has no adjacent pair to violate), and
/// `decode_partition_at_offset_for_salvage`'s uncompressed branch then hits
/// `offset_usize(200000) >= end(195018 == section_len, since this is the
/// only/last entry)` on the FIRST line, before a single byte of `Data.db`
/// is ever read — the cleanest, most deterministic route to `Truncated`
/// this codebase has, and the reason NO Data.db mutation is needed at all.
///
/// R2.3's "all earlier partitions are recovered" narrows to the same
/// single-partition illustration `data_db_bit_flip`'s test already accepted
/// in round 6 (this fixture's Index.db, like `lz4_table`'s, names exactly
/// one partition) — the classification/derivation logic under test is
/// exercised identically regardless of partition count.
#[tokio::test]
async fn index_entry_offset_past_eof_classifies_truncated() {
    const KEYSPACE: &str = "test_comp";
    const TABLE_NAME: &str = "uncompressed_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "uncompressed_table fixture",
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

    let schema_path =
        datasets_root::schema_path("compression-parity.cql").expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE_NAME}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut schema = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    schema.keyspace = KEYSPACE.to_string();

    let clean_data_db = single_data_db(&fixture_dir);
    let clean_data_len = std::fs::metadata(&clean_data_db)
        .expect("stat clean Data.db")
        .len();
    let clean_index_db = fixture_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );
    let clean_index_bytes = std::fs::read(&clean_index_db).expect("read clean Index.db");

    // Rebuild the ONE entry with a fabricated `data_offset` comfortably past
    // EOF, keeping the key + promoted-index payload byte-for-byte. Parsed
    // structurally (not hardcoded byte offsets) so this test does not
    // silently mis-corrupt a different field if the fixture is regenerated
    // with a differently-sized key.
    let key_len = u16::from_be_bytes([clean_index_bytes[0], clean_index_bytes[1]]) as usize;
    let key_end = 2 + key_len;
    let (_orig_offset, consumed) =
        cqlite_core::parser::vint::decode_unsigned(&clean_index_bytes[key_end..])
            .expect("clean Index.db entry has a well-formed data_offset VInt");
    let rest_after_offset = &clean_index_bytes[key_end + consumed..];
    const FABRICATED_OFFSET: u64 = 200_000;
    assert!(
        FABRICATED_OFFSET > clean_data_len,
        "{KEYSPACE}.{TABLE_NAME}: fabricated offset {FABRICATED_OFFSET} must exceed the clean \
         Data.db's real length {clean_data_len}, or this test proves nothing"
    );
    let mut corrupt_index_bytes = Vec::with_capacity(clean_index_bytes.len() + 8);
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[..key_end]);
    cqlite_core::storage::serialization::vint::encode_unsigned(
        FABRICATED_OFFSET,
        &mut corrupt_index_bytes,
    );
    corrupt_index_bytes.extend_from_slice(rest_after_offset);

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
            std::fs::write(corrupt_dir.join(&name), &corrupt_index_bytes)
                .expect("write corrupted Index.db");
        } else if !name_str.ends_with(".jsonl") && !name_str.ends_with("Statistics.db.txt") {
            std::fs::copy(entry.path(), corrupt_dir.join(&name)).expect("copy fixture component");
        }
    }
    let corrupt_data_db = single_data_db(&corrupt_dir);

    let out_root = temp.path().join("out");
    let report = salvage_sstable(
        &corrupt_data_db,
        &out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on an out-of-range Index.db offset (a Loss, not an \
             Err): {e:#}"
        )
    });

    assert_eq!(
        report.partitions.total, 1,
        "{KEYSPACE}.{TABLE_NAME}: expected exactly one partition in this fixture's Index.db; \
         got {}",
        report.partitions.total
    );
    assert_eq!(
        report.losses.len(),
        1,
        "expected exactly one loss (the file's only partition); got {:?}",
        report.losses
    );
    assert_eq!(
        report.losses[0].class,
        LossClass::Truncated,
        "expected LossClass::Truncated; got {:?} (component findings: {:?})",
        report.losses[0],
        report.component_findings
    );
    assert!(
        report.component_findings.is_empty(),
        "Data.db/CRC.db were never touched — no chunk-CRC finding should fire; got {:?}",
        report.component_findings
    );
    assert!(
        no_data_db_anywhere(&out_root),
        "--out must contain no Data.db when the file's only partition is a total loss"
    );
    eprintln!(
        "[issue_4196] Index.db offset past EOF: salvage classified the loss as {:?} as \
         expected.",
        report.losses[0].class
    );
}
/// Roborev, issue #4196 (round-9 High finding, `chunks.rs:49`): the
/// PRECEDING partition's chunk-range `end` (`recover.rs`'s
/// `chunk_range_end`) for a NON-last boundary entry comes directly from the
/// NEXT entry's `data_offset` — an unbounded, unvalidated VInt with no
/// sanity check against the file's real length anywhere between
/// `parse_big_index_entry` and `chunks_for_range`. A single flipped byte in
/// that NEXT entry's offset therefore makes `chunks_for_range` try to
/// materialize an astronomically large `Vec<u64>` (an offset near
/// `u64::MAX` implies ~1.4e14 chunk indices) BEFORE a single partition is
/// decoded — OOM, not the classified refusal the design promises. Fixed by
/// clamping `chunk_range_end` to the independently-measured `data_length`
/// and refusing to compute a chunk range at all for an entry whose OWN
/// `data_offset` is already implausible (classified `Truncated`
/// immediately).
///
/// This test corrupts the LAST entry's `data_offset` (the only position
/// `check_strictly_ascending` places NO upper bound on — see
/// `swapped_index_entry_keys_classify_key_mismatch`'s doc for the same
/// geometry) to `u64::MAX / 2` in a copy of `test_basic.multi_partition_table`
/// (100 partitions). This is BOTH the failure mode itself (the corrupted
/// entry's own chunk range) AND the exposure vector for the SECOND-TO-LAST
/// entry (whose `chunk_range_end` is this corrupted value) — covering both
/// halves of the fix in one fixture.
///
/// MEASURED (this round): the second-to-last partition is ALSO lost, not
/// cleanly recovered — clamping its `chunk_range_end` to `data_length` (the
/// best available bound once the true next-entry offset is gone) widens its
/// declared window to cover what were ORIGINALLY the corrupted last
/// partition's own bytes too (still physically present in `Data.db`, just
/// unindexed). The decoder correctly refuses to silently treat those extra
/// bytes as more of THIS partition (which would mean fabricating rows under
/// the wrong key, the exact resurrection hazard design D2 exists to
/// prevent) and reports `Truncated` instead — a SAFE, conservative outcome
/// given the corruption, just not the "only the corrupted entry itself is
/// lost" shape a first guess might expect. Asserted as measured (2 losses),
/// not as originally assumed (1).
///
/// Round-12 Medium finding, additionally: the second-to-last partition's
/// loss ALSO now exercises `decode_partition_at_offset_for_salvage`'s own
/// `end > safe_data_length` bound (added round 12) — its `end_bound` IS the
/// corrupted last entry's offset, passed RAW into that function. This
/// fixture's real `Data.db` is small (~13 KB) so the bound's absence was
/// never independently OBSERVABLE via timing here (reading "the rest of
/// the file" completes instantly either way) — the fix matters for a
/// genuinely large production `Data.db`, where the pre-fix behavior would
/// materialize the remaining decompressed section into one resident `Vec`
/// before reaching the SAME `Truncated` verdict. Verified by code
/// inspection (the guard fires before `pull_chunk_window` is ever called)
/// rather than by a timing difference this small fixture cannot exhibit.
#[tokio::test]
async fn implausible_last_offset_does_not_oom_and_classifies_truncated() {
    const KEYSPACE: &str = "test_basic";
    const TABLE_NAME: &str = "multi_partition_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "multi_partition_table fixture",
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

    let schema_path = datasets_root::schema_path("basic-types.cql").expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE_NAME}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut schema = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    schema.keyspace = KEYSPACE.to_string();

    let clean_data_db = single_data_db(&fixture_dir);
    let clean_index_db = fixture_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );
    let clean_index_bytes = std::fs::read(&clean_index_db).expect("read clean Index.db");
    let entries = split_big_index_entries(&clean_index_bytes);
    let total = entries.len();
    assert!(
        total >= 3,
        "{KEYSPACE}.{TABLE_NAME}: need at least 3 partitions; found {total}"
    );
    let (last_start, last_key_end, last_end) = entries[total - 1];

    let mut corrupt_index_bytes = Vec::with_capacity(clean_index_bytes.len());
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[..last_start]);
    // The last entry's key portion, unchanged.
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[last_start..last_key_end]);
    const IMPLAUSIBLE_OFFSET: u64 = u64::MAX / 2;
    cqlite_core::storage::serialization::vint::encode_unsigned(
        IMPLAUSIBLE_OFFSET,
        &mut corrupt_index_bytes,
    );
    // Re-parse the ORIGINAL promoted_len + payload from the clean entry
    // (everything after ITS OWN original data_offset field) so only the
    // offset itself changes.
    let (_orig_offset, offset_consumed) =
        cqlite_core::parser::vint::decode_unsigned(&clean_index_bytes[last_key_end..])
            .expect("clean Index.db entry has a well-formed data_offset VInt");
    corrupt_index_bytes
        .extend_from_slice(&clean_index_bytes[last_key_end + offset_consumed..last_end]);

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
            std::fs::write(corrupt_dir.join(&name), &corrupt_index_bytes)
                .expect("write corrupted Index.db");
        } else if !name_str.ends_with(".jsonl") && !name_str.ends_with("Statistics.db.txt") {
            std::fs::copy(entry.path(), corrupt_dir.join(&name)).expect("copy fixture component");
        }
    }
    let corrupt_data_db = single_data_db(&corrupt_dir);

    let out_root = temp.path().join("out");
    // The bound itself is the assertion: pre-fix this materializes ~1.1 PB
    // and OOM-kills the process; post-fix it must complete in well under a
    // second. `tokio::time::timeout` turns an unbounded hang/OOM into a
    // clean test failure instead of wedging the whole suite.
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
            "salvage did not complete within 30s on an implausible boundary offset — the \
             unbounded chunk-range allocation regressed"
        )
    })
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on an implausible Index.db offset (a Loss, not an \
             Err): {e:#}"
        )
    });

    assert_eq!(
        report.partitions.total, total,
        "expected the untouched partition count; got {}",
        report.partitions.total
    );
    // MEASURED (see this test's doc): the corrupted last entry AND the
    // second-to-last entry (whose widened, clamped window now overlaps what
    // were originally the last partition's own bytes) both classify as
    // losses — a safe, conservative outcome, not the corrupted entry alone.
    assert_eq!(
        report.losses.len(),
        2,
        "expected exactly the corrupted last partition plus the second-to-last (whose clamped \
         window now overlaps it) as losses; got {:?}",
        report.losses
    );
    for loss in &report.losses {
        assert_eq!(
            loss.class,
            LossClass::Truncated,
            "expected LossClass::Truncated for both affected slots; got {:?}",
            loss
        );
    }
    assert_eq!(
        report.partitions.recovered,
        total - 2,
        "every OTHER partition (not adjacent to the corrupted entry) must still recover cleanly"
    );
    eprintln!(
        "[issue_4196] implausible last-entry offset ({IMPLAUSIBLE_OFFSET}): completed without \
         OOM, {} of {total} partitions lost (both Truncated), {} recovered.",
        report.losses.len(),
        report.partitions.recovered
    );
}
/// Roborev, issue #4196 (round-10 High finding): round-9's OOM fix clamped
/// `chunk_range_end` to `data_length` on the premise that `data_length` is
/// "the REAL, independently-measured total, already size-bounded at its own
/// parse site" — FALSE for the COMPRESSED branch, where `data_length` is
/// taken VERBATIM from `CompressionInfo.db`'s 8-byte field.
/// `CompressionInfo::validate` bounds `chunk_count`/`chunk_length`/
/// `max_compressed_length`/offset monotonicity but never cross-checks
/// `data_length` against them, so a single flipped byte in THAT one field
/// (chunk offsets, chunk table and `Data.db` all intact, so
/// `compressed_chunk_preflight` reports zero bad chunks) reinstated the
/// exact unbounded chunk-range allocation round-9 removed for the
/// boundary-source case. Fixed: `compressed_chunk_preflight` now returns
/// `data_length.min(chunk_count * chunk_length)` — both factors already
/// independently bounded by `CompressionInfo::validate`.
///
/// This test corrupts ONLY `CompressionInfo.db`'s `data_length` field (via
/// the sanctioned fixture-synthesis route: parse the clean file with
/// `CompressionInfo::parse`, rebuild a `CompressionMetadata` with every
/// OTHER field byte-identical, re-serialize with
/// `CompressionInfoWriter::build_to_vec` — the same writer module's own doc
/// names exactly this "read-path fixtures legitimately use [it] to
/// synthesize compressed SSTables" as sanctioned, issue #1406) to
/// `2^62` in a copy of `test_comp.lz4_table`. `Data.db`/`Index.db`/`CRC.db`
/// stay byte-for-byte unchanged.
///
/// MEASURED (this round): the partition is classified `Truncated`, NOT
/// cleanly recovered — a first guess. `chunks.rs`'s clamp fixes the
/// PRE-FLIGHT's own chunk-range materialization (the OOM this finding is
/// about), but `decode_partition_at_offset_for_salvage`'s OWN internal
/// `end` resolution for a LAST/compressed partition (`end_bound == None`)
/// reads `self.compression_info.data_length` DIRECTLY off the reader — a
/// SEPARATE, unclamped copy of the same corrupted field — and asks
/// `pull_chunk_window` to read out to that (still-corrupted) `end`.
/// `pull_chunk_window` itself is ALREADY safe (it reads real, bounded
/// chunks one at a time and simply cannot reach an unreachable `end`,
/// setting `reached_end = false` rather than pre-allocating for it) — so no
/// second OOM exists here, just a correctly conservative `Truncated`
/// verdict once the requested window turns out to be unsatisfiable. Both
/// halves (no OOM, no silent wrong-data acceptance) are exactly what the
/// design promises; asserted as measured, not as first assumed.
#[tokio::test]
async fn implausible_compression_info_data_length_does_not_oom() {
    let Some(root) = resolve_root_with_corpus_fixture("data_db_bit_flip") else {
        // Reuses the clean-source presence check `data_db_bit_flip`'s test
        // already relies on (this test only needs the CLEAN `lz4_table`
        // source, not that specific corrupt fixture).
        skip_or_require(
            "lz4_table clean source",
            &format!(
                "no candidate root carries sstables/{CLEAN_KEYSPACE}/{CLEAN_TABLE_DIR}; searched \
                 {:?}",
                candidate_base_roots()
            ),
        );
        return;
    };
    let clean_dir = root
        .join("sstables")
        .join(CLEAN_KEYSPACE)
        .join(CLEAN_TABLE_DIR);
    let schema = table_schema();
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

    const IMPLAUSIBLE_DATA_LENGTH: u64 = 1u64 << 62;
    assert!(
        IMPLAUSIBLE_DATA_LENGTH > clean_ci.data_length,
        "the fabricated data_length must exceed the real one, or this test proves nothing"
    );
    let algorithm =
        cqlite_core::storage::sstable::writer::CompressionAlgorithm::from_cassandra_name(
            &clean_ci.algorithm,
        )
        .unwrap_or_else(|| panic!("unrecognized compressor name: {}", clean_ci.algorithm));
    let corrupt_metadata = cqlite_core::storage::sstable::writer::CompressionMetadata {
        algorithm,
        chunk_length: clean_ci.chunk_length,
        max_compressed_length: clean_ci.max_compressed_length,
        data_length: IMPLAUSIBLE_DATA_LENGTH,
        chunk_offsets: clean_ci.chunk_offsets.clone(),
        option_pairs: clean_ci.option_pairs.clone(),
    };
    let corrupt_ci_bytes =
        cqlite_core::storage::sstable::writer::CompressionInfoWriter::new(clean_ci_path.clone())
            .build_to_vec(&corrupt_metadata)
            .expect("re-serialize CompressionInfo.db with a fabricated data_length");

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
    // The bound itself is the assertion, exactly as
    // `implausible_last_offset_does_not_oom_and_classifies_truncated`
    // (round 9) — pre-fix this materializes ~1.1 PB and OOM-kills the
    // process; post-fix it must complete in well under a second.
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
            "salvage did not complete within 30s on an implausible CompressionInfo.data_length \
             — the unbounded chunk-range allocation regressed"
        )
    })
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on a fabricated CompressionInfo.data_length (a Loss \
             or a clean recovery, not an Err): {e:#}"
        )
    });

    // MEASURED (see this test's doc): the corrupted `data_length` makes
    // `decode_partition_at_offset_for_salvage`'s OWN internal `end`
    // resolution (a SEPARATE, unclamped read of the same field, reached
    // only when `end_bound == None` for a last/compressed partition)
    // request a window the real chunk table cannot satisfy —
    // `pull_chunk_window` correctly reports `reached_end = false` rather
    // than fabricating data or allocating unboundedly, so the ONLY safe
    // verdict is `Truncated`. The property under test is "no OOM, no
    // silent wrong-data acceptance", not "recovers cleanly" — asserted as
    // measured, not as originally assumed.
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
        "expected LossClass::Truncated (the requested window past the real chunk table cannot \
         be satisfied); got {:?}",
        report.losses[0]
    );
    assert_eq!(report.partitions.recovered, 0);
    eprintln!(
        "[issue_4196] implausible CompressionInfo.data_length ({IMPLAUSIBLE_DATA_LENGTH}): \
         completed without OOM, classified {:?} (no silent wrong-data acceptance).",
        report.losses[0].class
    );
}
/// Roborev, issue #4196 (round-11 Medium finding): `compressed_chunk_preflight`
/// walks every chunk through `ChunkReader::read_chunk`, which sizes its
/// allocation as `compressed_chunk_size(i, total_size)` — for a NON-LAST
/// chunk, `chunk_offsets[i+1] - chunk_offsets[i]`
/// (`compression_info.rs::compressed_chunk_size`), bounded ONLY by
/// `CompressionInfo::validate`'s ASCENDING-order check (`checked_sub`
/// guards underflow, nothing else) — never cross-checked against `Data.db`'s
/// real length. A corrupt-but-still-ascending offset table is therefore a
/// THIRD entry point into the SAME unbounded-allocation class rounds 9
/// (boundary-source `data_offset`) and 10 (`CompressionInfo.data_length`)
/// fixed, reachable through a field NEITHER of those fixes touches. Fixed:
/// `compressed_chunk_preflight` now bounds each chunk's declared
/// `[offset, offset+size)` against the real, already-measured `total_size`
/// BEFORE ever calling `read_chunk` — an implausible chunk is recorded bad
/// (never read) instead of handed to an allocation sized from the
/// untrusted field.
///
/// This test corrupts ONLY `CompressionInfo.db`'s `chunk_offsets` array
/// (via the same sanctioned `CompressionInfoWriter::build_to_vec` synthesis
/// route rounds 10/11 use) — shifting every offset from index 1 onward up
/// to start at `2^50` while preserving their RELATIVE gaps (so ascending
/// order — the ONE thing `CompressionInfo::validate` checks — still holds,
/// and only the DECLARED size of chunk 0 explodes to ~2^50 bytes; chunks
/// 1..N's declared sizes stay exactly as they were, though their declared
/// offsets no longer correspond to real bytes in the unchanged `Data.db`
/// either). `Data.db`/`Index.db`/`CRC.db` stay byte-for-byte unchanged.
#[tokio::test]
async fn implausible_chunk_offset_table_does_not_oom() {
    let Some(root) = resolve_root_with_corpus_fixture("data_db_bit_flip") else {
        skip_or_require(
            "lz4_table clean source",
            &format!(
                "no candidate root carries sstables/{CLEAN_KEYSPACE}/{CLEAN_TABLE_DIR}; searched \
                 {:?}",
                candidate_base_roots()
            ),
        );
        return;
    };
    let clean_dir = root
        .join("sstables")
        .join(CLEAN_KEYSPACE)
        .join(CLEAN_TABLE_DIR);
    let schema = table_schema();
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
        "need at least 2 chunks to demonstrate a shifted (but still ascending) offset table"
    );

    const SHIFT_BASE: u64 = 1u64 << 50;
    let anchor = clean_ci.chunk_offsets[1];
    let mut corrupt_offsets = clean_ci.chunk_offsets.clone();
    for off in corrupt_offsets.iter_mut().skip(1) {
        *off = SHIFT_BASE + (*off - anchor);
    }
    // Ascending order preserved by construction (a monotonic shift of an
    // already-ascending tail), which is the ONLY thing
    // `CompressionInfo::validate` checks — asserted here so a future change
    // to the shift arithmetic fails LOUDLY in this test rather than
    // producing a refusal at a different, unintended site.
    for w in corrupt_offsets.windows(2) {
        assert!(
            w[1] > w[0],
            "corrupted offset table must stay strictly ascending: {corrupt_offsets:?}"
        );
    }

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
        chunk_offsets: corrupt_offsets,
        option_pairs: clean_ci.option_pairs.clone(),
    };
    let corrupt_ci_bytes =
        cqlite_core::storage::sstable::writer::CompressionInfoWriter::new(clean_ci_path.clone())
            .build_to_vec(&corrupt_metadata)
            .expect("re-serialize CompressionInfo.db with a shifted chunk_offsets table");

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
    // The bound itself is the assertion, exactly as the round-9/10 OOM
    // tests: pre-fix, chunk 0 alone allocates ~1 PB and the process is
    // OOM-killed; post-fix this must complete in well under a second.
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
            "salvage did not complete within 30s on a shifted chunk_offsets table — the \
             unbounded chunk-buffer allocation regressed"
        )
    })
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on a shifted CompressionInfo chunk_offsets table (a \
             Loss or a classified Refusal, not an Err): {e:#}"
        )
    });

    // Every chunk's declared position now exceeds Data.db's real length, so
    // every chunk is bad and the file's one partition is a chunk-crc total
    // loss — NOT a refusal (the pre-flight itself completes and reports
    // classified findings; it does not hard-fail).
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
        LossClass::ChunkCrc,
        "expected LossClass::ChunkCrc (every declared chunk position is implausible); got {:?}",
        report.losses[0]
    );
    eprintln!(
        "[issue_4196] shifted chunk_offsets table: completed without OOM, classified {:?} (no \
         chunk handed to an allocation sized from the untrusted offset table).",
        report.losses[0].class
    );
}

/// Roborev, issue #4196 (round-12 Medium finding): both OOM guards
/// `recover.rs` added in rounds 9/10 are conditioned on `data_length > 0` —
/// `CompressionInfo::validate` bounds `algorithm`/`chunk_length`/offset
/// ordering but places NO bound on `data_length` at all (unlike
/// `chunk_count`, capped in `CompressionInfo::parse`), so a ZEROED
/// `data_length` field parses fine, and `chunks.rs`'s
/// `min(chunk_table_bound)` clamp used to yield exactly `0` for it too —
/// silently DISABLING both the `entry.data_offset >= data_length` short-
/// circuit and the `chunk_range_end.min(data_length)` clamp (both read `0`
/// as "unknown/disabled", matching the UNCOMPRESSED "no CRC.db" case's
/// legitimate `0`), reinstating the exact unbounded `chunks_for_range`
/// materialization rounds 9/10 fixed — through the OPPOSITE corruption
/// direction (zeroed rather than inflated) from `implausible_compression_info_data_length_does_not_oom`.
/// Fixed: `chunks.rs` now falls back to the structurally-derived
/// `chunk_table_bound` (`chunk_count * chunk_length`, always positive for a
/// validated `CompressionInfo`) whenever the declared `data_length` is
/// exactly `0`, instead of propagating a value that disables clamping.
///
/// This test combines BOTH corruptions needed to demonstrate the actual
/// risk (data_length alone, without an adjacent implausible boundary
/// entry, cannot by itself materialize anything large): `CompressionInfo.data_length`
/// zeroed, AND the LAST `Index.db` entry's `data_offset` corrupted to
/// `u64::MAX / 2` (the same construction
/// `implausible_last_offset_does_not_oom_and_classifies_truncated` uses,
/// reused here since `check_strictly_ascending` places no upper bound only
/// on the LAST entry).
#[tokio::test]
async fn implausible_zeroed_data_length_does_not_oom() {
    const KEYSPACE: &str = "test_basic";
    const TABLE_NAME: &str = "multi_partition_table";
    let Some(root) = datasets_root::sstables_root_for_table(KEYSPACE, TABLE_NAME) else {
        skip_or_require(
            "multi_partition_table fixture",
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

    let schema_path = datasets_root::schema_path("basic-types.cql").expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {TABLE_NAME}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut schema = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    schema.keyspace = KEYSPACE.to_string();

    // Corruption 1: the LAST Index.db entry's data_offset, same construction
    // as `implausible_last_offset_does_not_oom_and_classifies_truncated`.
    let clean_data_db = single_data_db(&fixture_dir);
    let clean_index_db = fixture_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-Index.db"),
    );
    let clean_index_bytes = std::fs::read(&clean_index_db).expect("read clean Index.db");
    let entries = split_big_index_entries(&clean_index_bytes);
    let total = entries.len();
    assert!(
        total >= 3,
        "{KEYSPACE}.{TABLE_NAME}: need at least 3 partitions; found {total}"
    );
    let (last_start, last_key_end, last_end) = entries[total - 1];
    let mut corrupt_index_bytes = Vec::with_capacity(clean_index_bytes.len());
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[..last_start]);
    corrupt_index_bytes.extend_from_slice(&clean_index_bytes[last_start..last_key_end]);
    const IMPLAUSIBLE_OFFSET: u64 = u64::MAX / 2;
    cqlite_core::storage::serialization::vint::encode_unsigned(
        IMPLAUSIBLE_OFFSET,
        &mut corrupt_index_bytes,
    );
    let (_orig_offset, offset_consumed) =
        cqlite_core::parser::vint::decode_unsigned(&clean_index_bytes[last_key_end..])
            .expect("clean Index.db entry has a well-formed data_offset VInt");
    corrupt_index_bytes
        .extend_from_slice(&clean_index_bytes[last_key_end + offset_consumed..last_end]);

    // Corruption 2: CompressionInfo.data_length zeroed, via the sanctioned
    // fixture-synthesis route (rounds 10/11's own pattern).
    let clean_ci_path = fixture_dir.join(
        clean_data_db
            .file_name()
            .expect("Data.db has a filename")
            .to_string_lossy()
            .replace("-Data.db", "-CompressionInfo.db"),
    );
    let clean_ci_bytes = std::fs::read(&clean_ci_path).expect("read clean CompressionInfo.db");
    let clean_ci = CompressionInfo::parse(&clean_ci_bytes).expect("parse clean CompressionInfo.db");
    let algorithm =
        cqlite_core::storage::sstable::writer::CompressionAlgorithm::from_cassandra_name(
            &clean_ci.algorithm,
        )
        .unwrap_or_else(|| panic!("unrecognized compressor name: {}", clean_ci.algorithm));
    let corrupt_metadata = cqlite_core::storage::sstable::writer::CompressionMetadata {
        algorithm,
        chunk_length: clean_ci.chunk_length,
        max_compressed_length: clean_ci.max_compressed_length,
        data_length: 0, // <-- the corruption under test
        chunk_offsets: clean_ci.chunk_offsets.clone(),
        option_pairs: clean_ci.option_pairs.clone(),
    };
    let corrupt_ci_bytes =
        cqlite_core::storage::sstable::writer::CompressionInfoWriter::new(clean_ci_path.clone())
            .build_to_vec(&corrupt_metadata)
            .expect("re-serialize CompressionInfo.db with data_length zeroed");

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
            std::fs::write(corrupt_dir.join(&name), &corrupt_index_bytes)
                .expect("write corrupted Index.db");
        } else if name_str.ends_with("-CompressionInfo.db") {
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
            "salvage did not complete within 30s on a zeroed CompressionInfo.data_length plus \
             an implausible last-entry offset — the unbounded chunk-range allocation regressed"
        )
    })
    .unwrap_or_else(|e| {
        panic!(
            "salvage must not hard-error on this combined corruption (a Loss or a classified \
             Refusal, not an Err): {e:#}"
        )
    });

    // MEASURED: 2 losses (not 1) — the SAME shape
    // `implausible_last_offset_does_not_oom_and_classifies_truncated`
    // establishes: the corrupted LAST entry itself (caught by `recover.rs`'s
    // own `entry.data_offset >= data_length` pre-check, using
    // `chunks.rs`'s now-fixed `chunk_table_bound` fallback) AND the
    // SECOND-TO-LAST entry (whose `end_bound` IS the corrupted last
    // entry's offset, caught by THIS fix's `end > safe_len` check inside
    // `decode_partition_at_offset_for_salvage`) — every OTHER partition
    // decodes normally, proving the zero-`data_length` fallback does not
    // over-refuse legitimate partitions whose `end_bound` was never
    // corrupted.
    assert_eq!(
        report.partitions.total, total,
        "expected the untouched partition count; got {}",
        report.partitions.total
    );
    assert_eq!(
        report.losses.len(),
        2,
        "expected exactly the corrupted last partition plus the second-to-last (whose end_bound \
         IS the corrupted value); got {:?}",
        report.losses
    );
    for loss in &report.losses {
        assert_eq!(
            loss.class,
            LossClass::Truncated,
            "expected LossClass::Truncated for both affected slots; got {:?}",
            loss
        );
    }
    assert_eq!(
        report.partitions.recovered,
        total - 2,
        "every OTHER partition (whose end_bound was never corrupted) must still recover \
         cleanly — the zero-data_length fallback must not over-refuse legitimate partitions"
    );
    eprintln!(
        "[issue_4196] zeroed CompressionInfo.data_length + implausible last offset: completed \
         without OOM, {} of {total} partitions lost (both Truncated), {} recovered.",
        report.losses.len(),
        report.partitions.recovered
    );
}
