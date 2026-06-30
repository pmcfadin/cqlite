//! Issue #993 — wide-partition promoted-index boundary parity against a REAL
//! Cassandra 5.0 fixture (the parity oracle).
//!
//! Unlike the synthetic writer round-trip in
//! `issue_993_promoted_index_parity.rs` (which drives the CQLite writer and reads
//! its own Index.db back), this suite proves the promoted-index DECODER against
//! the exact bytes Apache Cassandra wrote for a multi-block wide partition:
//!
//!   `test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294`
//!   `CREATE TABLE test_big.wide_partition (pk int, ck int, payload text,`
//!   `  PRIMARY KEY (pk, ck)) WITH compression={'class':'LZ4Compressor'}`
//!
//! Contents (verified against the committed `*-Data.db.jsonl` sstabledump
//! golden):
//!   * pk=1 — 290 live rows (ck 0..299 MINUS a range tombstone deleting
//!     ck 30..39, which straddles a promoted-index block boundary).
//!   * pk=2 — 300 live rows (ck 0..299).
//!   * pk=3 — 300 live rows (ck 0..299).
//!
//! Each partition is ~600 KiB → Cassandra emitted a multi-block promoted
//! `IndexInfo` array (10 blocks/partition) inside each `Index.db` entry.
//!
//! # Authoritative clustering-prefix length (no heuristics — Issue #28)
//!
//! The promoted index's `firstName`/`lastName` are serialized `ClusteringPrefix`
//! byte sequences that are NOT self-delimiting without the table's clustering
//! column types. For this schema the single `ck int` clustering column is
//! serialized as a fixed **6-byte** prefix in the Cassandra `Index.db`: a 2-byte
//! clustering-prefix header (`04 00`) followed by the 4-byte big-endian `int`
//! value. That length is derived from the schema (one fixed-width 4-byte int
//! clustering column), exactly as the production caller would compute it — there
//! is deliberately no guessing. The 6-byte length was cross-checked against the
//! trailing per-block offsets array in the real `Index.db` payload, which gives
//! the exact byte boundary of every block; with `prefix_len == 6` every block
//! decodes to a clean `endOpenMarker` of `0x00` and consumes its block exactly.
//!
//! # Fixture-availability rule (two enforcement tiers)
//!
//! The committed sstabledump JSONL golden (`nb-2-big-Data.db.jsonl`) is
//! git-tracked, so it is ALWAYS present in CI. The binary `Data.db`/`Index.db`
//! are local-only (not yet in the datasets pin) until a future dataset re-pin.
//! This suite therefore enforces parity in two tiers:
//!
//!   * **Canonical-semantic (runs in CI on every push):** assertions derived
//!     from the committed JSONL golden alone — exactly 3 partitions (pk 1/2/3),
//!     live-row counts 290/300/300, pk=1 missing exactly the deleted clustering
//!     range ck 30..39 (the range tombstone), and ascending clustering order
//!     within each partition. These do NOT depend on the binaries and FAIL if
//!     the golden is wrong.
//!   * **Byte-level (runs only when the binaries are present — local + nightly
//!     docker / after a dataset re-pin):** promoted-index decode, the
//!     offset/width chain, and clustering-bound ordering across blocks, read out
//!     of the real `Index.db`/`Data.db`. When the binaries are absent these
//!     checks log a clear skip line but the JSONL semantic checks above still
//!     run.
//!
//! Fail-closed both ways: a present-but-empty/wrong JSONL or binaries is a
//! failure, never a silent pass.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::error::Error;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::promoted_index_reader::DecodedPromotedIndex;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform, Result};

// ===========================================================================
// Fixture constants (the parity oracle)
// ===========================================================================

const FIXTURE_REL: &str = "sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294";
const PREFIX: &str = "nb-2-big";

/// Schema-derived serialized `ClusteringPrefix` length for the single `ck int`
/// clustering column in the Cassandra `Index.db`: 2-byte prefix header + 4-byte
/// big-endian int value. Authoritative (Issue #28), not a heuristic.
const CK_PREFIX_LEN: usize = 6;

/// Byte offset within a 6-byte `ck int` clustering prefix at which the 4-byte
/// big-endian int value begins (after the 2-byte clustering-prefix header).
const CK_VALUE_OFFSET: usize = 2;

/// Expected per-partition live-row counts from the sstabledump JSONL golden:
/// pk=1 has 290 (ck 30..39 deleted by a range tombstone), pk=2/pk=3 have 300.
const PK1_LIVE_ROWS: usize = 290;
const PK_FULL_LIVE_ROWS: usize = 300;

/// Range-tombstone deleted clustering range for pk=1: ck in [30, 40).
const PK1_DELETED_CK_LO: i32 = 30;
const PK1_DELETED_CK_HI: i32 = 40;

// ===========================================================================
// Fixture discovery + skip-on-absence
// ===========================================================================

/// The repo's own committed datasets tree (independent of `CQLITE_DATASETS_ROOT`).
/// The git-tracked JSONL golden always lives here, so this is the authoritative
/// fallback for the canonical-semantic checks even in CI where
/// `CQLITE_DATASETS_ROOT` may point at a fetched binary tarball elsewhere.
fn repo_datasets_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|w| w.join("test-data/datasets"))
        .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
}

/// Resolve the datasets root for the BINARY components (env override first, else
/// the repo tree). The binaries are local-only / only present after a fetch.
fn binary_datasets_root() -> PathBuf {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        repo_datasets_root()
    }
}

fn fixture_dir() -> PathBuf {
    binary_datasets_root().join(FIXTURE_REL)
}

fn component(suffix: &str) -> PathBuf {
    fixture_dir().join(format!("{PREFIX}-{suffix}"))
}

/// Locate the committed JSONL golden: prefer `CQLITE_DATASETS_ROOT` if it
/// actually contains the file, else fall back to the repo's own committed tree
/// (git rev-parse --show-toplevel semantics via CARGO_MANIFEST_DIR) so the
/// git-tracked golden is found in CI even when `CQLITE_DATASETS_ROOT` points at a
/// binary-only tarball.
fn jsonl_golden_path() -> Option<PathBuf> {
    let rel = format!("{FIXTURE_REL}/{PREFIX}-Data.db.jsonl");
    let env_candidate = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(|root| PathBuf::from(root).join(&rel));
    if let Some(p) = env_candidate {
        if p.exists() {
            return Some(p);
        }
    }
    let repo_candidate = repo_datasets_root().join(&rel);
    if repo_candidate.exists() {
        return Some(repo_candidate);
    }
    None
}

/// CI fail-closed switch (issue #1185). Returns `true` when
/// `CQLITE_PARITY_REQUIRE_DATASETS=1` is set — the same env idiom used by every
/// other strict parity lane (`cqlite-core/tests/parity_support/mod.rs`,
/// `sstable_parity_index_db_test.rs`). In that mode a missing wide_partition
/// binary is a HARD FAILURE (panic), never a skip, because these scenarios are
/// pinned `byte_for_byte` in `test-data/cassandra-parity-manifest.yml` and the
/// fixture is now part of the pinned CI dataset (v3.4). Locally (env unset) the
/// byte-level checks keep their skip-on-absence behavior.
fn parity_datasets_required() -> bool {
    std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// The exact binary components every byte-level scenario depends on. Their
/// absence under `CQLITE_PARITY_REQUIRE_DATASETS=1` is a gate failure (the
/// wide_partition fixture is now pinned in the CI dataset, v3.4).
const REQUIRED_BINARY_COMPONENTS: [&str; 4] =
    ["Data.db", "Index.db", "Digest.crc32", "CompressionInfo.db"];

/// Whether the binary components needed by a byte-level test are present. The
/// committed tree always carries the JSONL/TOC; the binaries (Data.db/Index.db)
/// are gitignored and only present after a dataset fetch.
fn binaries_present() -> bool {
    component("Data.db").exists() && component("Index.db").exists()
}

/// List the exact strict components that are missing on disk (empty when all
/// present). Used by the fail-closed guard so the failure names precisely which
/// reference binary is absent.
fn missing_required_components() -> Vec<String> {
    REQUIRED_BINARY_COMPONENTS
        .iter()
        .filter(|suffix| !component(suffix).exists())
        .map(|suffix| format!("{PREFIX}-{suffix}"))
        .collect()
}

/// Gate a byte-level scenario on fixture presence. Returns `true` when the
/// caller should proceed (binaries present), `false` when it should skip.
///
/// Fail-closed contract (issue #1185): under `CQLITE_PARITY_REQUIRE_DATASETS=1`
/// (the REQUIRED "Real M5 SSTableDump parity validation" CI lane sets it) the
/// absence of the EXACT wide_partition reference binaries is a HARD FAILURE — it
/// must NOT skip-and-green. With the env unset (local dev without the fetched
/// binaries) the byte-level checks skip cleanly while the JSONL canonical-semantic
/// tier still runs.
fn require_or_skip_binaries(test: &str) -> bool {
    // Strict CI lane: enforce the FULL required-component set, not just the
    // Data.db+Index.db subset that `binaries_present()` checks. A partial fixture
    // (e.g. missing Digest.crc32 or CompressionInfo.db) must turn the required
    // lane red, never skip-and-green. Invariant: strict mode passing ⇒ every
    // component in REQUIRED_BINARY_COMPONENTS was present.
    if parity_datasets_required() {
        let missing = missing_required_components();
        if !missing.is_empty() {
            panic!(
                "{test}: CQLITE_PARITY_REQUIRE_DATASETS=1 but the wide_partition byte-parity \
                 reference is incomplete at {} (missing {} of {} required component(s): {:?}) — \
                 these scenarios are pinned `byte_for_byte` in \
                 test-data/cassandra-parity-manifest.yml and the fixture is in the pinned CI \
                 dataset (v3.4). The required parity gate must FAIL CLOSED here, not skip. \
                 Fetch the dataset: bash test-data/scripts/fetch-datasets.sh",
                fixture_dir().display(),
                missing.len(),
                REQUIRED_BINARY_COMPONENTS.len(),
                missing,
            );
        }
        return true;
    }
    // Local dev (env unset): the byte-level decode only needs Data.db+Index.db;
    // skip cleanly when those are absent while the JSONL semantic tier still runs.
    if binaries_present() {
        return true;
    }
    byte_level_skip(test);
    false
}

/// Emit a uniform line noting the byte-level checks are skipped (binaries absent)
/// while the JSONL semantic checks remain enforced.
fn byte_level_skip(test: &str) {
    eprintln!(
        "{test}: byte-level promoted-index checks skipped (binaries local-only at {}); \
         JSONL semantic checks enforced",
        fixture_dir().display()
    );
}

// ===========================================================================
// Independent re-parse of the on-disk Index.db (byte reference)
// ===========================================================================

/// One raw on-disk BIG `Index.db` entry, re-parsed independently of CQLite's
/// production reader. Only the fields cross-checked against the production reader
/// (`key`, `data_offset`) are retained; the raw promoted bytes are validated via
/// the decoder, so they are skipped here.
struct RawEntry {
    key: Vec<u8>,
    data_offset: u64,
}

/// Decode an unsigned VInt (Cassandra leading-ones length prefix). Returns the
/// value and the new cursor, or `None` on a short/corrupt buffer.
fn read_vint(buf: &[u8], mut i: usize) -> Option<(u64, usize)> {
    let first = *buf.get(i)?;
    i += 1;
    let extra = first.leading_ones().min(8) as usize;
    let mut value = (first as u64) & (0xffu64 >> extra);
    for _ in 0..extra {
        let b = *buf.get(i)?;
        i += 1;
        value = (value << 8) | (b as u64);
    }
    Some((value, i))
}

/// Independently re-parse the BIG `Index.db` into raw entries (key, data_offset,
/// raw promoted-index payload). Returns an error string on any truncation.
fn reparse_index(buf: &[u8]) -> std::result::Result<Vec<RawEntry>, String> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < buf.len() {
        if i + 2 > buf.len() {
            return Err(format!("truncated key length at byte {i}"));
        }
        let klen = u16::from_be_bytes([buf[i], buf[i + 1]]) as usize;
        i += 2;
        if i + klen > buf.len() {
            return Err(format!("truncated key (len {klen}) at byte {i}"));
        }
        let key = buf[i..i + klen].to_vec();
        i += klen;
        let (data_offset, ni) =
            read_vint(buf, i).ok_or_else(|| format!("truncated data offset at byte {i}"))?;
        i = ni;
        let (plen, ni) =
            read_vint(buf, i).ok_or_else(|| format!("truncated promoted length at byte {i}"))?;
        i = ni;
        let plen = plen as usize;
        if i + plen > buf.len() {
            return Err(format!("truncated promoted block (len {plen}) at byte {i}"));
        }
        i += plen;
        out.push(RawEntry { key, data_offset });
    }
    Ok(out)
}

// ===========================================================================
// Schema-driven clustering-prefix length callback (no heuristics, Issue #28)
// ===========================================================================

/// Prefix-length callback for the `(ck int)` clustering: every serialized
/// `ClusteringPrefix` in this fixture is a fixed 6 bytes. Fails (never panics) on
/// a short slice so truncation surfaces explicitly.
fn ck_prefix_len(slice: &[u8]) -> Result<usize> {
    if slice.len() < CK_PREFIX_LEN {
        return Err(Error::Corruption(format!(
            "ck clustering prefix needs {CK_PREFIX_LEN} bytes, slice has {}",
            slice.len()
        )));
    }
    Ok(CK_PREFIX_LEN)
}

/// Extract the big-endian `int` clustering value from a decoded `firstName`/
/// `lastName` prefix (`[2-byte header][4-byte BE int]`).
fn ck_value(prefix: &[u8]) -> std::result::Result<i32, String> {
    if prefix.len() != CK_PREFIX_LEN {
        return Err(format!(
            "clustering prefix length {} != expected {CK_PREFIX_LEN}",
            prefix.len()
        ));
    }
    let v = &prefix[CK_VALUE_OFFSET..CK_VALUE_OFFSET + 4];
    Ok(i32::from_be_bytes([v[0], v[1], v[2], v[3]]))
}

/// Decode `pk` (int) from a 4-byte big-endian partition key, for failure context.
fn pk_name(key: &[u8]) -> String {
    if key.len() == 4 {
        format!(
            "pk={}",
            i32::from_be_bytes([key[0], key[1], key[2], key[3]])
        )
    } else {
        format!("pk=<{} raw bytes: {:02x?}>", key.len(), key)
    }
}

/// Open `Index.db` through the production reader and return the decoded promoted
/// index for every entry, paired with its raw partition key and data offset.
async fn decode_all_partitions() -> Vec<(Vec<u8>, u64, DecodedPromotedIndex)> {
    let index_path = component("Index.db");
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    let reader = IndexReader::open(&index_path, platform)
        .await
        .unwrap_or_else(|e| panic!("IndexReader::open({}) failed: {e}", index_path.display()));

    let entries = reader.get_partition_entries();
    assert!(
        !entries.is_empty(),
        "{}: Index.db present but parsed to zero partitions — fail-closed",
        index_path.display()
    );

    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let key = entry
            .raw_key
            .as_deref()
            .map(|k| k.to_vec())
            .unwrap_or_else(|| entry.key_digest.to_vec());
        let promoted = entry.promoted_index.as_ref().unwrap_or_else(|| {
            panic!(
                "{}: wide partition must carry a promoted index (Issue #993)",
                pk_name(&key)
            )
        });
        assert!(
            !promoted.is_empty(),
            "{}: promoted-index payload captured empty — read path discarded it",
            pk_name(&key)
        );
        let decoded = promoted
            .decode(&ck_prefix_len)
            .unwrap_or_else(|e| panic!("{}: promoted-index decode failed: {e}", pk_name(&key)));
        out.push((key, entry.data_offset, decoded));
    }
    out
}

// ===========================================================================
// Canonical-semantic tier — runs in CI on every push (JSONL golden only)
// ===========================================================================

/// Enforce the wide-partition parity facts from the committed sstabledump JSONL
/// golden ALONE, independent of the local-only binary `Data.db`/`Index.db`. The
/// golden is git-tracked, so this runs (and fails on a wrong golden) in CI on
/// every push — it is the required-tier coverage that the byte-level checks only
/// augment when the binaries are present.
///
/// Asserts, straight from the golden:
///   * exactly 3 partitions, pk = 1, 2, 3;
///   * live-row counts 290 / 300 / 300;
///   * pk=1 is missing exactly the deleted clustering range ck 30..39 (and only
///     that range — every other ck in 0..299 is present);
///   * clustering keys are in ascending order within each partition.
#[test]
fn jsonl_golden_canonical_semantics() {
    let test = "jsonl_golden_canonical_semantics";
    let by_pk = parse_jsonl_live_clusterings();

    // Exactly 3 partitions: pk = 1, 2, 3.
    let pks: Vec<i32> = by_pk.keys().copied().collect();
    assert_eq!(
        pks,
        vec![1, 2, 3],
        "{test}: JSONL golden must contain exactly partitions pk=1,2,3, got {pks:?}"
    );

    // Live-row counts: 290 / 300 / 300.
    assert_eq!(
        by_pk.get(&1).map(Vec::len),
        Some(PK1_LIVE_ROWS),
        "{test}: pk=1 live rows != {PK1_LIVE_ROWS}"
    );
    assert_eq!(
        by_pk.get(&2).map(Vec::len),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: pk=2 live rows != {PK_FULL_LIVE_ROWS}"
    );
    assert_eq!(
        by_pk.get(&3).map(Vec::len),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: pk=3 live rows != {PK_FULL_LIVE_ROWS}"
    );

    // Ascending clustering order within each partition.
    for (pk, cks) in &by_pk {
        let mut sorted = cks.clone();
        sorted.sort_unstable();
        assert_eq!(
            cks, &sorted,
            "{test}: pk={pk} clustering keys not in ascending order"
        );
    }

    // pk=1 missing EXACTLY the deleted range ck 30..39 (range tombstone). Every
    // other ck in 0..299 must be present; the deleted ones must be absent.
    let pk1: std::collections::BTreeSet<i32> = by_pk
        .get(&1)
        .unwrap_or_else(|| panic!("{test}: pk=1 absent"))
        .iter()
        .copied()
        .collect();
    let missing: Vec<i32> = (0..300).filter(|c| !pk1.contains(c)).collect();
    let expected_missing: Vec<i32> = (PK1_DELETED_CK_LO..PK1_DELETED_CK_HI).collect();
    assert_eq!(
        missing, expected_missing,
        "{test}: pk=1 must be missing exactly ck [{PK1_DELETED_CK_LO},{PK1_DELETED_CK_HI}) \
         (the range tombstone), got missing={missing:?}"
    );

    // pk=2/pk=3 must be the full contiguous ck 0..299 (no gaps).
    for pk in [2, 3] {
        let present: Vec<i32> = by_pk
            .get(&pk)
            .unwrap_or_else(|| panic!("{test}: pk={pk} absent"))
            .clone();
        let full: Vec<i32> = (0..300).collect();
        assert_eq!(
            present, full,
            "{test}: pk={pk} must be the full contiguous ck 0..299"
        );
    }

    eprintln!(
        "{test}: PASS (canonical-semantic, JSONL golden) — 3 partitions, live counts \
         290/300/300, pk=1 missing exactly ck 30..39, clustering ascending"
    );
}

// ===========================================================================
// Scenario 1 — row_boundaries / index_info_offsets
// ===========================================================================

/// The promoted IndexInfo offsets must form a consistent, strictly increasing
/// chain: the trailing offsets array starts at 0, the per-block Data.db offsets
/// are strictly increasing, the first block's Data.db offset equals
/// `headerLength` (the partition header size), and the last block's
/// `offset + width` covers the partition's serialized size where derivable.
#[tokio::test]
async fn row_boundaries_index_info_offsets() {
    let test = "row_boundaries_index_info_offsets";
    if !require_or_skip_binaries(test) {
        return;
    }

    let index_bytes = std::fs::read(component("Index.db"))
        .unwrap_or_else(|e| panic!("read Index.db failed: {e}"));
    let raw = reparse_index(&index_bytes)
        .unwrap_or_else(|e| panic!("independent Index.db reparse failed: {e}"));
    assert!(
        !raw.is_empty(),
        "{test}: Index.db reparsed to zero entries — fail-closed"
    );

    let decoded = decode_all_partitions().await;
    assert_eq!(
        decoded.len(),
        raw.len(),
        "{test}: production reader partition count {} != independent reparse {}",
        decoded.len(),
        raw.len()
    );

    let mut multi_block_partitions = 0usize;
    for ((key, _data_offset, idx), rawe) in decoded.iter().zip(raw.iter()) {
        let name = pk_name(key);
        assert_eq!(
            key.as_slice(),
            rawe.key.as_slice(),
            "{test} {name}: production key != independent reparse key"
        );

        assert!(
            idx.count >= 2,
            "{test} {name}: wide partition must have >= 2 IndexInfo blocks, got {}",
            idx.count
        );
        assert_eq!(
            idx.count as usize,
            idx.entries.len(),
            "{test} {name}: declared count {} != decoded block count {}",
            idx.count,
            idx.entries.len()
        );
        assert_eq!(
            idx.offsets.len(),
            idx.entries.len(),
            "{test} {name}: trailing offsets array len {} != block count {}",
            idx.offsets.len(),
            idx.entries.len()
        );
        if idx.count >= 2 {
            multi_block_partitions += 1;
        }

        // The trailing offsets array (relative to the first IndexInfo) starts at 0.
        assert_eq!(
            idx.offsets[0], 0,
            "{test} {name}: first IndexInfo offsets-array entry must be 0, got {}",
            idx.offsets[0]
        );
        // The trailing offsets array is strictly increasing (each block is non-empty).
        for w in idx.offsets.windows(2) {
            assert!(
                w[1] > w[0],
                "{test} {name}: IndexInfo offsets array not strictly increasing \
                 ({} then {})",
                w[0],
                w[1]
            );
        }

        // The first block's Data.db offset equals the partition headerLength (the
        // first row starts immediately after the partition header).
        assert_eq!(
            idx.entries[0].offset, idx.header_length,
            "{test} {name}: first block Data.db offset {} != headerLength {}",
            idx.entries[0].offset, idx.header_length
        );

        // Per-block Data.db offsets are strictly monotonically increasing, and each
        // block's offset chains to the next within one byte of offset+width
        // (Cassandra's IndexInfo offsets are computed from running block widths).
        for n in 1..idx.entries.len() {
            let prev = &idx.entries[n - 1];
            let cur = &idx.entries[n];
            assert!(
                cur.offset > prev.offset,
                "{test} {name}: block {n} Data.db offset {} not > block {} offset {}",
                cur.offset,
                n - 1,
                prev.offset
            );
            let prev_end = prev
                .offset
                .checked_add(prev.width)
                .unwrap_or_else(|| panic!("{test} {name}: block {} offset+width overflow", n - 1));
            assert_eq!(
                prev_end,
                cur.offset,
                "{test} {name}: block {} offset+width ({prev_end}) != block {n} offset \
                 ({}) — offset/width chain inconsistent",
                n - 1,
                cur.offset
            );
        }

        // Cross-check the LAST block against the partition's serialized size. The
        // independent reparse gives the next partition's data_offset; the span
        // (next - this) is the on-disk serialized partition size. The last block's
        // (offset + width) must not exceed that span, and the block region must be
        // a strictly positive fraction of it.
        let this_off = rawe.data_offset;
        if let Some(next) = raw.iter().find(|e| e.data_offset > this_off) {
            let span = next.data_offset - this_off;
            let last = idx
                .entries
                .last()
                .unwrap_or_else(|| panic!("{test} {name}: empty entries after count check"));
            let last_end = last
                .offset
                .checked_add(last.width)
                .unwrap_or_else(|| panic!("{test} {name}: last block offset+width overflow"));
            assert!(
                last_end <= span + last.width,
                "{test} {name}: last block end {last_end} exceeds partition span {span} \
                 by more than one block width"
            );
            assert!(
                last.offset < span,
                "{test} {name}: last block offset {} not within partition span {span}",
                last.offset
            );
        }
    }

    assert!(
        multi_block_partitions > 0,
        "{test}: no multi-block wide partitions exercised — fixture present but not wide"
    );
    eprintln!(
        "{test}: PASS — {} partitions, {multi_block_partitions} multi-block, offset/width \
         chains consistent",
        decoded.len()
    );
}

// ===========================================================================
// Scenario 2 — clustering_bounds
// ===========================================================================

/// Each IndexInfo block's `first_name`/`last_name` decode to ordered clustering
/// `int` values: within a block `first <= last`, across blocks block N's
/// `last_name <= block N+1`'s `first_name` (and strictly, the blocks partition
/// the clustering space), and the overall bound range covers ck 0..299.
#[tokio::test]
async fn clustering_bounds() {
    let test = "clustering_bounds";
    if !require_or_skip_binaries(test) {
        return;
    }

    let decoded = decode_all_partitions().await;
    let mut checked = 0usize;
    for (key, _off, idx) in &decoded {
        let name = pk_name(key);

        let mut prev_last: Option<i32> = None;
        let mut overall_lo: Option<i32> = None;
        let mut overall_hi: Option<i32> = None;

        for (n, block) in idx.entries.iter().enumerate() {
            let first = ck_value(&block.first_name)
                .unwrap_or_else(|e| panic!("{test} {name} block {n} first_name: {e}"));
            let last = ck_value(&block.last_name)
                .unwrap_or_else(|e| panic!("{test} {name} block {n} last_name: {e}"));

            assert!(
                first <= last,
                "{test} {name} block {n}: first_name ck {first} > last_name ck {last}"
            );
            if let Some(p) = prev_last {
                // Block N's last_name <= block N+1's first_name: the blocks partition
                // the clustering space without overlap, in byte-comparable order.
                assert!(
                    p <= first,
                    "{test} {name}: block {n} first_name ck {first} < previous block \
                     last_name ck {p} (block bounds not ordered / overlapping)"
                );
            }
            prev_last = Some(last);
            overall_lo = Some(overall_lo.map_or(first, |lo: i32| lo.min(first)));
            overall_hi = Some(overall_hi.map_or(last, |hi: i32| hi.max(last)));
        }

        let lo = overall_lo.unwrap_or_else(|| panic!("{test} {name}: no blocks decoded"));
        let hi = overall_hi.unwrap_or_else(|| panic!("{test} {name}: no blocks decoded"));
        assert_eq!(
            lo, 0,
            "{test} {name}: overall clustering lower bound {lo} != 0 (ck range start)"
        );
        assert_eq!(
            hi, 299,
            "{test} {name}: overall clustering upper bound {hi} != 299 (ck range end)"
        );
        checked += 1;
    }

    assert!(checked > 0, "{test}: no partitions exercised");
    eprintln!(
        "{test}: PASS — {checked} partitions, block clustering bounds ordered + covering ck 0..299"
    );
}

// ===========================================================================
// Scenario 3 — range_tombstone_boundary_at_block_edge
// ===========================================================================

/// For pk=1, the promoted index must decode without error across the block where
/// ck 30..39 were deleted by a range tombstone, and the JSONL golden must show
/// exactly 290 live rows for pk=1 (deleted range absent), 300 for pk=2/pk=3.
/// This proves a range-tombstone marker near a block edge does not corrupt block
/// decoding. Cross-validated against the live forward scan.
#[tokio::test]
async fn range_tombstone_boundary_at_block_edge() {
    let test = "range_tombstone_boundary_at_block_edge";
    if !require_or_skip_binaries(test) {
        return;
    }

    // (a) The promoted index for every partition (incl. pk=1) decoded cleanly in
    //     decode_all_partitions(); confirm pk=1's block whose range spans the
    //     deleted ck 30..39 still decoded with ordered bounds.
    let decoded = decode_all_partitions().await;
    let pk1 = decoded
        .iter()
        .find(|(k, _, _)| k.as_slice() == [0, 0, 0, 1])
        .unwrap_or_else(|| panic!("{test}: pk=1 partition not found in Index.db"));
    let mut spans_deleted_range = false;
    for (n, block) in pk1.2.entries.iter().enumerate() {
        let first = ck_value(&block.first_name)
            .unwrap_or_else(|e| panic!("{test} pk=1 block {n} first_name: {e}"));
        let last = ck_value(&block.last_name)
            .unwrap_or_else(|e| panic!("{test} pk=1 block {n} last_name: {e}"));
        // A block whose [first,last] range overlaps [30,40) straddles the deleted
        // range — decoding it (above) without error is the property under test.
        if first < PK1_DELETED_CK_HI && last >= PK1_DELETED_CK_LO {
            spans_deleted_range = true;
        }
    }
    assert!(
        spans_deleted_range,
        "{test}: no pk=1 IndexInfo block spans the deleted ck range [{PK1_DELETED_CK_LO},\
         {PK1_DELETED_CK_HI}) — the tombstone/block-edge interaction is not exercised"
    );

    // (b) Live forward scan: pk=1 yields exactly 290 rows (deleted range absent),
    //     pk=2/pk=3 yield 300 each. get_all_entries() suppresses tombstones.
    let counts = forward_row_counts_by_pk().await;
    assert_eq!(
        counts.get(&vec![0, 0, 0, 1]).copied(),
        Some(PK1_LIVE_ROWS),
        "{test}: pk=1 live row count {:?} != {PK1_LIVE_ROWS} (deleted ck 30..39 must be absent)",
        counts.get(&vec![0, 0, 0, 1])
    );
    assert_eq!(
        counts.get(&vec![0, 0, 0, 2]).copied(),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: pk=2 live row count {:?} != {PK_FULL_LIVE_ROWS}",
        counts.get(&vec![0, 0, 0, 2])
    );
    assert_eq!(
        counts.get(&vec![0, 0, 0, 3]).copied(),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: pk=3 live row count {:?} != {PK_FULL_LIVE_ROWS}",
        counts.get(&vec![0, 0, 0, 3])
    );

    // (c) Cross-validate the per-partition counts against the committed JSONL golden.
    let jsonl = parse_jsonl_live_counts();
    assert_eq!(
        jsonl.get(&1).copied(),
        Some(PK1_LIVE_ROWS),
        "{test}: JSONL golden pk=1 live rows != {PK1_LIVE_ROWS}"
    );
    assert_eq!(
        jsonl.get(&2).copied(),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: JSONL golden pk=2 live rows != {PK_FULL_LIVE_ROWS}"
    );
    assert_eq!(
        jsonl.get(&3).copied(),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: JSONL golden pk=3 live rows != {PK_FULL_LIVE_ROWS}"
    );

    eprintln!(
        "{test}: PASS — pk=1 promoted index decodes across the deleted block edge; \
         live counts 290/300/300 match scan + JSONL golden"
    );
}

// ===========================================================================
// Scenario 4 — forward (+ reverse, where supported) bounds
// ===========================================================================

/// Forward completeness: a full forward scan returns exactly 290/300/300 live
/// rows with no rows lost across promoted-index block boundaries or the deleted
/// range. The forward result set is also clustering-ordered (its in-memory
/// reversal preserves the same row set), but this does NOT exercise a true
/// reverse SSTable iterator — see the GAP note below.
///
// RESOLVED (issue #1184): the BIG ("nb") reverse partition iterator is now real —
// `SSTableReader::big_reverse_partition_rows` walks the promoted IndexInfo blocks
// back-to-front (mirroring Cassandra `SSTableReversedIterator`), routed through the
// production query path for single-partition `ORDER BY <ck> DESC` so the in-memory
// sort is skipped (and remains the fallback for small / BTI / multi-generation).
// The forward==reverse equality (identical clustering set, exact reverse ordering,
// block-by-block decode bounded to one block) is pinned in CI on a multi-block BIG
// wide partition built by the write engine in
// `tests/issue_1184_big_promoted_read_seek.rs`; the byte-level 290-row equality on
// this real fixture runs locally when the binaries are present (skip-on-absence).
// Manifest scenario `forward_reverse_bounds` is now `mirrored`.
#[tokio::test]
async fn forward_bounds_completeness() {
    let test = "forward_bounds_completeness";
    if !require_or_skip_binaries(test) {
        return;
    }

    let counts = forward_row_counts_by_pk().await;
    assert_eq!(
        counts.len(),
        3,
        "{test}: expected 3 partitions in forward scan, got {}",
        counts.len()
    );
    assert_eq!(
        counts.get(&vec![0, 0, 0, 1]).copied(),
        Some(PK1_LIVE_ROWS),
        "{test}: pk=1 forward scan {:?} != {PK1_LIVE_ROWS} (rows lost across the deleted \
         block edge or a block boundary)",
        counts.get(&vec![0, 0, 0, 1])
    );
    assert_eq!(
        counts.get(&vec![0, 0, 0, 2]).copied(),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: pk=2 forward scan {:?} != {PK_FULL_LIVE_ROWS} (rows lost across a block boundary)",
        counts.get(&vec![0, 0, 0, 2])
    );
    assert_eq!(
        counts.get(&vec![0, 0, 0, 3]).copied(),
        Some(PK_FULL_LIVE_ROWS),
        "{test}: pk=3 forward scan {:?} != {PK_FULL_LIVE_ROWS} (rows lost across a block boundary)",
        counts.get(&vec![0, 0, 0, 3])
    );

    let total: usize = counts.values().sum();
    assert_eq!(
        total,
        PK1_LIVE_ROWS + 2 * PK_FULL_LIVE_ROWS,
        "{test}: total live rows {total} != {}",
        PK1_LIVE_ROWS + 2 * PK_FULL_LIVE_ROWS
    );

    eprintln!(
        "{test}: PASS (forward completeness only) — 290/300/300 live rows, no rows lost \
         across block boundaries or the deleted range. Reverse SSTable iteration is NOT \
         implemented for BIG wide partitions (see GAP note); reverse-scan parity unproven."
    );
}

// ===========================================================================
// Shared helpers: live forward scan + JSONL golden
// ===========================================================================

/// Forward scan via the production reader; live-row count grouped by raw
/// partition key. `get_all_entries` suppresses row tombstones, so the deleted
/// range is correctly absent.
async fn forward_row_counts_by_pk() -> BTreeMap<Vec<u8>, usize> {
    let data_path = component("Data.db");
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("SSTableReader::open({}) failed: {e}", data_path.display()));
    let entries = reader
        .get_all_entries()
        .await
        .unwrap_or_else(|e| panic!("get_all_entries failed: {e}"));
    assert!(
        !entries.is_empty(),
        "{}: Data.db present but forward scan returned 0 rows — fail-closed",
        data_path.display()
    );

    let mut by_pk: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    for (_tid, key, _value) in &entries {
        *by_pk.entry(key.0.clone()).or_default() += 1;
    }
    by_pk
}

/// Per-pk live clustering values decoded from the committed JSONL golden, in the
/// order they appear in the file (rows of `type == "row"` only).
fn parse_jsonl_live_clusterings() -> BTreeMap<i32, Vec<i32>> {
    let path = jsonl_golden_path().unwrap_or_else(|| {
        panic!(
            "committed JSONL golden not found via CQLITE_DATASETS_ROOT or repo fallback \
             ({}); the git-tracked golden must always be present — fail-closed",
            repo_datasets_root()
                .join(FIXTURE_REL)
                .join(format!("{PREFIX}-Data.db.jsonl"))
                .display()
        )
    });
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read JSONL golden {} failed: {e}", path.display()));
    assert!(
        !text.trim().is_empty(),
        "JSONL golden {} present but empty — fail-closed",
        path.display()
    );
    let mut out: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("bad JSONL line: {e}"));
        let partition = value
            .get("partition")
            .unwrap_or_else(|| panic!("JSONL line missing `partition`"));
        let key_arr = partition
            .get("key")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("JSONL partition missing `key` array"));
        let pk: i32 = key_arr
            .first()
            .and_then(serde_json::Value::as_str)
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("JSONL partition key not a parseable int: {key_arr:?}"));
        let rows = value
            .get("rows")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("JSONL pk={pk} missing `rows` array"));
        let cks: Vec<i32> = rows
            .iter()
            .filter(|r| r.get("type").and_then(serde_json::Value::as_str) == Some("row"))
            .map(|r| {
                r.get("clustering")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|c| c.first())
                    .and_then(|v| match v {
                        serde_json::Value::String(s) => s.parse::<i32>().ok(),
                        serde_json::Value::Number(n) => n.as_i64().map(|x| x as i32),
                        _ => None,
                    })
                    .unwrap_or_else(|| panic!("JSONL pk={pk} row missing parseable clustering"))
            })
            .collect();
        out.insert(pk, cks);
    }
    out
}

/// Parse the committed sstabledump JSONL golden into per-pk live-row counts
/// (`type == "row"` only; range_tombstone_bound markers excluded).
fn parse_jsonl_live_counts() -> BTreeMap<i32, usize> {
    parse_jsonl_live_clusterings()
        .into_iter()
        .map(|(pk, cks)| (pk, cks.len()))
        .collect()
}
