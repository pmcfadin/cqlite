//! Strict `Filter.db` Bloom-filter parity (Epic #968 / issue #987).
//!
//! Proves that CQLite decodes the on-disk Cassandra 5.0 `Filter.db` Bloom filter
//! and reproduces its membership semantics exactly, with the core correctness gate
//! being **no false negatives**: every partition key Cassandra actually wrote into
//! an SSTable must be reported "maybe present" by the filter CQLite decoded.
//!
//! Scope owned here (issue #987):
//!   * **Decoded-parameter parity** — the Cassandra `Filter.db` on-disk layout is
//!     `[hash_count: i32 BE][num_longs: i32 BE][bitset: num_longs * 8 bytes]`
//!     (`BloomFilterSerializer` over an `OffHeapBitSet`). For every fixture the raw
//!     header bytes are re-read independently and asserted to match the parameters
//!     `BloomFilter::deserialize` recovers (hash count, bitset word/bit length).
//!   * **No-false-negative membership (the correctness gate)** — for every fixture
//!     whose raw partition keys are enumerable (BIG `Index.db`), every key is
//!     probed and the filter MUST report "maybe present". A single false negative
//!     fails the lane.
//!   * **Absent-key false-positive reporting** — deterministically-generated keys
//!     known NOT to be in the SSTable are probed; false positives are *counted and
//!     reported*, never treated as correctness failures (a Bloom filter is allowed
//!     false positives by definition).
//!   * **Serialization round-trip byte-exactness** — `deserialize` then `serialize`
//!     must reproduce the exact on-disk Filter.db bytes (header + bitset).
//!   * **Strict corruption / malformed-byte rejection** — truncated headers,
//!     declared-but-missing bitset bytes, and zero-length buffers must fail closed
//!     (no fabricated filter, no panic-on-unwrap).
//!   * **BIG vs BTI coverage classification** — BIG fixtures carry `Index.db`, the
//!     authoritative raw-key source, so they drive the no-false-negative gate. BTI
//!     (`da`) fixtures have no `Index.db`; the trie's byte-comparable, hash-routed
//!     `Partitions.db` does not expose the raw partition-key bytes the Bloom filter
//!     hashed, so BTI fixtures are covered for *parameter decode + round-trip* only
//!     and the membership gap is classified explicitly (not faked).
//!
//! Fail-closed contract (matches the established dataset convention, see
//! `sstable_parity_toc_component_test.rs`):
//!   * `Filter.db` binaries are local-only (gitignored, fetched on demand). When
//!     the dataset is entirely absent (0 `Data.db` anywhere), the lane SKIPS
//!     cleanly. When binaries ARE present, the lane FAILS if zero filters were
//!     compared, or if a present storage format (BIG/BTI) was left uncovered.
//!   * Data.db presence is tracked INDEPENDENTLY of the compare count so a green
//!     run that silently compared nothing is impossible when fixtures exist.
//!
//! Why the slow statistical FPR check is gated:
//!   * The empirical false-positive-rate check probes a large deterministic
//!     absent-key sample. It is correctness-irrelevant (false positives are legal)
//!     and slow, so it is gated behind `CQLITE_FILTER_FPR_SLOW=1` to keep the
//!     required lane fast — identical pattern to the project's other slow lanes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::bloom::BloomFilter;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};
use cqlite_core::Config;

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

/// Recursively collect every `*-Filter.db` binary under `dir`, skipping macOS
/// AppleDouble shadow files (`._*`).
fn collect_filter_files(dir: &Path, out: &mut Vec<PathBuf>) {
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
            collect_filter_files(&path, out);
        } else if name.ends_with("-Filter.db") {
            out.push(path);
        }
    }
}

/// All committed `*-Filter.db` binaries, sorted. Unlike the TOC/Statistics
/// reference text files, the binary `Filter.db` is local-only: an empty set means
/// the dataset was not fetched, which is a clean skip (handled by the callers),
/// NOT a fail-closed condition here.
fn all_filter_files() -> Vec<PathBuf> {
    let root = datasets_sstables_root();
    let mut out = Vec::new();
    collect_filter_files(&root, &mut out);
    out.sort();
    out
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
/// In strict mode (the `nightly_docker` parity lane, issue #1025), a run that
/// would otherwise SKIP because no `Filter.db` / `Data.db` binaries were fetched
/// must PANIC instead — the no-false-negative Bloom gate (P0 data loss) can never
/// be allowed to false-pass on an empty dataset (issue #28 no-heuristics / #1024
/// fail-closed mandate).
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Whether ANY `Data.db` binary exists under the dataset root — the independent
/// "dataset present" signal. Tracked separately from the filter-compare count so
/// a run that has binaries on disk but silently compared nothing fails closed
/// instead of masquerading as a clean skip.
fn any_data_db_present(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("._") {
            continue;
        }
        if path.is_dir() {
            if any_data_db_present(&path) {
                return true;
            }
        } else if name.ends_with("-Data.db") {
            return true;
        }
    }
    false
}

/// Authoritative storage format for a fixture, parsed from the filename's
/// `<format>` segment (`big`/`bti`) — never inferred from the directory path.
fn format_for(filter_path: &Path) -> SsTableFormat {
    let name = filter_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("non-UTF8 Filter.db name: {}", filter_path.display()));
    SsTableDescriptor::parse_filename(name)
        .unwrap_or_else(|e| panic!("{}: descriptor parse failed: {e}", filter_path.display()))
        .format
}

/// Re-read the Cassandra `Filter.db` header straight from the raw bytes,
/// independent of CQLite's decoder, returning `(hash_count, num_longs)`.
///
/// On-disk layout (`BloomFilterSerializer` + `OffHeapBitSet`):
///   `[hash_count: i32 BE][num_longs: i32 BE][bitset: num_longs * 8 bytes]`
fn read_raw_header(bytes: &[u8], path: &Path) -> (u32, u32) {
    assert!(
        bytes.len() >= 8,
        "{}: Filter.db too small ({} bytes) for the 8-byte Bloom header",
        path.display(),
        bytes.len(),
    );
    let hash_count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let num_longs = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    (hash_count, num_longs)
}

/// Per-fixture outcome of decoding + validating a `Filter.db`. `present_keys` and
/// `false_negatives` are only meaningful for fixtures whose raw keys are
/// enumerable (BIG); BTI fixtures record `keys_enumerable = false`.
struct FilterOutcome {
    format: SsTableFormat,
    hash_count: u32,
    num_longs: u32,
    keys_enumerable: bool,
    present_keys: usize,
    false_negatives: usize,
    absent_probes: usize,
    false_positives: usize,
}

/// Decode the on-disk header, validate it against CQLite's decoder, and assert a
/// byte-exact serialize round-trip. Shared by every membership / parameter test.
fn decode_and_validate(bytes: &[u8], path: &Path, format: SsTableFormat) -> BloomFilter {
    // (1) Raw header parity: CQLite's decoded parameters must equal the bytes.
    let (raw_hash_count, raw_num_longs) = read_raw_header(bytes, path);
    assert!(
        raw_hash_count > 0,
        "{}: Filter.db declares hash_count=0 — Cassandra always writes >= 1",
        path.display(),
    );
    assert!(
        raw_num_longs > 0,
        "{}: Filter.db declares num_longs=0 — Cassandra always writes >= 1",
        path.display(),
    );
    let expected_len = 8 + (raw_num_longs as usize) * 8;
    assert_eq!(
        bytes.len(),
        expected_len,
        "{}: Filter.db length {} != header-implied {} (8 + num_longs*8) — truncated/corrupt",
        path.display(),
        bytes.len(),
        expected_len,
    );

    let bloom = BloomFilter::deserialize(bytes).unwrap_or_else(|e| {
        panic!(
            "{}: CQLite failed to decode Cassandra Filter.db ({format:?}): {e:?}",
            path.display()
        )
    });
    assert_eq!(
        bloom.hash_count(),
        raw_hash_count,
        "{}: decoded hash_count {} != on-disk {}",
        path.display(),
        bloom.hash_count(),
        raw_hash_count,
    );
    assert_eq!(
        bloom.bit_count(),
        (raw_num_longs as u64) * 64,
        "{}: decoded bit_count {} != on-disk num_longs*64 ({})",
        path.display(),
        bloom.bit_count(),
        (raw_num_longs as u64) * 64,
    );

    // (2) Serialization round-trip: re-emit and assert byte-exact preservation.
    let reemitted = bloom
        .serialize()
        .unwrap_or_else(|e| panic!("{}: re-serialize failed: {e:?}", path.display()));
    assert_eq!(
        reemitted,
        bytes,
        "{}: Filter.db serialize round-trip is not byte-exact ({} vs {} bytes)",
        path.display(),
        reemitted.len(),
        bytes.len(),
    );

    bloom
}

/// Deterministically generate `count` absent-key probes for a fixture, salted by
/// the fixture path so two fixtures never share a probe set, and seeded by `seed`
/// for reproducibility. These are byte strings that, by construction (the
/// `__cqlite_absent__` prefix never appears in a real partition key), are not
/// members of the SSTable — used to *measure* (never gate) false positives.
fn absent_key_probes(path: &Path, seed: u64, count: usize) -> Vec<Vec<u8>> {
    let salt = path.to_string_lossy();
    (0..count)
        .map(|i| {
            // Splitmix64 for a deterministic, well-mixed pseudo-random tail.
            let mut z = seed
                .wrapping_add(i as u64)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15);
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^= z >> 31;
            let mut key = format!("__cqlite_absent__{salt}#{i}#").into_bytes();
            key.extend_from_slice(&z.to_le_bytes());
            key
        })
        .collect()
}

/// Strict, byte-and-field `Filter.db` parameter parity + serialization round-trip
/// across every committed fixture (BIG and BTI), plus the no-false-negative
/// membership gate for every fixture whose raw partition keys are enumerable.
#[tokio::test]
async fn filter_db_strict_parameters_and_no_false_negative() {
    let root = datasets_sstables_root();
    let filters = all_filter_files();
    let data_present = any_data_db_present(&root);

    // Skip-on-total-absence: no binaries fetched at all. The committed corpus has
    // no text reference for Filter.db (it is a pure binary component), so an empty
    // dataset is a clean skip by default. BUT under CQLITE_REQUIRE_FIXTURES=1 (the
    // nightly_docker parity lane, issue #1025) the no-false-negative Bloom gate is
    // a P0 hard leg that must FAIL CLOSED — never vacuously pass on missing data.
    if filters.is_empty() && !data_present {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but no Filter.db / Data.db binaries are present \
                 under {} — the no-false-negative Bloom gate cannot run; \
                 fetch the dataset (bash test-data/scripts/fetch-datasets.sh)",
                root.display()
            );
        }
        eprintln!(
            "filter_db_strict_parameters_and_no_false_negative: SKIP — no Filter.db / Data.db \
             binaries fetched under {} (run bash test-data/scripts/fetch-datasets.sh)",
            root.display()
        );
        return;
    }

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .unwrap_or_else(|e| panic!("Platform init failed: {e:?}")),
    );

    let mut outcomes: Vec<(PathBuf, FilterOutcome)> = Vec::new();

    let mut present_big = false;
    let mut present_bti = false;

    for filter in &filters {
        let format = format_for(filter);
        match format {
            SsTableFormat::Big => present_big = true,
            SsTableFormat::Bti => present_bti = true,
        }

        let bytes = std::fs::read(filter)
            .unwrap_or_else(|e| panic!("read {} failed: {e}", filter.display()));

        let bloom = decode_and_validate(&bytes, filter, format);
        let (hash_count, num_longs) = read_raw_header(&bytes, filter);

        // Enumerate raw partition keys when an Index.db sibling exists (BIG).
        let name = filter
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| panic!("non-UTF8 Filter.db name: {}", filter.display()));
        let base = name
            .strip_suffix("-Filter.db")
            .unwrap_or_else(|| panic!("Filter.db name without suffix: {name}"));
        let index_path = filter.with_file_name(format!("{base}-Index.db"));

        let mut present_keys = 0usize;
        let mut false_negatives = 0usize;
        let mut absent_probes = 0usize;
        let mut false_positives = 0usize;
        let keys_enumerable = index_path.exists();

        if keys_enumerable {
            let reader = IndexReader::open(&index_path, platform.clone())
                .await
                .unwrap_or_else(|e| panic!("open {} failed: {e:?}", index_path.display()));

            // NO-FALSE-NEGATIVE GATE: every key Cassandra wrote MUST be reported
            // "maybe present". The Index.db `key_digest` holds the RAW partition-key
            // bytes (Issue #552) — exactly what Cassandra hashed into Filter.db —
            // so this is the authoritative present-key set (no path heuristics).
            for entry in reader.get_partition_entries() {
                present_keys += 1;
                if !bloom.contains(&entry.key_digest) {
                    false_negatives += 1;
                    eprintln!(
                        "FALSE NEGATIVE in {} for present key (len={})",
                        filter.display(),
                        entry.key_digest.len()
                    );
                }
            }
            assert_eq!(
                false_negatives,
                0,
                "{}: Bloom filter produced {} false negative(s) over {} present partition keys \
                 — Filter.db membership is broken (a present key was reported absent)",
                filter.display(),
                false_negatives,
                present_keys,
            );
            assert!(
                present_keys > 0,
                "{}: Index.db present but yielded zero partition keys — cannot prove \
                 no-false-negative membership for this fixture",
                filter.display(),
            );

            // Absent-key probes: REPORT false positives, never gate on them. Use a
            // probe count comparable to the present-key set so the report is
            // meaningful but the lane stays fast.
            let probe_count = present_keys.max(64);
            let probes = absent_key_probes(filter, 0x5151_5151_5151_5151, probe_count);
            absent_probes = probes.len();
            for probe in &probes {
                if bloom.contains(probe) {
                    false_positives += 1;
                }
            }
        }

        outcomes.push((
            filter.clone(),
            FilterOutcome {
                format,
                hash_count,
                num_longs,
                keys_enumerable,
                present_keys,
                false_negatives,
                absent_probes,
                false_positives,
            },
        ));
    }

    // Aggregate reporting.
    let compared = outcomes.len();
    let big_count = outcomes
        .iter()
        .filter(|(_, o)| o.format == SsTableFormat::Big)
        .count();
    let bti_count = outcomes
        .iter()
        .filter(|(_, o)| o.format == SsTableFormat::Bti)
        .count();
    let membership_fixtures = outcomes.iter().filter(|(_, o)| o.keys_enumerable).count();
    let total_present_keys: usize = outcomes.iter().map(|(_, o)| o.present_keys).sum();
    let total_false_neg: usize = outcomes.iter().map(|(_, o)| o.false_negatives).sum();
    let total_absent_probes: usize = outcomes.iter().map(|(_, o)| o.absent_probes).sum();
    let total_false_pos: usize = outcomes.iter().map(|(_, o)| o.false_positives).sum();

    // Distinct decoded Bloom parameters across the corpus (hash_count, num_longs),
    // reported so a parameter regression in any fixture is visible in the log.
    let mut params: Vec<(u32, u32)> = outcomes
        .iter()
        .map(|(_, o)| (o.hash_count, o.num_longs))
        .collect();
    params.sort_unstable();
    params.dedup();
    eprintln!(
        "filter_db_strict_parameters_and_no_false_negative: distinct (hash_count, num_longs) = {params:?}"
    );

    eprintln!(
        "filter_db_strict_parameters_and_no_false_negative: compared={compared} \
         (BIG={big_count} BTI={bti_count}) | membership-fixtures={membership_fixtures} \
         present-keys-probed={total_present_keys} false-negatives={total_false_neg} \
         | absent-probes={total_absent_probes} false-positives(reported,not-failures)={total_false_pos}"
    );

    // Coverage proof: a present format must actually have been compared, and the
    // no-false-negative gate must have probed at least one real present key.
    if data_present {
        assert!(
            compared > 0,
            "Data.db binaries are present but zero Filter.db fixtures were compared — \
             strict Filter.db parity proved nothing"
        );
        if present_big {
            assert!(
                big_count > 0,
                "BIG Filter.db fixtures present but none compared — BIG parity unproven"
            );
            // BIG fixtures carry Index.db, so the no-false-negative gate must have run.
            assert!(
                membership_fixtures > 0 && total_present_keys > 0,
                "BIG fixtures present but no partition keys were probed — \
                 no-false-negative gate proved nothing"
            );
        }
        if present_bti {
            assert!(
                bti_count > 0,
                "BTI Filter.db fixtures present but none compared — BTI parameter parity unproven"
            );
        }
    } else {
        // Defensive: filters present without any Data.db is an inconsistent dataset.
        assert!(
            compared > 0,
            "Filter.db binaries present without Data.db — inconsistent dataset, nothing compared"
        );
    }

    // The total no-false-negative invariant (already asserted per-fixture; this is
    // the aggregate restatement that makes the gate impossible to skip).
    assert_eq!(
        total_false_neg, 0,
        "Filter.db lane observed {total_false_neg} total false negative(s) across \
         {total_present_keys} present keys — membership correctness is broken"
    );
}

/// Classify the storage-format coverage of the Filter.db corpus explicitly, so a
/// missing format family (e.g. no BTI fixtures fetched) is a documented, visible
/// fact rather than a silent gap. BIG drives the membership gate; BTI is covered
/// for parameter-decode + round-trip only (its trie does not expose raw keys).
#[tokio::test]
async fn filter_db_format_coverage_classification() {
    let root = datasets_sstables_root();
    let filters = all_filter_files();
    let data_present = any_data_db_present(&root);

    if filters.is_empty() && !data_present {
        eprintln!("filter_db_format_coverage_classification: SKIP — no Filter.db binaries fetched");
        return;
    }

    let mut big_with_index = 0usize;
    let mut big_without_index = 0usize;
    let mut bti = 0usize;

    for filter in &filters {
        let format = format_for(filter);
        let name = filter.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let base = name.strip_suffix("-Filter.db").unwrap_or(name);
        let index_path = filter.with_file_name(format!("{base}-Index.db"));
        match format {
            SsTableFormat::Big if index_path.exists() => big_with_index += 1,
            SsTableFormat::Big => big_without_index += 1,
            SsTableFormat::Bti => bti += 1,
        }
    }

    eprintln!(
        "filter_db_format_coverage_classification: BIG-with-Index.db(membership-capable)={big_with_index} \
         BIG-without-Index.db(parameter-only)={big_without_index} \
         BTI(parameter-only,no-raw-keys)={bti}"
    );

    if data_present {
        // The committed corpus is BIG-dominated with Index.db; at least the
        // membership-capable family must exist when binaries are present.
        assert!(
            big_with_index > 0,
            "Data.db present but no BIG fixture with an Index.db sibling found — \
             the no-false-negative membership gate would never run"
        );
    }
}

/// Strict corruption / malformed-byte rejection: the decoder must fail closed on
/// truncated headers, declared-but-missing bitset bytes, and zero-length buffers
/// (never fabricate a filter, never panic-on-unwrap). Uses a real fixture as the
/// clean baseline when present; otherwise exercises synthetic byte mutations so
/// the negative contract is always tested.
#[test]
fn filter_db_strict_corruption_fails_closed() {
    // Synthetic baseline: a valid header (hash_count=5, num_longs=1) + 8 bitset
    // bytes. This always exercises the negative paths even with no dataset.
    let mut clean = Vec::new();
    clean.extend_from_slice(&5u32.to_be_bytes());
    clean.extend_from_slice(&1u32.to_be_bytes());
    clean.extend_from_slice(&[0xAB; 8]);
    assert!(
        BloomFilter::deserialize(&clean).is_ok(),
        "clean synthetic Filter.db should decode"
    );

    // (a) Empty buffer → error (too short for the 8-byte header).
    assert!(
        BloomFilter::deserialize(&[]).is_err(),
        "empty Filter.db buffer must be rejected"
    );

    // (b) Header-only truncation (claims a bitset that is not present).
    let header_only = &clean[..8];
    assert!(
        BloomFilter::deserialize(header_only).is_err(),
        "Filter.db with a header declaring num_longs=1 but zero bitset bytes must be rejected"
    );

    // (c) Partial bitset (declares 1 long = 8 bytes, only 4 present).
    let partial = &clean[..12];
    assert!(
        BloomFilter::deserialize(partial).is_err(),
        "Filter.db with a truncated bitset must be rejected (size mismatch)"
    );

    // (d) Trailing junk (declares 1 long but carries 9 bitset bytes).
    let mut trailing = clean.clone();
    trailing.push(0xFF);
    assert!(
        BloomFilter::deserialize(&trailing).is_err(),
        "Filter.db with extra trailing bytes beyond the declared bitset must be rejected"
    );

    // (f) Length-consistent but degenerate header: hash_count=0. The buffer size
    // is internally consistent (header + declared bitset), so the size check alone
    // would accept it — but a filter with zero hash functions fails OPEN (always
    // "not present"), producing false negatives. Strict mode must reject it.
    let mut zero_hash = Vec::new();
    zero_hash.extend_from_slice(&0u32.to_be_bytes()); // hash_count = 0
    zero_hash.extend_from_slice(&1u32.to_be_bytes()); // num_longs = 1
    zero_hash.extend_from_slice(&[0xAB; 8]);
    let zero_hash_result = std::panic::catch_unwind(|| BloomFilter::deserialize(&zero_hash));
    assert!(
        matches!(zero_hash_result, Ok(Err(_))),
        "Filter.db header with hash_count=0 must be rejected (Err, not Ok, not panic)"
    );

    // (g) Length-consistent but degenerate header: num_longs=0. Zero bit-words
    // means a zero-capacity filter; the size check would accept an 8-byte buffer,
    // but such a filter is malformed and must be rejected in strict mode.
    let mut zero_longs = Vec::new();
    zero_longs.extend_from_slice(&5u32.to_be_bytes()); // hash_count = 5
    zero_longs.extend_from_slice(&0u32.to_be_bytes()); // num_longs = 0
    let zero_longs_result = std::panic::catch_unwind(|| BloomFilter::deserialize(&zero_longs));
    assert!(
        matches!(zero_longs_result, Ok(Err(_))),
        "Filter.db header with num_longs=0 must be rejected (Err, not Ok, not panic)"
    );

    // (h) High-bit-set hash_count: 0xFFFF_FFFF reads as i32 == -1. Parsing the
    // field as u32 would treat it as ~4 billion hash functions, making contains()
    // loop billions of times (DoS / pathological lookup). The header is otherwise
    // length-consistent (num_longs=1 + 8 bitset bytes), so only the signed reject
    // catches it. Must be rejected as Err with no panic and no pathological loop.
    let mut neg_hash = Vec::new();
    neg_hash.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes()); // hash_count = -1 (i32)
    neg_hash.extend_from_slice(&1u32.to_be_bytes()); // num_longs = 1
    neg_hash.extend_from_slice(&[0xAB; 8]);
    let neg_hash_result = std::panic::catch_unwind(|| BloomFilter::deserialize(&neg_hash));
    assert!(
        matches!(neg_hash_result, Ok(Err(_))),
        "Filter.db header with hash_count=0xFFFF_FFFF (-1 as i32) must be rejected (Err, not Ok, not panic)"
    );

    // (i) i32::MIN hash_count: 0x8000_0000 reads as i32 == -2147483648. As u32 this
    // is ~2.1 billion hash functions. Length-consistent header; must be rejected.
    let mut min_hash = Vec::new();
    min_hash.extend_from_slice(&0x8000_0000u32.to_be_bytes()); // hash_count = i32::MIN
    min_hash.extend_from_slice(&1u32.to_be_bytes()); // num_longs = 1
    min_hash.extend_from_slice(&[0xAB; 8]);
    let min_hash_result = std::panic::catch_unwind(|| BloomFilter::deserialize(&min_hash));
    assert!(
        matches!(min_hash_result, Ok(Err(_))),
        "Filter.db header with hash_count=0x8000_0000 (i32::MIN) must be rejected (Err, not Ok, not panic)"
    );

    // (j) High-bit-set num_longs: 0xFFFF_FFFF reads as i32 == -1. As u32 cast to
    // usize this would be a huge bit-word count and break size math (and could try
    // to allocate enormous buffers). Must be rejected by the signed `<= 0` guard
    // before any `as usize` widening. We keep hash_count valid to isolate the
    // num_longs path.
    let mut neg_longs = Vec::new();
    neg_longs.extend_from_slice(&5u32.to_be_bytes()); // hash_count = 5
    neg_longs.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes()); // num_longs = -1 (i32)
    let neg_longs_result = std::panic::catch_unwind(|| BloomFilter::deserialize(&neg_longs));
    assert!(
        matches!(neg_longs_result, Ok(Err(_))),
        "Filter.db header with num_longs=0xFFFF_FFFF (-1 as i32) must be rejected (Err, not Ok, not panic)"
    );

    // (k) i32::MIN num_longs: 0x8000_0000. Same hazard as (j); must be rejected.
    let mut min_longs = Vec::new();
    min_longs.extend_from_slice(&5u32.to_be_bytes()); // hash_count = 5
    min_longs.extend_from_slice(&0x8000_0000u32.to_be_bytes()); // num_longs = i32::MIN
    let min_longs_result = std::panic::catch_unwind(|| BloomFilter::deserialize(&min_longs));
    assert!(
        matches!(min_longs_result, Ok(Err(_))),
        "Filter.db header with num_longs=0x8000_0000 (i32::MIN) must be rejected (Err, not Ok, not panic)"
    );

    // (l) Absurd-but-POSITIVE hash_count: 0x7FFF_FFFF (i32::MAX) passes the signed
    // `<= 0` guard, yet would make every contains() loop ~2.1 billion times (DoS).
    // The upper-bound guard must reject it. num_longs=1 keeps the header
    // length-consistent so only the hash_count bound catches it.
    let mut huge_hash = Vec::new();
    huge_hash.extend_from_slice(&0x7FFF_FFFFu32.to_be_bytes()); // hash_count = i32::MAX
    huge_hash.extend_from_slice(&1u32.to_be_bytes()); // num_longs = 1
    huge_hash.extend_from_slice(&[0xAB; 8]);
    let huge_hash_result = std::panic::catch_unwind(|| BloomFilter::deserialize(&huge_hash));
    assert!(
        matches!(huge_hash_result, Ok(Err(_))),
        "Filter.db header with hash_count=0x7FFF_FFFF (i32::MAX) must be rejected (Err, not Ok, not panic)"
    );

    // (e) Real-fixture corruption when a binary is available: flip the declared
    // num_longs to an absurd value so the size check rejects it.
    if let Some(filter) = all_filter_files().into_iter().next() {
        let bytes = std::fs::read(&filter)
            .unwrap_or_else(|e| panic!("read {} failed: {e}", filter.display()));
        // Sanity: the clean fixture decodes.
        assert!(
            BloomFilter::deserialize(&bytes).is_ok(),
            "{}: clean fixture should decode",
            filter.display()
        );
        let mut corrupt = bytes.clone();
        // num_longs lives at bytes 4..8 (BE). Bump it so the declared bitset length
        // no longer matches the actual buffer size.
        corrupt[4] = 0x7F;
        corrupt[5] = 0xFF;
        assert!(
            BloomFilter::deserialize(&corrupt).is_err(),
            "{}: corrupted num_longs must be rejected by the size check",
            filter.display()
        );
        eprintln!(
            "filter_db_strict_corruption_fails_closed: corruption rejected against {}",
            filter.display()
        );
    } else {
        eprintln!(
            "filter_db_strict_corruption_fails_closed: synthetic-only (no Filter.db binary fetched)"
        );
    }
}

/// Slow, deterministic empirical false-positive-rate report. NOT a correctness
/// gate — false positives are legal for a Bloom filter — so it is gated behind
/// `CQLITE_FILTER_FPR_SLOW=1` to keep the required lane fast. When enabled it
/// probes a large deterministic absent-key sample per fixture and asserts only a
/// generous sanity ceiling (the filter is not pathologically saturated): the
/// measured FPR must stay well under 50%. The detailed per-fixture rate is
/// reported for human inspection.
#[tokio::test]
async fn filter_db_statistical_false_positive_rate_slow() {
    if std::env::var("CQLITE_FILTER_FPR_SLOW").ok().as_deref() != Some("1") {
        eprintln!(
            "filter_db_statistical_false_positive_rate_slow: SKIP — set CQLITE_FILTER_FPR_SLOW=1 \
             to run the slow empirical FPR report (false positives are not correctness failures)"
        );
        return;
    }

    let filters = all_filter_files();
    if filters.is_empty() {
        eprintln!("filter_db_statistical_false_positive_rate_slow: SKIP — no Filter.db fetched");
        return;
    }

    // Deterministic large absent-key sample. The same seed + per-fixture salt make
    // the measured rate reproducible across runs and machines.
    const SAMPLE: usize = 50_000;
    const SEED: u64 = 0xC0FF_EE00_1234_5678;
    // Generous sanity ceiling: a healthy Cassandra-sized filter for these small
    // fixtures should be far below this. This only catches a pathologically broken
    // (near-saturated) bitset, never a normal false-positive rate.
    const FPR_CEILING: f64 = 0.50;

    let mut reported = 0usize;
    for filter in &filters {
        let format = format_for(filter);
        let bytes = std::fs::read(filter)
            .unwrap_or_else(|e| panic!("read {} failed: {e}", filter.display()));
        let bloom = decode_and_validate(&bytes, filter, format);

        let probes = absent_key_probes(filter, SEED, SAMPLE);
        let hits = probes.iter().filter(|p| bloom.contains(p)).count();
        let rate = hits as f64 / SAMPLE as f64;
        eprintln!(
            "FPR {} ({:?}): hash_count={} num_longs={} measured_fpr={:.4} ({hits}/{SAMPLE})",
            filter.display(),
            format,
            bloom.hash_count(),
            bloom.bit_count() / 64,
            rate,
        );
        assert!(
            rate < FPR_CEILING,
            "{}: measured false-positive rate {:.4} exceeds sanity ceiling {:.2} — \
             the bitset appears pathologically saturated / broken",
            filter.display(),
            rate,
            FPR_CEILING,
        );
        reported += 1;
    }

    eprintln!(
        "filter_db_statistical_false_positive_rate_slow: reported FPR for {reported} fixtures"
    );
}
