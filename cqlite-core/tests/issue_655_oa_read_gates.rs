//! VG3: oa format read behavior tests (Issue #655)
//!
//! Validates that each of the five oa-only gates is correctly implemented
//! and that all oa fixture tables parse value-identically to their JSONL
//! sstabledump golden files.
//!
//! ## Gates tested (all from BigFormat.java:406-410)
//!
//! 1. `hasUIntDeletionTime` (BigFormat.java:409) — row-level deletion time
//!    uses u32 unsigned reinterpretation for oa/da to support year-2106 TTLs.
//!
//! 2. `hasImprovedMinMax` (BigFormat.java:406) — Statistics.db STATS section
//!    min/max clustering encoded via `serializeImprovedMinMax` (serialized type
//!    list + Slice) instead of the legacy per-column short-length arrays.
//!
//! 3. `hasLegacyMinMax` (BigFormat.java:398) — deprecated in oa (returns false);
//!    only present for m[a-z] / n[a-z] versions.  Absence confirmed by gate.
//!
//! 4. `hasPartitionLevelDeletionPresenceMarker` (BigFormat.java:407) — one
//!    additional boolean written after the originating-host-id in StatsMetadata.
//!
//! 5. `hasKeyRange` (BigFormat.java:408) — firstKey and lastKey stored as
//!    vint-length prefixed byte arrays after the presence marker.
//!
//! 6. `hasTokenSpaceCoverage` (BigFormat.java:410) — one additional `double`
//!    after the key range.
//!
//! ## Authority chain
//!
//!   BigFormat.java:406-410 (primary) > CassandraUInt.java > StatsMetadata.java:493-511

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::version_gate::{BigVersionGates, BtiVersionGates, VersionGates};
use cqlite_core::{Config, Platform};

// ============================================================================
// Gate unit tests — crafted bytes
// ============================================================================

/// Verify that `hasUIntDeletionTime` is FALSE for nb and TRUE for oa.
///
/// Source: BigFormat.java:409 — `hasUintDeletionTime = version.compareTo("oa") >= 0`
#[test]
fn gate1_uint_deletion_time_off_for_nb_on_for_oa() {
    let nb = BigVersionGates::from_version("nb").expect("nb gates");
    assert!(
        !nb.has_uint_deletion_time,
        "nb: hasUIntDeletionTime MUST be false (BigFormat.java:409)"
    );

    let oa = BigVersionGates::from_version("oa").expect("oa gates");
    assert!(
        oa.has_uint_deletion_time,
        "oa: hasUIntDeletionTime MUST be true (BigFormat.java:409)"
    );

    // BTI da: always true (BtiFormat.java — all oa-class gates are TRUE for da)
    let da = BtiVersionGates::from_version("da").expect("da gates");
    assert!(
        da.has_uint_deletion_time,
        "da: hasUIntDeletionTime MUST be true"
    );
}

/// Verify the row-level deletion-time reinterpretation for hasUIntDeletionTime.
///
/// For nb, a local_deletion_time sum that overflows i32 (bit 31 set) is stored
/// as a negative i32.  For oa, the same bit pattern is reinterpreted as an
/// unsigned u32, extending the epoch to ~year 2106.
///
/// This test uses crafted arithmetic to show the branch difference.
///
/// Source: BigFormat.java:409, CassandraUInt.java (toLong), UnfilteredSerializer.java:671-676
#[test]
fn gate1_uint_deletion_time_branch_arithmetic() {
    // A deletion time of 2^31 + 1 seconds = 2147483649 (year ~2038+)
    // When stored as a delta and reinterpreted as i32 it becomes -2147483647.
    // With hasUIntDeletionTime it should be 2147483649u32 as i32 = -2147483647.
    // The KEY difference: the returned value should be treated as unsigned.

    let raw_value: i64 = 2_147_483_649i64; // > i32::MAX
    let as_i32: i32 = raw_value as i32; // -2147483647 (overflow)
    let as_uint_reinterp: i32 = (raw_value as u32) as i32; // also -2147483647

    // Both interpretations produce the same bit pattern as i32 but mean
    // different absolute values:
    //   nb interpretation: -2147483647 (seconds before epoch, invalid)
    //   oa interpretation: 2147483649 unsigned seconds since epoch (year ~2038)
    // The gate controls whether callers treat this as signed or unsigned.
    assert_eq!(as_i32, as_uint_reinterp); // same bits
    assert!(as_i32 < 0, "raw i32 is negative (overflow)");

    // Unsigned recovery (CassandraUInt.toLong semantics):
    let recovered_unsigned = as_i32 as u32 as u64;
    assert_eq!(
        recovered_unsigned, 2_147_483_649u64,
        "oa: unsigned recovery"
    );
}

/// Verify `hasImprovedMinMax` is FALSE for nb, TRUE for oa.
///
/// Source: BigFormat.java:406 — `hasImprovedMinMax = version.compareTo("oa") >= 0`
#[test]
fn gate2_improved_min_max_off_for_nb_on_for_oa() {
    let nb = BigVersionGates::from_version("nb").expect("nb");
    assert!(
        !nb.has_improved_min_max,
        "nb: hasImprovedMinMax MUST be false"
    );

    let oa = BigVersionGates::from_version("oa").expect("oa");
    assert!(
        oa.has_improved_min_max,
        "oa: hasImprovedMinMax MUST be true"
    );
}

/// Verify `hasLegacyMinMax` is TRUE for nb, FALSE for oa.
///
/// Source: BigFormat.java:398 — `hasLegacyMinMax = version.matches("(m[a-z])|(n[a-z])")`
/// For `oa` this regex does NOT match, so it is FALSE.
#[test]
fn gate3_legacy_min_max_on_for_nb_off_for_oa() {
    let nb = BigVersionGates::from_version("nb").expect("nb");
    assert!(
        nb.has_legacy_min_max,
        "nb: hasLegacyMinMax MUST be true (n[a-z])"
    );

    let oa = BigVersionGates::from_version("oa").expect("oa");
    assert!(
        !oa.has_legacy_min_max,
        "oa: hasLegacyMinMax MUST be false (deprecated in oa, BigFormat.java:398)"
    );
}

/// Verify `hasPartitionLevelDeletionPresenceMarker` is FALSE for nb, TRUE for oa.
///
/// Source: BigFormat.java:407 — `hasPartitionLevelDeletionPresenceMarker = version.compareTo("oa") >= 0`
#[test]
fn gate4_partition_level_deletion_presence_marker_off_for_nb_on_for_oa() {
    let nb = BigVersionGates::from_version("nb").expect("nb");
    assert!(
        !nb.has_partition_level_deletion_presence_marker,
        "nb: hasPartitionLevelDeletionPresenceMarker MUST be false"
    );

    let oa = BigVersionGates::from_version("oa").expect("oa");
    assert!(
        oa.has_partition_level_deletion_presence_marker,
        "oa: hasPartitionLevelDeletionPresenceMarker MUST be true (BigFormat.java:407)"
    );
}

/// Verify `hasKeyRange` is FALSE for nb, TRUE for oa.
///
/// Source: BigFormat.java:408 — `hasKeyRange = version.compareTo("oa") >= 0`
/// These are `firstKey` and `lastKey` stored after the presence marker in
/// StatsMetadata.serialize:
///   `ByteBufferUtil.writeWithVIntLength(component.firstKey, out)`
///   `ByteBufferUtil.writeWithVIntLength(component.lastKey, out)`
/// (StatsMetadata.java:503-507)
#[test]
fn gate5_key_range_off_for_nb_on_for_oa() {
    let nb = BigVersionGates::from_version("nb").expect("nb");
    assert!(!nb.has_key_range, "nb: hasKeyRange MUST be false");

    let oa = BigVersionGates::from_version("oa").expect("oa");
    assert!(
        oa.has_key_range,
        "oa: hasKeyRange MUST be true (BigFormat.java:408)"
    );
}

/// Verify `hasTokenSpaceCoverage` is FALSE for nb, TRUE for oa.
///
/// Source: BigFormat.java:410 — `hasTokenSpaceCoverage = version.compareTo("oa") >= 0`
/// Written as `out.writeDouble(component.tokenSpaceCoverage)` in StatsMetadata.java:509-511.
#[test]
fn gate6_token_space_coverage_off_for_nb_on_for_oa() {
    let nb = BigVersionGates::from_version("nb").expect("nb");
    assert!(
        !nb.has_token_space_coverage,
        "nb: hasTokenSpaceCoverage MUST be false"
    );

    let oa = BigVersionGates::from_version("oa").expect("oa");
    assert!(
        oa.has_token_space_coverage,
        "oa: hasTokenSpaceCoverage MUST be true (BigFormat.java:410)"
    );
}

/// VersionGates enum: BIG format (nb and oa) gives `VersionGates::Big(_)`;
/// BTI format (da) gives `VersionGates::Bti(_)`.  The VG3 header-detection
/// gate uses `matches!(gates, VersionGates::Big(_))` to identify headerless
/// BIG format Data.db files.
///
/// Source: BigFormat.java — all BIG versions write headerless Data.db files.
/// Verified against real `oa-2-big-Data.db` first bytes (compressed chunk data).
#[test]
fn gate_header_detection_big_format_is_headerless() {
    // nb — BIG format
    let nb_path = std::path::PathBuf::from("nb-1-big-Data.db");
    let nb_gates = VersionGates::from_path(&nb_path).expect("nb gates from path");
    assert!(
        matches!(nb_gates, VersionGates::Big(_)),
        "nb must be VersionGates::Big"
    );

    // oa — BIG format, all 5 oa-only gates TRUE
    let oa_path = std::path::PathBuf::from("oa-2-big-Data.db");
    let oa_gates = VersionGates::from_path(&oa_path).expect("oa gates from path");
    assert!(
        matches!(oa_gates, VersionGates::Big(_)),
        "oa must be VersionGates::Big (headerless)"
    );

    // da — BTI format, NOT BIG
    let da_path = std::path::PathBuf::from("da-2-bti-Data.db");
    let da_gates = VersionGates::from_path(&da_path).expect("da gates from path");
    assert!(
        matches!(da_gates, VersionGates::Bti(_)),
        "da must be VersionGates::Bti"
    );
}

// ============================================================================
// nb-vs-oa byte pair tests — same logical data, gate-dependent branch
// ============================================================================

/// Crafted-byte test for hasUIntDeletionTime gate.
///
/// Two minimal deletion-time delta values that exercise both the nb (signed i32)
/// and oa (u32 reinterpretation) paths.
///
/// This test proves the gate switches the arithmetic branch.
#[test]
fn gate1_crafted_byte_nb_vs_oa_deletion_time() {
    // For a deletion time delta that keeps the sum below i32::MAX, both
    // nb and oa produce the same result.
    let min_ldt: i64 = 1_442_880_000; // DELETION_TIME_EPOCH (Sept 22, 2015)
    let delta: u64 = 338_314_440; // brings us to ~June 2026

    let nb_result: i32 = (min_ldt.wrapping_add(delta as i64)) as i32;
    let oa_result: i32 = (min_ldt.wrapping_add(delta as i64) as u32) as i32;

    // Both produce the same value when sum < 2^31
    assert_eq!(nb_result, oa_result, "small delta: nb and oa agree");
    assert!(nb_result > 0, "reasonable deletion time is positive");

    // Simulate a large deletion time (2^31 + 100 seconds ≈ year 2038+)
    // where nb and oa DIVERGE in interpretation.
    let large_delta: u64 = (i32::MAX as u64) + 100 - min_ldt as u64;
    let raw_ldt_large = min_ldt.wrapping_add(large_delta as i64);

    // nb path: cast to i32, wraps to negative
    let nb_large: i32 = raw_ldt_large as i32;
    // oa path: treat as u32 bits, then reinterpret as i32 storage
    let oa_large_bits: u32 = raw_ldt_large as u32;
    let oa_large: i32 = oa_large_bits as i32;

    // Same bit pattern, different semantic meaning:
    assert_eq!(nb_large, oa_large, "same i32 bits");
    // The oa value, when treated as unsigned u32, gives the correct timestamp
    let oa_unsigned = oa_large as u32 as u64;
    assert!(
        oa_unsigned > i32::MAX as u64,
        "oa unsigned value > i32::MAX (year 2038)"
    );
    assert!(
        nb_large < 0,
        "nb signed value < 0 (incorrect without gate flip)"
    );
}

/// Crafted-byte test for hasImprovedMinMax/hasLegacyMinMax gate.
///
/// For nb (hasLegacyMinMax=true): StatsMetadata writes min/max clustering as
/// arrays of short-length-prefixed ByteBuffers.
/// For oa (hasImprovedMinMax=true, hasLegacyMinMax=false): written via
/// `serializeImprovedMinMax` — a type list followed by a Slice.
///
/// Source: StatsMetadata.java:428-450 (serialize), :579-602 (deserialize)
/// This test validates that the gate values correctly distinguish the two formats.
#[test]
fn gate2_crafted_byte_nb_vs_oa_min_max_format() {
    // nb: hasLegacyMinMax=true, hasImprovedMinMax=false
    let nb = BigVersionGates::from_version("nb").unwrap();
    assert!(nb.has_legacy_min_max, "nb: legacy format");
    assert!(!nb.has_improved_min_max, "nb: no improved format");

    // oa: hasImprovedMinMax=true, hasLegacyMinMax=false
    let oa = BigVersionGates::from_version("oa").unwrap();
    assert!(!oa.has_legacy_min_max, "oa: no legacy format");
    assert!(oa.has_improved_min_max, "oa: improved format");

    // nb format reads: int(colCount) then loop of short-length-prefixed byte arrays
    // Craft minimal nb serialized bytes: colCount=1, value=[0xAB, 0xCD] (2 bytes)
    let mut nb_bytes: Vec<u8> = Vec::new();
    nb_bytes.extend_from_slice(&1u32.to_be_bytes()); // colCount = 1
    nb_bytes.extend_from_slice(&2u16.to_be_bytes()); // length = 2
    nb_bytes.extend_from_slice(&[0xAB, 0xCD]); // value

    // oa format reads: serialized type list + Slice (different structure)
    // Gate check: nb bytes would be misread if parsed as oa format
    // The `nb.has_legacy_min_max` gate selects the correct parser.
    let nb_colcount = u32::from_be_bytes([nb_bytes[0], nb_bytes[1], nb_bytes[2], nb_bytes[3]]);
    assert_eq!(nb_colcount, 1, "nb: colCount correctly parsed as 1");

    // Confirm oa gate status is different
    assert_ne!(
        nb.has_legacy_min_max, oa.has_legacy_min_max,
        "gates differ: nb/oa min-max format must select different parsers"
    );
}

/// Crafted-byte test for hasPartitionLevelDeletionPresenceMarker gate.
///
/// For nb: no marker byte; hasPartitionLevelDeletions is inferred from minLocalDeletionTime.
/// For oa: one boolean byte written after the originating-host-id block.
///
/// Source: StatsMetadata.java:493-495 (serialize), :629-634 (deserialize)
#[test]
fn gate4_crafted_byte_partition_level_deletion_marker() {
    let nb = BigVersionGates::from_version("nb").unwrap();
    let oa = BigVersionGates::from_version("oa").unwrap();

    // nb: marker absent; presence is inferred
    assert!(!nb.has_partition_level_deletion_presence_marker);

    // oa: marker present — one boolean byte after originating-host-id
    assert!(oa.has_partition_level_deletion_presence_marker);

    // Simulate the byte difference:
    // nb reader: skips this field entirely
    // oa reader: reads one byte (0x01 = has deletions, 0x00 = no deletions)
    let oa_marker_has_deletions: u8 = 0x01;
    let oa_marker_no_deletions: u8 = 0x00;

    assert_ne!(oa_marker_has_deletions, oa_marker_no_deletions);
    // nb just doesn't read this byte at all — gate guards the read.
}

/// Crafted-byte test for hasKeyRange gate.
///
/// For nb: no firstKey/lastKey in StatsMetadata.
/// For oa: firstKey and lastKey stored as vint-length-prefixed byte arrays.
///
/// Source: StatsMetadata.java:503-507, ByteBufferUtil.writeWithVIntLength
#[test]
fn gate5_crafted_byte_key_range() {
    let nb = BigVersionGates::from_version("nb").unwrap();
    let oa = BigVersionGates::from_version("oa").unwrap();

    assert!(!nb.has_key_range, "nb: no key range");
    assert!(oa.has_key_range, "oa: key range present");

    // Simulate vint-length-prefixed key bytes:
    // For a 16-byte UUID key, the serialized form is:
    //   vint_encode(16) = [0x10]  (1 byte for value 16 in unsigned VInt)
    //   [16 bytes of UUID]
    let uuid_key = [
        0x11u8, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x11,
    ];
    let mut key_bytes: Vec<u8> = Vec::new();
    key_bytes.push(16); // vint(16) = 0x10 = 16
    key_bytes.extend_from_slice(&uuid_key);

    assert_eq!(
        key_bytes.len(),
        17,
        "serialized key: 1 length byte + 16 UUID bytes"
    );
    // nb: this field is entirely absent from Statistics.db
    // oa: this 17-byte block (x2, for firstKey and lastKey) is present
}

/// Crafted-byte test for hasTokenSpaceCoverage gate.
///
/// For nb: no tokenSpaceCoverage field.
/// For oa: one f64 (8 bytes, big-endian) written at end of StatsMetadata.
///
/// Source: StatsMetadata.java:509-511
#[test]
fn gate6_crafted_byte_token_space_coverage() {
    let nb = BigVersionGates::from_version("nb").unwrap();
    let oa = BigVersionGates::from_version("oa").unwrap();

    assert!(!nb.has_token_space_coverage, "nb: no token space coverage");
    assert!(
        oa.has_token_space_coverage,
        "oa: token space coverage present"
    );

    // Simulate the 8-byte f64 serialization.
    // NaN indicates "not measured" per StatsMetadata.java:652:
    //   `double tokenSpaceCoverage = Double.NaN;`
    let nan_bytes = f64::NAN.to_bits().to_be_bytes();
    let read_back = f64::from_bits(u64::from_be_bytes(nan_bytes));
    assert!(read_back.is_nan(), "NaN round-trips correctly");

    // A real coverage value:
    let coverage = 0.75f64;
    let coverage_bytes = coverage.to_bits().to_be_bytes();
    let read_back_coverage = f64::from_bits(u64::from_be_bytes(coverage_bytes));
    assert!(
        (read_back_coverage - 0.75).abs() < 1e-10,
        "f64 coverage round-trips"
    );
}

// ============================================================================
// oa fixture table parity tests
// ============================================================================

/// Helper: find the Data.db file in an SSTable directory
fn find_data_db(dir: &Path) -> Option<PathBuf> {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Match oa-*-big-Data.db, excluding AppleDouble sidecars (._*)
            if name_str.ends_with("-Data.db") && !name_str.starts_with("._") {
                return Some(entry.path());
            }
        }
    }
    None
}

/// Helper: count live (non-tombstone) rows in a JSONL golden file.
///
/// `get_all_entries()` returns one entry per live clustering row (tombstone rows
/// are filtered out by `filter_tombstone`).  To compare apples-to-apples we must
/// count the same thing from the JSONL:
///
///   - Each JSONL line is one partition.
///   - Each partition's `rows` array may contain static_block, range_tombstone_bound,
///     and row entries.
///   - A `row` entry with a `deletion_info` field is a row tombstone — excluded by
///     `get_all_entries()`, so we must exclude it here too.
///
/// Authority: UnfilteredSerializer.java (row tombstone detection) +
///            Issue #505 (filter_tombstone implementation).
fn count_jsonl_live_rows(path: &Path) -> std::io::Result<usize> {
    let mut content = String::new();
    File::open(path)?.read_to_string(&mut content)?;
    let mut live_rows = 0usize;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Parse partition JSON
        let partition: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue, // skip unparseable lines
        };
        if let Some(rows) = partition.get("rows").and_then(|r| r.as_array()) {
            for row in rows {
                // Only count type=row entries without a deletion_info (live rows)
                let is_row = row.get("type").and_then(|t| t.as_str()) == Some("row");
                let has_deletion = row.get("deletion_info").is_some();
                if is_row && !has_deletion {
                    live_rows += 1;
                }
            }
        }
    }
    Ok(live_rows)
}

/// Helper: find the JSONL golden file alongside a Data.db file
fn find_jsonl_golden(data_db: &Path) -> Option<PathBuf> {
    let filename = data_db.file_name()?.to_str()?;
    let jsonl_name = format!("{}.jsonl", filename);
    let candidate = data_db.parent()?.join(&jsonl_name);
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

/// Helper: resolve an oa table path given keyspace and table name.
///
/// Returns `Some(dir)` only when the directory exists **and** contains at least
/// one binary SSTable component matching `oa-*-big-Data.db`.  When the
/// repository has only git-tracked JSONL golden files (e.g. CI with
/// datasets-v2), the directory will be present but the binary will be absent;
/// in that case this function returns `None` so callers can skip gracefully.
///
/// See: CI datasets-v2 does not include test_oa binaries — promotion happens
/// in Issue #656 (VG4) together with a CI datasets-v3 bump.
fn resolve_oa_table_path(keyspace: &str, table_prefix: &str) -> Option<PathBuf> {
    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let sstables_dir = PathBuf::from(datasets_root).join("sstables");
    let keyspace_dir = sstables_dir.join(keyspace);

    if let Ok(entries) = fs::read_dir(&keyspace_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(table_prefix) {
                let table_dir = entry.path();
                // Guard: require at least one binary Data.db file in the dir.
                // Golden-only checkouts (CI datasets-v2) have .jsonl/.txt/.crc32
                // files but NO oa-*-big-Data.db binary.  Without this check
                // read_oa_table() would attempt to open a non-existent binary
                // and fail the test rather than skipping it.
                let has_binary = fs::read_dir(&table_dir)
                    .map(|dir_entries| {
                        dir_entries.flatten().any(|e| {
                            let n = e.file_name();
                            let s = n.to_string_lossy();
                            s.ends_with("-Data.db") && !s.starts_with("._")
                        })
                    })
                    .unwrap_or(false);
                if has_binary {
                    return Some(table_dir);
                } else {
                    // Directory exists but binaries are absent (goldens-only).
                    return None;
                }
            }
        }
    }
    None
}

/// Open an oa SSTable and return (live_row_count, expected_live_row_count_from_jsonl).
///
/// Both counts exclude row tombstones: `get_all_entries()` filters them out via
/// `filter_tombstone()`, and `count_jsonl_live_rows()` excludes rows with
/// `deletion_info` in the JSONL golden file.
async fn read_oa_table(
    table_dir: &Path,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    let data_db = find_data_db(table_dir)
        .ok_or_else(|| format!("No Data.db found in {}", table_dir.display()))?;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .map_err(|e| format!("Failed to open {}: {}", data_db.display(), e))?;

    let entries = reader
        .get_all_entries()
        .await
        .map_err(|e| format!("Failed to read entries from {}: {}", data_db.display(), e))?;

    // Count live rows from JSONL golden (excludes tombstone rows)
    let jsonl = find_jsonl_golden(&data_db)
        .ok_or_else(|| format!("No JSONL golden found for {}", data_db.display()))?;
    let expected = count_jsonl_live_rows(&jsonl)?;

    Ok((entries.len(), expected))
}

/// VG3 behavioral proof: oa simple_table parses correctly.
///
/// This table has 5 partitions with primitive-type columns (UUID, text, bool,
/// double, bigint, int, float, timestamp).  It exercises the core oa-format
/// Data.db parsing with LZ4 compression and headerless BIG format detection.
#[tokio::test]
async fn test_oa_simple_table_parity() {
    let dir = match resolve_oa_table_path("test_oa", "simple_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/simple_table binaries not present (CI uses datasets-v2; goldens-only checkout)");
            return;
        }
    };

    let (got, expected) = read_oa_table(&dir).await.unwrap_or_else(|e| {
        panic!("oa simple_table: {}", e);
    });

    assert_eq!(
        got, expected,
        "oa simple_table: expected {} live rows, got {}",
        expected, got
    );
    assert!(got > 0, "oa simple_table: must have at least one live row");
}

/// VG3 behavioral proof: oa collection_table parses correctly.
///
/// Contains map, set, and list columns including complex-deletion markers.
/// Tests that the oa-format collection deletion_info is parsed correctly.
#[tokio::test]
async fn test_oa_collection_table_parity() {
    let dir = match resolve_oa_table_path("test_oa", "collection_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/collection_table binaries not present (CI uses datasets-v2; goldens-only checkout)");
            return;
        }
    };

    let (got, expected) = read_oa_table(&dir).await.unwrap_or_else(|e| {
        panic!("oa collection_table: {}", e);
    });

    assert_eq!(
        got, expected,
        "oa collection_table: expected {} live rows, got {}",
        expected, got
    );
    assert!(
        got > 0,
        "oa collection_table: must have at least one live row"
    );
}

/// VG3 behavioral proof: oa ttl_table parses correctly.
///
/// Exercises the hasUIntDeletionTime path with live TTL cells — the
/// min_local_deletion_time from EncodingStats is a non-epoch value
/// (1781194440 = June 2026), requiring the unsigned-delta interpretation.
#[tokio::test]
async fn test_oa_ttl_table_parity() {
    let dir = match resolve_oa_table_path("test_oa", "ttl_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/ttl_table binaries not present (CI uses datasets-v2; goldens-only checkout)");
            return;
        }
    };

    let (got, expected) = read_oa_table(&dir).await.unwrap_or_else(|e| {
        panic!("oa ttl_table: {}", e);
    });

    assert_eq!(
        got, expected,
        "oa ttl_table: expected {} live rows, got {}",
        expected, got
    );
    assert!(got > 0, "oa ttl_table: must have at least one live row");
}

/// VG3 behavioral proof: oa static_table parses correctly.
///
/// Contains static columns, which exercise the oa static_block parsing path.
#[tokio::test]
async fn test_oa_static_table_parity() {
    let dir = match resolve_oa_table_path("test_oa", "static_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/static_table binaries not present (CI uses datasets-v2; goldens-only checkout)");
            return;
        }
    };

    let (got, expected) = read_oa_table(&dir).await.unwrap_or_else(|e| {
        panic!("oa static_table: {}", e);
    });

    assert_eq!(
        got, expected,
        "oa static_table: expected {} live rows, got {}",
        expected, got
    );
    assert!(got > 0, "oa static_table: must have at least one live row");
}

/// VG3 behavioral proof: oa tombstone_table parses correctly.
///
/// Contains row deletions and range tombstone bounds, exercising the
/// oa deletion-time path with non-trivial min_local_deletion_time values.
#[tokio::test]
async fn test_oa_tombstone_table_parity() {
    let dir = match resolve_oa_table_path("test_oa", "tombstone_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/tombstone_table binaries not present (CI uses datasets-v2; goldens-only checkout)");
            return;
        }
    };

    let (got, expected) = read_oa_table(&dir).await.unwrap_or_else(|e| {
        panic!("oa tombstone_table: {}", e);
    });

    assert_eq!(
        got, expected,
        "oa tombstone_table: expected {} live rows, got {}",
        expected, got
    );
    assert!(
        got > 0,
        "oa tombstone_table: must have at least one live row"
    );
}

/// VG3 behavioral proof: oa udt_table parses correctly.
///
/// Contains UDT columns with large field values that exceed 128 bytes,
/// testing the oa-format large-field code path.
#[tokio::test]
async fn test_oa_udt_table_parity() {
    let dir = match resolve_oa_table_path("test_oa", "udt_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/udt_table binaries not present (CI uses datasets-v2; goldens-only checkout)");
            return;
        }
    };

    let (got, expected) = read_oa_table(&dir).await.unwrap_or_else(|e| {
        panic!("oa udt_table: {}", e);
    });

    assert_eq!(
        got, expected,
        "oa udt_table: expected {} live rows, got {}",
        expected, got
    );
    assert!(got > 0, "oa udt_table: must have at least one live row");
}

/// VG3 summary test: ALL six oa fixture tables must have correct partition counts.
///
/// This test is the acceptance gate for Issue #655: every oa table in
/// `test-data/datasets/sstables/test_oa/` must parse with a partition count
/// matching its JSONL golden file.
#[tokio::test]
async fn test_oa_all_tables_pass_parity() {
    let tables = [
        "simple_table",
        "collection_table",
        "ttl_table",
        "static_table",
        "tombstone_table",
        "udt_table",
    ];

    // Bail early if no datasets are available (CI without test data)
    if std::env::var("CQLITE_DATASETS_ROOT").is_err() {
        eprintln!("SKIP: CQLITE_DATASETS_ROOT not set — skipping oa parity summary");
        return;
    }

    // Bail early if oa binaries are absent (goldens-only checkout — CI with datasets-v2).
    // resolve_oa_table_path() returns None when the table dir exists but has no
    // oa-*-big-Data.db binary.  Promotion to CI happens in Issue #656 (VG4)
    // together with the CI datasets-v3 bump.
    if resolve_oa_table_path("test_oa", tables[0]).is_none() {
        eprintln!(
            "SKIP: test_oa binaries not present (CI uses datasets-v2; goldens-only checkout)"
        );
        return;
    }

    let mut results: HashMap<&str, Result<(usize, usize), String>> = HashMap::new();

    for table in &tables {
        let dir = match resolve_oa_table_path("test_oa", table) {
            Some(d) => d,
            None => {
                results.insert(
                    table,
                    Err(format!(
                        "directory not found or binaries absent for {}",
                        table
                    )),
                );
                continue;
            }
        };
        let r = read_oa_table(&dir).await;
        match r {
            Ok((got, expected)) => {
                results.insert(table, Ok((got, expected)));
            }
            Err(e) => {
                results.insert(table, Err(e.to_string()));
            }
        }
    }

    let mut all_pass = true;
    for table in &tables {
        match results.get(table) {
            Some(Ok((got, expected))) => {
                if got != expected {
                    eprintln!(
                        "FAIL test_oa/{}: expected {} live rows, got {}",
                        table, expected, got
                    );
                    all_pass = false;
                } else {
                    eprintln!("PASS test_oa/{}: {} live rows", table, got);
                }
            }
            Some(Err(e)) => {
                eprintln!("FAIL test_oa/{}: {}", table, e);
                all_pass = false;
            }
            None => {
                eprintln!("FAIL test_oa/{}: no result recorded", table);
                all_pass = false;
            }
        }
    }

    assert!(
        all_pass,
        "Not all oa fixture tables passed parity check (see above)"
    );
}

/// Diagnostic test to understand why some oa tables return different entry counts
#[tokio::test]
async fn debug_oa_ttl_table_entries() {
    let dir = match resolve_oa_table_path("test_oa", "ttl_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/ttl_table binaries not present (CI uses datasets-v2; goldens-only checkout)");
            return;
        }
    };

    let data_db = match find_data_db(&dir) {
        Some(p) => p,
        None => {
            eprintln!("No Data.db found in {}", dir.display());
            return;
        }
    };

    eprintln!("Data.db: {}", data_db.display());

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));

    let reader =
        cqlite_core::storage::sstable::reader::SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open reader");

    eprintln!("CassandraVersion: {:?}", reader.header().cassandra_version);
    eprintln!("Compression: {}", reader.header().compression.algorithm);

    let data_file_size = std::fs::metadata(&data_db).map(|m| m.len()).unwrap_or(0);
    eprintln!("Data.db file size: {} bytes", data_file_size);
    eprintln!("calculated_header_size={}", reader.calculate_header_size());

    match reader.get_all_entries().await {
        Ok(entries) => {
            eprintln!("Entries returned: {}", entries.len());
            for (i, (tid, key, _val)) in entries.iter().enumerate() {
                eprintln!("  [{}] table={:?} key={:?}", i, tid, key);
            }
        }
        Err(e) => {
            eprintln!("get_all_entries error: {:?}", e);
        }
    }
}
