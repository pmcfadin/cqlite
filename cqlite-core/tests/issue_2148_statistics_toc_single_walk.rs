//! Issue #2148 (oracle-driven perf): the Statistics.db metadata stack walks the
//! on-disk Table-of-Contents EXACTLY ONCE per parse.
//!
//! Before #2148 the stack re-walked the same small TOC **three times** during a
//! single parse: once to locate the `HEADER` (SerializationHeader) offset, then
//! once each inside `read_table_counts` and `parse_stats_extras` (both via
//! `stats_component_bounds`) — each re-parsing the same bytes to relocate an
//! offset already resolved earlier in the same parse. #2148 parses the TOC ONCE
//! (`repair_metadata::parse_statistics_toc`, resolving the HEADER offset AND the
//! STATS bounds in a single forward walk) and threads the result to those
//! consumers via their `*_with_toc` variants.
//!
//! ORACLE: the `parser::toc_walk_metrics` counter (from #1658) — reset, run one
//! parse through the public `parse_statistics_with_fallback` path, read the count.
//! It read `3` before the fix; it must read `1` after.
//!
//! This is a DEDICATED integration binary (process isolation for the
//! process-global counter) driven by a SYNTHETIC in-memory Statistics.db buffer,
//! so it NEVER dataset-skips — it runs and asserts on every invocation. The buffer
//! is crafted to reach the FULL success path (a valid SerializationHeader that
//! decodes EncodingStats + schema), because the two redundant walks the fix
//! eliminates only fired on that `Ok` branch — a buffer that failed earlier could
//! not distinguish 1 from 3.
//!
//! Compiled only with `--features cli-helpers` (the counter getters/`reset` and
//! `record_toc_walk` live behind it; see Cargo `required-features`).

#![cfg(feature = "cli-helpers")]

use cqlite_core::parser::parse_statistics_with_fallback;
use cqlite_core::parser::toc_walk_metrics::{reset_toc_walk_count, toc_walk_count};
use serial_test::serial;

/// A single unsigned-VInt byte for values in `0..=0x7F` (Cassandra's 1-byte VInt
/// form: `0xxxxxxx`). All values we encode here are tiny (deltas of 0, a length of
/// 9), so the single-byte form is exact.
fn vint_small(v: u8) -> u8 {
    assert!(v <= 0x7F, "helper only encodes 1-byte VInts");
    v
}

/// Build a minimal-but-fully-valid synthetic `Statistics.db` buffer whose parse
/// reaches the success path, exercising every site that historically re-walked
/// the TOC.
///
/// Layout (`parse_statistics_toc` reads `num_components` at byte 0 of the whole
/// buffer, skips a 4-byte marker, then reads `num_components` × `(u32 type, u32
/// offset)` entries starting at byte 8):
///
/// ```text
///   [0..4]   num_components = 1  (u32 BE)
///   [4..8]   marker (unused by the walk)
///   [8..16]  TOC entry 0: type = 3 (HEADER), offset = 16
///   [16..]   SerializationHeader body:
///              vint minTimestamp delta      = 0
///              vint minLocalDeletionTime d. = 0
///              vint minTTL delta            = 0
///              vint keyType len             = 9
///              "Int32Type"                  (9 bytes)
///              vint clusteringCount         = 0
///              vint staticColumnCount       = 0
///              vint regularColumnCount      = 0
/// ```
///
/// The first 32 bytes are also re-read by `parse_nb_format_header`, which performs
/// NO validation (it just consumes 8 big-endian u32s), so the overlap is benign.
/// There is intentionally no STATS component: `read_table_counts`/
/// `parse_stats_extras` then resolve `stats_bounds = Ok(None)` and return their
/// defaults — but in the pre-#2148 code each still walked the TOC first, so this
/// buffer reproduced exactly 3 walks.
fn synthetic_statistics_db() -> Vec<u8> {
    let mut buf = Vec::new();

    // num_components = 1
    buf.extend_from_slice(&1u32.to_be_bytes());
    // 4-byte marker (skipped by the walk)
    buf.extend_from_slice(&0u32.to_be_bytes());
    // TOC entry 0: HEADER (MetadataType ordinal 3) at offset 16.
    buf.extend_from_slice(&3u32.to_be_bytes()); // type
    buf.extend_from_slice(&16u32.to_be_bytes()); // offset

    debug_assert_eq!(buf.len(), 16, "SerializationHeader must start at offset 16");

    // SerializationHeader body: EncodingStats (3 zero VInt deltas) + schema.
    buf.push(vint_small(0)); // minTimestamp delta
    buf.push(vint_small(0)); // minLocalDeletionTime delta
    buf.push(vint_small(0)); // minTTL delta
    let key_type = b"Int32Type";
    buf.push(vint_small(key_type.len() as u8)); // keyType length (9)
    buf.extend_from_slice(key_type); // keyType marshal name
    buf.push(vint_small(0)); // clusteringTypes count
    buf.push(vint_small(0)); // staticColumns count
    buf.push(vint_small(0)); // regularColumns count

    // parse_nb_format_header consumes a fixed 32-byte prefix; the body above lands
    // us at exactly 32 bytes, so no padding is needed. Guard the invariant.
    assert!(
        buf.len() >= 32,
        "buffer must be >= 32 bytes for the fixed nb header prefix (got {})",
        buf.len()
    );

    buf
}

/// The oracle: exactly one TOC walk per full Statistics.db metadata parse.
///
/// FAILS at the pre-#2148 count of 3 (asserts `== 1`); PASSES at the post-fix
/// count of 1.
#[test]
#[serial]
fn statistics_toc_walked_exactly_once_per_parse() {
    let buf = synthetic_statistics_db();

    // Reset the process-global counter so we measure only this parse's walks.
    reset_toc_walk_count();
    assert_eq!(toc_walk_count(), 0, "reset must zero the TOC-walk counter");

    // Public parse path (same entry `SSTableReader::open` reaches for nb metadata).
    let (_remaining, _stats) = parse_statistics_with_fallback(&buf, None)
        .expect("synthetic nb Statistics.db must parse to the success path");

    assert_eq!(
        toc_walk_count(),
        1,
        "issue #2148: a single Statistics.db metadata parse must walk the on-disk \
         TOC EXACTLY ONCE (parse-once + thread offsets); it walked 3× before the \
         fix. Got {} walk(s).",
        toc_walk_count()
    );
}

/// Guard the oracle's premise: the synthetic buffer really does reach the FULL
/// success path (a decoded SerializationHeader), so the two redundant walks the
/// fix removes were genuinely on the exercised path. If this buffer regressed to
/// failing early, the count-of-1 assertion above could pass vacuously (old code
/// would also have walked only once), so pin the success explicitly.
#[test]
#[serial]
fn synthetic_buffer_reaches_full_success_path() {
    let buf = synthetic_statistics_db();
    let (_remaining, stats) = parse_statistics_with_fallback(&buf, None)
        .expect("synthetic nb Statistics.db must parse to the success path");
    // A decoded SerializationHeader yields exactly one partition-key column
    // (keyType = Int32Type) and no clustering/regular columns — proving we reached
    // the schema decode, not an early EncodingStats bail-out.
    assert_eq!(
        stats.serialization_header_partition_keys.len(),
        1,
        "success path must decode the single partition-key column from the header"
    );
}
