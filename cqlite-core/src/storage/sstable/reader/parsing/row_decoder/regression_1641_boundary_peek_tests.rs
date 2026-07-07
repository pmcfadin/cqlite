//! Issue #1641 (Epic K, finding K2): drift guard for the non-allocating
//! partition-boundary peek.
//!
//! The per-row emit loop asks "do the bytes here begin a new partition header?"
//! after every row. K2 replaced the FULL allocating try-parse (throwaway key
//! `to_vec` + `format!` error strings + a `PARTITION_HEADER_TRY_PARSES`
//! increment) with the non-allocating
//! [`V5CompressedLegacyParser::peek_partition_boundary`], which shares the
//! structural walk of `parse_partition_header_full` via `scan_partition_header`.
//!
//! The single hazard is DRIFT: a cheap peek that accepts a header the real parser
//! rejects (or vice-versa) desyncs the scan — a new bug class. This proptest pins
//! the exact equivalence the boundary detector must uphold for ARBITRARY bytes:
//!
//! ```text
//! peek == Header  ⟺  (data[offset] is NOT an END_OF_PARTITION / range-tombstone
//!                      marker)  AND  parse_partition_header_full(..).is_ok()
//! ```
//!
//! on both the oa/da (`hasUIntDeletionTime`) and nb (signed 12-byte) DeletionTime
//! forms — the two structural variants the peek must classify identically to the
//! full parse. That right-hand side is precisely the former allocating
//! `peek_is_partition_header` (marker pre-check + full-parse `is_ok`), so this
//! also proves `peek_is_partition_header` returns the same boolean it did on
//! `main`.

use super::row_framing::BoundaryPeek;
use super::V5CompressedLegacyParser;
use crate::storage::sstable::version_gate::{BigVersionGates, BtiVersionGates, VersionGates};
use proptest::prelude::*;
use std::sync::Arc;

/// Parser on the oa (BIG, `hasUIntDeletionTime`) path.
fn oa_parser() -> V5CompressedLegacyParser {
    let gates = VersionGates::Big(BigVersionGates::from_version("oa").expect("oa gates"));
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
        .with_version_gates(Arc::new(gates))
}

/// Parser on the da (BTI, `hasUIntDeletionTime`) path.
fn da_parser() -> V5CompressedLegacyParser {
    let gates = VersionGates::Bti(BtiVersionGates::from_version("da").expect("da gates"));
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
        .with_version_gates(Arc::new(gates))
}

/// Parser on the nb (BIG, signed 12-byte DeletionTime) path — `new`'s default.
fn nb_parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, Some(0))
}

/// The old (`main`) boolean semantics of `peek_is_partition_header`: the leading
/// byte is not a marker AND the FULL allocating parse succeeds. K2 must reproduce
/// this exactly.
fn old_peek_semantics(parser: &V5CompressedLegacyParser, data: &[u8], offset: usize) -> bool {
    if let Some(&flags) = data.get(offset) {
        if V5CompressedLegacyParser::is_end_of_partition(flags)
            || V5CompressedLegacyParser::is_range_tombstone_marker(flags)
        {
            return false;
        }
    }
    parser.parse_partition_header_full(data, offset).is_ok()
}

/// Assert the peek⟺parse equivalence at every offset in `data` for one parser.
/// Returns a `proptest` `TestCaseError` on mismatch (via `prop_assert_eq!`).
fn assert_equivalence(parser: &V5CompressedLegacyParser, data: &[u8]) -> Result<(), TestCaseError> {
    // Check offset 0 (the boundary the emit loop actually peeks) plus a few
    // interior offsets, and one-past-the-end (must be `NeedMoreBytes`).
    for offset in 0..=data.len() {
        let peek = parser.peek_partition_boundary(data, offset);
        let is_header = matches!(peek, BoundaryPeek::Header);
        let expected = old_peek_semantics(parser, data, offset);
        prop_assert_eq!(
            is_header,
            expected,
            "peek==Header must equal (!marker && full_parse.is_ok()) at offset {} for bytes {:?}",
            offset,
            data
        );
        // `peek_is_partition_header` must return the same boolean.
        prop_assert_eq!(
            parser.peek_is_partition_header(data, offset),
            expected,
            "peek_is_partition_header must match old semantics at offset {}",
            offset
        );
        // Offset past the end is always NeedMoreBytes (never a false Header).
        if offset >= data.len() {
            prop_assert_eq!(peek, BoundaryPeek::NeedMoreBytes);
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4000))]

    /// Arbitrary short byte slices: exercises truncated / malformed / marker /
    /// valid-header shapes uniformly across all three DeletionTime variants.
    #[test]
    fn peek_matches_full_parse_arbitrary(bytes in proptest::collection::vec(any::<u8>(), 0..40)) {
        assert_equivalence(&oa_parser(), &bytes)?;
        assert_equivalence(&da_parser(), &bytes)?;
        assert_equivalence(&nb_parser(), &bytes)?;
    }

    /// Structured near-header bytes: a plausible `flags(1) | key_len(1) | key |
    /// deletion` prefix so a meaningful fraction of cases are ACCEPTED headers
    /// (keeps the equivalence non-vacuous on the `Header` branch), including the
    /// oa/da LIVE-sentinel and DELETED forms and truncations thereof.
    #[test]
    fn peek_matches_full_parse_structured(
        flags in any::<u8>(),
        key_len in 0u8..12,
        key in proptest::collection::vec(any::<u8>(), 0..12),
        del in proptest::collection::vec(any::<u8>(), 0..14),
    ) {
        let mut bytes = Vec::with_capacity(2 + key.len() + del.len());
        bytes.push(flags);
        bytes.push(key_len);
        bytes.extend_from_slice(&key);
        bytes.extend_from_slice(&del);
        assert_equivalence(&oa_parser(), &bytes)?;
        assert_equivalence(&da_parser(), &bytes)?;
        assert_equivalence(&nb_parser(), &bytes)?;
    }
}

/// Deterministic sanity: a well-formed LIVE oa/da header (0x80 sentinel) peeks as
/// `Header`; flipping the sentinel to a non-`0x80` high-bit byte the full parser
/// rejects peeks as `NotHeader` (NOT `Header`) — the strict structural rule the
/// peek must NOT weaken. And a header truncated before its full DeletionTime peeks
/// as `NeedMoreBytes`.
#[test]
fn live_oa_header_and_strict_sentinel_and_truncation() {
    let mut header = vec![0x00u8, 0x04];
    header.extend_from_slice(&42i32.to_be_bytes());
    header.push(super::row_framing::OA_IS_LIVE_DELETION); // 0x80 LIVE

    for parser in [oa_parser(), da_parser()] {
        assert_eq!(
            parser.peek_partition_boundary(&header, 0),
            BoundaryPeek::Header,
            "a complete LIVE oa/da header is a boundary"
        );
        assert!(parser.peek_is_partition_header(&header, 0));

        // Non-0x80 high-bit byte: readiness treats it as a present 1-byte form,
        // but the strict scan rejects it (only 0x80 is a valid LIVE byte) => NOT
        // a header. The peek must NOT accept what the full parser rejects.
        let mut bad = header.clone();
        *bad.last_mut().unwrap() = 0x81;
        assert_eq!(
            parser.peek_partition_boundary(&bad, 0),
            BoundaryPeek::NotHeader,
            "an illegal oa/da IS_LIVE byte is not a header (no weakened validation)"
        );
        assert!(parser.parse_partition_header_full(&bad, 0).is_err());

        // Truncated before the deletion discriminator: NeedMoreBytes.
        let truncated = &header[..header.len() - 1];
        assert_eq!(
            parser.peek_partition_boundary(truncated, 0),
            BoundaryPeek::NeedMoreBytes,
            "a header missing its DeletionTime byte needs more bytes"
        );
    }
}
