//! Cassandra `UnfilteredSerializer` flag bits, split out of `row_decoder/mod.rs`
//! under the campsite rule (epic #1116).
//!
//! Authority: `cassandra-5.0.8:src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java`
//! (the `Flags`/`ExtendedFlags` constants at the head of that class).
//!
//! Re-exported into `row_decoder`'s namespace, so the sibling modules that read
//! these through their `use super::*` glob are unaffected by the move.

// Row header flag constants
pub(super) const ROW_HAS_TIMESTAMP: u8 = 0x04;
pub(super) const ROW_HAS_TTL: u8 = 0x08;
pub(super) const ROW_HAS_DELETION: u8 = 0x10;
pub(super) const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
/// Issue #221: row contains a complex column with deletion info.
pub(super) const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
pub(super) const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

// Unfiltered marker constants (UnfilteredSerializer.java lines 102-109).
// Issue #229: these markers were being misinterpreted as row data, causing
// parsing failures.
/// Signal end of partition — nothing follows this flag byte.
pub(super) const END_OF_PARTITION: u8 = 0x01;
/// Range tombstone marker (not a data row).
pub(super) const IS_MARKER: u8 = 0x02;

// Extended flags (UnfilteredSerializer.java lines 114-122). These live in the
// SECOND byte, when ROW_HAS_EXTENDED_FLAGS (0x80) is set.
/// Static row — has NO clustering prefix.
pub(super) const EXTENDED_IS_STATIC: u8 = 0x01;

// NOTE: the V5CompressedLegacy format has NO trailing field after row data. The
// next partition/row starts immediately after `row_size` bytes. (A former
// ROW_TRAILING_FIELD_SIZE constant was removed as part of the Issue #237 fix.)
