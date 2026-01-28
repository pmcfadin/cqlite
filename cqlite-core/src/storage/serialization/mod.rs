//! Type serialization for SSTable Data.db generation
//!
//! Provides CQL type encoders that produce byte-correct Cassandra 5.0 format.
//! Handles all primitive types, collections, UDTs, and edge cases.
//!
//! Critical encoding gotchas (from M5 Council Recommendation):
//! - VInt encoding: Must match VIntCoding.java exactly
//! - UDT field lengths: 4-byte big-endian i32, NOT VInt
//! - Decimal encoding: [i32 BE scale][VInt unscaled_len][unscaled_bytes]
//! - Duration encoding: [VInt months][VInt days][VInt nanos] (all signed)
//! - Date encoding: Unsigned with offset for lexicographic ordering
//!
//! TODO: Implementation in M5.0-14 through M5.0-16

#[cfg(feature = "write-support")]
pub mod vint;
#[cfg(feature = "write-support")]
pub mod types;
#[cfg(feature = "write-support")]
pub mod cell;
#[cfg(feature = "write-support")]
pub mod row;

#[cfg(feature = "write-support")]
pub use vint::{encode_signed, encode_unsigned, signed_len, unsigned_len};
#[cfg(feature = "write-support")]
pub use types::TypeSerializer;
#[cfg(feature = "write-support")]
pub use cell::CellEncoder;
#[cfg(feature = "write-support")]
pub use row::RowSerializer;
