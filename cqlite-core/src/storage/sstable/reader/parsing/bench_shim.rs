//! Bench-only decode shim (issue #1615, Epic H).
//!
//! `SSTableReader::parse_value_with_schema_type` — the live block-path value
//! decode entry — is `pub(in crate::storage::sstable::reader)`, so the `decode`
//! bench (`cqlite-core/benches/decode_bench.rs`, an external crate) cannot call it
//! directly. This module adds ONE `#[doc(hidden)]`, `bench-internals`-gated public
//! forwarder so the bench measures the REAL dispatch rather than a re-implemented
//! copy that would drift from production.
//!
//! It is compiled only when the non-default `bench-internals` feature is on
//! (`#[cfg(feature = "bench-internals")] mod bench_shim;` in the parent module), so
//! every default build / gate / CI run is byte-identical and the public API is
//! unchanged. It lives in its own tiny file so `value_parsing.rs` stays within the
//! source file-size ratchet (campsite rule).

use super::super::types::SSTableReader;
use crate::{Result, Value};

impl SSTableReader {
    /// Bench-only forwarder to the crate-private block-path decode entry
    /// [`parse_value_with_schema_type`](SSTableReader::parse_value_with_schema_type),
    /// forwarding its arguments verbatim. Adds no behavior of its own.
    #[doc(hidden)]
    pub fn decode_value_for_bench(&self, value_data: &[u8], data_type: &str) -> Result<Value> {
        self.parse_value_with_schema_type(value_data, data_type)
    }
}
