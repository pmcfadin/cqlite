#![no_main]
//! Fuzz target: VInt decoders (`parse_vint` / `parse_vuint` / `parse_vint_length`).
//!
//! Contract: arbitrary bytes decode to `Ok` or `Err`, never a panic/hang/OOM.
//! Both results are ignored — a decode error is a PASS.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = cqlite_core::fuzz_support::parse_vint(data);
    let _ = cqlite_core::fuzz_support::parse_vuint(data);
    let _ = cqlite_core::fuzz_support::parse_vint_length(data);
});
