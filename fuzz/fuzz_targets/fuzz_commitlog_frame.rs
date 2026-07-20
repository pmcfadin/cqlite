#![no_main]
//! Fuzz target: Cassandra CommitLog segment parser (issue #2389).
//!
//! Drives the descriptor header parser, the sync-marker/record frame walker, and
//! the mutation-body decoder over arbitrary bytes.
//!
//! Contract: arbitrary bytes decode to `Ok` or `Err`, never a panic/hang/OOM.
//! A decode error is a PASS.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    cqlite_core::fuzz_support::fuzz_commitlog_segment(data);
});
