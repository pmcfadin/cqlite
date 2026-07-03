#![no_main]
//! Fuzz target: BTI node decode + DFS traversal
//! (`iterate_partitions_in_bti_file` over an in-memory `Cursor`).
//!
//! Contract: arbitrary bytes (treated as a `Partitions.db` trie image) decode to
//! `Ok` or `Err`, never a panic/hang/OOM — in particular a seek past EOF or a
//! root offset beyond the trie must surface as `Err`. The result is ignored — a
//! decode error is a PASS.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = cqlite_core::fuzz_support::fuzz_bti_traverse(data);
});
