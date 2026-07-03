#![no_main]
//! Fuzz target: schema-typed value decoder over a fixed type list.
//!
//! For each type in `FUZZ_VALUE_TYPES` (every scalar plus `list<int>`,
//! `set<text>`, `map<text,int>`, a tuple, and nested `frozen<list<list<int>>>`)
//! the same arbitrary bytes are decoded. Contract: `Ok` or `Err`, never a
//! panic/hang/OOM. Results are ignored — a decode error is a PASS.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    for type_str in cqlite_core::fuzz_support::FUZZ_VALUE_TYPES {
        let _ = cqlite_core::fuzz_support::fuzz_decode_value(type_str, data);
    }
});
