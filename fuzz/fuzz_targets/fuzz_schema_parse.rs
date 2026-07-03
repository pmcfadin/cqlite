#![no_main]
//! Fuzz target: schema string parsing (`parse_create_table` + the nom `cql_type`
//! + `cql_type_to_type_id`).
//!
//! The arbitrary bytes are turned into a string via `String::from_utf8_lossy`
//! (so non-UTF-8 input is exercised too). Contract: `Ok` or `Err`, never a
//! panic/hang/OOM — in particular deeply nested `frozen<…>` must hit the
//! `MAX_TYPE_NESTING_DEPTH` guard (issue #1690) and return `Err`, never a stack
//! overflow. Results are ignored — a parse error is a PASS.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    let _ = cqlite_core::fuzz_support::parse_create_table(&s);
    let _ = cqlite_core::fuzz_support::fuzz_cql_type(&s);
    let _ = cqlite_core::fuzz_support::cql_type_to_type_id(&s);
});
