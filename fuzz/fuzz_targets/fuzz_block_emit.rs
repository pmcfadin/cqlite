#![no_main]
//! Fuzz target: the decompressed-block partition loop
//! (`V5CompressedLegacyParser::parse_block_emit`) under one fixed simple schema.
//!
//! Contract: arbitrary block bytes decode to `Ok` or `Err`, never a
//! panic/hang/OOM. The result is ignored — a decode error is a PASS. When the
//! `test_basic/simple_table` fixture is unavailable (no `CQLITE_DATASETS_ROOT`)
//! the driver returns `Err` and this target is a no-op; the never-panic contract
//! still holds.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = cqlite_core::fuzz_support::fuzz_block_emit(data);
});
