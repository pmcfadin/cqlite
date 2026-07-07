//! Unit tests for `value.rs` conversions (extracted per campsite rule, #1116).
//!
//! Wired from `value.rs` via `#[cfg(test)] #[path = "value_tests.rs"] mod tests;`,
//! so `super::*` here resolves to the `value` module and every helper under test
//! (`decimal_to_string`, `inet_bytes_to_string`, `cache_get_or_try_init`,
//! `testing::*`) is in scope exactly as it was inline.

use super::*;

/// Serializes every test that resets/reads the process-global
/// `ctor_lookups` counter. The increment site lives in library code (a true
/// process-global, unlike the local-instance trick in `cqlite-core`'s
/// `work_counters`), so two `reset`-then-assert tests running under Rust's
/// default parallel runner would race. Both counter tests take this guard.
static CTOR_COUNTER_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_decimal_to_string_positive() {
    // 123 with scale 2 = 1.23
    let unscaled = vec![123];
    assert_eq!(decimal_to_string(2, &unscaled).unwrap(), "1.23");
}

#[test]
fn test_decimal_to_string_no_scale() {
    // 123 with scale 0 = 123
    let unscaled = vec![123];
    assert_eq!(decimal_to_string(0, &unscaled).unwrap(), "123");
}

#[test]
fn test_decimal_to_string_negative_scale() {
    // 123 with scale -2 = 12300 (123e2)
    let unscaled = vec![123];
    assert_eq!(decimal_to_string(-2, &unscaled).unwrap(), "123e2");
}

#[test]
fn test_decimal_to_string_large_scale() {
    // 123 with scale 5 = 0.00123
    let unscaled = vec![123];
    assert_eq!(decimal_to_string(5, &unscaled).unwrap(), "0.00123");
}

#[test]
fn test_decimal_to_string_empty() {
    assert_eq!(decimal_to_string(0, &[]).unwrap(), "0");
}

#[test]
fn test_decimal_to_string_negative() {
    // -123 in two's complement (single byte) = 0x85 = 133, but need proper encoding
    // For -123: 256 - 123 = 133 = 0x85
    let unscaled = vec![0x85]; // -123 as two's complement byte
    assert_eq!(decimal_to_string(2, &unscaled).unwrap(), "-1.23");
}

/// Issue #1754: a pathological positive `scale` (used directly as an unbounded
/// `format!` padding width) must fail closed with a typed error rather than
/// PANIC ("Formatting argument out of range"). On the napi async-worker thread
/// that panic cannot unwind across FFI and the process `abort()`s, defeating
/// #1440's `panic=unwind` profile. This is the direct Rust reproduction of the
/// corrupt-DECIMAL abort; it PANICS on the pre-fix code and returns `Err` now.
#[test]
fn test_decimal_to_string_pathological_positive_scale_errors_not_panics() {
    // A tiny 1-byte unscaled value but an absurd scale that would drive a
    // ~2.1-billion-wide `format!("0.{digits:0>width$}")` padding.
    let unscaled = vec![0x01];
    let err = decimal_to_string(i32::MAX, &unscaled)
        .expect_err("a pathological scale must fail closed, not panic/abort");
    let msg = err.reason.to_string();
    assert!(
        msg.contains("corrupt SSTable") && msg.contains("issue #1754"),
        "expected a typed corruption error, got: {msg}"
    );
}

/// Issue #1754: `scale == i32::MIN` exercises `unsigned_abs()` (a plain
/// `-scale` would overflow-panic under `overflow-checks`). Still fail-closed,
/// never a panic.
#[test]
fn test_decimal_to_string_i32_min_scale_errors_not_panics() {
    let unscaled = vec![0x01];
    let err = decimal_to_string(i32::MIN, &unscaled)
        .expect_err("i32::MIN scale must fail closed without overflow panic");
    assert!(err.reason.to_string().contains("issue #1754"));
}

/// Issue #1754: an oversized unscaled magnitude (digit count beyond the cap)
/// also fails closed rather than allocating an unbounded string.
#[test]
fn test_decimal_to_string_oversized_unscaled_errors() {
    // ~1 MB of unscaled bytes → ~2.4M decimal digits, above the 1M cap.
    let unscaled = vec![0x7f; 1_000_000];
    let err = decimal_to_string(0, &unscaled)
        .expect_err("an oversized unscaled magnitude must fail closed");
    assert!(err.reason.to_string().contains("issue #1754"));
}

/// Regression guard: a large-but-representable scale at the boundary still
/// renders (the guard must not over-reject a legitimate value).
#[test]
fn test_decimal_to_string_large_representable_scale_ok() {
    let unscaled = vec![0x01]; // = 1
                               // scale 100 → "0." followed by 99 zeros then "1".
    let s = decimal_to_string(100, &unscaled).unwrap();
    assert!(s.starts_with("0.0") && s.ends_with('1') && s.len() == 102);
}

#[test]
fn test_inet_bytes_to_string_ipv4() {
    assert_eq!(
        inet_bytes_to_string(&[192, 168, 1, 1]),
        Ok("192.168.1.1".to_string())
    );
}

#[test]
fn test_inet_bytes_to_string_ipv6() {
    let raw = [
        0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73,
        0x34,
    ];
    assert_eq!(
        inet_bytes_to_string(&raw),
        Ok("2001:db8:85a3::8a2e:370:7334".to_string())
    );
}

/// Issue #1453: a malformed inet (length != 4/16) yields a typed error naming
/// the bad length — never a silent passthrough. This is the reference outcome
/// the Python binding was aligned to (both bindings now fail the same way).
#[test]
fn test_inet_bytes_to_string_malformed_lengths_error() {
    for bad_len in [0usize, 1, 3, 5, 6, 8, 15, 17, 32] {
        let raw = vec![0u8; bad_len];
        assert_eq!(
            inet_bytes_to_string(&raw),
            Err(format!(
                "Invalid inet address length: {bad_len} (expected 4 or 16)"
            )),
            "length {bad_len} must be a typed error"
        );
    }
}

// Issue #1448: prove the constructor-caching invariant without a live JS
// `Env`. `cache_get_or_try_init` is the single fetch-vs-cached decision point
// both `set_constructor` and `map_constructor` delegate to; exercising it with
// a plain `OnceCell<T>` and a counting `fetch` reproduces exactly the caching
// logic those methods use, so the work counter (bumped only on the fetch path)
// proves the "at most one lookup per cache, zero when unused" invariant.
//
// Both the zero-lookups case and the once-per-cache case live in a SINGLE
// test on purpose: the counter is a process-global (the increment site is in
// library code, unlike the local-instance trick in `cqlite-core`'s
// `work_counters`), so splitting them into two `reset`-then-assert tests would
// race under Rust's default parallel test runner. Tests that touch the counter
// serialize on `CTOR_COUNTER_GUARD`.
#[test]
fn ctor_cache_fetches_at_most_once_per_cache() {
    let _guard = CTOR_COUNTER_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    testing::reset_ctor_lookups();

    // Zero lookups when no set/map cell is ever converted (a `ConvCtx` never
    // reaches `cache_get_or_try_init` unless a collection cell needs a ctor).
    assert_eq!(testing::ctor_lookups(), 0);

    let cell: OnceCell<u32> = OnceCell::new();

    // First access: cache miss -> exactly one fetch (counter goes 0 -> 1).
    let first = cache_get_or_try_init(&cell, || Ok(7u32)).expect("first init");
    assert_eq!(*first, 7);
    assert_eq!(testing::ctor_lookups(), 1);

    // Second access on the SAME cell: cache hit -> NO further fetch. This is
    // the per-cell repeat that used to re-`get_global()` before #1448.
    let second = cache_get_or_try_init(&cell, || {
        panic!("must not fetch again once cached");
    })
    .expect("second hit");
    assert_eq!(*second, 7);
    assert_eq!(testing::ctor_lookups(), 1);
}

// Issue #1449: FFI-call BUDGET ratchet for the #1448 constructor-caching win.
//
// The `ctor_lookups` counter is Rust-`#[cfg(test)]` only and NOT exposed to
// JS, so per the issue this FFI-call budget is asserted here (Rust) while the
// JS test owns the per-row heap-delta budget.
//
// A `ConvCtx` lives for a WHOLE result conversion; its two `OnceCell`s back
// the `Set` and `Map` constructor caches. `row_to_object` -> `value_to_napi`
// routes every set/map cell through `set_constructor`/`map_constructor`, both
// of which delegate to the single `cache_get_or_try_init` fetch-vs-cached
// decision point. So converting a wide result of ROWS rows, each with several
// set AND map cells, must still fetch each global constructor at most once for
// the entire result — total lookups <= 2 (one Set cache + one Map cache),
// regardless of row/cell count. A regression to per-cell `get_global()` would
// make this O(rows x collection-cells).
//
// This exercises `cache_get_or_try_init` directly on two shared cells (exactly
// what `set_constructor`/`map_constructor` delegate to) because instantiating a
// real `ConvCtx` needs a live napi `Env`, which is unavailable in a unit test.
#[test]
fn set_map_ctor_lookups_bounded_per_result() {
    let _guard = CTOR_COUNTER_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    testing::reset_ctor_lookups();

    // One pair of caches shared across the whole simulated result, as a real
    // per-result `ConvCtx` holds.
    let set_cell: OnceCell<u32> = OnceCell::new();
    let map_cell: OnceCell<u32> = OnceCell::new();

    const ROWS: usize = 200;
    const COLLECTION_CELLS_PER_ROW: usize = 5;
    for _ in 0..ROWS {
        for _ in 0..COLLECTION_CELLS_PER_ROW {
            let _ = cache_get_or_try_init(&set_cell, || Ok(1u32)).expect("set ctor");
            let _ = cache_get_or_try_init(&map_cell, || Ok(2u32)).expect("map ctor");
        }
    }

    // 2 caches, each accessed ROWS * COLLECTION_CELLS_PER_ROW = 1000 times,
    // but each fetched exactly once -> total 2. Budget is 2 (<=1 per cache).
    let lookups = testing::ctor_lookups();
    assert!(
        lookups <= 2,
        "constructor lookups {lookups} exceeded FFI-call budget of 2 \
         (<=1 per Set/Map cache per result); a regression to per-cell \
         get_global() would make this O(rows x collection-cells) — see #1449"
    );
}
