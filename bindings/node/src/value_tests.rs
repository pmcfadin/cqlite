//! Unit tests for `value.rs` conversions (extracted per campsite rule, #1116).
//!
//! Wired from `value.rs` via `#[cfg(test)] #[path = "value_tests.rs"] mod tests;`,
//! so `super::*` here resolves to the `value` module and every helper under test
//! (`decimal_to_string`, `cache_get_or_try_init`, `testing::*`) is in scope
//! exactly as it was inline.

use super::*;

/// Serializes every test that resets/reads the process-global
/// `ctor_lookups` counter. The increment site lives in library code (a true
/// process-global, unlike the local-instance trick in `cqlite-core`'s
/// `work_counters`), so two `reset`-then-assert tests running under Rust's
/// default parallel runner would race. Both counter tests take this guard.
static CTOR_COUNTER_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

// DECIMAL and INET semantics — every scale/sign/boundary/ceiling case — are
// tested where the ONE implementation lives, in `cqlite-ffi-common`
// (`src/decimal_tests.rs`, `src/inet.rs`), and asserted end-to-end through this
// binding's production path by `__test__/shared-vectors.test.js` against the
// committed cross-binding vector table (issue #1452). What remains here is the
// part that is genuinely local: the ADAPTER, i.e. that the shared refusal
// becomes a napi error carrying the canonical message, and that the shared
// rendering is returned unaltered.

/// The adapter returns the shared rendering verbatim — it must not re-format.
#[test]
fn decimal_adapter_returns_the_shared_rendering_unaltered() {
    for (scale, unscaled) in [(2i32, &[123u8][..]), (0, &[123][..]), (-2, &[123][..])] {
        assert_eq!(
            decimal_to_string(scale, unscaled).expect("a well-formed value must render"),
            cqlite_ffi_common::decimal::decimal_to_string(scale, unscaled)
                .expect("the shared implementation must render it"),
        );
    }
}

/// A refusal from the shared implementation becomes a napi error whose reason
/// carries the ONE canonical message — not a second spelling invented here.
#[test]
fn decimal_adapter_maps_a_refusal_onto_the_canonical_napi_error() {
    let unscaled = vec![0x7f; cqlite_ffi_common::decimal::DECIMAL_MAX_UNSCALED_BYTES + 1];
    let err = decimal_to_string(3, &unscaled).expect_err("beyond the ceiling must fail closed");
    let canonical = cqlite_ffi_common::decimal::DecimalError::UnscaledTooLarge {
        scale: 3,
        unscaled_len: unscaled.len(),
        max_unscaled_bytes: cqlite_ffi_common::decimal::DECIMAL_MAX_UNSCALED_BYTES,
    }
    .to_string();
    assert!(
        err.reason.contains(&canonical),
        "napi error reason must carry the canonical message; got {}",
        err.reason
    );
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
