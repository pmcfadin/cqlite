//! Issue #3514 — contract tests for the PSI diagnostic in
//! `support/os_thread_budget.rs`.
//!
//! # Why this is a separate file
//!
//! The two thread-budget pins that consume that module
//! (`issue_2316_merge_thread_budget.rs`, `issue_2370_concurrent_merge_thread_budget.rs`)
//! each hold EXACTLY ONE `#[test]` by design: they observe a WHOLE-PROCESS OS
//! thread count, which is only meaningful with one test per process. So the PSI
//! contract cannot be tested in either of them, and the module itself cannot carry
//! `#[cfg(test)]` tests (it is `#[path]`-included into both binaries). This file is
//! the third consumer, and it counts no threads — so it is free to hold several
//! tests.
//!
//! # What is pinned here
//!
//! The PSI reading is DIAGNOSTIC ONLY — it never gates, skips or widens anything.
//! Its whole value is that the next person reading a red TRUSTS it, which makes two
//! properties correctness-bearing:
//!
//! 1. **An unmeasurable value is never rendered as `0%`** (#3514 blocker 1). A
//!    `saturating_sub` on a backwards `some total=` counter printed "0.0% stall" —
//!    on a possibly-starved host, i.e. it actively MISCLASSIFIED the one red it
//!    exists to classify, in the worst direction.
//! 2. **The percentage cannot exceed 100%** (#3514 nit 3), because the wall
//!    denominator's interval is a superset of the stall numerator's.

#![cfg(feature = "write-support")]

#[path = "support/os_thread_budget.rs"]
mod os_thread_budget;

use os_thread_budget::{describe_stall, min_cpus_for_amplification, reap_confirm_timeout};
use std::time::Duration;

/// A backwards counter must report UNMEASURED, naming the cause — never `0.0%`.
#[test]
fn backwards_psi_counter_reports_unmeasured_not_zero_percent() {
    let out = describe_stall(5_000_000, 4_000_000, 1_000_000);
    assert!(
        out.contains("UNMEASURED") && out.contains("BACKWARDS"),
        "a backwards 'some total=' counter must report UNMEASURED and say so; got: {out}"
    );
    // The regression this exists for: the old saturating_sub rendered 0 stall.
    assert!(
        !out.contains("0.0%"),
        "a backwards counter must NEVER be rendered as a stall percentage — an \
         unmeasurable value is not the value zero; got: {out}"
    );
    // Both readings must be in the message so the cause is diagnosable on sight.
    assert!(
        out.contains("5000000") && out.contains("4000000"),
        "the UNMEASURED message must name both readings; got: {out}"
    );
}

/// Equal readings are a genuine, measured zero — that one IS `0.0%`.
#[test]
fn genuinely_zero_stall_is_reported_as_a_measurement() {
    let out = describe_stall(7_000_000, 7_000_000, 2_000_000);
    assert!(
        out.contains("0.0%") && !out.contains("UNMEASURED"),
        "an equal pair of readings is a MEASURED zero stall, not an unmeasurable \
         one — the two must stay distinguishable; got: {out}"
    );
}

/// A zero-width window has no denominator, so it is unmeasurable, not 0%.
#[test]
fn zero_width_window_is_unmeasured_not_zero_percent() {
    let out = describe_stall(1_000, 2_000, 0);
    assert!(
        out.contains("UNMEASURED") && !out.contains("0.0%"),
        "a zero-width window cannot be normalised, so it must report UNMEASURED; got: {out}"
    );
}

/// The normal path reports a percentage normalised by wall time.
#[test]
fn stall_is_normalised_by_wall_time() {
    let out = describe_stall(0, 250_000, 1_000_000);
    assert!(
        out.contains("25.0%") && out.contains("DIAGNOSTIC ONLY"),
        "250ms stalled over 1s of wall is 25.0%, and the line must state it is \
         diagnostic only; got: {out}"
    );
}

/// A fully-stalled window saturates at 100%, never above it (#3514 nit 3): the
/// closing counter is read BEFORE the wall denominator is taken, so the
/// denominator's interval is a superset of the numerator's.
#[test]
fn stall_percentage_never_exceeds_one_hundred() {
    let out = describe_stall(0, 1_000_000, 1_000_000);
    assert!(
        out.contains("100.0%"),
        "a fully-stalled window is 100.0%; got: {out}"
    );
    for over in ["100.1%", "101.", "1000.", "200."] {
        assert!(
            !out.contains(over),
            "the percentage must not exceed 100% (found {over}); got: {out}"
        );
    }
}

/// The vacuity threshold is the arithmetic both pins claim it is (#3438 AC2).
/// Pinned here because it is the ONE place a divergence would silently un-run a
/// pin rather than fail it, and because both callers' skip paths report `ok`.
#[test]
fn vacuity_threshold_matches_the_derivation() {
    // #2370: producers = C·M = 2·3 = 6, bound = 3·3·2 + 6 = 24 → 24/6 = 4.
    assert_eq!(min_cpus_for_amplification(6, 24), 4);
    // #2316: producers = M = 4, bound = 3·4 + 3 = 15 → 15/4 = 3.
    assert_eq!(min_cpus_for_amplification(4, 15), 3);
    // The boundary is STRICT exceedance: at the returned c the pre-change cost
    // must exceed the bound, and at c-1 it must not.
    for (producers, bound) in [(6usize, 24usize), (4, 15), (8, 30), (1, 4), (5, 5)] {
        let c = min_cpus_for_amplification(producers, bound);
        assert!(
            producers * (1 + c) > bound,
            "at the derived minimum c={c} the pre-change cost {}·(1+{c}) must EXCEED \
             bound {bound}",
            producers
        );
        if c > 1 {
            assert!(
                producers * c <= bound,
                "at c-1={} the pre-change cost must NOT exceed bound {bound}, or the \
                 threshold is not minimal",
                c - 1
            );
        }
    }
    // Degenerate input must not divide by zero, and must never claim observable.
    assert_eq!(min_cpus_for_amplification(0, 24), usize::MAX);
}

/// The reap-confirm budget scales linearly in the producer count and reproduces
/// #2316's shipped 60 s exactly at its 4 producers (so that refactor was a pure
/// extraction), and 90 s at #2370's 6.
#[test]
fn reap_confirm_budget_scales_with_producers() {
    assert_eq!(reap_confirm_timeout(4), Duration::from_secs(60));
    assert_eq!(reap_confirm_timeout(6), Duration::from_secs(90));
    // Monotonic in the producer count.
    for p in 1..20usize {
        assert!(
            reap_confirm_timeout(p + 1) >= reap_confirm_timeout(p),
            "budget must not shrink as the producer count grows (at p={p})"
        );
    }
    // Floor: a drain can only be OBSERVED in at least one keep-alive plus one
    // full quiescence span, so even a degenerate count gets that much.
    assert!(reap_confirm_timeout(0) >= Duration::from_secs(22));
}
