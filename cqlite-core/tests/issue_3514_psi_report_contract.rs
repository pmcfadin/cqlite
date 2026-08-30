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

/// A fully-stalled window is exactly 100%, which is legal and must NOT be
/// rejected — the boundary between the accepted and the void.
#[test]
fn fully_stalled_window_is_exactly_one_hundred_percent() {
    let out = describe_stall(0, 1_000_000, 1_000_000);
    assert!(
        out.contains("100.0%") && !out.contains("UNMEASURED"),
        "stall equal to wall is the legal maximum, not an inverted window; got: {out}"
    );
}

/// An INVERTED window — stall exceeding wall — is impossible under the nesting
/// invariant, so it must be REJECTED as unmeasurable, never capped and never
/// printed as a percentage (#3514 r2).
///
/// # Why this case exists, and what the previous one could not see
///
/// The round-1 test fed only intervals where numerator == denominator, so it
/// pinned the ARITHMETIC (`x/x = 100%`) and not the ORDERING. It therefore PASSED
/// while the opening-side ordering bug was live: `start` was read before the wall
/// anchor was captured, so a scheduling delay let the stall interval begin outside
/// the wall interval and the ratio exceed 100%. A test that cannot distinguish the
/// fixed code from the broken code is not evidence.
///
/// Here the two intervals are injected INDEPENDENTLY, so the stall delta CAN
/// exceed the denominator — which is what makes the contract falsifiable at all.
/// The percentages are chosen to span the plausible skew (a hair over) and the
/// gross (double), because a clamp would hide both identically.
#[test]
fn inverted_window_is_rejected_not_capped() {
    for (started, ended, wall, label) in [
        (
            0u64,
            1_000_001u64,
            1_000_000u128,
            "a hair over — the real skew shape",
        ),
        (0, 2_000_000, 1_000_000, "double — a gross inversion"),
        (500_000, 1_600_000, 1_000_000, "non-zero start, over by 10%"),
        (0, 1, 0u128 + 0, "1us of stall in a zero-width window"),
    ] {
        let out = describe_stall(started, ended, wall);
        assert!(
            out.contains("UNMEASURED"),
            "an inverted window ({label}) must be rejected as UNMEASURED; got: {out}"
        );
        // REJECT, not CAP: a clamped "100.0%" is a plausible-looking number standing
        // in for an impossible measurement — the same mistake as blocker 1's 0.0%.
        assert!(
            !out.contains('%') || out.contains("not a value") || out.contains("not 0%"),
            "the rejection must not report a stall percentage ({label}); got: {out}"
        );
        for capped in ["100.0%", "110.0%", "200.0%", "0.0%"] {
            assert!(
                !out.contains(capped),
                "an inverted window must not be rendered as {capped} ({label}); got: {out}"
            );
        }
    }
}

/// No legal input may produce a percentage above 100%: swept across the whole
/// stall/wall lattice rather than at hand-picked points, so the ordering contract
/// is checked by its consequence and not by one example.
#[test]
fn no_input_ever_yields_a_percentage_above_one_hundred() {
    for wall in [1u128, 7, 1_000, 999_999, 1_000_000, 3_000_000] {
        for stalled in [0u64, 1, 3, 999, 999_999, 1_000_000, 2_999_999, 4_000_000] {
            let out = describe_stall(0, stalled, wall);
            if u128::from(stalled) > wall {
                assert!(
                    out.contains("UNMEASURED"),
                    "stalled={stalled} > wall={wall} must be UNMEASURED; got: {out}"
                );
                continue;
            }
            // Legal: parse the percentage back out and check the bound numerically,
            // so this cannot be defeated by a formatting change the way a
            // substring blacklist can.
            let pct: f64 = out
                .split("some-stall ")
                .nth(1)
                .and_then(|rest| rest.split('%').next())
                .unwrap_or_else(|| panic!("no percentage in a legal report: {out}"))
                .parse()
                .unwrap_or_else(|e| panic!("unparseable percentage in {out}: {e}"));
            assert!(
                (0.0..=100.0).contains(&pct),
                "stalled={stalled} over wall={wall} yielded {pct}%, outside 0..=100"
            );
        }
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
