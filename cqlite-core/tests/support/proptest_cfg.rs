//! Shared proptest harness configuration (issue #4497; reusable by #4257).
//!
//! * **Bounded PR tier, higher-volume nightly.** The case count is [`PR_TIER_CASES`]
//!   unless `PROPTEST_CASES` overrides it; the nightly sweep sets that variable
//!   (`.github/workflows/nightly-property-laws.yml`).
//! * **Seeded, and the seed is printed.** Every run picks (or is given, via
//!   `LAWS_SEED`) a `u64` seed and prints it. The libtest harness only shows captured
//!   output for a FAILING test, so a failure always carries the line
//!   `[<name>] seed=<n> cases=<c> (rerun: LAWS_SEED=<n> PROPTEST_CASES=<c> ...)`.
//! * **Regression files.** proptest persists a failing case as a `cc <hex>` line under
//!   `cqlite-core/proptest-regressions/`; those files are committed and replayed first.
//!
//! Included by `#[path]` from both the in-crate reconcile laws and the integration
//! byte-comparable test, so there is exactly one copy.
#![allow(dead_code)]

use proptest::test_runner::{Config, RngAlgorithm, RngSeed};

/// Cases per property on a PR (fixed and small: the property suite shares the gate's budget).
pub const PR_TIER_CASES: u32 = 64;

pub fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(PR_TIER_CASES)
}

fn fresh_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x4497_4497)
}

/// The `proptest!` config for property group `name`; prints the seed (see module docs).
pub fn config(name: &str) -> Config {
    let seed = std::env::var("LAWS_SEED")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or_else(fresh_seed);
    let cases = cases();
    eprintln!(
        "[{name}] seed={seed} cases={cases} (rerun: LAWS_SEED={seed} PROPTEST_CASES={cases} cargo test ...)"
    );
    Config {
        cases,
        rng_algorithm: RngAlgorithm::default(),
        rng_seed: RngSeed::Fixed(seed),
        ..Config::default()
    }
}
