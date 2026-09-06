//! Requirement R2.1 (issue #3997): the allocator the `cqlite-flight` BINARY
//! linked is observable from OUTSIDE the process.
//!
//! This drives the BUILT BINARY (`env!("CARGO_BIN_EXE_cqlite-flight")`) rather
//! than the library, because that is the only artifact that carries a
//! `#[global_allocator]` at all: `main.rs` — and with it the install site and the
//! `ALLOCATOR` const — is compiled into the bin target only, never into this test
//! binary. An in-library assertion could not tell a linked allocator from a
//! feature flag being on.
//!
//! **GATE FACT, so nobody reads a green gate as having run this.** The full
//! gate's `flight-tests` component runs `cqlite-flight` at `--lib --bins` ONLY,
//! and prints a run-time census naming the ~42 integration `--test` targets it
//! does NOT run and why (#3384/#3375). THIS TARGET IS ONE OF THEM: no gate
//! component executes it. The GATE-ENFORCING surface for R1/R2.1 is
//! `scripts/tests/test_flight_allocator_link.sh`, which is registered in
//! `tooling-tests`, builds the binary in BOTH feature states, checks the linked
//! symbols and re-checks this same `--version` contract. This file is the
//! cargo-native expression of the contract — valuable when someone runs
//! `cargo test -p cqlite-flight --test issue_3997_allocator_surface`, and not a
//! merge gate on its own.
//!
//! The single test passes in BOTH feature states by construction: it derives the
//! expected value from `cfg!(all(feature = "jemalloc", target_os = "linux"))`,
//! which cargo resolves for THIS build — the same predicate the binary's install
//! site uses, and cargo builds the bin dependency of an integration test with the
//! same feature set.

use std::process::Command;

/// The value R2.1 requires for the feature set this test was compiled under.
///
/// Derived from the predicate rather than hard-coded, so the one test is correct
/// with the feature on and off. Note the `target_os` half: `--features jemalloc`
/// on macOS is deliberately inert (the dependency is declared under a
/// `cfg(target_os = "linux")` target section), so `system` is the truthful answer
/// there and this test asserts it.
fn expected_allocator() -> &'static str {
    if cfg!(all(feature = "jemalloc", target_os = "linux")) {
        "jemalloc"
    } else {
        "system"
    }
}

/// A line satisfies R2.1's grammar iff it is EXACTLY `allocator: jemalloc` or
/// `allocator: system`.
///
/// Written as an exact whole-line match against a closed set rather than a
/// prefix test: a prefix accepts `allocator: jemalloc-maybe` and a whole-line
/// regex-free comparison is the honest spelling of `^allocator: (jemalloc|system)$`
/// for a line that has already been split on `\n`.
fn matches_grammar(line: &str) -> bool {
    line == "allocator: jemalloc" || line == "allocator: system"
}

#[test]
fn version_reports_exactly_one_allocator_line_naming_this_builds_allocator() {
    let exe = env!("CARGO_BIN_EXE_cqlite-flight");
    // NO `--data-dir`, which is a REQUIRED argument: `--version` must
    // short-circuit before required-argument validation, so a successful run
    // here also pins that property.
    let out = Command::new(exe)
        .arg("--version")
        .output()
        .unwrap_or_else(|e| panic!("failed to run {exe} --version: {e}"));

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "`{exe} --version` must exit 0 with no --data-dir supplied \
         (the flag has to short-circuit required-arg validation); \
         status={:?} stderr={stderr}",
        out.status.code()
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let allocator_lines: Vec<&str> = stdout.lines().filter(|l| matches_grammar(l)).collect();

    // EXACTLY one — "at least one" is not the contract. Two lines would mean two
    // sources of truth, and zero would mean the surface is missing.
    assert_eq!(
        allocator_lines.len(),
        1,
        "R2.1: `--version` stdout must hold EXACTLY ONE line matching \
         `^allocator: (jemalloc|system)$`; found {}. Full stdout:\n{stdout}",
        allocator_lines.len()
    );

    let expected = format!("allocator: {}", expected_allocator());
    assert_eq!(
        allocator_lines[0],
        expected,
        "R2.1: the reported allocator must match what THIS build linked. \
         cfg(all(feature = \"jemalloc\", target_os = \"linux\")) = {}, so the line \
         must be `{expected}`. Full stdout:\n{stdout}",
        cfg!(all(feature = "jemalloc", target_os = "linux"))
    );
}
