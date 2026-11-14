//! Issue #4498: the crash-consistency harness (`issue_4498_crash_consistency.rs`) interposes
//! libc with `LD_PRELOAD` and reads `/proc/self/fd`, so it is Linux-only and compiles to nothing
//! elsewhere, on non-x86_64/aarch64 CPUs, and with `tombstones` enabled (that build has no salvage/rebuild verbs). This sibling
//! target exists so such a run (macOS CI, `--all-features`) says so BY NAME instead of silently
//! reporting zero tests for the harness: it passes, and prints that the matrix was NOT exercised
//! and why. The default-feature Linux gate/CI lanes are the ones that run it.

#![cfg(all(
    feature = "write-support",
    any(
        feature = "tombstones",
        not(target_os = "linux"),
        not(any(target_arch = "x86_64", target_arch = "aarch64"))
    )
))]

#[test]
fn crash_consistency_harness_was_not_run_in_this_configuration() {
    // Two reasons the matrix compiles out, each named so the gap is never silently empty:
    //  * not Linux: it needs LD_PRELOAD + /proc;
    //  * not x86_64/aarch64: the shim's raw-syscall decoding is written for the 64-bit SysV ABI;
    //  * `tombstones` (e.g. `--all-features`): that build compiles the `salvage`/`rebuild` verbs
    //    out of the binary (see `commands::salvage`'s own gate), so there is nothing to sweep.
    let why = if cfg!(feature = "tombstones") {
        "the `tombstones` feature is enabled, which compiles the salvage/rebuild verbs out of the CLI"
    } else if cfg!(target_os = "linux") {
        "this CPU architecture is not x86_64/aarch64 (the shim's raw-syscall decoding targets the 64-bit SysV ABI)"
    } else {
        "this platform is not Linux (the harness needs LD_PRELOAD + /proc)"
    };
    eprintln!(
        "NOT RUN: issue #4498's crash-consistency matrix was not exercised here: {why} \
         (platform: {}).",
        std::env::consts::OS
    );
}
