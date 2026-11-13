//! Issue #4498: the crash-consistency harness (`issue_4498_crash_consistency.rs`) interposes
//! libc with `LD_PRELOAD` and reads `/proc/self/fd`, so it is Linux-only and compiles to nothing
//! elsewhere. This sibling target exists so a non-Linux run (macOS CI) says so BY NAME instead of
//! silently reporting zero tests for the harness: it passes, and prints that the matrix was NOT
//! exercised on this platform. The Linux gate/CI lanes are the ones that run it.

#![cfg(all(
    feature = "write-support",
    not(feature = "tombstones"),
    not(target_os = "linux")
))]

#[test]
fn crash_consistency_harness_is_linux_only_and_was_not_run_here() {
    eprintln!(
        "NOT RUN: issue #4498's crash-consistency matrix needs LD_PRELOAD + /proc (Linux); \
         this platform ({}) did not exercise it.",
        std::env::consts::OS
    );
}
