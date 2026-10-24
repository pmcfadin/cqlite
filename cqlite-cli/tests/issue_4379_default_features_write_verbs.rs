//! Issue #4379: the PACKAGED CLI (a default-features build of `cqlite-cli`) must
//! ship the write-backed toolbox verbs `salvage` (#4196), `rebuild` (#4197) and
//! `compact`.
//!
//! `release.yml`, the Homebrew tap and `cargo install cqlite-cli` all build
//! DEFAULT features. Before #4379 `write-support` was not a default feature, so
//! those binaries parsed and advertised the verbs (clap declares them
//! unconditionally) but printed "Write support is not enabled" and exited 1 the
//! moment one was invoked.
//!
//! `--help` alone cannot see that defect: clap renders it before dispatch, so the
//! stub binary passes it. Each verb is therefore ALSO invoked with arguments that
//! reach the handler (a nonexistent input path), and the test asserts the run did
//! not take the "not built" stub arm. A real handler rejects the missing input
//! with its own error; the stub never looks at it.
//!
//! This target deliberately declares NO `required-features` and is NOT gated on
//! `write-support`: it runs under a plain `cargo test -p cqlite-cli` (default
//! features), which is exactly the build this test exists to pin. It is gated off
//! only under `tombstones`, the one feature that compiles the salvage/rebuild
//! handlers out by design (`cqlite-cli/Cargo.toml`, issue #4196).

#![cfg(not(feature = "tombstones"))]

use assert_cmd::Command;
use tempfile::TempDir;

/// Substring every "not built" stub arm prints (`commands/mod.rs`
/// `dispatch_salvage` / `dispatch_rebuild`, `commands/write.rs` stubs).
const STUB_MARKER: &str = "Write support is not enabled";

fn cqlite() -> Command {
    Command::cargo_bin("cqlite").expect("cqlite binary is built for integration tests")
}

fn assert_help_ok(verb: &str) {
    let out = cqlite()
        .args([verb, "--help"])
        .output()
        .expect("spawn cqlite");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "`cqlite {verb} --help` exited {:?}; stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("Usage"),
        "`cqlite {verb} --help` printed no usage text: {stdout}"
    );
}

/// Run `cqlite <args>` and assert the handler was reached: neither stream
/// carries the stub marker. Returns combined output for further assertions.
fn assert_handler_reached(args: &[&str]) -> String {
    let out = cqlite().args(args).output().expect("spawn cqlite");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !combined.contains(STUB_MARKER),
        "`cqlite {}` took the write-support-disabled stub arm in a DEFAULT-features \
         build (issue #4379); output:\n{combined}",
        args.join(" ")
    );
    combined
}

#[test]
fn salvage_is_built_into_default_features() {
    assert_help_ok("salvage");
    let tmp = TempDir::new().expect("tempdir");
    let missing = tmp.path().join("no-such-sstable-dir");
    let out = tmp.path().join("out");
    assert_handler_reached(&[
        "salvage",
        missing.to_str().expect("utf8 path"),
        "--out",
        out.to_str().expect("utf8 path"),
    ]);
}

#[test]
fn rebuild_is_built_into_default_features() {
    assert_help_ok("rebuild");
    let tmp = TempDir::new().expect("tempdir");
    let missing = tmp.path().join("no-such-sstable-dir");
    let out = tmp.path().join("out");
    assert_handler_reached(&[
        "rebuild",
        missing.to_str().expect("utf8 path"),
        "--components",
        "Index.db",
        "--out",
        out.to_str().expect("utf8 path"),
    ]);
}

#[test]
fn compact_is_built_into_default_features() {
    assert_help_ok("compact");
    let tmp = TempDir::new().expect("tempdir");
    let missing = tmp.path().join("no-such-sstable-dir");
    let out = tmp.path().join("out");
    let schema = tmp.path().join("no-such-schema.cql");
    assert_handler_reached(&[
        "compact",
        missing.to_str().expect("utf8 path"),
        "--output",
        out.to_str().expect("utf8 path"),
        "--schema",
        schema.to_str().expect("utf8 path"),
    ]);
}
