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
//! reach its REAL handler and trip a handler-owned precondition, and the test
//! asserts that handler's own error text (and a failing exit). The stub never
//! evaluates those preconditions, and a clap argument error never prints them, so
//! the assertion distinguishes "handler reached" from "stub" and from "parse error".
//!
//! Two layers, so a read-only build stays testable:
//!   * `default_features_include_write_support` — runs in EVERY build. It reads
//!     this crate's own `Cargo.toml` and asserts `write-support` is in `default`.
//!     That is the property `release.yml` / Homebrew / `cargo install` depend on,
//!     and it holds independent of which features THIS test build enabled.
//!   * the per-verb binary tests — gated on `write-support` (and off under
//!     `tombstones`, which compiles the salvage/rebuild handlers out by design,
//!     issue #4196), so a read-only build
//!     (`--no-default-features --features tui,cli-helpers`) compiles them out
//!     instead of failing them. Under a plain `cargo test -p cqlite-cli` (default
//!     features) they run, which is exactly the packaged build.

use std::path::Path;

/// Parse the `default = [...]` list out of `cqlite-cli/Cargo.toml`'s
/// `[features]` table. Minimal on purpose: one line, double-quoted names.
fn default_features(manifest: &str) -> Vec<String> {
    let mut in_features = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_features = trimmed == "[features]";
            continue;
        }
        if !in_features {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("default") {
            let rest = rest.trim_start();
            let Some(list) = rest.strip_prefix('=') else {
                continue;
            };
            let list = list.trim();
            assert!(
                list.starts_with('[') && list.ends_with(']'),
                "`default` features must be a single-line list for this pin: {list}"
            );
            return list[1..list.len() - 1]
                .split(',')
                .map(|f| f.trim().trim_matches('"').to_string())
                .filter(|f| !f.is_empty())
                .collect();
        }
    }
    panic!("no `default = [...]` entry found in [features] of cqlite-cli/Cargo.toml");
}

#[test]
fn default_features_include_write_support() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let defaults = default_features(&manifest);
    assert!(
        defaults.iter().any(|f| f == "write-support"),
        "cqlite-cli default features {defaults:?} must include `write-support` so the \
         packaged CLI ships salvage/rebuild/compact (issue #4379)"
    );
}

#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
mod packaged_binary {
    use assert_cmd::Command;
    use tempfile::TempDir;

    /// Substring every "not built" stub arm prints (`commands/mod.rs`
    /// `dispatch_salvage` / `dispatch_rebuild`, `commands/write.rs` stubs).
    const STUB_MARKER: &str = "Write support is not enabled";

    fn cqlite() -> Command {
        let mut cmd =
            Command::cargo_bin("cqlite").expect("cqlite binary is built for integration tests");
        cmd.env("NO_COLOR", "1").env("RUST_LOG", "off");
        cmd
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

    /// Run `cqlite <args>`; assert it FAILED with the handler-owned error
    /// `handler_error` and did not take the stub arm.
    fn assert_handler_error(args: &[&str], handler_error: &str) {
        let out = cqlite().args(args).output().expect("spawn cqlite");
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let shown = args.join(" ");
        assert!(
            !combined.contains(STUB_MARKER),
            "`cqlite {shown}` took the write-support-disabled stub arm in a \
             default-features build (issue #4379); output:\n{combined}"
        );
        assert!(
            !out.status.success(),
            "`cqlite {shown}` succeeded on a nonexistent input; output:\n{combined}"
        );
        assert!(
            combined.contains(handler_error),
            "`cqlite {shown}` did not reach its handler: expected the handler's own \
             error {handler_error:?}; output:\n{combined}"
        );
    }

    #[test]
    fn salvage_is_built_into_default_features() {
        assert_help_ok("salvage");
        let input = TempDir::new().expect("tempdir");
        let out = TempDir::new().expect("tempdir");
        let missing = input.path().join("no-such-sstable-dir");
        let dest = out.path().join("out");
        // No global --schema: `commands/salvage` rejects that itself.
        assert_handler_error(
            &[
                "salvage",
                missing.to_str().expect("utf8 path"),
                "--out",
                dest.to_str().expect("utf8 path"),
            ],
            "cqlite salvage: --schema is required",
        );
    }

    #[test]
    fn rebuild_is_built_into_default_features() {
        assert_help_ok("rebuild");
        let input = TempDir::new().expect("tempdir");
        let out = TempDir::new().expect("tempdir");
        let missing = input.path().join("no-such-sstable-dir");
        let dest = out.path().join("out");
        // No global --schema: `commands/rebuild` rejects that itself.
        assert_handler_error(
            &[
                "rebuild",
                missing.to_str().expect("utf8 path"),
                "--components",
                "index",
                "--out",
                dest.to_str().expect("utf8 path"),
            ],
            "cqlite rebuild: --schema is required",
        );
    }

    #[test]
    fn compact_is_built_into_default_features() {
        assert_help_ok("compact");
        let input = TempDir::new().expect("tempdir");
        let out = TempDir::new().expect("tempdir");
        let missing = input.path().join("no-such-sstable-dir");
        let schema = out.path().join("schema.cql");
        std::fs::write(&schema, "CREATE TABLE ks.t (id int PRIMARY KEY, v text);\n")
            .expect("write schema");
        let dest = out.path().join("out");
        // Valid schema, missing input dir: `handle_compact` reads the dir itself.
        assert_handler_error(
            &[
                "compact",
                missing.to_str().expect("utf8 path"),
                "--output",
                dest.to_str().expect("utf8 path"),
                "--schema",
                schema.to_str().expect("utf8 path"),
            ],
            "Failed to read input directory",
        );
    }
}
