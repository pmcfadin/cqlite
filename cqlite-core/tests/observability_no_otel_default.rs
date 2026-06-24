//! Dependency-isolation guard for observability (epic #1031, issue #1043).
//!
//! Asserts the "zero-cost when off" contract at the dependency level: the DEFAULT
//! `cqlite-core` build must link NO opentelemetry crates, and the OTel stack must
//! appear ONLY under `--features observability`. This runs in the ordinary test
//! suite (it does not itself need the `observability` feature) by shelling out to
//! `cargo tree`.
//!
//! It mirrors `scripts/ci/observability_no_otel_default.sh` so the same guarantee
//! is enforced both in CI (script) and in `cargo test` (this file).

use std::process::Command;

/// Run `cargo tree` for cqlite-core with the given extra args and return stdout.
fn cargo_tree(extra_args: &[&str]) -> String {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut cmd = Command::new(cargo);
    cmd.arg("tree")
        .args(["-p", "cqlite-core", "-e", "features"])
        .args(extra_args);
    // Run from the workspace root (this crate's parent dir).
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    cmd.current_dir(format!("{manifest_dir}/.."));
    let out = cmd
        .output()
        .expect("failed to invoke `cargo tree` — is cargo on PATH?");
    assert!(
        out.status.success(),
        "cargo tree exited non-zero: {}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn opentelemetry_lines(tree: &str) -> Vec<String> {
    tree.lines()
        .filter(|l| l.to_ascii_lowercase().contains("opentelemetry"))
        .map(|l| l.to_string())
        .collect()
}

#[test]
fn default_build_links_no_opentelemetry() {
    let tree = cargo_tree(&[]);
    let hits = opentelemetry_lines(&tree);
    assert!(
        hits.is_empty(),
        "DEFAULT cqlite-core build must link NO opentelemetry crates, but found:\n{}",
        hits.join("\n")
    );
}

#[test]
fn observability_build_links_opentelemetry() {
    let tree = cargo_tree(&["--features", "observability"]);
    let hits = opentelemetry_lines(&tree);
    assert!(
        hits.iter().any(|l| l.contains("opentelemetry v")),
        "--features observability build must link the OTel stack, but cargo tree had no \
         `opentelemetry v` entry:\n{tree}"
    );
}
