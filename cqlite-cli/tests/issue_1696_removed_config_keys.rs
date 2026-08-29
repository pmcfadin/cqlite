//! Issue #1696 (AH3, epic #1685 "config honesty"): a CLI config file that still
//! names a REMOVED key loads, and says so.
//!
//! # The property
//!
//! `cqlite-core`'s `Config` is a Rust struct, so deleting a decorative field
//! gives an embedder a compile error — the loudest signal possible. The CLI's
//! config is a FILE, where serde discards unknown keys in silence, so the same
//! deletion would leave a user whose `cqlite.toml` still says `[connection]`
//! believing they had configured something.
//!
//! The posture (stated once, crate-wide, in #1696):
//!
//! > A removed knob must produce a LOUD signal at the layer where it is set,
//! > never silence.
//!
//! For a file that means **parse-and-ignore PLUS a named warning** — not
//! `deny_unknown_fields`, which would hard-fail every user who copied our own
//! shipped `examples/example-config.toml` (it named all three removed keys).
//!
//! Both halves are asserted here, over the file surface itself, in all three
//! formats the CLI accepts:
//!
//! * the warning NAMES each dead key (so the user can find and delete it), and
//! * the file still LOADS, with every surviving key still taking effect.

use std::fs;

use cqlite_cli::config::Config;
use cqlite_cli::config_removed_keys::{warning_for_file, REMOVED_KEYS};
use tempfile::TempDir;

/// A config naming every removed key alongside surviving ones, per format.
/// Deliberately shaped like the OLD shipped `examples/example-config.toml`.
fn fixtures() -> Vec<(&'static str, String)> {
    vec![
        (
            "cqlite.toml",
            r#"
default_keyspace = "ks"

[connection]
timeout_ms = 5000
retry_attempts = 9
pool_size = 42

[output]
max_rows = 500
colors = false
pager = "less -SR"
timestamp_format = "%Y-%m-%d"
"#
            .to_string(),
        ),
        (
            "cqlite.yaml",
            r#"
default_keyspace: ks
connection:
  timeout_ms: 5000
  retry_attempts: 9
  pool_size: 42
output:
  max_rows: 500
  colors: false
  pager: less -SR
  timestamp_format: "%Y-%m-%d"
"#
            .to_string(),
        ),
        (
            "cqlite.json",
            r#"{
  "default_keyspace": "ks",
  "connection": { "timeout_ms": 5000, "retry_attempts": 9, "pool_size": 42 },
  "output": { "max_rows": 500, "colors": false, "pager": "less -SR",
              "timestamp_format": "%Y-%m-%d" }
}"#
            .to_string(),
        ),
    ]
}

/// Half one: the warning fires and NAMES every dead key, for every format.
#[test]
fn every_format_warns_and_names_each_removed_key() {
    let temp = TempDir::new().expect("temp dir");
    for (name, content) in fixtures() {
        let path = temp.path().join(name);
        fs::write(&path, &content).expect("write config");

        let warning = warning_for_file(&path, &content)
            .unwrap_or_else(|| panic!("{name} names removed keys and MUST warn"));

        assert!(
            warning.contains(&path.display().to_string()),
            "{name}: the warning must name the offending file: {warning}"
        );
        for removed in REMOVED_KEYS {
            assert!(
                warning.contains(removed.path),
                "{name}: the warning must name {}: {warning}",
                removed.path
            );
        }
    }
}

/// Half two: the file still LOADS — the removed keys are ignored, not fatal —
/// and every SURVIVING key still takes effect. A hard failure here is the
/// regression this posture exists to prevent.
#[test]
fn a_config_naming_removed_keys_still_loads_with_surviving_keys_intact() {
    for (name, content) in fixtures() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join(name);
        fs::write(&path, &content).expect("write config");

        let cli = <cqlite_cli::cli_types::Cli as clap::Parser>::parse_from(["cqlite"]);
        let config = Config::load(Some(path.clone()), &cli)
            .unwrap_or_else(|e| panic!("{name} must still LOAD despite removed keys: {e}"));

        assert_eq!(
            config.default_keyspace.as_deref(),
            Some("ks"),
            "{name}: a surviving top-level key must still take effect"
        );
        assert_eq!(
            config.output.max_rows,
            Some(500),
            "{name}: a surviving nested key must still take effect"
        );
        assert!(
            !config.output.colors,
            "{name}: a surviving nested key must still take effect"
        );
    }
}

/// A clean config produces NO warning: the signal must not become noise printed
/// on every single load.
#[test]
fn a_clean_config_is_silent() {
    let temp = TempDir::new().expect("temp dir");
    let content = "default_keyspace = \"ks\"\n\n[output]\nmax_rows = 10\ncolors = true\n";
    let path = temp.path().join("cqlite.toml");
    fs::write(&path, content).expect("write config");

    assert!(
        warning_for_file(&path, content).is_none(),
        "a config naming no removed key must be silent"
    );
}

/// The shipped example must not itself trip the warning — otherwise we ship a
/// file that scolds the user who copies it. This reads the real committed file,
/// so a future edit that re-adds a removed key fails here.
#[test]
fn the_shipped_example_config_names_no_removed_keys() {
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("examples")
        .join("example-config.toml");
    let content = fs::read_to_string(&example)
        .unwrap_or_else(|e| panic!("read {}: {e}", example.display()));

    assert!(
        warning_for_file(&example, &content).is_none(),
        "examples/example-config.toml must not name a key #1696 removed:\n{}",
        warning_for_file(&example, &content).unwrap_or_default()
    );
}
