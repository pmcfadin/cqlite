//! Issue #1696 (AH3, epic #1685 "config honesty"): a CLI config file that still
//! names a REMOVED key loads, and says so.
//!
//! # The property
//!
//! `cqlite-core`'s `Config` is a Rust struct, so deleting a decorative field
//! gives an embedder writing RUST a compile error — the loudest signal possible.
//! The CLI's config is a FILE, where serde discards unknown keys in silence, so
//! the same deletion would leave a user whose `cqlite.toml` still says
//! `[connection]` believing they had configured something. (The same is true of
//! `Config`'s OWN JSON/dict surface, which is why core carries this mechanism
//! too — see `cqlite_core::config_removed_keys`, #1696 roborev F1.)
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

use cqlite_cli::config::removed_keys::{warning_for_file, REMOVED_KEYS};
use cqlite_cli::config::Config;
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
    let content =
        fs::read_to_string(&example).unwrap_or_else(|e| panic!("read {}: {e}", example.display()));

    assert!(
        warning_for_file(&example, &content).is_none(),
        "examples/example-config.toml must not name a key #1696 removed:\n{}",
        warning_for_file(&example, &content).unwrap_or_default()
    );
}

/// A failed DESERIALIZE produces no removed-key report at all (#1696 roborev F3).
///
/// Originally this test was about a false promise: the text said the keys "are
/// IGNORED — the configuration still loads", and a document naming a removed key
/// AND carrying an invalid surviving value printed that assurance and then failed
/// to load. The promise is GONE from the text (r5 F1), so the premise of the
/// original assertion is gone with it — but the coverage is not: the reporting
/// seam must still not hand a caller a warning for a document that never became a
/// `Config`, which is what this now pins.
///
/// Asserted over `parse_with_removed_key_report` — the seam `load_from_file`
/// itself runs, so the ordering under test is the real one, not a copy: on the
/// failure path there is no warning to print, and on the success path there is.
#[test]
fn a_failed_deserialize_produces_no_removed_key_report() {
    let temp = TempDir::new().expect("temp dir");

    // Names a removed key (`[connection]`) AND gives a SURVIVING key
    // (`output.max_rows`, an `Option<usize>`) a value of the wrong type, so the
    // document parses as TOML but cannot deserialize into `Config`.
    let content = r#"
default_keyspace = "ks"

[connection]
timeout_ms = 5000

[output]
max_rows = "not a number"
colors = true
"#;
    let path = temp.path().join("cqlite.toml");
    fs::write(&path, content).expect("write config");

    // Precondition: the removed key IS present, so this case really does exercise
    // the ordering rather than passing because there was nothing to warn about.
    assert!(
        warning_for_file(&path, content).is_some(),
        "fixture must name a removed key, else this test proves nothing"
    );

    let outcome = Config::parse_with_removed_key_report(&path, content);
    assert!(
        outcome.is_err(),
        "an invalid surviving value must still fail the load"
    );
    // `is_err()` carries no warning by construction: the tuple that would hold
    // one only exists on the `Ok` path. Stated as an assertion so the coupling is
    // not merely implied by the type.
    assert!(
        outcome
            .map(|(_, warning)| warning)
            .unwrap_or(None)
            .is_none(),
        "a failed deserialize must not yield a removed-key report"
    );

    // Control: the SAME removed key with every surviving value valid loads AND
    // warns — so what was removed is the false promise, not the warning.
    let good = r#"
default_keyspace = "ks"

[connection]
timeout_ms = 5000

[output]
max_rows = 500
colors = true
"#;
    let good_path = temp.path().join("good.toml");
    fs::write(&good_path, good).expect("write config");
    let (config, warning) = Config::parse_with_removed_key_report(&good_path, good)
        .expect("a valid config naming a removed key must load");
    assert_eq!(config.output.max_rows, Some(500));
    let warning = warning.expect("a successful load naming a removed key MUST warn");
    assert!(
        warning.contains("connection") && warning.contains("NO EFFECT"),
        "the warning must name the dead key and say it does nothing: {warning}"
    );
    assert!(
        !warning.contains("still loads"),
        "the warning must make NO claim about the fate of the load: {warning}"
    );
}

/// The class the previous test only covered ONE stage of (#1696 roborev r5 F1).
///
/// # Why the warning text, not its placement, had to change
///
/// Three rounds of review found the same defect at three DIFFERENT stages, each
/// fix moving the emission one stage later:
///
/// 1. F3 — emitted before deserialization succeeded (CLI).
/// 2. r2 F3 — same defect on the Python path, before validation there.
/// 3. r5 F1 — after deserialization but before the CLI's SEMANTIC validation.
///
/// This test is stage 3, and it is the one that shows why placement can never be
/// the fix. The document below deserializes PERFECTLY: `memory_limit_mb = 1` and
/// `cache_size_mb = 64` are both correctly-typed, in-range `[performance]`
/// values, so the removed-key scan runs on a fully successful load. It is
/// `to_core_config` — a LATER stage, mapping into `cqlite_core::Config` and
/// validating it — that rejects the file, because a 64 MiB block cache cannot fit
/// inside a 1 MiB memory limit. Any assurance about "the configuration" printed
/// at scan time is therefore false however late the scan is placed, because there
/// is always another stage after it.
///
/// So the assertion is not about ordering: it is that the warning the operator
/// sees names the dead keys and claims NOTHING about the outcome, while the load
/// as a whole still FAILS.
#[test]
fn a_removed_key_beside_a_semantically_invalid_value_warns_without_any_success_claim() {
    let temp = TempDir::new().expect("temp dir");

    let content = r#"
default_keyspace = "ks"

[connection]
timeout_ms = 5000

[performance]
query_timeout_ms = 30000
memory_limit_mb = 1
cache_size_mb = 64
"#;
    let path = temp.path().join("cqlite.toml");
    fs::write(&path, content).expect("write config");

    // Stage A: the deserialize SUCCEEDS, so the removed-key report is produced.
    // (If this ever starts failing, the fixture has stopped exercising the stage
    // this test exists for and the case below proves nothing.)
    let (config, warning) = Config::parse_with_removed_key_report(&path, content)
        .expect("the document must deserialize — the defect is at a LATER stage");
    let warning = warning.expect("a document naming `[connection]` MUST warn");
    assert!(
        warning.contains("connection"),
        "the warning must name the dead key: {warning}"
    );

    assert!(
        warning.contains("NO EFFECT"),
        "the warning must say the dead keys do nothing: {warning}"
    );
    // The whole point: no claim about the load, in either spelling we shipped.
    for forbidden in ["still loads", "IGNORED"] {
        assert!(
            !warning.contains(forbidden),
            "the warning must not claim anything about the fate of the load \
             (found {forbidden:?}), because a LATER stage still rejects this \
             document: {warning}"
        );
    }

    // Stage B: that later stage. `to_core_config` maps `[performance]` onto
    // `cqlite_core::Config` and validates it, and a 64 MiB block cache does not
    // fit in a 1 MiB memory limit — so the configuration does NOT, in fact, load.
    let error = cqlite_cli::core_config::to_core_config(&config)
        .expect_err("a 64 MiB cache inside a 1 MiB memory limit must be REJECTED")
        .to_string();
    assert!(
        error.contains("configuration") || error.contains("cache") || error.contains("memory"),
        "the rejection must name the real problem, not something incidental: {error}"
    );
}
