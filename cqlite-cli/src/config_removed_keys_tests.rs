//! Tests for [`crate::config_removed_keys`] (issue #1696).
//!
//! Every case asserts BOTH halves of the documented posture: the removed key is
//! REPORTED (never silently swallowed) and the file still PARSES (never a hard
//! failure that would break a user who copied our own shipped example).

use super::*;

/// A config file naming every removed key, in each of the three formats the CLI
/// accepts. Deliberately the shape of the OLD shipped `examples/example-config.toml`.
const OLD_TOML: &str = r#"
default_database = "/tmp/db"

[connection]
timeout_ms = 5000
retry_attempts = 9
pool_size = 42

[output]
max_rows = 500
pager = "less -SR"
colors = true
timestamp_format = "%Y-%m-%d"
"#;

const OLD_YAML: &str = r#"
default_database: /tmp/db
connection:
  timeout_ms: 5000
  retry_attempts: 9
  pool_size: 42
output:
  max_rows: 500
  pager: less -SR
  colors: true
  timestamp_format: "%Y-%m-%d"
"#;

const OLD_JSON: &str = r#"
{
  "default_database": "/tmp/db",
  "connection": { "timeout_ms": 5000, "retry_attempts": 9, "pool_size": 42 },
  "output": { "max_rows": 500, "pager": "less -SR", "colors": true,
              "timestamp_format": "%Y-%m-%d" }
}
"#;

fn detected(extension: &str, content: &str) -> Vec<&'static str> {
    let document = parse_for_inspection(Some(extension), content)
        .unwrap_or_else(|| panic!("{extension} fixture must parse for inspection"));
    removed_keys_present(&document)
        .into_iter()
        .map(|r| r.path)
        .collect()
}

/// TOML: all three removed keys are named.
#[test]
fn toml_removed_keys_are_detected() {
    let found = detected("toml", OLD_TOML);
    assert_eq!(
        found,
        vec!["connection", "output.pager", "output.timestamp_format"],
        "every removed key present in the document must be reported"
    );
}

/// YAML: the same detection, so a format cannot drift out of coverage.
#[test]
fn yaml_removed_keys_are_detected() {
    let found = detected("yaml", OLD_YAML);
    assert_eq!(
        found,
        vec!["connection", "output.pager", "output.timestamp_format"]
    );
}

/// JSON: likewise.
#[test]
fn json_removed_keys_are_detected() {
    let found = detected("json", OLD_JSON);
    assert_eq!(
        found,
        vec!["connection", "output.pager", "output.timestamp_format"]
    );
}

/// A document naming NONE of the removed keys must produce NO warning — the
/// warning has to be a signal, not noise on every load.
#[test]
fn a_clean_document_produces_no_warning() {
    let clean = r#"
default_database = "/tmp/db"

[output]
max_rows = 500
colors = true
"#;
    let found = detected("toml", clean);
    assert!(
        found.is_empty(),
        "clean config must report nothing: {found:?}"
    );

    let document = parse_for_inspection(Some("toml"), clean).expect("parses");
    let present = removed_keys_present(&document);
    assert!(deprecation_warning("cqlite.toml", &present).is_none());
}

/// The dotted path is scoped: a top-level `pager`, or a `pager` under some other
/// table, is NOT `output.pager` and must not be reported. Otherwise the warning
/// would accuse users of setting keys they never set.
#[test]
fn dotted_paths_are_scoped_to_their_parent() {
    let elsewhere = r#"
pager = "less"

[repl]
pager = "more"
timestamp_format = "whatever"
"#;
    let found = detected("toml", elsewhere);
    assert!(
        found.is_empty(),
        "a `pager` outside [output] is a different key: {found:?}"
    );
}

/// Key PRESENCE is what matters, including an explicitly-null value: the user
/// still wrote the key and still believes it does something.
#[test]
fn an_explicitly_null_removed_key_is_still_reported() {
    let json = r#"{ "output": { "pager": null } }"#;
    let found = detected("json", json);
    assert_eq!(found, vec!["output.pager"]);
}

/// The warning text must NAME each dead key — a generic "some keys were ignored"
/// leaves the user hunting, which is the silence this is meant to replace.
#[test]
fn the_warning_names_every_dead_key_and_the_file() {
    let document = parse_for_inspection(Some("toml"), OLD_TOML).expect("parses");
    let present = removed_keys_present(&document);
    let warning = deprecation_warning("/etc/cqlite.toml", &present).expect("keys are present");

    assert!(
        warning.contains("/etc/cqlite.toml"),
        "must name the file: {warning}"
    );
    assert!(
        warning.contains("NO EFFECT"),
        "must say the keys do nothing: {warning}"
    );
    assert!(
        !warning.contains("still loads"),
        "the warning must make NO claim about the fate of the load — it runs \
         before stages that can still fail (#1696 roborev r5 F1): {warning}"
    );
    for removed in REMOVED_KEYS {
        assert!(
            warning.contains(removed.path),
            "warning must name {}: {warning}",
            removed.path
        );
    }
    assert!(
        warning.contains("#1696"),
        "the warning must cite the issue that removed the keys: {warning}"
    );
}

/// Singular/plural: one dead key must not read "1 configuration keys".
#[test]
fn the_warning_agrees_in_number() {
    let one = parse_for_inspection(Some("toml"), "[output]\npager = \"less\"\n").expect("parses");
    let present = removed_keys_present(&one);
    let warning = deprecation_warning("cqlite.toml", &present).expect("one key present");
    assert!(
        warning.contains("1 configuration key that"),
        "singular phrasing expected: {warning}"
    );
}

/// The inspection parse must never be the thing that rejects a file: unparseable
/// content and unknown extensions both yield `None`, leaving the real error to
/// `Config::load_from_file`.
#[test]
fn inspection_never_owns_the_parse_error() {
    assert!(parse_for_inspection(Some("toml"), "this is not = = toml").is_none());
    assert!(parse_for_inspection(Some("ini"), "[connection]").is_none());
    assert!(parse_for_inspection(None, "{}").is_none());
}

/// Registry hygiene: no duplicate paths, and every note is substantive enough to
/// tell the user what happened.
#[test]
fn the_removed_key_table_is_well_formed() {
    let mut seen: Vec<&str> = Vec::new();
    for removed in REMOVED_KEYS {
        assert!(
            !seen.contains(&removed.path),
            "duplicate REMOVED_KEYS entry for {}",
            removed.path
        );
        seen.push(removed.path);
        assert!(
            removed.note.len() >= 40,
            "{}'s note is too thin to explain the removal: {:?}",
            removed.path,
            removed.note
        );
        assert!(
            removed.note.contains("#"),
            "{}'s note must cite the removing issue",
            removed.path
        );
    }
}
