//! Deprecation reporting for config-file keys CQLite has REMOVED (issue #1696).
//!
//! # Why this module exists
//!
//! Epic #1685 ("config honesty") deletes knobs nothing reads. In `cqlite-core`
//! that is free: `Config` is a Rust struct, so an embedder who still sets a
//! deleted field gets a COMPILE error — the loudest signal there is.
//!
//! The CLI's config is a **file** surface (`cqlite.toml` / `.yaml` / `.json`), and
//! serde silently discards unknown keys. So the same deletion would leave a user
//! whose `cqlite.toml` still says
//!
//! ```toml
//! [connection]
//! timeout_ms = 5000
//! ```
//!
//! believing they had configured a timeout, with nothing to tell them otherwise —
//! precisely the "decorative knob" experience the epic is removing.
//!
//! # Why not `deny_unknown_fields`
//!
//! Because our own shipped `examples/example-config.toml` named `[connection]`,
//! `pager` and `timestamp_format`. Hard-failing would break every user who copied
//! it, for a key that never did anything. So the posture is the crate-wide rule
//! stated in #1696:
//!
//! > A removed knob must produce a LOUD signal at the layer where it is set,
//! > never silence.
//!
//! For the file surface that means **parse-and-ignore PLUS a named warning**:
//! the config still loads, and the user is told exactly which keys are dead.
//!
//! # Why a raw-document scan
//!
//! The check runs on the PARSED DOCUMENT (`toml::Value` / `serde_yaml::Value` /
//! `serde_json::Value`) before deserializing into `Config`, because by the time
//! serde is done the removed keys are gone and unrecoverable. One `Removed` table
//! drives all three formats, so a format cannot drift out of coverage.

use std::fmt::Write as _;

/// A config-file key removed by an issue, and the issue that removed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Removed {
    /// Dotted path as it appears in the config file, e.g. `output.pager`.
    pub path: &'static str,
    /// Short note shown to the user: what it did, or that it never did anything.
    pub note: &'static str,
}

/// Every config-file key the CLI has removed.
///
/// Adding a removal here is what makes it a *reported* removal rather than a
/// silent one, so a deletion PR that forgets this table is incomplete.
pub const REMOVED_KEYS: &[Removed] = &[
    Removed {
        path: "connection",
        note: "the whole [connection] section (timeout_ms / retry_attempts / pool_size) \
               was removed in #1696: CQLite reads local SSTable files and never opens a \
               network connection, so none of it was ever read",
    },
    Removed {
        path: "output.pager",
        note: "removed in #1696: no code ever spawned a pager; use a shell pipe \
               (`cqlite ... | less`) instead",
    },
    Removed {
        path: "output.timestamp_format",
        note: "removed in #1696: no formatter ever read it; timestamps render in the \
               writer's own format",
    },
];

/// Which removed keys a parsed config document still names.
///
/// The document is walked by dotted path, so `output.pager` matches only a `pager`
/// under `output`, never a top-level `pager` or an unrelated `repl.pager`.
pub fn removed_keys_present(document: &Document) -> Vec<&'static Removed> {
    REMOVED_KEYS
        .iter()
        .filter(|removed| document.has_path(removed.path))
        .collect()
}

/// The operator-facing deprecation warning for a set of still-named removed keys,
/// or `None` when the document names none.
///
/// Returned as a string (rather than printed here) so the caller decides the sink
/// and a test can assert the exact text.
pub fn deprecation_warning(source: &str, present: &[&'static Removed]) -> Option<String> {
    if present.is_empty() {
        return None;
    }
    let mut out = format!(
        "warning: {source} names {} configuration key{} that CQLite has REMOVED. \
         They are IGNORED — the file still loads, but these settings have no effect:\n",
        present.len(),
        if present.len() == 1 { "" } else { "s" }
    );
    for removed in present {
        // Writing into a String is infallible; `let _` keeps this free of an
        // `unwrap()` in library code.
        let _ = writeln!(out, "  - {}: {}", removed.path, removed.note);
    }
    out.push_str("Delete them from the file to silence this warning.");
    Some(out)
}

/// A format-agnostic view of a parsed config document.
///
/// The three formats the CLI accepts have three unrelated `Value` types; this
/// wraps them so [`removed_keys_present`] is written once and every format is
/// covered by construction.
pub enum Document {
    /// A parsed `cqlite.toml`.
    Toml(toml::Value),
    /// A parsed `cqlite.yaml` / `cqlite.yml`.
    Yaml(serde_yaml::Value),
    /// A parsed `cqlite.json`.
    Json(serde_json::Value),
}

impl Document {
    /// Whether the document contains the given dotted path as a mapping key.
    ///
    /// Only KEY PRESENCE matters: a removed key set to any value at all — even
    /// `null` — is still a user believing they configured something.
    pub fn has_path(&self, path: &str) -> bool {
        match self {
            Self::Toml(root) => {
                let mut cursor = root;
                for segment in path.split('.') {
                    match cursor.as_table().and_then(|t| t.get(segment)) {
                        Some(next) => cursor = next,
                        None => return false,
                    }
                }
                true
            }
            Self::Yaml(root) => {
                let mut cursor = root;
                for segment in path.split('.') {
                    match cursor
                        .as_mapping()
                        .and_then(|m| m.get(serde_yaml::Value::String(segment.to_string())))
                    {
                        Some(next) => cursor = next,
                        None => return false,
                    }
                }
                true
            }
            Self::Json(root) => {
                let mut cursor = root;
                for segment in path.split('.') {
                    match cursor.as_object().and_then(|o| o.get(segment)) {
                        Some(next) => cursor = next,
                        None => return false,
                    }
                }
                true
            }
        }
    }
}

/// The deprecation warning a config FILE should produce, or `None` when it names
/// no removed keys (or is not an inspectable config format).
///
/// The single entry point [`crate::config::Config::load_from_file`] calls, and
/// the one an integration test can call for the same `(path, content)` pair — so
/// the wiring has no private step a test cannot reach.
pub fn warning_for_file(path: &std::path::Path, content: &str) -> Option<String> {
    let extension = path.extension().and_then(|ext| ext.to_str());
    let document = parse_for_inspection(extension, content)?;
    let present = removed_keys_present(&document);
    deprecation_warning(&path.display().to_string(), &present)
}

/// Parse `content` as `extension` purely to inspect it for removed keys.
///
/// Returns `None` when the extension is not a config format we accept or the
/// content does not parse: this check must never be the thing that rejects a
/// file. `load_from_file` does the real parse and owns the real error.
pub fn parse_for_inspection(extension: Option<&str>, content: &str) -> Option<Document> {
    match extension {
        Some("toml") => toml::from_str::<toml::Value>(content).ok().map(Document::Toml),
        Some("yaml") | Some("yml") => serde_yaml::from_str::<serde_yaml::Value>(content)
            .ok()
            .map(Document::Yaml),
        Some("json") => serde_json::from_str::<serde_json::Value>(content)
            .ok()
            .map(Document::Json),
        _ => None,
    }
}

#[cfg(test)]
#[path = "config_removed_keys_tests.rs"]
mod tests;
