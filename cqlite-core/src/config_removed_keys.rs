//! Deprecation reporting for configuration keys CQLite has REMOVED (issue #1696).
//!
//! # Why a Rust struct is not the whole story
//!
//! Deleting a decorative field from [`crate::Config`] is a COMPILE error for an
//! embedder writing Rust — the loudest signal available. But `Config` derives
//! `Deserialize`, and serde's default behaviour is to DISCARD unknown fields, so
//! every non-Rust authoring surface gets the opposite: a pre-change document that
//! still says
//!
//! ```json
//! { "storage": { "block_size": 65536 }, "query": { "parallel": { "enabled": true } } }
//! ```
//!
//! deserializes SUCCESSFULLY and is silently ignored. The Python bindings' dict /
//! JSON bridge is exactly that surface (`cqlite.open(path, config={...})`), which
//! made the crate-wide rule stated in #1696
//!
//! > A removed knob must produce a LOUD signal at the layer where it is set,
//! > never silence.
//!
//! false for anyone configuring CQLite through the bindings (#1696 roborev F1).
//!
//! # Why not `deny_unknown_fields`
//!
//! Because it would HARD-FAIL a Python caller whose config predates the removal,
//! with no migration path, for keys that never did anything. That is the opposite
//! of the parse-and-ignore-PLUS-a-named-warning posture deliberately chosen for
//! the CLI's file surface, and #1696 requires ONE consistent posture crate-wide.
//! So this module is the same mechanism, at the deserialization boundary.
//!
//! # One mechanism, one table per DOCUMENT SCHEMA
//!
//! [`Removed`], [`removed_keys_present`] and [`deprecation_warning`] live here and
//! are shared with `cqlite_cli::config::removed_keys`, so the wording, the
//! matching rule and the shape cannot drift between the two surfaces.
//!
//! The TABLES are necessarily per-schema and must not be crossed: this crate's
//! [`REMOVED_KEYS`] describes a `cqlite_core::Config` document, while the CLI's
//! describes a `cqlite.toml`/`.yaml`/`.json`. Applying this table to a CLI file
//! would be WRONG, not merely redundant — the CLI's `[performance]` section is
//! live (`query_timeout_ms`, `memory_limit_mb`, `cache_size_mb`) while the core's
//! `performance` tree was removed, so a shared table would scold a user for a key
//! that works.

use std::fmt::Write as _;

/// A configuration key removed by an issue, and why it is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Removed {
    /// Dotted path as it appears in the document, e.g. `storage.block_size`.
    pub path: &'static str,
    /// Short note shown to the user: what it did, or that it never did anything.
    pub note: &'static str,
}

/// Every `cqlite_core::Config` key removed by issue #1696.
///
/// # Scope
///
/// This table starts at #1696, which is where the mechanism starts: a removal PR
/// from now on extends it, and one that forgets to is incomplete. Removals that
/// predate the mechanism (e.g. #1619's decorative `compaction.strategy` /
/// `max_sstables` / `size_ratio` / `max_threads` / `background_interval`) are
/// deliberately NOT listed — they were never reported and adding them now would
/// be a separate, testable change rather than part of this one.
pub const REMOVED_KEYS: &[Removed] = &[
    Removed {
        path: "performance",
        note: "the whole `performance` tree (enable_metrics / metrics_interval / \
               enable_profiling / background_tasks) was removed in #1696: nothing \
               read any of it, so setting it changed nothing",
    },
    Removed {
        path: "storage.max_sstable_size",
        note: "removed in #1696: no writer or compaction path ever read it; SSTable \
               size follows from the memtable flush threshold \
               (`storage.memtable_size_threshold`)",
    },
    Removed {
        path: "storage.block_size",
        note: "removed in #1696: no reader or writer ever read it; on-disk block \
               framing comes from the SSTable's own CompressionInfo.db",
    },
    Removed {
        path: "storage.enable_bloom_filters",
        note: "removed in #1696: the read path never consulted it. The bloom-filter \
               code exists but is UNWIRED, so this knob could not switch anything \
               on or off",
    },
    Removed {
        path: "storage.bloom_filter_fp_rate",
        note: "removed in #1696: no filter was ever built from it (see \
               `storage.enable_bloom_filters`)",
    },
    Removed {
        path: "storage.io_threads",
        note: "removed in #1696: nothing sized a thread pool from it; I/O runs on \
               the caller's tokio runtime, which the embedder configures",
    },
    Removed {
        path: "storage.sync_mode",
        note: "removed in #1696: no write path ever read it; durability of the \
               write engine's own WAL is not selectable through this knob",
    },
    Removed {
        path: "query.plan_cache_size",
        note: "removed in #1696: no plan cache was ever sized from it",
    },
    Removed {
        path: "query.enable_optimization",
        note: "removed in #1696: the planner never consulted it, so it could not \
               disable anything",
    },
    Removed {
        path: "query.parallel",
        note: "the whole `query.parallel` tree (enabled / max_threads / \
               min_parallel_rows) was removed in #1696: no execution path ever \
               read it",
    },
];

/// Which of `table`'s removed keys a document still names.
///
/// `has_path` answers "does the document contain this dotted path as a mapping
/// key" for one concrete document type, so the FILTER is written once and every
/// surface (this crate's JSON, the CLI's TOML/YAML/JSON) shares it.
pub fn removed_keys_present<'a>(
    table: &'a [Removed],
    has_path: impl Fn(&str) -> bool,
) -> Vec<&'a Removed> {
    table
        .iter()
        .filter(|removed| has_path(removed.path))
        .collect()
}

/// The operator-facing deprecation warning for a set of still-named removed keys,
/// or `None` when the document names none.
///
/// Returned as a string rather than logged here so the caller picks the sink (the
/// CLI prints to stderr, the bindings raise a Python `DeprecationWarning`) and a
/// test can assert the exact text.
///
/// The text asserts "the configuration still loads", which is only true once the load
/// HAS succeeded — so every caller must produce this AFTER a successful
/// deserialize, never before (#1696 roborev F3).
pub fn deprecation_warning(source: &str, present: &[&'static Removed]) -> Option<String> {
    if present.is_empty() {
        return None;
    }
    let mut out = format!(
        "warning: {source} names {} configuration key{} that CQLite has REMOVED. \
         They are IGNORED — the configuration still loads, but these settings have no \
         effect:\n",
        present.len(),
        if present.len() == 1 { "" } else { "s" }
    );
    for removed in present {
        // Writing into a String is infallible; `let _` keeps this free of an
        // `unwrap()` in library code.
        let _ = writeln!(out, "  - {}: {}", removed.path, removed.note);
    }
    out.push_str("Delete them to silence this warning.");
    Some(out)
}

/// Whether a parsed JSON document contains `path` as an object key.
///
/// Walked segment by segment, so `storage.block_size` matches only a
/// `block_size` under `storage` — never a top-level one, nor a
/// `query.block_size`. Only KEY PRESENCE matters: a removed key set to any value
/// at all, `null` included, is still someone believing they configured something.
pub fn json_has_path(root: &serde_json::Value, path: &str) -> bool {
    let mut cursor = root;
    for segment in path.split('.') {
        match cursor.as_object().and_then(|object| object.get(segment)) {
            Some(next) => cursor = next,
            None => return false,
        }
    }
    true
}

/// The deprecation warning a JSON `Config` document should produce, or `None`
/// when it names no removed key (or is not parseable JSON at all).
///
/// Unparseable content yields `None` rather than an error: this scan must never
/// be the thing that rejects a document. [`crate::Config::from_json_str`] owns
/// the real parse and the real error, and only reaches this once that parse
/// SUCCEEDED.
pub fn warning_for_json(source: &str, content: &str) -> Option<String> {
    let document = serde_json::from_str::<serde_json::Value>(content).ok()?;
    let present = removed_keys_present(REMOVED_KEYS, |path| json_has_path(&document, path));
    deprecation_warning(source, &present)
}

#[cfg(test)]
#[path = "config_removed_keys_tests.rs"]
mod tests;
