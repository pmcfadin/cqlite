//! JSON deserialization entry points for [`Config`] (issue #1696).
//!
//! Split out of `config.rs` under the campsite rule (epic #1116): that file is
//! already over the size target, and these two constructors are one cohesive
//! responsibility — turning a JSON document into a `Config` while REPORTING the
//! keys CQLite has removed, which is the only signal a non-Rust authoring surface
//! can get (see [`crate::config_removed_keys`]).

use super::Config;

impl Config {
    /// Deserialize a JSON `Config` document, reporting every key #1696 REMOVED
    /// that it still names.
    ///
    /// # Why this exists (#1696 roborev F1)
    ///
    /// Deleting a decorative field from this struct is a compile error for an
    /// embedder writing Rust, which is the loudest signal available — but serde
    /// DISCARDS unknown fields, so a JSON or dict authoring surface (the Python
    /// bindings' `cqlite.open(path, config=...)` bridge) silently accepted a
    /// pre-change document naming `performance`, `storage.block_size`,
    /// `query.parallel` and the rest, and ignored it. The rule #1696 states —
    /// *a removed knob must produce a LOUD signal at the layer where it is set* —
    /// was therefore false at exactly the layer that cannot get a compile error.
    ///
    /// The posture matches the CLI's file surface, crate-wide and deliberately:
    /// **parse-and-ignore PLUS a named warning**, never `deny_unknown_fields`,
    /// which would hard-fail a caller whose config predates the removal with no
    /// migration path.
    ///
    /// The warning is logged at WARN via `tracing`. A caller that must SURFACE it
    /// (the bindings raise a Python `UserWarning`) or assert it wants
    /// [`Self::from_json_str_reporting_removed`].
    ///
    /// # This constructor is OPTIONAL, so it does not enforce the rule
    ///
    /// `Config` derives `Deserialize`, so an embedder can call
    /// `serde_json::from_str::<Config>` directly and bypass this entirely — serde
    /// then DISCARDS the removed keys in silence. Enforcement at the serde
    /// boundary itself is **issue #3520** (#1696 roborev r2 F3, scoped out
    /// deliberately); do not read this constructor as universal coverage.
    ///
    /// # Errors
    ///
    /// The document is not valid JSON, or does not deserialize into a `Config`.
    /// Note that `Config` is not `#[serde(default)]`, so the document must be
    /// COMPLETE. This does NOT run [`Self::validate`] — the caller owns validating
    /// the config it finally uses, possibly after folding in overrides.
    pub fn from_json_str(json: &str) -> crate::Result<Self> {
        let (config, warning) = Self::from_json_str_reporting_removed(json, "this configuration")?;
        if let Some(warning) = warning {
            tracing::warn!("{warning}");
        }
        Ok(config)
    }

    /// As [`Self::from_json_str`], but RETURNS the removed-key warning instead of
    /// logging it, labelled with `source` (e.g. `"config dict"`).
    ///
    /// # ORDER
    ///
    /// The deserialize runs FIRST and the scan only on success, so this
    /// constructor never returns a removed-key report for a document that did
    /// not become a `Config`. Nothing is lost: serde drops the removed keys from
    /// `Config`, but nothing drops them from the text they were read out of.
    ///
    /// This ordering is a property of the RETURN SHAPE, not a precondition of the
    /// text: since #1696 roborev r5 F1 the warning asserts nothing about whether
    /// the load succeeds, precisely so that no placement of it can be wrong (see
    /// [`crate::config_removed_keys::deprecation_warning`]).
    ///
    /// # Errors
    ///
    /// See [`Self::from_json_str`].
    pub fn from_json_str_reporting_removed(
        json: &str,
        source: &str,
    ) -> crate::Result<(Self, Option<String>)> {
        let config: Self = serde_json::from_str(json)
            .map_err(|e| crate::Error::configuration(format!("invalid {source}: {e}")))?;
        let warning = crate::config_removed_keys::warning_for_json(source, json);
        Ok((config, warning))
    }
}
