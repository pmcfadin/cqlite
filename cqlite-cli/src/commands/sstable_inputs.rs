//! Input resolution for `compact` (`write.rs`), split out for the #1116/#1135 file-size ratchet.
//!
//! The input set is the generation module's Live set of ONE directory (issue #4626, requirements
//! G3, G8 and G10): no recursion, so `snapshots/` and `backups/` are never read (#4622), and
//! nothing is skipped silently. A generation `compact` cannot handle is refused BY NAME, and the
//! whole run stops: it never proceeds with the subset it can handle.
//!
//! What `compact` handles today is exactly what its old `nb-<digits>-big-Data.db` filter
//! accepted: BIG, version `nb`, sequence id. Everything else the old scanner dropped silently
//! (`oa`, BTI `da`, UUID ids, a generation without `TOC.txt`) is now a named refusal.
//! `explain` accepts `TocMissing` and has its own refusals (see `explain_resolve.rs`).

use anyhow::{bail, Result};
use cqlite_core::storage::sstable::generation::{
    list_generations, Condition, Generation, GenerationId, Scope, TocPolicy,
};
use std::path::{Path, PathBuf};

/// The Data.db paths of the Live generations of `dir`, newest first (the order `compact_sstables`
/// expects: run index 0 is the newest run).
#[cfg(feature = "write-support")]
pub(crate) fn discover_input_sstables(dir: &Path) -> Result<Vec<PathBuf>> {
    // G3.2/G10: a TocMissing generation is refused by name here, not skipped.
    let listing = list_generations(dir, &Scope::Live, TocPolicy::Refuse)
        .map_err(|e| anyhow::anyhow!("cannot resolve compact inputs in {}: {e}", dir.display()))?;

    let refusals: Vec<String> = listing
        .live
        .iter()
        .filter_map(|g| refusal(g).map(|why| format!("{} ({why})", g.base_name())))
        .collect();
    if !refusals.is_empty() {
        bail!(
            "compact handles only BIG `nb` generations with a sequence id; refusing {} in {}: {}",
            if refusals.len() == 1 {
                "this generation"
            } else {
                "these generations"
            },
            dir.display(),
            refusals.join("; ")
        );
    }

    let mut inputs = Vec::with_capacity(listing.live.len());
    for g in listing.live.iter().rev() {
        match g.component_path("Data.db") {
            Some(p) => inputs.push(p.to_path_buf()),
            None => bail!("{} has no Data.db", g.base_name()),
        }
    }
    Ok(inputs)
}

/// Why `compact` cannot read `g`, or `None` when it can.
fn refusal(g: &Generation) -> Option<String> {
    if g.has_condition(Condition::UnsupportedVersion) {
        return Some(format!(
            "version {} is not a supported Cassandra 5.0 version",
            g.version()
        ));
    }
    if g.format().as_str() != "big" {
        return Some(format!("unsupported format {}", g.format().as_str()));
    }
    if g.version() != "nb" {
        return Some(format!("unsupported version {}", g.version()));
    }
    if matches!(g.id(), GenerationId::Uuid(_)) {
        return Some("unsupported UUID generation id".to_string());
    }
    None
}
