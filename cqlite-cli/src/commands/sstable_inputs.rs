//! Recursive SSTable input discovery for `compact` (`write.rs`), split out of `write.rs` for the
//! #1116/#1135 file-size ratchet. Behavior is unchanged from the original in-file helpers.
//! `explain` deliberately does NOT use this (it needs live-generation-only discovery; see
//! `explain_resolve.rs`).

use anyhow::{Context, Result};
use std::path::Path;

/// Recursively discover published input SSTables under `dir`, ordered
/// newest-to-oldest by generation (the order `compact_sstables` expects).
#[cfg(feature = "write-support")]
pub(crate) fn discover_input_sstables(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut found: Vec<(u64, std::path::PathBuf)> = Vec::new();
    collect_data_files(dir, &mut found, 8)?;
    // Highest generation first: run_index 0 must be the newest run.
    found.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(found.into_iter().map(|(_, p)| p).collect())
}

/// Recursively collect `nb-<gen>-big-Data.db` files that have a sibling
/// `TOC.txt` (the publication barrier), pairing each with its generation.
#[cfg(feature = "write-support")]
fn collect_data_files(
    dir: &Path,
    out: &mut Vec<(u64, std::path::PathBuf)>,
    depth: usize,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("Failed to read input directory: {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            // Honor the TOC.txt publication barrier: a Data.db without a sibling
            // TOC.txt is an unpublished/partial SSTable and must not be compacted.
            let base = name.trim_end_matches("-Data.db");
            let toc = path.with_file_name(format!("{base}-TOC.txt"));
            if !toc.exists() {
                continue;
            }
            // Parse generation from nb-<gen>-big-Data.db
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect_data_files(&path, out, depth - 1)?;
        }
    }
    Ok(())
}
