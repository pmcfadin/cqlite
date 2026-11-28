//! The input of one rebuild run, resolved through the generation module (issue #4626, GD1 to
//! GD3): which listed generation a `Data.db` path is, and which generation's `Statistics.db` the
//! run reads its recovered metadata from.

use super::RebuildOptions;
use crate::error::{Error, Result};
use crate::storage::sstable::generation::{
    list_generations, Generation, GenerationError, Scope, TocPolicy,
};
use std::path::Path;

/// A listing failure (a transaction log that is inconsistent, unreadable or misnamed) refuses the
/// run, naming the log.
pub(super) fn listing_error(error: GenerationError) -> Error {
    Error::InvalidInput(error.to_string())
}

/// The floor refusal for `generation`, from its descriptor alone (GD1): no component was opened.
pub(super) fn check_floor(generation: &Generation) -> Result<()> {
    match generation.below_floor() {
        Some(below) => Err(Error::UnsupportedVersion {
            version: below.version.clone(),
            floor: below.floor.to_string(),
        }),
        None => Ok(()),
    }
}

/// Resolve `data_db_path` to the generation the module lists for it (Scope Live, `TocMissing`
/// accepted: regenerating a missing `TOC.txt` is a headline rebuild case). A file the listing
/// does not return as live is refused by name: an Orphan, a file a transaction log hides, or a
/// name that is not an SSTable component at all.
pub(super) fn resolve_input(data_db_path: &Path) -> Result<Generation> {
    let dir = data_db_path.parent().ok_or_else(|| {
        Error::InvalidInput(format!(
            "input path has no parent directory: {}",
            data_db_path.display()
        ))
    })?;
    let file = data_db_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::InvalidPath(format!("Invalid SSTable path: {data_db_path:?}")))?;
    let listing = list_generations(dir, &Scope::Live, TocPolicy::Accept).map_err(listing_error)?;
    let is_this = |g: &Generation| format!("{}-Data.db", g.base_name()) == file;
    if let Some(found) = listing.live.iter().find(|g| is_this(g)) {
        check_floor(found)?;
        return Ok(found.clone());
    }
    if listing.orphans.iter().any(is_this) {
        return Err(Error::InvalidInput(format!(
            "{file} is an Orphan (Data.db missing or zero-length); rebuild reports an Orphan and \
             does not rebuild it"
        )));
    }
    if let Some(hidden) = listing
        .hidden
        .iter()
        .find(|h| h.path.file_name().and_then(|n| n.to_str()) == Some(file))
    {
        return Err(Error::InvalidInput(format!(
            "{file} is hidden by transaction log {} and is not part of the live set; rebuild \
             does not rebuild it",
            hidden.log.display()
        )));
    }
    Err(Error::InvalidInput(format!(
        "{file} is not a generation in {}: its name is not an SSTable component name",
        dir.display()
    )))
}

/// The generation whose `Statistics.db` this run recovers metadata from (R4.2): the caller's
/// "renamed aside" override when given, else the input itself. `None` when the override does not
/// name a `Statistics.db` the module lists, so it reads as unreadable and its fields are `lost`.
/// Resolved once, so the `EncodingStats` baseline and the repair fields never come from two
/// different files.
pub(super) fn statistics_source(
    generation: &Generation,
    options: &RebuildOptions,
) -> Option<Generation> {
    let Some(aside) = &options.statistics_recovery_source else {
        return Some(generation.clone());
    };
    let name = aside.file_name()?.to_str()?;
    // A file renamed aside has no `Data.db` beside it: the listing returns it as an Orphan, which
    // still carries its components and the trusted-metadata accessor.
    let listing = list_generations(aside.parent()?, &Scope::Live, TocPolicy::Accept).ok()?;
    listing.live.into_iter().chain(listing.orphans).find(|g| {
        g.component_path("Statistics.db")
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            == Some(name)
    })
}
