//! The read-only verbs' shared view of the generation module (issue #4626, requirements GD5 to
//! GD8): one listing call, one wording for the facts every verb reports the same way.
//!
//! `explain`, `find`, `diagnose` and `diff` list Scope Live and accept `TocMissing` (owner ruling
//! Q43). What each does with a generation it cannot use is its own policy and lives in the verb;
//! this file only holds what is common: the listing call, the stderr note for a directory-level
//! fact (Q44, Q47), the `(toc-missing)` suffix and the refusal text for a generation whose
//! trusted metadata is not available.

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::generation::{
    list_generations, Condition, DirectoryNote, Generation, GenerationError, Listing, Scope,
    TocPolicy,
};

/// List `dir` with Scope Live, accepting a generation whose `TOC.txt` is missing.
pub(crate) fn list_live(dir: &Path) -> Result<Listing, GenerationError> {
    list_generations(dir, &Scope::Live, TocPolicy::Accept)
}

/// The generation's name as the first line of `explain` and `diff` shows it: `nb-1-big`, with the
/// suffix `(toc-missing)` when `TOC.txt` is absent (GD5).
pub(crate) fn display_name(generation: &Generation) -> String {
    if generation.has_condition(Condition::TocMissing) {
        format!("{} (toc-missing)", generation.base_name())
    } else {
        generation.base_name()
    }
}

/// Where `generation`'s `Data.db` is (or would be, for one whose `Data.db` is gone).
pub(crate) fn data_path(generation: &Generation) -> PathBuf {
    generation
        .component_path("Data.db")
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            generation
                .directory()
                .join(format!("{}-Data.db", generation.base_name()))
        })
}

/// Why `generation`'s `Statistics.db` cannot be trusted, naming the path; `None` when it can.
/// Below-floor, `Corrupt`, `Unreadable` and `Absent` all land here (R8, GD5, GD6). The metadata is
/// cached, so asking again costs nothing.
pub(crate) fn metadata_refusal(generation: &Generation) -> Option<String> {
    match generation.trusted_metadata() {
        Ok(_) => None,
        Err(failure) => Some(format!(
            "{}: {}: {failure}",
            data_path(generation).display(),
            failure.condition()
        )),
    }
}

/// Report what the listing says about files that are not in the generation list: orphans and the
/// directory notes. Nothing here changes stdout.
pub(crate) fn print_listing_facts(verb: &str, listing: &Listing) {
    for orphan in &listing.orphans {
        eprintln!(
            "cqlite {verb}: note: {} is an Orphan (Data.db missing or zero-length) and is excluded \
             from the live set",
            data_path(orphan).display()
        );
    }
    print_notes(verb, &listing.notes);
}

/// Print directory-level notes (Q44 corrupt log records, Q47 relocated log) to stderr.
pub(crate) fn print_notes(verb: &str, notes: &[DirectoryNote]) {
    for note in notes {
        match note {
            DirectoryNote::CorruptTxnLog { path, reasons } => eprintln!(
                "cqlite {verb}: note: transaction log {} has records that failed verification ({}); \
                 listed from the readable records",
                path.display(),
                reasons.join("; ")
            ),
            DirectoryNote::RelocatedTxnLog {
                path,
                original_prefix,
            } => eprintln!(
                "cqlite {verb}: note: transaction log {} names {} and was applied to this \
                 directory (relocated or copied table directory)",
                path.display(),
                original_prefix.display()
            ),
        }
    }
}
