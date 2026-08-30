//! FIXTURE DISCOVERY, fail-closed — issue #1490 (AD1) round 18.
//!
//! # The defect this module exists to remove
//!
//! Discovery used to begin with `datasets_root::sstables_root_for_table`, whose
//! answer is an `Option<PathBuf>`, built on a `table_has_data` that maps a failed
//! `read_dir` to `false` and drops per-entry errors with
//! `filter_map(|e| e.ok())`. So a THREE-state signal — the table is here, the
//! table is verifiably not here, I could not read the directory to tell — was
//! forced onto TWO values, and "cannot tell" collapsed onto the PERMISSIVE one:
//! `None`, which `resolve_fixture` reports as `Ok(None)` and the case then
//! SKIPS. An unreadable corpus root therefore read as an absent fixture, and the
//! complete-directory checks that would have caught it
//! (`read_dir_completely`, the generation census, the golden/generation
//! correspondence) never ran, because discovery had already concluded the table
//! was not there.
//!
//! That is CLAUDE.md's named anti-pattern: *a positive verdict requires an
//! AFFIRMATIVE measurement*, and never derive a pass (here: a legitimate skip)
//! from the ABSENCE of a bad signal. It is also the same shape as
//! [`read_dir_completely`]'s own reason for existing, one stage EARLIER in the
//! pipeline — which is why the fix lives beside it rather than beside the caller.
//!
//! # The rule
//!
//! [`first_candidate_root_with_table`] is FALLIBLE. `Ok(None)` is returned ONLY
//! after every candidate root was read SUCCESSFULLY and genuinely lacks the
//! table; any directory or entry error is an `Err` — a refusal naming the path
//! and the OS error. A candidate root that does not exist at all is
//! `NotFound`, which IS an affirmative "the table is not here" (a machine
//! legitimately has no `CQLITE_DATASETS_ROOT` corpus, and the checkout candidate
//! is a compile-time path that need not hold every keyspace), so that one error
//! kind — and only that one — is a verified absence.
//!
//! A root it could not read refuses EVEN IF a later candidate holds the table:
//! the harness cannot know the unreadable root did not hold a DIFFERENT
//! generation of the same fixture, and it refuses ambiguity it cannot measure
//! rather than compare against whichever root it happened to be able to read.
//! (A root that HOLDS the table still ends the search before any later candidate
//! is read, so this is about readability, not about position in the list.)
//!
//! # Why the harness does not change `sstables_root_for_table` itself
//!
//! That helper lives in `cqlite-core/tests/support/datasets_root.rs` and has
//! callers in eight other test targets across two crates (#3220). Widening its
//! signature is a cross-lane change, so this harness keeps its own fallible
//! search over the SHARED candidate list
//! (`datasets_root::sstables_root_candidates`) — the same roots, in the same
//! order, judged by EVIDENCE (which root actually holds the table) rather than by
//! a preference between them. The permissive `Option` form is no longer reachable
//! from this harness.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// Every entry of `dir`, with an entry the OS could not deliver propagated as a
/// REFUSAL rather than dropped.
///
/// `read_dir` yields one `io::Result<DirEntry>` PER ENTRY, so an individual entry
/// can fail on its own — and the `filter_map(|e| e.ok())` this replaces collapsed
/// that three-valued signal ("here", "not here", "cannot tell") onto the
/// PERMISSIVE answer, exactly the shape CLAUDE.md names for two-valued file
/// predicates. The consequence is specific, not theoretical: every caller takes a
/// CENSUS of the directory — how many `*-Data.db` generations, how many goldens,
/// how many table directories — and concludes the fixture is UNIQUE. A census
/// taken over an incomplete listing can only ever conclude "fewer", so an entry
/// that was silently dropped is precisely how a SECOND generation, or a golden
/// belonging to another one, passes as a unique fixture.
pub fn read_dir_completely(dir: &Path) -> Result<Vec<std::fs::DirEntry>, String> {
    let listing =
        std::fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    let mut entries = Vec::new();
    for (i, entry) in listing.enumerate() {
        entries.push(entry.map_err(|e| {
            format!(
                "cannot read entry {i} of {}: {e}; the harness REFUSES a fixture directory it \
                 could not inspect COMPLETELY, because an entry it cannot read is UNKNOWN, not \
                 ABSENT — dropped, it leaves the generation census one entry short",
                dir.display()
            )
        })?);
    }
    Ok(entries)
}

/// The first candidate root carrying `<keyspace>/<table>-*/…-Data.db`, or a
/// REFUSAL naming what could not be read.
///
/// THREE-valued by contract:
///
///   * `Ok(Some(root))` — that root holds an actual `*-Data.db` for the table;
///   * `Ok(None)` — EVERY candidate was read successfully (or is verifiably
///     absent) and none holds the table. Only this is a legitimate skip;
///   * `Err(why)` — a directory or entry the harness could not read. "I could not
///     tell" is never reported as "it is not there".
///
/// Presence is judged by an actual `*-Data.db` component, never by directory
/// existence: the repo commits JSONL sidecars for fixtures whose binaries are
/// gitignored, so `<table>-<uuid>/` can exist with no readable SSTable in it.
///
/// Parameterized on the candidate list so the rule is testable against SYNTHETIC
/// roots — the real list is half environment and half a COMPILE-TIME checkout
/// path, so a test reading it can only ever observe this machine's layout.
pub fn first_candidate_root_with_table(
    roots: &[PathBuf],
    keyspace: &str,
    table: &str,
) -> Result<Option<PathBuf>, String> {
    for root in roots {
        if root_holds_table(root, keyspace, table)? {
            return Ok(Some(root.clone()));
        }
    }
    Ok(None)
}

/// The candidate roots this harness searches: the shared, order-stable list
/// (`CQLITE_DATASETS_ROOT` corpus then the checkout's committed corpus, #3220).
pub fn candidate_roots() -> Vec<PathBuf> {
    super::datasets_root::sstables_root_candidates()
}

/// Does `root` hold `<keyspace>/<table>-*/…-Data.db`? `Err` when it cannot be
/// determined.
fn root_holds_table(root: &Path, keyspace: &str, table: &str) -> Result<bool, String> {
    let ks_dir = root.join(keyspace);
    // A root (or keyspace directory) that is not there is an AFFIRMATIVE absence
    // — a machine legitimately has no fetched corpus, and the checkout candidate
    // is a compile-time path that need not hold every keyspace. Every OTHER error
    // kind (a permission denial, a path component that is not a directory, an I/O
    // error) is UNKNOWN and refuses.
    match std::fs::read_dir(&ks_dir) {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
        Err(e) => {
            return Err(format!(
                "cannot read the candidate keyspace directory {} while searching for \
                 {keyspace}.{table}: {e}. The harness REFUSES a candidate root it could not \
                 inspect rather than report the fixture ABSENT: an unreadable root and a root \
                 that genuinely lacks the table are DIFFERENT states, and collapsing the first \
                 onto the second turns a broken corpus into a silent SKIP",
                ks_dir.display()
            ))
        }
    }
    let prefix = format!("{table}-");
    // Matched on the entry name's BYTES, so a `<table>-<non-UTF-8>` generation
    // directory cannot vanish from the search (`OsStr` is an ASCII-compatible
    // superset, so an ASCII prefix match on its bytes is exact) — the same rule
    // the generation census in `resolve_fixture` applies.
    for entry in read_dir_completely(&ks_dir)? {
        let path = entry.path();
        if !entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(prefix.as_bytes())
            || !path.is_dir()
        {
            continue;
        }
        let holds = read_dir_completely(&path)?
            .iter()
            .any(|e| e.file_name().as_encoded_bytes().ends_with(b"-Data.db"));
        if holds {
            return Ok(true);
        }
    }
    Ok(false)
}
