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

use super::ParityCase;

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

/// Is `path` a directory? `Err` when the OS could not tell.
///
/// `Path::is_dir` is TWO-valued: it answers `false` both for "this is not a
/// directory" and for "I could not stat it", which is the collapse this whole
/// module exists to remove. Every caller uses the answer to decide whether an
/// entry COUNTS toward a census (how many generation directories, how many
/// components), and a census can only ever conclude "fewer" — so an entry
/// dropped because it could not be stat'd is exactly how a SECOND generation
/// passes as a unique fixture.
///
/// `NotFound` is the one error kind that IS an affirmative answer: the entry is
/// gone (a race with a regeneration, or a dangling symlink), so it is genuinely
/// not a directory. Symlinks are FOLLOWED, matching the `is_dir()` this replaces.
pub fn path_is_dir(path: &Path) -> Result<bool, String> {
    path_kind(path, PathKind::Dir)
}

/// Is `path` a regular file? `Err` when the OS could not tell — see
/// [`path_is_dir`] for why the two-valued form is unsafe here.
pub fn path_is_file(path: &Path) -> Result<bool, String> {
    path_kind(path, PathKind::File)
}

enum PathKind {
    Dir,
    File,
}

fn path_kind(path: &Path, want: PathKind) -> Result<bool, String> {
    match std::fs::metadata(path) {
        Ok(md) => Ok(match want {
            PathKind::Dir => md.is_dir(),
            PathKind::File => md.is_file(),
        }),
        // The entry is not there at all — an affirmative "it is not a
        // directory/file", and the only error kind that may answer.
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!(
            "cannot determine what {} is: {e}; the harness REFUSES an entry whose kind it could \
             not measure rather than DROP it, because a dropped entry leaves every census over \
             this directory one entry short — and the census is the whole basis for calling a \
             fixture unique",
            path.display()
        )),
    }
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
        {
            continue;
        }
        // FALLIBLE: an entry whose kind the OS could not report is UNKNOWN, and
        // the `is_dir()` this replaces would have skipped it — reporting the
        // table absent from a root that may well hold it.
        if !path_is_dir(&path)? {
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

// ===========================================================================
// FIXTURE RESOLUTION and ISOLATED STAGING
//
// Split out of `mod.rs` (issue #1490 round 19) so the whole fail-closed
// discovery responsibility — candidate search, generation census,
// golden/generation correspondence, and the copy into an isolated data root —
// lives in ONE file beside the primitives it is built on. Every predicate here
// is fallible for the reason enumerated in `super::permissive_sites`: an entry
// this code cannot read is UNKNOWN, and a census over an incomplete listing can
// only ever conclude "fewer".
// ===========================================================================

/// The resolved on-disk fixture: the single-generation table directory, and the
/// golden that BELONGS to that generation (see [`fixture_in_table_dir`]).
#[derive(Debug)]
pub struct Fixture {
    pub table_dir: PathBuf,
    pub golden: PathBuf,
}

/// Resolve `<root>/<keyspace>/<table>-*/` per TABLE across every candidate root.
///
/// Returns `Ok(None)` only when EVERY candidate root was read successfully and
/// none carries the table; a root that could not be READ is a REFUSAL
/// ([`first_candidate_root_with_table`], round 18 — an unreadable
/// root used to read as an absent fixture and SKIP), and a root that carries the
/// table in an unusable shape (several generations, no golden, a golden belonging
/// to a DIFFERENT generation — see [`fixture_in_table_dir`]) is an ERROR too,
/// never a skip.
pub fn resolve_fixture(case: &ParityCase) -> Result<Option<Fixture>, String> {
    resolve_fixture_in_roots(case, &candidate_roots())
}

/// [`resolve_fixture`] parameterized on the candidate roots — the seam the
/// round-18 refusal is proven against, since the real list is half environment
/// and half a COMPILE-TIME checkout path and a test reading it could only ever
/// observe this machine's layout.
pub fn resolve_fixture_in_roots(
    case: &ParityCase,
    roots: &[PathBuf],
) -> Result<Option<Fixture>, String> {
    let Some(root) = first_candidate_root_with_table(roots, case.keyspace, case.table)? else {
        return Ok(None);
    };
    let ks_dir = root.join(case.keyspace);
    let prefix = format!("{}-", case.table);
    // The prefix is matched on the entry name's BYTES, not on a `to_str()` that
    // drops a non-UTF-8 name: a `<table>-<non-UTF-8>` generation directory must
    // COUNT toward the uniqueness census below (and be refused as a second
    // generation), never vanish from it. `OsStr` is an ASCII-compatible
    // superset, so an ASCII prefix match on its bytes is exact.
    // The `.filter(|p| p.is_dir())` this replaces was two-valued: an entry the
    // OS could not stat answered `false` and left the census — and this census
    // is precisely what the `dirs.len() != 1` check below calls "exactly one
    // generation". A dropped entry can only make the count SMALLER, so it is
    // how a second generation passes as a unique fixture. `path_is_dir` is
    // fallible: only a verified `NotFound` may answer "not a directory".
    let mut dirs: Vec<PathBuf> = Vec::new();
    for entry in read_dir_completely(&ks_dir)? {
        if !entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(prefix.as_bytes())
        {
            continue;
        }
        let path = entry.path();
        if path_is_dir(&path)? {
            dirs.push(path);
        }
    }
    dirs.sort();
    if dirs.len() != 1 {
        return Err(format!(
            "{}: expected exactly one table directory under {}, found {:?}",
            case.id(),
            ks_dir.display(),
            dirs
        ));
    }
    let table_dir = dirs.remove(0);

    Ok(Some(fixture_in_table_dir(&case.id(), table_dir)?))
}

/// Select the one Data generation in `table_dir` AND the golden that belongs to
/// it — the golden is DERIVED from the selected Data file's name, never chosen
/// independently.
///
/// # Why the correspondence has to be checked (issue #1490 round 5)
///
/// `sstabledump` names its dump after the file it dumped, so the golden for
/// `nb-1-big-Data.db` is `nb-1-big-Data.db.jsonl` — and nothing else. Accepting
/// "one `*-Data.db`" and "one `*-Data.db.jsonl`" INDEPENDENTLY means a partially
/// regenerated fixture (a new `nb-2-big-Data.db` beside a stale
/// `nb-1-big-Data.db.jsonl`) compares one generation's DATA against another
/// generation's DUMP. That is not a near-miss: depending on which way the two
/// generations differ it produces either a FALSE FAILURE or a FALSE PASS, and
/// the harness would report neither as suspicious — the oracle would simply be
/// the wrong oracle. So a non-corresponding pair is a NAMED refusal, never a
/// fallback to "any `.jsonl` in the directory".
///
/// Public so the refusal can be proven against a scratch directory holding a
/// deliberately mismatched pair, without touching the committed corpus.
pub fn fixture_in_table_dir(case_id: &str, table_dir: PathBuf) -> Result<Fixture, String> {
    let mut entries: Vec<String> = Vec::new();
    for entry in read_dir_completely(&table_dir)? {
        let name = entry.file_name();
        // A name this harness cannot read is UNKNOWN, not ABSENT. Dropped, it
        // would leave the generation census one entry short — and the census is
        // the whole basis for calling this fixture unique.
        let name = name.to_str().ok_or_else(|| {
            format!(
                "{case_id}: {} holds an entry whose name is not UTF-8 ({name:?}); the harness \
                 REFUSES a fixture directory it could not inspect COMPLETELY rather than DROP \
                 the entry from the generation census, which would let a second `*-Data.db` (or \
                 a mismatched golden) pass as a unique fixture",
                table_dir.display()
            )
        })?;
        entries.push(name.to_string());
    }
    let datas: Vec<&String> = entries.iter().filter(|n| n.ends_with("-Data.db")).collect();
    let goldens: Vec<&String> = entries
        .iter()
        .filter(|n| n.ends_with("-Data.db.jsonl"))
        .collect();
    // A multi-generation table's per-generation dumps are not the reconciled
    // result set the export produces, so the harness refuses rather than
    // comparing one generation against a merged read.
    if datas.len() != 1 {
        return Err(format!(
            "{case_id}: expected exactly one *-Data.db generation in {}, found {}: the harness \
             compares a single-generation dump against a reconciled export",
            table_dir.display(),
            datas.len()
        ));
    }
    if goldens.len() != 1 {
        return Err(format!(
            "{case_id}: expected exactly one *-Data.db.jsonl golden in {}, found {}",
            table_dir.display(),
            goldens.len()
        ));
    }
    // The BINDING: the golden's name is derived from the Data file the harness
    // is actually going to export, and must match exactly.
    let data = datas[0];
    let expected = format!("{data}.jsonl");
    if *goldens[0] != expected {
        return Err(format!(
            "{case_id}: {} holds the Data generation '{data}' but its only sstabledump golden \
             is '{}', which belongs to a DIFFERENT generation (the golden for '{data}' is \
             '{expected}'). Comparing one generation's data against another generation's dump \
             can produce either a false failure or a false pass, so the harness refuses: \
             regenerate the dump for this generation, or restore the matching pair.",
            table_dir.display(),
            goldens[0],
        ));
    }
    Ok(Fixture {
        golden: table_dir.join(expected),
        table_dir,
    })
}

/// Copy the fixture's SSTable components into an isolated `<keyspace>/<table-…>`
/// data directory.
///
/// Isolation is not cosmetic: pointed at a shared corpus root the CLI ingests
/// EVERY table it finds, so one case's export would depend on unrelated
/// fixtures (and on this machine's corpus size). Only real SSTable components
/// are copied — the `.jsonl` / `.txt` sidecars stay out so the reader never sees
/// them.
pub(super) fn isolated_data_dir(
    case: &ParityCase,
    fixture: &Fixture,
    tmp: &Path,
) -> Result<PathBuf, String> {
    let data_dir = tmp.join("data");
    let dest = data_dir.join(case.keyspace).join(
        fixture
            .table_dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("fixture directory has no name")?,
    );
    std::fs::create_dir_all(&dest).map_err(|e| format!("mkdir {}: {e}", dest.display()))?;
    // Fail-closed on all three counts, because every one of them decides whether
    // an SSTable COMPONENT reaches the reader — and a component the reader never
    // sees produces a smaller (or aborted) export, not an error:
    //
    //   * entries come through `read_dir_completely`, so a per-entry error is a
    //     refusal rather than a silently short listing;
    //   * a non-UTF-8 component name is REFUSED, not `to_string_lossy`'d — the
    //     lossy name was the COPY DESTINATION, so a component with a non-UTF-8
    //     name was written under a DIFFERENT name and the reader could no longer
    //     find it (`fixture_in_table_dir` already refuses such an entry, so this
    //     is the same rule at the same directory);
    //   * `path_is_file` is fallible, where the `is_file()` it replaces answered
    //     `false` for "could not stat" and silently OMITTED the component.
    for entry in read_dir_completely(&fixture.table_dir)? {
        let raw = entry.file_name();
        let name = raw.to_str().ok_or_else(|| {
            format!(
                "{} holds a component whose name is not UTF-8 ({raw:?}); the harness REFUSES it \
                 rather than copy it under a lossily-renamed path, which would hide the \
                 component from the reader and shrink the export with nothing red",
                fixture.table_dir.display()
            )
        })?;
        if name.ends_with(".jsonl") || name.ends_with(".txt") {
            continue;
        }
        if !path_is_file(&entry.path())? {
            continue;
        }
        std::fs::copy(entry.path(), dest.join(name)).map_err(|e| format!("copy {name}: {e}"))?;
    }
    Ok(data_dir)
}
