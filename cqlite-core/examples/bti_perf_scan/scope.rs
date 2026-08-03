//! What this harness is allowed to ingest — resolved BEFORE the scan, fail-closed
//! (roborev #3234 M1/F1).
//!
//! The harness used to point `IngestionConfig::data_dir` at `<corpus>/sstables` with a
//! `table_directory_filter` of `/<keyspace>/<table>`, i.e. a SUBSTRING match over
//! everything discovery found. Two ways that silently changed the measured workload:
//!
//! - `gen-perf-corpus-bti.sh --no-prune` leaves several `<table>-<uuid>` directories in
//!   the discoverable tree. Discovery returns them ALL, so the Database opened MORE
//!   generations than the manifest describes — and the **generation count selects the
//!   scan route** (`readers.len() > 1 && schema.is_some()` picks
//!   `generation_merge::stream_generations_for_read`) and is what any throughput figure
//!   is attributed to. Meanwhile the row-count assert still passed, because the extra
//!   directory is another copy of the same rows only when it is, and nothing compared
//!   the directory set to the manifest at all.
//! - the filter is a substring, so `--table wide_multiclustering` also matched
//!   `wide_multiclustering_small-<uuid>` sitting in the same keyspace.
//!
//! So the scope is now PINNED and NAMED:
//!
//! 1. when the manifest documents `tables[].sstable_dir` (every generator-written
//!    manifest does), ingestion is scoped to **exactly that directory** — the harness's
//!    input is exactly what the authority describes, and a retained generation beside it
//!    is not ingested;
//! 2. otherwise (a hand-written minimal manifest, `--expect-rows`, `--no-expect-rows`)
//!    the corpus root must be UNAMBIGUOUS: exactly one `<table>-<uuid>` directory (the
//!    shape is `is_table_dir`, below; the cardinality is `matching_dirs(..).len() == 1`
//!    in `resolve`, whose other arms refuse), or the run refuses rather than measuring
//!    an unknown union.
//!
//! In both cases the resolved directory, its generation count and the provenance of
//! that decision are printed in the result block, so the workload a number describes
//! travels with the number.
//!
//! **"Scoped" means EXACT, and a filter is not exact (roborev #3234 F1).** Round 11
//! implemented (1) by handing `ingest` a `table_directory_filter` of
//! `/<keyspace>/<resolved dir>` — still a SUBSTRING match, so a sibling whose full name
//! EXTENDS the resolved one (`<table>-<uuid>-backup`) was ALSO ingested while
//! `generations` was counted in the selected directory alone: extra SSTables scanned,
//! the smaller count reported, the route attribution wrong again. Ingestion therefore
//! now goes through `ingest_with_selection(.., TableDirSelection::Exact(&[dir]))`, which
//! compares complete path components, and `main.rs` reports the generation count
//! OBSERVED in what was actually selected rather than in what was intended.

use super::manifest::ManifestScope;
use std::path::{Path, PathBuf};

pub struct IngestScope {
    /// `IngestionConfig::data_dir` — the corpus's `sstables` tree (unchanged, so
    /// discovery behaves exactly as before).
    pub data_dir: PathBuf,
    /// The one corpus directory that will be measured. Handed to
    /// `TableDirSelection::Exact`; there is deliberately no filter STRING here, because
    /// a substring filter cannot express this (roborev #3234 F1).
    pub dir: PathBuf,
    /// `*-Data.db` files in `dir`: the generation count this scope EXPECTS. What is
    /// reported is the count observed in the directories ingestion actually selected
    /// (`main.rs`); a disagreement is a refusal, never a silently smaller number.
    pub generations: usize,
    /// How this scope was decided, for the result block.
    pub provenance: String,
}

/// The number of hex digits in a Cassandra table-directory id: a UUID with its dashes
/// removed. Cassandra's own `Directories`/`Descriptor` layout writes `<table>-<32 hex>`,
/// and `discovery::scanner::has_cassandra_table_uuid_suffix` reads exactly that.
pub const TABLE_ID_HEX_LEN: usize = 32;

/// `<table>-<32 hex>` — the Cassandra 5.0 table-directory shape, EXACTLY (roborev
/// #3234 F1/F2).
///
/// Every part of this is a deliberate exactness choice, because each looser form is a
/// directory a measurement could be silently redirected into:
///
/// - not a prefix match on the table name: `wide_multiclustering_small-<uuid>` must not
///   be picked up by `--table wide_multiclustering`;
/// - not "any hex-ish suffix": `<table>-backup` and `<table>-<31 hex>` are not table
///   directories, and accepting them is how a backup copy becomes the measured corpus;
/// - no bare `<table>` form: Cassandra 5.0 always writes the id, so accepting a bare
///   name would accept something Cassandra never wrote.
///
/// This is the ONE definition of the shape — `manifest.rs` validates
/// `tables[].sstable_dir` with it too, so the two cannot drift into disagreeing about
/// what a table directory is.
pub fn is_table_dir(name: &str, table: &str) -> bool {
    match name.strip_prefix(table).and_then(|r| r.strip_prefix('-')) {
        Some(id) => id.len() == TABLE_ID_HEX_LEN && id.chars().all(|c| c.is_ascii_hexdigit()),
        None => false,
    }
}

/// The type of one directory entry, OBSERVED with `lstat` and fail-closed (roborev
/// #3234 round-12 F1).
///
/// Two deliberate choices, both about the same thing — a classification that CANNOT be
/// silently wrong:
///
/// - it is an `lstat`, not `DirEntry::file_type()`. On Linux the latter is served from
///   the `d_type` byte cached in the directory entry, so it answers even for an entry
///   the process cannot stat at all: the type would come from the directory's claim
///   rather than from an observation, and the one failure mode that matters here (an
///   entry we cannot look at) would be invisible. `symlink_metadata` is the observation,
///   and it can FAIL — which is the point.
/// - it does NOT follow symlinks, so a symlink is reported as a symlink rather than as
///   whatever it points at. Both callers below require the real thing.
///
/// An error here is returned, never swallowed: the counts these callers produce are the
/// generation count the AC3 figure is attributed to, so an entry that cannot be
/// classified must refuse rather than quietly change a number.
fn entry_file_type(path: &Path, dir: &Path) -> std::result::Result<std::fs::FileType, String> {
    std::fs::symlink_metadata(path)
        .map(|m| m.file_type())
        .map_err(|e| {
            format!(
                "cannot determine what {} is (lstat failed: {e}), while reading {}. An entry \
                 that cannot be classified is not an entry that can be counted, and skipping \
                 it would report a generation count or a corpus-directory set this run cannot \
                 stand behind. Refusing.",
                path.display(),
                dir.display()
            )
        })
}

fn matching_dirs(ks_dir: &Path, table: &str) -> std::result::Result<Vec<PathBuf>, String> {
    let mut out: Vec<PathBuf> = Vec::new();
    let entries = std::fs::read_dir(ks_dir).map_err(|e| {
        format!(
            "cannot read the keyspace directory {}: {e}",
            ks_dir.display()
        )
    })?;
    // Every entry error is PROPAGATED (roborev #3234 round-12 F1). `entries.flatten()`
    // dropped them, so an unreadable entry silently shrank the candidate set — and the
    // cardinality of that set is exactly what makes an ambiguous root a refusal rather
    // than a measurement over an unknown union.
    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "cannot read an entry of the keyspace directory {}: {e}. Skipping it would \
                 hide a `{table}-<uuid>` directory from the ambiguity check. Refusing.",
                ks_dir.display()
            )
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !is_table_dir(&name, table) {
            continue;
        }
        // A REAL directory: a symlink named `<table>-<32 hex>` is not a Cassandra table
        // directory, and resolving one would measure bytes from outside the corpus.
        if entry_file_type(&entry.path(), ks_dir)?.is_dir() {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// `*-Data.db` REGULAR FILES in one directory — one per generation.
///
/// The NAME predicate is a suffix match, and deliberately so: it is the exact rule
/// `discovery::scanner` counts SSTables with (`ends_with("-Data.db") || == "Data.db"`),
/// and this count is compared against what discovery selected. A stricter NAME rule here
/// would disagree with the thing it is checking.
///
/// The TYPE, though, is required to be a regular file (roborev #3234 round-12 F1). The
/// name alone counted a DIRECTORY or a SYMLINK named `*-Data.db` as a generation, and
/// this count is not decoration: `main.rs` prints it as `generations:` and derives
/// `storage_route:` from it (`generations > 1 && schema_resolved` selects
/// `generation_merge::stream_generations_for_read`). Measured on the committed 468-row
/// BTI fixture, one directory plus one symlink named `da-<n>-bti-Data.db` beside the one
/// real generation made this harness report `generations: 3` and attribute its own figure
/// to the multi-generation merge route — i.e. the instrument misreporting its own
/// measurement, with `rows_scanned` unchanged at 468 and the exit code 0. So a
/// non-regular entry is NOT a generation, and an entry whose type cannot be OBSERVED at
/// all is an error rather than one silently left out of the count.
pub fn count_generations(dir: &Path) -> std::result::Result<usize, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("cannot read the corpus directory {}: {e}", dir.display()))?;
    let mut count = 0usize;
    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "cannot read an entry of the corpus directory {}: {e}. Skipping it would \
                 report a generation count this run cannot stand behind (the count selects \
                 the scan route). Refusing.",
                dir.display()
            )
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !(name.ends_with("-Data.db") || name == "Data.db") {
            continue;
        }
        if entry_file_type(&entry.path(), dir)?.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

/// Resolve the one directory this run may ingest. `Err(message)` => exit `OPEN_FAILED`
/// (nothing was measured).
pub fn resolve(
    corpus: &Path,
    keyspace: &str,
    table: &str,
    documented: Option<&ManifestScope>,
) -> std::result::Result<IngestScope, String> {
    let data_dir = corpus.join("sstables");
    let ks_dir = data_dir.join(keyspace);

    let (dir, provenance) = match documented {
        Some(m) => {
            let dir = corpus.join(&m.sstable_dir_rel);
            if !dir.is_dir() {
                return Err(format!(
                    "{} documents `tables[].sstable_dir` = {}, but {} is not a directory. The \
                     manifest describes a corpus that is not here, so its row count cannot \
                     verify a scan of whatever is.\n  remedy: regenerate the corpus, or pass \
                     --manifest PATH for the corpus you are actually scanning.",
                    m.manifest.display(),
                    m.sstable_dir_rel,
                    dir.display()
                ));
            }
            let provenance = format!(
                "{} tables[].sstable_dir = {} (EXACT: anything else under {} is not ingested)",
                m.manifest.display(),
                m.sstable_dir_rel,
                ks_dir.display()
            );
            (dir, provenance)
        }
        None => {
            let mut found = matching_dirs(&ks_dir, table)?;
            match found.len() {
                0 => {
                    return Err(format!(
                        "no `{table}-<uuid>` directory under {} — nothing to measure.",
                        ks_dir.display()
                    ))
                }
                1 => {
                    let dir = found.remove(0);
                    let provenance = format!(
                        "the sole `{table}-<uuid>` directory under {} (this manifest documents \
                         no sstable_dir)",
                        ks_dir.display()
                    );
                    (dir, provenance)
                }
                n => {
                    return Err(format!(
                        "AMBIGUOUS corpus root: {n} `{table}-<uuid>` directories under {}:\n{}\n  \
                         Discovery would ingest ALL of them, which changes the GENERATION COUNT \
                         — and the generation count selects the scan route, so the measurement \
                         would describe a workload no manifest documents. Refusing.\n  remedy: \
                         leave exactly one (a normal generator run prunes the others; this state \
                         comes from --no-prune), or pass a --manifest whose \
                         `tables[].sstable_dir` names the one to measure.",
                        ks_dir.display(),
                        found
                            .iter()
                            .map(|p| format!("    {}", p.display()))
                            .collect::<Vec<_>>()
                            .join("\n")
                    ))
                }
            }
        }
    };

    if dir.file_name().is_none() {
        return Err(format!("{} has no final path component", dir.display()));
    }
    let generations = count_generations(&dir)?;
    Ok(IngestScope {
        data_dir,
        dir,
        generations,
        provenance,
    })
}
