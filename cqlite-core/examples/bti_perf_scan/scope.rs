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
//!
//! Two further properties, both earned in roborev job 27:
//!
//! - **The manifest documents WHICH generations, and that is compared before scanning**
//!   (B2, `verify_documented_generations`). Pinning the DIRECTORY is not pinning the
//!   WORKLOAD: another generation dropped into the documented directory carrying the same
//!   logical rows leaves the reconciled row count identical while changing the merge
//!   workload and the storage route the AC3 figure is attributed to. So
//!   `tables[].sstable_count` and `tables[].sstable_generations` are compared against the
//!   observed `*-Data.db` descriptors — count AND exact identifier set — and a mismatch is
//!   a refusal naming both.
//! - **Both branches require REAL directories inside the corpus root** (B3,
//!   `real_dir_beneath`). The documented branch accepted `dir.is_dir()`, which FOLLOWS
//!   symlinks, while the fallback branch lstat'ed its table directory: inconsistent
//!   hardening, in favour of the branch a manifest controls. A correctly shaped
//!   `sstable_dir` could therefore redirect ingestion outside the corpus entirely.

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
        // A REAL directory: `is_table_dir` above enforced the `<table>-<32 hex>` NAME shape,
        // but a symlink so named is not a Cassandra table directory, and resolving one would
        // measure bytes from outside the corpus.
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
    Ok(data_db_entries(dir)?.len())
}

/// The `*-Data.db` regular-file entry NAMES in one directory — the descriptors the
/// generation count and the generation-identifier set are both derived from, so the two
/// can never be read from different observations of the directory.
fn data_db_entries(dir: &Path) -> std::result::Result<Vec<String>, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("cannot read the corpus directory {}: {e}", dir.display()))?;
    let mut out: Vec<String> = Vec::new();
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
            out.push(name);
        }
    }
    out.sort();
    // The suffix predicate above matches `discovery::scanner` and therefore says nothing
    // about the FORMAT; `assert_bti_descriptors` is what refuses a non-BTI one.
    assert_bti_descriptors(dir, &out)?;
    Ok(out)
}

/// Every `*-Data.db` descriptor is a BTI one — `da-<generation>-bti-Data.db`.
///
/// `bti_perf_scan` is a BTI-ONLY instrument: every figure it prints is published as a `da`
/// BTI measurement, and `main.rs` attributes it to a BTI `storage_route:`. Nothing above
/// this point observes the format, though — the name predicate in `data_db_entries` is a
/// deliberate suffix match (it has to agree with the `discovery::scanner` rule it is
/// cross-checked against), and `generation_of` reads only the generation component. So a
/// directory of `nb-<n>-big-Data.db` (BIG) SSTables carrying the same generation
/// identifiers and the same row count scanned clean and was reported as a BTI measurement
/// (roborev job 28) — the instrument misreporting WHICH FORMAT its own published number
/// describes, with `rows_scanned` matching the manifest and the exit code 0.
///
/// Not hypothetical, and not only an adversarial case: a BTI-configured Cassandra 5.0 node
/// still writes its OWN system tables in BIG, so the generated corpus tree holds 11
/// `nb-*-big-Data.db` files beside the 27 `da-*-bti-*` generations under test — a corpus
/// root pointed one level too high reaches them.
///
/// A REFUSAL rather than a filter, deliberately: silently dropping a non-`da` descriptor
/// would make this count disagree with the discovery count it is compared against, and
/// would measure a subset while reporting the whole. The version/format test below is the
/// enforcement, and it does not judge the generation component — `da-x-bti-Data.db` stays
/// the unreadable-generation refusal that `observed_generation_ids` already raises.
fn assert_bti_descriptors(dir: &Path, names: &[String]) -> std::result::Result<(), String> {
    let foreign: Vec<&str> = names
        .iter()
        .map(String::as_str)
        .filter(|name| {
            let parts: Vec<&str> = name.split('-').collect();
            !(parts.len() == 4 && parts[0] == "da" && parts[2] == "bti")
        })
        .collect();
    if !foreign.is_empty() {
        return Err(format!(
            "{}: {} `*-Data.db` file(s) are not BTI descriptors \
             (`da-<generation>-bti-Data.db`): {}. This harness measures the BTI (`da`) read \
             path and every figure it prints is published as a BTI measurement, so scanning \
             a non-`da`/non-`bti` SSTable would attribute a number to a format it does not \
             describe — and neither the row count nor the generation set can see the \
             difference. Refusing.\n  note: a BTI-configured Cassandra 5.0 node still writes \
             its own system tables in BIG, so a corpus root pointed one level too high \
             reaches `nb-*-big-Data.db` files.\n  remedy: point --corpus at the BTI corpus \
             this manifest describes (its table directories hold only \
             `da-<generation>-bti-*` components).",
            dir.display(),
            foreign.len(),
            foreign.join(", ")
        ));
    }
    Ok(())
}

/// The generation identifier in a Cassandra 5.0 descriptor
/// `<version>-<generation>-<format>-Data.db` (e.g. `da-7-bti-Data.db` => 7).
///
/// Exactly four `-`-separated components, and the generation is all ASCII digits: this is
/// the same rule `test-data/scripts/write-perf-corpus-bti-manifest.py` derives
/// `sstable_generations` with, so the manifest and this comparison cannot disagree about
/// what a generation identifier is. `None` (an unrecognised descriptor) is an error at the
/// call site, never a generation quietly left out of the set.
fn generation_of(name: &str) -> Option<u64> {
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() != 4 || parts[3] != "Data.db" {
        return None;
    }
    if parts[1].is_empty() || !parts[1].chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    parts[1].parse::<u64>().ok()
}

/// The generation identifiers OBSERVED in `names`, sorted. An unrecognised descriptor is a
/// refusal: the manifest states WHICH generations it documents, so a generation whose
/// identifier cannot be read cannot be compared against that statement.
fn observed_generation_ids(dir: &Path, names: &[String]) -> std::result::Result<Vec<u64>, String> {
    let mut ids: Vec<u64> = Vec::with_capacity(names.len());
    let mut unparsed: Vec<&str> = Vec::new();
    for name in names {
        match generation_of(name) {
            Some(g) => ids.push(g),
            None => unparsed.push(name.as_str()),
        }
    }
    if !unparsed.is_empty() {
        return Err(format!(
            "{}: {} `*-Data.db` file(s) do not carry a readable generation identifier \
             (`<version>-<generation>-<format>-Data.db`): {}. The manifest documents WHICH \
             generations this directory holds, so a descriptor whose generation cannot be read \
             cannot be checked against it — and the generation count selects the storage route \
             the measurement is attributed to. Refusing.",
            dir.display(),
            unparsed.len(),
            unparsed.join(", ")
        ));
    }
    ids.sort_unstable();
    Ok(ids)
}

fn id_list(ids: &[u64]) -> String {
    ids.iter()
        .map(|g| g.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The documented generation set vs the observed one, BEFORE anything is scanned (roborev
/// job 27 B2).
///
/// This is the one workload change the row-count assert is BLIND to, and it is the sharpest
/// case of the whole class: adding a generation that carries the SAME logical rows leaves
/// reconciliation yielding the identical count, so `rows_scanned` matches the manifest and
/// the harness exits 0 — while the generation count has changed the measured merge workload
/// AND the storage route (`generations > 1 && schema` selects
/// `generation_merge::stream_generations_for_read`), which is exactly what the published AC3
/// figure is attributed to. Both the COUNT and the exact identifier SET are compared, so
/// swapping one generation for another (count preserved) is caught too.
fn verify_documented_generations(
    dir: &Path,
    scope: &ManifestScope,
    names: &[String],
) -> std::result::Result<(), String> {
    let observed = observed_generation_ids(dir, names)?;
    let expected = &scope.expected;
    let why = "The generation count selects the storage route \
               (`generations > 1 && schema` => `generation_merge::stream_generations_for_read`) \
               and the merge workload the throughput figure describes, and a generation holding \
               the SAME logical rows leaves the row count unchanged — so the row-count assert \
               cannot see this. Refusing.\n  remedy: measure the corpus this manifest describes, \
               or pass --manifest PATH for the corpus you are actually scanning.";
    if observed.len() != expected.count {
        return Err(format!(
            "{} documents `tables[].sstable_count` = {} generation(s) in {}, but {} \
             `*-Data.db` generation(s) are present there.\n  documented generations: [{}]\n  \
             observed generations:   [{}]\n  {why}",
            scope.manifest.display(),
            expected.count,
            dir.display(),
            observed.len(),
            id_list(&expected.ids),
            id_list(&observed)
        ));
    }
    if observed != expected.ids {
        return Err(format!(
            "{} documents generations [{}] in {}, but the generations present there are [{}] — \
             the same COUNT, a different SET, so this is a different set of bytes.\n  {why}",
            scope.manifest.display(),
            id_list(&expected.ids),
            dir.display(),
            id_list(&observed)
        ));
    }
    Ok(())
}

/// A path under the corpus root whose every component BELOW that root is a REAL directory,
/// and whose canonical form stays beneath the canonical corpus root (roborev job 27 B3).
///
/// The documented-scope branch used to accept `dir.is_dir()`, which FOLLOWS symlinks, while
/// the fallback branch lstat'ed its table directory and refused symlinks — inconsistent
/// hardening, in favour of the branch a MANIFEST controls. So a correctly shaped
/// `sstable_dir` (right keyspace, a name `is_table_dir` accepts: `<table>-<32 hex>`)
/// that happened to be a symlink redirected ingestion — and therefore the measurement —
/// anywhere on the filesystem, and exact ingestion canonicalizes through symlinks too, so
/// nothing downstream noticed. Both branches now go through this one function, so they
/// cannot drift apart again.
///
/// Only components BELOW the corpus root are checked: the root itself is operator-supplied
/// (`--corpus`), and requiring IT to be symlink-free would reject ordinary layouts like
/// `/data -> /mnt/data`. Its symlinks are resolved once, by canonicalizing it, and the
/// containment test is done in canonical space.
fn real_dir_beneath(
    corpus: &Path,
    rel: &[&str],
    documented_by: &str,
) -> std::result::Result<PathBuf, String> {
    let hardening = "Every component below the corpus root must be a REAL directory (lstat, no \
                     symlink) and must stay beneath that root: a symlinked component redirects \
                     ingestion — and the measurement — outside the corpus, so the figure would \
                     describe bytes the corpus does not contain.\n  remedy: regenerate the \
                     corpus, or pass --manifest PATH for the corpus you are actually scanning.";
    let canonical_root = corpus.canonicalize().map_err(|e| {
        format!(
            "cannot resolve the corpus root {} ({e}), so no path can be proven to stay inside it.",
            corpus.display()
        )
    })?;
    let mut path = corpus.to_path_buf();
    for component in rel {
        path.push(component);
        match std::fs::symlink_metadata(&path) {
            Ok(md) if md.is_dir() => {}
            Ok(md) => {
                let what = if md.file_type().is_symlink() {
                    let target = std::fs::read_link(&path)
                        .map(|t| format!(" -> {}", t.display()))
                        .unwrap_or_default();
                    format!("a symbolic link{target}")
                } else if md.is_file() {
                    "a regular file".to_string()
                } else {
                    format!("{:?}", md.file_type())
                };
                return Err(format!(
                    "{documented_by}, but {} is not a directory — it is {what}.\n  {hardening}",
                    path.display()
                ));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(format!(
                    "{documented_by}, but {} is not a directory: it does not exist. The manifest \
                     describes a corpus that is not here, so its row count cannot verify a scan \
                     of whatever is.\n  remedy: regenerate the corpus, or pass --manifest PATH \
                     for the corpus you are actually scanning.",
                    path.display()
                ))
            }
            Err(e) => {
                return Err(format!(
                    "{documented_by}, but what {} is cannot be OBSERVED (lstat failed: {e}). A \
                     component that cannot be classified is not one this run can measure \
                     through.\n  {hardening}",
                    path.display()
                ))
            }
        }
    }
    // Belt and braces: with no symlinked component below the root this cannot fail, and it
    // is asserted anyway because it is the property that actually matters.
    let canonical = path.canonicalize().map_err(|e| {
        format!(
            "{documented_by}, but {} cannot be resolved ({e}), so it cannot be proven to be \
             inside the corpus root.\n  {hardening}",
            path.display()
        )
    })?;
    if !canonical.starts_with(&canonical_root) {
        return Err(format!(
            "{documented_by}, but {} resolves to {}, which is OUTSIDE the corpus root {}.\n  \
             {hardening}",
            path.display(),
            canonical.display(),
            canonical_root.display()
        ));
    }
    Ok(path)
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
            // REAL directory components only, and the result must stay beneath the corpus
            // root (roborev job 27 B3): `is_dir()` follows symlinks, so a correctly shaped
            // `sstable_dir` could point the measurement outside the corpus entirely.
            let rel: Vec<&str> = m.sstable_dir_rel.split('/').collect();
            let dir = real_dir_beneath(
                corpus,
                &rel,
                &format!(
                    "{} documents `tables[].sstable_dir` = {}",
                    m.manifest.display(),
                    m.sstable_dir_rel
                ),
            )?;
            let provenance = format!(
                "{} tables[].sstable_dir = {} (EXACT: anything else under {} is not ingested)",
                m.manifest.display(),
                m.sstable_dir_rel,
                ks_dir.display()
            );
            (dir, provenance)
        }
        None => {
            // The keyspace path is reached under the SAME rule as the documented branch
            // (roborev job 27 B3). The fallback used to lstat only the TABLE directory, so
            // a symlinked `sstables` or `sstables/<keyspace>` still redirected the scan out
            // of the corpus: the two branches must be equally strict, and now share the
            // one function that makes them so.
            let ks_real = real_dir_beneath(
                corpus,
                &["sstables", keyspace],
                &format!(
                    "this run scans {keyspace}.{table} under the corpus root {}",
                    corpus.display()
                ),
            )?;
            let mut found = matching_dirs(&ks_real, table)?;
            match found.len() {
                0 => {
                    return Err(format!(
                        "no `{table}-<uuid>` directory under {} — nothing to measure.",
                        ks_dir.display()
                    ))
                }
                1 => {
                    let selected = found.remove(0);
                    let name = selected
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_else(|| {
                            format!(
                                "the sole `{table}-<uuid>` directory under {} has no readable \
                                 final path component",
                                ks_dir.display()
                            )
                        })?
                        .to_string();
                    // Same containment rule as the documented branch, on the same path.
                    let dir = real_dir_beneath(
                        corpus,
                        &["sstables", keyspace, &name],
                        &format!(
                            "this run resolved the sole `{table}-<uuid>` directory {name} under \
                             the corpus root {}",
                            corpus.display()
                        ),
                    )?;
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
    // ONE observation of the directory, used for both the count and the identifier set.
    let names = data_db_entries(&dir)?;
    let generations = names.len();
    // The manifest documents WHICH generations are in there, so that is checked BEFORE a
    // scan spends minutes measuring a workload the authority does not describe (roborev
    // job 27 B2).
    if let Some(m) = documented {
        verify_documented_generations(&dir, m, &names)?;
    }
    Ok(IngestScope {
        data_dir,
        dir,
        generations,
        provenance,
    })
}
