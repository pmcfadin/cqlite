//! Input discovery for `cqlite salvage` (issue #4196, #4626) — resolving `args.input` (a single
//! `Data.db` file, or a table directory) into the ordered set of generations to salvage, and
//! deriving a target table name from an input's own directory naming.
//!
//! A table directory is listed through the generation module (`list_generations`, Scope Live,
//! `TocMissing` accepted; GD10): this file keeps no scan of its own. What the module returns that
//! salvage cannot salvage is a named refusal here, in salvage's own vocabulary.

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::generation::{Condition, Generation};

use crate::commands::generation_listing::{data_path, list_live, print_notes};

/// A `*-Data.db` that [`discover_salvage_inputs`] declined to include — a below-floor generation,
/// an Orphan, or a name the listing does not recognise as an SSTable component — named with the
/// reason rather than silently dropped (roborev, issue #4196, batched finding h).
pub(super) struct SkippedInput {
    pub(super) path: PathBuf,
    pub(super) reason: String,
}

/// [`discover_salvage_inputs`]'s result: the generations it WILL salvage (the module's order,
/// `SSTableIdFactory.COMPARATOR`), separate from the ones it named and skipped.
pub(super) struct SalvageDiscovery {
    pub(super) generations: Vec<PathBuf>,
    pub(super) skipped: Vec<SkippedInput>,
    /// Generations that are salvaged although their `TOC.txt` is missing (`TocMissing`): listed
    /// from a table directory (GD10) or named explicitly as a single file. They ARE salvaged, but
    /// the absence is recorded so it reaches the manifest instead of vanishing: without the
    /// publication barrier salvage cannot know the generation was ever COMPLETELY written.
    pub(super) barrier_absent: Vec<PathBuf>,
}

/// `args.input` is a single `Data.db` file, or a table directory whose live generations are
/// salvaged separately, in the generation module's order.
///
/// # A table directory (issue #4626, GD10)
///
/// Listed with Scope Live, which never recurses: an `--out` directory placed inside the input tree
/// is never rediscovered as an input, and `snapshots/` and `backups/` are never entered.
///
/// * `TocMissing` is accepted: the generation is salvaged and recorded in
///   [`SalvageDiscovery::barrier_absent`] (an imperfect, exit 3, outcome, as for an explicit file).
/// * A below-floor generation is refused as `UnsupportedVersion` and never read: named and skipped.
/// * An Orphan is named and skipped with its condition.
/// * A `*-Data.db` the listing ignores (not an SSTable component name) is named and skipped.
/// * A UUID-id generation refuses the WHOLE run by name (salvage requires sequence ids, owner
///   ruling Q41): proceeding with the sequence generations alone would present a partial table as
///   a salvage of it.
/// * An inconsistent or unreadable transaction log refuses the run, naming the log.
///
/// # A single file
///
/// An explicit file path is an operator's explicit choice: salvaging an unpublished (partially
/// flushed, interrupted) generation is a legitimate recovery scenario, so a `TocMissing` file is
/// SALVAGED and recorded in [`SalvageDiscovery::barrier_absent`], never silently (round-22 Low
/// finding): same exit code (3) for the same file whichever way it is named.
pub(super) fn discover_salvage_inputs(input: &Path) -> anyhow::Result<SalvageDiscovery> {
    if input.is_file() {
        return single_file(input);
    }
    if !input.is_dir() {
        return Err(anyhow::anyhow!(
            "{} is neither a file nor a directory",
            input.display()
        ));
    }
    let listing = list_live(input)?;
    print_notes("salvage", &listing.notes);

    let uuid_ids: Vec<String> = listing
        .live
        .iter()
        .filter(|g| g.id().sequence().is_none())
        .map(Generation::base_name)
        .collect();
    if !uuid_ids.is_empty() {
        return Err(anyhow::anyhow!(
            "salvage requires a sequential-integer SSTable id, but {} holds UUID-id generation(s):              {} — UUID-form SSTable ids are not yet supported; the run did not proceed with the              other generations",
            input.display(),
            uuid_ids.join(", ")
        ));
    }

    let mut generations = Vec::new();
    let mut skipped = Vec::new();
    let mut barrier_absent = Vec::new();
    for g in &listing.live {
        if let Some(below) = g.below_floor() {
            skipped.push(SkippedInput {
                path: data_path(g),
                reason: format!(
                    "{}: UnsupportedVersion ({} is below the supported floor {}); salvage does not \
                     read it",
                    g.base_name(),
                    below.version,
                    below.floor
                ),
            });
            continue;
        }
        if g.has_condition(Condition::TocMissing) {
            barrier_absent.push(data_path(g));
        }
        generations.push(data_path(g));
    }
    for g in &listing.orphans {
        skipped.push(SkippedInput {
            path: data_path(g),
            reason: format!(
                "{}: Orphan (Data.db missing or zero-length); not salvaged",
                g.base_name()
            ),
        });
    }
    for path in &listing.ignored {
        let named = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if path.is_file() && named.ends_with("-Data.db") {
            skipped.push(SkippedInput {
                path: path.clone(),
                reason: format!(
                    "'{named}' is not an SSTable component name (the listing ignores it)"
                ),
            });
        }
    }
    Ok(SalvageDiscovery {
        generations,
        skipped,
        barrier_absent,
    })
}

/// The single-file input: salvaged as named; `TocMissing` is read from the module's listing of its
/// directory, not from a probe of its own.
fn single_file(input: &Path) -> anyhow::Result<SalvageDiscovery> {
    let mut barrier_absent = Vec::new();
    let parent = match input.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let listing = list_live(parent)?;
    if let Some(named) = input.file_name().and_then(|n| n.to_str()) {
        let is_this = |g: &&Generation| format!("{}-Data.db", g.base_name()) == named;
        if listing
            .live
            .iter()
            .find(is_this)
            .is_some_and(|g| g.has_condition(Condition::TocMissing))
        {
            barrier_absent.push(input.to_path_buf());
        }
    }
    Ok(SalvageDiscovery {
        generations: vec![input.to_path_buf()],
        skipped: Vec::new(),
        barrier_absent,
    })
}

/// Derive the target table's DIRECTORY NAME from `input` — either `input`
/// itself (a table dir) or its PARENT (a single `Data.db` file) — walking up
/// past a `snapshots/<tag>` layer when present, then stripping a trailing
/// `-<32-hex-id>` suffix, matching Cassandra's own `<table>-<tableId>`
/// directory convention.
///
/// Ports BOTH halves of `cqlite_core::storage::sstable::snapshot_path`'s
/// algorithm — [`resolve_table_dir_name`]'s `snapshots/` walk (mirroring
/// `resolve_table_dir`) and `extract_table_name`'s id strip. That module is
/// crate-private to `cqlite-core`, so it is reimplemented locally rather than
/// widening its visibility for one caller — roborev, issue #4196, round-12
/// High finding for the id strip, round-22 Medium finding for the snapshot
/// walk, which the original port CLAIMED ("mirrors … exactly") but did not
/// actually carry: for
/// `…/mytable-9f3a…e9/snapshots/pre-repair/nb-1-big-Data.db` it derived
/// `pre-repair`, so `load_compaction_table_schema_for_table` then failed
/// closed with the misleading "failed to resolve a schema for table
/// 'pre-repair'" — and a snapshot directory is one of the likeliest inputs a
/// recovery tool is ever pointed at.
pub(super) fn table_name_from_input(input: &Path) -> Option<String> {
    let dir_name = resolve_table_dir_name(input)?;
    match dir_name.rsplit_once('-') {
        Some((table_name, id)) if is_table_id_suffix(id) => Some(table_name.to_string()),
        _ => Some(dir_name.to_string()),
    }
}

/// The name of the real `<table>-<32-hex-id>` directory for `input`, walking up
/// past a `snapshots/<tag>` layer — a port of `snapshot_path::resolve_table_dir`
/// (roborev, issue #4196, round-22 Medium finding).
///
/// The starting candidate is `input` itself when it is a directory, else its
/// parent (a single `Data.db` file). From there the walk-up fires ONLY when
/// BOTH hold, exactly as the core module requires:
///
/// 1. the candidate's PARENT is literally named `snapshots`, AND
/// 2. the directory ABOVE `snapshots` matches the `<table>-<32-hex>` shape
///    ([`is_table_id_dir`]).
///
/// Guard 2 is not optional: without it an ordinary table living in a keyspace
/// literally NAMED `snapshots` (`…/data/snapshots/mytable-<id>/…-Data.db`)
/// would be misresolved to `data` — there the directory above `snapshots` is
/// the keyspace root, which is not a table dir, so the path is correctly left
/// alone as a normal layout.
fn resolve_table_dir_name(input: &Path) -> Option<&str> {
    let candidate = if input.is_dir() {
        input
    } else {
        input.parent()?
    };
    if let Some(maybe_snapshots) = candidate.parent() {
        if maybe_snapshots.file_name().and_then(|n| n.to_str()) == Some("snapshots") {
            if let Some(name) = maybe_snapshots
                .parent()
                .and_then(|real_table_dir| real_table_dir.file_name())
                .and_then(|n| n.to_str())
            {
                if is_table_id_dir(name) {
                    return Some(name);
                }
            }
        }
    }
    candidate.file_name()?.to_str()
}

/// `true` iff `name` matches Cassandra's `<table>-<32-hex-id>` directory shape:
/// a NON-EMPTY table name, a `-`, then exactly 32 lowercase hex chars (same
/// predicate as `snapshot_path::is_table_id_dir`).
fn is_table_id_dir(name: &str) -> bool {
    match name.rsplit_once('-') {
        Some((table_name, id)) => !table_name.is_empty() && is_table_id_suffix(id),
        None => false,
    }
}

/// `true` iff `id` is EXACTLY 32 lowercase hex chars — a Cassandra table-id
/// suffix (same predicate as `snapshot_path::is_table_id_suffix`).
fn is_table_id_suffix(id: &str) -> bool {
    id.len() == 32
        && id
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, 'a'..='f'))
}

#[cfg(test)]
mod tests {
    use super::{discover_salvage_inputs, is_table_id_dir, table_name_from_input};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    /// Stage a published generation: `<prefix>-Data.db` plus its `-TOC.txt`
    /// publication barrier (contents irrelevant — discovery reads names only).
    fn published(dir: &Path, prefix: &str) {
        std::fs::write(dir.join(format!("{prefix}-Data.db")), b"data")
            .unwrap_or_else(|e| panic!("write {prefix}-Data.db: {e}"));
        std::fs::write(dir.join(format!("{prefix}-TOC.txt")), b"Data.db\n")
            .unwrap_or_else(|e| panic!("write {prefix}-TOC.txt: {e}"));
    }

    /// Roborev, issue #4196, round-22 Low finding — an EXPLICITLY-named
    /// `Data.db` with no sibling `-TOC.txt` is still salvaged (the operator
    /// named it), but the missing publication barrier is RECORDED, never
    /// silent.
    #[test]
    fn explicit_file_without_toc_txt_is_salvaged_but_the_barrier_absence_is_recorded() {
        let temp = TempDir::new().expect("tempdir");
        let data_db = temp.path().join("nb-3-big-Data.db");
        std::fs::write(&data_db, b"data").expect("write Data.db");

        let discovery = discover_salvage_inputs(&data_db).expect("discovery must succeed");
        assert_eq!(discovery.generations, vec![data_db.clone()]);
        assert_eq!(
            discovery.barrier_absent,
            vec![data_db],
            "the missing -TOC.txt must be RECORDED so it reaches the manifest"
        );
        assert!(discovery.skipped.is_empty());
    }

    /// The barrier PRESENT: nothing recorded.
    #[test]
    fn explicit_file_with_toc_txt_records_no_barrier_finding() {
        let temp = TempDir::new().expect("tempdir");
        published(temp.path(), "nb-3-big");
        let data_db = temp.path().join("nb-3-big-Data.db");

        let discovery = discover_salvage_inputs(&data_db).expect("discovery must succeed");
        assert_eq!(discovery.generations, vec![data_db]);
        assert!(discovery.barrier_absent.is_empty());
    }

    /// A file NOT named like an SSTable component has no generation to look up, so nothing is
    /// recorded (the reader rejects it on its own merits).
    #[test]
    fn explicit_file_not_named_data_db_records_no_barrier_finding() {
        let temp = TempDir::new().expect("tempdir");
        let odd = temp.path().join("something-else.bin");
        std::fs::write(&odd, b"data").expect("write file");

        let discovery = discover_salvage_inputs(&odd).expect("discovery must succeed");
        assert_eq!(discovery.generations, vec![odd]);
        assert!(discovery.barrier_absent.is_empty());
    }

    /// GD10: a table-DIRECTORY input ACCEPTS `TocMissing` — the generation is salvaged and the
    /// absence recorded — and names an Orphan without salvaging it.
    #[test]
    fn directory_input_accepts_toc_missing_and_skips_an_orphan_by_name() {
        let temp = TempDir::new().expect("tempdir");
        published(temp.path(), "nb-1-big");
        std::fs::write(temp.path().join("nb-2-big-Data.db"), b"data").expect("write unpublished");
        std::fs::write(temp.path().join("nb-3-big-Data.db"), b"").expect("write orphan");

        let discovery = discover_salvage_inputs(temp.path()).expect("discovery must succeed");
        assert_eq!(
            discovery.generations.len(),
            2,
            "nb-1 and the TocMissing nb-2"
        );
        assert_eq!(discovery.barrier_absent.len(), 1);
        assert!(discovery.barrier_absent[0].ends_with("nb-2-big-Data.db"));
        assert_eq!(discovery.skipped.len(), 1);
        assert!(discovery.skipped[0].path.ends_with("nb-3-big-Data.db"));
        assert!(discovery.skipped[0].reason.contains("Orphan"));
    }

    /// The listing's order is the comparator's (numeric, never lexicographic), whatever order the
    /// entries were created in, with a tie between formats broken by format name.
    #[test]
    fn generations_come_back_in_the_modules_order() {
        let temp = TempDir::new().expect("tempdir");
        let dir = temp.path();
        published(dir, "nb-2-big");
        published(dir, "nb-1-big");
        published(dir, "da-1-bti");
        published(dir, "da-10-bti");

        let discovery = discover_salvage_inputs(dir).expect("discovery must succeed");
        let names: Vec<String> = discovery
            .generations
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        assert_eq!(
            names,
            vec![
                // A tie on the id is broken by format name (`big` before `bti`), the module's
                // deterministic rule; the old scan broke it by path (`da-` before `nb-`).
                "nb-1-big-Data.db",
                "da-1-bti-Data.db",
                "nb-2-big-Data.db",
                "da-10-bti-Data.db",
            ]
        );
        assert!(discovery.skipped.is_empty());
    }

    /// GD10: a below-floor generation is skipped by name as `UnsupportedVersion`, never salvaged.
    #[test]
    fn a_below_floor_generation_is_skipped_as_unsupported_version() {
        let temp = TempDir::new().expect("tempdir");
        published(temp.path(), "ma-1-big");
        published(temp.path(), "nb-1-big");
        let discovery = discover_salvage_inputs(temp.path()).expect("discovery must succeed");
        assert_eq!(discovery.generations.len(), 1);
        assert!(discovery.generations[0].ends_with("nb-1-big-Data.db"));
        assert_eq!(discovery.skipped.len(), 1);
        assert!(discovery.skipped[0].reason.contains("UnsupportedVersion"));
    }

    /// GD10.3: a UUID-id generation refuses the whole discovery, naming it.
    #[test]
    fn a_uuid_id_generation_refuses_discovery_by_name() {
        let temp = TempDir::new().expect("tempdir");
        published(temp.path(), "nb-1-big");
        published(temp.path(), "nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big");
        let err = discover_salvage_inputs(temp.path())
            .err()
            .expect("a UUID-id generation must refuse")
            .to_string();
        assert!(err.contains("nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big"), "{err}");
        assert!(err.contains("UUID"), "{err}");
    }

    /// A syntactically valid Cassandra table id: exactly 32 lowercase hex chars.
    const ID: &str = "9f3a1b2c4d5e6f708192a3b4c5d6e7e9";

    #[test]
    fn id_predicate_matches_cassandras_shape() {
        assert_eq!(
            ID.len(),
            32,
            "the test id must be 32 chars to mean anything"
        );
        assert!(is_table_id_dir(&format!("mytable-{ID}")));
        // A bare id with no table name is NOT a table dir (the core module's
        // `!table_name.is_empty()` half).
        assert!(!is_table_id_dir(&format!("-{ID}")));
        assert!(!is_table_id_dir("mytable-notlongenough"));
        // Uppercase hex is not Cassandra's spelling.
        assert!(!is_table_id_dir(&format!("mytable-{}", ID.to_uppercase())));
    }

    /// The normal (non-snapshot) layout, unchanged by the round-22 fix: a
    /// `Data.db` directly under `<table>-<id>/`.
    #[test]
    fn normal_layout_data_db_resolves_the_table_name() {
        let path = PathBuf::from(format!(
            "/var/lib/cassandra/data/ks/mytable-{ID}/nb-1-big-Data.db"
        ));
        assert_eq!(table_name_from_input(&path).as_deref(), Some("mytable"));
    }

    /// A directory name with no `-<32-hex>` suffix at all (a staged/synthetic
    /// fixture dir) is returned whole — the caller then fails closed when it
    /// matches no `CREATE TABLE`.
    #[test]
    fn id_less_directory_name_is_returned_whole() {
        let path = Path::new("/tmp/index_db_bit_flip_big/nb-1-big-Data.db");
        assert_eq!(
            table_name_from_input(path).as_deref(),
            Some("index_db_bit_flip_big")
        );
    }

    /// Roborev, issue #4196, round-22 Medium finding — a SNAPSHOT-layout
    /// `Data.db` resolves the REAL table name, not the snapshot TAG.
    ///
    /// Before the fix this derived `pre-repair` (the parent directory), and
    /// `load_compaction_table_schema_for_table` then failed closed with
    /// "failed to resolve a schema for table 'pre-repair'" — a misleading
    /// message for one of the likeliest inputs a recovery tool is pointed at.
    #[test]
    fn snapshot_layout_data_db_resolves_the_real_table_name() {
        let path = PathBuf::from(format!(
            "/var/lib/cassandra/data/ks/mytable-{ID}/snapshots/pre-repair/nb-1-big-Data.db"
        ));
        assert_eq!(
            table_name_from_input(&path).as_deref(),
            Some("mytable"),
            "a snapshot Data.db must resolve the table dir above `snapshots`, never the tag"
        );
    }

    /// The same walk for a snapshot DIRECTORY input (`salvage <…>/snapshots/<tag>`)
    /// — a REAL directory, since the resolution branches on `is_dir()`.
    #[test]
    fn snapshot_directory_input_resolves_the_real_table_name() {
        let temp = TempDir::new().expect("tempdir");
        let snapshot_dir = temp
            .path()
            .join("ks")
            .join(format!("mytable-{ID}"))
            .join("snapshots")
            .join("pre-repair");
        std::fs::create_dir_all(&snapshot_dir).expect("create snapshot dir");
        assert!(
            snapshot_dir.is_dir(),
            "the dir branch must actually be taken"
        );
        assert_eq!(
            table_name_from_input(&snapshot_dir).as_deref(),
            Some("mytable")
        );
    }

    /// The `is_table_id_dir` GUARD on the grandparent: an ordinary table in a
    /// keyspace literally NAMED `snapshots` must NOT be misresolved to the
    /// keyspace's own parent directory (`data`). Mirrors
    /// `snapshot_path::resolve_table_dir`'s second condition — dropping it
    /// would trade one wrong answer for another.
    #[test]
    fn keyspace_literally_named_snapshots_is_not_misresolved() {
        let path = PathBuf::from(format!(
            "/var/lib/cassandra/data/snapshots/mytable-{ID}/nb-1-big-Data.db"
        ));
        assert_eq!(
            table_name_from_input(&path).as_deref(),
            Some("mytable"),
            "the dir above `snapshots` here is the keyspace root, not a `<table>-<id>` dir, so \
             this is a NORMAL layout and the walk-up must not fire"
        );
    }

    /// A snapshot whose table dir carries NO id (CQLite's own write engine
    /// emits `<ks>/<table>/`) is the core module's declared, unresolvable
    /// limitation (#2415): the guard cannot fire, so the tag is returned. Pinned
    /// so the behavior is a KNOWN gap rather than a surprise, exactly as
    /// `snapshot_path`'s own `idless_snapshot_currently_unresolved_pending_followup`
    /// test does.
    #[test]
    fn idless_snapshot_table_dir_returns_the_tag_known_limitation_2415() {
        let path = Path::new("/data/ks/mytable/snapshots/pre-repair/nb-1-big-Data.db");
        assert_eq!(
            table_name_from_input(path).as_deref(),
            Some("pre-repair"),
            "an ID-LESS snapshot dir is structurally indistinguishable from a table in a \
             keyspace named `snapshots` (core issue #2415); --table is the operator's route"
        );
    }
}
