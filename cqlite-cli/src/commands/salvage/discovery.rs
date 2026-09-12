//! Input discovery for `cqlite salvage` (issue #4196) — resolving `args.input`
//! (a single `Data.db` file, or a table directory) into the ordered set of
//! generations to salvage, and deriving a target table name from an input's
//! own directory naming. Split out of `salvage.rs` (round 15, campsite rule
//! / epic #1116) when that file crossed the ~800-line source threshold — a
//! PURE MOVE, no behavior changed by the split.

use std::path::{Path, PathBuf};

/// A `*-Data.db` (with a publishing `*-TOC.txt` sibling) that
/// [`discover_salvage_inputs`] declined to include because its generation
/// number could not be parsed — named rather than silently defaulted
/// (roborev, issue #4196, batched finding h).
pub(super) struct SkippedInput {
    pub(super) path: PathBuf,
    pub(super) reason: String,
}

/// [`discover_salvage_inputs`]'s result: the generations it WILL salvage
/// (oldest first), separate from the ones it named and skipped.
pub(super) struct SalvageDiscovery {
    pub(super) generations: Vec<PathBuf>,
    pub(super) skipped: Vec<SkippedInput>,
}

/// `args.input` is a single `Data.db` file, or a table directory whose
/// generations (BIG `nb-*-big-Data.db` AND BTI `da-*-bti-Data.db`, each with
/// a sibling `TOC.txt` publication barrier) are salvaged separately, oldest
/// generation first for deterministic output.
///
/// A file whose generation number cannot be parsed out of its name is NEVER
/// folded into the sortable set at sort key `0` (roborev, issue #4196,
/// batched finding h): that risked sorting a malformed entry AHEAD of every
/// real generation and, since `salvage_sstable` hard-erroring on it used to
/// abort the WHOLE run (fixed alongside this — see
/// `exit_after_partial_failure`), a single badly-named file could silently
/// prevent every sibling generation from ever being attempted. Such an entry
/// is instead named in [`SalvageDiscovery::skipped`] and excluded from
/// `generations`; the caller still salvages every OTHER generation.
pub(super) fn discover_salvage_inputs(input: &Path) -> anyhow::Result<SalvageDiscovery> {
    use anyhow::Context;

    if input.is_file() {
        return Ok(SalvageDiscovery {
            generations: vec![input.to_path_buf()],
            skipped: Vec::new(),
        });
    }
    if !input.is_dir() {
        return Err(anyhow::anyhow!(
            "{} is neither a file nor a directory",
            input.display()
        ));
    }

    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    let mut skipped: Vec<SkippedInput> = Vec::new();
    for entry in
        std::fs::read_dir(input).with_context(|| format!("failed to read {}", input.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let family = if name.ends_with("-big-Data.db") {
            "-big"
        } else if name.ends_with("-bti-Data.db") {
            "-bti"
        } else {
            continue;
        };
        // roborev, issue #4196, round-11 Low finding: `trim_end_matches`
        // strips REPEATED trailing occurrences of the pattern — a file
        // literally named `...-Data.db-Data.db` (or any stem that itself
        // ends in `-Data.db`) would have BOTH stripped, yielding a `base`
        // that no longer names the real component prefix, so the `-TOC.txt`
        // probe below then looks for the wrong sibling. `strip_suffix`
        // removes the suffix EXACTLY ONCE, which is what `family`'s match
        // above already established this loop needs.
        let Some(base) = name.strip_suffix("-Data.db") else {
            continue;
        };
        let toc = path.with_file_name(format!("{base}-TOC.txt"));
        if !toc.exists() {
            // roborev, issue #4196 (round-5 Medium): the SAME visibility gap
            // finding 5 fixed for an unparseable generation number, left
            // open for a missing publication barrier — a very plausible
            // damage mode for this tool's own target audience. Named and
            // skipped via the SAME `SkippedInput` vehicle rather than
            // silently `continue`d, so it is recorded in the manifest and
            // forces an imperfect (exit 3) outcome instead of vanishing.
            skipped.push(SkippedInput {
                path,
                reason: format!("no sibling {base}-TOC.txt (unpublished generation)"),
            });
            continue;
        }
        match base
            .strip_suffix(family)
            .and_then(|s| s.rsplit_once('-'))
            .and_then(|(_, g)| g.parse::<u64>().ok())
        {
            Some(generation) => found.push((generation, path)),
            None => skipped.push(SkippedInput {
                path,
                reason: format!("'{name}' does not end in <family>-<integer>-Data.db"),
            }),
        }
    }
    found.sort_by_key(|(g, _)| *g);
    Ok(SalvageDiscovery {
        generations: found.into_iter().map(|(_, p)| p).collect(),
        skipped,
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
    use super::{is_table_id_dir, table_name_from_input};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

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
