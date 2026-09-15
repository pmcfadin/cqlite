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
    /// Generations named EXPLICITLY as a single-file input that carry no
    /// sibling `-TOC.txt` publication barrier (roborev, issue #4196, round-22
    /// Low finding). They ARE salvaged — an explicit file path overrides the
    /// barrier, see [`discover_salvage_inputs`]'s doc — but the absence is
    /// recorded so it reaches the manifest instead of vanishing. Always empty
    /// for a table-directory input, where the same condition is a
    /// [`SkippedInput`] instead.
    pub(super) barrier_absent: Vec<PathBuf>,
}

/// `args.input` is a single `Data.db` file, or a table directory whose
/// generations (BIG `nb-*-big-Data.db` AND BTI `da-*-bti-Data.db`, each with
/// a sibling `TOC.txt` publication barrier) are salvaged separately, oldest
/// generation first — then, for a TIE, by path — for deterministic output on
/// every filesystem (roborev, issue #4196, round-22 Low finding).
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
///
/// # The `-TOC.txt` publication barrier: enforced for a DIRECTORY, overridden
/// for an explicit FILE — and NEVER silently
///
/// Roborev, issue #4196, round-22 Low finding: the single-file branch returned
/// with no `-TOC.txt` probe at all, while the directory branch names a
/// barrier-less generation as a [`SkippedInput`] that lands in the manifest and
/// forces exit 3. So `salvage ./ks/t-<id>/nb-3-big-Data.db` salvaged an
/// unpublished generation SILENTLY while `salvage ./ks/t-<id>/` on the same file
/// named it and refused — an asymmetry nothing declared.
///
/// The resolution is deliberate, and it is not "make both refuse": an explicit
/// file path is an operator's explicit choice, and salvaging an unpublished
/// (partially flushed, interrupted) generation is a legitimate recovery
/// scenario this tool should not be able to be talked out of. So the file branch
/// SALVAGES it — and records the absence in
/// [`SalvageDiscovery::barrier_absent`], which becomes an
/// `UnpublishedInputGeneration` component finding in the manifest and, like the
/// directory branch, an imperfect (exit 3) outcome: without the barrier salvage
/// cannot know the generation was ever COMPLETELY written, so the run's premise
/// is unverified and must not read as clean. Same exit code for the same file
/// whichever way it is named; the difference is that the file form still
/// recovers the data.
///
/// The probe needs the component prefix, so it fires only for a file actually
/// named `*-Data.db`. Any other name has no derivable `-TOC.txt` sibling to look
/// for and is left to the reader to reject on its own merits.
pub(super) fn discover_salvage_inputs(input: &Path) -> anyhow::Result<SalvageDiscovery> {
    use anyhow::Context;

    if input.is_file() {
        let mut barrier_absent = Vec::new();
        if let Some(base) = input
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix("-Data.db"))
        {
            if !input.with_file_name(format!("{base}-TOC.txt")).exists() {
                barrier_absent.push(input.to_path_buf());
            }
        }
        return Ok(SalvageDiscovery {
            generations: vec![input.to_path_buf()],
            skipped: Vec::new(),
            barrier_absent,
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
    sort_generations(&mut found);
    Ok(SalvageDiscovery {
        generations: found.into_iter().map(|(_, p)| p).collect(),
        skipped,
        // A directory input enforces the barrier (a barrier-less generation is
        // a `SkippedInput` above), so there is never an overridden one here.
        barrier_absent: Vec::new(),
    })
}

/// Order the discovered generations TOTALLY: generation number ascending, then
/// PATH ascending as a tiebreak.
///
/// Roborev, issue #4196, round-22 Low finding: this was
/// `found.sort_by_key(|(g, _)| *g)` — the generation ALONE. `sort_by_key` is
/// STABLE, so tied generations kept `read_dir` order, which is filesystem- and
/// platform-dependent. A mixed-format mid-migration table dir holding both
/// `nb-1-big-Data.db` and `da-1-bti-Data.db` (the exact case
/// [`discover_salvage_inputs`]'s loop scans BOTH families for) therefore
/// salvaged in a non-reproducible order, contradicting that function's own
/// "oldest generation first for deterministic output" contract — and with it the
/// D5 manifest ARRAY's entry order, which is a machine-readable output.
///
/// Factored out as its own function specifically so the property can be tested
/// as ORDER-INVARIANCE (`sort_generations` produces the same result from any
/// input permutation) rather than through `read_dir`: the end-to-end version of
/// this test PASSES on macOS/APFS even with the tie-blind sort restored, because
/// that filesystem happened to enumerate the entries in the expected order — a
/// test that cannot fail on the machine running it proves nothing.
fn sort_generations(found: &mut [(u64, PathBuf)]) {
    found.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
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
    use super::{
        discover_salvage_inputs, is_table_id_dir, sort_generations, table_name_from_input,
    };
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
    /// silent. The directory branch's own behavior for the same file (a named
    /// `SkippedInput`) is pinned by `salvage_cli_tests`.
    #[test]
    fn explicit_file_without_toc_txt_is_salvaged_but_the_barrier_absence_is_recorded() {
        let temp = TempDir::new().expect("tempdir");
        let data_db = temp.path().join("nb-3-big-Data.db");
        std::fs::write(&data_db, b"data").expect("write Data.db");

        let discovery = discover_salvage_inputs(&data_db).expect("discovery must succeed");
        assert_eq!(
            discovery.generations,
            vec![data_db.clone()],
            "an explicit file path must still be salvaged — the barrier is overridden, not enforced"
        );
        assert_eq!(
            discovery.barrier_absent,
            vec![data_db],
            "the missing -TOC.txt must be RECORDED so it reaches the manifest"
        );
        assert!(discovery.skipped.is_empty());
    }

    /// The barrier PRESENT: nothing recorded. Without this the case above could
    /// pass while recording the absence unconditionally.
    #[test]
    fn explicit_file_with_toc_txt_records_no_barrier_finding() {
        let temp = TempDir::new().expect("tempdir");
        published(temp.path(), "nb-3-big");
        let data_db = temp.path().join("nb-3-big-Data.db");

        let discovery = discover_salvage_inputs(&data_db).expect("discovery must succeed");
        assert_eq!(discovery.generations, vec![data_db]);
        assert!(
            discovery.barrier_absent.is_empty(),
            "a published generation has nothing to record; got {:?}",
            discovery.barrier_absent
        );
    }

    /// A file NOT named `*-Data.db` has no derivable `-TOC.txt` sibling to probe
    /// for, so nothing is recorded (the reader rejects it on its own merits) —
    /// the probe must not invent a component prefix.
    #[test]
    fn explicit_file_not_named_data_db_records_no_barrier_finding() {
        let temp = TempDir::new().expect("tempdir");
        let odd = temp.path().join("something-else.bin");
        std::fs::write(&odd, b"data").expect("write file");

        let discovery = discover_salvage_inputs(&odd).expect("discovery must succeed");
        assert_eq!(discovery.generations, vec![odd]);
        assert!(discovery.barrier_absent.is_empty());
    }

    /// A table-DIRECTORY input never overrides the barrier: the same
    /// barrier-less generation is a named `SkippedInput` and
    /// `barrier_absent` stays empty.
    #[test]
    fn directory_input_enforces_the_barrier_and_records_no_override() {
        let temp = TempDir::new().expect("tempdir");
        published(temp.path(), "nb-1-big");
        std::fs::write(temp.path().join("nb-2-big-Data.db"), b"data").expect("write unpublished");

        let discovery = discover_salvage_inputs(temp.path()).expect("discovery must succeed");
        assert_eq!(discovery.generations.len(), 1, "only nb-1 is published");
        assert_eq!(discovery.skipped.len(), 1);
        assert!(discovery.skipped[0]
            .path
            .to_string_lossy()
            .ends_with("nb-2-big-Data.db"));
        assert!(
            discovery.barrier_absent.is_empty(),
            "a directory input ENFORCES the barrier; the override is the file form only"
        );
    }

    /// Roborev, issue #4196, round-22 Low finding — TIED generation numbers
    /// must order DETERMINISTICALLY, and the order must not depend on the order
    /// the entries were DISCOVERED in.
    ///
    /// This is the case that actually FAILS under the old
    /// `sort_by_key(|(g, _)| *g)`: that sort is STABLE, so it returns tied
    /// entries in input order — i.e. a DIFFERENT answer per permutation, which
    /// in `discover_salvage_inputs` is whatever `read_dir` yields on the
    /// operator's filesystem. Asserted as ORDER-INVARIANCE over every
    /// permutation of a tied pair, so it cannot pass by luck.
    ///
    /// The end-to-end sibling test below (through a real `read_dir`) is
    /// deliberately NOT the primary pin: it passes on macOS/APFS even with the
    /// defect restored, because that filesystem happened to enumerate the
    /// entries in the expected order — measured, not assumed.
    #[test]
    fn tied_generations_sort_identically_from_every_input_permutation() {
        let nb = PathBuf::from("/data/t/nb-1-big-Data.db");
        let da = PathBuf::from("/data/t/da-1-bti-Data.db");
        // `da-…` < `nb-…` lexicographically, so path order is the tiebreak.
        let expected = vec![(1u64, da.clone()), (1u64, nb.clone())];
        for input in [
            vec![(1u64, nb.clone()), (1u64, da.clone())],
            vec![(1u64, da.clone()), (1u64, nb.clone())],
        ] {
            let mut found = input.clone();
            sort_generations(&mut found);
            assert_eq!(
                found, expected,
                "tied generations must sort to ONE canonical order whatever order they were \
                 discovered in (input was {input:?}) — a stable sort on the generation alone \
                 returns them in discovery order, which is `read_dir`'s, which is the \
                 filesystem's"
            );
        }
    }

    /// The same ordering end to end, through a real `read_dir`, and covering the
    /// NUMERIC half (generation 2 before 10, never lexicographic). The FULL
    /// ordering is asserted, not just the first element.
    ///
    /// Weaker than its sibling above by construction — see that test's doc.
    #[test]
    fn tied_generations_order_by_path_and_the_full_order_is_deterministic() {
        let temp = TempDir::new().expect("tempdir");
        let dir = temp.path();
        // Written in an order that is neither the expected output order nor its
        // reverse, so a pass cannot be an artifact of insertion order.
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
                // generation 1, tie broken by path: `da-` sorts before `nb-`
                "da-1-bti-Data.db".to_string(),
                "nb-1-big-Data.db".to_string(),
                // then 2, then 10 — NUMERIC, never lexicographic ("10" < "2")
                "nb-2-big-Data.db".to_string(),
                "da-10-bti-Data.db".to_string(),
            ],
            "generations must be ordered by generation number, then by path"
        );
        assert!(
            discovery.skipped.is_empty(),
            "every staged generation is published and well-named; got {:?}",
            discovery
                .skipped
                .iter()
                .map(|s| s.path.display().to_string())
                .collect::<Vec<_>>()
        );
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
