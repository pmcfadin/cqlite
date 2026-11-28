//! Generation discovery over real and mutated-copy directories (#4626, spec G3 to G13).
//!
//! Oracles: committed Cassandra-written corpus tables (`sstables_root_for_table`, #3220) and the
//! Cassandra 5.0.8 fixtures under `test-data/fixtures/issue_4626`. Layout cases (TOC deleted,
//! Orphan, scopes, id forms) are built as COPIES in a tempdir; the shared corpus is never mutated
//! and no SSTable byte is hand-written. Every case resolves its own fixture and panics by name
//! when it is missing; there is no suite-wide "ran > 0" (#3220).

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use cqlite_core::storage::sstable::generation::{
    list_generations, next_generation_id, Condition, Generation, GenerationError, GenerationId,
    Listing, Scope, TocPolicy,
};
use generation_fixtures::*;

fn live_names(l: &Listing) -> Vec<String> {
    l.live.iter().map(Generation::base_name).collect()
}

fn list(dir: &std::path::Path, scope: Scope) -> Listing {
    list_generations(dir, &scope, TocPolicy::Accept)
        .unwrap_or_else(|e| panic!("listing {} failed: {e}", dir.display()))
}

fn next_id(dir: &std::path::Path) -> GenerationId {
    next_generation_id(dir)
        .unwrap_or_else(|e| panic!("allocation in {} failed: {e}", dir.display()))
}

/// Real corpus directories: BIG `nb` (several tables), BIG `oa` and BTI `da`.
const CORPUS: &[(&str, &str, &str)] = &[
    ("test_basic", "simple_table", "nb-1-big"),
    ("test_basic", "counters", "nb-1-big"),
    ("test_basic", "ttl_test_table", "nb-1-big"),
    ("test_collections", "collection_table", "nb-1-big"),
    ("test_wide_rows", "chat_messages", "nb-1-big"),
    ("test_oa", "simple_table", "oa-2-big"),
    ("test_da", "simple_table", "da-2-bti"),
];

/// G1.3, G9.2, G13.2: no log, no hiding; every `Data.db` is a Live generation, none carries
/// `UnsupportedVersion`, and every file in the directory is accounted for.
#[test]
fn corpus_tables_list_every_data_db_and_account_for_every_file() {
    for (ks, table, expected) in CORPUS {
        let dir = corpus_table(ks, table);
        let listing = list(&dir, Scope::Live);
        assert_eq!(
            live_names(&listing),
            vec![expected.to_string()],
            "{ks}.{table}"
        );
        assert!(
            listing.orphans.is_empty() && listing.hidden.is_empty(),
            "{ks}.{table}"
        );
        assert!(
            listing.txn_logs.is_empty() && listing.notes.is_empty(),
            "{ks}.{table}"
        );
        let g = &listing.live[0];
        assert!(
            g.conditions().is_empty(),
            "{ks}.{table}: {:?}",
            g.conditions()
        );
        assert!(g.below_floor().is_none(), "{ks}.{table}");

        // G13.2: components + ignored == every file on disk
        let on_disk = tree_signature(&dir).len();
        let accounted = listing
            .live
            .iter()
            .map(|g| g.components().len())
            .sum::<usize>()
            + listing.ignored.len();
        assert_eq!(accounted, on_disk, "{ks}.{table}: unaccounted files");
        assert!(g.component_path("Data.db").is_some(), "{ks}.{table}");
        assert!(g.components().iter().all(|c| !c.name.ends_with(".log")));
    }
}

/// G3.1 and G3.2 over a copy with `TOC.txt` deleted.
#[test]
fn toc_deleted_generation_stays_live_with_toc_missing_and_a_strict_caller_refuses() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    std::fs::remove_file(tmp.path().join("nb-1-big-TOC.txt")).unwrap();

    let listing = list(tmp.path(), Scope::Live);
    assert_eq!(live_names(&listing), vec!["nb-1-big"]);
    let g = &listing.live[0];
    assert_eq!(g.conditions(), &[Condition::TocMissing]);
    let discovered: Vec<&str> = g.components().iter().map(|c| c.name.as_str()).collect();
    assert!(discovered.contains(&"Data.db") && !discovered.contains(&"TOC.txt"));

    match list_generations(tmp.path(), &Scope::Live, TocPolicy::Refuse) {
        Err(GenerationError::TocMissingRefused { generations }) => {
            assert_eq!(generations, vec!["nb-1-big".to_string()]);
        }
        other => panic!("expected TocMissingRefused, got {other:?}"),
    }
}

/// G4.1: no `Data.db`: Orphan, not live, files untouched.
#[test]
fn missing_data_db_is_an_orphan_and_nothing_is_deleted() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    std::fs::remove_file(tmp.path().join("nb-1-big-Data.db")).unwrap();
    let before = tree_signature(tmp.path());

    let listing = list(tmp.path(), Scope::Live);
    assert!(listing.live.is_empty());
    assert_eq!(listing.orphans.len(), 1);
    assert_eq!(listing.orphans[0].base_name(), "nb-1-big");
    assert_eq!(listing.orphans[0].conditions(), &[Condition::Orphan]);
    assert_eq!(tree_signature(tmp.path()), before);
}

/// G4.2: a zero-length `Data.db` of a BTI generation.
#[test]
fn zero_length_data_db_is_an_orphan() {
    let src = corpus_table("test_da", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_generation_as(&src, "da-2-bti", tmp.path(), "da-5-bti");
    std::fs::write(tmp.path().join("da-5-bti-Data.db"), b"").unwrap();

    let listing = list(tmp.path(), Scope::Live);
    assert!(listing.live.is_empty());
    assert_eq!(listing.orphans[0].base_name(), "da-5-bti");
    assert!(listing.orphans[0].has_condition(Condition::Orphan));
}

/// G5.2, G5.3, G5.4: numeric order, sequence before UUID, out-of-range ids are not SSTables.
#[test]
fn ids_order_numerically_sequence_before_uuid_and_out_of_range_ids_are_ignored() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    for id in ["100", "9", "10", "7", "3gb2_0rbd_2lhua2pw7a2hlyvbsp"] {
        copy_generation_as(&src, "nb-1-big", tmp.path(), &format!("nb-{id}-big"));
    }
    touch(&tmp.path().join("nb-2147483648-big-Data.db"));
    touch(&tmp.path().join("nb-12345678901-big-Data.db"));

    let listing = list(tmp.path(), Scope::Live);
    assert_eq!(
        live_names(&listing),
        vec![
            "nb-7-big",
            "nb-9-big",
            "nb-10-big",
            "nb-100-big",
            "nb-3gb2_0rbd_2lhua2pw7a2hlyvbsp-big"
        ]
    );
    let ignored: Vec<String> = listing
        .ignored
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        ignored,
        vec!["nb-12345678901-big-Data.db", "nb-2147483648-big-Data.db"]
    );
}

/// G5.1, G5.3, G6.4 over the Cassandra-written mixed table (`uuid_sstable_identifiers_enabled`
/// flipped between flushes).
#[test]
fn cassandra_written_mixed_id_table_orders_sequence_first() {
    let dir = fixture_table("uuid_ids", "uuid_mixed");
    let listing = list(&dir, Scope::Live);
    assert_eq!(
        live_names(&listing),
        vec![
            "nb-1-big",
            "nb-2-big",
            "nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big",
            "nb-3h5t_0i6s_1tsu22lxu6305iz8wl-big"
        ]
    );
    let uuid = &listing.live[2];
    assert!(uuid.id().sequence().is_none());
    assert_eq!(uuid.id().to_string(), "3h5t_0i6r_2dipm2lxu6305iz8wl");
    // complete component set, in the same shape as a sequence generation
    assert_eq!(uuid.components().len(), listing.live[0].components().len());
    // G6.4: only the two sequence ids count
    assert_eq!(next_id(&dir), GenerationId::Sequence(3));
}

/// G5.1 and G6.4 with no sequence id at all.
#[test]
fn cassandra_written_uuid_only_table_allocates_one() {
    let dir = fixture_table("uuid_ids", "uuid_only");
    let listing = list(&dir, Scope::Live);
    assert_eq!(listing.live.len(), 2);
    assert!(listing.live.iter().all(|g| g.id().sequence().is_none()));
    assert!(listing.live.windows(2).all(|w| w[0].id() < w[1].id()));
    assert_eq!(next_id(&dir), GenerationId::Sequence(1));
}

/// G6.1: BIG and BTI, `nb`, `oa` and `da` share one id space.
#[test]
fn next_id_spans_formats_and_versions() {
    let tmp = tempfile::tempdir().unwrap();
    copy_generation_as(
        &corpus_table("test_basic", "simple_table"),
        "nb-1-big",
        tmp.path(),
        "nb-1-big",
    );
    copy_generation_as(
        &corpus_table("test_oa", "simple_table"),
        "oa-2-big",
        tmp.path(),
        "oa-4-big",
    );
    copy_generation_as(
        &corpus_table("test_da", "simple_table"),
        "da-2-bti",
        tmp.path(),
        "da-6-bti",
    );
    let listing = list(tmp.path(), Scope::Live);
    assert_eq!(
        live_names(&listing),
        vec!["nb-1-big", "oa-4-big", "da-6-bti"]
    );
    assert!(listing.live.iter().all(|g| g.conditions().is_empty()));
    assert_eq!(next_id(tmp.path()), GenerationId::Sequence(7));
}

/// G6.2 and G6.3 on synthetic copies: backups count, snapshots do not.
#[test]
fn next_id_counts_backups_but_not_snapshots() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_generation_as(&src, "nb-1-big", tmp.path(), "nb-3-big");
    copy_generation_as(
        &src,
        "nb-1-big",
        &tmp.path().join("snapshots/before-upgrade"),
        "nb-50-big",
    );
    assert_eq!(next_id(tmp.path()), GenerationId::Sequence(4));
    copy_generation_as(&src, "nb-1-big", &tmp.path().join("backups"), "nb-9-big");
    assert_eq!(next_id(tmp.path()), GenerationId::Sequence(10));
}

/// G6: an id held by an Orphan still counts, so allocation can never reuse it.
#[test]
fn next_id_never_reuses_an_orphans_id() {
    let tmp = tempfile::tempdir().unwrap();
    touch(&tmp.path().join("nb-8-big-Index.db"));
    assert_eq!(next_id(tmp.path()), GenerationId::Sequence(9));
}

/// G6.2 and G6.3 over Cassandra's own backups and snapshots.
#[test]
fn cassandra_written_backups_and_snapshots_allocation() {
    // live nb-1..5, backups nb-3,4, snapshots up to nb-5
    assert_eq!(
        next_id(&fixture_table("snap_backup", "snap_backup")),
        GenerationId::Sequence(6)
    );
    // live max 1, backups max 4, snapshot max 5 does not count: 5, not 2 and not 6
    assert_eq!(
        next_id(&fixture_table("snap_backup_live_low", "snap_backup")),
        GenerationId::Sequence(5)
    );
}

/// G7.1: at the limit, a named refusal and a byte-identical directory.
#[test]
fn allocation_at_the_int_limit_refuses_and_changes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    copy_generation_as(
        &corpus_table("test_basic", "simple_table"),
        "nb-1-big",
        tmp.path(),
        "nb-2147483647-big",
    );
    let before = tree_signature(tmp.path());
    match next_generation_id(tmp.path()) {
        Err(GenerationError::SequenceIdExhausted { highest }) => assert_eq!(highest, 2_147_483_647),
        other => panic!("expected SequenceIdExhausted, got {other:?}"),
    }
    assert_eq!(tree_signature(tmp.path()), before);
}

/// G7.2
#[test]
fn allocation_next_to_the_limit_gives_the_limit() {
    let tmp = tempfile::tempdir().unwrap();
    touch(&tmp.path().join("nb-2147483646-big-Data.db"));
    assert_eq!(next_id(tmp.path()), GenerationId::Sequence(2_147_483_647));
}

/// G8.1, G8.2, G8.4 over Cassandra's own snapshots and backups.
#[test]
fn scopes_list_exactly_one_directory_each() {
    let dir = fixture_table("snap_backup", "snap_backup");
    let live = list(&dir, Scope::Live);
    assert_eq!(
        live_names(&live),
        vec!["nb-1-big", "nb-2-big", "nb-3-big", "nb-4-big", "nb-5-big"]
    );
    assert!(live.live.iter().all(|g| g.directory() == dir));

    let snap = list(&dir, Scope::Snapshot("pre-upgrade".into()));
    assert_eq!(live_names(&snap), vec!["nb-1-big", "nb-2-big"]);
    assert!(snap
        .live
        .iter()
        .all(|g| g.directory() == dir.join("snapshots/pre-upgrade")));
    // manifest.json and schema.cql are in the directory and are accounted for as ignored
    let ignored: Vec<_> = snap.ignored.iter().filter_map(|p| p.file_name()).collect();
    assert!(ignored.iter().any(|n| *n == "manifest.json"));

    let backups = list(&dir, Scope::Backups);
    assert_eq!(live_names(&backups), vec!["nb-3-big", "nb-4-big"]);

    let both = list(&dir, Scope::LiveAndBackups);
    assert_eq!(both.live.len(), 7);

    // G8.4: absent scope directory
    let other = fixture_table("uuid_ids", "uuid_only");
    assert!(list(&other, Scope::Backups).live.is_empty());
    assert!(list(&other, Scope::Snapshot("nope".into())).live.is_empty());
}

/// G8: a tag that is not one directory name is refused, never joined into a path.
#[test]
fn snapshot_tag_cannot_traverse() {
    let tmp = tempfile::tempdir().unwrap();
    for tag in ["", "..", ".", "a/b", "../x"] {
        match list_generations(tmp.path(), &Scope::Snapshot(tag.into()), TocPolicy::Accept) {
            Err(GenerationError::InvalidSnapshotTag { .. }) => {}
            other => panic!("tag {tag:?}: {other:?}"),
        }
    }
}

/// G8.3: a secondary-index subdirectory is not entered.
#[test]
fn secondary_index_subdirectory_is_not_entered() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    copy_dir(&src, &tmp.path().join(".by_email"));
    let listing = list(tmp.path(), Scope::Live);
    assert_eq!(live_names(&listing), vec!["nb-1-big"]);
    assert_eq!(listing.live[0].directory(), tmp.path());
}

/// A missing table directory is an error for the Live scope; it is not an empty table.
#[test]
fn missing_table_directory_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(matches!(
        list_generations(&tmp.path().join("nope"), &Scope::Live, TocPolicy::Accept),
        Err(GenerationError::Io { .. })
    ));
}

/// G13.1
#[test]
fn unrelated_files_are_counted_not_hidden() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    touch(&tmp.path().join(".gitignore"));
    touch(&tmp.path().join("notes.txt"));
    let listing = list(tmp.path(), Scope::Live);
    assert_eq!(listing.live.len(), 1);
    let ignored: Vec<_> = listing
        .ignored
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(ignored, vec![".gitignore", "notes.txt"]);
}

/// G9.1 end to end: a below-floor listing entry refuses its metadata by name and the floor.
#[test]
fn below_floor_generation_is_listed_and_refuses_metadata() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_generation_as(&src, "nb-1-big", tmp.path(), "ma-1-big");
    std::fs::remove_file(tmp.path().join("ma-1-big-Statistics.db")).unwrap();
    let listing = list(tmp.path(), Scope::Live);
    let g = &listing.live[0];
    assert_eq!(g.conditions(), &[Condition::UnsupportedVersion]);
    let failure = g.trusted_metadata().unwrap_err();
    assert_eq!(failure.condition(), Condition::UnsupportedVersion);
    let text = failure.to_string();
    assert!(text.contains("ma") && text.contains("na"), "{text}");
}

/// G11.1: a second request does not touch the disk.
#[test]
fn trusted_metadata_value_is_cached() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    let listing = list(tmp.path(), Scope::Live);
    let g = &listing.live[0];
    let first = g
        .trusted_metadata()
        .expect("Cassandra-written Statistics.db must pass");
    std::fs::remove_file(tmp.path().join("nb-1-big-Statistics.db")).unwrap();
    let second = g
        .trusted_metadata()
        .expect("cached value survives the delete");
    assert!(std::sync::Arc::ptr_eq(&first, &second));
}

/// G11 over every corpus format: BIG `nb`, BIG `oa`, BTI `da` all read.
#[test]
fn trusted_metadata_reads_every_supported_corpus_format() {
    for (ks, table, expected) in CORPUS {
        let listing = list(&corpus_table(ks, table), Scope::Live);
        let g = &listing.live[0];
        assert_eq!(&g.base_name(), expected);
        g.trusted_metadata()
            .unwrap_or_else(|e| panic!("{ks}.{table}: {e}"));
    }
}

/// G11.2 and G12.2: truncated `Statistics.db` and no `TOC.txt` combine; the failure is `Corrupt`,
/// names the path, and is the same on the second request.
#[test]
fn conditions_combine_and_a_corrupt_failure_is_stable() {
    let src = corpus_table("test_basic", "simple_table");
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    std::fs::remove_file(tmp.path().join("nb-1-big-TOC.txt")).unwrap();
    let stats = tmp.path().join("nb-1-big-Statistics.db");
    let len = std::fs::metadata(&stats).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&stats)
        .unwrap()
        .set_len(len / 2)
        .unwrap();

    let listing = list(tmp.path(), Scope::Live);
    let g = &listing.live[0];
    assert_eq!(g.conditions(), &[Condition::TocMissing]);
    let a = g.trusted_metadata().unwrap_err();
    let b = g.trusted_metadata().unwrap_err();
    assert_eq!(a.condition(), Condition::Corrupt);
    assert_eq!(a.to_string(), b.to_string());
    assert!(a.to_string().contains("nb-1-big-Statistics.db"), "{a}");
}

/// G12.1: no wildcard arm, so a seventh variant fails this test to compile.
#[test]
fn condition_set_is_closed() {
    fn name(c: Condition) -> &'static str {
        match c {
            Condition::TocMissing => "TocMissing",
            Condition::Orphan => "Orphan",
            Condition::UnsupportedVersion => "UnsupportedVersion",
            Condition::Unreadable => "Unreadable",
            Condition::Corrupt => "Corrupt",
            Condition::Absent => "Absent",
        }
    }
    assert_eq!(name(Condition::Orphan), "Orphan");
}

/// A 28-byte id token whose multi-byte char straddles the slice offsets of the UUID layout must not
/// panic; a non-ASCII name is not an SSTable (`UUIDBasedSSTableId.java:41`), so it is ignored.
#[test]
fn non_ascii_28_byte_id_is_ignored_without_panicking() {
    let tmp = tempfile::tempdir().unwrap();
    let id = "abcd_efgh_abcd\u{e9}abcdefghijkl";
    assert_eq!(id.len(), 28);
    touch(&tmp.path().join(format!("nb-{id}-big-Data.db")));
    let listing = list(tmp.path(), Scope::Live);
    assert!(listing.live.is_empty() && listing.orphans.is_empty());
    assert_eq!(listing.ignored.len(), 1);
}
