//! Issue #4626 phase 2 (tasks 2.2 to 2.4; closes #4622 and #4623): `cqlite compact` resolves its
//! inputs through the generation module and never publishes onto an existing SSTable.
//!
//! Every case drives the BUILT BINARY. Oracles: Cassandra 5.0.8-written fixtures
//! (`test-data/fixtures/issue_4626`: `snap_backup`, `snap_backup_live_low`, `uuid_ids`) and the
//! committed corpus (`oa`, BTI `da`) via `sstables_root_for_table`. Layouts are tempdir COPIES; the
//! shared fixtures and corpus are never mutated, and no SSTable byte is hand-written.

#![cfg(feature = "write-support")]

#[path = "../../cqlite-core/tests/support/datasets_root.rs"]
mod datasets_root;
#[path = "../../cqlite-core/tests/support/generation_fixtures.rs"]
mod generation_fixtures;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use generation_fixtures::*;
use tempfile::TempDir;

const SCHEMA: &str = "CREATE TABLE gd4626.snap_backup (id int PRIMARY KEY, v text);\n";

fn compact(input: &Path, out_root: &Path, schema: &Path, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .arg("compact")
        .arg(input)
        .arg("--output")
        .arg(out_root)
        .arg("--schema")
        .arg(schema)
        .args(extra)
        .output()
        .expect("spawn cqlite")
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn schema_file(dir: &Path) -> PathBuf {
    let p = dir.join("schema.cql");
    std::fs::write(&p, SCHEMA).unwrap();
    p
}

/// Every file under `dir` with its full content: the byte-identity witness for "nothing written".
fn content_signature(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                let rel = p.strip_prefix(base).unwrap().to_string_lossy().into_owned();
                out.push((rel, std::fs::read(&p).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// `<root>/gd4626/snap_backup`: the directory compact publishes into, seeded from a fixture.
fn seeded_table(root: &Path, state: &str) -> PathBuf {
    let dir = root.join("gd4626").join("snap_backup");
    copy_dir(&fixture_table(state, "snap_backup"), &dir);
    dir
}

/// #4622 / G8.1: the input directory has live `nb-1..5`, `backups/` (nb-3, nb-4) and
/// `snapshots/{later,pre-upgrade}`. Only the five live generations are compacted, and the result
/// equals compacting a copy that has no `backups/` or `snapshots/` at all.
#[test]
fn compact_never_reads_snapshots_or_backups() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path());
    let full = seeded_table(&tmp.path().join("in_full"), "snap_backup");
    assert!(full.join("backups").is_dir() && full.join("snapshots").is_dir());
    let stripped = seeded_table(&tmp.path().join("in_stripped"), "snap_backup");
    std::fs::remove_dir_all(stripped.join("backups")).unwrap();
    std::fs::remove_dir_all(stripped.join("snapshots")).unwrap();
    let before = content_signature(&full);

    let out_full = tmp.path().join("out_full");
    let o = compact(&full, &out_full, &schema, &[]);
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(
        stdout(&o).contains("compacted 5 SSTable(s)"),
        "{}",
        stdout(&o)
    );
    assert_eq!(
        before,
        content_signature(&full),
        "compact modified its input"
    );

    let out_stripped = tmp.path().join("out_stripped");
    let o2 = compact(&stripped, &out_stripped, &schema, &[]);
    assert!(o2.status.success(), "{}", stderr(&o2));
    let data = |root: &Path| {
        std::fs::read(root.join("gd4626/snap_backup/nb-1-big-Data.db")).expect("output Data.db")
    };
    assert_eq!(data(&out_full), data(&out_stripped));
}

/// G3.2 / G10 (owner Q43): a generation without TOC.txt is refused by name, and nothing is written.
#[test]
fn compact_refuses_a_toc_missing_generation_by_name() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path());
    let dir = seeded_table(&tmp.path().join("in"), "snap_backup");
    std::fs::remove_file(dir.join("nb-2-big-TOC.txt")).unwrap();
    let out = tmp.path().join("out");
    let o = compact(&dir, &out, &schema, &[]);
    assert!(!o.status.success());
    let err = stderr(&o);
    assert!(err.contains("nb-2-big") && err.contains("TOC.txt"), "{err}");
    assert!(!out.exists(), "nothing may be created on refusal");
}

/// G10.1: a generation compact cannot handle is refused by name and the run does not proceed with
/// the generations it can. A UUID id (Cassandra-written), an `oa` BIG and a BTI `da` generation
/// (corpus), each beside real `nb` generations.
#[test]
fn compact_refuses_unsupported_formats_and_id_forms_by_name() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path());
    let oa = corpus_table("test_oa", "simple_table");
    let da = corpus_table("test_da", "simple_table");
    let uuid_dir = fixture_table("uuid_ids", "uuid_mixed");
    let cases: [(&str, PathBuf, &str, &str); 3] = [
        (
            "uuid",
            uuid_dir,
            "nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big",
            "UUID",
        ),
        ("oa", oa, "oa-2-big", "unsupported version oa"),
        ("da", da, "da-2-bti", "unsupported format bti"),
    ];
    for (label, src, refused, why) in cases {
        let dir = tmp
            .path()
            .join(format!("in_{label}"))
            .join("gd4626")
            .join("snap_backup");
        if label == "uuid" {
            copy_dir(&src, &dir);
        } else {
            // a real nb generation beside the foreign one
            copy_dir(&fixture_table("snap_backup_live_low", "snap_backup"), &dir);
            let base = if label == "oa" {
                "oa-2-big"
            } else {
                "da-2-bti"
            };
            copy_generation_as(&src, base, &dir, base);
        }
        let out = tmp.path().join(format!("out_{label}"));
        let o = compact(&dir, &out, &schema, &[]);
        assert!(!o.status.success(), "{label}: {}", stdout(&o));
        let err = stderr(&o);
        assert!(err.contains(refused) && err.contains(why), "{label}: {err}");
        assert!(!out.exists(), "{label}: nothing may be created on refusal");
    }
}

/// #4623: `--output` is the root whose `gd4626/snap_backup` is the LIVE table directory holding
/// `nb-1-big`. Without `--generation` the old default (1) overwrote it; now the id is the next one
/// (live max 1, backups max 4, so 5), and every existing byte is unchanged.
#[test]
fn default_generation_never_overwrites_the_live_directory() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path());
    let root = tmp.path().join("root");
    let live = seeded_table(&root, "snap_backup_live_low");
    let before = content_signature(&live);

    let o = compact(&live, &root, &schema, &[]);
    assert!(o.status.success(), "{}", stderr(&o));
    let after = content_signature(&live);
    for entry in &before {
        assert!(after.contains(entry), "{} changed", entry.0);
    }
    assert!(live.join("nb-5-big-Data.db").is_file(), "next id is 5");
    assert!(live.join("nb-5-big-TOC.txt").is_file());
    assert!(
        after.iter().all(|(n, _)| !n.contains(".cqlite-publish")),
        "no stage left behind"
    );
}

/// #4623: an explicit `--generation 1` onto an existing `nb-1-big` is refused with a named error
/// and the directory is byte-identical afterwards. Also ids outside the Java int range.
#[test]
fn explicit_generation_onto_an_existing_name_is_refused() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path());
    let root = tmp.path().join("root");
    let live = seeded_table(&root, "snap_backup_live_low");
    let before = content_signature(&root);

    let o = compact(&live, &root, &schema, &["--generation", "1"]);
    assert!(!o.status.success());
    let err = stderr(&o);
    assert!(
        err.contains("nb-1-big") && err.contains("already in use"),
        "{err}"
    );
    assert_eq!(before, content_signature(&root), "refusal wrote something");

    let o = compact(&live, &root, &schema, &["--generation", "2147483648"]);
    assert!(!o.status.success());
    assert!(stderr(&o).contains("2147483648"), "{}", stderr(&o));
    assert_eq!(before, content_signature(&root), "refusal wrote something");
}

/// An explicit id that is free is honoured, and a fresh output root defaults to 1.
#[test]
fn explicit_free_generation_and_fresh_default() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path());
    let input = seeded_table(&tmp.path().join("in"), "snap_backup_live_low");

    let fresh = tmp.path().join("fresh");
    let o = compact(&input, &fresh, &schema, &[]);
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(fresh.join("gd4626/snap_backup/nb-1-big-Data.db").is_file());

    let seven = tmp.path().join("seven");
    let o = compact(&input, &seven, &schema, &["--generation", "7"]);
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(seven.join("gd4626/snap_backup/nb-7-big-Data.db").is_file());
}

/// 2.4: `--help` states the new default.
#[test]
fn help_states_the_generation_default() {
    let o = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(["compact", "--help"])
        .output()
        .expect("spawn cqlite");
    let text = stdout(&o).replace('\n', " ");
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flat.contains("one above the highest generation id") && flat.contains("never overwrites"),
        "{flat}"
    );
}
