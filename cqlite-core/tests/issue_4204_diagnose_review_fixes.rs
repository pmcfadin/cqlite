//! Issue #4204 roborev round 1: UUID-based sstable ids keep their real label, an unreadable
//! endpoint token makes the token-overlap histogram `unmeasured` (never a partial count), an
//! out-of-range `gcBefore` is an error, and the dry-compaction prediction costs ONE merger pass
//! (its `rows_in` is the deep scan's unpurged count).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::diagnose::{diagnose_table, DiagnoseOptions, SourcedField};

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn fixture(ks: &str, table: &str) -> Option<PathBuf> {
    match datasets_root::resolve_table_generation_dir(ks, table) {
        Ok(d) => Some(d),
        Err(e) if strict() => panic!("{ks}.{table}: missing under CQLITE_REQUIRE_FIXTURES: {e}"),
        Err(e) => {
            eprintln!("SKIP {ks}.{table}: {e}");
            None
        }
    }
}

fn opts() -> DiagnoseOptions {
    DiagnoseOptions {
        now_secs: 1_794_545_000,
        deep: false,
        top_n: 5,
        gc_grace_seconds: Some(864_000),
        schema: None,
    }
}

/// Copy the first generation of `src` into `dst`, renaming its sstable id to `new_id`
/// (`<ver>-<id>-<fmt>-<Component>`), Data.db emptied (the cheap tier never reads it).
fn copy_generation_as(src: &Path, dst: &Path, new_id: &str, skip_suffix: Option<&str>) {
    let data = std::fs::read_dir(src)
        .expect("dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with("-Data.db"))
        .min()
        .expect("Data.db");
    let prefix = data.trim_end_matches("Data.db").to_string();
    for e in std::fs::read_dir(src).expect("dir").flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !e.path().is_file() || name.ends_with(".jsonl") || !name.starts_with(&prefix) {
            continue;
        }
        if skip_suffix.is_some_and(|s| name.ends_with(s)) {
            continue;
        }
        let mut it = name.splitn(4, '-');
        let (v, _id, f, c) = (
            it.next().expect("v"),
            it.next(),
            it.next().expect("f"),
            it.next().expect("c"),
        );
        let dest = dst.join(format!("{v}-{new_id}-{f}-{c}"));
        if c == "Data.db" {
            std::fs::write(dest, b"").expect("empty");
        } else {
            std::fs::copy(e.path(), dest).expect("copy");
        }
    }
}

/// UUID-based ids (Cassandra 5.0.8 `UUIDBasedSSTableId`: `<4 base-36>_<4>_<18>` chars) are
/// reported verbatim and distinctly — never collapsed to one `0` label.
#[tokio::test]
async fn uuid_based_sstable_ids_are_reported_verbatim_and_distinct() {
    let Some(src) = fixture("test_basic", "simple_table") else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let ids = [
        "3gkx_1f1t_2z5ah8vjn8sd9d8sjh",
        "3gkx_1f1u_0abcdefghijklmnopq",
        "7",
    ];
    for id in ids {
        copy_generation_as(&src, tmp.path(), id, None);
    }
    let report = diagnose_table(tmp.path(), &opts()).await.expect("diagnose");
    assert!(
        report.unreadable_generations.is_empty(),
        "{:?}",
        report.unreadable_generations
    );
    let mut got: Vec<&str> = report
        .generations
        .iter()
        .map(|g| g.generation.as_str())
        .collect();
    got.sort();
    let mut want = ids.to_vec();
    want.sort();
    assert_eq!(got, want, "every generation keeps its real on-disk id");
}

/// A generation whose endpoint tokens cannot be read makes the WHOLE histogram `unmeasured`,
/// naming that generation; with both readable it is measured.
#[tokio::test]
async fn unreadable_endpoint_tokens_make_token_overlap_unmeasured() {
    let (Some(a), Some(b)) = (
        fixture("test_basic", "simple_table"),
        fixture("test_tomb", "skipped_partition_delete"),
    ) else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    copy_generation_as(&a, tmp.path(), "1", None);
    copy_generation_as(&b, tmp.path(), "2", None);
    let ok = diagnose_table(tmp.path(), &opts()).await.expect("diagnose");
    assert!(
        ok.token_overlap.counts.is_measured(),
        "both Summary.db present → measured"
    );

    std::fs::remove_file(
        std::fs::read_dir(tmp.path())
            .expect("dir")
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.to_string_lossy().contains("-2-") && p.to_string_lossy().ends_with("Summary.db")
            })
            .expect("generation 2 Summary.db"),
    )
    .expect("remove");
    let r = diagnose_table(tmp.path(), &opts()).await.expect("diagnose");
    match &r.token_overlap.counts {
        SourcedField::Unmeasured { cause } => {
            assert!(
                cause.contains('2') && cause.contains("undercount"),
                "cause must name the generation: {cause}"
            );
        }
        other => panic!("expected unmeasured token overlap, got {other:?}"),
    }
    assert_eq!(
        r.generations.len(),
        2,
        "the cheap-tier report itself is unaffected"
    );
}

/// `now - gc_grace` outside i64 is an error, not a wrapped cutoff; a negative gc_grace stays valid.
#[tokio::test]
async fn out_of_range_gc_before_is_an_error() {
    let Some(src) = fixture("test_basic", "simple_table") else {
        return;
    };
    let mut o = opts();
    o.now_secs = i64::MIN;
    o.gc_grace_seconds = Some(1);
    assert!(
        diagnose_table(&src, &o).await.is_err(),
        "i64::MIN - 1 must not wrap"
    );
    o.gc_grace_seconds = None; // the 10-day default overflows too
    assert!(diagnose_table(&src, &o).await.is_err());
    o.gc_grace_seconds = Some(-1); // purging disabled: no subtraction, a valid report
    assert!(diagnose_table(&src, &o).await.is_ok());
}
