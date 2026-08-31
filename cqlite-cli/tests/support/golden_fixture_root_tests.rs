//! Unit coverage for the AD2 lane's FIXTURE RESOLUTION (issue #1491): which root
//! serves a case, what a committed case may be served from, and every three-valued
//! verdict in between.
//!
//! Split out of `golden_fixture_root.rs` under the campsite rule (CLAUDE.md, epic
//! #1135) when that file crossed the 1500-line test-file threshold; a pure move, so
//! it stays a child module of the code it tests and reaches it through
//! `use super::*`.
//!
//! Each case's expectation comes from the lane's own contract — a committed fixture
//! is the oracle for its committed values and is never served from an external
//! corpus root, and a root that cannot be READ is not a root that verifiably lacks
//! the table — never from whatever the current environment happens to hold.

use super::*;

fn write(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().expect("has a parent")).expect("mkdir");
    std::fs::write(path, bytes).expect("write");
}

fn fixtures(entries: &[(&str, &str, &str, &str)]) -> CommittedFixtures {
    let mut out = CommittedFixtures::new();
    for (ks, tbl, dir, file) in entries {
        out.entry((ks.to_string(), tbl.to_string()))
            .or_default()
            .insert((dir.to_string(), file.to_string()));
    }
    out
}

/// The listing is split on NUL and each path is read STRICTLY: a name that is
/// not valid UTF-8 is refused, not converted lossily.
///
/// The demonstration is in the test: `from_utf8_lossy` maps two DIFFERENT
/// invalid bytes onto the SAME U+FFFD string, so under a lossy read two distinct
/// tracked fixtures become one key — and the committed set keys on those strings,
/// so the second one would silently leave the census instead of being reported.
#[test]
fn a_listing_path_that_is_not_utf8_is_refused_rather_than_read_lossily() {
    let ok = parse_listing(
        b"test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db\0              test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db.jsonl\0",
    )
    .expect("two valid paths");
    assert_eq!(
        ok.len(),
        2,
        "NUL-separated, trailing separator not an element"
    );
    assert!(ok[0].ends_with("nb-1-big-Data.db"), "{ok:?}");

    // Why a lossy read cannot be used HERE: distinct bytes, one string.
    assert_eq!(
        String::from_utf8_lossy(&[0xff]),
        String::from_utf8_lossy(&[0xfe]),
        "two distinct invalid bytes read lossily as the same string"
    );

    for odd in [0xffu8, 0xfe] {
        let mut listing = b"test-data/datasets/sstables/ks/t-abc/nb-1-big".to_vec();
        listing.push(odd);
        listing.extend_from_slice(b"-Data.db\0");
        let why = parse_listing(&listing).expect_err("a non-UTF-8 path must be refused");
        assert!(
            why.contains("not valid UTF-8") && why.contains("census"),
            "the refusal must name the cause and what it protects: {why}"
        );
    }
}

#[test]
fn a_committed_fixture_resolves_under_the_checkout_root() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let checkout = tmp.path().join("checkout");
    write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");
    let committed = fixtures(&[("ks", "t", "t-abc", "nb-1-big-Data.db")]);
    let fixture = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
        .expect("resolves");
    assert_eq!(fixture.dir, checkout.join("ks").join("t-abc"));
    assert_eq!(fixture.of_dirs, 1, "one tracked directory");
    assert_eq!(
        fixture.source,
        RootSource::GitTracked,
        "`git ls-files` established the tracking, so this provenance may claim it"
    );
}

/// T3: the SAME path, found by the evidence walk instead, is NOT reported as the
/// git-committed copy.
///
/// Only a fetched-corpus case reaches that walk, and `resolve_fixture` has
/// already established that git tracks no `*-Data.db` for its table — so calling
/// this "checkout (git-committed)" told a reader the oracle was the committed copy
/// when nothing had established that, and something had established the opposite.
#[test]
fn a_fixture_found_under_the_checkout_root_is_not_reported_as_git_tracked() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let checkout = tmp.path().join("checkout");
    write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");

    let fixture = corpus_fixture_in(&checkout, "ks", "t", &checkout).unwrap_or_else(|e| {
        panic!(
            "the checkout root holds the table, so the walk resolves: {}",
            match e {
                CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => why,
            }
        )
    });
    assert_eq!(fixture.dir, checkout.join("ks").join("t-abc"));
    assert_eq!(
        fixture.source,
        RootSource::CheckoutUntracked,
        "the walk established only that this root HOLDS the table"
    );
}

/// BB2: the checkout root reached by ANOTHER SPELLING is still the checkout.
///
/// Provenance was decided by lexical `Path` equality, so a root that IS the
/// checkout written differently — a relative `CQLITE_DATASETS_ROOT`, a `..`
/// component, a symlink into the checkout — was reported as a `fetched corpus`.
/// The `..` spelling below is that class without needing a symlink (and so
/// without `std::os::unix`, which this lane deliberately does not use).
#[test]
fn the_checkout_root_reached_by_another_spelling_is_not_reported_as_a_corpus() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let checkout = tmp.path().join("checkout");
    write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");
    let detoured = checkout.join("..").join("checkout");
    assert_ne!(
        detoured, checkout,
        "the two spellings are not lexically equal — that is the defect"
    );

    let fixture = corpus_fixture_in(&detoured, "ks", "t", &checkout).unwrap_or_else(|e| {
        panic!(
            "the root holds the table, so the walk resolves: {}",
            match e {
                CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => why,
            }
        )
    });
    assert_eq!(
        fixture.source,
        RootSource::CheckoutUntracked,
        "a root that RESOLVES to the checkout is the checkout, however it is spelled"
    );
}

/// And a genuinely different root is still reported as the fetched corpus, so the
/// resolved comparison did not simply collapse every root onto the checkout.
#[test]
fn a_root_that_resolves_elsewhere_is_still_the_fetched_corpus() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let checkout = tmp.path().join("checkout");
    let corpus = tmp.path().join("corpus");
    write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");
    write(&corpus.join("ks/t-def/nb-1-big-Data.db"), b"x");

    let fixture = corpus_fixture_in(&corpus, "ks", "t", &checkout).expect("the corpus holds it");
    assert_eq!(fixture.dir, corpus.join("ks").join("t-def"));
    assert_eq!(fixture.source, RootSource::Corpus);
}

/// And the three provenances must be tellable apart in a census line, since that
/// line is the only record of which bytes were the oracle. Pinned as a property of
/// the tokens rather than by transcribing them: exactly one may claim git
/// tracking, and no two may read the same.
#[test]
fn every_provenance_has_its_own_census_token_and_only_one_claims_git_tracking() {
    let all = [
        RootSource::GitTracked,
        RootSource::CheckoutUntracked,
        RootSource::Corpus,
    ];
    let tokens: BTreeSet<&str> = all.iter().map(|s| s.as_str()).collect();
    assert_eq!(
        tokens.len(),
        all.len(),
        "two provenances sharing a token is the T3 defect itself: {tokens:?}"
    );
    let claim_tracking: Vec<&str> = all
        .iter()
        .map(|s| s.as_str())
        .filter(|t| t.contains("git-tracked") && !t.contains("NOT git-tracked"))
        .collect();
    assert_eq!(
        claim_tracking,
        vec![RootSource::GitTracked.as_str()],
        "only the `git ls-files`-established provenance may claim git tracking"
    );
}

/// The J1 property: an external copy of a committed table is NOT consulted, so a
/// committed fixture missing from the checkout FAILS instead of silently
/// resolving to the corpus copy.
#[test]
fn a_committed_fixture_absent_from_the_checkout_is_a_failure_not_a_fallback() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let checkout = tmp.path().join("checkout");
    let corpus = tmp.path().join("corpus");
    // The table exists in the external corpus, and only there.
    write(&corpus.join("ks/t-abc/nb-1-big-Data.db"), b"x");
    std::fs::create_dir_all(&checkout).expect("mkdir");
    let committed = fixtures(&[("ks", "t", "t-abc", "nb-1-big-Data.db")]);
    let why = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
        .expect_err("must not fall back to the corpus copy");
    assert!(
        why.contains("missing from the checkout") && why.contains("nb-1-big-Data.db"),
        "the failure must name the git-tracked path: {why}"
    );
    assert!(
        !why.contains(&corpus.display().to_string()),
        "the corpus root is not a candidate for a committed fixture: {why}"
    );
}

#[test]
fn a_case_git_tracks_no_sstable_for_is_a_named_failure() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let why = committed_fixture_dir(None, "ks", "t", tmp.path())
        .expect_err("an untracked table cannot be a committed case");
    assert!(why.contains("tracks no *-Data.db"), "{why}");
}

/// An untracked directory sitting beside the tracked one cannot be chosen: the
/// compared path comes from `git ls-files`, not from a directory scan.
#[test]
fn an_untracked_sibling_directory_cannot_shadow_the_tracked_fixture() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let checkout = tmp.path().join("checkout");
    write(&checkout.join("ks/t-0000/nb-9-big-Data.db"), b"stray");
    write(&checkout.join("ks/t-abc/nb-1-big-Data.db"), b"x");
    let committed = fixtures(&[("ks", "t", "t-abc", "nb-1-big-Data.db")]);
    let fixture = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
        .expect("resolves");
    assert_eq!(fixture.dir, checkout.join("ks").join("t-abc"));
}

/// M3/N4: a corpus root whose keyspace directory cannot be READ is a FAILURE,
/// not an absence. Flattened onto the absence verdict, an unreadable corpus
/// produced a green run labelled "NOT PRESENT".
#[test]
fn a_selected_root_whose_keyspace_cannot_be_read_is_unusable_not_absent() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let root = tmp.path().join("root");
    // A FILE where the keyspace directory belongs: `read_dir` fails, which is the
    // same class of answer as a permission failure and needs no chmod.
    write(&root.join("ks"), b"not a directory");
    let miss = match corpus_fixture_in(&root, "ks", "t", tmp.path()) {
        Err(miss) => miss,
        Ok(_) => panic!("an unreadable keyspace directory cannot resolve"),
    };
    match miss {
        CorpusMiss::Unusable(why) => assert!(
            why.contains("ks.t") && why.contains("cannot be listed"),
            "the failure must name the table and the cause: {why}"
        ),
        CorpusMiss::Absent(why) => {
            panic!("an unreadable corpus must not read as absent: {why}")
        }
    }
}

/// The other side of the same line: a root that IS readable and holds no
/// `<table>-*` directory with a `*-Data.db` is VERIFIABLY absent, so the walk
/// keeps going and the case may legally skip.
///
/// This is not a softening — it is what the corpus really looks like. This
/// repository commits the `test_types` goldens WITHOUT their gitignored
/// binaries, so every checkout carries
/// `test-data/datasets/sstables/test_types/nb_*-<uuid>/` directories holding a
/// `*-Data.db.jsonl` and no `*-Data.db`. Calling that malformed would fail
/// every checkout-only run of the fetched-corpus tier, which is the tier's
/// legal skip. (The old "the root walk and the directory scan disagree"
/// verdict is gone with the second opinion that produced it: there is now ONE
/// scan, and it is three-valued — see [`corpus_fixture_from`].)
#[test]
fn a_readable_root_with_no_data_db_is_absent_and_the_walk_continues() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let jsonl_only = tmp.path().join("jsonl-only");
    write(&jsonl_only.join("ks/t-abc/nb-1-big-Data.db.jsonl"), b"{}");
    let corpus = tmp.path().join("corpus");
    write(&corpus.join("ks/t-abc/nb-1-big-Data.db"), b"x");

    match corpus_fixture_in(&jsonl_only, "ks", "t", tmp.path()) {
        Err(CorpusMiss::Absent(why)) => assert!(
            why.contains("ks.t") && why.contains(&jsonl_only.display().to_string()),
            "the absence must name the root it was verified against: {why}"
        ),
        Err(CorpusMiss::Unusable(why)) => {
            panic!("a readable root without the table is absent, not unusable: {why}")
        }
        Ok(_) => panic!("a directory with no *-Data.db cannot resolve"),
    }

    // …and the walk goes on to the root that does carry it.
    let fixture = corpus_fixture_from(&[jsonl_only, corpus.clone()], "ks", "t", tmp.path())
        .unwrap_or_else(|e| match e {
            CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => {
                panic!("the second root carries the table: {why}")
            }
        });
    assert_eq!(fixture.dir, corpus.join("ks").join("t-abc"));
}

/// N4: an UNREADABLE candidate root FAILS the case and is never walked past —
/// not even when a later root carries the table.
///
/// "I could not tell" is not "there is nothing there". The shared
/// `sstables_root_for_table` answers `Option` and every predicate beneath it
/// collapses a read failure onto `false`, so an inaccessible corpus read as
/// absent and EVERY optional corpus case reported `NOT PRESENT` and passed. This
/// lane's walk therefore asks `super::fs_probe` instead. And falling
/// through would be the same defect wearing a different hat: the unreadable
/// root is the one the walk would have picked, so a later root's answer cannot
/// stand in for it.
#[test]
fn an_unreadable_candidate_root_fails_and_is_not_walked_past() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let broken = tmp.path().join("broken");
    // A FILE where the keyspace directory belongs: `read_dir` fails with a
    // non-NotFound error, which is the same branch a permission failure takes
    // and needs no chmod (a chmod-based case passes vacuously as root).
    write(&broken.join("ks"), b"not a directory");
    let good = tmp.path().join("good");
    write(&good.join("ks/t-abc/nb-1-big-Data.db"), b"x");

    match corpus_fixture_from(&[broken.clone(), good], "ks", "t", tmp.path()) {
        Err(CorpusMiss::Unusable(why)) => {
            assert!(
                why.contains("ks.t") && why.contains(&broken.display().to_string()),
                "the failure must name the table and the root: {why}"
            );
            assert!(
                why.contains("NOT PRESENT"),
                "the failure must say why it is not a skip: {why}"
            );
        }
        Err(CorpusMiss::Absent(why)) => {
            panic!("an unreadable root must not read as absent: {why}")
        }
        Ok(_) => panic!("the walk must not resolve past a root it could not read"),
    }
}

/// V1's own site: a `<table>-*` ENTRY the filesystem cannot DESCRIBE is
/// unusable, and a verified-absent one is still a legal skip.
///
/// `path.is_dir()` stood here and answers `false` for both, so an inaccessible
/// candidate directory was indistinguishable from one that is not there: the
/// root read as verifiably lacking the table and an optional corpus case passed
/// reporting `NOT PRESENT`.
///
/// Both directions are staged with symlinks (`#[cfg(unix)]`) because that is the
/// one way to make `metadata` fail on an entry that IS in the listing without a
/// chmod, which passes vacuously as root: a SELF-REFERENTIAL link cannot be
/// resolved (`ELOOP`), while a DANGLING one resolves to `ENOENT`, which is an
/// answer.
#[cfg(unix)]
#[test]
fn an_undescribable_table_entry_is_unusable_and_an_absent_one_still_skips() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let root = tmp.path().join("root");
    std::fs::create_dir_all(root.join("ks")).expect("mkdir");
    std::os::unix::fs::symlink("t-dangling-target", root.join("ks/t-dangling")).expect("symlink");
    match corpus_fixture_in(&root, "ks", "t", tmp.path()) {
        Err(CorpusMiss::Absent(why)) => assert!(
            why.contains("ks.t") || why.contains("t-"),
            "a dangling link resolves to ENOENT, which IS an answer: {why}"
        ),
        Err(CorpusMiss::Unusable(why)) => {
            panic!("a verified absence must stay a legal skip: {why}")
        }
        Ok(_) => panic!("nothing to resolve"),
    }

    // Self-referential: the entry is in the listing and cannot be described.
    std::os::unix::fs::symlink("t-loop", root.join("ks/t-loop")).expect("symlink");
    match corpus_fixture_in(&root, "ks", "t", tmp.path()) {
        Err(CorpusMiss::Unusable(why)) => {
            assert!(
                why.contains("t-loop") && why.contains("cannot be described"),
                "the failure must name the entry and the cause: {why}"
            );
            assert!(
                why.contains("NOT PRESENT"),
                "and must say why it is not a skip: {why}"
            );
        }
        Err(CorpusMiss::Absent(why)) => {
            panic!("an entry the filesystem could not describe must not read as absent: {why}")
        }
        Ok(_) => panic!("an undescribable candidate cannot resolve"),
    }
}

/// The same three-valued rule ONE LEVEL IN: a fixture directory that cannot be
/// listed is unusable, never "holds no `*-Data.db`".
///
/// `has_data_db` used to end in `.unwrap_or(false)`, which made an unreadable
/// fixture directory a verified absence and skipped the case — the finding's own
/// shape, one level down. Classification is by `ErrorKind::NotFound` alone (in
/// `super::fs_probe`), so a permission failure takes exactly this branch.
#[test]
fn an_unlistable_fixture_directory_is_unusable_not_empty() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let not_a_dir = tmp.path().join("t-abc");
    write(&not_a_dir, b"x");
    match holds_data_db(&not_a_dir, "ks", "t") {
        Err(CorpusMiss::Unusable(why)) => assert!(
            why.contains("cannot read the fixture directory") && why.contains("ks.t"),
            "{why}"
        ),
        Err(CorpusMiss::Absent(why)) => panic!("not an absence: {why}"),
        Ok(answer) => panic!("an unreadable directory cannot answer {answer}"),
    }
    // A directory that has VANISHED did answer, so it is absent, not unusable.
    assert_eq!(
        holds_data_db(&tmp.path().join("gone"), "ks", "t").ok(),
        Some(false)
    );
}

/// The sweep's outermost site: the shared candidate LIST tests
/// `CQLITE_DATASETS_ROOT` with `p.is_dir()`, so a value the filesystem could not
/// describe contributes no candidate and the walk answers from the remaining
/// candidates alone.
///
/// Exercised through the pure form, because mutating the environment would race
/// every other test in this binary. Staged as a path THROUGH a regular file,
/// which cannot be resolved (`ENOTDIR`) — the same branch a permission failure
/// takes, and it needs no chmod (a chmod-based case passes vacuously as root).
#[test]
fn an_unclassifiable_datasets_root_env_value_is_unusable_not_absent() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let file = tmp.path().join("f");
    write(&file, b"not a directory");

    let through = file.join("inside").into_os_string();
    match datasets_root_usable(Some(&through), "ks", "t") {
        Err(CorpusMiss::Unusable(why)) => assert!(
            why.contains("ks.t")
                && why.contains("CQLITE_DATASETS_ROOT")
                && why.contains("cannot be described"),
            "the failure must name the table, the variable and the cause: {why}"
        ),
        Err(CorpusMiss::Absent(why)) => panic!("not a verified absence: {why}"),
        Ok(state) => panic!(
            "a value that could not be classified cannot be waved \
                            through as {state:?}"
        ),
    }
}

/// W1: a value that IS classifiable and is not a corpus is a FAILURE too.
///
/// Every one of these answered — a regular file, a nonexistent path, a directory
/// with no `sstables/`, an `sstables` that is a file — and every one was accepted,
/// because the check asked only whether the filesystem could answer. The shared
/// candidate list then drops the value (not a directory) or walks past it (no
/// `sstables/<keyspace>`), so every corpus-only case in the lane reported
/// `NOT PRESENT` and passed with an explicitly configured corpus contributing
/// nothing. "It is a readable path" is not "it is a corpus".
#[test]
fn a_configured_root_that_is_not_a_corpus_is_unusable_not_a_skip() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let tarball = tmp.path().join("datasets.tar.gz");
    write(&tarball, b"not a corpus");
    let no_sstables = tmp.path().join("parent");
    write(&no_sstables.join("some-other-tree/x"), b"x");
    let sstables_is_a_file = tmp.path().join("weird");
    write(&sstables_is_a_file.join("sstables"), b"not a directory");

    for (label, root, expected) in [
        (
            "a regular file",
            tarball,
            "is a regular file, not a directory",
        ),
        (
            "a path that does not exist",
            tmp.path().join("typo"),
            "names a path that does not exist",
        ),
        ("a directory with no sstables/", no_sstables, "is absent"),
        (
            "an sstables/ that is a file",
            sstables_is_a_file,
            "is a regular file",
        ),
    ] {
        let raw = root.clone().into_os_string();
        match datasets_root_usable(Some(&raw), "ks", "t") {
            Err(CorpusMiss::Unusable(why)) => {
                assert!(
                    why.contains("configured but unusable")
                        && why.contains("ks.t")
                        && why.contains("CQLITE_DATASETS_ROOT")
                        && why.contains(&root.display().to_string())
                        && why.contains(expected),
                    "{label}: the failure must name the table, the variable, the \
                     root and what was wrong with it: {why}"
                );
                assert!(
                    why.contains("NOT PRESENT"),
                    "{label}: and must say why it is not a skip: {why}"
                );
            }
            Err(CorpusMiss::Absent(why)) => {
                panic!("{label}: a configured non-corpus is not a verified absence: {why}")
            }
            Ok(state) => panic!("{label}: must not be accepted as {state:?}"),
        }
    }
}

/// The other side of W1: the two situations that are NOT failures, and each says
/// which one it is.
///
/// An unset or blank variable asked for no corpus, and a real corpus that does not
/// carry the table is the tier's legal skip. Blankness is judged by the SAME test
/// the shared candidate list applies — a trim-empty value contributes no candidate
/// there, so calling it "configured" here would fail a run whose corpus really was
/// unset.
#[test]
fn an_unconfigured_root_and_a_corpus_without_the_table_are_the_two_legal_skips() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    for (label, raw) in [
        ("unset", None),
        ("blank", Some(std::ffi::OsString::from(""))),
        ("whitespace only", Some(std::ffi::OsString::from("  \t "))),
    ] {
        assert_eq!(
            datasets_root_usable(raw.as_deref(), "ks", "t").ok(),
            Some(EnvCorpus::NotConfigured),
            "{label} asked for no fetched corpus, so the case may legally skip"
        );
    }

    let corpus = tmp.path().join("corpus");
    std::fs::create_dir_all(corpus.join("sstables/other_ks")).expect("mkdir");
    let raw = corpus.clone().into_os_string();
    assert_eq!(
        datasets_root_usable(Some(&raw), "ks", "t").ok(),
        Some(EnvCorpus::Corpus {
            sstables: corpus.join("sstables")
        }),
        "a real corpus missing this table is usable — the absence is verified against it"
    );
}

/// And the three situations must be tellable apart in the census line, since that
/// line is what an operator acts on: "not configured", "configured but unusable"
/// and "valid corpus, table absent" call for three different actions.
#[test]
fn the_three_corpus_situations_are_distinguishable_in_the_diagnostic() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let corpus = tmp.path().join("corpus");
    std::fs::create_dir_all(corpus.join("sstables")).expect("mkdir");

    let unconfigured = EnvCorpus::NotConfigured.describe_absence();
    let absent_from_corpus = EnvCorpus::Corpus {
        sstables: corpus.join("sstables"),
    }
    .describe_absence();
    let raw = tmp.path().join("typo").into_os_string();
    let unusable = match datasets_root_usable(Some(&raw), "ks", "t") {
        Err(CorpusMiss::Unusable(why)) => why,
        other => panic!("a configured non-corpus is a failure: {other:?}"),
    };

    assert!(
        unconfigured.contains("not configured"),
        "the unset situation must say so: {unconfigured}"
    );
    assert!(
        absent_from_corpus.contains("valid corpus, table absent")
            && absent_from_corpus.contains(&corpus.join("sstables").display().to_string()),
        "the absent-from-a-real-corpus situation must say so, and name the root: \
         {absent_from_corpus}"
    );
    assert!(
        unusable.contains("configured but unusable"),
        "the unusable situation must say so: {unusable}"
    );
    let phrases = [
        "not configured",
        "configured but unusable",
        "valid corpus, table absent",
    ];
    for (label, message) in [
        ("unconfigured", &unconfigured),
        ("absent from a corpus", &absent_from_corpus),
        ("unusable", &unusable),
    ] {
        let hits: Vec<&str> = phrases
            .iter()
            .copied()
            .filter(|p| message.contains(p))
            .collect();
        assert_eq!(
            hits.len(),
            1,
            "{label}: exactly one situation may be claimed, or a reader cannot tell \
             which it is: {hits:?} in {message}"
        );
    }
}

/// The end-to-end shape of W1 through the walk: an unusable configured root fails
/// the case BEFORE any candidate is consulted, so a corpus-only case cannot skip
/// on an absence the invalid root never established.
#[test]
fn an_unusable_configured_root_fails_before_the_candidate_walk() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let file = tmp.path().join("datasets.tar.gz");
    write(&file, b"not a corpus");
    // The walk itself would have SUCCEEDED from a second root holding the table,
    // so the failure is attributable to the configured root and to nothing else.
    let good = tmp.path().join("good");
    write(&good.join("ks/t-abc/nb-1-big-Data.db"), b"x");
    let raw = file.clone().into_os_string();
    match datasets_root_usable(Some(&raw), "ks", "t") {
        Err(CorpusMiss::Unusable(_)) => {}
        other => panic!("an unusable configured root must fail: {other:?}"),
    }
    let fixture = corpus_fixture_from(std::slice::from_ref(&good), "ks", "t", tmp.path())
        .unwrap_or_else(|e| match e {
            CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => {
                panic!("the candidate walk on its own resolves: {why}")
            }
        });
    assert_eq!(fixture.dir, good.join("ks").join("t-abc"));
}

/// When no candidate root carries the table at all, the verdict is the legal
/// skip and its message names the search.
#[test]
fn no_candidate_root_carrying_the_table_is_the_legal_skip() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let empty = tmp.path().join("empty");
    std::fs::create_dir_all(&empty).expect("mkdir");
    match corpus_fixture_from(&[empty], "ks", "t", tmp.path()) {
        Err(CorpusMiss::Absent(why)) => {
            assert!(why.contains("ks.t"), "the skip must name the table: {why}")
        }
        Err(CorpusMiss::Unusable(why)) => panic!("a verified absence is not a failure: {why}"),
        Ok(_) => panic!("nothing to resolve"),
    }
}

/// And the ordinary shape still resolves, so the two failures above are
/// attributable to what they synthesize rather than to the scaffolding.
#[test]
fn a_usable_corpus_root_resolves_and_reports_its_source() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let root = tmp.path().join("root");
    write(&root.join("ks/t-abc/nb-1-big-Data.db"), b"x");
    let fixture = corpus_fixture_in(&root, "ks", "t", tmp.path()).unwrap_or_else(|e| {
        panic!(
            "a root holding the table must resolve: {}",
            match e {
                CorpusMiss::Absent(why) | CorpusMiss::Unusable(why) => why,
            }
        )
    });
    assert_eq!(fixture.dir, root.join("ks").join("t-abc"));
    assert_eq!(fixture.of_dirs, 1);
    // The checkout passed above is NOT this root, so the source is the corpus.
    assert!(matches!(fixture.source, RootSource::Corpus));
}

/// L3: when git tracks SEVERAL SSTable directories for one table, the first is
/// compared and the COUNT travels with it, so the caller's census can declare
/// how many directories went untested. Without the count the choice is a silent
/// pick of one of N — the property this lane exists to prevent.
#[test]
fn several_tracked_directories_are_counted_not_just_narrowed_to_one() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let checkout = tmp.path().join("checkout");
    write(&checkout.join("ks/t-aaa/nb-1-big-Data.db"), b"x");
    write(&checkout.join("ks/t-bbb/nb-1-big-Data.db"), b"y");
    let committed = fixtures(&[
        ("ks", "t", "t-bbb", "nb-1-big-Data.db"),
        ("ks", "t", "t-aaa", "nb-1-big-Data.db"),
    ]);
    let fixture = committed_fixture_dir(committed.get(&key("ks", "t")), "ks", "t", &checkout)
        .expect("resolves");
    assert_eq!(
        fixture.dir,
        checkout.join("ks").join("t-aaa"),
        "the sorted-first directory is the one compared"
    );
    assert_eq!(
        fixture.of_dirs, 2,
        "both tracked directories must be counted, so the narrowing can be declared"
    );

    // Two SSTables tracked in ONE directory is one directory, not two: that
    // shape is refused by `compare::golden_path`, not counted here.
    let one_dir = fixtures(&[
        ("ks", "u", "u-aaa", "nb-1-big-Data.db"),
        ("ks", "u", "u-aaa", "nb-2-big-Data.db"),
    ]);
    write(&checkout.join("ks/u-aaa/nb-1-big-Data.db"), b"x");
    write(&checkout.join("ks/u-aaa/nb-2-big-Data.db"), b"y");
    let fixture = committed_fixture_dir(one_dir.get(&key("ks", "u")), "ks", "u", &checkout)
        .expect("resolves");
    assert_eq!(fixture.of_dirs, 1);
}

fn key(ks: &str, table: &str) -> (String, String) {
    (ks.to_string(), table.to_string())
}

#[test]
fn classify_reads_the_committed_path_shape() {
    let data = classify("test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db")
        .expect("well-shaped")
        .expect("a fixture path");
    assert_eq!(
        (
            data.keyspace.as_str(),
            data.table.as_str(),
            data.dir.as_str(),
            data.file.as_str(),
            data.is_golden
        ),
        ("ks", "t", "t-abc", "nb-1-big-Data.db", false)
    );
    let golden = classify("test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db.jsonl")
        .expect("well-shaped")
        .expect("a golden path");
    assert!(golden.is_golden);
    assert_eq!(golden.table, "t");
    assert!(
        classify("test-data/datasets/sstables/ks/t-abc/nb-1-big-Index.db")
            .expect("well-shaped")
            .is_none()
    );
}

#[test]
fn classify_refuses_an_unrecognised_shape_rather_than_guessing() {
    for line in [
        "test-data/datasets/sstables/ks/nb-1-big-Data.db",
        "test-data/datasets/sstables/ks/t-abc/deeper/nb-1-big-Data.db",
        "test-data/datasets/sstables/ks/tabc/nb-1-big-Data.db",
        "elsewhere/datasets/sstables/ks/t-abc/nb-1-big-Data.db",
    ] {
        assert!(
            classify(line).is_err(),
            "an unrecognised committed path must be refused: {line}"
        );
    }
}

#[test]
fn committed_fixtures_keeps_the_sstables_and_drops_the_goldens() {
    let listing: Vec<String> = [
        "test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db",
        "test-data/datasets/sstables/ks/t-abc/nb-1-big-Data.db.jsonl",
        "test-data/datasets/sstables/ks/t-abc/nb-1-big-Statistics.db",
        "test-data/datasets/sstables/ks/u-def/nb-2-big-Data.db",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let out = committed_fixtures(&listing).expect("well-shaped listing");
    assert_eq!(out.len(), 2);
    assert_eq!(
        out.get(&key("ks", "t")).map(|s| s.len()),
        Some(1),
        "the golden must not add a second SSTable entry"
    );
}

/// `git ls-files` is the real subject: the repository must actually track the
/// committed fixtures this lane's committed tier depends on.
#[test]
fn the_repository_tracks_committed_fixtures_under_the_checkout_root() {
    let listing = committed_listing().expect("git ls-files");
    let committed = committed_fixtures(&listing).expect("well-shaped listing");
    assert!(
        !committed.is_empty(),
        "no git-tracked *-Data.db under test-data/datasets/sstables — the committed \
         tier would have no subject"
    );
    let checkout = checkout_sstables_root();
    for ((ks, table), sstables) in &committed {
        committed_fixture_dir(Some(sstables), ks, table, &checkout)
            .unwrap_or_else(|why| panic!("{ks}.{table}: {why}"));
    }
}
