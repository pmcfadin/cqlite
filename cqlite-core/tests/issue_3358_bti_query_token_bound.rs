//! Issue #3358: `stream_all_partitions_for_query` must narrow to its
//! `token_bound` on a BTI (`da`) reader, as it already does on an `nb` one.
//!
//! `ScanTokenBound::contains` is called in one place, the Summary-guided walk in
//! `summary_scan/mod.rs`, and `stream_all_partitions_for_query` gates that walk on
//! `self.index_reader.is_some() && self.bti_partitions_db.is_none() &&
//! summary_usable`. Every BTI generation has a `Partitions.db`, so the second term
//! is false for every `da` reader on every call: the "full-ring fallback" below the
//! gate is not a fallback for that format but its only route. Both routes there
//! (`stream_all_partitions_for_compaction` and `stream_all_partitions_cancellable`)
//! take no bound in their signatures, so the caller's range was accepted and
//! dropped, with no error and no WARN, and the call returned the whole ring.
//!
//! The asymmetry is what makes this a defect rather than a documented pushdown
//! hint. `issue_2412_wraparound_scan.rs` drives the SAME public surface over an
//! `nb` generation and pins its narrowing with `assert_eq!`: "a non-wraparound
//! range must emit exactly its one contiguous segment". So one parameter meant two
//! different things depending on the file format underneath, and a consumer that
//! splits a scan by token range read every row once per split.
//!
//! Why no existing test caught it: every token-bound test builds its generation
//! with `WriteEngine`, which writes `Summary.db`/`Index.db` and never a BTI
//! generation. This test reads a COMMITTED, Cassandra-5.0.2-written `da` fixture
//! instead, so the route under test is the one a Cassandra 5 node's files take.
//!
//! # Oracle
//!
//! The expected partitions are derived, never assumed. `test_da`'s
//! `wide_multiclustering_small` fixture is 600 rows over 5 single-`int`-PK
//! partitions, and its committed `sstabledump -l` golden records both numbers; the
//! golden is re-read here, so a regenerated corpus fails loudly rather than
//! quietly weakening the assertions. Each bound's expected set is then computed
//! from `cassandra_murmur3_token` over the fixture's actual keys, and every case
//! asserts that the bound genuinely excludes at least one partition — otherwise
//! "the whole ring" and "the range" would be the same answer and the test could
//! not discriminate.
//!
//! This fixture is COMMITTED SOURCE, not part of the gitignored fetched corpus:
//! `git ls-files` lists its `da-1-bti-*` components and `git check-ignore` does not
//! match them, so it is present in every complete checkout and appears in a fresh
//! `git worktree add` without any fetch. Absence therefore means a BROKEN CHECKOUT,
//! never an expected condition — so every case here is `must_run` and fails CLOSED
//! (issue #3220). It deliberately does NOT skip: this file is the only end-to-end
//! pin of #3358, a silent-wrong-data defect, and a skip would leave a green suite
//! that certified nothing. The reachable way to lose the file is real and already
//! documented — #3310 added reporting for git-tracked fixtures a SIGKILLed fetch
//! left deleted — which is exactly the case a skip would swallow.
//!
//! `CQLITE_DATASETS_ROOT` is honored first, then the in-repo `test-data/datasets`
//! corpus; EVERY candidate root is walked for this TABLE's own `Data.db` rather than
//! committing to a root by keyspace (issue #3220), because neither root is a superset
//! — the root `fetch-datasets.sh` prints does not carry this fixture and the checkout
//! does.

#![cfg(feature = "state_machine")]

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::{SSTableReader, ScanTokenBound};
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::Config;

const KEYSPACE: &str = "test_da";
const TABLE: &str = "wide_multiclustering_small";
const SSTABLE_PREFIX: &str = "da-1-bti";

/// Repo root = the parent of this crate's manifest dir (`<repo>/cqlite-core`).
/// Single-quote a path for safe pasting into a shell.
///
/// Checkout paths on this fleet contain no spaces today, but a remedy is copied by
/// hand into a terminal, so a path holding a space or a shell metacharacter would
/// silently run a different command. Single quotes disable all expansion; an embedded
/// single quote is closed, escaped and reopened, the standard POSIX form.
fn shell_quote(p: &Path) -> String {
    format!("'{}'", p.display().to_string().replace('\'', "'\\''"))
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent repo dir")
        .to_path_buf()
}

/// The `<table>-<cfid>` generation dir, requiring a real `Data.db` so a JSONL-only
/// root is passed over rather than yielding zero rows. `None` means no candidate root
/// held it, which [`fixture`] turns into a hard failure (this fixture is committed).
fn fixture_dir() -> Option<PathBuf> {
    let roots = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .into_iter()
        .chain(std::iter::once(repo_root().join("test-data/datasets")));
    for root in roots {
        let keyspace_dir = root.join("sstables").join(KEYSPACE);
        let Ok(entries) = std::fs::read_dir(&keyspace_dir) else {
            continue;
        };
        let mut candidates: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
            })
            .collect();
        candidates.sort();
        if let Some(dir) = candidates
            .into_iter()
            .find(|p| p.join(format!("{SSTABLE_PREFIX}-Data.db")).is_file())
        {
            return Some(dir);
        }
    }
    None
}

/// The DDL of the fixture, as `test-data/schemas/wide-multiclustering-small-bti.cql`
/// declares it. A single `int` partition key, so a partition is labelled here by
/// the 4-byte big-endian value its raw key holds.
fn schema() -> TableSchema {
    let cql = format!(
        "CREATE TABLE {KEYSPACE}.{TABLE} (\
             pk int, bucket text, seq int, payload text, \
             PRIMARY KEY (pk, bucket, seq));"
    );
    parse_cql_schema(&cql).expect("parse the fixture schema")
}

/// Rows per partition, as Cassandra's own `sstabledump -l` recorded them.
///
/// The oracle is a dump of Cassandra-written bytes, never CQLite output (#3042).
/// One line of the golden is one PARTITION and each `"type":"row"` within it one
/// row, so the two are counted separately: a range that emits the right partitions
/// with the wrong row counts is still wrong.
fn golden_rows_by_pk(dir: &Path) -> BTreeMap<i32, usize> {
    let golden = dir.join(format!("{SSTABLE_PREFIX}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&golden)
        .unwrap_or_else(|e| panic!("read golden {}: {e}", golden.display()));
    let mut by_pk = BTreeMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        // `"key":["3"]` — one component, the `int` PK rendered as decimal text.
        let marker = "\"key\":[\"";
        let start = line
            .find(marker)
            .unwrap_or_else(|| panic!("golden line carries a partition key: {line}"))
            + marker.len();
        let end = start
            + line[start..]
                .find('"')
                .unwrap_or_else(|| panic!("golden partition key is quoted: {line}"));
        let pk: i32 = line[start..end].parse().unwrap_or_else(|e| {
            panic!("golden partition key {} is an int: {e}", &line[start..end])
        });
        let rows = line.matches("\"type\":\"row\"").count();
        assert!(rows > 0, "golden partition {pk} must hold at least one row");
        assert!(
            by_pk.insert(pk, rows).is_none(),
            "golden must record partition {pk} once"
        );
    }
    assert!(
        by_pk.len() > 2,
        "the fixture needs three or more partitions for a range to exclude one; \
         the golden records {}",
        by_pk.len()
    );
    by_pk
}

async fn open_reader(data_db: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(data_db, &config, platform)
        .await
        .expect("open the BTI SSTable reader");
    assert!(
        reader.is_bti(),
        "fixture {} must open as a BTI (`da`) reader, else the dropped bound is \
         never exercised",
        data_db.display()
    );
    reader
}

/// Drive `stream_all_partitions_for_query` and count the rows it emits per
/// partition key.
///
/// This is the public surface a consumer that splits a scan by token range calls
/// (`write_engine::merge::from_readers::drive_query_stream` reaches it through
/// `KWayMerger::new_from_readers`), and the same one
/// `issue_2412_wraparound_scan.rs` drives for an `nb` generation.
async fn emitted_rows_by_pk(
    reader: &SSTableReader,
    token_bound: Option<ScanTokenBound>,
) -> BTreeMap<i32, usize> {
    let schema = schema();
    let cancel = ScanCancel::default();
    let mut by_pk: BTreeMap<i32, usize> = BTreeMap::new();
    reader
        .stream_all_partitions_for_query(Some(&schema), &cancel, token_bound, |row| {
            let key_bytes: &[u8] = &row.key.0;
            let pk = i32::from_be_bytes(key_bytes.try_into().expect("4-byte int PK"));
            *by_pk.entry(pk).or_default() += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect("stream_all_partitions_for_query");
    by_pk
}

/// The fixture, as the golden describes it.
struct Fixture {
    data_db: PathBuf,
    /// `(partition key, rows)` in ascending token order, which is the order the
    /// ring is divided in and so the order a range is chosen from.
    ring: Vec<(i32, usize)>,
    /// The same counts keyed by partition, for comparing against a whole scan.
    golden: BTreeMap<i32, usize>,
}

/// The fixture's partitions in ring order, with the golden's row count each.
///
/// PANICS when the fixture is absent — it is committed source, so absence is a broken
/// checkout and #3220 requires `must_run`, fail-closed unconditionally. There is no
/// opt-out env var and none may be added: committed source in a checkout is never
/// legitimately absent, so an escape hatch could only buy a vacuous green.
fn fixture() -> Fixture {
    let dir = fixture_dir().unwrap_or_else(|| {
        // DELIBERATELY NOT a synthesized `git restore` command. Five review rounds
        // produced five different defects in one hand-built remedy string — an
        // unsubstituted `<cfid>`, shell-redirection from the `<`, crate-relative paths
        // that resolve to nothing when run from `cqlite-core/`, a glob that would
        // restore the WHOLE fixture directory and silently discard a reader's unrelated
        // uncommitted changes, and a repo-root anchor that still fails when pasted
        // outside the checkout. The variant list was not closing.
        //
        // `fetch-datasets.sh --verify-only` already emits a precise, safe restore
        // command for exactly this case (#3310: it names git-tracked fixtures a
        // SIGKILLed fetch deleted). Pointing at the tested emitter is strictly better
        // than reproducing it here badly, so this message names ONE command and lets
        // that tool produce the restore line. Do not "helpfully" add one back.
        //
        // BOTH values in the emitted command are ABSOLUTE, and both are load-bearing.
        //
        // The probe is scoped to the root it is given. On a fleet box
        // CQLITE_DATASETS_ROOT points at a machine-local corpus OUTSIDE any git work
        // tree, where it correctly reports `NO SUBJECT` and exits 0 — measured: it finds
        // none of the 10 deleted files. So the caller's value must not be inherited.
        //
        // Unsetting it is NOT enough, which cost a review round: the script then falls
        // back to a CWD-RELATIVE `test-data/datasets`, and `cargo test -p cqlite-core`
        // is routinely run from `cqlite-core/`, where that names nothing and the command
        // silently reports no missing fixtures. Measured from that directory: the
        // unset form found none of the 10; this form finds all 10.
        //
        // Passing the absolute checkout corpus makes the command independent of both the
        // caller's environment and the caller's directory. A remedy that is correct only
        // from the repository root is a remedy that gets pasted from somewhere else and
        // quietly reads as an all-clear — the exact silent-pass this test exists to stop.
        let root = repo_root();
        let verify_cmd = shell_quote(&root.join("test-data/scripts/fetch-datasets.sh"));
        let verify_root = shell_quote(&root.join("test-data/datasets"));
        let default_root = root.join("test-data/datasets").display().to_string();
        panic!(
            "{KEYSPACE}.{TABLE} `{SSTABLE_PREFIX}-Data.db` was not found under any \
             candidate dataset root (CQLITE_DATASETS_ROOT, then {default_root}).\n\
             \n\
             This fixture is COMMITTED SOURCE (git-tracked, not gitignored), so this \
             is a broken checkout rather than a missing optional corpus, and it fails \
             closed rather than skipping: this file is the only end-to-end pin of \
             #3358 and a skip would leave a green suite that certified nothing \
             (issue #3220).\n\
             \n\
             Remedy: a SIGKILLed `fetch-datasets.sh` can delete tracked files \
             (#3310). Run:\n\
             \n\
             \x20   CQLITE_DATASETS_ROOT={verify_root} bash {verify_cmd} --verify-only\n\
             \n\
             It names every git-tracked fixture that is missing and prints the exact, \
             correctly-scoped restore command for them — which is why this message \
             does not try to construct one."
        )
    });
    let golden = golden_rows_by_pk(&dir);
    let mut by_token: Vec<(i32, i64, usize)> = golden
        .iter()
        .map(|(&pk, &rows)| (pk, cassandra_murmur3_token(&pk.to_be_bytes()), rows))
        .collect();
    by_token.sort_by_key(|(_, token, _)| *token);
    let ring: Vec<(i32, usize)> = by_token.iter().map(|(pk, _, rows)| (*pk, *rows)).collect();
    Fixture {
        data_db: dir.join(format!("{SSTABLE_PREFIX}-Data.db")),
        ring,
        golden,
    }
}

fn token_of(pk: i32) -> i64 {
    cassandra_murmur3_token(&pk.to_be_bytes())
}

/// Control: the surface reads the whole fixture when no range is given, so a
/// narrowed answer below is a narrowing rather than a failure to read.
#[test]
fn unbounded_scan_matches_the_golden() {
    let Fixture {
        data_db, golden, ..
    } = fixture();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = rt.block_on(open_reader(&data_db));
    let got = rt.block_on(emitted_rows_by_pk(&reader, None));
    assert_eq!(
        got, golden,
        "an unbounded scan must emit every partition of the fixture, with the row \
         counts Cassandra's own dump records"
    );
}

/// THE pin: a non-wraparound range must emit exactly its one contiguous segment,
/// which is what the `nb` surface already guarantees.
///
/// Before the fix this returned all five partitions and all 600 rows for any
/// range, so N consumers splitting the ring between them each read the whole
/// table.
#[test]
fn non_wraparound_range_emits_only_its_segment() {
    let Fixture { data_db, ring, .. } = fixture();
    // A mid-ring segment: every partition above the lowest and below the highest.
    // The two it excludes are what makes the case discriminating.
    let last = ring.len() - 1;
    let start_excl = token_of(ring[0].0);
    let end_incl = token_of(ring[last - 1].0);
    assert!(
        start_excl < end_incl,
        "a non-wraparound pair must have start < end; got {start_excl} and {end_incl}"
    );
    let expected: BTreeMap<i32, usize> = ring[1..last].iter().copied().collect();
    assert!(
        !expected.is_empty() && expected.len() < ring.len(),
        "the range must hold something and exclude something, else it cannot \
         discriminate a narrowed scan from a full one"
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = rt.block_on(open_reader(&data_db));
    let bound = ScanTokenBound {
        start_excl,
        end_incl,
    };
    let got = rt.block_on(emitted_rows_by_pk(&reader, Some(bound)));

    assert_eq!(
        got,
        expected,
        "a non-wraparound range over a BTI (`da`) generation must emit exactly its \
         one contiguous segment ({} of {} partitions), as the `nb` surface does \
         (issue_2412_wraparound_scan.rs). Emitting all {} means the bound reached \
         the full-ring fallback and was dropped there",
        expected.len(),
        ring.len(),
        ring.len()
    );
}

/// The wraparound arm: both segments, and nothing between them.
///
/// `can_stop_past` refuses to early-stop on a wraparound range, so this also pins
/// that the fix takes no early exit it is not entitled to.
#[test]
fn wraparound_range_emits_both_segments_and_nothing_between() {
    let Fixture { data_db, ring, .. } = fixture();
    // `(highest-but-one, MAX] ∪ [MIN, lowest]`: the highest partition and the
    // lowest one, with everything between them excluded.
    let last = ring.len() - 1;
    let start_excl = token_of(ring[last - 1].0);
    let end_incl = token_of(ring[0].0);
    assert!(
        start_excl > end_incl,
        "a wraparound pair must have start > end; got {start_excl} and {end_incl}"
    );
    let expected: BTreeMap<i32, usize> = [ring[0], ring[last]].into_iter().collect();
    assert!(
        expected.len() == 2 && ring.len() > 3,
        "both segments must hold a partition, and at least one partition must lie \
         between them"
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = rt.block_on(open_reader(&data_db));
    let bound = ScanTokenBound {
        start_excl,
        end_incl,
    };
    let got = rt.block_on(emitted_rows_by_pk(&reader, Some(bound)));

    assert_eq!(
        got, expected,
        "a wraparound range must emit the partitions of BOTH segments and none of \
         those between them"
    );
}
