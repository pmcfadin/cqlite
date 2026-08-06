//! ARM A MUST INGEST **EXACTLY** THE MEASURED TABLE DIRECTORY (issue #3272 round 10, F-B).
//!
//! # The finding
//!
//! `ws0-scan-bench` selected its corpus with
//! `table_directory_filter: Some(format!("/{ks}/{table}"))`. `cqlite-core` documents that field,
//! at `cqlite-core/src/ingestion.rs`, as a **SUBSTRING** match that is "loose by design" and
//! "cannot express 'exactly this directory': any sibling whose full name EXTENDS the filter also
//! matches" — directing callers who need one directory to `TableDirSelection::Exact`, which
//! compares complete path components after canonicalization (issue #3234).
//!
//! So a `ws0/events-backup/` (or `events-<uuid>-backup/`, or any name-extending sibling) was
//! silently ingested by this arm.
//!
//! # Why that voids the rig rather than merely adding a directory
//!
//! The rig's ONLY output is arm A against arm B **over the same bytes**. The two arms reach
//! ingestion by different routes — this binary's `IngestionConfig` versus `cqlite-flight
//! --data-dir` — so a sibling absorbed here and not there means the two arms measured DIFFERENT
//! SSTABLE SETS, and the cross-arm ratio compares nothing. It is not only a row-count effect
//! either: an extra directory changes the GENERATION COUNT, and the generation count selects the
//! scan route.
//!
//! # What makes each case non-vacuous
//!
//! The sibling is a REAL, byte-identical copy of the generated table directory, so under the old
//! substring filter it genuinely doubles the observable rows. Each case therefore compares the
//! MEASURED row denominator with and without the sibling present, rather than inspecting a
//! selection list alone — and the artifact's own recorded `table_dirs_ingested` is checked against
//! it, so the artifact and the measurement cannot disagree.
//!
//! The control case proves the pre-fix loss directly: `cqlite_core::ingestion::ingest` with the
//! OLD filter string is called against the same on-disk layout and asserted to select BOTH
//! directories. That is what the shipped code did.
//!
//! This drives the BINARY for the fixed behaviour (the defect lived in its ingestion call) and the
//! LIBRARY for the control (the pre-fix spelling no longer exists in the binary to invoke).

use std::path::Path;
use std::process::Command;

use ws0_corpus_gen::generate::{generate, CorpusSpec, DEFAULT_SEED};

const BIN: &str = env!("CARGO_BIN_EXE_ws0-scan-bench");

/// Small enough to generate in under a second. The property under test is a DIRECTORY-SELECTION
/// relation, which is size-independent — but the row COUNT is what makes the effect observable, so
/// it is asserted exactly.
const ROWS: u64 = 200;
const ROWS_PER_PARTITION: u64 = 10;

/// A generated corpus at `out`, plus nothing else.
async fn corpus_at(out: &Path) {
    let spec = CorpusSpec {
        out: out.to_path_buf(),
        rows: ROWS,
        rows_per_partition: ROWS_PER_PARTITION,
        seed: DEFAULT_SEED,
        no_clobber: false,
        progress_every: 0,
    };
    generate(&spec).await.expect("corpus generation");
}

/// Copy `<out>/ws0/events` to `<out>/ws0/events<suffix>` — a sibling whose full name EXTENDS the
/// intended directory's, which is exactly what a substring filter cannot exclude.
fn add_name_extending_sibling(out: &Path, suffix: &str) -> std::path::PathBuf {
    let src = out.join("ws0").join("events");
    let dst = out.join("ws0").join(format!("events{suffix}"));
    std::fs::create_dir_all(&dst).expect("sibling dir");
    for entry in std::fs::read_dir(&src).expect("read table dir") {
        let entry = entry.expect("entry");
        if entry.file_type().expect("file type").is_file() {
            std::fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy component");
        }
    }
    dst
}

struct Bench {
    ok: bool,
    json: serde_json::Value,
    all: String,
}

/// Run `ws0-scan-bench` over `out` with one timed pass.
fn bench(out: &Path) -> Bench {
    let res = Command::new(BIN)
        .args(["--corpus", out.to_str().expect("utf8"), "--passes", "1"])
        .output()
        .expect("ws0-scan-bench runs");
    let stdout = String::from_utf8_lossy(&res.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&res.stderr).into_owned();
    Bench {
        ok: res.status.success(),
        json: serde_json::from_str(&stdout).unwrap_or(serde_json::Value::Null),
        all: format!("{stdout}\n{stderr}"),
    }
}

/// BASELINE — the intended table directory alone is ingested and scanned.
///
/// The accept case, so the fix cannot be "refuse everything": the guard must still measure the
/// corpus it is pointed at, and the recorded scope must name exactly that directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_intended_table_directory_is_ingested_and_scanned() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    corpus_at(&out).await;

    let run = bench(&out);
    assert!(
        run.ok,
        "the bare scan over a clean corpus must succeed: {}",
        run.all
    );
    assert_eq!(
        run.json["rows_denominator"].as_u64(),
        Some(ROWS),
        "one pass over the corpus observes exactly its rows: {}",
        run.all
    );
    let dirs = run.json["table_dirs_ingested"]
        .as_array()
        .expect("the artifact records the ingested scope")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();
    assert_eq!(dirs.len(), 1, "exactly one table directory: {dirs:?}");
    assert!(
        dirs[0].ends_with("ws0/events"),
        "and it is the intended one: {dirs:?}"
    );
}

/// THE FIX — a NAME-EXTENDING SIBLING is NOT ingested, and the measurement is unchanged by it.
///
/// The strongest statement of the property: adding `ws0/events-backup/` beside the corpus must
/// leave arm A's row denominator, and its recorded scope, EXACTLY as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_name_extending_sibling_is_not_ingested() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    corpus_at(&out).await;

    // MEASURE FIRST, so "unchanged" is a comparison against an observation rather than against a
    // constant this test chose.
    let before = bench(&out);
    assert!(before.ok, "baseline run: {}", before.all);
    let rows_before = before.json["rows_denominator"]
        .as_u64()
        .expect("baseline rows");

    let sibling = add_name_extending_sibling(&out, "-backup");
    // NON-VACUITY: the sibling is a REAL table directory holding real data, so it genuinely offers
    // more rows to anything that ingests it. A sibling with no `*-Data.db` would prove nothing.
    assert!(
        ws0_corpus_gen::generate::has_data_db(&sibling),
        "the sibling must hold *-Data.db, or ingesting it would change nothing anyway"
    );

    let after = bench(&out);
    assert!(
        after.ok,
        "an unrelated sibling must not break arm A — it must be IGNORED, not fatal: {}",
        after.all
    );
    assert_eq!(
        after.json["rows_denominator"].as_u64(),
        Some(rows_before),
        "the sibling must not contribute a single row. Pre-fix the substring filter \
         `/ws0/events` matched `/ws0/events-backup` too, so arm A silently scanned both while arm \
         B (a different ingestion route) may have scanned one — which voids the cross-arm ratio \
         that is this rig's only output: {}",
        after.all
    );
    let dirs = after.json["table_dirs_ingested"]
        .as_array()
        .expect("recorded scope")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        dirs.len(),
        1,
        "the RECORDED scope must name one directory too, so the artifact cannot claim a scope the \
         measurement did not have: {dirs:?}"
    );
    assert!(
        !dirs[0].contains("events-backup"),
        "and it must not be the sibling: {dirs:?}"
    );
}

/// The same property for a sibling whose name extends the intended one WITH A UUID-ish tail, the
/// shape a Cassandra-style `<table>-<uuid>` layout actually produces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_uuid_suffixed_sibling_is_not_ingested_either() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    corpus_at(&out).await;
    add_name_extending_sibling(&out, "-71a911f1000000000000000000000000");

    let run = bench(&out);
    assert!(run.ok, "{}", run.all);
    assert_eq!(
        run.json["rows_denominator"].as_u64(),
        Some(ROWS),
        "a `<table>-<uuid>`-shaped sibling extends the filter string just as `-backup` does: {}",
        run.all
    );
}

/// NON-VACUITY CONTROL — the PRE-FIX call ACCEPTED the sibling. Measured, not asserted.
///
/// The binary no longer contains the substring-filter spelling, so the control invokes it directly
/// through the library: `ingest` (which is `TableDirSelection::Filter`) with the exact filter
/// string the shipped code used. If this ever stops selecting both directories, the sibling fixture
/// has stopped reproducing the defect and the cases above would pass for the wrong reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pre_fix_substring_filter_selected_the_sibling_too() {
    use cqlite_core::ingestion::{
        ingest, ingest_with_selection, IngestionConfig, TableDirSelection,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    corpus_at(&out).await;
    add_name_extending_sibling(&out, "-backup");

    let cfg = |filter: Option<String>| IngestionConfig {
        schema_paths: vec![out.join("ws0-events.cql")],
        data_dir: out.clone(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: filter,
    };

    // EXACTLY the spelling `scan_bench.rs` shipped with.
    let pre_fix = ingest(cfg(Some("/ws0/events".to_string())))
        .await
        .expect("pre-fix ingest");
    let selected: Vec<String> = pre_fix
        .discovery_summary
        .table_directories
        .iter()
        .map(|d| d.display().to_string())
        .collect();
    assert_eq!(
        selected.len(),
        2,
        "THE PRE-FIX BEHAVIOUR: the substring filter `/ws0/events` selected the name-extending \
         sibling as well, so arm A ingested a directory set the rig never intended and never \
         recorded. Selected: {selected:?}"
    );
    assert!(
        selected.iter().any(|d| d.ends_with("events-backup")),
        "and the extra directory is the sibling: {selected:?}"
    );

    // ...and the replacement selects exactly one, over the identical on-disk layout — so the
    // difference is the SELECTION MODE and nothing about the fixture.
    let wanted = [out.join("ws0").join("events")];
    let fixed = ingest_with_selection(cfg(None), TableDirSelection::Exact(&wanted))
        .await
        .expect("exact ingest");
    let selected_exact: Vec<String> = fixed
        .discovery_summary
        .table_directories
        .iter()
        .map(|d| d.display().to_string())
        .collect();
    assert_eq!(
        selected_exact.len(),
        1,
        "`Exact` compares complete path components, so the sibling contributes nothing: \
         {selected_exact:?}"
    );
}

/// The binary WIRES the scope verification — it does not merely ask for `Exact` and hope.
///
/// # Why this is asserted here rather than by provoking a refusal through the CLI
///
/// Both of `scan_scope::verify_exact_scope`'s refusal branches are, correctly, near-unprovokable
/// from a shell once `TableDirSelection::Exact` is in use: `Exact` compares complete path
/// components, so on every layout reachable this way it selects exactly the requested directory or
/// nothing. Six layouts were tried against the built binary (a name-extending sibling, a
/// `<uuid>`-suffixed sibling, a symlinked table dir, a symlinked corpus root, a lone unparseable
/// `*-Data.db`, and a pre-`na`-named generation set) and NONE reached either refusal — every one was
/// either scanned correctly or stopped by the pre-existing zero-rows rule.
///
/// That is the guard working, but it means an in-binary assertion would be one **no test can watch
/// fail** — this issue's own governing defect. So the predicate is a pure function with both
/// branches unit-tested in `src/scan_scope.rs`, and what THIS case establishes is the WIRING: the
/// binary reaches that function on the ordinary path, evidenced by the artifact recording the scope
/// the function was given. A `table_dirs_ingested` naming exactly the intended directory cannot be
/// produced by a binary that skipped the call, because that field is derived from the same
/// `selected` set the call verifies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_records_the_verified_scope_it_measured() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("corpus");
    corpus_at(&out).await;

    // The scanning path...
    let scan = bench(&out);
    assert!(scan.ok, "{}", scan.all);
    assert_eq!(
        scan.json["table_dirs_ingested"],
        serde_json::json!([out.join("ws0").join("events").display().to_string()]),
        "the scanning path must record the exact verified scope: {}",
        scan.all
    );

    // ...and the `--setup-only` path, which is a SEPARATE early return and so a separate chance to
    // skip the verification. `ws0-baseline.sh` runs both per rep (setup is subtracted from the
    // cycles/row denominator), so both must carry the scope.
    let res = Command::new(BIN)
        .args(["--corpus", out.to_str().expect("utf8"), "--setup-only"])
        .output()
        .expect("runs");
    let stdout = String::from_utf8_lossy(&res.stdout).into_owned();
    assert!(res.status.success(), "setup-only run: {stdout}");
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("setup-only json");
    assert_eq!(
        json["table_dirs_ingested"],
        serde_json::json!([out.join("ws0").join("events").display().to_string()]),
        "the --setup-only path must record the verified scope too: {stdout}"
    );
    assert!(
        json["surface"]
            .as_str()
            .unwrap_or_default()
            .contains("TableDirSelection::Exact"),
        "and name the selection mode it used, so an artifact states which one produced it: {stdout}"
    );
}
