//! Issue #4196, roborev round-23 findings F1/F4a/F5 — DESTRUCTIVE-WRITE
//! CONTAINMENT for `cqlite salvage`, asserted END-TO-END through the compiled
//! binary against a real Cassandra-written fixture.
//!
//! All three findings were REPRODUCED against `./target/debug/cqlite` and real
//! Cassandra-written SSTable bytes by an independent Cassandra-format expert
//! review, and all three previously **exited 0 with a clean console summary**
//! while bytes were destroyed:
//!
//! * **F1** — `--manifest <out>/<keyspace>/<table>/nb-1-big-Data.db` recovered
//!   100 partitions, wrote the real `Data.db`, then overwrote it with 607 bytes
//!   of manifest JSON. `recovered=100 lost=0`, exit 0.
//! * **F4a** — a SYMLINK named `salvage.json`, not component-shaped and with its
//!   own parent OUTSIDE the input, pointing at a real `nb-1-big-Statistics.db`
//!   INSIDE the input: `File::create` followed it, 5265 bytes -> 531 bytes of
//!   JSON. A direct spec R5.1 violation ("the input is not modified").
//! * **F5** — `--out <input>/recovered` wrote a full recovered generation INSIDE
//!   the input tree the tool exists to preserve, which a re-run then discovered
//!   as one more generation to salvage.
//!
//! # Why these cases exist SEPARATELY from the unit tests
//!
//! The fix is one shared guard (`commands::salvage::write_guard`), unit-pinned by
//! 49 `--lib salvage` tests. Those tests prove the guard's LOGIC; they cannot
//! prove the CLI actually CALLS it on every destructive path — and the failure
//! shape here is "exit 0, clean summary, bytes destroyed", which only a test that
//! inspects the bytes AFTERWARDS can see (CLAUDE.md wiring-evidence rule: a
//! feature is done only when its public surface exercises it, and green
//! helper-only unit tests are not sufficient). So every case below asserts THREE
//! things: the process EXIT CODE, that the refusal NAMES the offending path, and
//! that the bytes that were at risk are BYTE-IDENTICAL afterwards.
//!
//! # Why its own test target
//!
//! `salvage_cli_tests.rs` sits at ~1430 of the ~1500-line test-file threshold the
//! gate's `file-size` ratchet enforces (epic #1135), so these cases would push it
//! over — the same reason `issue_4196_salvage_publication_barrier.rs` exists.
//! The helpers below are deliberately local and minimal; the part that matters is
//! the fixture-resolution DOCTRINE (issue #3220: a committed fixture fails
//! CLOSED, per case, never skips), which is preserved verbatim.
//!
//! # The controls matter as much as the refusals
//!
//! A guard that refuses everything is not a fix, and a suite of refusals alone
//! cannot tell a working guard from a broken tool. So
//! [`both_documented_manifest_locations_still_succeed_and_recover_real_bytes`]
//! drives the two legitimate `--manifest` locations and asserts a real manifest
//! AND a real recovered `Data.db` that is NOT manifest JSON — F1's exact failure
//! shape, checked positively.

// `not(tombstones)`: mirrors `commands::salvage`'s own module gate — the binary
// this file drives via `CARGO_BIN_EXE_cqlite` has no `salvage` verb at all when
// `tombstones` is on.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

/// `test_comp.lz4_table` — BIG (`nb`), LZ4, real Cassandra 5.0 output, and the
/// fixture the expert review's own reproductions ran against. Every component
/// listed is git-TRACKED (`git ls-files test-data/datasets/sstables/test_comp/`),
/// so an absence is a broken checkout and never an unfetched dataset.
const FIXTURE_RELATIVE: &str = "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77";

/// The staged input directory's name. Cassandra's `<table>-<32-hex-id>`
/// convention, preserved so salvage's own table-name derivation resolves
/// `lz4_table` without `--table`.
const FIXTURE_DIR_NAME: &str = "lz4_table-25801a0071a911f19b3225f9984c6a77";

/// Every component the staging copy needs, named EXPLICITLY (rather than
/// "whatever is in the directory") so a fixture that regenerates with a
/// component missing fails by NAME.
const FIXTURE_COMPONENTS: &[&str] = &[
    "nb-1-big-Data.db",
    "nb-1-big-Index.db",
    "nb-1-big-Summary.db",
    "nb-1-big-Statistics.db",
    "nb-1-big-CompressionInfo.db",
    "nb-1-big-Filter.db",
    "nb-1-big-Digest.crc32",
    "nb-1-big-TOC.txt",
];

/// The committed CQL schema declaring `test_comp.lz4_table`.
const FIXTURE_SCHEMA: &str = "compression-parity.cql";

/// The keyspace/table `FIXTURE_SCHEMA` declares — and therefore the two path
/// components salvage nests its recovered generation under
/// (`<--out>/<keyspace>/<table>/`, per `recover.rs`). Spelled out because F1's
/// whole subject is that directory being PROTECTED, so the case has to be able
/// to aim at it.
const FIXTURE_KEYSPACE: &str = "test_comp";
const FIXTURE_TABLE: &str = "lz4_table";

/// The component F4a destroys: a real Cassandra-written `Statistics.db` inside
/// the input, reached through a non-component-shaped symlink whose own parent
/// sits outside the input.
const VICTIM_COMPONENT: &str = "nb-1-big-Statistics.db";

/// Every candidate base root (the `CQLITE_DATASETS_ROOT` corpus, then the
/// checkout's own committed corpus — issue #3220: neither is a superset of the
/// other, so both are searched and the one carrying the fixture wins).
fn candidate_base_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(env_root));
    }
    let checkout = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/datasets");
    if !roots.contains(&checkout) {
        roots.push(checkout);
    }
    roots
}

/// The first candidate root carrying EVERY component of the fixture, or a hard
/// failure naming the roots searched.
///
/// Resolved per TABLE and asserted per CASE (issue #3220): a resolver that
/// commits to a root by KEYSPACE can pass without ever running, and a skip that
/// reads as a pass is the exact defect class the guard under test exists for.
/// Never gated on `CQLITE_REQUIRE_FIXTURES` — these binaries are git-tracked.
fn resolve_committed_fixture() -> PathBuf {
    let roots = candidate_base_roots();
    for root in &roots {
        let dir = root.join(FIXTURE_RELATIVE);
        if FIXTURE_COMPONENTS.iter().all(|c| dir.join(c).is_file()) {
            return dir;
        }
    }
    panic!(
        "COMMITTED fixture {FIXTURE_RELATIVE} (components {FIXTURE_COMPONENTS:?}) is absent — its \
         binaries are git-tracked, so this is a broken checkout, NOT an unfetched dataset, and \
         must never skip (issue #3220, fail-closed UNCONDITIONALLY). Searched: {roots:?}"
    );
}

/// Copy the fixture's real components (never the `.jsonl`/`.db.txt` sidecars)
/// into a fresh staged table directory under `dest_parent`.
///
/// Every destructive case stages its OWN copy inside its OWN `TempDir`, so no
/// case can damage the real corpus and no case can contaminate another.
fn stage_input(clean_dir: &Path, dest_parent: &Path) -> PathBuf {
    let dest = dest_parent.join(FIXTURE_DIR_NAME);
    std::fs::create_dir_all(&dest).unwrap_or_else(|e| panic!("create {dest:?}: {e}"));
    for component in FIXTURE_COMPONENTS {
        std::fs::copy(clean_dir.join(component), dest.join(component))
            .unwrap_or_else(|e| panic!("copy {component}: {e}"));
    }
    dest
}

fn schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/schemas")
        .join(FIXTURE_SCHEMA)
}

fn as_arg(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("{path:?} is not utf-8"))
}

/// `cqlite --schema <compression-parity.cql> salvage <input> --out <out> [--manifest <m>]`
/// through the REAL compiled binary — `execute_salvage_command` enforces its exit
/// codes via `std::process::exit`, which only a subprocess can observe.
fn run_salvage(input: &Path, out: &Path, manifest: Option<&Path>) -> Output {
    let schema = schema_path();
    let mut args: Vec<&str> = vec![
        "--schema",
        as_arg(&schema),
        "salvage",
        as_arg(input),
        "--out",
        as_arg(out),
    ];
    if let Some(manifest) = manifest {
        args.push("--manifest");
        args.push(as_arg(manifest));
    }
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(&args)
        .output()
        .expect("failed to execute cqlite binary")
}

/// `file name -> bytes` for every file directly under `dir`.
fn read_dir_bytes(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .flatten()
    {
        if entry.path().is_file() {
            let bytes = std::fs::read(entry.path())
                .unwrap_or_else(|e| panic!("read {:?}: {e}", entry.path()));
            out.insert(entry.file_name().to_string_lossy().to_string(), bytes);
        }
    }
    assert!(!out.is_empty(), "{dir:?}: nothing to read");
    out
}

/// Spec R5.1 — the input is not modified: every staged component byte-identical
/// (SIZE **and** CONTENT), and the file SET unchanged in both directions.
fn assert_input_byte_identical(input_dir: &Path, before: &BTreeMap<String, Vec<u8>>) {
    let after = read_dir_bytes(input_dir);
    for (name, bytes) in before {
        match after.get(name) {
            None => panic!("input component {name:?} was REMOVED from {input_dir:?} (spec R5.1)"),
            Some(now) => {
                assert_eq!(
                    now.len(),
                    bytes.len(),
                    "input component {name:?} changed SIZE ({} -> {}) — spec R5.1: salvage must \
                     not modify a byte of its input",
                    bytes.len(),
                    now.len()
                );
                assert!(
                    now == bytes,
                    "input component {name:?} changed CONTENT at the same length ({} bytes) — the \
                     size check alone would have missed this (spec R5.1)",
                    bytes.len()
                );
            }
        }
    }
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "the input file SET must be unchanged in BOTH directions (spec R5.1)"
    );
}

/// Every `*-Data.db` anywhere under `dir` (recursive), relative-ish by file name
/// plus full path so a failure message can name what was written.
fn data_dbs_under(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(data_dbs_under(&path));
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            found.push(path);
        }
    }
    found.sort();
    found
}

/// A refused run must leave `--out` unpopulated: no recovered generation, and in
/// particular no `*-Data.db`. Asserted recursively, since the generation nests
/// two levels down (`<--out>/<keyspace>/<table>/`).
fn assert_out_unpopulated(out: &Path) {
    let written = data_dbs_under(out);
    assert!(
        written.is_empty(),
        "nothing may be written under --out {out:?} when the run is refused as a usage error; \
         found {written:?}"
    );
}

fn stdio(output: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// **F1** — a `--manifest` resolving under the run's OWN planned output root
/// `<--out>/<keyspace>/<table>/` is refused BEFORE any work, and nothing is
/// written under `--out`.
///
/// Reproduced by the expert review against the compiled binary: the run
/// recovered 100 partitions, wrote the real `Data.db`, then overwrote it with 607
/// bytes of manifest JSON and printed `recovered=100 lost=0`, exit 0. The
/// round-22 guard checked `--manifest` against the INPUT only, so aiming it at
/// the run's own output walked straight through.
#[test]
fn f1_manifest_inside_the_planned_output_generation_is_refused_and_out_stays_empty() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let before = read_dir_bytes(&input_dir);

    let out = temp.path().join("out");
    // Exactly the expert's reproduction: the recovered Data.db's own path.
    let manifest = out
        .join(FIXTURE_KEYSPACE)
        .join(FIXTURE_TABLE)
        .join("nb-1-big-Data.db");

    let output = run_salvage(&input_dir, &out, Some(&manifest));
    let (stdout, stderr) = stdio(&output);

    assert_eq!(
        output.status.code(),
        Some(1),
        "a --manifest aimed at the run's OWN recovered generation is a USAGE error (exit 1), \
         refused before any I/O — the F1 shape was exit 0 with a clean `recovered=100 lost=0` \
         summary over a truncated Data.db; stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(as_arg(&manifest)),
        "the refusal must NAME the offending path so the operator can see what was about to be \
         destroyed; got: {stderr}"
    );
    assert!(
        stderr.contains("OUTPUT"),
        "the refusal must say WHICH boundary was crossed — 'the input' and 'your own output' are \
         very different mistakes; got: {stderr}"
    );

    assert_out_unpopulated(&out);
    assert!(
        !manifest.exists(),
        "a refused --manifest must never have been created at {manifest:?}"
    );
    // R5.1 holds trivially here (the refusal precedes discovery), and is asserted
    // anyway: "refused" and "refused without touching anything" are two claims.
    assert_input_byte_identical(&input_dir, &before);
}

/// **F4a** — a SYMLINK named `salvage.json`, whose own parent sits OUTSIDE the
/// input and whose name is not component-shaped, pointing at a real
/// Cassandra-written component INSIDE the input, is refused — and that component
/// is BYTE-IDENTICAL afterwards.
///
/// This is the case that destroyed 5265 bytes of a real Cassandra file (-> 531
/// bytes of manifest JSON, exit 0, clean summary): the round-22 guard
/// canonicalized the manifest's PARENT and pattern-matched its FILE NAME, so a
/// symlink passed both halves and `File::create` followed it. The full CONTENT is
/// asserted, not the length alone — a same-length corruption must not read as a
/// pass.
#[cfg(unix)]
#[test]
fn f4a_manifest_symlink_into_the_input_is_refused_and_the_victim_is_byte_identical() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let before = read_dir_bytes(&input_dir);

    let victim = input_dir.join(VICTIM_COMPONENT);
    let victim_before = std::fs::read(&victim).unwrap_or_else(|e| panic!("read {victim:?}: {e}"));
    assert!(
        victim_before.len() > 1000,
        "the case needs a substantial real Cassandra component to protect; {victim:?} is {} bytes",
        victim_before.len()
    );

    // The link's own parent is OUTSIDE the input — that is precisely what
    // defeated the round-22 guard — and its name is the innocuous, documented
    // `salvage.json`, which the component-name taboo does not match either.
    let link_dir = temp.path().join("manifests");
    std::fs::create_dir_all(&link_dir).expect("create link dir");
    let link = link_dir.join("salvage.json");
    std::os::unix::fs::symlink(&victim, &link).expect("create symlink");
    assert!(
        link.symlink_metadata()
            .expect("the link must exist as an entry")
            .file_type()
            .is_symlink(),
        "the case needs a real SYMLINK at {link:?}"
    );

    let out = temp.path().join("out");
    let output = run_salvage(&input_dir, &out, Some(&link));
    let (stdout, stderr) = stdio(&output);

    assert_eq!(
        output.status.code(),
        Some(1),
        "a --manifest symlink resolving into the input is a USAGE error (exit 1) — F4a's shape was \
         exit 0 with a clean summary over a destroyed Statistics.db (spec R5.1); \
         stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(as_arg(&link)),
        "the refusal must NAME the path the operator typed; got: {stderr}"
    );
    assert!(
        stderr.contains(VICTIM_COMPONENT),
        "the refusal must name the RESOLVED victim — the operator typed `salvage.json` and needs \
         to be told it points at {VICTIM_COMPONENT}; got: {stderr}"
    );
    assert!(
        stderr.contains("INPUT"),
        "the refusal must say the INPUT boundary was crossed; got: {stderr}"
    );

    // The load-bearing assertion: SIZE and CONTENT, both.
    let victim_after = std::fs::read(&victim).unwrap_or_else(|e| panic!("read {victim:?}: {e}"));
    assert_eq!(
        victim_after.len(),
        victim_before.len(),
        "{VICTIM_COMPONENT} changed SIZE ({} -> {}) — this is F4a EXACTLY: File::create followed \
         the symlink and truncated a real Cassandra-written component (spec R5.1)",
        victim_before.len(),
        victim_after.len()
    );
    assert!(
        victim_after == victim_before,
        "{VICTIM_COMPONENT} changed CONTENT at the same length ({} bytes) — a length-only check \
         would have called this a pass (spec R5.1)",
        victim_before.len()
    );
    assert!(
        link.symlink_metadata()
            .expect("the link must still exist")
            .file_type()
            .is_symlink(),
        "a refused run must not have replaced the symlink with a regular file"
    );
    assert_input_byte_identical(&input_dir, &before);
    assert_out_unpopulated(&out);
}

/// **F5** — `--out` inside the input tree is refused, and the input gains no
/// `recovered/` subdirectory.
///
/// The round-22 `--out` guard was an EMPTINESS probe and nothing else: it asked
/// whether `--out` was safe to POPULATE and never WHERE it pointed. So
/// `--out <input>/recovered` wrote a full recovered generation inside the input
/// the tool exists to preserve, and a re-run then discovered that output as one
/// more generation to salvage.
#[test]
fn f5_out_inside_the_input_tree_is_refused_and_the_input_gains_no_recovered_dir() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let before = read_dir_bytes(&input_dir);

    let out = input_dir.join("recovered");
    let output = run_salvage(&input_dir, &out, None);
    let (stdout, stderr) = stdio(&output);

    assert_eq!(
        output.status.code(),
        Some(1),
        "--out inside the input tree is a USAGE error (exit 1) — F5's shape was a full recovered \
         generation written inside the input and exit 0; stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(as_arg(&out)),
        "the refusal must NAME the offending --out path; got: {stderr}"
    );
    assert!(
        stderr.contains("INPUT"),
        "the refusal must say the INPUT boundary was crossed; got: {stderr}"
    );

    assert!(
        !out.exists(),
        "the input directory must gain no `recovered/` subdirectory at {out:?} (spec R5.1)"
    );
    assert!(
        data_dbs_under(&input_dir).len() == 1,
        "the input must still hold exactly its ONE staged generation's Data.db — a recovered \
         generation nested inside it is F5; found {:?}",
        data_dbs_under(&input_dir)
    );
    assert_input_byte_identical(&input_dir, &before);
}

/// The CONTROLS. A guard that refuses everything is not a fix, and the three
/// refusals above cannot distinguish a working guard from a broken tool.
///
/// Both legitimate `--manifest` locations must still SUCCEED end-to-end:
///
/// 1. `<--out>/salvage.json` — the pattern `--help` and `dev-cookbook.md`
///    document. It lives BESIDE the recovered generation, never inside it, so
///    `--out` itself is deliberately NOT protected.
/// 2. a path in an unrelated temporary directory, outside both the input and
///    `--out` entirely.
///
/// Each asserts exit 0, a real parseable manifest, AND a real recovered
/// `Data.db` that is NOT manifest JSON — F1's failure shape (`Data.db` replaced
/// by 607 bytes of JSON) checked POSITIVELY, since a `Data.db` that merely
/// EXISTS is exactly what F1 left behind.
#[test]
fn both_documented_manifest_locations_still_succeed_and_recover_real_bytes() {
    let clean_dir = resolve_committed_fixture();
    let source_data_len = std::fs::metadata(clean_dir.join("nb-1-big-Data.db"))
        .expect("stat the fixture Data.db")
        .len();

    // ---- leg 1: the documented `<--out>/salvage.json` ----
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let before = read_dir_bytes(&input_dir);
    let out = temp.path().join("out");
    let manifest = out.join("salvage.json");
    let output = run_salvage(&input_dir, &out, Some(&manifest));
    assert_manifest_run_succeeded(
        &output,
        &manifest,
        &out,
        source_data_len,
        "<--out>/salvage.json",
    );
    assert_input_byte_identical(&input_dir, &before);

    // ---- leg 2: a manifest outside BOTH the input and --out ----
    let temp2 = TempDir::new().expect("tempdir");
    let input_dir2 = stage_input(&clean_dir, temp2.path());
    let before2 = read_dir_bytes(&input_dir2);
    let out2 = temp2.path().join("out");
    // A unique path in the system temp dir, outside every protected root.
    let elsewhere = TempDir::new().expect("tempdir");
    let manifest2 = elsewhere.path().join("salvage-manifest.json");
    let output2 = run_salvage(&input_dir2, &out2, Some(&manifest2));
    assert_manifest_run_succeeded(
        &output2,
        &manifest2,
        &out2,
        source_data_len,
        "an unrelated temp-dir manifest path",
    );
    assert_input_byte_identical(&input_dir2, &before2);
}

/// One control leg's assertions: exit 0, a manifest that PARSES, and a recovered
/// `Data.db` of plausible size that is not itself manifest JSON.
fn assert_manifest_run_succeeded(
    output: &Output,
    manifest: &Path,
    out: &Path,
    source_data_len: u64,
    what: &str,
) {
    let (stdout, stderr) = stdio(output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "the LEGITIMATE invocation ({what}) must still succeed — a guard that refuses this refuses \
         every invocation in --help; stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        manifest.is_file(),
        "{what} must actually write the manifest to {manifest:?}"
    );
    let manifest_bytes =
        std::fs::read(manifest).unwrap_or_else(|e| panic!("read {manifest:?}: {e}"));
    let parsed: serde_json::Value = serde_json::from_slice(&manifest_bytes)
        .unwrap_or_else(|e| panic!("{manifest:?} is not JSON: {e}"));
    assert!(
        parsed.as_array().map(|a| !a.is_empty()).unwrap_or(false),
        "a table-dir input's manifest is a NON-EMPTY JSON array, one entry per generation (design \
         D5); got {parsed}"
    );

    // And a REAL recovered generation, not F1's JSON-in-a-Data.db.
    let recovered = data_dbs_under(out);
    assert_eq!(
        recovered.len(),
        1,
        "{what} must recover exactly the one staged generation under {out:?}; found {recovered:?}"
    );
    let data_bytes =
        std::fs::read(&recovered[0]).unwrap_or_else(|e| panic!("read {:?}: {e}", recovered[0]));
    assert!(
        data_bytes.len() as u64 >= source_data_len / 2,
        "the recovered Data.db at {:?} is {} bytes against a {source_data_len}-byte source — F1 \
         left a {}-byte manifest JSON here and still reported success, so 'the file exists' is not \
         the property; manifest is {} bytes",
        recovered[0],
        data_bytes.len(),
        manifest_bytes.len(),
        manifest_bytes.len()
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&data_bytes).is_err(),
        "the recovered Data.db at {:?} PARSED AS JSON — that is F1 exactly: the manifest write \
         overwrote the recovered generation",
        recovered[0]
    );
}

/// Issue #4196 C-audit gap (R7.8, case a) — a `--manifest` SYMLINK whose own
/// immediate link target does NOT exist yet, but which lexically sits INSIDE
/// the input directory, must be refused rather than allowed through as "just
/// a not-yet-existing path".
///
/// This is [`write_guard::resolve_write_target`]'s `dangling_symlink_reason`
/// branch, proven only by the inline unit test
/// `write_guard::tests::an_unresolvable_candidate_is_refused_with_a_named_reason`
/// (no gate component executes an inline `#[cfg(test)]` module) — `NotFound`
/// is ambiguous between "does not exist yet" (an ALLOW, e.g. the documented
/// `<--out>/salvage.json`) and "a dangling symlink" (a REFUSAL, since
/// `File::create` would FOLLOW it and create its target wherever that points,
/// even inside the input this tool exists to preserve). Only an end-to-end run
/// through the compiled binary can show the CLI actually reaches this branch
/// rather than, say, treating the symlink's `NotFound` as "safe to create".
#[cfg(unix)]
#[test]
fn manifest_dangling_symlink_into_input_is_refused_and_target_not_created() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let before = read_dir_bytes(&input_dir);

    // The link's own PARENT sits outside the input (so the round-22
    // parent-only check would have allowed it), and its immediate target —
    // read directly off the link, never canonicalized, since canonicalize
    // fails on a dangling target — is a file that does not exist yet but
    // lexically sits INSIDE the input directory.
    let link_dir = temp.path().join("manifests");
    std::fs::create_dir_all(&link_dir).expect("create link dir");
    let link = link_dir.join("salvage.json");
    let dangling_target = input_dir.join("not-yet-written.json");
    std::os::unix::fs::symlink(&dangling_target, &link).expect("create dangling symlink");
    assert!(
        !dangling_target.exists(),
        "the case needs a DANGLING target: absent on disk at {dangling_target:?}"
    );
    assert!(
        link.symlink_metadata()
            .expect("the link must exist as an entry")
            .file_type()
            .is_symlink(),
        "the case needs a real SYMLINK at {link:?}"
    );

    let out = temp.path().join("out");
    let output = run_salvage(&input_dir, &out, Some(&link));
    let (stdout, stderr) = stdio(&output);

    assert_eq!(
        output.status.code(),
        Some(1),
        "a dangling --manifest symlink is a USAGE error (exit 1), the same shape as F4a's refusal \
         of a symlink whose target already exists; stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(as_arg(&link)),
        "the refusal must NAME the path the operator typed; got: {stderr}"
    );
    assert!(
        stderr.contains(as_arg(&dangling_target)),
        "the refusal must NAME the link's target — the operator needs to see where the write \
         would actually have landed, inside the input directory; got: {stderr}"
    );

    assert!(
        !dangling_target.exists(),
        "a refused run must never have created the dangling symlink's target at \
         {dangling_target:?}"
    );
    assert!(
        link.symlink_metadata()
            .expect("the link must still exist")
            .file_type()
            .is_symlink(),
        "a refused run must not have replaced the symlink with a regular file"
    );
    assert_input_byte_identical(&input_dir, &before);
    assert_out_unpopulated(&out);
}

/// Issue #4196 C-audit gap (R7.8, case b) — a `--manifest` path that walks
/// through a directory that does NOT exist yet and rejoins (`..`) back into
/// the input is refused, exactly like an existing-ancestor traversal — proven
/// only by the inline unit test
/// `write_guard::tests::a_traversal_through_a_nonexistent_dir_still_resolves_into_the_input`.
///
/// Naive re-joining of a not-yet-existing path would leave the `..` components
/// in the result, and a containment check keyed on string prefixes would then
/// read `<out>/nope/../../<input-dir>/m.json` as living under `<out>` while the
/// write in fact lands inside the input — this is the LEXICAL resolution
/// [`write_guard::resolve_write_target`] performs instead, checked here through
/// the compiled binary rather than the resolver alone.
#[test]
fn manifest_traversal_through_nonexistent_dir_resolves_into_input_and_is_refused() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_input(&clean_dir, temp.path());
    let before = read_dir_bytes(&input_dir);

    let out = temp.path().join("out");
    // <out>/nonexistent-dir/../../<FIXTURE_DIR_NAME>/m.json -> lexically
    // inside the input, even though neither `out` nor `nonexistent-dir`
    // exists yet.
    let manifest = out
        .join("nonexistent-dir")
        .join("..")
        .join("..")
        .join(FIXTURE_DIR_NAME)
        .join("m.json");
    assert!(
        !out.join("nonexistent-dir").exists(),
        "the case needs a traversal through a directory that does not exist yet"
    );

    let output = run_salvage(&input_dir, &out, Some(&manifest));
    let (stdout, stderr) = stdio(&output);

    assert_eq!(
        output.status.code(),
        Some(1),
        "a --manifest traversal that lexically resolves into the input is a USAGE error (exit 1); \
         stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(as_arg(&manifest)),
        "the refusal must NAME the path the operator typed; got: {stderr}"
    );
    assert!(
        stderr.contains("INPUT"),
        "the refusal must say the INPUT boundary was crossed; got: {stderr}"
    );

    assert!(
        !manifest.exists(),
        "a refused --manifest must never have been created at {manifest:?}"
    );
    assert_out_unpopulated(&out);
    assert_input_byte_identical(&input_dir, &before);
}

/// Issue #4196 C-audit gap (R7.8, case c) — an `--input` that exists NOWHERE
/// contributes no protected entry (`WriteGuard::new`'s ONE documented
/// exception: "nothing to destroy" is not "cannot decide"), proven only by the
/// inline unit test `write_guard::tests::a_nonexistent_input_protects_nothing`.
/// End-to-end, that must still mean the RUN fails closed — never a panic,
/// never a silent success, and never a write under `--out` — even though the
/// guard itself raises no objection.
#[test]
fn nonexistent_input_protects_nothing_and_the_run_still_fails_closed() {
    let temp = TempDir::new().expect("tempdir");
    let input = temp.path().join("no-such-input-dir");
    assert!(
        !input.exists(),
        "the case needs an --input that does not exist at all: {input:?}"
    );
    let out = temp.path().join("out");

    // --table named explicitly so this case isolates "the input does not
    // exist" from the unrelated table-name-derivation question — the schema
    // already declares FIXTURE_TABLE, so a clean run would otherwise resolve
    // it.
    let schema = schema_path();
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            as_arg(&schema),
            "salvage",
            as_arg(&input),
            "--out",
            as_arg(&out),
            "--table",
            FIXTURE_TABLE,
        ])
        .output()
        .expect("failed to execute cqlite binary");
    let (stdout, stderr) = stdio(&output);

    assert!(
        !output.status.success(),
        "a nonexistent --input must fail closed rather than report success; \
         stdout={stdout}\nstderr={stderr}"
    );
    assert!(
        stderr.contains(as_arg(&input)),
        "the refusal must NAME the missing input path; got: {stderr}"
    );

    assert_out_unpopulated(&out);
    assert!(
        !out.exists()
            || std::fs::read_dir(&out)
                .map(|mut rd| rd.next().is_none())
                .unwrap_or(true),
        "a refused run for a nonexistent input must leave --out untouched (absent, or created but \
         empty); out={out:?}"
    );
}

/// Every run of whitespace (clap's own wrapping included) collapsed to one
/// space, ends trimmed — clap re-wraps a doc comment to the terminal width, so
/// a raw `contains` on a multi-word phrase is a terminal-width-dependent
/// assertion (green in a pipe, red under a narrower one).
fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Issue #4196 C-audit gap (R8.2) — `salvage --help` must STATE the
/// destructive-write containment this file otherwise only PROVES by running
/// the binary: the `--out` refusal, that a candidate path is RESOLVED first
/// (symlinks followed, `..` applied — the exact mechanism
/// `write_guard::resolve_write_target` implements and this file's traversal/
/// dangling-symlink cases exercise), that the run's own planned output
/// generation is one of the things checked for containment, and the
/// documented `<--out>/salvage.json` location.
///
/// `help_states_uncompressed_whole_partition_and_rebuild_boundaries`
/// (`salvage_cli_tests.rs`) already pins the top-level `long_about`'s three
/// boundaries (uncompressed output, whole-or-nothing recovery, the rebuild
/// remedy); this is the "And" clause the C-audit found unpinned — the
/// PER-ARGUMENT doc comments on `SalvageArgs::out` and `SalvageArgs::manifest`
/// (`cli_types.rs:584-609`), which clap ALSO prints under `salvage --help`
/// (verified directly: `cqlite salvage --help` includes an `Options:` section
/// listing `--out` and `--manifest` each followed by their own doc comment).
/// A future edit that dropped one of these from the doc comment would compile
/// fine and previously had nothing pinning the resulting silent narrowing of
/// what the tool promises to protect.
#[test]
fn help_states_the_out_and_manifest_write_containment_boundaries() {
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(["salvage", "--help"])
        .output()
        .expect("failed to execute cqlite binary");
    assert_eq!(
        output.status.code(),
        Some(0),
        "`salvage --help` must exit 0; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let help = collapse_whitespace(&String::from_utf8_lossy(&output.stdout));

    for (boundary, phrase) in [
        (
            "the --out refusal for a path resolving inside the input tree",
            "REFUSED (exit 1) when it resolves INSIDE the input tree: salvage writes only the \
             recovered generation and must not modify a byte of its input",
        ),
        (
            "--manifest is RESOLVED first, symlinks followed and `..` applied",
            "The path is RESOLVED first — symlinks followed, `..` applied — so a link named \
             `salvage.json` cannot reach past the check, and a path that cannot be resolved is \
             refused rather than allowed.",
        ),
        (
            "--manifest containment against the run's OWN planned output generation",
            "the path is REFUSED (exit 1) when it resolves inside the INPUT directory, inside the \
             run's OWN planned output generation `<--out>/<keyspace>/<table>/`, or onto an \
             existing file named like an SSTable component",
        ),
        (
            "the documented <--out>/salvage.json manifest location",
            "recommended value `<--out>/salvage.json`",
        ),
    ] {
        assert!(
            help.contains(&collapse_whitespace(phrase)),
            "`salvage --help` must state the {boundary} boundary. Expected (whitespace-collapsed) \
             phrase:\n  {phrase}\nHelp text was:\n{help}"
        );
    }
}
