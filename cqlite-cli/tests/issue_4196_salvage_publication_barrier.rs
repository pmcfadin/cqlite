//! Issue #4196, roborev round-22 Low finding — the `-TOC.txt` PUBLICATION
//! BARRIER, through the compiled `cqlite` binary.
//!
//! `discover_salvage_inputs`'s single-FILE branch used to return with no
//! `-TOC.txt` probe at all, while its DIRECTORY branch names a barrier-less
//! generation as a `SkippedInput` that lands in the manifest and forces exit 3.
//! So `salvage ./ks/t-<id>/nb-3-big-Data.db` salvaged an unpublished generation
//! SILENTLY, while `salvage ./ks/t-<id>/` on the same file named it and refused —
//! an asymmetry nothing declared.
//!
//! The resolution (design D3, and `discover_salvage_inputs`'s own doc): an
//! explicit file path OVERRIDES the barrier — recovering an unpublished,
//! partially-flushed generation is a legitimate thing for an operator to ask for
//! — but the absence is RECORDED as an `UnpublishedInputGeneration` component
//! finding and counts as a verification gap, so `$?` is the same 3 for the same
//! file whichever way it is named. The difference is that the file form still
//! recovers the data.
//!
//! # Why its own test target
//!
//! `salvage_cli_tests.rs` is the home of this issue's CLI cases, but it sits at
//! ~1420 of the ~1500-line test-file threshold the gate's `file-size` ratchet
//! enforces (epic #1135), so these cases would push it over. Extracting that
//! file's helper block into a shared `tests/support/` module is a separate,
//! purely mechanical change; until then the three helpers below are local. They
//! are deliberately minimal — the fixture-resolution DOCTRINE (issue #3220:
//! committed fixtures fail closed, per case) is the part that matters and is
//! preserved.

// `not(tombstones)`: mirrors `commands::salvage`'s own module gate — the binary
// this file drives via `CARGO_BIN_EXE_cqlite` has no `salvage` verb at all when
// `tombstones` is on.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

/// `test_comp.lz4_table`'s `nb-1-big-*` component set is git-TRACKED, so its
/// absence is a broken checkout, never an unfetched dataset — this resolver
/// fails closed UNCONDITIONALLY, not gated on `CQLITE_REQUIRE_FIXTURES`
/// (issue #3220).
const LZ4_TABLE_DIR: &str = "lz4_table-25801a0071a911f19b3225f9984c6a77";
const LZ4_TABLE_FIXTURE: &str = "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77";
const LZ4_TABLE_COMPONENTS: &[&str] = &[
    "nb-1-big-Data.db",
    "nb-1-big-Index.db",
    "nb-1-big-Summary.db",
    "nb-1-big-Statistics.db",
    "nb-1-big-CompressionInfo.db",
    "nb-1-big-Filter.db",
    "nb-1-big-Digest.crc32",
    "nb-1-big-TOC.txt",
];

/// Every candidate base root (the `CQLITE_DATASETS_ROOT` corpus, then the
/// checkout's own committed corpus — issue #3220: neither is a superset).
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

/// The first candidate root carrying EVERY named component, or a hard failure
/// naming the roots searched. "The directory resolved" is not "the fixture is
/// usable", so each component is asserted present.
fn resolve_committed_fixture() -> PathBuf {
    let roots = candidate_base_roots();
    for root in &roots {
        let dir = root.join(LZ4_TABLE_FIXTURE);
        if LZ4_TABLE_COMPONENTS.iter().all(|c| dir.join(c).is_file()) {
            return dir;
        }
    }
    panic!(
        "COMMITTED fixture {LZ4_TABLE_FIXTURE} (components {LZ4_TABLE_COMPONENTS:?}) is absent — \
         its binaries are git-tracked, so this is a broken checkout, NOT an unfetched dataset, and \
         must never skip (issue #3220). Searched: {roots:?}"
    );
}

/// Copy the fixture's `nb-1-big-*` components (never the `.jsonl`/`.db.txt`
/// sidecars) into a fresh staged table directory named as Cassandra would.
fn stage_generation(clean_dir: &Path, dest_parent: &Path) -> PathBuf {
    let dest = dest_parent.join(LZ4_TABLE_DIR);
    std::fs::create_dir_all(&dest).unwrap_or_else(|e| panic!("create {dest:?}: {e}"));
    for component in LZ4_TABLE_COMPONENTS {
        std::fs::copy(clean_dir.join(component), dest.join(component))
            .unwrap_or_else(|e| panic!("copy {component}: {e}"));
    }
    dest
}

fn schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/schemas/compression-parity.cql")
}

fn run_salvage(input: &Path, out: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            schema_path().to_str().expect("schema path is utf-8"),
            "salvage",
            input.to_str().expect("input path is utf-8"),
            "--out",
            out.to_str().expect("out path is utf-8"),
            "--out-format",
            "json",
        ])
        .output()
        .expect("failed to execute cqlite binary")
}

/// A single-`Data.db` input's manifest is a bare OBJECT (design D5's shape),
/// never an array.
fn manifest_object(stdout: &str) -> serde_json::Value {
    let manifest: serde_json::Value = serde_json::from_str(stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON: {e}\n{stdout}"));
    assert!(
        manifest.is_object(),
        "a single-Data.db input must emit a bare manifest OBJECT (D5); got {manifest}"
    );
    manifest
}

fn finding_classes(manifest: &serde_json::Value) -> Vec<String> {
    manifest
        .get("component_findings")
        .and_then(|f| f.as_array())
        .unwrap_or_else(|| panic!("manifest missing 'component_findings': {manifest}"))
        .iter()
        .map(|f| {
            f.get("class")
                .and_then(|c| c.as_str())
                .unwrap_or_else(|| panic!("finding missing 'class': {f}"))
                .to_string()
        })
        .collect()
}

/// The gap leg: an explicitly-named `Data.db` whose `-TOC.txt` sibling is gone
/// is SALVAGED, the missing barrier is NAMED in the manifest, and the run exits
/// 3 rather than 0.
#[test]
fn explicit_data_db_without_toc_txt_is_salvaged_named_and_exits_3() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_generation(&clean_dir, temp.path());
    let toc = input_dir.join("nb-1-big-TOC.txt");
    std::fs::remove_file(&toc).unwrap_or_else(|e| panic!("remove {toc:?}: {e}"));
    assert!(
        !toc.exists(),
        "the gap leg requires the barrier to be ABSENT"
    );

    let out = temp.path().join("out");
    let output = run_salvage(&input_dir.join("nb-1-big-Data.db"), &out);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(3),
        "an unpublished generation is salvaged, but its unverifiable publication barrier makes the \
         run imperfect — the same file reached through its table DIRECTORY already exits 3, so a 0 \
         here means `$?` depends on how the operator spelled the input; stdout={stdout}\n\
         stderr={stderr}"
    );

    let manifest = manifest_object(&stdout);
    let classes = finding_classes(&manifest);
    assert!(
        classes.iter().any(|c| c == "UnpublishedInputGeneration"),
        "the manifest must NAME the absent publication barrier — the exit code alone does not tell \
         an operator which premise went unverified; classes={classes:?} manifest={manifest}"
    );
    // The barrier's absence is not a LOSS: every partition is still recovered
    // and the generation is written.
    assert_eq!(
        manifest
            .get("losses")
            .and_then(|l| l.as_array())
            .map(Vec::len),
        Some(0),
        "an absent TOC.txt must not be reported as a partition loss; manifest={manifest}"
    );
    assert!(
        data_db_exists_under(&out),
        "the data must still be recovered — an explicit file path OVERRIDES the barrier, it does \
         not refuse; nothing found under {out:?}"
    );
}

/// The control leg: the SAME staged generation WITH its `-TOC.txt` exits 0 and
/// records no barrier finding. Without it, "exit 3" could be this fixture's
/// permanent outcome and the gap leg would prove nothing.
#[test]
fn explicit_data_db_with_toc_txt_exits_0_and_records_no_barrier_finding() {
    let clean_dir = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input_dir = stage_generation(&clean_dir, temp.path());
    assert!(
        input_dir.join("nb-1-big-TOC.txt").is_file(),
        "the control leg requires the barrier to be PRESENT"
    );

    let out = temp.path().join("out");
    let output = run_salvage(&input_dir.join("nb-1-big-Data.db"), &out);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a published, healthy generation must exit 0; stdout={stdout}\nstderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let classes = finding_classes(&manifest_object(&stdout));
    assert!(
        !classes.iter().any(|c| c == "UnpublishedInputGeneration"),
        "a published generation has no absent barrier to report; classes={classes:?}"
    );
    assert!(data_db_exists_under(&out));
}

/// `true` iff any `*-Data.db` exists anywhere under `dir` (the writer nests
/// `<out>/<keyspace>/<table>/`, so a top-level listing sees no components).
fn data_db_exists_under(dir: &Path) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if data_db_exists_under(&path) {
                return true;
            }
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}
