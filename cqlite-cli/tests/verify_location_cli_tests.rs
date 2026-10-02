//! Issue #4194 (corruption-locator, spec verify-location L4.2) — `cqlite
//! verify --mode full` renders `location` in both `--out text` and `--out
//! json`, and a finding with no location (e.g. a companion `DigestMismatch`)
//! renders exactly as it did before this change.
//!
//! Drives the REAL compiled `cqlite` binary (`CARGO_BIN_EXE_cqlite`) — the
//! existing `execute_verify_command` exit-code contract (non-zero on any
//! finding) is only observable through a subprocess.
//!
//! Dataset doctrine (issue #1094): SKIP when the corruption corpus binaries
//! are absent; `CQLITE_REQUIRE_FIXTURES=1` turns that into a hard failure.

use std::path::PathBuf;
use std::process::Command;

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
}

/// Every candidate BASE root — the `CQLITE_DATASETS_ROOT` corpus, then the
/// checkout's own committed corpus.
///
/// Issue #3220 doctrine, mirrored from `cqlite-core/tests/
/// issue_4194_verify_location.rs`'s `candidate_base_roots()` and the sibling
/// `salvage_cli_tests.rs`: resolution here was env-ONLY, so every case in this
/// file skipped silently whenever `CQLITE_DATASETS_ROOT` was unset — and would
/// skip even with it set if the fixture lived under the OTHER root, since
/// neither root is a superset of the other (#3104).
fn candidate_base_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(r) = datasets_root() {
        roots.push(r);
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

/// Does `dir` carry at least one `*-Data.db`? "The directory resolved" is not
/// "the fixture is usable": a candidate root can hold a same-named directory
/// carrying only the JSONL sidecar.
fn usable(dir: &std::path::Path) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten().any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// The `data_db_bit_flip` corruption fixture directory, resolved by EVIDENCE
/// across every candidate base root and gated per #1094 doctrine: `None`
/// (after an eprintln SKIP, or a panic under `CQLITE_REQUIRE_FIXTURES=1`) when
/// no candidate root carries a usable copy.
fn corruption_fixture_dir(case: &str) -> Option<PathBuf> {
    let found = candidate_base_roots()
        .into_iter()
        .map(|root| root.join("corruption/test_comp_corrupt").join(case))
        .find(|dir| usable(dir));
    let Some(dir) = found else {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but {case} is unusable under every candidate base \
             root: {:?}",
            candidate_base_roots()
        );
        eprintln!(
            "SKIP: {case} unusable under every candidate base root ({:?})",
            candidate_base_roots()
        );
        return None;
    };
    Some(dir)
}

fn data_db_bit_flip_dir() -> Option<PathBuf> {
    corruption_fixture_dir("data_db_bit_flip")
}

fn run_verify(dir: &std::path::Path, out: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "verify",
            &dir.display().to_string(),
            "--mode",
            "full",
            "--out",
            out,
        ])
        .output()
        .expect("spawn cqlite verify");
    // verify_sstable's contract (unchanged by this issue): exit 2 on any
    // finding, never a usage-error exit. Assert it here so a future
    // regression in exit-code plumbing fails this suite too, not just
    // silently produces empty stdout.
    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit 2 (verification failed) for a corrupt fixture; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

#[test]
fn cli_text_output_names_the_chunk_index_and_a_partition_key() {
    let Some(dir) = data_db_bit_flip_dir() else {
        return;
    };
    let stdout = run_verify(&dir, "text");
    assert!(
        stdout.contains("ChunkDecompressionError"),
        "stdout did not name the finding class: {stdout}"
    );
    assert!(
        stdout.contains("location:") && stdout.contains("chunk 0"),
        "stdout did not render a location naming chunk 0: {stdout}"
    );
    // The oracle-pinned partition key for this fixture (issue_4194_verify_location.rs
    // asserts the full set independently) — this CLI-level check only needs
    // AT LEAST one partition key rendered, per spec L4.2's "at least one".
    assert!(
        stdout.contains("partition(s):"),
        "stdout did not render any partition key: {stdout}"
    );
}

#[test]
fn cli_json_output_carries_a_location_object_with_chunk_index_and_partitions() {
    let Some(dir) = data_db_bit_flip_dir() else {
        return;
    };
    let stdout = run_verify(&dir, "json");
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("cqlite verify --out json did not emit valid JSON: {e}\n{stdout}")
    });
    let findings = value["findings"].as_array().expect("findings array");

    let chunk_finding = findings
        .iter()
        .find(|f| f["class"] == "ChunkDecompressionError")
        .unwrap_or_else(|| panic!("no ChunkDecompressionError finding in {value}"));
    let location = &chunk_finding["location"];
    assert!(
        !location.is_null(),
        "ChunkDecompressionError finding's location was null: {value}"
    );
    assert_eq!(location["chunk_index"], 0);
    assert_eq!(location["component"], "Data.db");
    // spec L6.3 (roborev job 92 MEDIUM): the machine channel must disclose
    // WHICH reading the physical fields carry. A chunk decompression failure
    // damages bytes that are present, so this one is a damaged extent — a
    // JSON consumer reading byte_offset/byte_len needs that stated, not
    // inferred from the finding class.
    assert_eq!(
        location["anchor"], "damaged_extent",
        "location must disclose its physical-anchor reading: {location}"
    );
    let resolved = location["partitions"]["resolved"]
        .as_array()
        .unwrap_or_else(|| {
            panic!("location.partitions was not {{\"resolved\": [...]}}: {location}")
        });
    assert!(
        !resolved.is_empty(),
        "resolved partitions array was empty: {location}"
    );
    assert!(
        resolved[0]["key_hex"].is_string(),
        "resolved partition entry missing key_hex: {location}"
    );

    // A companion finding with no natural byte range (DigestMismatch, a
    // whole-file check) renders EXACTLY as it did before this change — its
    // `location` is JSON `null`, additive-only (spec L4.2).
    let digest_finding = findings
        .iter()
        .find(|f| f["class"] == "DigestMismatch")
        .unwrap_or_else(|| panic!("no DigestMismatch finding in {value}"));
    assert!(
        digest_finding["location"].is_null(),
        "DigestMismatch (no natural byte range) unexpectedly carried a location: {digest_finding}"
    );
}

// ---------------------------------------------------------------------------
// Spec L6.3, the OTHER anchor value — `"declared_record"`.
//
// Found by the spec-auditor at 135eda0d6: `grep -rn "declared_record"
// --include=*.rs` matched exactly ONE line, the match arm that PRODUCES it
// (`cqlite-cli/src/commands/verify.rs:160`). Nothing asserted it. So a typo
// in that string literal — `"declared_recrod"`, or silently swapping the two
// arms — would have shipped green, in precisely the machine-readable channel
// L6 exists to protect, and the `damaged_extent` case above would not have
// noticed because it exercises the other arm.
//
// `data_db_truncation` is the fixture shape that produces it: a truncation
// makes declared chunk offsets outrun the file, which is the ONLY corruption
// class whose physical range is a location the metadata DECLARES rather than
// damaged bytes that are present. Measured on the real fixture:
//   {"byte_offset": 4102, "byte_len": 4, "anchor": "declared_record",
//    "chunk_index": 7, ...}
// ---------------------------------------------------------------------------

#[test]
fn cli_json_discloses_a_declared_record_anchor_for_a_truncated_fixture() {
    let Some(dir) = corruption_fixture_dir("data_db_truncation") else {
        return;
    };
    let stdout = run_verify(&dir, "json");
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("cqlite verify --out json did not emit valid JSON: {e}\n{stdout}")
    });
    let findings = value["findings"].as_array().expect("findings array");

    let oob = findings
        .iter()
        .find(|f| f["class"] == "ChunkOffsetOutOfBounds")
        .unwrap_or_else(|| panic!("no ChunkOffsetOutOfBounds finding in {value}"));
    let location = &oob["location"];
    assert!(
        !location.is_null(),
        "ChunkOffsetOutOfBounds finding's location was null: {value}"
    );
    // THE assertion this case exists for: the string literal itself.
    assert_eq!(
        location["anchor"], "declared_record",
        "a truncation's physical range is DECLARED by metadata, not damaged bytes that are \
         present; the JSON channel must say which reading it carries: {location}"
    );
    // ...and it must be the OTHER value, not both arms collapsed into one.
    assert_ne!(
        location["anchor"], "damaged_extent",
        "the two anchor readings must be distinguishable in JSON: {location}"
    );
    // The fields the anchor qualifies are still present and typed, so a
    // consumer that branches on `anchor` has something to branch over.
    assert!(
        location["byte_offset"].is_u64() && location["byte_len"].is_u64(),
        "declared-record locations must still carry typed byte fields: {location}"
    );
    assert!(
        location["chunk_index"].is_u64(),
        "a chunk-anchored finding must name its chunk index: {location}"
    );
}
