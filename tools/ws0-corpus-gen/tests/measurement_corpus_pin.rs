//! The measurement-corpus pin, checked against the COMMITTED artifact — plus the
//! `#[ignore]`d full-size verification and its operator command (issue #3272,
//! item 9).
//!
//! # Option (b), and why
//!
//! The issue offers (a) "pin it as a constant with a test that verifies it, only
//! if that test can run without generating 4M rows" or (b) "the explicit
//! documented reason it cannot, plus the operator procedure". This is **(b)**,
//! because (a)'s precondition genuinely fails: reproducing
//! [`DATA_DB_SHA256`] requires WRITING the 4,000,000-row corpus — ~2.8 GB and
//! minutes of CPU — and re-folding [`ARROW_BUFFER_DIGEST`] requires reading it back
//! through the Flight producer. Neither belongs in a gate component. There is no
//! shortcut: a digest is not derivable from a smaller corpus's digest, by design.
//!
//! So what lands is (b) done as tightly as possible, which is more than the issue
//! asks for:
//!
//! 1. The values are CONSTANTS IN SOURCE ([`ws0_corpus_gen::measurement_corpus`]),
//!    not doc prose.
//! 2. This test proves those constants EQUAL the committed
//!    `docs/reports/ws0-3096-artifacts/corpus-identity.json`, field by field — so
//!    the source pin and the recorded artifact cannot drift apart, and editing
//!    either alone reds the gate. That check needs no corpus: it reads a committed
//!    JSON file. **This is the machine-checked half, and it is what makes the pin
//!    a pin rather than a second copy of the prose.**
//! 3. [`the_full_size_verification_is_an_operator_procedure`] is the `#[ignore]`d
//!    test that WOULD verify the corpus end-to-end, carrying the exact commands.
//!    It is not `#[ignore]`d for being slow-but-fine — it is `#[ignore]`d because
//!    it requires an out-of-repo scratch volume, which a gate does not have.
//!
//! # Non-vacuity
//!
//! The artifact-vs-source comparison is only evidence if it can FAIL, and a JSON
//! reader that silently yields `None` on a missing field would make every
//! assertion below vacuous. So [`the_artifact_comparison_can_fail`] runs the SAME
//! comparison against a DELIBERATELY PERTURBED copy of the artifact and asserts it
//! reports the divergence — and the artifact-reading helper PANICS on an absent or
//! unparseable field rather than defaulting, so an artifact that lost a field
//! cannot read as agreement.
//!
//! # PERFORMANCE FIXTURE ONLY (#3042)
//!
//! Every value here describes a CQLite-written, CQLite-read corpus. It is a
//! DETERMINISM/IDENTITY pin, never a correctness oracle.

use std::path::{Path, PathBuf};

use ws0_corpus_gen::measurement_corpus as mc;

/// The committed artifact this pin is anchored to.
const ARTIFACT: &str = "docs/reports/ws0-3096-artifacts/corpus-identity.json";

/// Repo root, resolved from this crate's manifest dir (`tools/ws0-corpus-gen`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("tools/ws0-corpus-gen has a grandparent")
        .to_path_buf()
}

/// Read a required field out of the committed artifact.
///
/// PANICS on an absent/unparseable field. Deliberately: a `None`-tolerant reader
/// would let an artifact that lost `data_db_sha256` compare "equal" to the pin,
/// which is the fail-open shape this whole issue is about (a value not observed is
/// an error, never a default).
fn field(json: &serde_json::Value, key: &str) -> serde_json::Value {
    json.get(key)
        .cloned()
        .unwrap_or_else(|| panic!("{ARTIFACT} has no `{key}` field — the pin cannot be verified"))
}

fn read_artifact() -> serde_json::Value {
    let path = repo_root().join(ARTIFACT);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read the committed pin artifact {path:?}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path:?} is not valid JSON: {e}"))
}

/// Every way the in-source pin and the committed artifact disagree. EMPTY = agree.
///
/// Split out from the assertion so [`the_artifact_comparison_can_fail`] can drive
/// it against a perturbed artifact — the same code path, not a reimplementation.
fn pin_vs_artifact(json: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut u64_eq = |key: &str, pinned: u64| {
        let got = field(json, key)
            .as_u64()
            .unwrap_or_else(|| panic!("{ARTIFACT}.{key} is not an unsigned integer"));
        if got != pinned {
            out.push(format!("{key}: artifact {got} != pinned {pinned}"));
        }
    };
    u64_eq("rows", mc::ROWS);
    u64_eq("partitions", mc::PARTITIONS);
    u64_eq("rows_per_partition", mc::ROWS_PER_PARTITION);
    u64_eq("data_db_bytes", mc::DATA_DB_BYTES);
    u64_eq("total_component_bytes", mc::TOTAL_COMPONENT_BYTES);
    u64_eq("cells_per_row", mc::CELLS_PER_ROW as u64);

    let sha = field(json, "data_db_sha256");
    let sha = sha
        .as_str()
        .unwrap_or_else(|| panic!("{ARTIFACT}.data_db_sha256 is not a string"));
    if sha != mc::DATA_DB_SHA256 {
        out.push(format!(
            "data_db_sha256: artifact {sha} != pinned {}",
            mc::DATA_DB_SHA256
        ));
    }
    let bpr = field(json, "bytes_per_row")
        .as_f64()
        .unwrap_or_else(|| panic!("{ARTIFACT}.bytes_per_row is not a number"));
    if (bpr - mc::BYTES_PER_ROW).abs() > 1e-6 {
        out.push(format!(
            "bytes_per_row: artifact {bpr} != pinned {}",
            mc::BYTES_PER_ROW
        ));
    }
    // Issue #1406: the corpus is uncompressed, and the artifact must say so.
    if field(json, "compression_info_present").as_bool() != Some(false) {
        out.push("compression_info_present: artifact does not record `false` (#1406)".to_string());
    }
    out
}

/// THE machine-checked half: the in-source pin equals the committed artifact.
///
/// Runs in the gate. Reads a committed JSON file; generates nothing.
#[test]
fn the_in_source_pin_matches_the_committed_artifact() {
    let json = read_artifact();
    let diffs = pin_vs_artifact(&json);
    assert!(
        diffs.is_empty(),
        "the in-source measurement-corpus pin (tools/ws0-corpus-gen/src/measurement_corpus.rs) \
         disagrees with the committed {ARTIFACT}. ONE of them was edited alone; both must move \
         together, and whichever is wrong must be corrected against a real re-run \
         (see ws0_corpus_gen::measurement_corpus::OPERATOR_VERIFY_CORPUS):\n  {}",
        diffs.join("\n  ")
    );
    // The Data.db component's own recorded digest must equal the top-level one —
    // an internally inconsistent artifact must not read as agreement with the pin.
    let components = field(&json, "components");
    let data = components
        .as_object()
        .expect("components is an object")
        .iter()
        .find(|(k, _)| k.ends_with("-Data.db"))
        .map(|(_, v)| v.clone())
        .expect("the artifact records a *-Data.db component");
    assert_eq!(
        data.get("sha256").and_then(|v| v.as_str()),
        Some(mc::DATA_DB_SHA256),
        "the artifact's Data.db COMPONENT digest disagrees with the pinned digest"
    );
    assert_eq!(
        data.get("bytes").and_then(|v| v.as_u64()),
        Some(mc::DATA_DB_BYTES),
        "the artifact's Data.db COMPONENT size disagrees with the pinned size"
    );
}

/// NON-VACUITY: the comparison above REPORTS a perturbed artifact.
///
/// Without this, `the_in_source_pin_matches_the_committed_artifact` would also
/// pass against a `pin_vs_artifact` that returned `vec![]` unconditionally — the
/// #3249 shape exactly (a hardcoded `ok` surviving every test). Each perturbation
/// is applied to an in-memory copy of the real artifact; the file is never
/// written.
#[test]
fn the_artifact_comparison_can_fail() {
    let base = read_artifact();

    /// One perturbation of the committed artifact, applied in memory.
    type Perturb = Box<dyn Fn(&mut serde_json::Value)>;

    let cases: Vec<(&str, Perturb)> = vec![
        (
            "rows",
            Box::new(|j: &mut serde_json::Value| j["rows"] = serde_json::json!(3_999_999)),
        ),
        (
            "partitions",
            Box::new(|j: &mut serde_json::Value| j["partitions"] = serde_json::json!(39_999)),
        ),
        (
            "data_db_bytes",
            Box::new(|j: &mut serde_json::Value| j["data_db_bytes"] = serde_json::json!(1)),
        ),
        (
            "data_db_sha256",
            Box::new(|j: &mut serde_json::Value| {
                // A ONE-CHARACTER change: the comparison must be exact, not a
                // prefix/length check.
                let s: String = mc::DATA_DB_SHA256.to_string();
                let mut b = s.into_bytes();
                b[0] = if b[0] == b'4' { b'5' } else { b'4' };
                j["data_db_sha256"] = serde_json::json!(String::from_utf8_lossy(&b));
            }),
        ),
        (
            "bytes_per_row",
            Box::new(|j: &mut serde_json::Value| j["bytes_per_row"] = serde_json::json!(1.0)),
        ),
        (
            "cells_per_row",
            Box::new(|j: &mut serde_json::Value| j["cells_per_row"] = serde_json::json!(11)),
        ),
        (
            "total_component_bytes",
            Box::new(|j: &mut serde_json::Value| j["total_component_bytes"] = serde_json::json!(7)),
        ),
        (
            "compression_info_present",
            Box::new(|j: &mut serde_json::Value| {
                j["compression_info_present"] = serde_json::json!(true)
            }),
        ),
    ];
    assert_eq!(
        cases.len(),
        8,
        "every field pin_vs_artifact compares needs a perturbation case"
    );
    for (field_name, perturb) in cases {
        let mut j = base.clone();
        perturb(&mut j);
        let diffs = pin_vs_artifact(&j);
        assert!(
            !diffs.is_empty(),
            "a perturbed `{field_name}` read as AGREEMENT — the pin comparison is vacuous for \
             that field"
        );
        assert!(
            diffs.iter().any(|m| m.starts_with(field_name)),
            "the divergence must NAME `{field_name}`; got {diffs:?}"
        );
    }

    // And the unperturbed artifact still agrees — a comparison that fails on
    // everything is equally useless (the positive control).
    assert!(
        pin_vs_artifact(&base).is_empty(),
        "the UNPERTURBED artifact must agree with the pin"
    );
}

/// The pinned Arrow-buffer digest's relationship to the corpus shape, checked
/// without a corpus.
///
/// This is the half of [`mc::ARROW_BUFFER_DIGEST`] that IS machine-checkable: the
/// digest itself needs a 4M-row fold, but the batch accounting it was recorded
/// with does not — and that accounting is where the artifact's "(batch 8192)"
/// label was found to be arithmetically impossible (see the doc comment on
/// [`mc::ARROW_BUFFER_BATCH_SIZE`]).
#[test]
fn the_pinned_digest_carries_a_consistent_batch_accounting() {
    assert_eq!(
        mc::ROWS / mc::ARROW_BUFFER_BATCH_SIZE,
        mc::ARROW_BUFFER_BATCHES,
        "the pinned batch size and batch count cannot both be true"
    );
    assert_ne!(mc::ARROW_BUFFER_DIGEST, 0, "a 0 digest is a sentinel");
}

/// The full-size verification, as an EXECUTABLE record of the operator procedure.
///
/// `#[ignore]`d — and the reason is a hard constraint, not a preference:
///
/// * it writes ~2.8 GB, which needs an out-of-repo scratch volume no gate has;
/// * it takes minutes of CPU (measured: the 2026-08-03 generation of this corpus);
/// * and re-folding the Arrow digest reads all of it back through Flight.
///
/// Run it deliberately, with an explicit scratch root:
///
/// ```bash
/// CQLITE_WS0_VERIFY_ROOT=/data/ws0-3096-verify \
///   cargo test --release -p ws0-corpus-gen --test measurement_corpus_pin -- \
///   --ignored --nocapture the_full_size_verification_is_an_operator_procedure
/// ```
///
/// It fails closed if `CQLITE_WS0_VERIFY_ROOT` is unset: an `--ignored` run the
/// operator asked for must not silently do nothing. The Arrow-digest half is NOT
/// run here (it lives in `cqlite-flight`, a different crate) — the command is
/// printed instead, from [`mc::OPERATOR_VERIFY_DIGEST`], so the two halves of the
/// procedure stay together.
#[test]
#[ignore = "writes ~2.8 GB and takes minutes; needs CQLITE_WS0_VERIFY_ROOT (see the doc comment)"]
fn the_full_size_verification_is_an_operator_procedure() {
    let root = std::env::var("CQLITE_WS0_VERIFY_ROOT").unwrap_or_else(|_| {
        panic!(
            "CQLITE_WS0_VERIFY_ROOT is unset. This test writes ~2.8 GB — point it at a scratch \
             volume OUTSIDE the repo. It fails rather than skipping: an --ignored test the \
             operator explicitly selected must never quietly measure nothing.\n\n\
             Corpus verification:\n{}\n\nDigest verification:\n{}",
            mc::OPERATOR_VERIFY_CORPUS,
            mc::OPERATOR_VERIFY_DIGEST
        )
    });

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let spec = mc::spec(PathBuf::from(&root));
    eprintln!(
        "generating the FULL {} -row measurement corpus into {root} — minutes, ~2.8 GB…",
        mc::ROWS
    );
    let identity = rt
        .block_on(ws0_corpus_gen::generate::generate(&spec))
        .expect("full-size generation");

    // Compared against the IN-SOURCE pin, which the gate has already proved equal
    // to the committed artifact — so one comparison covers both.
    assert_eq!(identity.rows, mc::ROWS, "row count");
    assert_eq!(identity.partitions, mc::PARTITIONS, "partition count");
    assert_eq!(
        identity.data_db_bytes,
        mc::DATA_DB_BYTES,
        "Data.db size moved"
    );
    assert_eq!(
        identity.data_db_sha256,
        mc::DATA_DB_SHA256,
        "THE MEASUREMENT CORPUS NO LONGER REPRODUCES. Every #3096 figure was measured over \
         sha256 {}; this generation produced {}. Report the divergence — do NOT re-pin the \
         constant to make it agree.",
        mc::DATA_DB_SHA256,
        identity.data_db_sha256
    );
    assert_eq!(
        identity.total_component_bytes,
        mc::TOTAL_COMPONENT_BYTES,
        "total component bytes moved"
    );
    assert!(
        !identity.compression_info_present,
        "a CompressionInfo.db appeared (#1406)"
    );
    eprintln!(
        "corpus verified: {} rows / {} partitions / Data.db {} B / sha256 {}\n\
         now re-fold the Arrow-buffer digest:\n{}",
        identity.rows,
        identity.partitions,
        identity.data_db_bytes,
        identity.data_db_sha256,
        mc::OPERATOR_VERIFY_DIGEST
    );
}
