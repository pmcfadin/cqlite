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

/// Fields of the committed artifact that are DELIBERATELY not compared, each with the
/// reason (#3272 review B2).
///
/// This exists so [`the_comparison_covers_every_artifact_field`] can be derived from the
/// artifact's ACTUAL key set: any field present in the artifact and absent from both
/// the comparison and this list FAILS. The previous exhaustiveness check counted the
/// perturbation CASES (`cases.len() == 8`), which enumerated only the fields already
/// compared — it certified its own scope, and could not have caught the missing `seed`
/// and `table` anchors that prompted this.
// Each reason NAMES ITS FIELD and states WHY the field is not a pin — the two properties
// asserted at the bottom of `the_comparison_covers_every_artifact_field`. Writing the field
// name into the reason is what makes a copied-from-a-neighbour entry detectable; the old
// `reason.len() > 20` could not see one at all (#3272 review round 2 nit).
const DELIBERATELY_NOT_COMPARED: &[(&str, &str)] = &[
    (
        "issue",
        "`issue` is a provenance label, not a measured quantity; #3096 is stated in prose \
         everywhere this corpus is described",
    ),
    (
        "components",
        "`components` is compared separately and more strictly in \
         the_in_source_pin_matches_the_committed_artifact, which asserts the Data.db \
         COMPONENT's own sha256 and size equal the pinned ones",
    ),
    (
        "not_a_correctness_oracle",
        "`not_a_correctness_oracle` is the #3042 disclaimer string; its wording is prose \
         and not a pin",
    ),
    (
        "differs_from_prior_corpus",
        "`differs_from_prior_corpus` is prose recording which digest this corpus is NOT \
         (#3058/#3100), so there is nothing to compare it against",
    ),
];

/// Every artifact key `pin_vs_artifact` compares, in comparison order.
///
/// Named separately from the comparison so the exhaustiveness test can subtract it
/// from the artifact's real key set. Kept beside the comparison body: adding a
/// comparison without adding its key here makes the coverage test's arithmetic
/// disagree, which is visible rather than silent.
const COMPARED_FIELDS: &[&str] = &[
    "seed",
    "table",
    "rows",
    "partitions",
    "rows_per_partition",
    "data_db_bytes",
    "total_component_bytes",
    "cells_per_row",
    "data_db_sha256",
    "bytes_per_row",
    "compression_info_present",
];

/// Fields that ARE compared, but which the committed 2026-08-03 artifact PREDATES — so their
/// absence from that artifact is accounted for rather than either red or silent (#3272 review
/// round 7, F6).
///
/// # The decision this records, made NOW rather than on the regeneration
///
/// `schema_sha256` was added to [`CorpusIdentity`] by R2. The committed artifact was recorded
/// before the field existed, so it carries 15 keys and not this one — which is why
/// [`the_comparison_covers_every_artifact_field`] passed while the field was in neither
/// [`COMPARED_FIELDS`] nor [`DELIBERATELY_NOT_COMPARED`]: the check derives its subject from the
/// artifact's REAL key set, and a key the artifact does not carry is not in that subject.
///
/// That left a trap on the documented remedy: the next real regeneration emits the key, and the
/// coverage check would then red it as an unaccounted field — i.e. the honest fix (regenerate)
/// would break the gate, and the tempting fix (add it to the exempt list) would park a field
/// with a complete machine oracle as "not a pin". So the decision is recorded here instead:
///
/// **COMPARED, not exempted.** `mc::SCHEMA_SHA256` is asserted equal to `sha256(DDL + "\n")` by
/// [`the_pinned_schema_digest_is_the_digest_of_the_ddl_that_is_written`], which is the strongest
/// oracle any value in this pin has (the INPUT, not a record of it). A field with that oracle is
/// exactly what a pin is for.
///
/// The third state is what keeps this from being a permissive branch: absence is not "skip",
/// it is a NAMED, ASSERTED condition — [`the_schema_digest_is_compared_or_recorded_as_pre_pin`]
/// fails unless the key is either present-and-equal-to-the-pin, or absent-and-listed-here.
/// There is no branch in which nothing is checked.
const COMPARED_WHEN_PRESENT: &[(&str, &str)] = &[(
    "schema_sha256",
    "`schema_sha256` is absent from this artifact BECAUSE #3272 R2 added it to CorpusIdentity \
     AFTER the artifact was recorded (2026-08-03), so the 15-key record cannot carry it. It IS \
     compared — against mc::SCHEMA_SHA256, itself asserted equal to sha256(DDL + \"\\n\") — the \
     moment a regeneration emits it; see the_schema_digest_is_compared_or_recorded_as_pre_pin",
)];

/// Every way the in-source pin and the committed artifact disagree. EMPTY = agree.
///
/// Split out from the assertion so [`the_artifact_comparison_can_fail`] can drive
/// it against a perturbed artifact — the same code path, not a reimplementation.
///
/// # The INPUT anchors, and why their absence made the whole pin unmoored (#3272 B2)
///
/// This compared the artifact's OUTPUT quantities (`rows`, `data_db_bytes`,
/// `data_db_sha256`, …) and never the INPUT that produced them. MEASURED: changing
/// [`ws0_corpus_gen::generate::DEFAULT_SEED`] from `30_960_001` to `99_999_999` and
/// running `cargo test -p ws0-corpus-gen` left ALL 47 tests GREEN — the determinism
/// tests use the constant symmetrically (both generations get the new seed, so they
/// still agree), and nothing compared it to the artifact. Yet [`mc::DATA_DB_SHA256`] is
/// the digest of a corpus generated at `30_960_001`, so after such an edit the pinned
/// digest is not reproducible by ANY code path in the repo: the operator procedure
/// would regenerate at the new seed and "fail" against a digest nothing could produce.
///
/// A digest pin is only a pin together with the inputs that determine it. So the seed
/// and the table identity are compared against the constants the generator actually
/// uses — `generate::DEFAULT_SEED` and `schema::{KEYSPACE, TABLE}` — not against
/// literals retyped here, which would be a third copy free to drift from both.
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
    // THE INPUT ANCHOR. `DEFAULT_SEED` is read from the generator, so this comparison
    // moves the moment the generator's seed does.
    u64_eq("seed", ws0_corpus_gen::generate::DEFAULT_SEED);
    u64_eq("rows", mc::ROWS);
    u64_eq("partitions", mc::PARTITIONS);
    u64_eq("rows_per_partition", mc::ROWS_PER_PARTITION);
    u64_eq("data_db_bytes", mc::DATA_DB_BYTES);
    u64_eq("total_component_bytes", mc::TOTAL_COMPONENT_BYTES);
    u64_eq("cells_per_row", mc::CELLS_PER_ROW as u64);

    // The other INPUT anchor: which table these bytes are of. Built from the schema
    // constants the generator writes with, exactly as `generate()` builds the field.
    let expected_table = format!(
        "{}.{}",
        ws0_corpus_gen::schema::KEYSPACE,
        ws0_corpus_gen::schema::TABLE
    );
    let table = field(json, "table");
    let table = table
        .as_str()
        .unwrap_or_else(|| panic!("{ARTIFACT}.table is not a string"));
    if table != expected_table {
        out.push(format!(
            "table: artifact {table} != pinned {expected_table}"
        ));
    }

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
    // THE SCHEMA DIGEST, compared WHEN THE ARTIFACT CARRIES IT (#3272 review round 7, F6).
    //
    // The committed 2026-08-03 artifact predates the field, so `field()` (which PANICS on an
    // absent key, deliberately) cannot be used here. `get` is, and the absence is NOT a
    // permissive branch: it is accounted for by `COMPARED_WHEN_PRESENT` and asserted by
    // `the_schema_digest_is_compared_or_recorded_as_pre_pin`, which fails unless the key is
    // either present-and-equal or absent-and-listed. So a post-regeneration artifact that
    // carries a WRONG digest is caught here, and one that carries none is caught there.
    if let Some(v) = json.get("schema_sha256") {
        match v.as_str() {
            Some(sha) if sha == mc::SCHEMA_SHA256 => {}
            Some(sha) => out.push(format!(
                "schema_sha256: artifact {sha} != pinned {}",
                mc::SCHEMA_SHA256
            )),
            None => out.push(format!(
                "schema_sha256: artifact records {v}, which is not a string"
            )),
        }
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
         (see ws0_corpus_gen::measurement_corpus::operator_verify_corpus):\n  {}",
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
            // THE INPUT ANCHOR (#3272 review B2). Without this case, `pin_vs_artifact`
            // could stop comparing `seed` and every other assertion here would still
            // pass — which is precisely the state the review found.
            "seed",
            Box::new(|j: &mut serde_json::Value| j["seed"] = serde_json::json!(1)),
        ),
        (
            "table",
            Box::new(|j: &mut serde_json::Value| j["table"] = serde_json::json!("somewhere.else")),
        ),
        (
            "rows",
            Box::new(|j: &mut serde_json::Value| j["rows"] = serde_json::json!(3_999_999)),
        ),
        (
            "partitions",
            Box::new(|j: &mut serde_json::Value| j["partitions"] = serde_json::json!(39_999)),
        ),
        (
            // FOUND BY the exhaustiveness rewrite above: `rows_per_partition` was
            // compared by `pin_vs_artifact` with NO perturbation case, so its comparison
            // was unproven — the old `cases.len() == 8` could not see that, because 8
            // was the number of cases, not the number of comparisons.
            "rows_per_partition",
            Box::new(|j: &mut serde_json::Value| j["rows_per_partition"] = serde_json::json!(99)),
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
    // Every COMPARED field needs a perturbation case, checked against the comparison's
    // own field list rather than against a hardcoded count. `cases.len() == 8` used to
    // stand here, and it could not fail for a field the comparison had never covered:
    // it enumerated the cases, and the cases enumerated the comparisons — the assert
    // certified its own scope (#3272 review B2).
    let case_names: Vec<&str> = cases.iter().map(|(n, _)| *n).collect();
    let uncovered: Vec<&&str> = COMPARED_FIELDS
        .iter()
        .filter(|f| !case_names.contains(f))
        .collect();
    assert!(
        uncovered.is_empty(),
        "pin_vs_artifact compares {uncovered:?} with no perturbation case, so the \
         comparison of those fields is unproven — a `pin_vs_artifact` that silently \
         stopped checking them would still pass this test"
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

/// EVERY field the committed artifact carries is either COMPARED or explicitly
/// exempted — derived from the artifact's ACTUAL key set (#3272 review B2).
///
/// This is the assert `cases.len() == 8` was pretending to be. That one enumerated the
/// perturbation cases, which enumerated the comparisons, so its "exhaustiveness" was
/// over the fields already covered — it could never see a field the artifact carried and
/// the comparison ignored. Which is exactly how `seed` (the INPUT that determines the
/// pinned digest) and `table` went uncompared: MEASURED, changing
/// `generate::DEFAULT_SEED` from `30_960_001` to `99_999_999` left all 47 tests GREEN
/// while `DATA_DB_SHA256` became unreproducible by any code path.
///
/// So the direction is inverted: read the artifact, subtract what is compared, subtract
/// what is deliberately exempted WITH A STATED REASON, and FAIL on whatever is left. A
/// new artifact field is then a decision someone has to make, not a silent gap.
#[test]
fn the_comparison_covers_every_artifact_field() {
    let json = read_artifact();
    let obj = json
        .as_object()
        .expect("the committed artifact is a JSON object");
    let exempt: Vec<&str> = DELIBERATELY_NOT_COMPARED.iter().map(|(k, _)| *k).collect();
    // `COMPARED_WHEN_PRESENT` counts as ACCOUNTED FOR, because those fields ARE compared by
    // `pin_vs_artifact` — see `COMPARED_WHEN_PRESENT`'s own doc for why they are listed
    // separately from `COMPARED_FIELDS` (the artifact predates them, so the `absent` direction
    // below must not red on them). This is what makes the documented regeneration remedy land
    // green instead of turning the honest fix into a gate failure (#3272 F6).
    let when_present: Vec<&str> = COMPARED_WHEN_PRESENT.iter().map(|(k, _)| *k).collect();
    let unaccounted: Vec<&String> = obj
        .keys()
        .filter(|k| {
            !COMPARED_FIELDS.contains(&k.as_str())
                && !exempt.contains(&k.as_str())
                && !when_present.contains(&k.as_str())
        })
        .collect();
    assert!(
        unaccounted.is_empty(),
        "{ARTIFACT} carries field(s) {unaccounted:?} that pin_vs_artifact neither \
         compares nor lists in DELIBERATELY_NOT_COMPARED. Every field is a decision: \
         either compare it against an in-source constant, or record WHY it is not a \
         pin. An unaccounted field is a quantity the artifact asserts and nothing \
         checks."
    );
    // The reverse direction: a COMPARED field that the artifact does not carry would
    // make `field()` panic at some point — surfaced here as a named failure instead.
    let absent: Vec<&&str> = COMPARED_FIELDS
        .iter()
        .filter(|f| !obj.contains_key(**f))
        .collect();
    assert!(
        absent.is_empty(),
        "pin_vs_artifact compares field(s) {absent:?} that {ARTIFACT} does not carry"
    );
    // Every exemption must NAME ITS FIELD and say why that field is not a pin.
    //
    // This used to be `reason.len() > 20` (#3272 review round 2 nit) — a test of LENGTH
    // where the property is CONTENT. A 21-character placeholder ("not compared for now.")
    // satisfies it, which is precisely how a real gap gets parked as an audited exemption:
    // the check reads as coverage, the reason reads as a decision, and nobody looks again.
    //
    // The property is stated instead, and it is checkable without judging prose:
    //
    //  * the reason MENTIONS THE FIELD, so it is about this exemption rather than copied
    //    from a neighbour (the copy-paste failure a length test cannot see at all);
    //  * it says WHY, evidenced by a rationale connective — and the acceptable set is
    //    small and CLOSED, so a reason that merely restates the field name does not pass;
    //  * it is not one of the placeholder forms.
    //
    // This is still a proxy for "a human wrote a real reason", and it is deliberately a
    // WEAK one — a strong one would be judging English. The strength is elsewhere: the
    // exemption list is EXHAUSTIVE against the artifact's real key set above, so an
    // exemption cannot be added without appearing here, and there are four of them,
    // reviewed in code. What is closed here is the specific way a length test fails.
    for (key, reason) in DELIBERATELY_NOT_COMPARED {
        assert!(
            reason_is_acceptable(key, reason),
            "the exemption for `{key}` must NAME the field and say WHY it is not compared, \
             and must not be a placeholder. This replaced a `reason.len() > 20` check, \
             which a 21-character placeholder satisfied (#3272 review round 2). See \
             `reason_is_acceptable` for the three properties; got {reason:?}"
        );
    }
}

/// The three properties an exemption reason must have. Named so
/// [`the_exemption_reason_check_rejects_a_placeholder_and_a_copied_reason`] drives the
/// SAME predicate the assertion uses — a re-implemented check in the non-vacuity test
/// would be a second thing to keep in sync, and its divergence would be invisible in
/// exactly the permissive direction.
///
/// It is a deliberately WEAK proxy for "a human wrote a real reason"; a strong one would be
/// judging English. The strength is elsewhere: the exemption list is EXHAUSTIVE against the
/// artifact's real key set, so an exemption cannot be added without appearing in code
/// review. What this closes is the specific way a LENGTH test fails — it cannot see a
/// placeholder, and it cannot see a reason copied from a neighbouring entry.
fn reason_is_acceptable(key: &str, reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    // (1) NAMES ITS FIELD — and NAMES IT, with no key-independent escape hatch (#3272
    // review round 3 nit).
    //
    // This used to read:
    //
    //     lower.contains(&key.to_ascii_lowercase())
    //         || lower.contains("compared separately")
    //         || lower.contains("digest")
    //
    // The two alternatives are KEY-INDEPENDENT, so they satisfy property (1) for EVERY key
    // — and `"compared separately"` is also in the RATIONALE set below, so it satisfies (2)
    // as well. MEASURED: the shipped `components` reason, copy-pasted verbatim onto a new
    // exemption, is ACCEPTED for `rows`, for `partitions` and for `seed`. That is the
    // COPY-PASTE CASE THIS CHECK EXISTS TO CATCH, waved through by the escape hatch added
    // to accommodate a field name that "does not read naturally in prose".
    //
    // The accommodation is kept but made KEY-BOUND: a reason may name the field, or the
    // field's SNAKE-CASE WORDS individually (`not_a_correctness_oracle` reads as "the #3042
    // disclaimer", so requiring the literal identifier would force awkward prose). Both
    // forms are ABOUT THIS KEY, which a blanket phrase is not.
    let key_lower = key.to_ascii_lowercase();
    let names_field = lower.contains(&key_lower)
        || key_lower
            .split('_')
            // Words too short to identify a field ("a", "of", "not") would reintroduce the
            // key-independent hole through the back door.
            .filter(|w| w.len() >= 4)
            .all(|w| lower.contains(w));
    // (2) SAYS WHY, from a small CLOSED set of rationale forms — so a reason that merely
    // restates the field name does not pass.
    const RATIONALE: [&str; 8] = [
        "not a measured quantity",
        "not a pin",
        "compared separately",
        "prose",
        "label",
        "disclaimer",
        "because",
        "nothing to compare",
    ];
    let says_why = RATIONALE.iter().any(|r| lower.contains(r));
    // (3) IS NOT A PLACEHOLDER — the form that parks a real gap as an audited decision.
    let placeholder = ["tbd", "todo", "fixme", "for now", "n/a reason", "see above"]
        .iter()
        .any(|p| lower.contains(p));
    names_field && says_why && !placeholder
}

/// NON-VACUITY for the exemption-reason check (#3272 review round 2 nit).
///
/// The check above replaced `reason.len() > 20`, which a 21-character placeholder
/// satisfied. So the replacement is itself driven over the inputs it must REJECT — the
/// placeholder that used to pass, and a reason copied from a neighbouring entry — and over
/// one it must ACCEPT. A check whose own discriminating power is unmeasured is the #3249
/// shape one level up: `_PERF_STATE="ok"` survived 118/118 tests.
///
/// The predicate is factored here so the test and the assertion cannot diverge; the
/// assertion above calls the same function.
#[test]
fn the_exemption_reason_check_rejects_a_placeholder_and_a_copied_reason() {
    // A 21-CHARACTER PLACEHOLDER: passes `len() > 20`, names nothing, says nothing.
    let placeholder = "not compared for now.";
    assert!(
        placeholder.len() > 20,
        "the historical check was `len() > 20`; this input must satisfy it, or it is not \
         the input that used to pass"
    );
    assert!(
        !reason_is_acceptable("rows", placeholder),
        "a 21-character placeholder must be REJECTED — it satisfied the length check it \
         replaced"
    );
    // A reason COPIED from a neighbouring entry: real prose, real rationale, WRONG FIELD.
    // A length check cannot see this at all.
    let copied = "`issue` is a provenance label, not a measured quantity";
    assert!(
        !reason_is_acceptable("partitions", copied),
        "a reason naming a DIFFERENT field must be rejected: {copied:?}"
    );
    // A reason that only RESTATES the field name says why nothing.
    assert!(
        !reason_is_acceptable("partitions", "`partitions` is the partitions field value"),
        "a reason that restates the field without a rationale must be rejected"
    );
    // The ACCEPT direction, so the check is not one that rejects everything — which is the
    // check whose exemption list someone deletes.
    for (key, reason) in DELIBERATELY_NOT_COMPARED {
        assert!(
            reason_is_acceptable(key, reason),
            "the shipped exemption for `{key}` must be acceptable: {reason:?}"
        );
    }
    assert!(
        reason_is_acceptable(
            "bytes_per_row",
            "`bytes_per_row` is derived from two fields already pinned, so it is not a pin \
             of its own"
        ),
        "a well-formed new exemption must be accepted"
    );

    // THE EXACT COPY-PASTE THE KEY-INDEPENDENT ESCAPE HATCH ADMITTED (#3272 round 3 nit).
    //
    // `names_field` used to be satisfied by the phrase `"compared separately"` or the word
    // `"digest"` IRRESPECTIVE OF KEY — and `"compared separately"` is also a RATIONALE, so
    // one blanket phrase satisfied BOTH properties. MEASURED against that version, the
    // SHIPPED `components` reason, copy-pasted verbatim, was ACCEPTED for `rows`, for
    // `partitions` and for `seed`: the copy-paste case the check exists to catch.
    let shipped_components_reason = DELIBERATELY_NOT_COMPARED
        .iter()
        .find(|(k, _)| *k == "components")
        .map(|(_, r)| *r)
        .expect("the `components` exemption is the one whose reason contains the hatch phrase");
    assert!(
        shipped_components_reason.contains("compared separately"),
        "this case needs the reason that carried the key-independent phrase; got \
         {shipped_components_reason:?}"
    );
    for foreign_key in ["rows", "partitions", "seed", "data_db_bytes"] {
        assert!(
            !reason_is_acceptable(foreign_key, shipped_components_reason),
            "the shipped `components` reason must NOT be acceptable for `{foreign_key}` — \
             copy-pasting it onto a new exemption is exactly the case this check exists to \
             catch, and the key-independent `contains(\"compared separately\")` alternative \
             admitted it for every key (#3272 round 3)"
        );
    }
    // ...and the same for the OTHER key-independent phrase.
    for foreign_key in ["rows", "partitions"] {
        assert!(
            !reason_is_acceptable(
                foreign_key,
                "`data_db_sha256` is the digest, compared separately"
            ),
            "a reason naming a DIFFERENT field's digest must not be acceptable for \
             `{foreign_key}`"
        );
    }
    // The ACCOMMODATION the hatch existed for must still work, KEY-BOUND: a field whose
    // identifier does not read naturally in prose may name its snake-case WORDS instead.
    assert!(
        reason_is_acceptable(
            "not_a_correctness_oracle",
            "this is the #3042 correctness oracle disclaimer string; its wording is prose \
             and not a pin"
        ),
        "a reason naming the field's WORDS rather than its identifier must be accepted — \
         removing the escape hatch must not force awkward prose"
    );
    // ...but the words must be THAT field's: a subset does not identify it.
    assert!(
        !reason_is_acceptable(
            "not_a_correctness_oracle",
            "this is a disclaimer string; its wording is prose and not a pin"
        ),
        "naming NONE of the field's words must be rejected, or the word rule is the old \
         blanket hatch under another name"
    );
}

/// The seed anchor, stated as its own assertion so the failure NAMES the hazard.
///
/// `pin_vs_artifact` already compares `seed`, but a divergence there prints a generic
/// "artifact X != pinned Y". This says what it means: the pinned digest was produced at
/// one specific seed, and if the generator's seed moves, that digest is no longer
/// reproducible by anything in the repo.
#[test]
fn the_pinned_digest_is_anchored_to_the_seed_that_produced_it() {
    let json = read_artifact();
    let recorded = field(&json, "seed")
        .as_u64()
        .expect("the artifact records an integer seed");
    assert_eq!(
        recorded,
        ws0_corpus_gen::generate::DEFAULT_SEED,
        "the committed corpus identity was generated at seed {recorded}, but \
         generate::DEFAULT_SEED is now {}. The pinned DATA_DB_SHA256 ({}) is the digest \
         of a corpus generated at {recorded} — with the generator on a different seed, \
         NO code path in this repo can reproduce it, and the operator procedure would \
         regenerate at the new seed and 'fail' against a digest nothing can produce. \
         Either restore the seed, or regenerate the corpus and re-pin BOTH the seed and \
         every digest together (see measurement_corpus::operator_verify_corpus).",
        ws0_corpus_gen::generate::DEFAULT_SEED,
        mc::DATA_DB_SHA256,
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
/// printed instead, from [`mc::operator_verify_digest`], so the two halves of the
/// procedure stay together.
///
/// # Two defects that made this procedure unable to verify its own output (#3272 round 4)
///
/// * IT NEVER WROTE `corpus-identity.json`. It called `generate()` directly, which
///   returns the identity in memory; only the BINARY writes the file. The digest
///   command it then printed REQUIRES that file (the Flight oracle reads the corpus
///   root's identity for its row count), so following the printed procedure failed
///   on a missing file the procedure itself was supposed to have produced.
/// * IT PRINTED A HARDCODED ROOT. Both commands were `&'static str`s naming
///   `/data/ws0-3096(-verify)` while the generation went to `CQLITE_WS0_VERIFY_ROOT`
///   — so the operator was handed commands pointing at a directory their corpus was
///   not in. Both commands are now built from the ACTUAL root.
#[test]
#[ignore = "writes ~2.8 GB and takes minutes; needs CQLITE_WS0_VERIFY_ROOT (see the doc comment)"]
fn the_full_size_verification_is_an_operator_procedure() {
    let root = std::env::var("CQLITE_WS0_VERIFY_ROOT").unwrap_or_else(|_| {
        panic!(
            "CQLITE_WS0_VERIFY_ROOT is unset. This test writes ~2.8 GB — point it at a scratch \
             volume OUTSIDE the repo. It fails rather than skipping: an --ignored test the \
             operator explicitly selected must never quietly measure nothing.\n\n\
             Corpus verification (substitute your scratch root for {example}):\n{corpus}\n\n\
             Digest verification:\n{digest}",
            example = mc::EXAMPLE_VERIFY_ROOT,
            corpus = mc::operator_verify_corpus(mc::EXAMPLE_VERIFY_ROOT),
            digest = mc::operator_verify_digest(mc::EXAMPLE_VERIFY_ROOT)
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

    // WRITE `corpus-identity.json` BESIDE THE CORPUS (#3272 review round 4). `generate()`
    // returns the identity in memory; only the BINARY writes the file, and this procedure
    // calls `generate()` directly — so the corpus it produced carried NO identity, while the
    // digest command it printed REQUIRES one (the Flight oracle reads the corpus root's
    // identity for its row count). The procedure could not verify its own output.
    let identity_path = spec.out.join("corpus-identity.json");
    identity
        .write_json(&identity_path)
        .unwrap_or_else(|e| panic!("could not write {}: {e}", identity_path.display()));
    // ...and it must be READABLE AND COMPLETE, asserted rather than assumed: a procedure whose
    // output is unusable is the same failure one step later.
    let written = std::fs::read_to_string(&identity_path)
        .unwrap_or_else(|e| panic!("could not read back {}: {e}", identity_path.display()));
    assert!(
        written.contains(&identity.data_db_sha256),
        "the written {} does not carry the digest it was generated with",
        identity_path.display()
    );

    eprintln!(
        "corpus verified: {} rows / {} partitions / Data.db {} B / sha256 {}\n\
         identity written: {}\n\
         now re-fold the Arrow-buffer digest (this command names the root you generated \
         into, not a hardcoded one):\n{}",
        identity.rows,
        identity.partitions,
        identity.data_db_bytes,
        identity.data_db_sha256,
        identity_path.display(),
        mc::operator_verify_digest(&root)
    );
}

/// THE SCHEMA DIGEST IS FULLY MACHINE-CHECKED, against the DDL that produces it (#3272 R2).
///
/// `ws0-events.cql` is a MEASUREMENT INPUT: both arms read it, asymmetrically — the bare scan
/// ingests it on EVERY invocation while the Flight ticket is generated from it ONCE — so a
/// modification between the two makes the two arms use DIFFERENT SCHEMAS. It was outside corpus
/// verification and outside the pre-measurement pin, so nothing could see that and the report
/// stayed valid by its own account.
///
/// # Why this is the ONE corpus digest with a complete gate oracle
///
/// Every other pinned digest describes 2.8 GB of generated data, so verifying it needs a
/// minutes-long write no gate component may perform (see `operator_verify_corpus`). The schema
/// does not: it is `schema::DDL`, a **source constant**, plus the trailing newline `generate`
/// writes. So the oracle is the INPUT itself rather than a record of it, which is strictly
/// stronger than comparing two recorded values — a co-edit of pin and artifact cannot satisfy
/// it, only an actual DDL change can move it.
///
/// This is deliberately asserted against `sha256(DDL + "\n")` and NOT against the committed
/// 2026-08-03 artifact, which predates the field. Regenerating that artifact to acquire the key
/// would be re-pinning a record to agree with changed output — the confirmation trap this issue
/// exists to refuse.
#[test]
fn the_pinned_schema_digest_is_the_digest_of_the_ddl_that_is_written() {
    use sha2::{Digest, Sha256};
    // Exactly what `generate` writes: `format!("{DDL}\n")`.
    let content = format!("{}\n", ws0_corpus_gen::schema::DDL);
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let derived = format!("{:x}", hasher.finalize());
    assert_eq!(
        derived,
        mc::SCHEMA_SHA256,
        "measurement_corpus::SCHEMA_SHA256 is not the digest of the DDL this generator writes. \
         Either the DDL changed (in which case the corpus must be REGENERATED — the schema is a \
         measurement input, and a corpus written from a different DDL is a different corpus, so \
         every figure measured against the old one is incomparable), or the constant was edited \
         alone. Do NOT re-pin the constant to match a changed DDL without regenerating: that \
         records agreement instead of verifying it (#3272 R2)."
    );
    // NON-VACUITY: a 64-hex string, and not the all-zero placeholder a truncated derivation
    // would produce. Without this the assertion above is satisfied by two equal empty strings.
    assert_eq!(
        mc::SCHEMA_SHA256.len(),
        64,
        "a schema digest that is not 64 hex characters cannot identify the schema"
    );
    assert!(
        mc::SCHEMA_SHA256
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "the pinned schema digest must be lowercase hex"
    );
    assert_ne!(
        mc::SCHEMA_SHA256,
        "0".repeat(64),
        "an all-zero digest is a placeholder, not an observation"
    );
    // ...and the DDL itself is non-empty, so the digest is not the digest of nothing.
    assert!(
        ws0_corpus_gen::schema::DDL.len() > 100,
        "the DDL must be the real schema (got {} bytes)",
        ws0_corpus_gen::schema::DDL.len()
    );
}

/// The comparison above must be able to FAIL — driven, per #3249.
///
/// Hashing a PERTURBED DDL must produce something other than the pinned digest. Without this,
/// `the_pinned_schema_digest_is_the_digest_of_the_ddl_that_is_written` would also pass against a
/// hasher that returned the pinned constant unconditionally.
#[test]
fn the_schema_digest_comparison_can_fail() {
    use sha2::{Digest, Sha256};
    // A ONE-CHARACTER change to the schema — a different clustering order, a renamed column, a
    // changed type would all be at least this large.
    let perturbed = ws0_corpus_gen::schema::DDL.replacen("metric_a int", "metric_a bigint", 1);
    assert_ne!(
        perturbed,
        ws0_corpus_gen::schema::DDL,
        "the perturbation must actually change the DDL, or this test proves nothing"
    );
    let mut hasher = Sha256::new();
    hasher.update(format!("{perturbed}\n").as_bytes());
    let derived = format!("{:x}", hasher.finalize());
    assert_ne!(
        derived,
        mc::SCHEMA_SHA256,
        "a MODIFIED schema must not hash to the pinned digest — otherwise the schema pin cannot \
         detect the change that makes the two measurement arms read different schemas"
    );
}

// ===========================================================================================
// #3272 review round 7, F1 — THE COMMITTED ARTIFACT MUST DESERIALIZE AS A `CorpusIdentity`
// ===========================================================================================

/// The committed artifact loads as a [`CorpusIdentity`] — THE test whose absence let F1 ship.
///
/// # The regression, and why nothing above could see it
///
/// R2 added `schema_sha256` to [`CorpusIdentity`] as a REQUIRED `String`. Every test in this
/// file reads the artifact as a `serde_json::Value` and compares fields individually, so all of
/// them stayed green — while `--verify-against`, which DESERIALIZES the artifact into the
/// struct, failed with `missing field schema_sha256` **before generation began**. The documented
/// determinism command (`tools/ws0-corpus-gen/README.md` §Quick start step 2,
/// `docs/reports/ws0-3096-artifacts/measurement-method.md` §1) was unrunnable against the only
/// artifact it has ever been pointed at, and the 2.8 GB corpus that artifact describes became
/// un-reportable (`ws0_schema_input.recorded_schema_digest` refuses an identity with no schema
/// digest).
///
/// A field-by-field `Value` comparison is not a substitute for loading the type: serde's
/// required/optional decision is invisible to it. So this test asserts the DESERIALIZATION
/// itself, which is what every consumer of the artifact actually performs.
#[test]
fn the_committed_artifact_deserializes_as_a_corpus_identity() {
    let path = repo_root().join(ARTIFACT);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read the committed pin artifact {path:?}: {e}"));
    let identity: ws0_corpus_gen::identity::CorpusIdentity = serde_json::from_str(&text)
        .unwrap_or_else(|e| {
            panic!(
                "the COMMITTED artifact {ARTIFACT} no longer deserializes as a CorpusIdentity: \
                 {e}.\n\nThis is F1 recurring. A field added to CorpusIdentity WITHOUT \
                 `#[serde(default)]` (or `Option`) makes every recorded identity that predates \
                 it unreadable, which breaks `--verify-against` — the determinism check every \
                 future comparison against this corpus rests on — BEFORE generation begins, and \
                 makes the corpus this artifact describes un-reportable. Identities recorded \
                 before a field existed genuinely exist and always will: make the new field \
                 optional AND give its absence a LOUD third state (see \
                 IdentityComparison::unverified), never a silent skip."
            )
        });
    // ...and the load is not vacuous: the values that came back are the recorded ones.
    assert_eq!(identity.rows, mc::ROWS, "deserialized row count");
    assert_eq!(
        identity.data_db_sha256,
        mc::DATA_DB_SHA256,
        "deserialized Data.db digest"
    );
    assert!(
        !identity.compression_info_present,
        "deserialized compression flag (#1406)"
    );
    assert!(
        !identity.components.is_empty(),
        "the deserialized identity carries no components — an empty load would satisfy the \
         assertions above only if they were also empty"
    );
}

/// The documented `--verify-against` COMPARISON against the committed artifact reaches a
/// verdict, and that verdict is the LOUD third state — never `Reproduced` (#3272 F1).
///
/// Two properties in one, because separating them would let either half look fine alone:
///
///  * the comparison RUNS (it did not, pre-fix: the deserialization failed first);
///  * a prior with no `schema_sha256` yields `PartialUnverified` with the schema NAMED in
///    `unverified` — the schema is UNVERIFIED against a pre-pin identity, and saying so is the
///    whole point. Folded into "matches", it would be an unobserved field read as agreement.
#[test]
fn comparing_against_the_committed_pre_pin_artifact_is_partial_never_reproduced() {
    use ws0_corpus_gen::identity::{CorpusIdentity, IdentityVerdict};
    let path = repo_root().join(ARTIFACT);
    let text = std::fs::read_to_string(&path).expect("read the committed artifact");
    let prior: CorpusIdentity = serde_json::from_str(&text).expect("deserialize the artifact");
    assert!(
        prior.schema_sha256.is_none(),
        "the committed 2026-08-03 artifact predates the R2 schema pin, so it must carry NO \
         schema digest. If it now does, this artifact was regenerated — in which case this test \
         should be comparing a genuine digest and `COMPARED_WHEN_PRESENT` should become a plain \
         `COMPARED_FIELDS` entry (#3272 F6)"
    );
    // Compare the artifact against ITSELF, so the ONLY thing that can be reported is the
    // unverified schema: every other field is trivially equal. That isolates the third state.
    let cmp = prior.compare(&prior);
    assert!(
        cmp.divergences.is_empty(),
        "an artifact compared against itself must show no divergence; got {:?}",
        cmp.divergences
    );
    assert_eq!(
        cmp.verdict(),
        IdentityVerdict::PartialUnverified,
        "a prior carrying NO schema digest must yield PartialUnverified — a comparison that \
         could not see a field must not read as reproduction (#3272 F1); got {cmp:?}"
    );
    assert!(
        cmp.unverified
            .iter()
            .any(|u| u.contains("ws0-events.cql") && u.contains("NO `schema_sha256`")),
        "the unverified entry must NAME the schema and say it was not compared; got {:?}",
        cmp.unverified
    );
}

/// F6's DECISION, asserted: the schema digest is either COMPARED against the pin, or the
/// artifact's lack of it is RECORDED as pre-pin. There is no third, silent branch.
///
/// This is what makes the documented regeneration remedy safe. Post-regeneration the artifact
/// carries the key, `pin_vs_artifact` compares it against `mc::SCHEMA_SHA256` (itself asserted
/// equal to `sha256(DDL + "\n")`), and this test takes its first branch. Pre-regeneration it
/// takes the second, which requires the `COMPARED_WHEN_PRESENT` entry to exist — so the absence
/// is an accounted-for state rather than a gap nobody named.
#[test]
fn the_schema_digest_is_compared_or_recorded_as_pre_pin() {
    let json = read_artifact();
    let obj = json.as_object().expect("the artifact is a JSON object");
    match obj.get("schema_sha256") {
        Some(v) => {
            let sha = v
                .as_str()
                .unwrap_or_else(|| panic!("{ARTIFACT}.schema_sha256 is not a string: {v}"));
            assert_eq!(
                sha,
                mc::SCHEMA_SHA256,
                "{ARTIFACT} carries a schema digest that disagrees with the in-source pin. The \
                 pin is asserted equal to sha256(DDL + \"\\n\"), so either the DDL changed (the \
                 corpus must be REGENERATED — a corpus written from a different schema is a \
                 different corpus) or one of the two was edited alone."
            );
        }
        None => {
            let listed = COMPARED_WHEN_PRESENT
                .iter()
                .any(|(k, _)| *k == "schema_sha256");
            assert!(
                listed,
                "{ARTIFACT} carries no `schema_sha256` and nothing records WHY. An absent field \
                 with no recorded reason is indistinguishable from an unnoticed gap — list it in \
                 COMPARED_WHEN_PRESENT with the reason, or regenerate the artifact (#3272 F6)."
            );
        }
    }
    // Every `COMPARED_WHEN_PRESENT` reason must pass the SAME content check the exemption
    // reasons do, so this list cannot become the softer place to park a field.
    for (key, reason) in COMPARED_WHEN_PRESENT {
        assert!(
            reason_is_acceptable(key, reason),
            "the COMPARED_WHEN_PRESENT reason for `{key}` must NAME the field and say WHY it is \
             not yet in the artifact, and must not be a placeholder; got {reason:?}"
        );
    }
    // ...and the two lists must be DISJOINT: a field in both would be compared under one rule
    // and excused under another, and which one applied would depend on read order.
    let exempt: Vec<&str> = DELIBERATELY_NOT_COMPARED.iter().map(|(k, _)| *k).collect();
    for (key, _) in COMPARED_WHEN_PRESENT {
        assert!(
            !exempt.contains(key) && !COMPARED_FIELDS.contains(key),
            "`{key}` appears in COMPARED_WHEN_PRESENT and ALSO in COMPARED_FIELDS or \
             DELIBERATELY_NOT_COMPARED — one field, one rule"
        );
    }
}
