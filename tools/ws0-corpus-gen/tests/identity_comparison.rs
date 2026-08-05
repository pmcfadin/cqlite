//! `CorpusIdentity::compare` — every field reported, and the THIRD STATE for a field the
//! recorded identity does not carry (issue #3096 review finding 3, #3272 review round 7 F1/F5).
//!
//! # Why these are an INTEGRATION test rather than an inline `mod tests`
//!
//! Split out of `src/identity.rs` under the campsite rule: with round 7's F1/F5 cases that file
//! reached 925 lines against the ~800 source target, and the gate's `file-size` ratchet FAILED it
//! (correctly — the cost being controlled is tokens-to-load when an agent reads the file before
//! editing it). Source is now 445 lines and these tests are 500-odd, both comfortably inside
//! their thresholds.
//!
//! The split cost NOTHING in visibility, which is what made it the right seam rather than a
//! waiver: every item these tests touch (`Component`, `CorpusIdentity`, `IdentityComparison`,
//! `IdentityVerdict`, and the two caveat constants) was ALREADY `pub`, because the binary and the
//! measurement rig consume them. So no `pub(crate)` was widened to accommodate a test — the
//! anti-pattern that makes "move it to tests/" a bad trade — and these tests now exercise
//! exactly the surface an external caller has.
//!
//! # What is asserted here
//!
//! 1. Per-field divergence reporting, each case paired with a FROZEN REPLICA of the pre-review
//!    comparison observed NOT to have reported it (#3096 finding 3: `diff` compared 4 of 15
//!    fields and called the rest "reproduced exactly").
//! 2. COVERAGE OF THE STRUCT, derived from the type via serde rather than counted by hand — the
//!    F5 fix. `assert_eq!(mutations.len(), 15)` passed while R2's 16th field had no case at all,
//!    so the mechanism meant to force a new field to acquire one could not.
//! 3. The THIRD STATE (F1): a prior identity with no `schema_sha256` is `unverified`, never
//!    `divergences` and never `Reproduced`.

use std::collections::BTreeMap;

use ws0_corpus_gen::identity::{
    Component, CorpusIdentity, IdentityVerdict, DIFFERS_FROM_PRIOR_CORPUS, NOT_A_CORRECTNESS_ORACLE,
};

fn ident(sha: &str, rows: u64) -> CorpusIdentity {
    let mut components = BTreeMap::new();
    components.insert(
        "nb-1-big-Data.db".to_string(),
        Component {
            name: "nb-1-big-Data.db".to_string(),
            bytes: 100,
            sha256: sha.to_string(),
        },
    );
    CorpusIdentity {
        issue: "#3096".to_string(),
        seed: 1,
        table: "ws0.events".to_string(),
        rows,
        partitions: 1,
        rows_per_partition: rows,
        cells_per_row: 12,
        data_db_bytes: 100,
        data_db_sha256: sha.to_string(),
        bytes_per_row: 100.0 / rows as f64,
        total_component_bytes: 100,
        components,
        compression_info_present: false,
        not_a_correctness_oracle: NOT_A_CORRECTNESS_ORACLE.to_string(),
        differs_from_prior_corpus: DIFFERS_FROM_PRIOR_CORPUS.to_string(),
        schema_sha256: Some("cc".to_string()),
    }
}

/// Every field name a [`CorpusIdentity`] actually carries, **DERIVED FROM THE STRUCT**
/// through serde rather than counted by hand (#3272 review round 7, F5).
///
/// # Why the hand-written count had to go
///
/// The backstop below used to end in `assert_eq!(mutations.len(), 15)`, whose stated
/// purpose (in the doc comment two paragraphs down) was to FORCE a future field to get a
/// divergence case. R2 then added a 16th field, `schema_sha256`, and the count stayed at
/// `15` — so the assert PASSED while the new field had no case at all. The mechanism meant
/// to catch exactly that could not, because a hardcoded number is not a measurement of the
/// struct: it is a second copy of a fact, free to drift from the first. Bumping it to `16`
/// would reinstate the same shape for the 17th field.
///
/// serde's derived `Serialize` emits one key per field, so the serialized key set IS the
/// field set — read from the type at runtime, with no reflection and no macro. A 17th field
/// therefore appears here the moment it is added to the struct, and the coverage assertion
/// below FAILS naming it.
///
/// This is a genuine derivation and not a cleverer enumeration: nothing in this function
/// mentions any field name.
fn corpus_identity_field_names() -> Vec<String> {
    let value = serde_json::to_value(ident("aa", 10)).expect("an identity serializes");
    let obj = value
        .as_object()
        .expect("CorpusIdentity serializes to a JSON object");
    // A struct with no fields would make the coverage check below vacuous, so the subject
    // is asserted non-empty rather than trusted — the same rule the rest of this issue
    // applies to every other subject.
    assert!(
        !obj.is_empty(),
        "the serialized identity carries NO keys, so the coverage check below would have \
         an empty subject and pass having compared nothing"
    );
    obj.keys().cloned().collect()
}

#[test]
fn identical_identities_diff_empty() {
    let cmp = ident("aa", 10).compare(&ident("aa", 10));
    assert!(cmp.divergences.is_empty(), "got {cmp:?}");
    // ...and nothing UNVERIFIED either: two identities that both carry every field are
    // fully compared, so the verdict must be the strong one.
    assert_eq!(cmp.verdict(), IdentityVerdict::Reproduced, "got {cmp:?}");
}

/// A changed `Data.db` digest must be reported FIRST and by name — that is the
/// determinism assertion the committed corpus rests on.
#[test]
fn a_changed_data_db_digest_is_reported() {
    let d = ident("bb", 10).compare(&ident("aa", 10)).divergences;
    assert!(d[0].starts_with("Data.db sha256:"), "got {d:?}");
}

#[test]
fn a_changed_row_count_is_reported() {
    let d = ident("aa", 11).compare(&ident("aa", 10)).divergences;
    assert!(d.iter().any(|m| m.starts_with("rows:")), "got {d:?}");
}

/// A CHANGED schema digest is a DIVERGENCE (#3272 R2), and it is named.
#[test]
fn a_changed_schema_digest_is_reported() {
    let mut now = ident("aa", 10);
    now.schema_sha256 = Some("dd".to_string());
    let d = now.compare(&ident("aa", 10)).divergences;
    assert!(
        d.iter().any(|m| m.starts_with("ws0-events.cql sha256:")),
        "got {d:?}"
    );
}

/// THE THIRD STATE (#3272 review round 7, F1): a prior with NO schema digest is
/// `unverified`, never `divergences`, and NEVER `Reproduced`.
///
/// This is the assertion whose absence would let the fail-open read ship: an `Option` field
/// compared with `!=` would have made `None == None` read as agreement, and a `None` prior
/// against a `Some` regeneration read as a divergence about a schema that never changed.
#[test]
fn a_prior_without_a_schema_digest_is_unverified_not_reproduced() {
    let mut prior = ident("aa", 10);
    prior.schema_sha256 = None;
    let cmp = ident("aa", 10).compare(&prior);
    assert!(
        cmp.divergences.is_empty(),
        "an absent recorded digest is not a DIVERGENCE — nothing disagreed; got {cmp:?}"
    );
    assert_eq!(
        cmp.unverified.len(),
        1,
        "the absent schema digest must be reported as UNVERIFIED; got {cmp:?}"
    );
    assert!(
        cmp.unverified[0].contains("NO `schema_sha256`"),
        "the unverified entry must name what could not be compared; got {cmp:?}"
    );
    assert_eq!(
        cmp.verdict(),
        IdentityVerdict::PartialUnverified,
        "a comparison that could not see a field must NOT read as Reproduced; got {cmp:?}"
    );
    // NON-VACUITY, stated as the failing alternative: had the field been compared with a
    // plain `!=` on the `Option` (the smaller edit), `None` vs `None` would have compared
    // EQUAL and the verdict would have been `Reproduced` — a schema that was never checked
    // reported as reproduced. Driven here rather than argued.
    let mut both_absent_prior = ident("aa", 10);
    both_absent_prior.schema_sha256 = None;
    let mut both_absent_now = ident("aa", 10);
    both_absent_now.schema_sha256 = None;
    assert_eq!(
        both_absent_now.schema_sha256, both_absent_prior.schema_sha256,
        "the pre-fix `!=` comparison really did see these two as EQUAL — which is why \
         `None` had to become a third state rather than a compared value"
    );
    let cmp = both_absent_now.compare(&both_absent_prior);
    assert_eq!(
        cmp.verdict(),
        IdentityVerdict::PartialUnverified,
        "two identities that BOTH lack the digest are still UNVERIFIED, not reproduced"
    );
}

/// A regenerated identity missing the digest a prior HAS is a divergence, not an
/// unverified field: `generate()` always records it, so its absence means a hand-edited
/// identity — a different fault with a different remedy.
#[test]
fn a_regenerated_identity_missing_the_schema_digest_is_a_divergence() {
    let mut now = ident("aa", 10);
    now.schema_sha256 = None;
    let cmp = now.compare(&ident("aa", 10));
    assert!(
        cmp.divergences
            .iter()
            .any(|m| m.contains("hand-edited rather than generated")),
        "got {cmp:?}"
    );
    assert_eq!(cmp.verdict(), IdentityVerdict::Diverged, "got {cmp:?}");
}

/// The caveat travels IN the artifact — a reader of the JSON alone must see it.
#[test]
fn the_json_carries_the_performance_fixture_only_caveat() {
    let json = ident("aa", 10).to_json().expect("serialize");
    assert!(json.contains("PERFORMANCE FIXTURE ONLY"));
    assert!(json.contains("3042"));
    // And it must NOT assert the prior corpus's digest as this corpus's own.
    assert!(!json.contains("\"data_db_sha256\": \"0185909de6da"));
}

/// The comparison `diff` performed BEFORE the issue-#3096 review (finding 3),
/// kept as a **non-vacuity oracle** for the per-field tests below.
///
/// Each per-field test asserts BOTH halves: the current `diff` reports the
/// divergence, AND this pre-fix comparison did NOT — i.e. the same input really
/// did read as "reproduced exactly" before the fix. Without the second half a
/// passing test proves nothing about whether the guard was ever broken.
///
/// This is a frozen historical replica. It must never be called by production
/// code and must never be "kept in sync" with `diff`.
fn diff_pre_review(now: &CorpusIdentity, prior: &CorpusIdentity) -> Vec<String> {
    let mut out = Vec::new();
    if now.data_db_sha256 != prior.data_db_sha256 {
        out.push("Data.db sha256".to_string());
    }
    if now.rows != prior.rows {
        out.push("rows".to_string());
    }
    if now.partitions != prior.partitions {
        out.push("partitions".to_string());
    }
    if now.data_db_bytes != prior.data_db_bytes {
        out.push("Data.db bytes".to_string());
    }
    for (name, prior_c) in &prior.components {
        match now.components.get(name) {
            None => out.push(format!("component {name} MISSING")),
            Some(c) if c != prior_c => out.push(format!("component {name} changed")),
            Some(_) => {}
        }
    }
    for name in now.components.keys() {
        if !prior.components.contains_key(name) {
            out.push(format!("component {name} NEW"));
        }
    }
    out
}

/// Mutate one field of an otherwise byte-identical identity and return
/// `(current divergences, pre-review diff)` for it.
fn diverge(mutate: impl FnOnce(&mut CorpusIdentity)) -> (Vec<String>, Vec<String>) {
    let prior = ident("aa", 10);
    let mut now = ident("aa", 10);
    mutate(&mut now);
    (
        now.compare(&prior).divergences,
        diff_pre_review(&now, &prior),
    )
}

/// Every field the pre-review `diff` ignored is now reported — and each case
/// proves its own non-vacuity: the pre-review comparison returned EMPTY, i.e.
/// "reproduced exactly", for the very same divergence.
///
/// One test per field rather than a loop, so a failure names the field.
macro_rules! previously_ignored_field {
    ($test:ident, $prefix:literal, $mutate:expr) => {
        #[test]
        fn $test() {
            let (now, pre) = diverge($mutate);
            assert!(
                now.iter().any(|m| m.starts_with($prefix)),
                "diff must report a {} divergence; got {now:?}",
                $prefix
            );
            assert!(
                pre.is_empty(),
                "NON-VACUITY: the pre-review diff must have reported this as \
                 'reproduced exactly'; got {pre:?}"
            );
        }
    };
}

previously_ignored_field!(
    a_changed_seed_is_reported,
    "seed:",
    |i: &mut CorpusIdentity| i.seed = 2
);
previously_ignored_field!(
    a_changed_table_is_reported,
    "table:",
    |i: &mut CorpusIdentity| { i.table = "ws0.other".to_string() }
);
previously_ignored_field!(
    a_changed_issue_is_reported,
    "issue:",
    |i: &mut CorpusIdentity| { i.issue = "#0000".to_string() }
);
previously_ignored_field!(
    a_changed_rows_per_partition_is_reported,
    "rows_per_partition:",
    |i: &mut CorpusIdentity| i.rows_per_partition = 7
);
previously_ignored_field!(
    a_changed_cells_per_row_is_reported,
    "cells_per_row:",
    |i: &mut CorpusIdentity| i.cells_per_row = 11
);
previously_ignored_field!(
    a_changed_bytes_per_row_is_reported,
    "bytes_per_row:",
    |i: &mut CorpusIdentity| i.bytes_per_row = 99.0
);
previously_ignored_field!(
    a_changed_total_component_bytes_is_reported,
    "total_component_bytes:",
    |i: &mut CorpusIdentity| i.total_component_bytes = 101
);
// Issue #1406: the FLAG can disagree with the component set. That is an
// internally inconsistent identity, and it used to read as reproduced exactly.
previously_ignored_field!(
    a_flipped_compression_info_flag_is_reported,
    "compression_info_present:",
    |i: &mut CorpusIdentity| i.compression_info_present = true
);
previously_ignored_field!(
    an_edited_correctness_caveat_is_reported,
    "not_a_correctness_oracle:",
    |i: &mut CorpusIdentity| i.not_a_correctness_oracle = "it is fine actually".to_string()
);
previously_ignored_field!(
    an_edited_prior_corpus_caveat_is_reported,
    "differs_from_prior_corpus:",
    |i: &mut CorpusIdentity| i.differs_from_prior_corpus = String::new()
);

/// One perturbation of an identity, applied to a single field.
type FieldMutation = fn(&mut CorpusIdentity);

/// One perturbation per field of [`CorpusIdentity`], KEYED BY THE FIELD'S SERDE NAME.
///
/// Separate from the assertions so BOTH the per-field property and the
/// COVERAGE-OF-THE-STRUCT property can be driven from the same single source, rather than
/// one list plus a hand-written count of it (#3272 review round 7, F5).
fn single_field_mutations() -> Vec<(&'static str, FieldMutation)> {
    vec![
        ("issue", |i| i.issue = "#0000".to_string()),
        ("seed", |i| i.seed = 2),
        ("table", |i| i.table = "ws0.other".to_string()),
        ("rows", |i| i.rows = 11),
        ("partitions", |i| i.partitions = 2),
        ("rows_per_partition", |i| i.rows_per_partition = 7),
        ("cells_per_row", |i| i.cells_per_row = 11),
        ("data_db_bytes", |i| i.data_db_bytes = 101),
        ("data_db_sha256", |i| i.data_db_sha256 = "bb".to_string()),
        ("bytes_per_row", |i| i.bytes_per_row = 99.0),
        ("total_component_bytes", |i| i.total_component_bytes = 101),
        ("components", |i| {
            i.components.clear();
        }),
        ("compression_info_present", |i| {
            i.compression_info_present = true
        }),
        ("not_a_correctness_oracle", |i| {
            i.not_a_correctness_oracle = "nope".to_string()
        }),
        ("differs_from_prior_corpus", |i| {
            i.differs_from_prior_corpus = "nope".to_string()
        }),
        // The R2 field. Its case was MISSING for a whole review round while the
        // `mutations.len() == 15` assert passed (#3272 F5), which is why the coverage
        // check below is now derived from the struct instead of counted here.
        ("schema_sha256", |i| {
            i.schema_sha256 = Some("dd".to_string())
        }),
    ]
}

/// EVERY field of [`CorpusIdentity`] has a divergence case — DERIVED FROM THE STRUCT.
///
/// # The mechanism, and why the count it replaces was the defect
///
/// This used to be `assert_eq!(mutations.len(), 15)` inside the per-field test, whose
/// stated purpose was to force a NEW field to acquire a case. It could not: R2 added
/// `schema_sha256` and left the number at 15, so the assert passed with the new field
/// uncovered — the guard satisfied without covering its subject. Bumping the literal would
/// reinstate exactly that for the 17th field.
///
/// So the subject is READ FROM THE TYPE ([`corpus_identity_field_names`], via serde's
/// derived `Serialize`) and the mutation set is compared against it in BOTH directions:
///
///  * a field of the struct with NO mutation case FAILS, naming the field. This is the
///    property the count was meant to have.
///  * a mutation case naming a field the struct does not have FAILS too, so a rename
///    leaves a case pointing at nothing rather than silently testing a field that is gone.
///
/// Nothing here mentions a field name or a count, so a 17th field cannot compile-and-pass.
#[test]
fn every_identity_field_has_a_divergence_case() {
    let fields = corpus_identity_field_names();
    let cases: Vec<&str> = single_field_mutations().iter().map(|(k, _)| *k).collect();

    let uncovered: Vec<&String> = fields
        .iter()
        .filter(|f| !cases.contains(&f.as_str()))
        .collect();
    assert!(
        uncovered.is_empty(),
        "CorpusIdentity carries field(s) {uncovered:?} with NO divergence case in \
         `single_field_mutations`. This subject is DERIVED from the struct via serde, \
         precisely because the `mutations.len() == 15` assert it replaced passed while R2's \
         `schema_sha256` had no case at all (#3272 F5). Add the case; do not relax this."
    );

    let stale: Vec<&&str> = cases
        .iter()
        .filter(|c| !fields.contains(&c.to_string()))
        .collect();
    assert!(
        stale.is_empty(),
        "`single_field_mutations` names field(s) {stale:?} that CorpusIdentity does not \
         carry — a renamed or removed field leaves a case testing nothing"
    );
}

/// The backstop property, stated directly: for EVERY field, a divergence in it alone must
/// produce a non-empty divergence list. Coverage of the field set is asserted separately by
/// [`every_identity_field_has_a_divergence_case`], which derives it from the struct.
#[test]
fn no_single_field_divergence_reads_as_reproduced_exactly() {
    for (field, mutate) in single_field_mutations() {
        let (now, _) = diverge(mutate);
        assert!(
            !now.is_empty(),
            "a divergence in `{field}` alone read as 'reproduced exactly'"
        );
    }
}

/// NON-VACUITY for the coverage check above: it must FAIL on a field with no case.
///
/// Driven by SUBTRACTING a case from the real set and re-running the same comparison the
/// assertion performs — so "the coverage check has teeth" is observed rather than asserted
/// of the code. Without this, a `corpus_identity_field_names` that returned an empty vec
/// (or a `contains` that always answered true) would leave the check green forever, which
/// is the shape the count-assert died of.
#[test]
fn the_field_coverage_check_fires_on_an_uncovered_field() {
    let fields = corpus_identity_field_names();
    // Drop the R2 field's case — the exact state round 7 found in the tree.
    let cases: Vec<&str> = single_field_mutations()
        .iter()
        .map(|(k, _)| *k)
        .filter(|k| *k != "schema_sha256")
        .collect();
    let uncovered: Vec<&String> = fields
        .iter()
        .filter(|f| !cases.contains(&f.as_str()))
        .collect();
    assert_eq!(
        uncovered.len(),
        1,
        "removing ONE case must leave exactly one field uncovered; got {uncovered:?}"
    );
    assert_eq!(
        uncovered[0], "schema_sha256",
        "the uncovered field must be NAMED, so the failure is actionable"
    );
    // ...and the positive control: with the case present, nothing is uncovered. A check
    // that reported an uncovered field unconditionally would satisfy the half above.
    let all: Vec<&str> = single_field_mutations().iter().map(|(k, _)| *k).collect();
    assert!(
        fields.iter().all(|f| all.contains(&f.as_str())),
        "with every case present the check must find NOTHING uncovered"
    );
}

/// A missing or extra component must be visible, so a component-set change
/// (e.g. a stray `CompressionInfo.db`) cannot slip through as "Data.db matched".
#[test]
fn component_set_changes_are_reported() {
    let prior = ident("aa", 10);
    let mut now = ident("aa", 10);
    now.components.insert(
        "nb-1-big-CompressionInfo.db".to_string(),
        Component {
            name: "nb-1-big-CompressionInfo.db".to_string(),
            bytes: 8,
            sha256: "cc".to_string(),
        },
    );
    let d = now.compare(&prior).divergences;
    assert!(
        d.iter().any(|m| m.contains("CompressionInfo.db: NEW")),
        "got {d:?}"
    );

    let d = prior.compare(&now).divergences;
    assert!(
        d.iter()
            .any(|m| m.contains("CompressionInfo.db: recorded, now MISSING")),
        "got {d:?}"
    );
}
