//! Unit coverage for the comparator's DECLARED-GAP machinery and its fixture
//! pairing (issue #1491) — the `SkipPaths` staleness rules (finding L1) and the
//! golden↔SSTable pairing (finding L3).
//!
//! Split out of `golden_value_compare_tests.rs` under the campsite rule
//! (CLAUDE.md, epic #1135), which had reached the ~1500-line test target. A child
//! of that module, so the shared `row`/`schema_of` helpers and its imports are
//! reached through `use super::*` and are stated once.

use super::*;

// =======================================================================
// L1: a declared gap retires itself once the divergence is gone
// =======================================================================

/// The gap this lane scoped to a UDT FIELD until #3631, with its own divergence:
/// the golden decodes the nested `frozen<address>` and the egress renders the raw
/// bytes as a CQL blob literal. SYNTHETIC since #3631 — the egress decodes that
/// field now, no case declares this gap, and these cases exercise the staleness
/// machinery that retired it.
const HOME_GAP: [(&str, Divergence); 1] = [("e.home", Divergence::NestedFrozenUdtRendersAsBlobHex)];

/// The `udt_nested` golden's `e` value: `home` DECODED, as `sstabledump` writes it.
fn employee_golden() -> Vec<Row> {
    vec![row(&[
        ("id", json!(1)),
        (
            "e",
            json!({"name": "Grace",
                     "home": {"street": "1 Navy Way", "city": "Arlington", "zip": "22201"},
                     "level": 9}),
        ),
    ])]
}

/// The same value as the JSON egress renders it, with `home` as `home_rendering`.
fn employee_cli(id: Value, home: Value) -> Vec<Row> {
    vec![row(&[
        ("id", id),
        ("e", json!({"name": "Grace", "home": home, "level": 9})),
    ])]
}

/// The blob-hex spelling CQLite rendered `e.home` as BEFORE #3631 — the nested
/// UDT's serialized bytes, which is the SHAPE the gap declares (the exact bytes are
/// not what the gap is keyed on; see `Divergence::NestedFrozenUdtRendersAsBlobHex`).
/// A synthetic value from #3631 on: the egress decodes that field now.
const HOME_AS_BLOB_HEX: &str =
    "0x0000000a31204e617679205761790000000941726c696e67746f6e000000053232323031";

/// The property the whole `SkipPaths` mechanism exists for, in the direction
/// nothing used to test: once CQLite renders the excluded path CORRECTLY, the
/// exclusion is STALE and must FAIL, naming the path — otherwise the column stays
/// excluded forever and the recovered coverage never comes back.
///
/// A visit-keyed tally could never see this: the path is visited in both worlds,
/// so it registered a hit either way (issue #1491 review finding L1).
#[test]
fn a_skip_whose_divergence_is_gone_is_reported_as_stale() {
    let schema = schema_of(NESTED_UDT_DDL, "t");
    let golden = employee_golden();

    // STILL DIVERGING, exactly as declared: the exclusion suppressed a real
    // divergence, so it stands.
    let diverged = employee_cli(json!(1), json!(HOME_AS_BLOB_HEX));
    let report = compare_rows(
        &golden,
        &diverged,
        &schema,
        &["id"],
        &[],
        &HOME_GAP,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(
        report.stale_skips.is_empty(),
        "an exclusion suppressing a real divergence is not stale: {:?}",
        report.stale_skips
    );

    // FIXED: the same excluded path now agrees. The comparison must not fail (the
    // gap is declared, so the value is not compared), but the GAP must.
    let fixed = employee_cli(
        json!(1),
        json!({"street": "1 Navy Way", "city": "Arlington", "zip": "22201"}),
    );
    let report = compare_rows(
        &golden,
        &fixed,
        &schema,
        &["id"],
        &[],
        &HOME_GAP,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(
        report.stale_skips.len(),
        1,
        "a fixed divergence must retire its gap: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("e.home") && report.stale_skips[0].contains("AGREE"),
        "the failure must name the path and why it is stale: {:?}",
        report.stale_skips
    );
}

/// One divergent row keeps the gap alive even when another row agrees: a gap is a
/// property of the output, and suppressing anywhere is suppressing. The opposite
/// rule (last row wins) would make staleness depend on row order.
#[test]
fn one_diverging_row_keeps_a_skip_applied() {
    let schema = schema_of(NESTED_UDT_DDL, "t");
    let home = json!({"street": "1 Navy Way", "city": "Arlington", "zip": "22201"});
    let decoded = json!({"street": "1 Navy Way", "city": "Arlington", "zip": "22201"});
    let golden = vec![
        employee_golden()[0].clone(),
        row(&[
            ("id", json!(2)),
            ("e", json!({"name": "Grace", "home": home, "level": 9})),
        ]),
    ];
    // Row 1 agrees at the excluded path; row 2 diverges exactly as declared.
    let cli = vec![
        employee_cli(json!(1), decoded)[0].clone(),
        employee_cli(json!(2), json!(HOME_AS_BLOB_HEX))[0].clone(),
    ];
    let report = compare_rows(
        &golden,
        &cli,
        &schema,
        &["id"],
        &[],
        &HOME_GAP,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(
        report.stale_skips.is_empty(),
        "one diverging row is enough to keep the gap: {:?}",
        report.stale_skips
    );
}

/// The CSV half of the same property, on a CONTAINER member — the shape where
/// the decode actually matters (a scalar member is raw text either way). An
/// excluded member's text is decoded when it CAN be: an un-invertible rendering
/// falls back to raw text, so one member cannot fail a whole cell nobody
/// compares, and still counts as suppressed; a member that now decodes and agrees
/// retires the gap. Returning raw text unconditionally made the excluded position
/// diverge forever — the finding's own shape, one level down.
///
/// The DDL is the real `udt_nested` shape (`test-data/schemas/*.cql`), whose
/// `e.home` gap was one of this lane's declared CSV exclusions until #3631 retired
/// it — by this very rule, on the real corpus.
#[test]
fn a_csv_skip_on_a_nested_container_retires_when_it_decodes_and_agrees() {
    let schema = schema_of(NESTED_UDT_DDL, "t");
    let golden = employee_golden();

    // Diverging exactly as the gap declares: the inner frozen UDT arrives as blob
    // hex, which the `{…}` grammar cannot invert.
    let diverged = vec![row(&[
        ("id", json!("1")),
        (
            "e",
            json!(format!(
                "{{name: Grace, home: {HOME_AS_BLOB_HEX}, level: 9}}"
            )),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &diverged,
        &schema,
        &["id"],
        &[],
        &HOME_GAP,
        Egress::Csv,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(
        report.stale_skips.is_empty(),
        "a member whose rendering does not invert keeps its gap: {:?}",
        report.stale_skips
    );

    let fixed = vec![row(&[
        ("id", json!("1")),
        (
            "e",
            json!(
                "{name: Grace, home: {street: 1 Navy Way, city: Arlington, zip: 22201}, \
                   level: 9}"
            ),
        ),
    ])];
    let report = compare_rows(
        &golden,
        &fixed,
        &schema,
        &["id"],
        &[],
        &HOME_GAP,
        Egress::Csv,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(
        report.stale_skips.len(),
        1,
        "a nested member that now decodes and agrees must retire its gap: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("e.home") && report.stale_skips[0].contains("AGREE"),
        "{:?}",
        report.stale_skips
    );
}

/// The third cause, kept distinct from the other two: when the cell the exclusion
/// names was REFUSED as CSV-unrepresentable there is no comparison to read an
/// answer from. "I could not tell" is not "the gap is still real", so it is
/// reported — with its own cause — rather than counted as applied.
#[test]
fn a_skip_whose_cell_was_refused_is_reported_as_unevaluable() {
    let schema = set_schema();
    // `, ` inside a member: `csv_container::node_refusal` refuses that container
    // from the GOLDEN alone, so the refusal is independent of what the CLI
    // rendered. The refused node here IS the excluded whole column; a refusal at
    // any DEPTH inside an excluded subtree is reported the same way.
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a, b"]))])];
    let cli = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
    // WHICH divergence the gap declares does not decide this case: the refusal is
    // taken before any divergence match, precisely so a partially-decided node
    // cannot mint a suppression (see `compare_value_at`).
    let report = compare_rows(
        &golden,
        &cli,
        &schema,
        &["id"],
        &[],
        &[("s", Divergence::AbsentMulticellRendersEmpty)],
        Egress::Csv,
    );
    assert_eq!(report.ambiguous_container_cells, 1);
    assert_eq!(
        report.stale_skips.len(),
        1,
        "an unevaluable exclusion must be reported: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("could not be evaluated"),
        "the cause must be the unevaluable one, not `AGREE` or `matched no value`: {:?}",
        report.stale_skips
    );
}

/// P1: a declared exclusion suppresses a VALUE divergence, never the COLUMN's
/// PRESENCE. A skip used to record an omitted column as `Suppressed`, so each of
/// the five declared skips could hide a regression that dropped its column from
/// the egress row altogether — the one shape the comparator's own contract (every
/// DDL column is rendered) puts outside any gap's reach.
///
/// Asserted in BOTH lanes, because the omission is a property of the egress row
/// and not of one format's spelling: neither the presence of a skip nor the format
/// may excuse it.
#[test]
fn a_skip_cannot_hide_a_column_the_egress_omits() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
    for (egress, id) in [(Egress::Json, json!(1)), (Egress::Csv, json!("1"))] {
        // The egress row renders `id` and DROPS the declared `s` entirely.
        let cli = vec![row(&[("id", id)])];
        let report = compare_rows(
            &golden,
            &cli,
            &schema,
            &["id"],
            &[],
            &[("s", Divergence::AbsentMulticellRendersEmpty)],
            egress,
        );
        assert_eq!(
            report.diffs.len(),
            1,
            "{egress:?}: an omitted declared column must fail even under a whole-column \
             skip: {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains(".s:") && report.diffs[0].contains("absent from the"),
            "{egress:?}: the diff must name the omitted column: {:?}",
            report.diffs
        );
        // …and the skip itself is UNRESOLVED, not applied: with no value at that
        // path there is nothing to read an answer from. The two failures agree —
        // neither says the gap was measured.
        assert_eq!(
            report.stale_skips.len(),
            1,
            "{egress:?}: {:?}",
            report.stale_skips
        );
        assert!(
            report.stale_skips[0].contains("could not be evaluated")
                && report.stale_skips[0].contains("no `s` column"),
            "{egress:?}: the cause must be the unevaluable one: {:?}",
            report.stale_skips
        );
    }
}

// =======================================================================
// Review round 17: a gap suppresses the divergence it NAMES, and no other
// =======================================================================
//
// Each case below is a divergence at a path a declared gap covers, which the gap
// does NOT declare. Every one of them passed silently before this rule — the gap
// swallowed any error at its path and `Suppressed` then dominated table-wide — so
// each declared gap was a permanent blind spot for its whole column. Both
// directions are pinned per gap: the declared shape must still be suppressed, and
// the undeclared one must be reported.

/// The empty-collection gaps (`ml`/`ms`/`mm`) declare ONE thing: the golden has no
/// value and the egress renders a present EMPTY container. A NON-EMPTY row of the
/// same column is compared member by member, so wrong members are a real diff.
///
/// This is the finding's own first example. `nb_empty_collections` has exactly this
/// shape — `ck=1` written empty, `ck=2` written with members — so the gap covered
/// the non-empty row of every one of the three columns.
#[test]
fn an_empty_collection_gap_does_not_cover_a_non_empty_rows_members() {
    let schema = schema_of(
        "CREATE TABLE t (pk int, ck int, ml list<int>, PRIMARY KEY (pk, ck));",
        "t",
    );
    let gap = [("ml", Divergence::AbsentMulticellRendersEmpty)];
    // Row ck=1: no `ml` cell at all. Row ck=2: two members.
    let golden = vec![
        row(&[("pk", json!(1)), ("ck", json!(1))]),
        row(&[("pk", json!(1)), ("ck", json!(2)), ("ml", json!([1, 2]))]),
    ];

    // The DECLARED divergence on the empty row, and the members right on the other:
    // suppressed, applied, no diff.
    let declared = vec![
        row(&[("pk", json!(1)), ("ck", json!(1)), ("ml", json!([]))]),
        row(&[("pk", json!(1)), ("ck", json!(2)), ("ml", json!([1, 2]))]),
    ];
    let report = compare_rows(
        &golden,
        &declared,
        &schema,
        &["pk"],
        &["ck"],
        &gap,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(report.stale_skips.is_empty(), "{:?}", report.stale_skips);

    // The same gap, and the NON-EMPTY row's members are wrong. The gap declares
    // nothing about them, so this must be reported.
    for wrong in [json!([1, 3]), json!([1]), json!([2, 1]), json!([])] {
        let cli = vec![
            row(&[("pk", json!(1)), ("ck", json!(1)), ("ml", json!([]))]),
            row(&[("pk", json!(1)), ("ck", json!(2)), ("ml", wrong.clone())]),
        ];
        let report = compare_rows(&golden, &cli, &schema, &["pk"], &["ck"], &gap, Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a wrong non-empty rendering ({wrong}) must be reported under the \
             empty-collection gap: {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains("ml") && report.diffs[0].contains("NOT the divergence"),
            "the diff must name the column and the declared gap: {:?}",
            report.diffs
        );
    }
}

/// The `e.home` gap declares blob hex. Arbitrary text there is a DIFFERENT
/// divergence — the finding's own second example — and so is a decoded object
/// whose content is wrong.
#[test]
fn the_nested_udt_gap_does_not_cover_arbitrary_text_or_wrong_content() {
    let schema = schema_of(NESTED_UDT_DDL, "t");
    let golden = employee_golden();
    for undeclared in [
        // Not a blob literal at all.
        json!("1 Navy Way, Arlington"),
        // `0x` with a non-hex body, and with an odd digit count: neither is CQL's
        // spelling of a byte string.
        json!("0xnothex"),
        json!("0xabc"),
        // Decoded — which is what closing the gap looks like — but WRONG.
        json!({"street": "9 Apollo", "city": "Arlington", "zip": "22201"}),
        json!(null),
        json!(9),
    ] {
        let cli = employee_cli(json!(1), undeclared.clone());
        let report = compare_rows(
            &golden,
            &cli,
            &schema,
            &["id"],
            &[],
            &HOME_GAP,
            Egress::Json,
        );
        assert_eq!(
            report.diffs.len(),
            1,
            "`{undeclared}` at `e.home` is not the declared blob-hex divergence: {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains("e.home") || report.diffs[0].contains(".home"),
            "the diff must name the field: {:?}",
            report.diffs
        );
        assert_eq!(
            report.stale_skips.len(),
            1,
            "a gap that suppressed nothing must be reported: {:?}",
            report.stale_skips
        );
        assert!(
            report.stale_skips[0].contains("NOT the one this gap declares"),
            "the cause must be the undeclared-divergence one: {:?}",
            report.stale_skips
        );
    }
}

/// The `sf` gap declares that a NON-FINITE float renders as JSON null, because
/// JSON has no literal for it. A FINITE member rendering as null is data loss with
/// no format excuse, and the four finite members of that set are compared.
///
/// The golden spelling of the three tokens is `sstabledump`'s own — the committed
/// `signed_special_collections` golden carries `"-Infinity"`, `"Infinity"`, `"NaN"`
/// as `sf`'s cell paths.
#[test]
fn the_non_finite_float_gap_does_not_cover_a_finite_member() {
    let schema = schema_of("CREATE TABLE t (id int PRIMARY KEY, sf set<double>);", "t");
    let gap = [("sf", Divergence::NonFiniteFloatRendersAsJsonNull)];
    // A multicell set: `sstabledump` writes each element as its stringified cell
    // path, so the golden's members are strings.
    let golden = vec![row(&[
        ("id", json!(1)),
        ("sf", json!(["-Infinity", "-1.5", "2.5", "Infinity", "NaN"])),
    ])];

    // DECLARED: the three non-finite members are null, the finite ones are numbers.
    let declared = vec![row(&[
        ("id", json!(1)),
        ("sf", json!([null, -1.5, 2.5, null, null])),
    ])];
    let report = compare_rows(
        &golden,
        &declared,
        &schema,
        &["id"],
        &[],
        &gap,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(report.stale_skips.is_empty(), "{:?}", report.stale_skips);

    // UNDECLARED: a FINITE member lost, and a finite member with the wrong value.
    for wrong in [
        json!([null, null, 2.5, null, null]),
        json!([null, -1.5, 9.5, null, null]),
    ] {
        let cli = vec![row(&[("id", json!(1)), ("sf", wrong.clone())])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &gap, Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a finite member is compared under the non-finite gap ({wrong}): {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains("NOT the divergence"),
            "{:?}",
            report.diffs
        );
    }
}

/// The `sd` gap declares that a `decimal` is QUOTED where
/// `cassandra-5.0.8 DecimalType.toJSONString` emits an unquoted number — the JSON
/// KIND and nothing else. The quoted NUMBER must still be the golden's, which is
/// the 30-digit exactness this lane exists to check.
#[test]
fn the_decimal_quoting_gap_does_not_cover_a_different_number() {
    let schema = schema_of("CREATE TABLE t (id int PRIMARY KEY, sd set<decimal>);", "t");
    let gap = [("sd", Divergence::DecimalRendersAsJsonString)];
    let golden = vec![row(&[
        ("id", json!(1)),
        ("sd", json!(["-999999999999999999999999999999.999", "0"])),
    ])];

    // DECLARED: the same numbers, quoted.
    let declared = vec![row(&[
        ("id", json!(1)),
        ("sd", json!(["-999999999999999999999999999999.999", "0"])),
    ])];
    let report = compare_rows(
        &golden,
        &declared,
        &schema,
        &["id"],
        &[],
        &gap,
        Egress::Json,
    );
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert!(report.stale_skips.is_empty(), "{:?}", report.stale_skips);

    // UNDECLARED: one digit of the 30 lost, and a rounded rendering. Both are
    // quoted, so the old whole-path suppression absorbed them.
    for wrong in [
        json!(["-999999999999999999999999999999.998", "0"]),
        json!(["-1.0E+30", "0"]),
        json!(["not-a-number", "0"]),
    ] {
        let cli = vec![row(&[("id", json!(1)), ("sd", wrong.clone())])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &gap, Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a quoted decimal must still hold the golden's number ({wrong}): {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains("NOT the divergence"),
            "{:?}",
            report.diffs
        );
    }
}

/// The gap is FORMAT-SCOPED in the divergence too: `NonFiniteFloatRendersAsJsonNull`
/// and `DecimalRendersAsJsonString` are statements about JSON's own vocabulary, so
/// neither may fire in the CSV lane — where every cell is text and all three tokens
/// and all 30 digits survive verbatim. A gap mis-declared for CSV suppresses
/// nothing and is reported, rather than quietly widening.
#[test]
fn a_json_vocabulary_gap_never_fires_in_the_csv_lane() {
    let schema = schema_of("CREATE TABLE t (id int PRIMARY KEY, sf set<double>);", "t");
    let gap = [("sf", Divergence::NonFiniteFloatRendersAsJsonNull)];
    let golden = vec![row(&[
        ("id", json!(1)),
        ("sf", json!(["-Infinity", "-1.5", "NaN"])),
    ])];
    // The CSV egress carries the tokens verbatim, so the two sides AGREE and the
    // gap retires itself.
    let cli = vec![row(&[
        ("id", json!("1")),
        ("sf", json!("{-Infinity, -1.5, NaN}")),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &gap, Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(
        report.stale_skips.len(),
        1,
        "a JSON-vocabulary gap declared for CSV must be reported: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("AGREE"),
        "{:?}",
        report.stale_skips
    );
}

/// A CSV REFUSAL at a gap's path wins over a divergence match, so a
/// partially-decided node can never mint a suppression. At a refused node only the
/// bracket frame and the body's EMPTINESS are decided (`csv_container`), so no
/// verdict there is a measurement — including "this is the declared divergence".
///
/// The discriminating shape: the pair WOULD match the declared divergence (the
/// golden decoded an object, the egress rendered blob hex) and the golden's own
/// rendering is un-splittable, because a field value contains the `, ` the grammar
/// separates members with. Without the precedence this recorded `Suppressed` and
/// the refusal became invisible to the staleness check.
#[test]
fn a_refusal_at_a_gaps_path_wins_over_a_divergence_match() {
    let schema = schema_of(NESTED_UDT_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        (
            "e",
            json!({"name": "Grace",
                     "home": {"street": "1 Navy Way, Suite 2", "city": "Arlington",
                                "zip": "22201"},
                     "level": 9}),
        ),
    ])];
    let cli = vec![row(&[
        ("id", json!("1")),
        (
            "e",
            json!(format!(
                "{{name: Grace, home: {HOME_AS_BLOB_HEX}, level: 9}}"
            )),
        ),
    ])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &HOME_GAP, Egress::Csv);
    assert_eq!(
        report.ambiguous_container_cells, 1,
        "the refused position must still be counted and named: {:?}",
        report.ambiguity_reasons
    );
    assert_eq!(
        report.stale_skips.len(),
        1,
        "a gap over a refused node is unevaluable, not applied: {:?}",
        report.stale_skips
    );
    assert!(
        report.stale_skips[0].contains("could not be evaluated"),
        "the cause must be the unevaluable one, not a suppression: {:?}",
        report.stale_skips
    );
}

/// `NestedFrozenValueLeftUndecodedByGolden` must require the CLI's ARRAY spelling.
///
/// An earlier version also accepted `Value::Object`, reasoning that the CLI spells a
/// UDT as an object. The arm was unreachable — the type guard admits only
/// list/set/map/tuple — but still permissive, so it would have excused an object
/// rendered where only an array is legal (roborev job 305). An
/// unreachable-but-permissive arm is worse than no arm.
#[test]
fn the_undecoded_golden_gap_requires_the_cli_array_spelling() {
    let gap = Divergence::NestedFrozenValueLeftUndecodedByGolden;
    let inner_set = CqlType::Set(Box::new(CqlType::Text("text".into())));
    let golden_hex = json!("000000020000001100000005616c706861");
    let ask = |cli: &Value, ty: &CqlType| {
        gap.matched(
            &golden_hex,
            cli,
            ty,
            Egress::Json,
            Depth::TopLevel,
            Kinding::Natural,
        )
    };

    assert!(
        ask(&json!([{"label": "alpha"}]), &inner_set),
        "an undecoded golden scalar against a DECODED CLI array is the declared gap"
    );
    for not_an_array in [
        json!({"label": "alpha"}),
        json!(null),
        json!(0),
        json!("000000020000001100000005616c706861"),
    ] {
        assert!(
            !ask(&not_an_array, &inner_set),
            "only the CLI's ARRAY spelling is this gap: {not_an_array:?}"
        );
    }
}
