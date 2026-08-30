//! Unit coverage for the CSV REFUSAL machinery of the AD2 comparator (issue
//! #1491): which positions are refused as CSV-unrepresentable, at what
//! GRANULARITY, and what stays compared around them.
//!
//! Split out of `golden_value_compare_tests.rs` under the campsite rule (CLAUDE.md,
//! epic #1135). A child of that module, so the shared `row`/`schema_of`/`set_schema`
//! helpers and its imports are reached through `use super::*` and are stated once.
//!
//! Every expectation is derived from the committed DDL and from the rendering
//! grammar `super::super::csv_container` documents — never from CQLite's output.
//!
//! # Why the granularity has its own cases
//!
//! The comparison walk is per MEMBER, per DEPTH, and this lane's review history is
//! three rounds of the refusal decision being made at a COARSER granularity than
//! that walk: per LANE (CSV skipped every container), then per CELL (an ambiguous
//! golden refused the whole cell, so `null` passed for it — finding N3), then per
//! OUTER CONTAINER (an ambiguous NESTED member suppressed its unambiguous
//! siblings and the outer structure, so a golden `[[]]` accepted a CLI `[]` —
//! finding P2). So each case here pins BOTH what the refusal suppresses AND what
//! it must still compare; a refusal that grew back one level up re-reds a case
//! rather than quietly switching coverage off.

use super::*;

/// The refusal PATH, not just the predicate: no corpus fixture carries a
/// `, `-bearing collection member, so without this the wiring from
/// `csv_container::node_refusal` to the census counters never executes and the
/// lane's `0 REFUSED` line would be unfalsifiable.
#[test]
fn a_csv_unrepresentable_container_is_refused_and_named() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a, b"]))])];
    // WHETHER the cell is refused is decided from the golden alone, so the defect
    // under test can never cause it. What survives the refusal is compared —
    // here the frame, which this rendering satisfies (see the case below).
    let cli = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);

    assert!(
        report.diffs.is_empty(),
        "unexpected diffs: {:?}",
        report.diffs
    );
    assert_eq!(report.ambiguous_container_cells, 1);
    assert_eq!(
        report.container_cells, 0,
        "a refused cell is not a compared one"
    );
    assert_eq!(report.compared_cells, 1, "`id` is still compared");
    assert_eq!(report.ambiguity_reasons.len(), 1);
    assert!(
        report.ambiguity_reasons[0].starts_with("s ("),
        "the refusal must name its column: {:?}",
        report.ambiguity_reasons
    );
}

/// Finding N3, through the comparator: a refused cell still reports the
/// divergences the ambiguity cannot reach, and is still counted as refused rather
/// than as compared coverage.
///
/// The subject is the one refusal the corpus reaches — an EMPTY `set<text>`,
/// indistinguishable from a set of one empty member; here it is the whole cell's
/// own node, so the refusal is named for the column. Refusing the position before
/// looking at the CLI value at all let `null` (and any other text) pass.
#[test]
fn a_refused_cell_still_reports_what_the_ambiguity_cannot_hide() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!([]))])];

    // The indistinguishable rendering: still no diff, still counted as refused.
    let ambiguous = vec![row(&[("id", json!("1")), ("s", json!("{}"))])];
    let report = compare_rows(&golden, &ambiguous, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(report.ambiguous_container_cells, 1);

    // An empty CSV field is not a container rendering at all, so it diverges.
    let absent = vec![row(&[("id", json!("1")), ("s", Value::Null)])];
    let report = compare_rows(&golden, &absent, &schema, &["id"], &[], &[], Egress::Csv);
    assert_eq!(
        report.diffs.len(),
        1,
        "an absent cell against a golden container must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains(".s:"), "{:?}", report.diffs);
    assert_eq!(
        report.ambiguous_container_cells, 1,
        "the cell is still refused: only part of it was decided"
    );
    assert_eq!(
        report.container_cells, 0,
        "a partly-decided cell is not counted as container coverage"
    );
    assert_eq!(report.compared_cells, 1, "`id` is still compared");
}

/// A representable container is compared, and a wrong member fails. Pins the
/// other side of the same branch so "refused" can never quietly become the
/// default.
#[test]
fn a_representable_container_is_compared_and_a_wrong_member_fails() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
    let good = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
    let report = compare_rows(&golden, &good, &schema, &["id"], &[], &[], Egress::Csv);
    assert!(
        report.diffs.is_empty(),
        "unexpected diffs: {:?}",
        report.diffs
    );
    assert_eq!(report.container_cells, 1);
    assert_eq!(report.ambiguous_container_cells, 0);

    let bad = vec![row(&[("id", json!("1")), ("s", json!("{a, c}"))])];
    let report = compare_rows(&golden, &bad, &schema, &["id"], &[], &[], Egress::Csv);
    assert_eq!(
        report.diffs.len(),
        1,
        "a wrong member must fail: {:?}",
        report.diffs
    );
    assert!(report.diffs[0].contains(".s:"), "{:?}", report.diffs);
}

/// A CLI cell that is not the grammar at all is a DIVERGENCE, not a refusal.
#[test]
fn an_unparseable_container_is_reported_not_refused() {
    let schema = set_schema();
    let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
    let cli = vec![row(&[("id", json!("1")), ("s", json!("a, b"))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);
    assert_eq!(report.ambiguous_container_cells, 0);
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("unparseable CSV container"),
        "{:?}",
        report.diffs
    );
}

// =======================================================================
// P2: the refusal is per MEMBER, per DEPTH — never one level up
// =======================================================================

/// A `list<frozen<list<text>>>`: the outer member is unambiguous, the inner empty
/// `list<text>` is not (EMPTY-CONTAINER — `[]` is also how one empty member
/// renders).
const NESTED_LIST_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, nl list<frozen<list<text>>>);";

/// A `map<text, frozen<set<text>>>`: the same shape one kind over, because an
/// object holding an ambiguous nested value had the identical hole.
const AMBIGUOUS_VALUE_MAP_DDL: &str =
    "CREATE TABLE t (id int PRIMARY KEY, m map<text, frozen<set<text>>>);";

fn csv_report(schema: &TableSchema, golden: &[Row], cell: &str) -> Report {
    let column = match schema.columns.iter().find(|c| c.name != "id") {
        Some(column) => column.name.clone(),
        None => panic!("the schema has no non-key column"),
    };
    let cli = vec![row(&[("id", json!("1")), (column.as_str(), json!(cell))])];
    compare_rows(golden, &cli, schema, &["id"], &[], &[], Egress::Csv)
}

/// Finding P2: an ambiguous NESTED member suppresses ITSELF and nothing else.
///
/// The golden `[[]]` holds ONE outer member, which the rendering states
/// unambiguously (`[[]]` vs `[]` are different member counts at depth 0, and the
/// inner ambiguity is at depth 1). Deciding the refusal from the OUTER
/// container's member count let the CLI drop that member entirely and pass.
#[test]
fn an_ambiguous_nested_member_does_not_suppress_its_container() {
    let schema = schema_of(NESTED_LIST_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("nl", json!([[]]))])];

    // The genuinely indistinguishable rendering: the inner body could be zero
    // members or one empty member, so THAT member is refused — and named at its
    // own path, which is how the census states the gap at member granularity.
    let report = csv_report(&schema, &golden, "[[]]");
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(report.ambiguous_container_cells, 1);
    assert_eq!(report.ambiguity_reasons.len(), 1);
    assert!(
        report.ambiguity_reasons[0].starts_with("nl[0] ("),
        "the refusal must name the MEMBER, not the column: {:?}",
        report.ambiguity_reasons
    );
    assert_eq!(
        report.container_cells, 0,
        "a partly-decided cell is not container coverage"
    );

    // THE REGRESSION P2 IS ABOUT: the unambiguous OUTER member is dropped. The
    // inner ambiguity says nothing about how many members the outer list has.
    let report = csv_report(&schema, &golden, "[]");
    assert_eq!(
        report.diffs.len(),
        1,
        "a dropped outer member must fail even though its content is refused: {:?}",
        report.diffs
    );
    assert!(
        report.diffs[0].contains("collection length"),
        "the diff must name the member count: {:?}",
        report.diffs
    );

    // The outer BRACKET is still required too (the declared kind is a list).
    let report = csv_report(&schema, &golden, "{[]}");
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);

    // …and so is the refused member's OWN frame, one level down.
    let report = csv_report(&schema, &golden, "[{}]");
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("unparseable CSV container"),
        "{:?}",
        report.diffs
    );
}

/// The same golden with an UNAMBIGUOUS sibling: the sibling keeps being compared,
/// which a refusal decided at the container (or the cell) could not do.
#[test]
fn an_ambiguous_member_leaves_its_siblings_compared() {
    let schema = schema_of(NESTED_LIST_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("nl", json!([[], ["x"]]))])];

    // Correct rendering: only the ambiguous member is refused.
    let report = csv_report(&schema, &golden, "[[], [x]]");
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(report.ambiguous_container_cells, 1);
    assert!(
        report.ambiguity_reasons[0].starts_with("nl[0] ("),
        "{:?}",
        report.ambiguity_reasons
    );

    // A wrong UNAMBIGUOUS sibling still fails, and the diff names its position.
    let report = csv_report(&schema, &golden, "[[], [y]]");
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("[1]") && report.diffs[0].contains('y'),
        "the diff must name the diverging sibling: {:?}",
        report.diffs
    );

    // And a dropped sibling is a member-count divergence, not a refusal.
    let report = csv_report(&schema, &golden, "[[]]");
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(
        report.diffs[0].contains("collection length"),
        "{:?}",
        report.diffs
    );
}

/// The object half: a map whose VALUE at one key is ambiguous. The map's SIZE, its
/// keys and its other values are all still compared.
#[test]
fn an_ambiguous_map_value_does_not_suppress_the_map() {
    let schema = schema_of(AMBIGUOUS_VALUE_MAP_DDL, "t");
    let golden = vec![row(&[
        ("id", json!(1)),
        ("m", json!({"a": [], "b": ["x"]})),
    ])];

    let report = csv_report(&schema, &golden, "{a: {}, b: {x}}");
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(report.ambiguous_container_cells, 1);
    assert!(
        // The comparator's own path spelling for a map entry: the canonicalized
        // key under its declared key type, which is what `SkipPaths` also matches.
        report.ambiguity_reasons[0].starts_with("m[text:a] ("),
        "the refusal must name the entry: {:?}",
        report.ambiguity_reasons
    );

    // The entry whose value is ambiguous cannot be dropped…
    let report = csv_report(&schema, &golden, "{b: {x}}");
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(report.diffs[0].contains("map size"), "{:?}", report.diffs);

    // …its KEY is still compared…
    let report = csv_report(&schema, &golden, "{z: {}, b: {x}}");
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(report.diffs[0].contains("map key"), "{:?}", report.diffs);

    // …and so is the unambiguous value beside it.
    let report = csv_report(&schema, &golden, "{a: {}, b: {y}}");
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    assert!(report.diffs[0].contains('y'), "{:?}", report.diffs);
}

/// A `, ` inside a SCALAR map VALUE, and inside a UDT FIELD: the entry split is
/// destroyed exactly as it is by a `, ` in a KEY, so the node must be REFUSED —
/// not reported as a divergence of the CLI.
///
/// This is the direction the object scan missed (review round 10, finding Q2): it
/// asked `, `/`: ` of the KEYS only, so CORRECT output for a golden
/// `{"k": "a, b"}` — the rendering `{k: a, b}`, which is what `ValueFormatter`
/// emits — was split into a second entry carrying no `: ` and reported
/// `unparseable CSV container`. A lane that reds on correct input is the lane
/// agents learn to waive (CLAUDE.md), so this is the worse half of the two
/// failure modes.
///
/// A `: ` inside a VALUE stays NOT refused and IS compared: entries split at their
/// FIRST top-level `: `, so a colon in the value is already read correctly.
#[test]
fn a_separator_inside_a_scalar_object_value_is_refused_not_called_unparseable() {
    // The refused NODE is the OBJECT itself, not the entry: what the `, `
    // destroys is THIS object's entry SPLIT, so the object is the narrowest node
    // whose decode it ruins (contrast the nested-container case above, where the
    // cause lives one level down and is named there).
    for (ddl, column, key, path) in [
        (
            "CREATE TABLE t (id int PRIMARY KEY, m map<text, text>);",
            "m",
            "k",
            "m (",
        ),
        (
            "CREATE TYPE holder (f text); \
             CREATE TABLE t (id int PRIMARY KEY, u frozen<holder>);",
            "u",
            "f",
            "u (",
        ),
    ] {
        let schema = schema_of(ddl, "t");
        let golden = vec![row(&[("id", json!(1)), (column, json!({key: "a, b"}))])];

        // Exactly the rendering the documented grammar produces for that golden.
        let report = csv_report(&schema, &golden, &format!("{{{key}: a, b}}"));
        assert!(
            report.diffs.is_empty(),
            "correct output must not be reported as a divergence: {:?}",
            report.diffs
        );
        assert_eq!(report.ambiguous_container_cells, 1);
        assert!(
            report.ambiguity_reasons[0].starts_with(path),
            "the refusal must name the refused node: {:?}",
            report.ambiguity_reasons
        );

        // A `: ` in the VALUE is a different matter: the split is unaffected, so
        // nothing is refused and the value is compared.
        let golden = vec![row(&[("id", json!(1)), (column, json!({key: "a: b"}))])];
        let report = csv_report(&schema, &golden, &format!("{{{key}: a: b}}"));
        assert!(report.diffs.is_empty(), "{:?}", report.diffs);
        assert_eq!(
            report.ambiguous_container_cells, 0,
            "a colon inside a value is decidable, so nothing is refused"
        );
        assert_eq!(report.container_cells, 1, "the cell is fully compared");
    }
}

/// The JSON lane carries its own structure, so nothing there is ever refused —
/// the same golden that refuses a member in CSV is fully compared in JSON.
#[test]
fn the_json_lane_refuses_nothing() {
    let schema = schema_of(NESTED_LIST_DDL, "t");
    let golden = vec![row(&[("id", json!(1)), ("nl", json!([[]]))])];
    let cli = vec![row(&[("id", json!(1)), ("nl", json!([[]]))])];
    let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
    assert!(report.diffs.is_empty(), "{:?}", report.diffs);
    assert_eq!(report.ambiguous_container_cells, 0);
    assert_eq!(report.container_cells, 1, "the cell is fully compared");

    // And the dropped member is an ordinary divergence there.
    let dropped = vec![row(&[("id", json!(1)), ("nl", json!([]))])];
    let report = compare_rows(&golden, &dropped, &schema, &["id"], &[], &[], Egress::Json);
    assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
}

/// The CLASS-level property, exercised for EVERY node-local refusal cause rather
/// than for the one shape that surfaced finding P2.
///
/// A refusal is right about `[[]]` and wrong about the next shape exactly when it
/// is decided per CAUSE-SITE instead of per NODE. So each cause is planted ONE
/// LEVEL DOWN, inside an outer container that is itself unambiguous, and both
/// halves are required: the refusal is named at the NESTED path, and the outer
/// container's own member count is still compared (a dropped outer member FAILS).
///
/// `list<frozen<…>>` is the vehicle because a nested collection must be frozen in
/// CQL; the outer list's members are separated at bracket depth 0, which is why
/// the inner ambiguity cannot reach it.
#[test]
fn every_refusal_cause_suppresses_only_its_own_node() {
    // (cause, declared column type, golden member, the CORRECT rendering of it)
    let causes = [
        (
            "EMPTY-CONTAINER",
            "list<frozen<list<text>>>",
            json!([]),
            "[[]]",
        ),
        (
            "EMPTY-MEMBER",
            "list<frozen<list<text>>>",
            json!([""]),
            "[[]]",
        ),
        (
            "SEPARATOR",
            "list<frozen<list<text>>>",
            json!(["a, b"]),
            "[[a, b]]",
        ),
        (
            "KEY-SEPARATOR",
            "list<frozen<map<text, text>>>",
            json!({"a: b": "v"}),
            "[{a: b: v}]",
        ),
    ];
    for (cause, decl, member, rendering) in causes {
        let schema = schema_of(
            &format!("CREATE TABLE t (id int PRIMARY KEY, nl {decl});"),
            "t",
        );
        let golden = vec![row(&[("id", json!(1)), ("nl", json!([member]))])];

        let report = csv_report(&schema, &golden, rendering);
        assert!(report.diffs.is_empty(), "{cause}: {:?}", report.diffs);
        assert_eq!(
            report.ambiguous_container_cells, 1,
            "{cause}: the nested position must be refused"
        );
        assert!(
            report.ambiguity_reasons[0].starts_with("nl[0] ("),
            "{cause}: the refusal must be named at the NESTED path: {:?}",
            report.ambiguity_reasons
        );

        // The outer container is unambiguous whatever the member turns out to
        // mean, so dropping that member is a divergence — the P2 property, per
        // cause.
        let report = csv_report(&schema, &golden, "[]");
        assert_eq!(
            report.diffs.len(),
            1,
            "{cause}: a dropped outer member must fail: {:?}",
            report.diffs
        );
        assert!(
            report.diffs[0].contains("collection length"),
            "{cause}: {:?}",
            report.diffs
        );
    }
}
