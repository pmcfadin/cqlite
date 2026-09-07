//! The KEY-SCOPED refusal cases: what a map's ambiguous KEYS cost, and what they must
//! NOT cost (issue #3815, split out of `golden_csv_container_tests.rs` under the
//! campsite rule — CLAUDE.md epic #1135 — which this issue's three review rounds
//! pushed to the ~1500-line test-file target).
//!
//! ONE responsibility, and it is the [`super::super::Reach`] decision: which cause
//! reaches a map node's whole BODY and which reaches its KEYS alone. Every case here
//! pins BOTH halves of a blast radius — what IS suppressed and what is NOT — because
//! the defects this file exists for were all radius errors: a whole-node refusal that
//! cost the entry values (finding 1), a key cause recorded nowhere at all (finding 2),
//! a whole-MAP abstention triggered by ONE unrenderable key (round 2), and a `MapKeys`
//! promise made where the body checks could not run (round 3).
//!
//! The end-to-end verdicts for the same shapes — that a corrupted value IS reported and
//! correct egress is NOT — live in `compare::map::tests`, which drives `compare_rows`.
//! These are the unit half: they ask `node_refusal_reach` and `map_key_refusals`
//! directly and NAME the reach, which no public-surface test can do.
//!
//! A child of the container test module, so `ty_of`, `node_refusal`, `decode` and the
//! `CqlType`/`Kinding` names are reached through `use super::*` and are stated once —
//! the same arrangement `golden_csv_container_spelling_tests.rs` uses.

use super::*;

/// KEY-SCOPED REFUSAL: two keys that render ALIKE cost the KEYS, and NOT the entry
/// values, the entry count or the pair shape (issue #3815).
///
/// This replaces #3726's `a_duplicate_rendering_refusal_also_costs_the_entry_values`,
/// which asserted the opposite ON PURPOSE as an executable residual: the node was
/// refused whole, `decode_shape` returned the un-split body and a value corrupted
/// 20 -> 999 inside such a cell was invisible. That test's own instruction was to
/// delete it when key-scoped refusal landed, which is here.
///
/// The KEY is still suppressed and never resolved — see
/// `super::two_container_keys_that_render_alike_are_refused`, which stayed in the
/// PARENT test module (it predates this file and is held byte-identical to
/// `origin/main`) and is the reason the refusal exists at all — so what the decoder
/// leaves at a key node is its
/// stripped BODY, the same thing it leaves at every other refused node. The
/// end-to-end verdicts (a corrupted value IS reported, a correct rendering is NOT)
/// are in `compare::map::tests`, which drives `compare_rows`.
#[test]
fn a_duplicate_rendering_refusal_is_scoped_to_the_keys() {
    let ty = ty_of("frozen<map<frozen<key_part>, int>>");
    let golden = json!({
        "{\"label\": null, \"rank\": 1}": 10,
        "{\"label\": \"null\", \"rank\": 1}": 20
    });
    let csv = "{{label: null, rank: 1}: 10, {label: null, rank: 1}: 20}";
    let (reach, why) = match super::node_refusal_reach(&golden, Some(&ty), Kinding::Natural) {
        Some(found) => found,
        None => panic!("premise: colliding renderings are refused"),
    };
    assert_eq!(
        reach,
        Reach::MapKeys,
        "the cause reaches the KEYS and not the body: {why}"
    );
    // BOTH colliding keys are refused, and the vector says which by INDEX.
    let refusals = match &ty {
        CqlType::Map(key_ty, _) => match golden.as_object() {
            Some(fields) => map_key_refusals(fields, key_ty),
            None => panic!("the golden is an object"),
        },
        _ => panic!("the declared type is a map"),
    };
    assert_eq!(refusals.len(), 2, "{refusals:?}");
    assert!(
        refusals
            .iter()
            .all(|r| r.as_ref().is_some_and(|why| why.contains("SAME key text"))),
        "both keys of a colliding PAIR are ambiguous: {refusals:?}"
    );
    // The node is SPLIT, so the entries and their values come back.
    let decoded = match decode(&golden, csv, &ty) {
        Ok(decoded) => decoded,
        Err(why) => panic!("a key-scoped refusal must not fail the cell: {why}"),
    };
    assert_eq!(
        decoded,
        json!([
            {"key": "label: null, rank: 1", "value": "10"},
            {"key": "label: null, rank: 1", "value": "20"}
        ]),
        "each entry's VALUE is recovered; each KEY is left as the stripped body a \
         refused node leaves: {decoded}"
    );
}

/// A key one of whose SCALAR MEMBERS carries the `, ` separator is refused at the
/// KEY, while the map node itself recovers (issue #3815, finding 2).
///
/// The separator sits at bracket depth 1, so the entry split and [`entry_cut`] both
/// survive — which is exactly why this could not be seen at the map node, and why
/// the key used to be left as raw text for `compare::compare_map` to fail to
/// canonicalize, reporting CORRECT egress as a divergence.
#[test]
fn a_separator_inside_a_scalar_member_of_a_key_is_refused_at_the_key() {
    let ty = ty_of("frozen<map<frozen<list<text>>, int>>");
    let golden = json!({"[\"a, b\"]": 5});
    let (reach, why) = match super::node_refusal_reach(&golden, Some(&ty), Kinding::Natural) {
        Some(found) => found,
        None => panic!("the key of this map cannot be read back"),
    };
    assert_eq!(reach, Reach::MapKeys, "{why}");
    // The map node's OWN split is intact — the control that makes this key-scoped
    // rather than a body cause.
    assert_eq!(
        members("{[a, b]: 5}", &ty),
        Ok(vec!["[a, b]: 5"]),
        "the `, ` sits at depth 1, so the entry split survives"
    );
    assert_eq!(entry_cut("[a, b]: 5"), Ok(("[a, b]", "5")));
    // …and a key whose member carries NO separator is not refused, so this narrows.
    assert_eq!(
        super::node_refusal_reach(&json!({"[\"a\", \"b\"]": 5}), Some(&ty), Kinding::Natural),
        None
    );
}

/// The cause INSIDE a key is asked of the key's WHOLE value tree, not just its own
/// node — because `compare::compare_map` canonicalizes a key as ONE value (#3815).
///
/// A `frozen<list<frozen<list<text>>>>` key holding `[["a, b"]]` recovers at its
/// OUTER node: that `, ` sits at depth 2, so `[[a, b]]` splits back into the one
/// member `[a, b]`. Only the INNER node loses it. Asking the outer node alone would
/// have missed this and the false divergence would have stood.
#[test]
fn a_key_refusal_is_asked_of_the_keys_whole_value_tree() {
    let ty = ty_of("frozen<map<frozen<list<frozen<list<text>>>>, int>>");
    let golden = json!({"[[\"a, b\"]]": 5});
    let (reach, why) = match super::node_refusal_reach(&golden, Some(&ty), Kinding::Natural) {
        Some(found) => found,
        None => panic!("the key's INNER node cannot be read back"),
    };
    assert_eq!(reach, Reach::MapKeys, "{why}");
    // The key's own OUTER node recovers, which is the whole point of recursing.
    let outer = ty_of("frozen<list<frozen<list<text>>>>");
    assert_eq!(members("[[a, b]]", &outer), Ok(vec!["[a, b]"]));
    assert_eq!(node_refusal(&json!([["a, b"]]), Some(&outer)), None);
}

/// ONE UNRENDERABLE KEY MUST NOT COST ITS SIBLINGS THEIR REFUSAL (roborev job 444,
/// issue #3815 round 2).
///
/// A MIXED node: entry 0's key is not a `toJSONString` document at all (the
/// `getString` cell-path shape), entries 1 and 2 render ALIKE. Abstaining at entry 0
/// is correct and intended — a golden key contradicting the DDL is a divergence for
/// the comparison to report, never a format limit — and the defect was applying that
/// abstention to the whole MAP: the colliding pair then got no refusal at any reach,
/// was canonicalized and PAIRED, and the decoder resolved both to the FIRST of them,
/// which is how #1491 finding T1 reported correct CLI egress as divergent.
///
/// PINS NEW BEHAVIOUR, and does not recover old: `origin/main` was already fail-open
/// here (its whole-map bail sits one line above the duplicate check it gates) and
/// this issue's first round carried the same bail into the new function. So there is
/// no earlier state this restores.
#[test]
fn one_unrenderable_key_does_not_cost_its_siblings_their_refusal() {
    let ty = ty_of("frozen<map<frozen<key_part>, int>>");
    let golden = json!({
        "charlie\\:3:8": 80,
        "{\"label\": null, \"rank\": 1}": 10,
        "{\"label\": \"null\", \"rank\": 1}": 20
    });
    let fields = match golden.as_object() {
        Some(fields) => fields,
        None => panic!("the golden is an object"),
    };
    let key_ty = match &ty {
        CqlType::Map(key_ty, _) => key_ty,
        _ => panic!("the declared type is a map"),
    };
    // The PREMISE: entry 0's key really does not render, and 1 and 2 really do
    // render alike. Both halves stated, so a fixture-spelling change cannot make
    // this test vacuous.
    let renderings: Vec<Option<String>> = fields
        .keys()
        .map(|key| entry_key_rendering(&ty, key))
        .collect();
    assert_eq!(
        renderings[0], None,
        "premise: entry 0's key does not render"
    );
    assert!(
        renderings[1].is_some() && renderings[1] == renderings[2],
        "premise: entries 1 and 2 render alike: {renderings:?}"
    );

    let refusals = map_key_refusals(fields, key_ty);
    assert_eq!(
        refusals.len(),
        3,
        "the answer is PER GOLDEN ENTRY, at full length: {refusals:?}"
    );
    assert_eq!(
        refusals[0], None,
        "an unrenderable key is a DIVERGENCE for the comparison to report, not a \
         refusal — and it says nothing about its siblings"
    );
    for i in [1usize, 2] {
        let why = match &refusals[i] {
            Some(why) => why,
            None => panic!("entry {i} of a colliding PAIR must stay suppressed: {refusals:?}"),
        };
        assert!(why.contains("SAME key text"), "{why}");
        // THE INDICES ARE THE GOLDEN'S, not indices into a compacted list of the
        // renderable keys — which would name entries 0 and 1 and so blame the very
        // key that is NOT ambiguous.
        assert!(
            why.contains("entries 1 and 2 "),
            "the reason must name the GOLDEN's entry indices: {why}"
        );
    }
}

/// …and the node-level answer for that same mixed shape is a REFUSAL, not `None` —
/// at [`Reach::Body`], because no split may be PROMISED here (roborev job 445).
///
/// The guard for the property above, at the level the decoder and the comparator both
/// read — and, like it, pinning NEW behaviour rather than recovering any.
///
/// TWO assertions, and they are independent:
///
///   * REFUSED AT ALL. Round 2's purpose: without this the colliding pair is
///     canonicalized and paired and the decoder resolves both to the first of them
///     (#1491 finding T1).
///   * REFUSED AT `Body`, NOT `MapKeys`. `MapKeys` tells the decoder it may split
///     this node's entries, and the three body checks that decide whether it may
///     cannot have run — they consume a rendering that could not be synthesized.
///     Round 2 answered `MapKeys` and so contradicted the dominance invariant this
///     very function documents. See [`super::unsynthesizable_rendering`] for the
///     residual that widening costs.
///
/// NOT VACUOUS, verified by mutation rather than asserted: restoring the whole-map
/// bail in `map_key_refusals` REDs this and its sibling; restoring `None` at
/// `decode_does_not_recover`'s site 1 REDs this one; and switching the reach back to
/// `MapKeys` REDs the second assertion below. (Restoring `None` at site 3 reds
/// NOTHING — that site is subsumed by site 1; `unsynthesizable_rendering`'s own doc
/// records the measurement.)
#[test]
fn a_mixed_key_node_does_not_fail_open() {
    let ty = ty_of("frozen<map<frozen<key_part>, int>>");
    let golden = json!({
        "charlie\\:3:8": 80,
        "{\"label\": null, \"rank\": 1}": 10,
        "{\"label\": \"null\", \"rank\": 1}": 20
    });
    let (reach, why) = match super::node_refusal_reach(&golden, Some(&ty), Kinding::Natural) {
        Some(found) => found,
        None => panic!(
            "FAIL-OPEN: an unrenderable key silenced the ambiguity between two OTHER \
             keys, so the colliding pair would be paired and mis-guided (#1491 T1)"
        ),
    };
    assert_eq!(
        reach,
        Reach::Body,
        "a split that could not be CHECKED must not be PROMISED: {why}"
    );
    assert!(
        why.contains("SAME key text") && why.contains("cannot be synthesized"),
        "the reason must name BOTH the key cause and why the reach is widened: {why}"
    );
    // The CONTROL: the same unrenderable key with NO colliding siblings is still not
    // refused at all — so this narrows rather than refusing every mixed node.
    let no_collision = json!({
        "charlie\\:3:8": 80,
        "{\"label\": \"a\", \"rank\": 1}": 10,
        "{\"label\": \"b\", \"rank\": 1}": 20
    });
    assert_eq!(
        super::node_refusal_reach(&no_collision, Some(&ty), Kinding::Natural),
        None,
        "a key that does not render is a divergence to REPORT, never a refusal"
    );
}

/// A BODY cause DOMINATES a key-scoped one: a node whose entries cannot be split
/// must not be reported [`Reach::MapKeys`], because that reach PROMISES the entry
/// boundaries are recoverable and the decoder splits on that promise (#3815).
#[test]
fn a_body_cause_dominates_a_key_scoped_one() {
    // A map VALUE carrying the separator breaks the ENTRY split, and the two keys
    // ALSO collide — so both causes are present at once.
    let ty = ty_of("frozen<map<frozen<key_part>, text>>");
    let golden = json!({
        "{\"label\": null, \"rank\": 1}": "x, y",
        "{\"label\": \"null\", \"rank\": 1}": "z"
    });
    let (reach, why) = match super::node_refusal_reach(&golden, Some(&ty), Kinding::Natural) {
        Some(found) => found,
        None => panic!("this node cannot be split at all"),
    };
    assert_eq!(reach, Reach::Body, "the body cause must win: {why}");
    assert!(why.contains("entry(s)"), "{why}");
    // THE PREMISE, asserted rather than assumed: there really IS a key-scoped cause
    // here for the body cause to dominate. Without this the case passes with nothing
    // to dominate, so a regression in collision detection would leave it green — the
    // same premise `one_unrenderable_key_does_not_cost_its_siblings_their_refusal`
    // states for its own input.
    let fields = match golden.as_object() {
        Some(fields) => fields,
        None => panic!("the golden is an object"),
    };
    let key_ty = match &ty {
        CqlType::Map(key_ty, _) => key_ty,
        _ => panic!("the declared type is a map"),
    };
    let refusals = map_key_refusals(fields, key_ty);
    assert!(
        refusals
            .iter()
            .filter_map(Option::as_ref)
            .any(|why| why.contains("SAME key text")),
        "premise: the two keys collide, so a key-scoped cause exists: {refusals:?}"
    );
}
