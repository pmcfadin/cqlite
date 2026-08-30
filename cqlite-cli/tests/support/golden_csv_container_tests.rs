//! Unit coverage for the CSV container decoder + refusal core (issue #1491),
//! split out of `golden_csv_container.rs` under the campsite rule (CLAUDE.md,
//! epic #1135). A child of that module, so `use super::*` reaches the decoder,
//! the refusal predicates and their helpers.
//!
//! The corpus reaches ONE refusal shape and none of the strictness rules.
//! Measured on the census: with the fetched corpus present the CSV lane reports
//! `1 REFUSED` — `test_types.nb_empty_collections`'s `fs`, a `frozen<set<text>>`
//! the golden carries EMPTY, i.e. EMPTY-CONTAINER. The committed tier alone
//! reports `0 REFUSED`, and no container member anywhere in the committed or
//! fetched corpus carries a `, `, a bracket, a `: ` in a map key, or an empty
//! scalar member. So every other shape the derived cause covers, and every
//! strictness rule, is exercised only here — which is what makes a census `0` mean
//! "the scan ran and found none" rather than "the scan may not work".
//!
//! Inputs are renderings in the grammar `ValueFormatter` documents; expected
//! outputs are the GOLDEN-side shapes `sstabledump` produces. Nothing here is
//! derived from CQLite's current output.

use super::*;
use serde_json::json;

/// The declared type of a column, parsed by the lane's OWN DDL parser from a
/// `CREATE TABLE` — so these cases exercise the real authority (the committed
/// schema) rather than a hand-built type tree.
fn ty_of(decl: &str) -> CqlType {
    let ddl = format!(
        "CREATE TYPE address (street text, city text, zip text); \
         CREATE TYPE person (first_name text, last_name text, age int); \
         CREATE TABLE t (id int PRIMARY KEY, c {decl});"
    );
    let schema = match super::super::schema::from_ddl(&ddl, "t") {
        Ok(schema) => schema,
        Err(why) => panic!("{decl}: {why}"),
    };
    match schema.column("c") {
        Some(column) => column.ty.clone(),
        None => panic!("{decl}: no column `c`"),
    }
}

// --- the refusal valve, per NODE and per CELL --------------------------
//
// Each case pins BOTH which function refuses and which does NOT, because the
// whole content of finding P2 is the BLAST RADIUS: a node-local cause that
// reported a whole-cell refusal would suppress positions that are decidable,
// and a whole-cell cause reported per node would let an unbalanced rendering
// be split anyway.

/// The element type's own bracket pair, for a node-local refusal query on a
/// nested position (`node_refusal` takes the declared type OF THAT NODE).
fn element_of(decl: &str) -> CqlType {
    match ty_of(decl) {
        CqlType::List(element) | CqlType::Set(element) => (*element).clone(),
        other => panic!("{decl} is not a list/set: {}", other.describe()),
    }
}

#[test]
fn member_containing_the_element_separator_is_refused_at_its_container() {
    // The golden's own rendering `{a, b}` splits into TWO members, so the
    // decoder cannot give this one-member golden back and a CORRECT rendering
    // would be read as something else…
    let ty = ty_of("set<text>");
    let why = node_refusal(&json!(["a, b"]), Some(&ty))
        .expect("a `, `-bearing member must refuse its container");
    assert!(
        why.contains("splits into 2 member(s), not the golden's 1"),
        "the reason must state what the decoder gave back: {why}"
    );
    // …and the rendering is still splittable at every OTHER depth, so the cell
    // is not refused: a `, ` inside a member sits at bracket depth ≥ 1 for
    // every enclosing level.
    assert_eq!(
        cell_refusal(&json!(["a, b"])),
        None,
        "a `, ` corrupts one body's split, not the whole cell's"
    );
}

#[test]
fn member_containing_an_unbalanced_bracket_refuses_the_whole_cell() {
    // A stray bracket unbalances the depth counter every level is split on, so
    // no level can be split reliably.
    let why = cell_refusal(&json!(["x}y"])).expect("an unbalanced member must refuse the cell");
    assert!(
        why.contains("not bracket-balanced"),
        "unexpected reason: {why}"
    );
    // Both directions of unbalance, and at depth too: the cause is a property
    // of the whole rendering.
    assert!(cell_refusal(&json!(["x[y"])).is_some());
    assert!(cell_refusal(&json!([["x}y"]])).is_some());
    assert!(cell_refusal(&json!({"k": "x}y"})).is_some());
    assert!(cell_refusal(&json!({"x}y": 1})).is_some());
}

/// Review round 11, finding R1: a BALANCED bracket pair inside a scalar member
/// leaves the depth counter where it found it, so every level's split is
/// undisturbed and the member decodes back byte for byte. Refusing it cost the
/// position its comparison for nothing — a refused node keeps only the
/// emptiness bound, so an incorrect NON-EMPTY body passed there.
///
/// The earlier rule was a character test (`contains('[')`), which is exactly
/// the parallel-predicate shape this module no longer has: the question is now
/// asked of the splitter, whose only failure mode is an IMBALANCE.
#[test]
fn a_balanced_bracket_inside_a_member_is_not_refused_and_is_compared() {
    let ty = ty_of("list<text>");
    let golden = json!(["[ok]"]);
    assert_eq!(
        cell_refusal(&golden),
        None,
        "a balanced bracket does not disturb any level's split"
    );
    assert_eq!(node_refusal(&golden, Some(&ty)), None);
    // …and the member is recovered exactly, so a WRONG member is a divergence
    // rather than a position nothing checks.
    assert_eq!(decode(&golden, "[[ok]]", &ty).expect("decodes"), golden);
    assert_ne!(decode(&golden, "[[wrong]]", &ty).expect("decodes"), golden);
    // A balanced pair carrying the separator INSIDE it is recovered too: the
    // `, ` sits at depth 1, so it is not a cut.
    let inner = json!(["[a, b]"]);
    assert_eq!(cell_refusal(&inner), None);
    assert_eq!(node_refusal(&inner, Some(&ty)), None);
    assert_eq!(decode(&inner, "[[a, b]]", &ty).expect("decodes"), inner);
}

#[test]
fn map_key_containing_a_separator_is_refused_at_its_object() {
    let ty = ty_of("map<text, int>");
    // The key/value cut is made at the FIRST top-level `: `, so the decoder
    // gives the key back as `a` — not the golden's `a: b`.
    let why = node_refusal(&json!({"a: b": 1}), Some(&ty))
        .expect("a `: `-bearing KEY must refuse its object");
    assert!(
        why.contains("recovers key `a`") && why.contains("not the golden's key `a: b`"),
        "the reason must state which key came back: {why}"
    );
    // A `, ` in a key splits one entry into two, which is the same loss.
    let why = node_refusal(&json!({"a, b": 1}), Some(&ty))
        .expect("a `, `-bearing KEY must refuse its object");
    assert!(
        why.contains("splits into 2 entry(s), not the golden's 1"),
        "the reason must state what the decoder gave back: {why}"
    );
    assert_eq!(cell_refusal(&json!({"a: b": 1})), None);
}

#[test]
fn map_value_containing_the_pair_separator_is_not_refused() {
    // Entries split at their FIRST top-level `: `, which is the real
    // separator, so a colon inside the VALUE is already decoded correctly.
    // Refusing it would narrow the lane for no reason.
    let ty = ty_of("map<text, text>");
    assert_eq!(node_refusal(&json!({"k": "a: b"}), Some(&ty)), None);
    assert_eq!(cell_refusal(&json!({"k": "a: b"})), None);
    let decoded = decode(&json!({"k": "a: b"}), "{k: a: b}", &ty).expect("decodes");
    assert_eq!(decoded, json!([{"key": "k", "value": "a: b"}]));
}

#[test]
fn a_sole_empty_member_is_refused_because_its_rendering_splits_into_none() {
    // `{}` is both "no members" and "one empty member", which the decoder
    // shows by giving ZERO members back for this one-member golden.
    let ty = ty_of("set<text>");
    let why = node_refusal(&json!([""]), Some(&ty)).expect("an empty member must be refused");
    assert!(
        why.contains("splits into 0 member(s), not the golden's 1"),
        "the reason must state what the decoder gave back: {why}"
    );
}

/// Review round 11, finding R2: an empty member WITH SIBLINGS is recovered
/// exactly, so the node is not refused. `["", "x"]` renders `[, x]`, whose
/// depth-zero `, ` is the separator, and the split gives `""` and `"x"` back.
///
/// The earlier rule refused the node for ANY empty member, which suppressed
/// its siblings' comparison and left only the emptiness bound — so any
/// non-empty body passed.
#[test]
fn an_empty_member_with_siblings_is_not_refused_and_is_compared() {
    let ty = ty_of("list<text>");
    for golden in [json!(["", "x"]), json!(["x", ""]), json!(["", ""])] {
        assert_eq!(
            node_refusal(&golden, Some(&ty)),
            None,
            "{golden}: the rendering splits back into exactly these members"
        );
    }
    // Recovered exactly, in both member positions…
    assert_eq!(
        decode(&json!(["", "x"]), "[, x]", &ty).expect("decodes"),
        json!(["", "x"])
    );
    assert_eq!(
        decode(&json!(["x", ""]), "[x, ]", &ty).expect("decodes"),
        json!(["x", ""])
    );
    // …so a DROPPED member is a length divergence the comparator reports,
    // where before it was a refused node accepting any non-empty body.
    assert_eq!(
        decode(&json!(["", "x"]), "[x]", &ty).expect("decodes"),
        json!(["x"])
    );
}

/// EMPTY-CONTAINER, the MIRROR of the case above: the golden container is
/// EMPTY and the CLI could perfectly well have held one member that renders
/// empty. The golden-side scan saw nothing to refuse there, so `{}` accepted
/// both readings — and the two are different values.
///
/// Bounded by the DECLARED element type, which is what makes this a
/// measurement and not blanket strictness: a `set<text>` member can BE the
/// empty string, a `list<int>` member always carries a digit, and a `tuple`'s
/// member count comes from the DDL (so the comparison's arity check sees a
/// dropped member).
#[test]
fn an_empty_container_is_refused_only_where_its_element_can_render_empty() {
    for decl in ["set<text>", "list<ascii>", "set<blob>", "list<timestamp>"] {
        let ty = ty_of(decl);
        let why = node_refusal(&json!([]), Some(&ty))
            .unwrap_or_else(|| panic!("{decl}: an empty container must be refused"));
        assert!(
            why.contains("empty container is indistinguishable"),
            "{decl}: unexpected reason: {why}"
        );
    }
    for decl in [
        "set<int>",
        "list<double>",
        "set<boolean>",
        "list<frozen<set<int>>>",
    ] {
        assert_eq!(
            node_refusal(&json!([]), Some(&ty_of(decl))),
            None,
            "{decl}: no member of this element type can render empty, so `[]` can \
             only mean zero members and must stay compared"
        );
    }
    // A tuple's arity is the DDL's, so `()` cannot hide a member.
    assert_eq!(
        node_refusal(&json!([]), Some(&ty_of("tuple<text, text>"))),
        None
    );
    // An empty map/UDT body is unambiguous too: every entry carries a `: `, so
    // a one-entry rendering can never be `{}`.
    assert_eq!(
        node_refusal(&json!({}), Some(&ty_of("map<text, text>"))),
        None
    );
    // And an UNDECLARED type refuses nothing: the comparison reports the shape.
    assert_eq!(node_refusal(&json!([]), None), None);
}

/// Finding P2: an ambiguous NESTED member is refused AT ITSELF, so the outer
/// container stays decidable. Deciding it one level up made a golden `[[]]`
/// accept a CLI `[]`, the unambiguous outer member silently dropped.
///
/// The end-to-end half of this (the comparator reporting that dropped member)
/// is `compare::tests::an_ambiguous_nested_member_does_not_suppress_its_container`.
#[test]
fn an_ambiguous_nested_member_is_refused_at_itself_and_not_at_its_container() {
    let outer = ty_of("list<frozen<list<text>>>");
    let inner = element_of("list<frozen<list<text>>>");
    // The inner empty `list<text>` is the indistinguishable position…
    let why = node_refusal(&json!([]), Some(&inner))
        .expect("the inner empty list<text> is the ambiguous position");
    assert!(
        why.contains("empty container is indistinguishable"),
        "{why}"
    );
    // …and the OUTER container, which holds exactly one member, is decidable:
    // its bracket kind and its member count are recoverable whatever the
    // member's own body turns out to mean.
    assert_eq!(
        node_refusal(&json!([[]]), Some(&outer)),
        None,
        "the outer container's own body is splittable, so it is not refused"
    );
    assert_eq!(cell_refusal(&json!([[]])), None);
    // The decode reflects that: the outer level is SPLIT (one member), and only
    // the refused member's body is left un-split for the count bounds.
    assert_eq!(
        decode(&json!([[]]), "[[]]", &outer).expect("decodes"),
        json!([""]),
        "one outer member, whose refused body is empty"
    );
    // A dropped outer member decodes to zero members, which is what lets the
    // comparator report it.
    assert_eq!(
        decode(&json!([[]]), "[]", &outer).expect("decodes"),
        json!([])
    );
    // And the refused member's own FRAME is still required at its depth.
    assert!(
        decode(&json!([[]]), "[{}]", &outer).is_err(),
        "the inner list's `[…]` frame is required even though its body is refused"
    );
}

/// Finding N3: a refusal suppresses the INDISTINGUISHABLE readings only.
///
/// The subject is the ambiguity the corpus actually reaches — an empty
/// `frozen<set<text>>`, which renders `{}` whether it holds nothing or one
/// empty member. Everything else about the cell is still decided, so a
/// `null`, an unrelated word, the wrong bracket or a non-empty body is a
/// divergence. Before this the whole cell was discarded before the CLI value
/// was looked at, so all four passed.
#[test]
fn a_refused_cell_still_has_its_frame_and_body_emptiness_compared() {
    let ty = ty_of("frozen<set<text>>");
    let empty = json!([]);
    // The one reading pair the format genuinely cannot tell apart.
    assert_eq!(
        decidable_despite_cell_refusal(&empty, &json!("{}"), &ty),
        Ok(()),
        "the empty bracket pair is exactly the indistinguishable case"
    );
    for (cli, expect) in [
        (Value::Null, "empty or non-text field"),
        (json!("null"), "opening"),
        (json!("unrelated text"), "opening"),
        (json!("[]"), "opening"),
        (json!("{a}"), "carries a body"),
    ] {
        let why = decidable_despite_cell_refusal(&empty, &cli, &ty)
            .expect_err(&format!("{cli} must diverge from an empty golden set"));
        assert!(why.contains(expect), "unexpected reason for {cli}: {why}");
    }

    // The other decidable count: two or more members cannot render empty,
    // whatever the refusal cause — here a `, `-bearing member.
    let two = json!(["a, b", "c"]);
    assert!(
        node_refusal(&two, Some(&ty)).is_some(),
        "the `, ` in a member is what refuses this node"
    );
    let why = decidable_despite_cell_refusal(&two, &json!("{}"), &ty)
        .expect_err("two members cannot render as an empty body");
    assert!(why.contains("cannot render as an empty body"), "{why}");
    // …and WHICH members the body holds stays suppressed, because that is
    // what the ambiguity destroys.
    assert_eq!(
        decidable_despite_cell_refusal(&two, &json!("{something, else}"), &ty),
        Ok(())
    );
    // At exactly ONE member the empty body is a legal rendering (of one empty
    // member), so nothing is asserted about the count there.
    assert_eq!(
        decidable_despite_cell_refusal(&json!([""]), &json!("{}"), &ty),
        Ok(())
    );
}

/// The per-NODE half of the same rule, on the BODY the decoder leaves: the
/// same emptiness bound, and the same suppression of which members the body
/// holds.
/// The frame is not re-checked here because the decoder already required it at
/// that depth — which is what makes it checked at EVERY depth rather than only
/// at the cell's outer level.
#[test]
fn a_refused_node_still_has_its_body_emptiness_compared() {
    for (golden, body, expect) in [
        (json!([]), "", None),
        (json!([]), "a", Some("carries a body")),
        (json!([""]), "", None),
        (json!([""]), "anything", None),
        (
            json!(["a, b", "c"]),
            "",
            Some("cannot render as an empty body"),
        ),
        (json!(["a, b", "c"]), "something, else", None),
    ] {
        let outcome = decidable_despite_node_refusal(&golden, &Value::String(body.into()));
        match expect {
            None => assert_eq!(outcome, Ok(()), "golden {golden} vs body `{body}`"),
            Some(needle) => {
                let why = outcome.expect_err(&format!("golden {golden} vs body `{body}`"));
                assert!(why.contains(needle), "unexpected reason: {why}");
            }
        }
    }
    // A refused node whose CLI side is not text at all: the decoder always
    // leaves a body, so this is a divergence rather than an ambiguity.
    assert!(decidable_despite_node_refusal(&json!([]), &Value::Null).is_err());
}

#[test]
fn ordinary_corpus_content_is_not_refused() {
    // Spaces, hyphens, `0x` hex, exact decimals and nesting are all fine —
    // only the separators and brackets are structural. (`1 Navy Way` is real
    // content from test_compactionparityudt.udt_collections.)
    let map_ty = ty_of("map<text, frozen<address>>");
    let nested = json!({"home": {"street": "1 Navy Way", "zip": "22201"}});
    assert_eq!(cell_refusal(&nested), None);
    assert_eq!(node_refusal(&nested, Some(&map_ty)), None);
    // The nested UDT node too, under its own declared type.
    let address = match &map_ty {
        CqlType::Map(_, value) => (**value).clone(),
        other => panic!("not a map: {}", other.describe()),
    };
    assert_eq!(
        node_refusal(
            &json!({"street": "1 Navy Way", "zip": "22201"}),
            Some(&address)
        ),
        None
    );
    let list_ty = ty_of("list<text>");
    let scalars = json!(["0xdeadbeef", "-1.5", "neg-five", null]);
    assert_eq!(cell_refusal(&scalars), None);
    assert_eq!(node_refusal(&scalars, Some(&list_ty)), None);
}

// --- strictness: the decoder must not repair a malformed rendering ------

#[test]
fn the_element_separator_must_be_exactly_comma_space() {
    // A writer that dropped the space must NOT decode as two members; that
    // tolerance is what would let a framing regression through.
    let decoded = decode(&json!([1, 2]), "[1,2]", &ty_of("frozen<list<int>>")).expect("one member");
    assert_eq!(decoded, json!(["1,2"]), "`,` was wrongly treated as `, `");
}

#[test]
fn a_mismatched_or_unbalanced_bracket_is_an_error() {
    let list = ty_of("frozen<list<int>>");
    assert!(
        decode(&json!([1]), "[1}", &list).is_err(),
        "mismatched bracket must fail"
    );
    assert!(
        decode(&json!([1]), "[[1]", &list).is_err(),
        "unclosed bracket must fail"
    );
    assert!(
        decode(&json!([1]), "1, 2", &list).is_err(),
        "a bare body must fail"
    );
    assert!(
        decode(&json!({"k": 1}), "[k: 1]", &ty_of("map<text, int>")).is_err(),
        "a map needs braces"
    );
}

#[test]
fn a_map_entry_without_the_pair_separator_is_an_error() {
    assert!(decode(&json!({"k": 1}), "{k=1}", &ty_of("map<text, int>")).is_err());
}

// --- the bracket comes from the DECLARED type (review finding R2) -------

#[test]
fn each_collection_kind_requires_its_own_bracket() {
    // The grammar `ValueFormatter` documents, one kind per bracket. The
    // golden is a JSON array for all three, which is exactly why the DDL — not
    // the golden — has to answer the question.
    assert_eq!(
        decode(&json!([1, 2]), "[1, 2]", &ty_of("frozen<list<int>>")).unwrap(),
        json!(["1", "2"])
    );
    assert_eq!(
        decode(&json!([1, 2]), "{1, 2}", &ty_of("set<int>")).unwrap(),
        json!(["1", "2"])
    );
    assert_eq!(
        decode(&json!([1, 2]), "(1, 2)", &ty_of("tuple<int, int>")).unwrap(),
        json!(["1", "2"])
    );
}

/// The other side of R2: a set or tuple rendered with LIST brackets is a
/// failure. The earlier rule accepted `[`, `{` and `(` for any golden array,
/// so this regression passed.
#[test]
fn a_collection_rendered_with_another_kinds_bracket_is_an_error() {
    for (decl, wrong) in [
        ("set<int>", "[1, 2]"),
        ("set<int>", "(1, 2)"),
        ("tuple<int, int>", "[1, 2]"),
        ("tuple<int, int>", "{1, 2}"),
        ("frozen<list<int>>", "{1, 2}"),
        ("frozen<list<int>>", "(1, 2)"),
    ] {
        let ty = ty_of(decl);
        let why = decode(&json!([1, 2]), wrong, &ty)
            .expect_err("the declared kind's bracket is required: {decl} vs {wrong}");
        assert!(
            why.contains(&ty.describe()),
            "the failure must name the declared type: {why}"
        );
    }
    // A map/UDT rendered with list brackets likewise.
    assert!(decode(&json!({"k": 1}), "(k: 1)", &ty_of("map<text, int>")).is_err());
    assert!(decode(&json!({"zip": "1"}), "[zip: 1]", &ty_of("frozen<address>")).is_err());
}

/// A NESTED collection's bracket is required too — the type is threaded all
/// the way down, so an inner set rendered `[…]` fails at depth.
#[test]
fn a_nested_collections_bracket_is_required_at_depth() {
    let ty = ty_of("frozen<map<text, frozen<set<int>>>>");
    assert_eq!(
        decode(&json!({"a": [1]}), "{a: {1}}", &ty).unwrap(),
        json!([{"key": "a", "value": ["1"]}])
    );
    assert!(
        decode(&json!({"a": [1]}), "{a: [1]}", &ty).is_err(),
        "an inner set rendered with list brackets must fail"
    );
}

// --- decoding ----------------------------------------------------------

#[test]
fn an_empty_body_decodes_to_zero_members() {
    assert_eq!(
        decode(&json!([]), "[]", &ty_of("frozen<list<int>>")).unwrap(),
        json!([])
    );
    assert_eq!(
        decode(&json!([]), "{}", &ty_of("set<int>")).unwrap(),
        json!([])
    );
    assert_eq!(
        decode(&json!({}), "{}", &ty_of("map<text, int>")).unwrap(),
        json!([])
    );
}

#[test]
fn nesting_is_decoded_at_depth() {
    // A map<text, frozen<udt>>, as in test_compactionparityudt.udt_collections:
    // the inner `, ` and `: ` must not be mistaken for outer separators.
    let golden = json!({"home": {"street": "1 Navy Way", "city": "Arlington"}});
    let decoded = decode(
        &golden,
        "{home: {street: 1 Navy Way, city: Arlington}}",
        &ty_of("map<text, frozen<address>>"),
    )
    .unwrap();
    assert_eq!(
        decoded,
        json!([{
            "key": "home",
            "value": [
                {"key": "street", "value": "1 Navy Way"},
                {"key": "city", "value": "Arlington"},
            ],
        }])
    );
}

#[test]
fn the_null_token_is_resolved_from_the_goldens_type() {
    // NULL-TOKEN, in both directions: a null member decodes to null, and a
    // `text` member holding "null" stays text.
    let person = ty_of("frozen<person>");
    assert_eq!(
        decode(&json!({"last_name": null}), "{last_name: null}", &person).unwrap(),
        json!([{"key": "last_name", "value": null}])
    );
    assert_eq!(
        decode(&json!({"last_name": "null"}), "{last_name: null}", &person).unwrap(),
        json!([{"key": "last_name", "value": "null"}])
    );
}

#[test]
fn a_surplus_member_is_kept_so_the_length_mismatch_is_reported() {
    // The decoder must not silently truncate to the golden's length — the
    // comparison is what reports the divergence.
    let decoded = decode(&json!([1]), "[1, 2]", &ty_of("frozen<list<int>>")).unwrap();
    assert_eq!(decoded, json!(["1", "2"]));
}

/// A member beyond a TUPLE's declared arity has no declared type, so it is
/// kept as raw text rather than guessed at — the comparator's arity check is
/// what reports it.
#[test]
fn a_member_beyond_a_tuples_arity_is_kept_as_text() {
    let decoded = decode(&json!([1]), "(1, 2)", &ty_of("tuple<int>")).unwrap();
    assert_eq!(decoded, json!(["1", "2"]));
}
