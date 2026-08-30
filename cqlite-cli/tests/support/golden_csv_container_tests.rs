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

// --- the refusal valve, per NODE ---------------------------------------
//
// There is ONE refusal predicate, asked per NODE, because the whole content of
// findings P2 and S1 is the BLAST RADIUS: a cause reported one level up
// suppresses positions that are perfectly decidable, and a refused node keeps
// only its frame and its body's emptiness — so over-refusal is a blind spot,
// not conservatism. Each case therefore pins BOTH the node that IS refused and
// the enclosing (or sibling) node that is NOT.

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
    // …and the rendering is still splittable at every OTHER depth, so no
    // enclosing level is refused: a `, ` inside a member sits at bracket
    // depth ≥ 1 for every level above the one holding it.
    let outer = ty_of("list<frozen<set<text>>>");
    assert_eq!(
        node_refusal(&json!([["a, b"]]), Some(&outer)),
        None,
        "a `, ` corrupts one body's split, not its enclosing levels'"
    );
}

/// An UNBALANCED bracket defeats the depth counter, so the node whose body it
/// sits in cannot be split at all — and unlike a stray `, ` it can reach levels
/// ABOVE that one. It is asked at EVERY node, on that node's own complete
/// rendering, so it refuses exactly the levels whose split it breaks.
#[test]
fn an_unbalanced_bracket_is_refused_at_each_node_whose_split_it_breaks() {
    let list = ty_of("list<text>");
    // `[x}y]`: the `}` closes a bracket that never opened, so the splitter
    // cannot read the golden's own rendering at all.
    let why = node_refusal(&json!(["x}y"]), Some(&list))
        .expect("an unbalanced member must refuse its container");
    assert!(
        why.contains("cannot split the golden's own rendering"),
        "unexpected reason: {why}"
    );
    // Both directions of the imbalance, and in a map/UDT key as well as in a
    // member or a value.
    assert!(node_refusal(&json!(["x[y"]), Some(&list)).is_some());
    assert!(node_refusal(&json!({"k": "x}y"}), Some(&ty_of("map<text, text>"))).is_some());
    assert!(node_refusal(&json!({"x}y": 1}), Some(&ty_of("map<text, int>"))).is_some());

    // It reaches an ENCLOSING level when the enclosing body really is
    // unsplittable: `[[x}y]]` closes one bracket too many at depth 1, so the
    // outer body cannot be scanned either and BOTH nodes are refused.
    let outer = ty_of("list<frozen<list<text>>>");
    assert!(node_refusal(&json!([["x}y"]]), Some(&outer)).is_some());
    assert!(node_refusal(
        &json!(["x}y"]),
        Some(&element_of("list<frozen<list<text>>>"))
    )
    .is_some());

    // And because the refusal comes FIRST, the decoder is never asked to split
    // the text a correct CLI renders for such a node: it requires the frame and
    // hands the un-split body on for the emptiness bound, instead of reporting
    // an "unbalanced bracket" divergence the CLI did not cause.
    assert_eq!(
        decode(&json!(["x}y"]), "[x}y]", &list).expect("the frame is satisfied"),
        json!("x}y")
    );
}

/// Review round 12, finding S1: BALANCE IS A PROPERTY OF THE CONCATENATED
/// RENDERING, not of each scalar in isolation.
///
/// An inner `list<text>` holding `"["` and `"]"` renders `[[, ]]`: the two
/// members' brackets balance EACH OTHER before the enclosing boundary, so every
/// enclosing level's depth-zero split is intact and only the inner node is
/// undecodable. The earlier rule scanned each scalar on its own and promoted any
/// individually-unbalanced one to a WHOLE-CELL refusal, which left the outer
/// member count and every unambiguous outer sibling with nothing but the
/// emptiness bound.
#[test]
fn opposing_brackets_in_nested_siblings_refuse_only_the_inner_node() {
    let outer = ty_of("list<frozen<list<text>>>");
    let inner = element_of("list<frozen<list<text>>>");
    let golden = json!([["[", "]"], ["ok"]]);

    // The inner node's own body `[, ]` scans (the brackets balance) but gives
    // ONE member back where the golden has two, so it is refused…
    let why = node_refusal(&json!(["[", "]"]), Some(&inner))
        .expect("the inner node's members are not recovered");
    assert!(
        why.contains("splits into 1 member(s), not the golden's 2"),
        "unexpected reason: {why}"
    );
    // …and the OUTER node is not: its body `[[, ]], [ok]` scans to depth zero
    // between the two members, so both come back exactly.
    assert_eq!(
        node_refusal(&golden, Some(&outer)),
        None,
        "the outer split is intact, so the outer node is decidable"
    );
    // Which the decode shows: the outer level IS split, the refused inner node
    // keeps its un-split body, and the unambiguous sibling is decoded.
    assert_eq!(
        decode(&golden, "[[[, ]], [ok]]", &outer).expect("decodes"),
        json!(["[, ]", ["ok"]])
    );
    // So a dropped outer member is a length divergence, and a wrong sibling a
    // value one — neither of which a whole-cell refusal could see.
    assert_eq!(
        decode(&golden, "[[[, ]]]", &outer).expect("decodes"),
        json!(["[, ]"])
    );
    assert_eq!(
        decode(&golden, "[[[, ]], [wrong]]", &outer).expect("decodes"),
        json!(["[, ]", ["wrong"]])
    );
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
        node_refusal(&golden, Some(&ty)),
        None,
        "a balanced bracket does not disturb any level's split"
    );
    // …and the member is recovered exactly, so a WRONG member is a divergence
    // rather than a position nothing checks.
    assert_eq!(decode(&golden, "[[ok]]", &ty).expect("decodes"), golden);
    assert_ne!(decode(&golden, "[[wrong]]", &ty).expect("decodes"), golden);
    // A balanced pair carrying the separator INSIDE it is recovered too: the
    // `, ` sits at depth 1, so it is not a cut.
    let inner = json!(["[a, b]"]);
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
    // And no ENCLOSING level is refused for it: the entry's own brackets
    // balance, so the level above splits normally.
    assert_eq!(
        node_refusal(
            &json!([{"a: b": 1}]),
            Some(&ty_of("list<frozen<map<text, int>>>"))
        ),
        None
    );
}

#[test]
fn map_value_containing_the_pair_separator_is_not_refused() {
    // Entries split at their FIRST top-level `: `, which is the real
    // separator, so a colon inside the VALUE is already decoded correctly.
    // Refusing it would narrow the lane for no reason.
    let ty = ty_of("map<text, text>");
    assert_eq!(node_refusal(&json!({"k": "a: b"}), Some(&ty)), None);
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

/// Finding N3 at a CELL's OWN ROOT NODE: a refusal there suppresses the
/// INDISTINGUISHABLE readings only, and the FRAME is required by the decoder at
/// that node's depth exactly as at any deeper one.
///
/// The subject is the ambiguity the corpus actually reaches — an empty
/// `frozen<set<text>>`, which renders `{}` whether it holds nothing or one
/// empty member. Everything else about the cell is still decided, so `null`, an
/// unrelated word or the wrong bracket is a divergence. Before finding N3 the
/// whole cell was discarded before the CLI value was looked at, so all three
/// passed; since round 12 there is no cell-level entry point at all (finding
/// S1) and this is the ROOT node of the same one rule.
#[test]
fn a_refused_root_node_still_has_its_frame_required_by_the_decoder() {
    let ty = ty_of("frozen<set<text>>");
    let empty = json!([]);
    assert!(
        node_refusal(&empty, Some(&ty)).is_some(),
        "an empty set<text> is the corpus's own refusal"
    );
    // The one reading pair the format genuinely cannot tell apart: the frame is
    // satisfied and the un-split body is handed on for the emptiness bound.
    assert_eq!(
        decode(&empty, "{}", &ty).expect("the frame is satisfied"),
        json!(""),
        "the empty bracket pair is exactly the indistinguishable case"
    );
    // Property 2, at the ROOT node's own depth: the bracket pair the DECLARED
    // type requires. A `set` rendered `[…]` is a failure, not a list.
    for cli in ["null", "unrelated text", "[]"] {
        let why = decode(&empty, cli, &ty)
            .expect_err(&format!("`{cli}` is not a `set` rendering at all"));
        assert!(
            why.contains("opening"),
            "unexpected reason for `{cli}`: {why}"
        );
    }
    let why = decode(&empty, "{a", &ty).expect_err("an unclosed frame is a divergence");
    assert!(why.contains("does not close"), "{why}");
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
    // A refused node whose CLI side is not text at all — the shape an EMPTY
    // CSV field takes, which the decode is never even attempted for. The
    // decoder otherwise always leaves a body, so this is a divergence rather
    // than an ambiguity (property 1).
    let why = decidable_despite_node_refusal(&json!([]), &Value::Null)
        .expect_err("an absent cell against a golden container is a divergence");
    assert!(why.contains("empty or non-text field"), "{why}");
}

#[test]
fn ordinary_corpus_content_is_not_refused() {
    // Spaces, hyphens, `0x` hex, exact decimals and nesting are all fine —
    // only the separators and brackets are structural. (`1 Navy Way` is real
    // content from test_compactionparityudt.udt_collections.)
    let map_ty = ty_of("map<text, frozen<address>>");
    let nested = json!({"home": {"street": "1 Navy Way", "zip": "22201"}});
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
