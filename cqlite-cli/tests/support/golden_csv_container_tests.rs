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

/// [`super::node_refusal`] at a NATURAL position, which is where all but the
/// stringified cases below live. Shadows the module's own name on purpose, so a
/// case that is not about [`Kinding`] does not have to spell one; a STRINGIFIED
/// case calls `super::node_refusal` directly and NAMES its kinding, which is the
/// only way to tell the two apart in a diff.
fn node_refusal(golden: &Value, ty: Option<&CqlType>) -> Option<String> {
    super::node_refusal(golden, ty, Kinding::Natural)
}

/// [`super::decode`] at a NATURAL position, for the same reason.
fn decode(golden: &Value, text: &str, ty: &CqlType) -> Result<Value, String> {
    super::decode(golden, text, ty, Kinding::Natural)
}

/// The declared type of a column, parsed by the lane's OWN DDL parser from a
/// `CREATE TABLE` — so these cases exercise the real authority (the committed
/// schema) rather than a hand-built type tree.
fn ty_of(decl: &str) -> CqlType {
    let ddl = format!(
        "CREATE TYPE address (street text, city text, zip text); \
         CREATE TYPE person (first_name text, last_name text, age int); \
         CREATE TYPE key_part (label text, rank int); \
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

/// The `CqlType` variant a declared type parses to. TOTAL, with no `_` arm, so a
/// new variant is a compile error here exactly as it is in
/// [`member_can_render_empty`] and [`stringified_csv_text`] — which is what keeps
/// the [`VARIANTS`] census honest for every test that takes one.
fn tag(ty: &CqlType) -> &'static str {
    match ty {
        CqlType::Numeric(_) => "numeric",
        CqlType::Text(_) => "text",
        CqlType::Boolean => "boolean",
        CqlType::Blob => "blob",
        CqlType::Timestamp => "timestamp",
        CqlType::Opaque(_) => "opaque",
        CqlType::List(_) => "list",
        CqlType::Set(_) => "set",
        CqlType::Map(..) => "map",
        CqlType::Tuple(_) => "tuple",
        CqlType::Udt(_) => "udt",
    }
}

/// Every tag [`tag`] can return. A test that censuses declared types requires each
/// entry to be REACHED by one of its cases, so a variant with no case FAILS rather
/// than being silently unmeasured; `tag`'s total match is the other half (a new
/// variant cannot compile without an arm, and the author then lands here).
const VARIANTS: &[&str] = &[
    "numeric",
    "text",
    "boolean",
    "blob",
    "timestamp",
    "opaque",
    "list",
    "set",
    "map",
    "tuple",
    "udt",
];

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
/// dropped member). The bound is `member_can_render_empty`, established per
/// type from the formatter itself by
/// `an_empty_rendering_is_possible_only_for_text` below.
#[test]
fn an_empty_container_is_refused_only_where_its_element_can_render_empty() {
    for decl in ["set<text>", "list<ascii>", "set<varchar>"] {
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
        // Finding Y2: these THREE used to be refused, by a deny-list that answered
        // "this element can render empty" for every type nobody had established. An
        // empty `blob` renders `0x`, a `timestamp` a fixed-width pattern and a
        // `uuid` 36 characters, so `[]`/`{}` there can only mean zero members —
        // refusing them dropped decidable cells from the coverage counts.
        "set<blob>",
        "list<timestamp>",
        "set<uuid>",
        "list<inet>",
        "set<duration>",
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

// --- the SEAM: a stringified spelling is not a CSV spelling ---------------
//
// `golden_rendering` synthesizes the text the golden WOULD render as, so the
// decoder can be asked whether that text round-trips. Its scalars come from the
// golden — and at a `Kinding::Stringified` position `sstabledump` wrote the
// golden with `writeString(type.getString(v))`, whose blob spelling is the BARE
// hex. Left untranslated, the empty blob's `""` synthesized an EMPTY body, the
// node was refused as unrecoverable, and a refused ONE-member node accepts ANY
// framed body — so the member went uncompared.

/// The synthetic rendering AT A STRINGIFIED POSITION carries the CSV spelling,
/// not the golden's cell-path spelling.
///
/// Expectations: the golden side is the pin (`cassandra-5.0.8`
/// `BytesSerializer.toString` = `ByteBufferUtil.bytesToHex`, so the empty blob is
/// `""`); the CSV side is `ValueFormatter`'s `format!("0x{hex}")`, measured
/// directly below rather than asserted in prose.
#[test]
fn a_stringified_blob_renders_as_the_0x_form_the_csv_egress_emits() {
    use cqlite_core::types::Value as CoreValue;
    use cqlite_core::util::value_fmt::ValueFormatter;

    // The CSV half of the claim, from the egress itself.
    assert_eq!(
        ValueFormatter::format_value(&CoreValue::blob(Vec::new())),
        "0x"
    );
    assert_eq!(
        ValueFormatter::format_value(&CoreValue::blob(vec![0xde, 0xad])),
        "0xdead"
    );

    let set = ty_of("set<blob>");
    assert_eq!(
        golden_rendering(&json!([""]), Some(&set), Kinding::Stringified).as_deref(),
        Some("{0x}"),
        "the empty blob's cell path is `\"\"`, and the CSV egress renders it `0x`"
    );
    assert_eq!(
        golden_rendering(&json!(["dead", ""]), Some(&set), Kinding::Stringified).as_deref(),
        Some("{0xdead, 0x}")
    );
    // A map KEY is a value the golden always spells stringified (a JSON object key
    // can only be a string), which is how `compare::compare_map` reads it.
    assert_eq!(
        golden_rendering(
            &json!({"": 7}),
            Some(&ty_of("map<blob, int>")),
            Kinding::Natural
        )
        .as_deref(),
        Some("{0x: 7}")
    );
    // A UDT entry's key is a FIELD NAME, not a value: never translated.
    assert_eq!(
        golden_rendering(
            &json!({"street": "s", "city": "c", "zip": "z"}),
            Some(&ty_of("frozen<address>")),
            Kinding::Natural,
        )
        .as_deref(),
        Some("{street: s, city: c, zip: z}")
    );
}

/// The translation is keyed on the POSITION's kinding, not applied blindly —
/// the same asymmetry `canon_typed` applies (finding M1's rule): only the golden,
/// and only where `sstabledump` stringified.
///
/// At a NATURAL position the golden already carries `BytesType.toJSONString`'s
/// `0x` form, so translating there would either double the prefix or invent a
/// spelling; a bare-hex golden there is not a spelling Cassandra emits and stays
/// verbatim, which is what keeps this from becoming a second, looser notion of
/// what a blob is spelled as.
#[test]
fn the_blob_translation_applies_only_at_a_stringified_position() {
    let set = ty_of("set<blob>");
    assert_eq!(
        golden_rendering(&json!(["0xdead"]), Some(&set), Kinding::Natural).as_deref(),
        Some("{0xdead}"),
        "an already-prefixed golden must not gain a second prefix"
    );
    assert_eq!(
        golden_rendering(&json!(["0xdead"]), Some(&set), Kinding::Stringified).as_deref(),
        Some("{0xdead}"),
        "`0xdead` is not a spelling `BytesSerializer.toString` can produce"
    );
    assert_eq!(
        golden_rendering(&json!([""]), Some(&set), Kinding::Natural).as_deref(),
        Some("{}"),
        "a NATURAL position is left verbatim, so this stays the ambiguous body"
    );
    // And a FROZEN map's key comes from `toJSONString`, so it is already prefixed
    // and is left alone by the same shape guard.
    assert_eq!(
        golden_rendering(
            &json!({"0x61": 7}),
            Some(&ty_of("frozen<map<blob, int>>")),
            Kinding::Natural,
        )
        .as_deref(),
        Some("{0x61: 7}")
    );
}

/// The consequence at the refusal valve, both ways: with the CSV spelling the sole
/// empty-blob member is RECOVERABLE and therefore COMPARED, and the decoder reads
/// the egress's own `{0x}` back into the golden's one member.
#[test]
fn a_sole_empty_blob_member_is_recovered_and_not_refused() {
    let set = ty_of("set<blob>");
    assert_eq!(
        super::node_refusal(&json!([""]), Some(&set), Kinding::Stringified),
        None,
        "`{{0x}}` splits back into exactly the golden's one member"
    );
    assert_eq!(
        super::decode(&json!([""]), "{0x}", &set, Kinding::Stringified),
        Ok(json!(["0x"]))
    );
    // The same node at a NATURAL position IS refused — the empty body it
    // synthesizes there splits into zero members.
    let why = super::node_refusal(&json!([""]), Some(&set), Kinding::Natural)
        .expect("an empty verbatim member must be refused");
    assert!(why.contains("splits into 0 member"), "unexpected: {why}");
}

/// The EMPTY-CONTAINER bound, taken from the FORMATTER instead of asserted in
/// prose: render the EMPTIEST value each declared type has through
/// `cqlite_core::util::value_fmt::ValueFormatter` — the very formatter the CSV
/// egress uses — and require [`member_can_render_empty`] to agree with what came
/// out.
///
/// # Why consulting CQLite here is not circular
///
/// This is the ONE question in this lane answered from CQLite's own code, and it is
/// a different question from the ones the golden answers. It does not ask what a
/// value of this type SHOULD render as — that is the `sstabledump` golden's, and
/// answering it from CQLite would be circular (CLAUDE.md, #3042). It asks whether
/// this egress's rendering of SOME value of the type can be ZERO-LENGTH, i.e. what
/// shape the output can take, which nothing outside the formatter can answer. The
/// answer only ever decides whether an empty container is REFUSED; every value the
/// comparison then makes is still the golden's.
///
/// # What the sample is, and what it is not
///
/// Each case is the type's emptiest or most degenerate value — a zero-length
/// blob/varint/inet, an all-zero duration, the epoch instant, the empty string —
/// i.e. the branch an empty rendering could plausibly come from. It is a SAMPLE and
/// not a proof over every value; the proof is the per-branch enumeration
/// [`member_can_render_empty`] records, and this case is what stops that record
/// drifting from the formatter it describes (round 19, finding Y2: the record said
/// `blob` could render empty, and `0x` is what the formatter has always produced).
#[test]
fn an_empty_rendering_is_possible_only_for_text() {
    use cqlite_core::types::{UdtField, UdtValue, Value};
    use cqlite_core::util::value_fmt::ValueFormatter;
    use std::collections::BTreeSet;

    let cases: Vec<(&str, Value)> = vec![
        // The one type whose rendering CAN be empty: `Value::Text` is the single
        // formatter branch that passes its payload through unchanged.
        ("text", Value::text("")),
        ("varchar", Value::text("")),
        ("ascii", Value::text("")),
        ("int", Value::Integer(0)),
        ("varint", Value::varint(Vec::new())),
        (
            "decimal",
            Value::Decimal {
                scale: 0,
                unscaled: Vec::new(),
            },
        ),
        ("double", Value::Float(0.0)),
        ("boolean", Value::Boolean(false)),
        // `0x` — the finding's own case.
        ("blob", Value::blob(Vec::new())),
        ("timestamp", Value::Timestamp(0)),
        ("date", Value::Date(0)),
        ("time", Value::Time(0)),
        ("uuid", Value::Uuid([0u8; 16])),
        ("timeuuid", Value::Uuid([0u8; 16])),
        // A length no address has, i.e. the formatter's degenerate branch.
        ("inet", Value::inet(Vec::new())),
        (
            "duration",
            Value::Duration {
                months: 0,
                days: 0,
                nanos: 0,
            },
        ),
        ("set<int>", Value::Set(Vec::new())),
        ("list<text>", Value::List(Vec::new())),
        ("map<text, text>", Value::Map(Vec::new())),
        ("tuple<int, text>", Value::Tuple(Vec::new())),
        (
            "frozen<address>",
            Value::Udt(Box::new(UdtValue {
                type_name: "address".to_string(),
                keyspace: "ks".to_string(),
                fields: vec![UdtField {
                    name: "street".to_string(),
                    value: None,
                }],
            })),
        ),
    ];

    let mut seen: BTreeSet<&'static str> = BTreeSet::new();
    for (decl, value) in &cases {
        let rendered = ValueFormatter::format_value(value);
        let ty = ty_of(decl);
        seen.insert(tag(&ty));
        assert_eq!(
            member_can_render_empty(&ty),
            rendered.is_empty(),
            "{decl}: the formatter rendered {rendered:?}, which the EMPTY-CONTAINER \
             bound contradicts"
        );
    }
    for variant in VARIANTS {
        assert!(
            seen.contains(variant),
            "no case establishes the {variant} variant's emptiness answer from the \
             formatter"
        );
    }
    // A NULL member widens no type's answer: it renders as the `null` TOKEN (the
    // module doc's NULL-TOKEN), which is four characters.
    assert_eq!(ValueFormatter::format_value(&Value::Null), "null");
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

// --- a MAP whose declared KEY type is a container (issue #3726) ---------
//
// The key type used throughout is quoted from the committed
// `test-data/schemas/nested-udt-keys.cql`; `key_part` is added to [`ty_of`]'s DDL
// preamble so these cases parse against the real schema reader.

/// A container key is DECODED under its declared type, not kept as raw text. Left
/// as text, `compare::compare_map` was handed a flat scalar where the DDL declares
/// a container, so the CSV half of a container-keyed map could not be compared.
///
/// The rendering is the one the CSV egress MEASURABLY emits for
/// `test_nested_udt_keys.nested_udt_keys`'s `f_map_tuple_udt` (row `id=1`); the
/// golden is that row's own golden cell.
#[test]
fn a_container_map_key_is_decoded_under_its_declared_type() {
    let ty = ty_of("frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>");
    let golden = json!({"[{\"label\": \"mkey-a\", \"rank\": 21}, 1]": 210});
    let decoded = decode(&golden, "{({label: mkey-a, rank: 21}, 1): 210}", &ty)
        .expect("the rendering inverts the grammar");
    assert_eq!(
        decoded,
        json!([{
            // The tuple's two slots, the first of them a UDT in this module's
            // `{key,value}` spelling (CSV cannot tell a UDT from a map, so every
            // brace-delimited body decodes that way).
            "key": [
                [{"key": "label", "value": "mkey-a"}, {"key": "rank", "value": "21"}],
                "1"
            ],
            "value": "210"
        }]),
        "decoded: {decoded}"
    );
}

/// The `, ` and `: ` INSIDE a container key are not top-level cuts, because
/// [`scan`] tracks `[ { (` depth — which is what lets the entry split and the
/// key/value cut of a container-keyed map work at all. Asserted on [`scan`]
/// directly, so the property is pinned where it lives rather than inferred from a
/// decode that happens to succeed.
#[test]
fn the_separators_inside_a_container_key_are_not_top_level_cuts() {
    let body = "({label: mkey-a, rank: 21}, 1): 210, ({label: mkey-b, rank: 22}, 2): 220";
    assert_eq!(
        scan(body, ", ").expect("balanced"),
        vec![body.find(", (").expect("the entry separator")],
        "only the `, ` BETWEEN the two entries is at depth zero"
    );
    let entry = "({label: mkey-a, rank: 21}, 1): 210";
    assert_eq!(
        scan(entry, ": ").expect("balanced"),
        vec![entry.rfind(": ").expect("the entry cut")],
        "only the `: ` after the key's closing bracket is at depth zero"
    );
    // And the cut the decoder actually makes agrees with that.
    assert_eq!(
        entry_cut(entry).expect("cuts"),
        ("({label: mkey-a, rank: 21}, 1)", "210")
    );
}

/// The golden's own rendering of a container key is what the refusal machinery
/// requires the decoder to recover, so [`entry_key_rendering`] must render the
/// PARSED key rather than the golden's JSON text. Left as that text, the `, ` and
/// `: ` inside it would make the golden's own rendering unsplittable and the node
/// would be REFUSED — which is how the CSV half of this stayed open.
#[test]
fn the_goldens_container_key_renders_in_the_csv_grammar() {
    let ty = ty_of("frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>");
    let golden = json!({"[{\"label\": \"mkey-a\", \"rank\": 21}, 1]": 210});
    assert_eq!(
        golden_rendering(&golden, Some(&ty), Kinding::Natural),
        Some("{({label: mkey-a, rank: 21}, 1): 210}".to_string())
    );
    assert_eq!(
        node_refusal(&golden, Some(&ty)),
        None,
        "the golden's own rendering must round-trip, or the node is refused"
    );
}

/// A golden key that is NOT the declared type's `toJSONString` spelling renders as
/// nothing, and that is deliberately NOT a refusal: the golden's key contradicting
/// the DDL is a divergence for the comparison to report, not a limit of the flat
/// format. This is the MEASURED multicell `m_tuple_udt` shape — `getString`'s
/// colon-joined cell path.
#[test]
fn a_getstring_spelled_golden_key_renders_as_nothing_and_is_not_refused() {
    let ty = ty_of("frozen<map<frozen<tuple<frozen<key_part>, int>>, int>>");
    let golden = json!({"charlie\\:3:8": 80});
    assert_eq!(entry_key_rendering(&ty, "charlie\\:3:8"), None);
    assert_eq!(golden_rendering(&golden, Some(&ty), Kinding::Natural), None);
    assert_eq!(node_refusal(&golden, Some(&ty)), None);
}

/// A KEY THE TWO SIDES SPELL DIFFERENTLY STILL FINDS ITS GUIDE, and the guide is
/// chosen by asking each CANDIDATE whether the CSV text READ UNDER IT denotes it
/// (roborev job 11, issue #3726).
///
/// `entry_key_rendering` translates only the spellings this lane knows (`blob`, via
/// `stringified_csv_text`) and deliberately leaves `timestamp` alone, so the golden
/// renders `2024-01-01T00:00:00Z` where the CSV cell carries
/// `2024-01-01 00:00:00+0000` and the TEXT lookup finds nothing.
///
/// BOTH DIRECTIONS ARE HERE ON PURPOSE, because testing only the first is what made an
/// earlier round of this work conclude — wrongly — that the missing guide was harmless:
///
///   * golden slot `null`, CSV token `null` — harmless. `decode_shape`'s null-token arm
///     is `Value::Null if text == "null"`, so an ABSENT guide (which is `Value::Null`)
///     resolves the token correctly by coincidence.
///   * golden slot the TEXT `"null"`, CSV token `null` — NOT harmless, and it is the
///     same bytes on the CSV side, because CSV is unquoted. With no guide the token
///     reads as `Null` where the golden says `Text("null")`, so CORRECT egress is
///     reported as a divergence.
///
/// Note what the second case rules out: canonicalizing the CSV text ON ITS OWN and
/// matching that against the golden keys cannot work, because reading the text needs
/// the guide being chosen. Hence the per-candidate question.
#[test]
fn a_key_spelled_differently_by_the_two_sides_still_finds_its_guide() {
    let ty = ty_of("frozen<map<frozen<tuple<timestamp, text>>, int>>");
    let csv = "{(2024-01-01 00:00:00+0000, null): 7}";

    // The premise both cases share: the golden's rendering and the CSV text differ, so
    // the text lookup finds nothing and the fallback is what is under test.
    assert_eq!(
        entry_key_rendering(&ty, "[\"2024-01-01T00:00:00Z\", null]").as_deref(),
        Some("(2024-01-01T00:00:00Z, null)"),
        "premise: the golden renders with the T separator"
    );

    for (slot, expected) in [
        (json!(null), json!(["2024-01-01 00:00:00+0000", null])),
        (json!("null"), json!(["2024-01-01 00:00:00+0000", "null"])),
    ] {
        let key = format!("[\"2024-01-01T00:00:00Z\", {slot}]");
        let golden = json!({ key.clone(): 7 });
        let decoded = match decode(&golden, csv, &ty) {
            Ok(decoded) => decoded,
            Err(why) => panic!("golden slot {slot}: the CSV cell must decode: {why}"),
        };
        assert_eq!(
            decoded[0]["key"], expected,
            "golden slot {slot}: the guide must be found, so the CSV token `null` reads \
             as whatever the golden says it is"
        );
    }
}

/// A MULTICELL container-keyed map's VALUES get their guide POSITIONALLY, so a legitimate
/// text value spelled `null` is not read as an actual null (roborev job 36, issue #3726).
///
/// Such a map resolves NO `golden_key` by text: the golden's object key is `getString`'s
/// cell-path text, which is not the declared type's `toJSONString` document, so it renders to
/// nothing and matches no entry. Every value was therefore decoded against `Value::Null` —
/// and that is not inert, because `decode_shape` reads the token `null` as `Value::Null`
/// exactly when the guide is null. A text value spelled `null` came back as a real null and
/// was reported as a divergence it is not.
///
/// The fallback is the i-th golden entry, which is not a guess: emitted order IS
/// `compare::map::compare_map`'s pairing rule and both sides preserve it.
#[test]
fn a_multicell_container_keyed_maps_values_are_guided_positionally() {
    let ty = ty_of("frozen<map<frozen<tuple<timestamp, text>>, text>>");
    // Golden keys are getString cell-path text — they render to nothing, so no key matches.
    // The VALUES are the text "null" and an ordinary word.
    let golden = json!({"a\\:1": "null", "b\\:2": "word"});
    let csv = "{(a, 1): null, (b, 2): word}";
    let decoded = match decode(&golden, csv, &ty) {
        Ok(decoded) => decoded,
        Err(why) => panic!("the cell must decode: {why}"),
    };
    assert_eq!(
        decoded[0]["value"],
        json!("null"),
        "the golden says this value is the TEXT `null`, so the CSV token must not become a \
         real null: {decoded}"
    );
    assert_eq!(decoded[1]["value"], json!("word"), "{decoded}");
}

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
/// [`two_container_keys_that_render_alike_are_refused`] below, which is the reason
/// the refusal exists at all — so what the decoder leaves at a key node is its
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
}

/// TWO DISTINCT CONTAINER KEYS THAT RENDER ALIKE make the node unrecoverable, so it
/// is REFUSED rather than decoded against the wrong guide (roborev finding, #3726).
///
/// CSV is unquoted, so a `key_part` whose `label` is NULL and one whose `label` is
/// the TEXT `"null"` both render `{label: null, rank: 1}`. The decoder looks a CSV
/// entry's key text up among the golden's rendered keys, so without this refusal both
/// entries would resolve to the FIRST golden key: the second is then decoded against
/// the wrong type guide and CORRECT egress is reported as a divergence — a false
/// divergence, which this lane treats as a defect in its own right (#1491 finding T1).
///
/// Refusing is the fail-closed answer AND the precondition that makes the decoder's
/// lookup single-valued. It is the EMPTY-CONTAINER refusal's sibling: an OBSERVED
/// ambiguity between two keys actually present in this golden, not the general
/// "could another value have rendered these bytes", which the module doc declines.
#[test]
fn two_container_keys_that_render_alike_are_refused() {
    let ty = ty_of("frozen<map<frozen<key_part>, int>>");
    let golden = json!({
        "{\"label\": null, \"rank\": 1}": 10,
        "{\"label\": \"null\", \"rank\": 1}": 20
    });
    // Both keys DO render — individually they are perfectly legal spellings.
    assert_eq!(
        entry_key_rendering(&ty, "{\"label\": null, \"rank\": 1}"),
        entry_key_rendering(&ty, "{\"label\": \"null\", \"rank\": 1}"),
        "the premise of this test: the two distinct keys render identically"
    );
    let why = match node_refusal(&golden, Some(&ty)) {
        Some(why) => why,
        None => panic!("a node whose two keys render alike cannot be decoded"),
    };
    assert!(
        why.contains("SAME key text"),
        "the refusal must name the collision: {why}"
    );

    // And the control: two keys that render DIFFERENTLY are not refused, so this
    // narrows rather than refusing every container-keyed map.
    let distinct = json!({
        "{\"label\": \"a\", \"rank\": 1}": 10,
        "{\"label\": \"b\", \"rank\": 1}": 20
    });
    assert_eq!(node_refusal(&distinct, Some(&ty)), None);
}

/// A UDT entry's key is a FIELD NAME, not a value, and stays verbatim — the one
/// thing that must NOT change with the map rule above.
#[test]
fn a_udt_entry_key_is_still_a_verbatim_field_name() {
    let ty = ty_of("frozen<person>");
    assert_eq!(
        entry_key_rendering(&ty, "first_name"),
        Some("first_name".to_string())
    );
    let golden = json!({"first_name": "ada", "last_name": "l", "age": 36});
    let decoded = decode(&golden, "{first_name: ada, last_name: l, age: 36}", &ty)
        .expect("inverts the grammar");
    assert_eq!(
        decoded,
        json!([
            {"key": "first_name", "value": "ada"},
            {"key": "last_name", "value": "l"},
            {"key": "age", "value": "36"}
        ])
    );
}

#[path = "golden_csv_container_spelling_tests.rs"]
mod spelling;
