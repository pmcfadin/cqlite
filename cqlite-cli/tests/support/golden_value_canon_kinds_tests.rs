//! Direct unit coverage for [`super::canon_matches_declared_kinds`] — the LEAF
//! question the gap machinery asks of a canonical value (issue #3846).
//!
//! # Why a suite of its own
//!
//! That function had no direct test at all. It was reached only through
//! `compare::gap::Divergence::NestedFrozenValueLeftUndecodedByGolden` — one call
//! site, one declared type family — so only its `Numeric` and `Boolean` arms were
//! ever exercised, and then only indirectly, while the arms added since (the
//! per-type scalar spellings of roborev job 105, the CSV boolean pair of job 72,
//! `NullHere`'s positional rule from job 60, and every container arm) were covered
//! by nothing that names them. A rule reached only as a side effect of another
//! check is a rule whose narrowing or widening is invisible: an arm can be relaxed
//! without reddening anything as long as the one caller's cases keep their verdicts.
//!
//! It is also now a SECOND caller's rule
//! (`Divergence::NestedFrozenUdtRendersAsBlobHex`, issue #3846), which makes each
//! arm's behaviour shared rather than incidental to one gap.
//!
//! # Where the expectations come from
//!
//! Every accepted spelling below is `cassandra-5.0.8`'s, read at the pin, or the
//! committed DDL's — never CQLite's current output (CLAUDE.md: a CQLite `file:line`
//! is never format authority). The individual scalar spellings are pinned as
//! predicates in `golden_value_scalar_spelling_tests.rs`; what these cases pin is
//! that THIS function consults them, per declared type, and what each container arm
//! does with its members.
//!
//! Every rule is asked from BOTH sides — a `Canon` the declared type implies, and
//! one it does not — because a one-sided case passes just as well when the arm has
//! stopped discriminating at all.

use super::super::schema::{CqlType, UdtType};
use super::super::{Canon, Egress};
use super::canon_matches_declared_kinds;

// =======================================================================
// Helpers: the declared types and the canonical values, spelled out
// =======================================================================

fn text_ty() -> CqlType {
    CqlType::Text("text".to_string())
}

fn int_ty() -> CqlType {
    CqlType::Numeric("int".to_string())
}

fn num(text: &str) -> Canon {
    Canon::Num(text.to_string())
}

fn text(s: &str) -> Canon {
    Canon::Text(s.to_string())
}

/// `matches` under BOTH egresses, for the arms whose answer does not depend on one.
/// Stated as a helper rather than repeated so a case cannot silently cover one
/// egress only.
fn matches_both(canon: &Canon, ty: &CqlType) -> (bool, bool) {
    (
        canon_matches_declared_kinds(canon, ty, Egress::Json),
        canon_matches_declared_kinds(canon, ty, Egress::Csv),
    )
}

// =======================================================================
// The entry point: a whole CELL may be null; a MEMBER may not
// =======================================================================

/// A null CELL is accepted at the entry point for every declared type, and that is
/// a positional fact rather than a relaxation: the row may simply not have written
/// the column.
#[test]
fn a_null_cell_is_accepted_at_every_declared_type() {
    for ty in [
        int_ty(),
        text_ty(),
        CqlType::Boolean,
        CqlType::Blob,
        CqlType::Timestamp,
        CqlType::Opaque("uuid".to_string()),
        CqlType::List(Box::new(int_ty())),
        CqlType::Set(Box::new(int_ty())),
        CqlType::Map(Box::new(text_ty()), Box::new(int_ty())),
        CqlType::Tuple(vec![int_ty()]),
        CqlType::Udt(UdtType {
            name: "u".to_string(),
            fields: vec![("f".to_string(), int_ty())],
        }),
    ] {
        let (json, csv) = matches_both(&Canon::Null, &ty);
        assert!(
            json && csv,
            "a null CELL of declared type `{}` is legal in both egresses",
            ty.describe()
        );
    }
}

/// CQL does not permit a null INSIDE a collection — a `set<int>` cannot hold one,
/// and writing a null map value DELETES the entry rather than storing one — so a
/// null at a collection ELEMENT, a map KEY or a map VALUE is not a decode of that
/// type (roborev job 60). It DOES permit a null TUPLE SLOT and a null UDT FIELD:
/// `cassandra-5.0.8 UserType.toJSONString` writes `null` for a field whose buffer
/// is absent, which is why a dump of a frozen UDT always carries every declared
/// field.
#[test]
fn a_null_member_is_positional_forbidden_in_collections_legal_in_tuples_and_udts() {
    let forbidden: &[(CqlType, Canon)] = &[
        (
            CqlType::List(Box::new(int_ty())),
            Canon::Seq(vec![Canon::Null]),
        ),
        (
            CqlType::Set(Box::new(int_ty())),
            Canon::Seq(vec![Canon::Null]),
        ),
        (
            CqlType::Map(Box::new(text_ty()), Box::new(int_ty())),
            Canon::Entries(vec![(Canon::Null, num("1"))]),
        ),
        (
            CqlType::Map(Box::new(text_ty()), Box::new(int_ty())),
            Canon::Entries(vec![(text("k"), Canon::Null)]),
        ),
    ];
    for (ty, canon) in forbidden {
        let (json, csv) = matches_both(canon, ty);
        assert!(
            !json && !csv,
            "a null member of `{}` is not a decode of that type",
            ty.describe()
        );
    }

    let allowed: &[(CqlType, Canon)] = &[
        (
            CqlType::Tuple(vec![int_ty(), text_ty()]),
            Canon::Seq(vec![Canon::Null, text("x")]),
        ),
        (
            CqlType::Udt(UdtType {
                name: "u".to_string(),
                fields: vec![("a".to_string(), int_ty()), ("b".to_string(), text_ty())],
            }),
            Canon::Fields(vec![
                ("a".to_string(), Canon::Null),
                ("b".to_string(), text("x")),
            ]),
        ),
    ];
    for (ty, canon) in allowed {
        let (json, csv) = matches_both(canon, ty);
        assert!(
            json && csv,
            "a null tuple slot / UDT field IS what toJSONString writes for `{}`",
            ty.describe()
        );
    }
}

// =======================================================================
// The scalar arms, one declared type at a time
// =======================================================================

/// A NUMERIC declared type takes [`Canon::Num`] and nothing else. This is the arm
/// the one historical caller exercised, and the reason the function exists:
/// `canon_typed` canonicalizes a string at a declared `int` as `Canon::Text` ON
/// PURPOSE, so that the COMPARISON reports the inequality — which is no use at a
/// position where the other side is undecoded (roborev job 38).
#[test]
fn a_numeric_declared_type_takes_a_number_only() {
    let ty = int_ty();
    let (json, csv) = matches_both(&num("42"), &ty);
    assert!(json && csv, "a canonicalized number IS an `int` decode");
    for wrong in [text("not-an-int"), text("42"), Canon::Bool(true)] {
        let (json, csv) = matches_both(&wrong, &ty);
        assert!(
            !json && !csv,
            "{wrong:?} is not a decode of `int`: canon_typed emits `Canon::Num` there"
        );
    }
}

/// `text`/`varchar`/`ascii`: EVERY string is a well-formed value, so
/// [`Canon::Text`] is the whole question and there is deliberately nothing to
/// narrow — including a string that happens to look like another type's spelling.
/// A number is still refused: `UTF8Type.toJSONString` quotes its value with
/// `JsonUtils.quoteAsJsonString`, so a `text` column is a JSON string.
#[test]
fn a_text_declared_type_takes_any_string_and_no_other_kind() {
    for name in ["text", "varchar", "ascii"] {
        let ty = CqlType::Text(name.to_string());
        for any in [text(""), text("0xnothex"), text("not-a-uuid"), text("42")] {
            let (json, csv) = matches_both(&any, &ty);
            assert!(
                json && csv,
                "every string is a well-formed `{name}` value: {any:?}"
            );
        }
        for wrong in [num("42"), Canon::Bool(true)] {
            let (json, csv) = matches_both(&wrong, &ty);
            assert!(!json && !csv, "{wrong:?} is not a `{name}` decode");
        }
    }
}

/// BOOLEAN is the one EGRESS-AWARE arm, because the projection differs:
/// `Canon::for_csv` renders a boolean as its TEXT spelling, so the text form is
/// legal under [`Egress::Csv`] and not under [`Egress::Json`] — but ONLY the two
/// spellings that projection can produce. Accepting every `Canon::Text` there let
/// `"not-a-bool"` qualify as a decoded boolean (roborev job 72). Authority for the
/// pair: `cassandra-5.0.8 BooleanSerializer.toString` returns `value.toString()`,
/// i.e. `true`/`false`, and `for_csv` uses Rust's `bool::to_string`, the same two
/// words.
#[test]
fn a_boolean_declared_type_is_egress_aware_and_takes_only_the_two_spellings() {
    let ty = CqlType::Boolean;
    for b in [true, false] {
        let (json, csv) = matches_both(&Canon::Bool(b), &ty);
        assert!(
            json && csv,
            "a `Canon::Bool` is a boolean decode in both egresses"
        );
    }
    for spelling in ["true", "false"] {
        assert!(
            canon_matches_declared_kinds(&text(spelling), &ty, Egress::Csv),
            "`{spelling}` is what `for_csv` renders a boolean as"
        );
        assert!(
            !canon_matches_declared_kinds(&text(spelling), &ty, Egress::Json),
            "the JSON egress carries the raw JSON boolean, so `\"{spelling}\"` is a \
             kind divergence there and not a decode"
        );
    }
    for wrong in [text("not-a-bool"), text("TRUE"), text("1"), num("1")] {
        let (json, csv) = matches_both(&wrong, &ty);
        assert!(
            !json && !csv,
            "{wrong:?} is not a spelling BooleanSerializer.toString or for_csv can produce"
        );
    }
}

/// The three declared types that share [`Canon::Text`] with `text` each get ONE
/// BOUNDED SPELLING CHECK, so a non-hex blob, a malformed timestamp and arbitrary
/// text at an opaque position are no longer decodes of those types (roborev job
/// 105). What is pinned here is that this function CONSULTS the per-type rule; the
/// spellings themselves are `scalar_spelling`'s, pinned in its own suite.
#[test]
fn blob_timestamp_and_opaque_declared_types_each_consult_their_own_spelling() {
    // (declared type, a spelling that type HAS, a spelling it does NOT).
    let cases: &[(CqlType, &str, &str)] = &[
        // `BytesType.toJSONString` = `"0x" + bytesToHex(buffer)`; the refused text is
        // `BytesSerializer.toString`'s BARE hex, which is the `getString` spelling.
        (CqlType::Blob, "0xdeadbeef", "deadbeef"),
        // `TimestampType.toJSONString` writes `TimestampSerializer.toString`'s UTC
        // form; `canon_timestamp` maps both legitimate spellings onto one canonical
        // form and passes anything else through verbatim, so an unrecognised spelling
        // is exactly "the canonicalizer did not see a timestamp here".
        (
            CqlType::Timestamp,
            "2025-10-06T01:12:05.394Z",
            "not-a-timestamp",
        ),
        // `UUIDSerializer.toString` = Java `UUID.toString`.
        (
            CqlType::Opaque("uuid".to_string()),
            "15291a77-d739-4e73-8397-b787442f3a1f",
            "not-a-uuid",
        ),
        (
            CqlType::Opaque("timeuuid".to_string()),
            "78f64100-a251-11f0-a18d-d6726a637a4c",
            "not-a-timeuuid",
        ),
        // `SimpleDateSerializer.toString` = `ISO_LOCAL_DATE`.
        (
            CqlType::Opaque("date".to_string()),
            "2025-06-18",
            "18/06/2025",
        ),
        // `TimeSerializer.toString` = fixed-width `HH:MM:SS.nnnnnnnnn`.
        (
            CqlType::Opaque("time".to_string()),
            "01:12:05.394017000",
            "01:12:05",
        ),
        // `Duration.toString` = `<digits><unit>` groups in the fixed order
        // `y mo d h m s ms us ns`.
        (
            CqlType::Opaque("duration".to_string()),
            "46702000000000ns",
            "12 hours",
        ),
        // `InetAddressSerializer.toString` = `getHostAddress()`, i.e. an address
        // literal and nothing else.
        (
            CqlType::Opaque("inet".to_string()),
            "154.47.65.214",
            "not-an-address",
        ),
    ];
    for (ty, spelled, misspelled) in cases {
        // The CONTROL first, in both egresses: without it the refusal could pass
        // because the arm had stopped matching this type at all.
        let (json, csv) = matches_both(&text(spelled), ty);
        assert!(
            json && csv,
            "`{spelled}` IS a spelling a `{}` value has",
            ty.describe()
        );
        let (json, csv) = matches_both(&text(misspelled), ty);
        assert!(
            !json && !csv,
            "`{misspelled}` is not a spelling a `{}` value has, so it is not a decode \
             of that type",
            ty.describe()
        );
        // And the VARIANT still has to be `Canon::Text`.
        let (json, csv) = matches_both(&num("1"), ty);
        assert!(
            !json && !csv,
            "a number is not a `{}` decode: canon_typed emits `Canon::Text` there",
            ty.describe()
        );
    }
}

/// An OPAQUE type this module has read NO authority for is REFUSED rather than
/// given a guessed rule: `opaque_spelling_matches` answers `None`, and the caller
/// must treat that as a non-match (`== Some(true)`, never `!= Some(false)`). That
/// is the fail-closed direction — the gap stops suppressing and an ordinary diff is
/// reported, which a reader can act on, where accepting arbitrary text cannot be
/// recovered from.
///
/// Constructed directly BECAUSE the schema reader cannot produce this value (an
/// unknown type name is a hard parse error there), so this pins that arm's own
/// fail-closed branch.
#[test]
fn an_opaque_type_with_no_authority_is_refused_never_given_a_guessed_rule() {
    let ty = CqlType::Opaque("vector".to_string());
    for any in [text("anything"), text(""), text("0xdeadbeef")] {
        let (json, csv) = matches_both(&any, &ty);
        assert!(
            !json && !csv,
            "an opaque type with no spelling rule must be refused: {any:?}"
        );
    }
}

// =======================================================================
// The container arms: the variant, and every member recursively
// =======================================================================

/// A LIST and a SET both require [`Canon::Seq`] and hold EVERY element to the
/// declared element type. One bad element is enough: the question is whether the
/// value is a decode of the declared type, not whether most of it is.
#[test]
fn a_list_or_set_requires_a_seq_and_checks_every_element() {
    for ty in [
        CqlType::List(Box::new(int_ty())),
        CqlType::Set(Box::new(int_ty())),
    ] {
        let (json, csv) = matches_both(&Canon::Seq(vec![num("1"), num("2")]), &ty);
        assert!(
            json && csv,
            "a seq of numbers IS a decode of `{}`",
            ty.describe()
        );
        // Empty is a decode too: an empty frozen collection is a legal value.
        let (json, csv) = matches_both(&Canon::Seq(vec![]), &ty);
        assert!(
            json && csv,
            "an empty seq is a decode of `{}`",
            ty.describe()
        );
        // ONE bad element, in each position, and a wrong VARIANT.
        for wrong in [
            Canon::Seq(vec![text("not-an-int"), num("2")]),
            Canon::Seq(vec![num("1"), text("not-an-int")]),
            Canon::Entries(vec![(num("1"), num("2"))]),
            Canon::Fields(vec![("f".to_string(), num("1"))]),
            num("1"),
            text("[1, 2]"),
        ] {
            let (json, csv) = matches_both(&wrong, &ty);
            assert!(
                !json && !csv,
                "{wrong:?} is not a decode of `{}`",
                ty.describe()
            );
        }
    }
}

/// A TUPLE requires [`Canon::Seq`] and checks each slot against its OWN declared
/// type, positionally. ARITY is deliberately not this function's question —
/// `canon_container` refuses a tuple of the wrong arity before it can get here, and
/// duplicating that rule would be a second opinion about it — so a SHORT seq's
/// declared slots are checked pairwise, which is what `zip` does.
#[test]
fn a_tuple_checks_each_slot_against_its_own_declared_type() {
    let ty = CqlType::Tuple(vec![int_ty(), text_ty()]);
    let (json, csv) = matches_both(&Canon::Seq(vec![num("1"), text("x")]), &ty);
    assert!(
        json && csv,
        "a number then a string IS a decode of that tuple"
    );
    // The slot types are NOT interchangeable: the same two members swapped are a
    // decode of neither slot.
    let (json, csv) = matches_both(&Canon::Seq(vec![text("x"), num("1")]), &ty);
    assert!(!json && !csv, "the slot order is the declared one");
    for wrong in [
        Canon::Seq(vec![text("not-an-int"), text("x")]),
        Canon::Entries(vec![(num("1"), text("x"))]),
        num("1"),
    ] {
        let (json, csv) = matches_both(&wrong, &ty);
        assert!(!json && !csv, "{wrong:?} is not a decode of that tuple");
    }
}

/// A MAP requires [`Canon::Entries`] and checks BOTH halves of every entry against
/// their own declared types.
#[test]
fn a_map_checks_both_halves_of_every_entry() {
    let ty = CqlType::Map(Box::new(text_ty()), Box::new(int_ty()));
    let (json, csv) = matches_both(
        &Canon::Entries(vec![(text("a"), num("1")), (text("b"), num("2"))]),
        &ty,
    );
    assert!(json && csv, "text keys and numeric values ARE a decode");
    let (json, csv) = matches_both(&Canon::Entries(vec![]), &ty);
    assert!(json && csv, "an empty map is a decode");
    for wrong in [
        // A KEY of the value's type, and a VALUE of the key's.
        Canon::Entries(vec![(num("1"), num("1"))]),
        Canon::Entries(vec![(text("a"), text("not-an-int"))]),
        // One bad entry among good ones.
        Canon::Entries(vec![(text("a"), num("1")), (text("b"), text("no"))]),
        Canon::Seq(vec![text("a"), num("1")]),
        text("{a: 1}"),
    ] {
        let (json, csv) = matches_both(&wrong, &ty);
        assert!(!json && !csv, "{wrong:?} is not a decode of that map");
    }
}

/// A UDT requires [`Canon::Fields`] and checks each field against the type the
/// committed `CREATE TYPE` declares FOR THAT NAME.
///
/// A name the DDL does not declare answers FALSE rather than being tolerated: an
/// undeclared name has no declared type, and a value with no declared type is never
/// compared permissively. `canon_udt` already refuses such a value — the field SET
/// and ORDER are its rules, quoting `cassandra-5.0.8 UserType.toJSONString` — so
/// this arm's own branch is unreachable through `canon_typed`; it is pinned here
/// because an unreachable-but-PERMISSIVE branch is worse than no branch (roborev
/// job 305), and this suite is where that can be asked at all.
#[test]
fn a_udt_checks_each_field_against_the_type_declared_for_that_name() {
    let ty = CqlType::Udt(UdtType {
        name: "geo".to_string(),
        fields: vec![
            ("lat".to_string(), int_ty()),
            ("tag".to_string(), text_ty()),
        ],
    });
    let good = Canon::Fields(vec![
        ("lat".to_string(), num("42")),
        ("tag".to_string(), text("x")),
    ]);
    let (json, csv) = matches_both(&good, &ty);
    assert!(json && csv, "the declared kinds per name ARE a decode");
    for wrong in [
        // The right names, the WRONG kinds — each field's own type is consulted.
        Canon::Fields(vec![
            ("lat".to_string(), text("not-an-int")),
            ("tag".to_string(), text("x")),
        ]),
        Canon::Fields(vec![
            ("lat".to_string(), num("42")),
            ("tag".to_string(), num("42")),
        ]),
        // A name the CREATE TYPE does not declare.
        Canon::Fields(vec![
            ("lat".to_string(), num("42")),
            ("elevation".to_string(), num("42")),
        ]),
        // The wrong VARIANT: a UDT is not a seq or a map, whatever its members are.
        Canon::Seq(vec![num("42"), text("x")]),
        Canon::Entries(vec![(text("lat"), num("42"))]),
    ] {
        let (json, csv) = matches_both(&wrong, &ty);
        assert!(!json && !csv, "{wrong:?} is not a decode of `geo`");
    }
}

/// The recursion carries the declared type down and the POSITIONAL null rule with
/// it, so a leaf several levels deep is held to its own declared type and a null
/// inside a nested collection is still forbidden while a nested UDT field's null is
/// still legal.
#[test]
fn the_recursion_reaches_a_nested_leaf_and_keeps_the_positional_null_rule() {
    // `list<frozen<tuple<int, frozen<map<text, blob>>>>>`
    let ty = CqlType::List(Box::new(CqlType::Tuple(vec![
        int_ty(),
        CqlType::Map(Box::new(text_ty()), Box::new(CqlType::Blob)),
    ])));
    let good = Canon::Seq(vec![Canon::Seq(vec![
        num("1"),
        Canon::Entries(vec![(text("k"), text("0xdeadbeef"))]),
    ])]);
    let (json, csv) = matches_both(&good, &ty);
    assert!(
        json && csv,
        "the nested decode is well-formed at every leaf"
    );
    // The DEEPEST leaf's own spelling rule still applies three levels down.
    let bad_leaf = Canon::Seq(vec![Canon::Seq(vec![
        num("1"),
        Canon::Entries(vec![(text("k"), text("deadbeef"))]),
    ])]);
    let (json, csv) = matches_both(&bad_leaf, &ty);
    assert!(
        !json && !csv,
        "bare hex at a nested `blob` is the getString spelling, not toJSONString's"
    );
    // A null MAP VALUE stays forbidden at depth…
    let null_value = Canon::Seq(vec![Canon::Seq(vec![
        num("1"),
        Canon::Entries(vec![(text("k"), Canon::Null)]),
    ])]);
    let (json, csv) = matches_both(&null_value, &ty);
    assert!(!json && !csv, "a null map value is not a stored entry");
    // …while a null TUPLE SLOT stays legal at the same depth.
    let null_slot = Canon::Seq(vec![Canon::Seq(vec![num("1"), Canon::Null])]);
    let (json, csv) = matches_both(&null_slot, &ty);
    assert!(json && csv, "a null tuple slot is legal at any depth");
}
