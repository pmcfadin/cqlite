//! Decoding a CSV container cell back into a comparable value (issue #1491).
//!
//! # Why a decoder and not a skip
//!
//! Acceptance criterion 2 of #1491 is "parse the CSV output back into rows and
//! compare cells to the golden", and it carves out no container exception. CSV
//! carries no types at all, so decoding ANY cell needs an external statement of
//! its shape — that is a property of the format, not a weakness of this lane.
//! Here the shape comes from TWO committed authorities: the GOLDEN
//! (`sstabledump` renders a list/set/frozen collection as a JSON array and a
//! map/UDT as a JSON object), and the committed `CREATE TABLE`/`CREATE TYPE` DDL,
//! which is what distinguishes `list<>` from `set<>` from `tuple<>` — a
//! distinction the golden's JSON array cannot express. Nothing here is derived
//! from CQLite's own output.
//!
//! # Why this is not redundant with the JSON lane — MEASURED, not argued
//!
//! The standing objection to this module is that the JSON lane already compares
//! container VALUES at full strictness, so all CSV adds is a rendering no oracle
//! governs (see the grammar section below) — on which reading the whole decoder
//! could become a declared gap and this file could be deleted. The objection is
//! sound in outline and FALSE in fact, so the measurement is recorded here rather
//! than left to be retaken:
//!
//! `test_signed_coll.signed_special_collections` declares `sd SET<DECIMAL>` and
//! `sf SET<DOUBLE>`, and BOTH are format-scoped declared gaps of the JSON lane —
//! `sf` because JSON has no literal for `Infinity`/`NaN`, `sd` because the egress
//! renders a `decimal` as a JSON string where `cassandra-5.0.8`
//! `DecimalType.toJSONString` emits an unquoted number. The table has no other
//! non-key column, so in JSON that case compares its `id` and nothing else. The
//! CSV lane is therefore the ONLY place any oracle checks how CQLite renders a
//! `set<double>` carrying `-Infinity`/`NaN` or a `set<decimal>` carrying exact
//! 30-digit unscaled text — against the `sstabledump` golden, member by member.
//!
//! Measured across the whole corpus when this was written: 46 container cells
//! value-compared under CSV against 45 under JSON, and the two lanes' sets are
//! NOT nested either way — CSV compares those 2 cells JSON declares away, JSON
//! compares 1 (`nb_empty_collections`'s `fs`) that CSV refuses. So neither lane
//! subsumes the other, and deleting this one would drop two container columns to
//! zero coverage in any lane.
//!
//! The counts will drift as the corpus grows; the STRUCTURAL fact is the durable
//! one — a format-scoped gap on a container column moves that column's only
//! coverage into the other format's lane.
//!
//! # The grammar, and what pinning it is (and is not) worth
//!
//! `cqlite_core::util::value_fmt::ValueFormatter` renders a container as
//! `[a, b]` (list), `{a, b}` (set), `{k: v}` (map and UDT) and `(a, b)` (tuple):
//! `, ` and `: ` separators, and NO quoting or escaping of members.
//!
//! That syntax is CQLite's own product decision — Cassandra has no CSV egress
//! counterpart for it — so it is deliberately NOT treated as authority for
//! anything, and this module asserts no claim that it is correct. What IS
//! asserted against an external oracle is every VALUE the syntax carries:
//! member count, member order, nesting depth, map keys, and each scalar's
//! rendering (blob hex, exact decimal digits, timestamp spelling), all compared
//! against the `sstabledump` golden by the same rules the JSON lane uses.
//!
//! The decoder is STRICT precisely so that pinning the grammar is worth
//! something. A tolerant decoder would absorb a writer regression symmetrically,
//! which is the round-trip blind spot CLAUDE.md names ("a CQLite-written +
//! CQLite-read round-trip is invariant to a uniform framing error"). So the
//! separators must be exactly `, ` and `: `, the brackets must balance, and
//! nothing is trimmed: a separator change, a bracket change, a dropped member or
//! a re-ordered one all surface as a failure rather than being normalized away.
//!
//! # The bracket is required, per DECLARED type
//!
//! The expected bracket pair comes from the column's declared CQL type, so
//! `list<>` requires `[…]`, `set<>` requires `{…}`, `tuple<>` requires `(…)` and
//! `map<>`/UDT require `{…}` (the grammar above). A set or tuple rendered with
//! list brackets is therefore a FAILURE, not an accepted spelling.
//!
//! This replaces an earlier concession that accepted `[`, `{` and `(`
//! interchangeably for any golden ARRAY, on the grounds that `sstabledump`
//! renders a list and a set alike so the golden cannot say which is which (issue
//! #1491 review finding R2). The premise was true of the golden and false of the
//! lane: the DDL is committed beside the fixtures and says exactly which kind the
//! column is.
//!
//! It leaves the JSON lane strictly less discriminating here, which is a property
//! of that format rather than a gap in this one: CQLite's JSON egress renders a
//! set as an array (measured on `test_da.collection_table`: `tags SET<TEXT>` →
//! `["alpha","beta"]`) exactly as the golden does, so in JSON there is no bracket
//! to check.
//!
//! # Refusals, declared rather than papered over — each attributed to the
//! NARROWEST node it destroys
//!
//! Some golden content cannot be recovered from the flat rendering at all. Such a
//! position is REFUSED: never guessed, always counted and named in the run census.
//!
//! What matters as much as the refusal is its BLAST RADIUS, because the
//! comparison walk is per MEMBER, per DEPTH. A refusal decided at any coarser
//! granularity suppresses positions that are perfectly decidable — the same
//! defect three times over in this lane's review history: first per LANE (CSV
//! skipped every container), then per CELL (an ambiguous golden refused the whole
//! cell, so `null` or unrelated text passed for it), then per OUTER CONTAINER (an
//! ambiguous NESTED member suppressed its unambiguous siblings and the outer
//! structure, so a golden `[[]]` of `list<frozen<list<text>>>` accepted a CLI `[]`
//! — the unambiguous outer member silently dropped, review finding P2).
//!
//! So each cause is attributed to the narrowest node whose decode it destroys,
//! and the refusal RIDES THE SAME RECURSION as the comparison: [`node_refusal`] is
//! asked at every node, by the decoder ([`decode`]) and by the comparator alike,
//! on the same golden value and the same declared type — so what one leaves the
//! other expects, and the two cannot drift. Everything above and beside a refused
//! node keeps being compared: every enclosing bracket frame, every enclosing
//! member count, and every unambiguous sibling.
//!
//! The BRACKET DEPTH is what makes that attribution sound rather than convenient:
//! [`scan`] splits a body only at DEPTH ZERO, so a `, ` (or a `: `) inside a
//! member can corrupt the split of the container that DIRECTLY holds it and of no
//! other level — every enclosing level sees that member's own brackets and never
//! looks inside them. An UNBALANCED bracket is the one cause that can reach
//! FURTHER than the container directly holding it, and it is not exempted from the
//! rule: it is asked, and answered, at EVERY node independently, so it refuses
//! exactly the levels whose split it actually breaks and no others (the section on
//! it below).
//!
//! ## The refusal question is the DECODE, not a look at the golden
//!
//! Six consecutive review rounds landed in this one decision, and five were the
//! same error in a different place: the refusal was a PREDICATE over suspicious
//! content ("this member holds a `, `", "this member holds a bracket"), while the
//! property that actually matters is whether the DECODER can read the golden
//! back. Two parallel notions of decodability, maintained by hand, that kept
//! disagreeing — in BOTH directions: a `, ` in a map VALUE was a cause nobody had
//! listed (round 10, finding Q2), and a balanced bracket and an empty member with
//! siblings were causes that should never have been listed (round 11, findings R1
//! and R2). Over-refusal is not the safe direction: a refused node keeps only the
//! bound below, so refusing a decidable position makes it UNCHECKED.
//!
//! So there is now ONE derived cause, asked through the decode's own code
//! ([`members`], [`entry_cut`], [`scan`]): run the splitter on the golden's OWN
//! structural rendering ([`golden_rendering`], derived from the golden, the
//! committed DDL and the position's [`Kinding`] — never from CQLite's output,
//! which would be circular) and refuse
//! exactly when it does not give this node's members back
//! ([`decode_does_not_recover`]).
//!
//! That is the property the comparison needs, and it is one-directional on
//! purpose. The comparison asserts `decode(cli) == golden`, so a CORRECT CLI — one
//! that renders what the golden means — passes exactly when
//! `decode(render(golden)) == golden`. Where that fails, a correct CLI would be
//! reported as a divergence it did not cause, which is the failure mode this lane
//! cares about most ("a lane that reds on correct input is the lane agents learn
//! to waive", CLAUDE.md).
//!
//! It is deliberately NOT the stronger question "could a DIFFERENT value have
//! rendered these bytes". That question is unusable here, and refusing on it would
//! delete the lane: members are unquoted, so a `list<text>` holding `["a", "b"]`
//! and one holding `["a, b"]` render the SAME bytes, and every multi-member text
//! collection in the corpus would be refused. The inherent limit of an unquoted
//! format is therefore ACCEPTED and stated rather than half-defended: a CLI that
//! emits a different value whose rendering collides with the golden's passes. What
//! the lane guarantees is the other direction — a rendering that decodes to
//! anything but the golden FAILS.
//!
//! What the derived cause covers, each shape once (the unit cases pin all of them):
//!
//! * a `, ` inside a scalar member splits it in two, so the members come back
//!   different — REFUSED;
//! * a `, ` in a map/UDT KEY or scalar VALUE splits one entry into two, so the
//!   entries come back different — REFUSED. That is finding Q2's direction: only
//!   the KEYS used to be scanned, so CORRECT output for a golden `{"k": "a, b"}` —
//!   the rendering `{k: a, b}` — was reported `unparseable`;
//! * a `: ` in a KEY moves the key/value cut, so the key comes back different —
//!   REFUSED, while a `: ` in a VALUE is cut correctly and is COMPARED;
//! * a SOLE empty scalar member renders as an empty body, which splits into ZERO
//!   members — REFUSED;
//! * an empty member WITH SIBLINGS is recovered exactly (`["", "x"]` renders
//!   `[, x]`, whose depth-zero `, ` is the separator) and is COMPARED (finding R2);
//! * a BALANCED bracket pair inside a member does not move the depth-zero split,
//!   so it is recovered and COMPARED (finding R1) — `[[ok]]` splits into the one
//!   member `[ok]`, and so does `[[a, b]]`, because that `, ` sits at depth 1.
//!
//! ## The one cause that is NOT a decode question: EMPTY-CONTAINER
//!
//! An EMPTY golden container's rendering DOES decode back to it, so recovery says
//! nothing about it — and yet `{}` is byte for byte how a `set<text>` holding
//! exactly the empty string renders. The two are different values, so the node is
//! REFUSED, bounded by the DECLARED element type: a `list<int>` member always
//! carries a digit, so `[]` there can only mean zero members and IS compared.
//!
//! That bound is `text`/`varchar`/`ascii` and NOTHING else, established from
//! `ValueFormatter`'s own branch per type by [`member_can_render_empty`] — an empty
//! `blob` renders `0x`, an `inet` or a `uuid` always carries characters, and a
//! zeroed `duration` renders `0ns`. It used to be a deny-list that answered "can
//! render empty" for every type nobody had checked, which refused empty collections
//! of all of those and dropped them from the coverage counts (review round 19,
//! finding Y2). Over-refusal costs coverage: a refused node keeps only the
//! emptiness bound below.
//!
//! It is an instance of the inherent collision above, kept as its own rule because
//! it is the instance that attacks the member COUNT — the one property the bound
//! below then re-establishes. It is also the mirror of the SOLE empty member:
//! without it the golden side of an empty container looked recoverable while the
//! CLI could perfectly well have held one empty member.
//!
//! ## An UNBALANCED bracket is NOT a whole-cell cause (review round 12)
//!
//! A bracket that does not balance inside a member's text defeats the depth
//! counter, so a level whose body it sits in cannot be split at all — and unlike a
//! stray `, `, it can reach levels ABOVE that one too. That used to be stated as a
//! whole-CELL refusal, scanned per SCALAR (a `cell_refusal` predicate, since
//! deleted): any individually-unbalanced scalar refused the entire cell.
//!
//! It was over-refusal, i.e. a blind spot (finding S1), because BALANCE IS A
//! PROPERTY OF THE CONCATENATED RENDERING AND NOT OF EACH SCALAR IN ISOLATION. An
//! inner `list<text>` holding `"["` and `"]"` renders `[[, ]]`: the two members'
//! brackets balance each other BEFORE the enclosing boundary, so every enclosing
//! level's depth-zero split is intact and only the inner node is undecodable. With
//! the whole cell refused, the outer member COUNT and every unambiguous outer
//! SIBLING kept nothing but the emptiness bound.
//!
//! So there is no whole-cell tier left. An imbalance is simply one way the ONE
//! derived question fails — [`members`] cannot split the golden's own rendering at
//! that node — and it is asked at every node on that node's own complete
//! rendering, which is why it now refuses the levels it really does break (each
//! independently) instead of all of them. Two consequences worth stating, both of
//! which the unit cases pin:
//!
//! * a node whose OWN rendering is unbalanced is refused, at every enclosing level
//!   whose body the imbalance also reaches, and at no other;
//! * the decoder never has to split a text a CORRECT CLI would render
//!   unbalanced, because such a node was refused before the split: at a
//!   NON-refused node the golden's rendering both scanned and gave the node's
//!   members back, and a correct CLI's text carries the same brackets and
//!   separators — the scalar-SPELLING residual [`golden_rendering`] declares can
//!   move neither, no CQL scalar spelling carrying a bracket. That is what makes
//!   an imbalance a REFUSAL (a declared gap) rather than an "unbalanced bracket"
//!   DIVERGENCE blamed on a CLI that did nothing wrong.
//!
//! ## Not a refusal: NULL-TOKEN, `null` vs the text `"null"`
//!
//! A container has no empty-field mechanism, so `ValueFormatter` spells a null
//! member `null` — the same text a `text` member holding `"null"` produces (issue
//! #1499's ambiguity, one level in). The token is resolved from the GOLDEN's own
//! type: null there decodes to null here, anything else stays text. That keeps the
//! distinction wherever the oracle knows it, and loses it only where CSV genuinely
//! cannot express it. A CLI that emits the wrong member still fails — only the
//! exact null/`"null"` swap is invisible.
//!
//! ## What survives a refusal, stated exactly — and the residual it leaves
//!
//! EXACTLY TWO things are still required at a refused position, and it is worth
//! saying so precisely, because an earlier wording here ("never a blind spot the
//! size of the node"; "the member counts no confusable reading can explain are
//! still compared") implied a stronger guarantee than the code has ever had —
//! review round 10, finding Q1:
//!
//!   1. the FRAME: the position carries a text rendering, framed with the bracket
//!      pair the DECLARED type requires;
//!   2. BODY EMPTINESS, in the two directions no confusable reading can reach: an
//!      EMPTY golden container must render as an empty body, and a golden of TWO
//!      OR MORE members must render as a non-empty one.
//!
//! The FRAME is required by the DECODER, at each node's own depth ([`strip`]),
//! including the cell's root node; the emptiness bound is
//! [`decidable_despite_node_refusal`] + [`body_emptiness_bound`] (finding N3).
//!
//! THE RESIDUAL, which is real and is therefore declared rather than implied
//! away: WHICH members the body holds is NOT compared at a refused node. So a
//! golden of two or more members accepts ANY non-empty body there, and a golden
//! of EXACTLY ONE member accepts ANY body at all (an empty body is the legal
//! rendering of one empty member, so not even emptiness is decidable there). At
//! such a node the blind spot IS the size of the node's CONTENT — bounded by the
//! frame and by emptiness, and by nothing else. The unit cases below assert that
//! residual directly, so a future strengthening has to come and change them.
//!
//! What keeps it from being a SILENT gap is the census: every refused position is
//! counted and NAMED by path and cause, and a refused cell is deliberately not
//! counted as compared container coverage. Every refusal is decided from the
//! GOLDEN and the committed DDL alone, so it can never be caused by the very
//! defect under test.
//!
//! ## A refusal at a MAP KEY is scoped to the KEY (issue #3815)
//!
//! Every position the COMPARISON walks is asked [`node_refusal`], because
//! `compare::compare_value_at` asks it at each node it visits. A map KEY is not
//! such a node: `compare::compare_map` PAIRS keys (it canonicalizes them) rather
//! than recursing into them. So a cause that reaches a KEY and nothing else has its
//! own channel — [`map_key_refusals`], reported as [`Reach::MapKeys`] — and
//! `compare_map` records it at the key's own node.
//!
//! It is the module's blast-radius rule applied one place it had not reached, and
//! the two causes it covers had OPPOSITE symptoms, which is why both are named here:
//!
//! * **two keys that render ALIKE** used to refuse the whole map NODE. That was
//!   FAIL-CLOSED — it produced no wrong verdict — but the node kept only its frame
//!   and its body's emptiness, so the entry COUNT, the pair SHAPE and every VALUE
//!   went uncompared; measured, a value corrupted 20 -> 999 inside such a cell was
//!   invisible. Exactly the defect roborev job 28 found in `compare_map`'s multicell
//!   path, and the same answer works: the entries pair POSITIONALLY — the
//!   key-comparator order both sides see, though see the residual below on what that
//!   pairing does NOT check — with only the ambiguous KEYS suppressed;
//! * **a `, ` inside a scalar member of a key** refused NOTHING. A `list<text>` key
//!   holding `["a, b"]` renders `[a, b]`, whose separator sits at bracket depth 1, so
//!   the map node's entry split and its [`entry_cut`] both survive and the node
//!   recovers. The KEY did not: [`decode`] left raw text there and `compare_map` then
//!   failed to canonicalize a string as a container and propagated THAT as a diff —
//!   CORRECT egress reported as a divergence, with `Report::ambiguous_container_cells`
//!   staying 0 so nothing in the census marked it. A wrong verdict, and a silent one.
//!
//! THE RESIDUAL THAT REMAINS, narrowed to what is still true: a cause inside a key
//! is attributed to the WHOLE key rather than to the node within it, because
//! `compare_map` canonicalizes a key as one value and never walks its members —
//! there is no per-member position there to report. At a refused key exactly what
//! survives at any other refused node survives: the bracket FRAME (required by the
//! decoder's [`strip`], whose error is propagated) and the body's EMPTINESS. A THIRD
//! item joins those two when EVERY key of a node is refused: the entries' emitted
//! ORDER is then not compared either, because nothing distinguishes entry i from
//! entry j once no key is. Positional pairing is the right PAIRING — it is the
//! key-comparator order both sides see — but it is not a CHECK. Measured and
//! asserted by
//! `compare::map::tests::the_emitted_order_of_wholly_refused_keys_is_a_declared_residual`;
//! not new, since a wholly-ambiguous node was refused WHOLE before and its order was
//! invisible then too, behind a coarser census entry. And a
//! key is never RESOLVED, which is the point of refusing rather than guessing: two
//! keys sharing one spelling make either reading self-consistent, so picking one
//! reports correct egress as a divergence for whichever entry guessed wrong (#1491
//! finding T1).
//!
//! A THIRD RESIDUAL, and the one this issue's round 3 chose deliberately: where the
//! golden's own rendering CANNOT BE SYNTHESIZED — some key or member is not a spelling
//! the declared type has — a key-scoped cause found there is reported at
//! [`Reach::Body`] rather than [`Reach::MapKeys`], because the three body checks that
//! decide whether the entries may be split cannot run without that rendering. So such
//! a node loses its positional entry/value pairing: its entry COUNT and values go
//! uncompared, and only the frame and the body's emptiness survive.
//!
//! That is OVER-REFUSAL, which this module treats as a real cost rather than as
//! conservatism — and it is chosen over the alternative, which is telling the decoder
//! it may split a node whose splittability was never checked. The concrete cost of
//! that false promise: a node with one unrenderable key, two colliding sibling keys
//! and a text VALUE carrying a depth-0 `, ` would be split, OVER-split, and reported
//! as a divergence the CLI did not cause. `origin/main` has neither the pairing nor
//! the promise here (it refuses nothing, splits, and over-splits on that same input),
//! so this is fail-CLOSED against main and not a regression from it.
//! [`unsynthesizable_rendering`] owns the reasoning; recovering the pairing needs a
//! way to ask a body question with no rendering to ask it of, which is real new
//! analysis and is deferred.
//!
//! NEITHER CAUSE IS REACHED BY THE CORPUS, so both are pinned by unit cases and by
//! `compare::map::tests` rather than by a fixture. MEASURED on the corpus when this
//! was written: no container map key anywhere in it carries a `, `, a `: ` or a
//! bracket inside a member, and none collide — `test_nested_udt_keys.nested_udt_keys`'s
//! keys are `key_part` UDTs whose `label`s are plain identifiers, an empty string or
//! null. The CSV census is unchanged by this change (66 container cells compared, 1
//! refused), which is what says the scoping cost the corpus no coverage.

use super::schema::CqlType;
use super::{
    canon_typed, container, stringified_blob_spelling, Canon, Depth, Egress, Kinding, Side,
};
use serde_json::{Map, Value};

/// The ONE bracket pair a container of this declared type may be rendered with
/// (the grammar in the module doc), or `None` for a scalar type.
///
/// Taken from the DDL, so each kind is required to use its own bracket: a `set`
/// rendered `[a, b]` or a `tuple` rendered `[a, b]` is a failure (review finding
/// R2), where the earlier golden-shape-only rule accepted any of the three.
/// This type's declared map KEY type, or `None` when it is not a map.
fn map_key_ty(ty: &CqlType) -> Option<&CqlType> {
    match ty {
        CqlType::Map(key_ty, _) => Some(key_ty),
        _ => None,
    }
}

/// A GOLDEN map key as the canonical value it denotes, or `None` when it does not
/// denote one under the declared key type.
///
/// Goes through the lane's ONE answer to "what does the golden's map key denote"
/// (`container::golden_map_key_value`) and the ONE canonicalizer, so the guide lookup
/// pairs keys by exactly the equality `compare::compare_map` will use on them — which
/// is the whole point of matching this way rather than on text.
fn canonical_golden_key(key: &str, key_ty: &CqlType) -> Option<(Canon, Value)> {
    let value =
        container::golden_map_key_value(key, key_ty, container::MapKeySpelling::ToJsonString)
            .ok()?;
    let canon = canon_typed(
        &value,
        Egress::Csv,
        key_ty,
        Depth::Inside,
        container::golden_map_key_kinding(key_ty, container::MapKeySpelling::ToJsonString),
        Side::Golden,
    )
    .ok()?;
    Some((canon, value))
}

/// A CSV entry's key TEXT as the canonical value it denotes WHEN READ UNDER `guide`.
///
/// Used only to CHOOSE the guide, by asking of each candidate "does this text, read
/// under you, denote you?" — never to produce the value, which is decoded again by the
/// caller once a candidate is selected. It must take a guide because reading the text
/// depends on one: with `Null` the token `null` reads as `Null`, so a golden slot
/// holding the TEXT `"null"` would never match its own entry.
fn canonical_cli_key(
    text: &str,
    key_ty: &CqlType,
    guide: &Value,
    excluded: &Excluded<'_>,
) -> Option<Canon> {
    let decoded = decode_at(guide, text, key_ty, "", excluded, Kinding::Natural).ok()?;
    canon_typed(
        &decoded,
        Egress::Csv,
        key_ty,
        Depth::Inside,
        Kinding::Natural,
        Side::Cli,
    )
    .ok()
}

fn brackets(ty: &CqlType) -> Option<(char, char)> {
    match ty {
        CqlType::List(_) => Some(('[', ']')),
        CqlType::Set(_) => Some(('{', '}')),
        CqlType::Tuple(_) => Some(('(', ')')),
        CqlType::Map(..) | CqlType::Udt(_) => Some(('{', '}')),
        _ => None,
    }
}

/// The ONE spelling a container of this declared type has when it is EMPTY: its
/// own bracket pair with nothing between them (the grammar in the module doc).
/// `None` for a scalar type, which has no bracket frame.
///
/// Derived from the DDL through [`brackets`], so an empty `list` is `[]` and an
/// empty `set`/`map` is `{}` — never "whichever frame the egress happened to
/// use". A declared gap about an empty container asks for exactly this text (see
/// `super::compare::gap::Divergence::AbsentMulticellRendersEmpty`).
pub fn empty_rendering(ty: &CqlType) -> Option<String> {
    brackets(ty).map(|(open, close)| format!("{open}{close}"))
}

/// Is the golden AT THIS NODE unrecoverable from the flat rendering? `Some(reason)`
/// means something at this node is refused — see [`node_refusal_reach`] for HOW FAR,
/// which is [`Reach::Body`] (the node's contents and count, and nothing else — review
/// finding P2) for every cause but one: a MAP node's KEY-scoped cause reaches only
/// its keys (issue #3815).
///
/// This is the REASON-ONLY form, for the callers that need to know THAT a node is
/// refused and not how far it reaches — the gap composition in
/// `compare::compare_value_at` (a gap whose subject could not be measured is not a
/// measured gap, whatever the reach) and [`subtree_refusal`].
///
/// NON-RECURSIVE by construction: every cause it reports is a property of this
/// node's own body, so a nested position's refusal is reported when the walk
/// reaches THAT node and cannot suppress this one's siblings, count or frame. Both
/// the decoder ([`decode`]) and the comparator ask this at every node, which is
/// what keeps "what the decoder left" and "what the comparator expects" the same
/// question.
///
/// `ty` is the declared type of THIS position; `None` means the declared type does
/// not describe this shape (the comparison reports that), and no type-dependent
/// rule may then fire — refusing there would suppress a real divergence rather
/// than declare a format limit.
///
/// # Two causes, and why only one of them is a hand-written predicate
///
/// [`decode_does_not_recover`] is the general one, and it is not a predicate over
/// suspicious characters at all: it RUNS the decoder's own splitter on the
/// golden's own rendering and asks whether it gives this node's members back.
/// That is the property the comparison needs — the comparison asserts
/// `decode(cli) == golden`, so a correct CLI passes exactly when
/// `decode(render(golden)) == golden` — and deriving it from the splitter is what
/// stops "the golden looks unsafe" and "the decoder cannot read it back" drifting
/// apart, which they had done in every round of this lane's review history.
///
/// EMPTY-CONTAINER is the ONE cause that survives as its own rule, because it is
/// a different question: an empty golden container's rendering DOES decode back to
/// it, and what the rendering cannot do is tell it apart from a container of one
/// member that renders empty. The DDL is consulted for that one question only —
/// whether a member of the declared element type can render as the empty string.
pub fn node_refusal(golden: &Value, ty: Option<&CqlType>, kinding: Kinding) -> Option<String> {
    node_refusal_reach(golden, ty, kinding).map(|(_, why)| why)
}

/// HOW FAR a refusal at one node reaches — its BLAST RADIUS, which the module doc
/// argues matters as much as the refusal itself (issue #3815).
///
/// Refusing at any coarser granularity than the cause destroys makes decidable
/// positions UNCHECKED, which is the same defect this lane's review history already
/// records four times over (per lane, per cell, per outer container, and now per
/// map NODE for a cause that reaches only its KEYS).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// This node's whole BODY: which members it holds, and how many. Only the
    /// bracket FRAME and the body's EMPTINESS survive
    /// ([`decidable_despite_node_refusal`]).
    Body,
    /// A MAP node's entry KEYS, and nothing else: the node's BODY is not refused, so
    /// the entries pair POSITIONALLY, the entry COUNT and every VALUE keep being
    /// compared, and only the ambiguous keys are suppressed. [`map_key_refusals`]
    /// says WHICH.
    ///
    /// The cause sits at bracket depth >= 1 inside a key, or is an ambiguity BETWEEN
    /// two keys — either way it cannot move this node's own separators.
    ///
    /// WHAT IT DOES AND DOES NOT ASSERT, because the difference cost a review round:
    /// the body is NOT REFUSED, which is weaker than "the body's recoverability was
    /// VERIFIED". Where every key renders, the three body checks did run and decline.
    /// Where one key does NOT, they could not be evaluated — and that state was
    /// already `None`, under which the decoder splits the node anyway. So `MapKeys`
    /// grants the decoder exactly the licence `None` does and no more; the only
    /// difference is that `decode_object` suppresses the ambiguous KEYS instead of
    /// resolving one by guess (#3815 round 2).
    MapKeys,
}

/// [`node_refusal`], with the [`Reach`] of the cause it found.
///
/// The two are ONE function on purpose: a second predicate answering "how far does
/// this reach" by inspecting the golden again would be a second notion of
/// decodability maintained by hand, which is the drift this module's history is
/// made of (six review rounds, five of them the same error).
pub fn node_refusal_reach(
    golden: &Value,
    ty: Option<&CqlType>,
    kinding: Kinding,
) -> Option<(Reach, String)> {
    // EMPTY-CONTAINER: zero members and one EMPTY member render identically.
    // Keyed on the AFFIRMATIVE answer — this element type really can render
    // empty — so an unknown or non-collection type does not refuse.
    if let Value::Array(items) = golden {
        if items.is_empty() && empty_container_is_ambiguous(ty) {
            return Some((
                Reach::Body,
                "an empty container is indistinguishable from a container of one empty \
                 member of this element type"
                    .into(),
            ));
        }
    }
    decode_does_not_recover(golden, ty, kinding)
}

/// The refusal at EACH of a map node's entry KEYS, in the golden's emitted order —
/// `None` at an index whose key IS recoverable from the flat rendering (#3815).
///
/// This is the [`Reach::MapKeys`] half, and it exists because `compare::compare_map`
/// PAIRS keys (it canonicalizes them) rather than recursing into them, so there was
/// nowhere in that path to record a refusal and the whole MAP node carried the
/// cause instead. What that cost, measured: a duplicate key rendering refused the
/// node, [`decode`] returned the un-split body, and the entry COUNT, the pair SHAPE
/// and every VALUE went uncompared — a value corrupted 20 -> 999 was invisible.
///
/// # The two causes
///
/// * **BETWEEN two keys** — they render to the SAME text. Neither can select a
///   decode guide, so both are suppressed; the entries beside them still pair by
///   emitted order. This is the [`decode_does_not_recover`] cause #3726 added, moved
///   here from the node so it stops suppressing the node's body. It is the
///   EMPTY-CONTAINER refusal's sibling and is bounded the same way: the OBSERVED
///   question "do two keys PRESENT IN THIS GOLDEN collide", never the general "could
///   another value have rendered these bytes", which the module doc declines.
/// * **INSIDE one key** — the key's own value tree is not recoverable, i.e.
///   [`subtree_refusal`] fires somewhere in it. A `list<text>` key holding `["a, b"]`
///   renders `[a, b]`, whose `, ` sits at bracket depth 1: the map node's entry split
///   and its [`entry_cut`] both survive, so the node is NOT refused, and yet the key
///   decodes into two members where the golden has one. Before this was recorded,
///   [`decode`] left raw text at that key and `compare_map` then canonicalized it as a
///   container, FAILED, and propagated the failure as a DIFF — CORRECT egress reported
///   as a divergence, with `ambiguous_container_cells` staying 0 so nothing in the
///   census marked it (issue #3815, finding 2).
///
/// # NOT NARROWED further than the KEY, and why that is a stated residual
///
/// A cause INSIDE a key is attributed to the whole key rather than to the node
/// within it, because `compare_map` canonicalizes a key as ONE value and never walks
/// its members: there is no per-member position there to report. So a key one of
/// whose deep members is ambiguous is suppressed entirely. The narrowing stops where
/// the comparison's own granularity does — which is the same rule the rest of this
/// module follows, applied to a walk that happens to be one node deep.
///
/// # EVERY answer is PER KEY, and the one that is not a refusal is `None` AT ITS OWN
/// INDEX (issue #3815 round 2)
///
/// A key that is not a spelling of the declared type at all — the MULTICELL shape,
/// whose `getString` key the declared
/// `MulticellMapKeyUndecodedByGoldenRendersAsBlobHex` gap covers — is left
/// UNREFUSED, because a golden key contradicting the DDL is a divergence to report
/// and never a format limit (exactly as [`node_refusal`]'s `ty` note says). It is
/// `None` at ITS index and says NOTHING about its siblings.
///
/// THE ABSTENTION IS PER-KEY BY INTENT; THE DEFECT WAS APPLYING IT TO THE WHOLE MAP.
/// The intent is `origin/main`'s own, stated there verbatim — "the 'a key that does
/// not render' case is decided in one place (here, by returning `None`: no refusal,
/// because the golden's key is not a spelling of this declared type at all)". The
/// spelling was a whole-map `collect::<Option<Vec<_>>>()`, so ONE unrenderable key
/// cost every OTHER key of the same map its key-scoped refusal.
///
/// What that costs: a MIXED node — one `getString` key beside two keys that render
/// ALIKE — gets no refusal at ANY reach, the colliding pair is canonicalized and
/// paired, and the decoder resolves both to the first of them. That is #1491 finding
/// T1, the false divergence the refusal exists to prevent.
///
/// NOT A REGRESSION AND NOT A RESTORATION — the one place this repository states
/// that, so it cannot rot in four: main was already fail-open here (its whole-map bail
/// sits one line above the duplicate check it gates) and this issue's first round
/// carried the same bail into this new function. Fail-closed at this node is NEW
/// behaviour this change establishes. Fixed here because the function is new code this
/// change owns and the fix is local; severity Medium.
///
/// The vector is ALWAYS the golden's own length for a map, so `rendered[i]` and the
/// answer at `i` are both indexed BY GOLDEN ENTRY — never by a compacted list of the
/// renderable ones, which would misname the entries in the reason a reader is given.
/// An EMPTY vector therefore means only "not a map".
pub fn map_key_refusals(golden: &Map<String, Value>, key_ty: &CqlType) -> Vec<Option<String>> {
    // Rendered ONCE, for the same reason `decode_object` renders once: a container
    // key's rendering parses a JSON document. PER ENTRY, and kept at full length —
    // see the index note above.
    let rendered: Vec<Option<String>> = golden
        .keys()
        .map(|key| map_entry_key_rendering(key_ty, key))
        .collect();
    golden
        .keys()
        .zip(rendered.iter())
        .enumerate()
        .map(|(i, (source, text))| {
            // A key with NO rendering has no text to be ambiguous WITH and no
            // recoverable value tree to ask about, so neither cause below can be
            // decided for it: it is not refused, and the comparison reports it.
            let text = text.as_ref()?;
            // BETWEEN two keys — among the RENDERED ones only (an unrenderable key
            // has no text, so it collides with nothing), and reported with the
            // GOLDEN's entry indices.
            if let Some(other) = rendered
                .iter()
                .enumerate()
                .find(|(j, candidate)| *j != i && candidate.as_ref() == Some(text))
                .map(|(j, _)| j)
            {
                return Some(format!(
                    "entries {} and {} of the golden render the SAME key text {} — the \
                     CSV rendering cannot tell them apart, so no reading of it recovers \
                     which entry's key this is",
                    other.min(i),
                    other.max(i),
                    brief(text)
                ));
            }
            // INSIDE one key. Only a CONTAINER key has a member split of its own; a
            // scalar key's CSV text is the key itself, and a separator in THAT
            // breaks the map node's own entry split, which the node reports.
            if !container::is_container_type(key_ty) {
                return None;
            }
            let value = container::golden_map_key_value(
                source,
                key_ty,
                container::MapKeySpelling::ToJsonString,
            )
            .ok()?;
            // `Kinding::Natural`, which is what `entry_key_rendering` rendered it at
            // and what `decode_object` decodes it at: a frozen container's members
            // are cell values. Two kindings here would be two questions.
            subtree_refusal(&value, key_ty, Kinding::Natural)
                .map(|why| format!("the golden's own rendering of this key gives {why}"))
        })
        .collect()
}

/// Is ANY node of this value's own tree refused? The question a position that is
/// canonicalized AS ONE VALUE has to ask (issue #3815).
///
/// [`node_refusal`] is NON-RECURSIVE by construction — every cause it reports is a
/// property of one node's own body — which is right for a walk that visits every
/// node, and wrong for a map KEY, which `compare::compare_map` compares whole. A
/// `frozen<list<frozen<list<text>>>>` key holding `[["a, b"]]` recovers at its OUTER
/// node (that `, ` sits at depth 2) and not at its inner one, so asking only the
/// outer node would miss it and the false divergence would stand.
fn subtree_refusal(golden: &Value, ty: &CqlType, kinding: Kinding) -> Option<String> {
    if let Some(why) = node_refusal(golden, Some(ty), kinding) {
        return Some(why);
    }
    match golden {
        Value::Array(items) => items.iter().enumerate().find_map(|(i, item)| {
            let member = member_type(Some(ty), i)?;
            subtree_refusal(item, member, member_kinding(ty, kinding))
        }),
        // A nested MAP's own keys need no separate walk: a key-scoped refusal there
        // is reported by `node_refusal` AT that map's node (this function's first
        // line), because `decode_does_not_recover` asks `map_key_refusals` there.
        Value::Object(fields) => fields.iter().find_map(|(key, value)| {
            let field = field_type(Some(ty), key)?;
            subtree_refusal(value, field, Kinding::Natural)
        }),
        _ => None,
    }
}

/// Run the DECODER'S OWN splitter on the golden's own rendering: does it give THIS
/// node's members back? `Some(reason)` means it does not, so a CORRECT rendering
/// would be read as something other than the golden — and the node is refused
/// rather than reported as a divergence the CLI did not cause.
///
/// This replaces four hand-written causes (a `, ` in a member, a `, ` or a `: ` in
/// a map/UDT key, a `, ` in a map/UDT value, an empty scalar member), each of
/// which was one symptom of the same thing, and each of which had to be
/// discovered. What it asks instead is the decision itself, through the same
/// [`members`] / [`entry_cut`] / [`scan`] code the decode runs, so the refusal set
/// cannot drift from the decode:
///
/// * a `, ` inside a scalar member splits it in two, so the members come back
///   different — REFUSED, exactly as before;
/// * a `: ` in a KEY moves the key/value cut, so the key comes back different —
///   REFUSED, while a `: ` in a VALUE is cut correctly and is COMPARED;
/// * a SOLE empty scalar member renders as an empty body, which splits into zero
///   members — REFUSED;
/// * an empty member WITH SIBLINGS is recovered exactly (`["", "x"]` renders
///   `[, x]` and splits back into `""` and `"x"`), so it is COMPARED. The earlier
///   rule refused the whole node for any empty member, which cost the siblings
///   their comparison and left only the emptiness bound (review round 11, finding
///   R2);
/// * a BALANCED bracket pair in a member does not move the depth-zero split, so
///   it is recovered and COMPARED (finding R1).
///
/// # The [`Reach`] travels with the reason (issue #3815)
///
/// Every cause above is about THIS node's BODY, so each is [`Reach::Body`]. A MAP
/// node has one further class of cause, asked LAST and reported [`Reach::MapKeys`]:
/// [`map_key_refusals`], for a key that cannot be read back while the ENTRY split
/// and the emitted ORDER still can. Order matters and is not stylistic — a body
/// cause DOMINATES, because `MapKeys` promises the decoder it may split this node's
/// entries, and the three checks above are precisely the ones that decide whether it
/// may.
///
/// SO `MapKeys` IS REPORTED ONLY WHERE THOSE THREE CHECKS ACTUALLY RAN. Where the
/// golden's own rendering cannot be SYNTHESIZED they cannot run at all — they consume
/// it — and a key-scoped cause found in that state is widened to [`Reach::Body`] by
/// [`unsynthesizable_rendering`], which is the only thing this function may honestly
/// say there. Round 2 returned `MapKeys` on that route and so promised a split it had
/// never checked, contradicting this very paragraph (roborev job 445).
///
/// It is deliberately NOT the stronger question "could another value have rendered
/// the same bytes". That question is unusable here: CSV members are unquoted, so a
/// `list<text>` holding `["a", "b"]` and one holding `["a, b"]` render the SAME
/// bytes, and refusing on non-uniqueness would refuse every multi-member text
/// collection in the corpus. What one-directional recovery buys is stated
/// exactly: a CLI that renders what the golden means PASSES, and a CLI whose
/// rendering decodes to anything else FAILS. A CLI that renders a DIFFERENT value
/// which happens to share the golden's bytes passes — the module doc's inherent
/// limit of an unquoted format, of which EMPTY-CONTAINER is the one instance
/// bounded separately because it attacks the member COUNT.
fn decode_does_not_recover(
    golden: &Value,
    ty: Option<&CqlType>,
    kinding: Kinding,
) -> Option<(Reach, String)> {
    // A scalar node has no body of its own: every cause here is about the body
    // that HOLDS it, so the container one level up is what reports them.
    if is_scalar(golden) {
        return None;
    }
    // An undeclared type is not refused: see the note on `node_refusal`'s `ty`.
    let ty = ty?;
    // SITE 1 of 4 — and the only LIVE one, since `golden_rendering` recursively
    // performs the very calls the other three re-derive.
    let Some(rendering) = golden_rendering(golden, Some(ty), kinding) else {
        return unsynthesizable_rendering(golden, ty);
    };
    let parts = match members(&rendering, ty) {
        Ok(parts) => parts,
        // The splitter cannot read the golden's own rendering at all. `members`
        // fails ONLY on an UNBALANCED bracket (`scan`'s single failure mode; the
        // frame here was built from the DDL, so `strip` cannot fail), which is
        // this node's share of the cause the module doc's round-12 section
        // describes: the imbalance is reported at every node whose body it
        // reaches, each asked independently on that node's OWN complete
        // rendering, and at no other. Before round 12 it was hoisted to a
        // whole-CELL refusal scanned per scalar, so one inner member's bracket
        // suppressed every outer sibling and every member count in the cell.
        Err(why) => {
            return Some((
                Reach::Body,
                format!(
                    "the decoder cannot split the golden's own rendering {}: {why}",
                    brief(&rendering)
                ),
            ))
        }
    };
    match golden {
        Value::Array(items) => {
            // SITE 2 of 4 (see [`unsynthesizable_rendering`]).
            let Some(want) = items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    golden_rendering(item, member_type(Some(ty), i), member_kinding(ty, kinding))
                })
                .collect::<Option<Vec<String>>>()
            else {
                return unsynthesizable_rendering(golden, ty);
            };
            split_mismatch(&rendering, "member", &parts, &want).map(|why| (Reach::Body, why))
        }
        Value::Object(fields) => {
            // The keys are rendered ONCE and reused below, so the split check and
            // the key-cut check cannot ask two different questions about the same
            // key. SITE 3 of 4 (see [`unsynthesizable_rendering`]).
            let Some(keys) = fields
                .keys()
                .map(|key| entry_key_rendering(ty, key))
                .collect::<Option<Vec<String>>>()
            else {
                return unsynthesizable_rendering(golden, ty);
            };
            // SITE 4 of 4 (see [`unsynthesizable_rendering`]).
            let Some(want) = fields
                .iter()
                .zip(keys.iter())
                .map(|((key, value), rendered_key)| {
                    // The VALUE of a map entry / UDT field is a cell value, so it
                    // keeps its natural kind — `compare::compare_map` and
                    // `compare::udt::compare_udt` say the same thing.
                    golden_rendering(value, field_type(Some(ty), key), Kinding::Natural)
                        .map(|value| format!("{rendered_key}: {value}"))
                })
                .collect::<Option<Vec<String>>>()
            else {
                return unsynthesizable_rendering(golden, ty);
            };
            if let Some(why) = split_mismatch(&rendering, "entry", &parts, &want) {
                return Some((Reach::Body, why));
            }
            // The entry texts came back whole; the remaining decision
            // `decode_object` makes is the key/value cut INSIDE each, which a `: `
            // in a KEY moves. The value needs no separate check: an entry is
            // `key: value` by construction, so a recovered key leaves the golden's
            // value text as the remainder.
            //
            // `entry_cut`'s OWN error is unreachable from here for the same reason
            // as the split's above — a key whose brackets balance leaves the `: `
            // that follows it at depth zero — and is reported rather than dropped
            // on the same grounds.
            let cut = keys.iter().zip(parts.iter()).find_map(|(key, part)| {
                // Compared against the key AS RENDERED — for a map key the CSV
                // spelling of the value the golden's object key DENOTES, which
                // is the same value `compare::compare_map` canonicalizes it to.
                match entry_cut(part) {
                    Err(why) => Some(format!("the golden's own rendering: {why}")),
                    Ok((got, _)) if got != key => Some(format!(
                        "the decoder recovers key {} from the golden's own entry {}, not \
                             the golden's key {}",
                        brief(got),
                        brief(part),
                        brief(key)
                    )),
                    Ok(_) => None,
                }
            });
            if let Some(why) = cut {
                return Some((Reach::Body, why));
            }
            // THE KEY-SCOPED CAUSES, asked LAST because a BODY cause DOMINATES them
            // (issue #3815). [`Reach::MapKeys`] promises that the entry boundaries
            // and the emitted order ARE recoverable, and the three checks above are
            // exactly the ones that decide that: the entry split gave the golden's
            // entries back and `entry_cut` gave each rendered KEY back. Asking the
            // key-scoped question first would return `MapKeys` for a node whose
            // entries cannot be split at all, and the decoder would then split it.
            //
            // A UDT reaches this arm too, and has no key-scoped cause: its entry
            // keys are FIELD NAMES rather than values, so `map_key_ty` answers
            // `None` and nothing fires.
            key_scoped_refusal(fields, ty)
        }
        // Unreachable: the scalar case returned above and every other shape is a
        // container the two arms cover.
        _ => None,
    }
}

/// THE ONE ANSWER to "the golden's own rendering of this node cannot be SYNTHESIZED",
/// for the four sites in [`decode_does_not_recover`] that can discover it (#3815
/// round 3).
///
/// # Why one answer, and not a guard per site
///
/// Four sites ask the same question, because each needs a piece of the synthesized
/// rendering: the whole node's text (site 1, [`golden_rendering`]), an array's member
/// texts (2), an object's key texts (3), an object's entry texts (4). Round 2 closed
/// three of them ONE AT A TIME and gave two of them a DIFFERENT answer from the
/// third, which is how the ordering violation below got in. So the answer lives here
/// and the sites only route to it: a fifth site can be wrong about WHEN to ask, but
/// not about WHAT to say.
///
/// SITES 2, 3 and 4 ARE SUBSUMED BY SITE 1, and measured to be: `golden_rendering`
/// builds the node's text by making the very calls those three re-derive — the same
/// `entry_key_rendering` for a key, the same `golden_rendering` for a member or
/// value — so any of them failing means site 1 already returned. Mutation-confirmed:
/// replacing site 3 with `None` reds NOTHING, while doing the same to site 1 reds
/// `tests::a_mixed_key_node_does_not_fail_open`. They are kept, routed here, so the
/// four cannot disagree if `golden_rendering`'s requirements are ever narrowed.
///
/// # The answer: [`Reach::Body`], and NOT `MapKeys`
///
/// Unsynthesizability ALONE is not a refusal — a golden key or member contradicting
/// the DDL is a divergence for the comparison to report, not a limit of the flat
/// format — so `None` unless the node ALSO carries a key-scoped cause.
///
/// When it does, that cause's natural reach is [`Reach::MapKeys`], and reporting it
/// would be a FALSE PROMISE: `MapKeys` tells the decoder it may split this node's
/// entries, and the three body checks that decide whether it may CANNOT HAVE RUN
/// here — they consume the rendering that does not exist. Round 2 promised it anyway
/// (roborev job 445). The concrete cost: a node with one unrenderable key, two
/// colliding sibling keys and a text VALUE carrying a depth-0 `, ` would be split,
/// over-split, and reported as a divergence the CLI did not cause.
///
/// So the reach is WIDENED to `Body`. That is over-refusal, which this module treats
/// as a real cost and not as conservatism — see the module doc's residual — but it is
/// the only honest answer available without a way to ask a body question with no
/// rendering to ask it of. Recovering that is the deferred follow-up; it is genuinely
/// new analysis and does not belong in this change.
///
/// WHAT IS PRESERVED, which was round 2's whole purpose: the node is still REFUSED,
/// so the decoder never splits it and never resolves a colliding key to the first of
/// its spelling — #1491 finding T1 stays closed. WHAT IS LOST is this mixed node's
/// positional entry/value pairing, i.e. its entry COUNT and values. `origin/main`
/// did not have those either (it refused nothing here, split, over-split on the input
/// above and reported a false divergence), so this is fail-CLOSED against main rather
/// than a regression against it.
fn unsynthesizable_rendering(golden: &Value, ty: &CqlType) -> Option<(Reach, String)> {
    // An array has no keys, so it has no key-scoped cause.
    let Value::Object(fields) = golden else {
        return None;
    };
    // The reach is DISCARDED and replaced deliberately: `key_scoped_refusal` answers
    // `MapKeys`, which is exactly what cannot be promised here.
    let (_, why) = key_scoped_refusal(fields, ty)?;
    Some((
        Reach::Body,
        format!(
            "the golden's own rendering of this node cannot be synthesized, so no body \
             cause could be evaluated and no entry split can be promised — and this node \
             also carries a key-scoped cause: {why}"
        ),
    ))
}

/// The FIRST key-scoped refusal of an object node, as a [`Reach::MapKeys`] answer —
/// the one place [`decode_does_not_recover`]'s object arm asks [`map_key_refusals`].
///
/// Factored out because that arm reaches it by TWO routes, and a second spelling of
/// it would be a second notion of what a key-scoped refusal is: once after the body
/// causes have all been evaluated and declined, and once when they CANNOT be
/// evaluated because a key does not render (#3815 round 2, where returning `None` on
/// the second route was a fail-open).
///
/// `None` for a UDT, whose entry keys are FIELD NAMES rather than values.
fn key_scoped_refusal(fields: &Map<String, Value>, ty: &CqlType) -> Option<(Reach, String)> {
    let key_ty = map_key_ty(ty)?;
    map_key_refusals(fields, key_ty)
        .into_iter()
        .flatten()
        .next()
        .map(|why| (Reach::MapKeys, why))
}

/// The refusal reason for a split that did not give the golden's own members back,
/// or `None` when it did.
fn split_mismatch(rendering: &str, unit: &str, got: &[&str], want: &[String]) -> Option<String> {
    if got.len() != want.len() {
        return Some(format!(
            "the golden's own rendering {} splits into {} {unit}(s), not the golden's {}",
            brief(rendering),
            got.len(),
            want.len()
        ));
    }
    got.iter()
        .zip(want.iter())
        .enumerate()
        .find(|(_, (got, want))| *got != want)
        .map(|(i, (got, want))| {
            format!(
                "the golden's own rendering {} gives {unit} {i} back as {}, not {}",
                brief(rendering),
                brief(got),
                brief(want)
            )
        })
}

/// The text the documented grammar renders this golden node as, derived from the
/// GOLDEN, the committed DDL and the position's [`Kinding`]: the bracket pair the
/// declared kind requires, `, ` between members, `: ` between a key and its value,
/// and each scalar as the text the golden carries for it AT THAT POSITION.
///
/// It is NOT a second `ValueFormatter`, and it must never become one. It is asked
/// one STRUCTURAL question — where the separators and brackets of the golden's own
/// rendering fall. Deciding a scalar's spelling from CQLite's output would make
/// the refusal circular (#3042): the output under test would be choosing which of
/// its own positions get compared.
///
/// # The spelling rule, and why it is not the golden's text alone
///
/// `sstabledump` uses TWO writers, and the one it uses at a STRINGIFIED position
/// (`writeString(type.getString(v))` — a partition-key component, a multicell
/// set's element, a map key) spells a `blob` as the BARE hex, so the empty blob is
/// `""` where the CSV egress renders `0x`. Reading the golden's text verbatim
/// there synthesized `{}` for a sole empty-blob member, judged the node
/// unrecoverable and REFUSED it — and a refused ONE-member node accepts any framed
/// body at all, so the member went uncompared. [`stringified_csv_text`] performs
/// exactly that translation, and its per-type census names what it does and does
/// NOT translate.
///
/// That is a bounded CONSTANT recorded here from reading `ValueFormatter`, not a
/// call into it: nothing about the CLI's actual output reaches this decision at
/// run time, and `tests::a_stringified_blob_renders_as_the_0x_form_the_csv_egress_emits`
/// measures the constant against that formatter, so a CQLite regression that
/// dropped the prefix reds a test instead of being silently followed here.
///
/// # The RESIDUAL that remains, NARROWED to what is still true
///
/// For every position the translation does not cover, the scalar's text is the
/// GOLDEN's, and the two sides' spellings can still differ — the lane's declared
/// narrowings (a timestamp's `T`-vs-space separator, a decimal's trailing zeros, a
/// JSON integer beyond `f64`) are exactly those cases. Each is IMMATERIAL to the
/// structural question, and stated in both directions because this is the one
/// assumption the function still rests on:
///
/// * none of those spellings is ever EMPTY and none carries a `, `, a `: ` or a
///   bracket on either side, so none can move a separator or the depth count.
///   `blob` was the one that could — via emptiness — and it is translated;
/// * if CQLite were to spell some scalar WITH a `, ` where the golden does not,
///   this says "recovered" and the node is compared — and the CLI's extra
///   separator then shows up as a member-count divergence rather than as a value
///   one. A noisier diagnostic on output that is diverging anyway; never a missed
///   divergence;
/// * if the GOLDEN spelling carried a `, ` where CQLite's does not, the node would
///   be refused and that spelling difference would go uncompared. It takes a
///   non-`text` type whose `sstabledump` spelling embeds a `, `, and no CQL scalar
///   has one; `text` — the type that can — is byte-identical on both sides.
///
/// `None` means the declared type does not describe the golden's shape here. That
/// is deliberately NOT a refusal: the disagreement is a divergence the comparison
/// reports, and refusing would suppress it.
fn golden_rendering(golden: &Value, ty: Option<&CqlType>, kinding: Kinding) -> Option<String> {
    match (golden, ty) {
        (
            Value::Array(items),
            Some(seq @ (CqlType::List(_) | CqlType::Set(_) | CqlType::Tuple(_))),
        ) => {
            let (open, close) = brackets(seq)?;
            let element_kinding = member_kinding(seq, kinding);
            let body = items
                .iter()
                .enumerate()
                .map(|(i, item)| golden_rendering(item, member_type(ty, i), element_kinding))
                .collect::<Option<Vec<String>>>()?
                .join(", ");
            Some(format!("{open}{body}{close}"))
        }
        (Value::Object(fields), Some(object @ (CqlType::Map(..) | CqlType::Udt(_)))) => {
            let (open, close) = brackets(object)?;
            let body = fields
                .iter()
                .map(|(key, value)| {
                    let rendered_key = entry_key_rendering(object, key)?;
                    // A map VALUE / UDT field is a cell value: natural kind.
                    let rendered_value =
                        golden_rendering(value, field_type(ty, key), Kinding::Natural)?;
                    Some(format!("{rendered_key}: {rendered_value}"))
                })
                .collect::<Option<Vec<String>>>()?
                .join(", ");
            Some(format!("{open}{body}{close}"))
        }
        // A scalar renders as its own text whatever the DDL says of it: the golden
        // is the authority for what the VALUE is, and a golden/DDL shape
        // disagreement is reported by the comparison rather than refused here.
        (scalar, _) if is_scalar(scalar) => Some(scalar_csv_text(scalar, ty, kinding)),
        // A container golden under a declared type that is not that kind of
        // container: the shape divergence belongs to the comparison.
        _ => None,
    }
}

/// The [`Kinding`] of a member of a SEQUENCE node of declared type `seq`.
///
/// Mirrors `compare::compare_value_body`'s list/set/tuple split, which is where
/// the rule is derived from `cassandra-5.0.8 JsonTransformer` (see [`Kinding`]): a
/// multicell SET's element IS its cell PATH and is written `writeString`, while a
/// list's element, a tuple's slot and every frozen collection's member is a cell
/// VALUE written `writeRawValue`.
///
/// `seq`'s own `kinding` is passed straight through for a set, exactly as the
/// comparator passes `at.kinding` at [`super::Depth::TopLevel`]. It needs no depth
/// test of its own because a set reached by RECURSION is always given
/// [`Kinding::Natural`] here — every recursive call below hands a child either
/// `Kinding::Natural` or this function's answer for its own sequence — so only the
/// caller-supplied ROOT kinding can ever be `Stringified`, and a nested set is
/// frozen and holds cell values.
fn member_kinding(seq: &CqlType, kinding: Kinding) -> Kinding {
    match seq {
        CqlType::Set(_) => kinding,
        _ => Kinding::Natural,
    }
}

/// The text the CSV rendering carries for one object entry's KEY, or `None` when
/// the golden's key is not a spelling the declared key type has (see
/// [`golden_rendering`], whose `None` this propagates).
///
/// A UDT entry's key is a FIELD NAME — not a value — so the grammar writes it
/// verbatim. A MAP entry's key is a VALUE, and [`map_entry_key_rendering`] states
/// the two authorities that spell one.
fn entry_key_rendering(object: &CqlType, key: &str) -> Option<String> {
    match object {
        CqlType::Map(key_ty, _) => map_entry_key_rendering(key_ty, key),
        _ => Some(key.to_string()),
    }
}

/// [`entry_key_rendering`] for a MAP entry's key, taking the declared KEY type
/// directly — the form [`map_key_refusals`] needs, which is handed a key type
/// rather than the enclosing map's (issue #3815).
///
/// Two cases, and each one's authority:
///
///   * a CONTAINER key is the key value's own `toJSONString` document
///     (`cassandra-5.0.8 MapType.toJSONString` writes
///     `keys.toJSONString(kv, protocolVersion)` and quotes it only when it does not
///     already start with `"`), so it is PARSED — through the lane's one
///     `container::golden_map_key_value` — and then rendered by this module's own
///     grammar, at [`Kinding::Natural`] because a frozen container's members are
///     cell values (issue #3726). Left as the raw JSON text it would carry `, ` and
///     `: ` of its own, so [`decode_does_not_recover`] would judge every such node
///     unrecoverable and REFUSE it — which is how the CSV half of that issue stayed
///     open;
///   * a SCALAR key is spelled by the golden under [`Kinding::Stringified`], because
///     a JSON object key can only be a string. That is the same reading
///     `compare::compare_map` applies (and the reason it holds the CLI's own key to
///     [`Kinding::Natural`]), so the same translation applies here.
fn map_entry_key_rendering(key_ty: &CqlType, key: &str) -> Option<String> {
    if !container::is_container_type(key_ty) {
        return Some(stringified_csv_text(key.to_string(), key_ty));
    }
    // [`container::MapKeySpelling::ToJsonString`] because that is the question THIS
    // site asks: is the golden's key text the toJSONString document this module can
    // re-render through its own grammar? A MULTICELL map's `getString` key answers
    // no and the `None` propagates.
    //
    // WHAT THAT `None` DOES, stated exactly, because the obvious reading is wrong:
    // it does NOT refuse the node. `decode_does_not_recover` returns `None` for "no
    // refusal", so a key that does not render leaves the node UNREFUSED and the
    // divergence is reported by the comparison instead — which is deliberate and is
    // stated on `node_refusal`: a golden key contradicting the DDL is a divergence
    // to report, not a limit of the flat format. For the multicell shape that report
    // is then suppressed by the declared
    // `MulticellMapKeyUndecodedByGoldenRendersAsBlobHex` gap.
    // `a_getstring_spelled_golden_key_renders_as_nothing_and_is_not_refused` asserts
    // exactly this. [`map_key_refusals`] answers the same `None` the same way: no
    // key-scoped refusal either.
    let value =
        container::golden_map_key_value(key, key_ty, container::MapKeySpelling::ToJsonString)
            .ok()?;
    golden_rendering(&value, Some(key_ty), Kinding::Natural)
}

fn is_scalar(v: &Value) -> bool {
    !matches!(v, Value::Array(_) | Value::Object(_))
}

/// The declared type of member `i` of an array position, or `None` when the
/// declared type does not describe an array.
fn member_type(ty: Option<&CqlType>, i: usize) -> Option<&CqlType> {
    match ty {
        Some(CqlType::List(element)) | Some(CqlType::Set(element)) => Some(element),
        Some(CqlType::Tuple(items)) => items.get(i),
        _ => None,
    }
}

/// The declared type of entry `key` of an object position: a map's VALUE type, or
/// the named UDT field's type. `None` for an undeclared field or a type that does
/// not describe an object.
fn field_type<'t>(ty: Option<&'t CqlType>, key: &str) -> Option<&'t CqlType> {
    match ty {
        Some(CqlType::Map(_, value)) => Some(value),
        Some(CqlType::Udt(udt)) => udt
            .fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, t)| t),
        _ => None,
    }
}

/// What is STILL decidable at a position whose golden was refused — so a refusal
/// suppresses only the genuinely indistinguishable readings and not the whole
/// position (issue #1491 review finding N3).
///
/// Every refusal cause is about the CONTENT of a body: which members it holds, and
/// how many. None is about its FRAME. So three properties survive every cause,
/// each decided from the GOLDEN and the committed DDL exactly as the refusal
/// itself is — and these three are ALL that survive, which the module doc states
/// together with the residual they leave (finding Q1):
///
///   1. the position carries a rendering AT ALL. The golden is a container, and
///      the shortest rendering of any container is its bracket PAIR, so an empty
///      CSV field (which [`super::compare::cli_csv_rows`] reads as `null`) or a
///      non-text cell is a divergence, not an ambiguity;
///   2. it is framed with the bracket pair the DECLARED type requires. That one is
///      the DECODER'S, applied by [`strip`] at this node's own depth whether the
///      node was refused or not — the same rule as on the decodable path, where a
///      `set` rendered `[a, b]` is a failure (review finding R2) — so a frame
///      divergence is reported as an unparseable rendering rather than here;
///   3. BODY EMPTINESS, i.e. the member count in the only two directions no
///      confusable reading can reach — never WHICH members the body holds. A
///      golden container with NO members can only render as the empty bracket
///      pair: both readings EMPTY-CONTAINER confuses — zero members, and one
///      member that renders empty — are `{}` byte for byte, so ANY other body is a
///      third thing and diverges. Symmetrically a golden with TWO OR MORE members
///      cannot render as an empty body, because even all-empty members are
///      separated by `, `. (At exactly ONE member the empty body IS a legal
///      rendering — of a single empty member — so nothing is asserted there.)
///
/// What stays suppressed is exactly the indistinguishable set: WHICH members the
/// body holds, and how many when the count is 1.
///
/// # ONE entry point, at every depth (review round 12)
///
/// There used to be a second, cell-level entry point (`decidable_despite_cell_refusal`),
/// which stripped the frame itself because the whole-cell refusal was taken before
/// any decode. With the whole-cell tier gone (finding S1) every refusal is a NODE
/// refusal, including one at the cell's own root node, and property 2 is applied
/// by the decoder's [`strip`] at that node's depth — for the root exactly as for
/// any other, so a frame divergence there is reported as an unparseable rendering
/// instead of being folded in here. `cli` is therefore always the un-split BODY
/// the decoder left, and the only thing left to apply is the emptiness bound.
///
/// `cli` may still be a non-text value: when the CSV cell is empty the decode is
/// never attempted, and property 1 is what reports that.
pub fn decidable_despite_node_refusal(golden: &Value, cli: &Value) -> Result<(), String> {
    let Some(members) = member_count(golden) else {
        // Not a container, so no node refusal can have been taken for it.
        return Ok(());
    };
    let body = cli_text(cli)?;
    body_emptiness_bound(members, body, body)
}

/// The member count of a golden container, or `None` for a scalar.
fn member_count(golden: &Value) -> Option<usize> {
    match golden {
        Value::Array(items) => Some(items.len()),
        Value::Object(fields) => Some(fields.len()),
        _ => None,
    }
}

/// The CLI side of a refused position must be TEXT: property 1 above.
fn cli_text(cli: &Value) -> Result<&str, String> {
    match cli {
        Value::String(text) => Ok(text),
        other => Err(format!(
            "the golden carries a container the CSV rendering cannot express \
             unambiguously, but the csv egress cell is {} — a container always \
             renders as at least its bracket pair, so an empty or non-text field \
             is a divergence the ambiguity does not cover",
            match other {
                Value::Null => "absent/empty".to_string(),
                other => brief(&other.to_string()),
            }
        )),
    }
}

/// Property 3, and NOTHING more: is the body's EMPTINESS consistent with the
/// golden's member count, in the only two directions no confusable reading can
/// reach?
///
/// Named for what it checks, because it is NOT a bound on the count itself: at
/// exactly ONE member it accepts any body, and at two or more it accepts any
/// NON-EMPTY body, so WHICH members the body holds is never compared here (the
/// module doc's residual, finding Q1).
///
/// `rendering` is what the diagnostic quotes (the whole cell, or the body).
fn body_emptiness_bound(members: usize, body: &str, rendering: &str) -> Result<(), String> {
    if members == 0 && !body.is_empty() {
        return Err(format!(
            "the golden container is EMPTY, so the only renderings the CSV \
             ambiguity confuses are the empty bracket pair — but the csv egress \
             cell {} carries a body",
            brief(rendering)
        ));
    }
    if members >= 2 && body.is_empty() {
        return Err(format!(
            "the golden container holds {members} members, which cannot render as \
             an empty body (members are `, `-separated even when each is empty), \
             but the csv egress cell is {}",
            brief(rendering)
        ));
    }
    Ok(())
}

/// The per-type SPELLING half of this module — what text a value of a declared type
/// carries, and whether it can be empty (issue #3815 round 3 split it out under the
/// campsite rule; that file's module doc states the seam). Re-exported so no call
/// site changes.
#[path = "golden_csv_spelling.rs"]
mod spelling_half;
use spelling_half::{
    empty_container_is_ambiguous, member_can_render_empty, scalar_csv_text, stringified_csv_text,
};

/// Inverting the rendering — the DECODE half of this module (issue #3726 split it
/// out under the campsite rule). Re-exported so no call site changes.
#[path = "golden_csv_decode.rs"]
mod decode_half;
pub use decode_half::{decode, decode_at, Excluded};
// `members` and `entry_cut` are the grammar helpers the REFUSAL half asks its question
// WITH — the same two the decode runs, which is what stops the refusal set drifting from
// the decode. `scan` is imported for this module's own test cases, which exercise the
// depth rule directly; it is not used by the refusal path.
use decode_half::{entry_cut, members, scan};

/// Truncate a rendering for a diagnostic (the corpus carries 4 KiB blobs).
fn brief(s: &str) -> String {
    const LIMIT: usize = 80;
    if s.chars().count() <= LIMIT {
        return format!("`{s}`");
    }
    let head: String = s.chars().take(LIMIT).collect();
    format!("`{head}…`({} chars)", s.chars().count())
}

/// Unit coverage for the branches the corpus does not reach — the refusal shapes
/// and every strictness rule (split out under the campsite rule; the preamble
/// that states WHY they are unit cases is that file's module doc).
#[cfg(test)]
#[path = "golden_csv_container_tests.rs"]
mod tests;
