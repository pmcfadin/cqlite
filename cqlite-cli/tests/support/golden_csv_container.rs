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
//! ## A SECOND declared residual: no refusal is asked at a MAP KEY node (#3726)
//!
//! Every position the COMPARISON walks is asked [`node_refusal`], because
//! `compare::compare_value_at` asks it at each node it visits. A map KEY is not
//! such a node: `compare::compare_map` PAIRS keys (it canonicalizes them) rather
//! than recursing into them, so there is nowhere in that path to record a refusal.
//!
//! What that costs, exactly. At the map NODE, [`decode_does_not_recover`] does
//! verify a container key's recoverability twice over — the entry split must give
//! the golden's entries back, and [`entry_cut`] must give each rendered KEY back —
//! so a `, ` or a `: ` that would move either cut IS refused. What it does not
//! reach is the key's OWN member split: a `, ` inside a scalar member of the key
//! (a `list<text>` key holding `["a, b"]`, rendering `{a, b}`) leaves both of those
//! cuts intact and yet decodes into two members where the golden has one — which
//! this lane would report as a divergence the CLI did not cause. Bounding it means
//! `compare_map` asking [`node_refusal`] at the key node and recording it, which is
//! a code path no committed fixture reaches; it is declared here instead. MEASURED
//! on the corpus: no container map key anywhere in it carries a `, `, a `: ` or a
//! bracket inside a member — `test_nested_udt_keys.nested_udt_keys`'s keys are
//! `key_part` UDTs whose `label`s are plain identifiers, an empty string or null.

use super::schema::CqlType;
use super::{container, stringified_blob_spelling, Kinding};
use serde_json::{Map, Value};

/// The ONE bracket pair a container of this declared type may be rendered with
/// (the grammar in the module doc), or `None` for a scalar type.
///
/// Taken from the DDL, so each kind is required to use its own bracket: a `set`
/// rendered `[a, b]` or a `tuple` rendered `[a, b]` is a failure (review finding
/// R2), where the earlier golden-shape-only rule accepted any of the three.
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

/// Is the golden AT THIS NODE unrecoverable from the flat rendering, for a reason
/// whose blast radius is THIS node's member split? `Some(reason)` means the node's
/// contents and count are refused — and nothing else is (review finding P2).
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
    // EMPTY-CONTAINER: zero members and one EMPTY member render identically.
    // Keyed on the AFFIRMATIVE answer — this element type really can render
    // empty — so an unknown or non-collection type does not refuse.
    if let Value::Array(items) = golden {
        if items.is_empty() && empty_container_is_ambiguous(ty) {
            return Some(
                "an empty container is indistinguishable from a container of one empty \
                 member of this element type"
                    .into(),
            );
        }
    }
    decode_does_not_recover(golden, ty, kinding)
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
) -> Option<String> {
    // A scalar node has no body of its own: every cause here is about the body
    // that HOLDS it, so the container one level up is what reports them.
    if is_scalar(golden) {
        return None;
    }
    // An undeclared type is not refused: see the note on `node_refusal`'s `ty`.
    let ty = ty?;
    let rendering = golden_rendering(golden, Some(ty), kinding)?;
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
            return Some(format!(
                "the decoder cannot split the golden's own rendering {}: {why}",
                brief(&rendering)
            ))
        }
    };
    match golden {
        Value::Array(items) => {
            let want = items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    golden_rendering(item, member_type(Some(ty), i), member_kinding(ty, kinding))
                })
                .collect::<Option<Vec<String>>>()?;
            split_mismatch(&rendering, "member", &parts, &want)
        }
        Value::Object(fields) => {
            // The keys are rendered ONCE and reused below, so the split check and
            // the key-cut check cannot ask two different questions about the same
            // key — and so the "a key that does not render" case is decided in one
            // place (here, by returning `None`: no refusal, because the golden's
            // key is not a spelling of this declared type at all, which is a
            // divergence for the comparison to report and not a format limit).
            let keys = fields
                .keys()
                .map(|key| entry_key_rendering(ty, key))
                .collect::<Option<Vec<String>>>()?;
            let want = fields
                .iter()
                .zip(keys.iter())
                .map(|((key, value), rendered_key)| {
                    // The VALUE of a map entry / UDT field is a cell value, so it
                    // keeps its natural kind — `compare::compare_map` and
                    // `compare::udt::compare_udt` say the same thing.
                    golden_rendering(value, field_type(Some(ty), key), Kinding::Natural)
                        .map(|value| format!("{rendered_key}: {value}"))
                })
                .collect::<Option<Vec<String>>>()?;
            if let Some(why) = split_mismatch(&rendering, "entry", &parts, &want) {
                return Some(why);
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
            keys.iter().zip(parts.iter()).find_map(|(key, part)| {
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
            })
        }
        // Unreachable: the scalar case returned above and every other shape is a
        // container the two arms cover.
        _ => None,
    }
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
/// Three cases, and each one's authority:
///
///   * a UDT entry's key is a FIELD NAME — not a value — so the grammar writes it
///     verbatim;
///   * a MAP entry's key whose declared type is a CONTAINER is the key value's own
///     `toJSONString` document (`cassandra-5.0.8 MapType.toJSONString` writes
///     `keys.toJSONString(kv, protocolVersion)` and quotes it only when it does not
///     already start with `"`), so it is PARSED — through the lane's one
///     `container::golden_map_key_value` — and then rendered by this module's own
///     grammar, at [`Kinding::Natural`] because a frozen container's members are
///     cell values (issue #3726). Left as the raw JSON text it would carry `, ` and
///     `: ` of its own, so [`decode_does_not_recover`] would judge every such node
///     unrecoverable and REFUSE it — which is how the CSV half of this issue stayed
///     open;
///   * a MAP entry's key whose declared type is a SCALAR is spelled by the golden
///     under [`Kinding::Stringified`], because a JSON object key can only be a
///     string. That is the same reading `compare::compare_map` applies (and the
///     reason it holds the CLI's own key to [`Kinding::Natural`]), so the same
///     translation applies here.
fn entry_key_rendering(object: &CqlType, key: &str) -> Option<String> {
    match object {
        CqlType::Map(key_ty, _) if container::is_container_type(key_ty) => {
            let value = container::golden_map_key_value(key, key_ty).ok()?;
            golden_rendering(&value, Some(key_ty), Kinding::Natural)
        }
        CqlType::Map(key_ty, _) => Some(stringified_csv_text(key.to_string(), key_ty)),
        _ => Some(key.to_string()),
    }
}

/// The text a scalar carries inside the golden's own rendering, translated to the
/// CSV spelling where the golden's own spelling at THIS position is not it.
///
/// An untyped position is left verbatim: no translation can be derived without a
/// declared type, and the shape disagreement belongs to the comparison.
fn scalar_csv_text(scalar: &Value, ty: Option<&CqlType>, kinding: Kinding) -> String {
    let text = scalar_text(scalar);
    match (kinding, ty) {
        (Kinding::Stringified, Some(ty)) => stringified_csv_text(text, ty),
        _ => text,
    }
}

/// The CSV text a golden scalar at a [`Kinding::Stringified`] position denotes.
///
/// # The two sides, and which authority each comes from
///
/// A stringified golden is `writeString(type.getString(v))` — the GOLDEN side, read
/// from the pin `cassandra-5.0.8` (the per-type census is in [`Kinding`]'s doc).
/// The CSV side is a question about CQLite's OWN output shape, so it is read from
/// `cqlite_core::util::value_fmt::ValueFormatter::format_value`, which is
/// legitimate for the same reason [`member_can_render_empty`] states at length.
///
/// # Why only `blob`
///
/// Walked over every type that can occupy a stringified position (a partition-key
/// component, a multicell set's element, a map key). `getString` is
/// `serializer.toString(deserialize(v))`:
///
///   * **`blob` — DIFFERS, and MATERIALLY.** `BytesSerializer.toString` is
///     `ByteBufferUtil.bytesToHex`, the BARE lowercase hex, so the empty blob is
///     `""`; `ValueFormatter` renders `format!("0x{hex}")`, so the empty blob is
///     `0x`. Left untranslated, a sole empty blob member synthesized an EMPTY body
///     and the node was refused as unrecoverable — and a refused one-member node
///     accepts any framed body at all, so the member went uncompared. Translated
///     by [`super::stringified_blob_spelling`], the one place this repository
///     states the rule;
///   * **`timestamp` — differs, IMMATERIALLY.** `FORMATTER_UTC`'s
///     `yyyy-MM-dd'T'HH:mm:ss.SSSX` against `ValueFormatter::format_timestamp`'s
///     `YYYY-MM-DD HH:MM:SS.fff+0000`. That is the lane's DECLARED timestamp
///     narrowing and this function does not close it; it cannot move a `, `, a
///     `: ` (the pattern's colons are digit-flanked) or a bracket, and neither
///     spelling is ever empty, so the structural question is unaffected;
///   * **every other type — IDENTICAL text.** `boolean` is `Boolean.toString()` on
///     both sides; the integer family is `String.valueOf` / `BigInteger.toString(10)`
///     against `to_string()`; `float`/`double`/`decimal` differ only in the
///     narrowings this lane already declares (trailing zeros, exponent form), which
///     like the timestamp cannot carry a separator, a bracket or an empty spelling;
///     `text`/`varchar`/`ascii`, `uuid`/`timeuuid`, `date`, `time`, `duration` and
///     `inet` are spelled by the same function on both sides.
///
/// A CONTAINER type cannot be reached: this is called for a scalar golden only,
/// and a frozen container at a stringified position is the case [`Kinding`] names
/// as NOT COVERED (`getString` spells the whole value as one string, which the
/// comparison reports as a shape divergence).
///
/// TOTAL over `CqlType` with no wildcard, for the reason
/// [`member_can_render_empty`] gives: a new variant must have its answer
/// established here rather than inherited from whichever side a wildcard sat on.
fn stringified_csv_text(text: String, ty: &CqlType) -> String {
    match ty {
        CqlType::Blob => stringified_blob_spelling(&text).unwrap_or(text),
        CqlType::Numeric(_)
        | CqlType::Text(_)
        | CqlType::Boolean
        | CqlType::Timestamp
        | CqlType::Opaque(_)
        | CqlType::List(_)
        | CqlType::Set(_)
        | CqlType::Map(..)
        | CqlType::Tuple(_)
        | CqlType::Udt(_) => text,
    }
}

/// The text a scalar carries inside the golden's own rendering
/// ([`golden_rendering`]).
/// `Value::Null` renders as the `null` token (NULL-TOKEN in the module doc), which
/// is a text a `text` member can also produce — resolved by [`decode_shape`] from
/// the golden's own type, and deliberately not a refusal.
fn scalar_text(scalar: &Value) -> String {
    match scalar {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

fn is_scalar(v: &Value) -> bool {
    !matches!(v, Value::Array(_) | Value::Object(_))
}

/// Is an EMPTY golden array of this declared type genuinely unrecoverable?
///
/// Only for a `list`/`set` whose ELEMENT can render as the empty string. A
/// `tuple` is exempt because its member count comes from the DDL, so the
/// comparison's arity check sees a dropped member; every other type does not
/// describe an array at all, and refusing there would hide the shape divergence
/// the comparison exists to report.
fn empty_container_is_ambiguous(ty: Option<&CqlType>) -> bool {
    match ty {
        Some(CqlType::List(element)) | Some(CqlType::Set(element)) => {
            member_can_render_empty(element)
        }
        _ => false,
    }
}

/// Can a value of this declared type render as the EMPTY string?
///
/// # Why asking CQLite's own formatter is legitimate here
///
/// This is the one question in this lane whose answer comes from
/// `cqlite_core::util::value_fmt::ValueFormatter` rather than from an external
/// oracle, and the distinction is worth stating: it does not ask what a value of
/// this type SHOULD render as — that is the `sstabledump` golden's answer, and
/// taking it from CQLite would be circular (CLAUDE.md, #3042) — it asks what
/// SHAPE this egress's own output can take, i.e. whether the CSV rendering of SOME
/// value of this type can be zero-length. Nothing outside the formatter can answer
/// that, and the answer only ever decides whether an EMPTY container is refused;
/// every value the comparison then makes is still the golden's.
///
/// # The answer, per type, from that formatter's branches
///
/// `ValueFormatter::format_value` has exactly one branch that passes its payload
/// straight through — `Value::Text(s)` renders `String::from_utf8_lossy(s)` — so
/// `text`/`varchar`/`ascii` are the ONLY types with an empty rendering. Every other
/// branch emits at least one character on every path it can take, including its
/// emptiest and its invalid inputs:
///
///   * integers/floats render through `to_string()`, a `{:e}`/`{}` format, or the
///     tokens `NaN`/`Infinity`/`-Infinity`; a zero-length `varint` renders `0`, and
///     a zero-length or over-ceiling `decimal` renders `0` or
///     `<corrupt-decimal:…>`;
///   * `boolean` is `true`/`false`;
///   * `blob` is `format!("0x{hex}")`, so an EMPTY blob is `0x` — 2 characters, not
///     none. This is the type the earlier deny-list got wrong;
///   * `timestamp`/`date`/`time` render a fixed-width `chrono` pattern, or an
///     `<invalid-…:{value}>` marker;
///   * `uuid`/`timeuuid` render 36 characters; `inet` renders an
///     `Ipv4Addr`/`Ipv6Addr` display or `<invalid-inet:N-bytes>`; a `duration`
///     whose every component is zero renders `0ns`;
///   * a container renders its bracket pair, so an empty one is `[]`/`{}`/`()`.
///
/// A NULL member does not widen any of these: `Value::Null` renders as the `null`
/// token (the module doc's NULL-TOKEN), which is 4 characters.
///
/// `tests::an_empty_rendering_is_possible_only_for_text` runs that formatter over
/// each type's emptiest value and requires this function to agree with it, so the
/// claim above is measured rather than asserted in prose.
///
/// # Exhaustive on purpose, and why the DEFAULT matters
///
/// Written as a total match with no `_` arm. The earlier form was a deny-list —
/// "answer `false` for these variants, `true` for everything else" — which
/// answered `true` for `blob`, `timestamp` and every opaque scalar, so an empty
/// collection of any of them was refused and dropped from the coverage counts
/// (review round 19, finding Y2). Over-refusal is a BLIND SPOT and not
/// conservatism: a refused node keeps only [`body_emptiness_bound`], so refusing a
/// recoverable position makes it unchecked. A wildcard would also decide a FUTURE
/// `CqlType` variant's answer silently, in whichever direction the wildcard
/// happens to sit; with the match total, a new variant is a compile error whose
/// fix is to establish that type's answer from the formatter, here.
fn member_can_render_empty(ty: &CqlType) -> bool {
    match ty {
        // The one pass-the-payload-through branch: an empty string renders as
        // nothing at all, which is what makes `{}` ambiguous for a `set<text>`.
        CqlType::Text(_) => true,
        CqlType::Numeric(_)
        | CqlType::Boolean
        | CqlType::Blob
        | CqlType::Timestamp
        | CqlType::Opaque(_)
        | CqlType::List(_)
        | CqlType::Set(_)
        | CqlType::Map(..)
        | CqlType::Tuple(_)
        | CqlType::Udt(_) => false,
    }
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

/// A predicate over a value path: `true` means the comparison excludes that path,
/// so the decoder must not require the CLI's text there to invert the grammar.
///
/// Kept as a bare closure type rather than a dependency on the comparator's
/// `SkipPaths`, so this module stays independent of it.
pub type Excluded<'a> = dyn Fn(&str) -> bool + 'a;

/// Decode `text` (one CSV field, or one member of one) into the shape `golden`
/// declares, with `ty` — the column's DECLARED CQL type — supplying the bracket
/// each container kind must be rendered with. A map/UDT decodes to the
/// `[{"key":…,"value":…}, …]` spelling the JSON egress uses, so the existing map
/// comparison applies unchanged.
pub fn decode(golden: &Value, text: &str, ty: &CqlType, kinding: Kinding) -> Result<Value, String> {
    decode_at(golden, text, ty, "", &|_| false, kinding)
}

/// [`decode`], but aware of the paths the comparison excludes.
///
/// `path` is the fully-qualified position of `text` in the row, spelled the same
/// way the comparator spells it (`col.field` for a named field, `col[i]` for a
/// positional member). At an EXCLUDED path the decode is ATTEMPTED and its result
/// used when it succeeds, falling back to the raw, UNDECODED text only when the
/// grammar does not invert there. That fallback is what keeps one un-invertible
/// member from failing a whole cell nobody compares — which is what forced the
/// `udt_nested` exclusion to be whole-column (issue #1491 review finding F5).
///
/// Attempting it rather than short-circuiting matters for the STALENESS side
/// (finding L1): an exclusion is applied only while it suppresses a real
/// divergence, and an unconditional raw-text answer here would keep the excluded
/// position diverging (an object against a string) even after CQLite renders it
/// correctly — so the gap could never retire itself.
///
/// The refusal scan is deliberately NOT exclusion-aware: it is decided from the
/// golden alone, and refusing a node is a conservative, counted, NAMED outcome in
/// the census, never a silent pass.
pub fn decode_at(
    golden: &Value,
    text: &str,
    ty: &CqlType,
    path: &str,
    excluded: &Excluded<'_>,
    kinding: Kinding,
) -> Result<Value, String> {
    if excluded(path) {
        return Ok(decode_shape(golden, text, ty, path, excluded, kinding)
            .unwrap_or_else(|_| Value::String(text.to_string())));
    }
    decode_shape(golden, text, ty, path, excluded, kinding)
}

/// The decode itself, with no exclusion check of its own at this level — that
/// belongs to [`decode_at`], so this can be run for an excluded path too. Nested
/// members still go through [`decode_at`], so a deeper exclusion applies normally.
fn decode_shape(
    golden: &Value,
    text: &str,
    ty: &CqlType,
    path: &str,
    excluded: &Excluded<'_>,
    kinding: Kinding,
) -> Result<Value, String> {
    // A node the GOLDEN's own content makes unsplittable is not split: its FRAME
    // is still required (that is `strip`, i.e. property 2 of
    // `decidable_despite_node_refusal`, applied at every depth) and its un-split
    // BODY is handed on for the count bounds. Interpreting the body here is what
    // would produce a member count no reading of the rendering supports — and
    // deciding the refusal for the whole CELL instead is what let an ambiguous
    // NESTED member suppress its unambiguous siblings and the outer structure
    // (review finding P2).
    //
    // The comparator asks `node_refusal` at the same node, on the same golden and
    // the same declared type, so it expects exactly what is left here.
    if node_refusal(golden, Some(ty), kinding).is_some() {
        return Ok(Value::String(strip(text, ty)?.to_string()));
    }
    // The declared TYPE decides the structure — including which bracket is
    // required — and the golden decides the member shapes underneath it. When the
    // two disagree the child is decoded against `null`, and the comparison is what
    // reports the shape divergence.
    match ty {
        CqlType::List(element) | CqlType::Set(element) => decode_sequence(
            golden,
            text,
            ty,
            &|_| Some(element),
            path,
            excluded,
            member_kinding(ty, kinding),
        ),
        CqlType::Tuple(items) => decode_sequence(
            golden,
            text,
            ty,
            &|i| items.get(i),
            path,
            excluded,
            member_kinding(ty, kinding),
        ),
        CqlType::Map(_, value_ty) => {
            decode_object(golden, text, ty, &|_| Some(value_ty), path, excluded)
        }
        CqlType::Udt(udt) => decode_object(
            golden,
            text,
            ty,
            &|field| {
                udt.fields
                    .iter()
                    .find(|(name, _)| name == field)
                    .map(|(_, t)| t)
            },
            path,
            excluded,
        ),
        // NULL-TOKEN: the golden's own type resolves the `null` token.
        _ => match golden {
            Value::Null if text == "null" => Ok(Value::Null),
            _ => Ok(Value::String(text.to_string())),
        },
    }
}

/// A list / set / tuple: one bracket pair fixed by `ty`, `, `-separated members,
/// each decoded under the element type `element_ty` gives for its position.
///
/// `element_ty` answers `None` only for a member BEYOND a tuple's declared arity,
/// which has no declared type; such a member is kept as raw text so the
/// comparator reports the arity divergence rather than the decoder swallowing it.
fn decode_sequence<'t>(
    golden: &Value,
    text: &str,
    ty: &'t CqlType,
    element_ty: &dyn Fn(usize) -> Option<&'t CqlType>,
    path: &str,
    excluded: &Excluded<'_>,
    element_kinding: Kinding,
) -> Result<Value, String> {
    let parts = members(text, ty)?;
    let items = golden.as_array();
    let mut out = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        // A member the golden does not have is decoded against `null`; the
        // length mismatch is what the comparison then reports.
        let child_golden = items.and_then(|g| g.get(i)).unwrap_or(&Value::Null);
        out.push(match element_ty(i) {
            Some(et) => decode_at(
                child_golden,
                part,
                et,
                &format!("{path}[{i}]"),
                excluded,
                element_kinding,
            )?,
            None => Value::String((*part).to_string()),
        });
    }
    Ok(Value::Array(out))
}

/// A map or UDT: `{…}`, `, `-separated `key: value` entries. `value_ty` answers
/// with the declared type of the value under a given key — `None` for a UDT field
/// the `CREATE TYPE` does not declare, whose value is therefore left as raw text
/// for the comparator to reject by name.
///
/// # A MAP's key is a VALUE and is decoded under its declared type (issue #3726)
///
/// A UDT entry's key is a FIELD NAME and stays verbatim. A map entry's key is a
/// value, so when the DDL declares a CONTAINER key type the key text is decoded
/// recursively like any other position — without that, CSV handed
/// `compare::compare_map` a flat string where the declared type says a container,
/// and the CSV half of a container-keyed map could not be compared at all.
///
/// # WHICH golden entry is this one, and why the answer is a RENDERING
///
/// The golden's own key is looked up by the text [`entry_key_rendering`] renders it
/// as — the same text [`decode_does_not_recover`] requires the decoder to recover
/// from the golden's own rendering — so the decoder and the refusal agree about
/// which entry is which. Matching the CLI's raw key text against the golden's
/// object key directly (the previous rule) held only while the two spellings
/// coincided, which is exactly what a container key does not do (nor, latently, a
/// `blob` key, whose golden `getString` text is the BARE hex while the CSV egress
/// renders `0x…`).
fn decode_object<'t>(
    golden: &Value,
    text: &str,
    ty: &'t CqlType,
    value_ty: &dyn Fn(&str) -> Option<&'t CqlType>,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    let parts = members(text, ty)?;
    let fields = golden.as_object();
    let container_key_ty = match ty {
        CqlType::Map(key_ty, _) if container::is_container_type(key_ty) => Some(&**key_ty),
        _ => None,
    };
    let mut out = Vec::with_capacity(parts.len());
    for part in parts {
        let (key, value) = entry_cut(part)?;
        // A UDT field step, spelled `parent.field` as the comparator spells it. A
        // MAP key reaches the same branch (CSV cannot tell the two apart), but a
        // dotted skip path through a map is rejected when the case is validated
        // against the DDL, so no exclusion can ever name one.
        let child = if path.is_empty() {
            key.to_string()
        } else {
            format!("{path}.{key}")
        };
        let golden_key = fields.and_then(|g| {
            g.keys()
                .find(|k| entry_key_rendering(ty, k).as_deref() == Some(key))
        });
        let mut entry = Map::new();
        entry.insert(
            "key".to_string(),
            match container_key_ty {
                Some(key_ty) => {
                    // The golden's own parsed key guides the decode where there is
                    // one; a key the golden does not have is decoded from the
                    // declared type alone and the comparison reports the difference.
                    let guide = golden_key
                        .and_then(|k| container::golden_map_key_value(k, key_ty).ok())
                        .unwrap_or(Value::Null);
                    // A key whose text does not invert the grammar is left as RAW
                    // TEXT rather than failing the whole cell — the same rule this
                    // module already applies to a member beyond a tuple's declared
                    // arity and to an undeclared UDT field. Nothing is swallowed:
                    // `compare::compare_map` canonicalizes that text under the
                    // declared container key type, which REFUSES a flat scalar and
                    // names the position.
                    decode_at(&guide, key, key_ty, &child, excluded, Kinding::Natural)
                        .unwrap_or_else(|_| Value::String(key.to_string()))
                }
                None => Value::String(key.to_string()),
            },
        );
        let child_golden = golden_key
            .and_then(|k| fields.and_then(|g| g.get(k)))
            .unwrap_or(&Value::Null);
        let decoded = match value_ty(key) {
            // A map VALUE / UDT field is a cell value: natural kind, as in
            // `golden_rendering`'s object arm and in the comparator.
            Some(vt) => decode_at(child_golden, value, vt, &child, excluded, Kinding::Natural)?,
            None => Value::String(value.to_string()),
        };
        entry.insert("value".to_string(), decoded);
        out.push(Value::Object(entry));
    }
    Ok(Value::Array(out))
}

/// Cut one map/UDT entry into its key and its value at the FIRST top-level `: `,
/// which is the only cut the grammar defines: a `: ` in a VALUE is ordinary text.
///
/// Factored out because [`decode_object`] and [`decode_does_not_recover`] must make
/// the IDENTICAL cut — the refusal asks whether this cut gives the golden's key
/// back, so a second spelling of it is a second notion of decodability, which is
/// the drift this lane's review history is made of.
fn entry_cut(part: &str) -> Result<(&str, &str), String> {
    let cut = *scan(part, ": ")?.first().ok_or_else(|| {
        format!(
            "map/UDT entry {} has no top-level `: ` separator",
            brief(part)
        )
    })?;
    Ok((&part[..cut], &part[cut + 2..]))
}

/// Strip the bracket pair `ty` requires and split the body at every depth-zero
/// `, `. An empty body is zero members.
fn members<'a>(text: &'a str, ty: &CqlType) -> Result<Vec<&'a str>, String> {
    let inner = strip(text, ty)?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    split_top_level(inner, ", ")
}

/// Remove the opening/closing bracket pair the DECLARED type requires. Strict in
/// both directions: a body that does not open with THAT bracket, or does not close
/// with its mate, is an error rather than a best-effort parse — so a `set`
/// rendered `[a, b]` fails instead of being read as a list.
fn strip<'a>(text: &'a str, ty: &CqlType) -> Result<&'a str, String> {
    let (open, close) = brackets(ty).ok_or_else(|| {
        format!(
            "{} was decoded as a container but the schema declares the scalar type \
             `{}`",
            brief(text),
            ty.describe()
        )
    })?;
    let rest = text.strip_prefix(open).ok_or_else(|| {
        format!(
            "{} is not a `{}` rendering: the declared type requires an opening \
             `{open}` (`{open}…{close}`)",
            brief(text),
            ty.describe()
        )
    })?;
    rest.strip_suffix(close).ok_or_else(|| {
        format!(
            "{} opens with `{open}` but does not close with `{close}`",
            brief(text)
        )
    })
}

/// Split `body` at every depth-zero `sep`.
fn split_top_level<'a>(body: &'a str, sep: &str) -> Result<Vec<&'a str>, String> {
    let cuts = scan(body, sep)?;
    let mut parts = Vec::with_capacity(cuts.len() + 1);
    let mut start = 0usize;
    for cut in cuts {
        parts.push(&body[start..cut]);
        start = cut + sep.len();
    }
    parts.push(&body[start..]);
    Ok(parts)
}

/// Byte offsets of every depth-zero, non-overlapping `sep` in `body`.
///
/// Iterates by `char_indices` so slicing stays on UTF-8 boundaries (member text
/// is arbitrary CQL `text`). Unbalanced brackets are an error — the rendering is
/// then not the grammar this decoder inverts, and silently tolerating it is how
/// a decoder starts absorbing writer defects.
fn scan(body: &str, sep: &str) -> Result<Vec<usize>, String> {
    let mut cuts = Vec::new();
    let mut depth: i32 = 0;
    let mut consumed = 0usize;
    for (idx, ch) in body.char_indices() {
        match ch {
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(format!(
                        "{} closes a bracket that never opened",
                        brief(body)
                    ));
                }
            }
            _ => {}
        }
        if depth == 0 && idx >= consumed && body[idx..].starts_with(sep) {
            cuts.push(idx);
            consumed = idx + sep.len();
        }
    }
    if depth != 0 {
        return Err(format!(
            "{} leaves {depth} bracket(s) unclosed",
            brief(body)
        ));
    }
    Ok(cuts)
}

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
