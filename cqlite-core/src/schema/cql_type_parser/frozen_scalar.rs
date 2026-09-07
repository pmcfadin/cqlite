//! `frozen<T>` IS NOT DECLARABLE OVER A SCALAR — the one rule, both spellings
//! (issue #4104).
//!
//! # SCOPE OF THE CLAIM — read this before trusting the title
//!
//! "Both spellings" means the two METADATA ENTRY POINTS (CQL type string,
//! SerializationHeader marshal string), NOT "every syntactic position". The title
//! states a rule, not a totality proof; a guarantee this module does not deliver
//! would be its own defect (roborev job 116, finding 2).
//!
//! The one position #4104 DECLARED as not covered — a frozen scalar nested in a
//! VECTOR ELEMENT, `frozen<vector<frozen<int>, 3>>` — is now covered, and NOT by
//! anything this module does. #4149 gave `CqlType` a `Vector(element, dimension)`
//! variant whose parser recurses into the element through `parse_with_depth`, which
//! re-enters the CQL gate below; the refusal reaches that position for free. Pinned
//! by `a_frozen_scalar_in_a_vector_element_is_refused_since_4149`. Issue #4154 stays
//! open for the audit of the remaining positions — this note records one position
//! closing, not the whole issue. The MARSHAL half still checks a head class only and
//! descends into nothing; its bound is declared on
//! [`FREEZABLE_MARSHAL_SIMPLE_NAMES`].
//!
//! # The oracle is Cassandra's GRAMMAR, not its bytes
//!
//! `CQL3Type.Raw::freeze()` is the BASE implementation and it does nothing but
//! throw (`cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`):
//!
//! ```text
//!         public Raw freeze()
//!         {
//!             String message = String.format("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", this);
//!             throw new InvalidRequestException(message);
//!         }
//! ```
//!
//! and the grammar routes EVERY `frozen<…>` through it
//! (`cassandra-5.0.8:src/antlr/Parser.g:1853-1860`):
//!
//! ```text
//!     | K_FROZEN '<' f=comparatorType '>'
//!       {
//!         try {
//!             $t = f.freeze();
//!         } catch (InvalidRequestException e) {
//!             addRecognitionError(e.getMessage());
//!         }
//!       }
//! ```
//!
//! Only four subclasses override it — `RawCollection` (`:773`, list/set/map),
//! `RawVector` (`:916`), `RawUT` (`:958`) and `RawTuple` (`:1037`). Everything
//! else, `RawType` (every native scalar) and a `STRING_LITERAL` custom class
//! included, reaches the base and is a recognition error.
//!
//! Three consequences, and they are the whole content of this module:
//!
//! 1. no Cassandra table can carry a `frozen<scalar>` column, map key, collection
//!    element or UDT field;
//! 2. no Cassandra-written `Statistics.db` SerializationHeader can spell
//!    `FrozenType(Int32Type)`;
//! 3. **there are no Cassandra-written bytes for this input, BY CONSTRUCTION** — so
//!    under the no-heuristics mandate (#28) CQLite must not invent a decode result
//!    for it. Two lanes did invent one, in opposite directions (`Value::blob(b"")`
//!    on #3847/PR #4017, `Value::Empty(Int)` on #3805/PR #4033); the lead's
//!    `REQ-3805-14` ruling deleted BOTH pins and sent the refusal here.
//!
//! # Corroborated on the real corpus, and the recipe is reproducible
//!
//! Every `FrozenType(` occurrence in the fetched corpus was censused
//! (310 `Statistics.db`/`Data.db` files under `/data/datasets`, i.e. every one the
//! box holds) and the inner head was one of exactly four classes — `MapType` (25),
//! `ListType` (16), `UserType` (10), `SetType` (9); no scalar, ever. Reproduce with:
//!
//! ```text
//! python3 - <<'EOF'
//! import re, os, collections
//! pat = re.compile(rb'FrozenType\(([A-Za-z0-9_.]+)')
//! c = collections.Counter()
//! for root, _, files in os.walk(os.environ['CQLITE_DATASETS_ROOT']):
//!     for f in files:
//!         if f.endswith(('Statistics.db', 'Data.db')):
//!             for m in pat.finditer(open(os.path.join(root, f), 'rb').read()):
//!                 c[m.group(1).decode('ascii', 'replace')] += 1
//! print(c.most_common())
//! EOF
//! ```
//!
//! That is CORROBORATION and not the oracle: an absence measured over one corpus
//! could never establish an impossibility, which is why the grammar above does.
//!
//! # Where the two gates are, and why there are exactly two
//!
//! A declared type reaches CQLite through exactly two metadata entry points, and
//! this module is called from both:
//!
//! * the CQL type-string parser, [`CqlType::parse`] — via
//!   [`frozen_inner_supports_freezing`] + [`refuse_frozen_scalar_cql`];
//! * the `Statistics.db` SerializationHeader type parser — via
//!   [`validate_marshal_frozen`], called from the one choke point every header
//!   type string passes through
//!   (`parser::enhanced_statistics_parser::marshal_type::convert_marshal_type_to_cql_checked`).
//!
//! **No DECODE path gains a frozen-scalar branch**, and that is deliberate: the
//! refusal belongs upstream of decode, at the metadata boundary, so that no decoder
//! ever has to hold an opinion about bytes that cannot exist.

// # THIS DECLARATION SITS AT THE TOP ON PURPOSE, AND MOVING IT DOWN BREAKS THE GATE
//
// Conventional resolution: this file is `cql_type_parser/frozen_scalar.rs`, so it
// owns `cql_type_parser/frozen_scalar/` and the tests file sits there. No
// path-override attribute anywhere in the chain (see this module's declaration in
// `cql_type_parser.rs` for why an override put the tests file beyond the walker).
//
// The remaining hazard is POSITION, not path. With this exact declaration at the
// END of the file, `issue_1714_mod_reachability` reported the tests file as an
// orphan — "compiles nowhere, is linted nowhere, is tested nowhere" — while all 13
// of its tests were passing in the same run. Moved here, above the first `use`, the
// guard passes and the tests still run. The guard tracks inline-`mod` scope by
// counting braces as it walks the file, and this module's header quotes Java and
// ANTLR bodies (`CQL3Type.java` freeze(), `Parser.g`'s K_FROZEN rule) that carry
// braces and char literals; a late top-level `mod` is then resolved against a
// phantom scope directory. So: do not relocate this below the header. The guard's
// disagreement with rustc is a real defect, but it is not this issue's to fix, and
// a shared gate guard cannot certify its own change from a lane.
#[cfg(test)]
mod frozen_scalar_tests;

use crate::error::{Error, Result};
use crate::schema::{is_udt_identifier, CqlType};

/// The one wording, carrying the one citation. Kept as a `const` so the CQL and
/// marshal refusals cannot drift into two different explanations of one rule.
const CITATION: &str = "frozen<> is only allowed on collections, tuples, and user-defined types \
     (cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651 — \
     CQL3Type.Raw::freeze() throws for every type that does not override it)";

/// Whether a PARSED inner type may legally carry a `frozen<>` wrapper.
///
/// The membership set is Cassandra's override set, mapped onto [`CqlType`]:
/// `RawCollection` -> [`CqlType::List`]/[`CqlType::Set`]/[`CqlType::Map`],
/// `RawTuple` -> [`CqlType::Tuple`], `RawUT` -> [`CqlType::Udt`], and an
/// already-frozen inner ([`CqlType::Frozen`]) because `RawCollection::freeze`
/// (`CQL3Type.java:773-786`) freezes an already-frozen collection to itself.
///
/// [`CqlType::Custom`] carries TWO things `CqlType::parse` cannot model, and both
/// are legally freezable, so the arm decides between them by SPELLING:
///
///   * a UDT REFERENCE. `CqlType::parse` has no UDT registry, so a bare
///     `frozen<address_type>` lands in `Custom` and refusing it would refuse every
///     real frozen UDT. Admitted when the name is a plausible UDT identifier
///     ([`is_udt_identifier`]).
///   * a VECTOR — and as of #4149 this half is UNREACHABLE FROM
///     [`CqlType::parse`], which is stated here rather than quietly left to rot.
///     When #4104 wrote it, `CqlType` had no `Vector` variant and every
///     `vector<..>` spelling landed in `Custom`. #4149 added
///     `CqlType::Vector(element, dimension)` and sited its arm BEFORE the
///     UDT/primitive fall-through, so today `CqlType::parse` resolves a
///     `vector<..>` spelling to either `CqlType::Vector` (well-formed) or an
///     `Err` (malformed) — never to a `Custom`. Derivation: for
///     [`is_vector_spelling`] to answer `true` the text before the first `<` must
///     trim to exactly `vector`, which is precisely the shape
///     `schema::vector_type::cql_vector_kind` classifies as `Args` or
///     `Malformed`, and BOTH are handled upstream of this arm.
///
///     It is KEPT, not deleted, because this function is a `pub(crate)`
///     membership predicate over an ARBITRARY [`CqlType`] value and
///     `Custom("vector<float, 3>")` is still representable. Deleting the carve-out
///     would make the predicate answer `false` there while the
///     [`CqlType::Vector`] arm answers `true` for the same declared type — one
///     rule with two answers, which is the exact divergence class this module
///     exists to prevent. The two AGREE today: a well-formed spelling is `true`
///     either way, and a malformed one is refused either way (by this predicate
///     via `Custom`, or by `cql_vector_kind` before the predicate is reached).
///     Full derivation, factory to grammar, at [`FREEZABLE_MARSHAL_SIMPLE_NAMES`].
///
/// Everything else in `Custom` is refused, which keeps the quoted custom-class
/// spelling (`frozen<'org.apache.cassandra.db.marshal.Int32Type'>`) out — Cassandra
/// routes that through `CQL3Type.Raw.from(new CQL3Type.Custom(..))`, i.e. a
/// `RawType`, i.e. the throwing base (`Parser.g:1861-1864`).
///
/// Every native scalar is `false`. This function is the ONLY membership statement;
/// the enumeration is exhaustive with no `_` arm on purpose, so a new [`CqlType`]
/// variant cannot silently inherit either answer.
pub(crate) fn frozen_inner_supports_freezing(inner: &CqlType) -> bool {
    match inner {
        // Cassandra's four `freeze()` overrides, plus the already-frozen case.
        CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _) => true,
        CqlType::Tuple(_) => true,
        CqlType::Udt(_, _) => true,
        CqlType::Frozen(_) => true,
        // `RawVector` is Cassandra's FIFTH `freeze()` override, and it RETURNS
        // `this` rather than throwing (`CQL3Type.java:915-919`, with
        // `supportsFreezing() -> true` at `:909-913`), so `frozen<vector<float, 3>>`
        // IS declarable CQL and must not be refused.
        //
        // WHY THIS ARM IS NEW rather than always having been here: before #4149
        // `CqlType` had NO `Vector` variant, so `frozen<vector<float, 3>>` parsed to
        // `Frozen(Custom("vector<float, 3>"))` and the permission was granted by the
        // `Custom` arm's [`is_vector_spelling`] carve-out below. #4149 added
        // `CqlType::Vector(element, dimension)`, so the same spelling now parses to
        // `Frozen(Vector(Float, 3))` and the ONE rule has to be expressed on the new
        // variant too — otherwise this gate would refuse declarable CQL, which is a
        // worse defect than the hole it exists to close.
        //
        // Unconditional, and that is Cassandra's answer too: `RawVector::freeze`
        // returns `this` whatever the parameters, so freezability is a HEAD-CLASS
        // question here exactly as it is in [`FREEZABLE_MARSHAL_SIMPLE_NAMES`]. A
        // `CqlType::Vector` value can only exist because `schema::vector_type`
        // already validated its element and dimension (the CQL parser errors on a
        // malformed one BEFORE this predicate is reached), so there is nothing left
        // for a freezability gate to re-check.
        CqlType::Vector(_, _) => true,
        CqlType::Custom(name) => {
            let name = name.strip_prefix("udt:").unwrap_or(name.as_str());
            is_udt_identifier(name) || is_vector_spelling(name)
        }
        // `RawType` — every native scalar reaches the throwing base.
        CqlType::Boolean
        | CqlType::TinyInt
        | CqlType::SmallInt
        | CqlType::Int
        | CqlType::BigInt
        | CqlType::Counter
        | CqlType::Float
        | CqlType::Double
        | CqlType::Decimal
        | CqlType::Text
        | CqlType::Ascii
        | CqlType::Varchar
        | CqlType::Blob
        | CqlType::Timestamp
        | CqlType::Date
        | CqlType::Time
        | CqlType::Uuid
        | CqlType::TimeUuid
        | CqlType::Inet
        | CqlType::Duration
        | CqlType::Varint => false,
    }
}

/// Whether a `Custom` type name is a COMPLETE, DECLARABLE `vector<element,
/// dimension>` spelling.
///
/// # A HEAD-KEYWORD MATCH IS NOT ENOUGH, and that was roborev job 108's finding
/// This checked only that the text before the first `<` was `vector`, so
/// `frozen<vector<int>>` (no dimension), `frozen<vector<>>` (no arguments) and
/// `frozen<vector<int, nope>>` (dimension not an integer) were all granted
/// freezability. None of them is declarable CQL, so that was a permissive
/// fall-through GRANTING A PERMISSION CASSANDRA DOES NOT — the same
/// no-heuristics defect as an invented decode result, pointed the other way.
///
/// The `Custom` arm needs the spelling to be a vector because the SPELLING is all
/// it has: `CqlType` has no `Vector` variant, so a vector and an unparseable
/// string are the same value. A spelling that never becomes a `RawVector` never
/// reaches `RawVector::freeze` either, so it must be refused here.
///
/// # THE ACCEPTED GRAMMAR, DERIVED AT THE PINNED TAG
///
/// ```text
/// Parser.g:1916-1919
///   vector_type returns [CQL3Type.Raw vt]
///       : K_VECTOR '<' t1=comparatorType ','  d=INTEGER '>'
///           { $vt = CQL3Type.Raw.vector(t1, Integer.parseInt($d.text)); }
///       ;
/// Lexer.g:337-339
///   INTEGER : '-'? DIGIT+ ;
/// VectorType.java:86-92
///   private VectorType(AbstractType<T> elementType, int dimension) {
///       ...
///       if (dimension <= 0)
///           throw new InvalidRequestException("vectors may only have positive dimensions; given %d");
/// ```
///
/// So, exactly: the head keyword `vector`; EXACTLY TWO top-level arguments (the
/// element may itself contain commas, e.g. `vector<map<int, text>, 3>`); a
/// non-empty element; and a dimension that is a bare DIGIT string (`INTEGER`
/// permits a leading `-` and nothing else — no `+`, no separators, no radix),
/// fits a Java `int` (`Integer.parseInt` throws beyond it), and is `> 0`
/// (`VectorType`'s constructor). Rust's `parse::<i32>()` reproduces the `int`
/// bound exactly and RETURNS `Err` rather than saturating; requiring all-digits
/// first is what excludes the `-`/`+` spellings `parse` would otherwise accept.
///
/// # UNREACHABLE FROM `CqlType::parse` SINCE #4149
/// This predicate is retained for the reasons given on
/// [`frozen_inner_supports_freezing`]'s `Custom` arm (it must keep answering
/// consistently for a representable `Custom("vector<..>")`), but no `CqlType::parse`
/// input reaches it any more: #4149's vector arm resolves every `vector<..>`
/// spelling to `CqlType::Vector` or an `Err` before the `Custom` fall-through.
/// Consequences worth stating, because both were true when this was written and are
/// no longer:
///  * **The ELEMENT is now inspected** — by #4149's `parse_with_depth` recursion,
///    not here — so `frozen<vector<frozen<int>, 3>>` is refused. That closes the
///    position #4104 declared out of scope (issue #4154 stays open for the rest).
///    This predicate STILL does not inspect the element, and that no longer matters
///    on the parse path; it is why the predicate must not be the only gate.
///  * Re-entering `CqlType::parse` from HERE remains the wrong fix, and #4149 did
///    not do it: a fresh `parse` call restarts at depth 0, so
///    `frozen<vector<frozen<vector<…` would recurse unbounded past
///    `MAX_NESTING_DEPTH` (issue #1690's stack-overflow hazard). #4149 threads the
///    EXISTING depth instead.
///
/// # ONE FURTHER BOUND, DECLARED RATHER THAN IMPLIED
///  * **The MARSHAL half does not validate arity/dimension** — see
///    [`FREEZABLE_MARSHAL_SIMPLE_NAMES`], which is a head-CLASS lookup over names
///    a Cassandra WRITER produced, not a spelling proxy.
fn is_vector_spelling(name: &str) -> bool {
    let Some((head, rest)) = name.split_once('<') else {
        return false;
    };
    if !head.trim().eq_ignore_ascii_case("vector") {
        return false;
    }
    let Some(inner) = rest.strip_suffix('>') else {
        return false;
    };
    // An EMPTY argument is a CQL syntax error at whatever depth it occurs, and the
    // shared splitter FILTERS empty segments — so `vector<int, , 3>` would
    // otherwise split to two parts and pass. Refuse the shape directly: no legal
    // type string carries a leading, trailing or doubled comma at any depth, so
    // this cannot over-refuse.
    let squeezed: String = inner.chars().filter(|c| !c.is_whitespace()).collect();
    if squeezed.starts_with(',') || squeezed.ends_with(',') || squeezed.contains(",,") {
        return false;
    }
    // THE SHARED SPLITTER, reused rather than re-implemented: it is the parent
    // module's own `<>`-depth walk (and it already refuses unbalanced nesting), so
    // this predicate cannot form a second opinion about where an argument ends.
    let Ok(parts) = CqlType::split_top_level_types(inner) else {
        return false;
    };
    let [element, dimension] = parts[..] else {
        return false;
    };
    !element.trim().is_empty() && is_vector_dimension(dimension.trim())
}

/// Whether `d` is a dimension `Integer.parseInt` would accept and `VectorType`'s
/// constructor would keep: a bare digit string, inside Java `int`, strictly `> 0`.
///
/// `is_ascii_digit` FIRST, and it is load-bearing: `str::parse::<i32>` accepts a
/// leading `+` that `Lexer.g:337-339`'s `INTEGER` does not, and would accept the
/// `-` spellings `VectorType.java:89-90` throws on. Digits-only settles both, and
/// leaves `parse` to do only the one thing Java's does — enforce the `int` bound,
/// by returning `Err` instead of saturating.
fn is_vector_dimension(d: &str) -> bool {
    !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()) && d.parse::<i32>().is_ok_and(|n| n > 0)
}

/// The refusal a CQL-form `frozen<scalar>` earns, naming both the spelling that
/// was refused and the inner type that cannot be frozen.
pub(crate) fn refuse_frozen_scalar_cql(spelling: &str, inner: &str) -> Error {
    Error::schema(format!(
        "not declarable CQL: '{spelling}' freezes '{inner}', which is not a collection, \
         tuple, or user-defined type — {CITATION}"
    ))
}

/// The MARSHAL simple names whose `CQL3Type.Raw` counterpart overrides `freeze()`.
///
/// # `VectorType` is in this set, and here is the whole chain at the pinned tag
/// The one entry not evidenced by the corpus census, so it is derived end to end
/// rather than assumed — a permission Cassandra does not grant would be the same
/// no-heuristics defect as an invented decode result, pointed the other way:
///
///   1. `Parser.g:1916-1919` — `vector_type : K_VECTOR '<' comparatorType ','
///      INTEGER '>' { $vt = CQL3Type.Raw.vector(t1, ...); }`
///   2. `CQL3Type.java:705-708` — `public static Raw vector(..) { return new
///      RawVector(t, dimension); }`
///   3. `CQL3Type.java:885` — `private static class RawVector extends Raw`
///   4. `CQL3Type.java:915-919` — `@Override public Raw freeze() { return this; }`
///      — it DOES override, and it RETURNS rather than throws. (`:909-913`
///      `supportsFreezing() -> true`; `:897-901` `isVector() -> true`; base
///      `:632-635` `isImplicitlyFrozen() -> isTuple() || isVector()`.)
///   5. `Parser.g:1851` puts `vector_type` in `comparatorType`, and `:1853-1860`
///      routes `K_FROZEN '<' comparatorType '>'` through `freeze()` — so
///      `frozen<vector<float, 3>>` raises no `InvalidRequestException` and no
///      recognition error.
///
/// Conclusion: `frozen<vector<..>>` is declarable CQL and `FrozenType(VectorType(..))`
/// is a grammatical header type, even though no corpus file spells either.
///
/// # THIS IS A HEAD-CLASS LOOKUP AND CHECKS NO ARGUMENTS — deliberately
/// Unlike the CQL side's [`is_vector_spelling`], which must validate the whole
/// `vector<..>` spelling because the spelling is its only evidence that a `Custom`
/// IS a vector, these names arrive from a marshal string a Cassandra WRITER
/// produced, and the freezability question they answer is decided by the class
/// alone: `RawVector::freeze` returns `this` whatever the dimension. Validating a
/// `VectorType(..)`'s arity belongs to the marshal type parser, not to a
/// freezability gate. Stated so the asymmetry reads as a decision rather than an
/// oversight.
const FREEZABLE_MARSHAL_SIMPLE_NAMES: &[&str] = &[
    "ListType",
    "SetType",
    "MapType",
    "TupleType",
    "UserType",
    "VectorType",
    // An already-frozen inner: `FrozenType(FrozenType(SetType(..)))`.
    "FrozenType",
];

/// The canonical Cassandra marshal package. A `FrozenType` inner head must be
/// either a BARE simple name or this package's — the same package rule
/// `row_decoder::udt::marshal_name` enforces, and for the same reason: a
/// third-party `com.acme.SetType` is not Cassandra's `SetType` and must not be
/// read as if it were (#28, roborev job 76).
const MARSHAL_PACKAGE: &str = "org.apache.cassandra.db.marshal.";

/// Refuse a `Statistics.db` SerializationHeader type string that freezes a
/// non-freezable type, ANYWHERE inside it — fail-closed.
///
/// Scans EVERY `FrozenType(` occurrence rather than only a leading one, because a
/// header type string nests: the frozen wrapper can sit on a map key
/// (`MapType(FrozenType(Int32Type),Int32Type)`), on a collection element, or on a
/// UDT field (`UserType(ks,name,f:FrozenType(Int32Type))`) — and the UDT case is
/// exactly the one a leading-prefix check would miss, since
/// `convert_marshal_type_to_cql` returns a `UserType`-bearing string verbatim
/// without descending into it.
///
/// Fail-closed in every direction: an EMPTY inner (`FrozenType()`), an unbalanced
/// parenthesis, a non-canonical package and an unrecognised head are each refused
/// rather than allowed through.
///
/// # THE SCAN'S BOUND, STATED RATHER THAN IMPLIED
/// The marker is a plain substring search for `FrozenType(`, so it assumes the
/// marshal grammar in which a `(` follows only a CLASS NAME. That holds for
/// everything Cassandra writes: a keyspace or UDT name cannot contain `(`, and a
/// UDT's FIELD names are hex-encoded. It is NOT proven for a third-party CUSTOM
/// class free to put arbitrary text inside its own parameters — such a class would
/// have to embed the literal `FrozenType(` to be affected, and the outcome would be
/// a refusal (fail-closed), never a silent misread. The `FrozenType(` marker itself is matched
/// case-insensitively (as `extract_frozen_inner_type` does), and the inner head is
/// compared case-insensitively against the table above for the same reason: a
/// spelling this crate would not decode as `SetType` must not be admitted as one.
pub(crate) fn validate_marshal_frozen(marshal: &str) -> Result<()> {
    const MARKER: &str = "frozentype(";
    let lower = marshal.to_ascii_lowercase();
    let mut search_from = 0usize;
    while let Some(rel) = lower[search_from..].find(MARKER) {
        let open = search_from + rel + MARKER.len();
        // Advance past THIS marker whatever the outcome, so a nested
        // `FrozenType(FrozenType(..))` is inspected at both levels and the loop
        // always makes progress.
        search_from = open;
        let inner = balanced_inner(&marshal[open..]).ok_or_else(|| {
            Error::schema(format!(
                "unreadable SerializationHeader type '{marshal}': a FrozenType(…) has no \
                 matching close parenthesis, so what it freezes cannot be checked — {CITATION}"
            ))
        })?;
        let inner = inner.trim();
        if !marshal_head_supports_freezing(inner) {
            return Err(Error::schema(format!(
                "SerializationHeader type '{marshal}' is not writable by Cassandra: it spells \
                 FrozenType({inner}), and '{inner}' is not a collection, tuple, vector, or \
                 user-defined type — {CITATION}"
            )));
        }
    }
    Ok(())
}

/// The content of a parenthesised group whose opening `(` has already been
/// consumed, or `None` if the parentheses do not balance.
fn balanced_inner(after_open: &str) -> Option<&str> {
    let mut depth = 1usize;
    for (idx, ch) in after_open.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&after_open[..idx]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether a MARSHAL type string's head class overrides `freeze()`.
///
/// The leading `[` and `(` the header sometimes prefixes a comparator with are
/// stripped first, mirroring `convert_marshal_type_to_cql`'s own
/// `strip_wrapping_parens` (roborev jobs 43/48): a normalization one reader applies
/// and another does not is how two readers form two opinions about one string.
fn marshal_head_supports_freezing(inner: &str) -> bool {
    let inner = inner.trim().trim_start_matches(['[', '(']).trim_start();
    // The head is everything before the first `(` (a parameterised class) or the
    // whole string (a bare class name).
    let head = match inner.find('(') {
        Some(i) => &inner[..i],
        None => inner,
    }
    .trim();
    // THE PACKAGE RULE. A bare simple name, or the canonical package — nothing else.
    let simple = if let Some(rest) = head.strip_prefix(MARSHAL_PACKAGE) {
        rest
    } else if head.contains('.') {
        return false;
    } else {
        head
    };
    FREEZABLE_MARSHAL_SIMPLE_NAMES
        .iter()
        .any(|n| n.eq_ignore_ascii_case(simple))
}
