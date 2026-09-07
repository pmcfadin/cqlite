//! `frozen<T>` IS NOT DECLARABLE OVER A SCALAR — the one rule, both spellings
//! (issue #4104).
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
///   * a VECTOR. `CqlType` has no `Vector` variant, so `vector<float, 3>` also
///     lands in `Custom` — and `RawVector` DOES override `freeze()`, returning
///     `this` rather than throwing (`CQL3Type.java:915-919`), so
///     `frozen<vector<float, 3>>` IS declarable CQL. Without this arm the gate
///     would refuse declarable CQL, which is why the marshal half's allowlist
///     naming `VectorType` is not enough on its own: the two spellings are one
///     rule. Full derivation, factory to grammar, at
///     [`FREEZABLE_MARSHAL_SIMPLE_NAMES`].
///
///     CQLite does not DECODE vectors, and this arm does not claim it does — the
///     type still lands in `Custom`, exactly as before #4104. A metadata gate's
///     job is to refuse what Cassandra cannot have written, never to narrow what
///     it can.
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

/// Whether a `Custom` type name is CQL's `vector<element, dimension>` spelling.
///
/// Matched on the HEAD keyword only, case-insensitively, exactly as
/// `CqlType::parse` matches `list<`/`set<`/`map<`/`tuple<`. Deciding freezability
/// needs nothing more: the element and dimension are the vector's business, and a
/// vector is freezable whatever they are.
fn is_vector_spelling(name: &str) -> bool {
    name.split_once('<')
        .is_some_and(|(head, _)| head.trim().eq_ignore_ascii_case("vector"))
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

#[cfg(test)]
#[path = "frozen_scalar_tests.rs"]
mod frozen_scalar_tests;
