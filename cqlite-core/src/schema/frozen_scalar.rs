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
//! (`cassandra-5.0.8:src/antlr/Parser.g:1853-1859`):
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
//! Only four subclasses override it — `RawCollection` (`:777`, list/set/map),
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

use super::{is_udt_identifier, CqlType};
use crate::error::{Error, Result};

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
/// (`CQL3Type.java:777-786`) freezes an already-frozen collection to itself.
///
/// [`CqlType::Custom`] is CQLite's UDT-REFERENCE carrier: `CqlType::parse` has no
/// UDT registry, so a bare `frozen<address_type>` lands there and refusing it would
/// refuse every real frozen UDT. It is admitted ONLY when the name is a plausible
/// UDT identifier ([`is_udt_identifier`]), which keeps the quoted custom-class
/// spelling (`frozen<'org.apache.cassandra.db.marshal.Int32Type'>`) refused —
/// Cassandra routes that through `CQL3Type.Raw.from(new CQL3Type.Custom(..))`, i.e.
/// a `RawType`, i.e. the throwing base (`Parser.g:1861-1864`).
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
            is_udt_identifier(name.strip_prefix("udt:").unwrap_or(name.as_str()))
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
/// `VectorType` is included because `RawVector::freeze` (`CQL3Type.java:916-920`)
/// returns `this` — a vector is implicitly frozen (`isImplicitlyFrozen`,
/// `:632-635`) — so `FrozenType(VectorType(..))` is grammatical even though no
/// corpus file spells it.
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
/// rather than allowed through. The `FrozenType(` marker itself is matched
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
fn marshal_head_supports_freezing(inner: &str) -> bool {
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
