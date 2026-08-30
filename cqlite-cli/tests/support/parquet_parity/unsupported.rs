//! Representations the parity harness REFUSES to compare (issue #1490).
//!
//! # Why a refusal is a THIRD outcome, not a pass and not a defect
//!
//! A comparison harness has exactly one job: detect wrongness. So the one thing
//! it must never do is emit a verdict it did not measure. Two round-4 findings
//! were the same shape:
//!
//!   * a CQL `tuple` column: the sstabledump golden holds it as a JSON ARRAY
//!     (canonicalized to [`CanonicalValue::List`]) while the Arrow side decodes a
//!     `Struct` to [`CanonicalValue::Tuple`], so the two sides are in DIFFERENT
//!     representations. Comparing them says nothing about the export;
//!   * a UDT column: the Arrow TYPE expectation accepted ANY `Struct`, without
//!     checking the field types — so a UDT whose CQL `int` field was exported as
//!     `Int64` (exactly the mis-width family the type check exists for) passed.
//!
//! In both cases the harness was reporting a POSITIVE verdict about something it
//! had not measured — the vacuous-pass shape, sitting inside the thing whose
//! whole purpose is to catch it. The comparison outcome is therefore
//! THREE-VALUED: `equal` / `unequal` / **unsupported-representation**, the same
//! shape as #3473 replacing an unsupportable positive `REAPED` claim with an
//! explicit `UNKNOWN`.
//!
//! # A refusal FAILS the case. Deliberately.
//!
//! Every column in a [`super::ParityCase`] is DECLARED coverage: the case lists
//! it, its `covers:` line advertises it, and the run census counts its cells. So
//! a refusal on a declared column means the case claims to cover a
//! representation the harness cannot measure — the case is lying about its
//! coverage — and the fail-closed answer is to RED, naming the column and the
//! representation. The alternative (report-and-tolerate) was rejected: it would
//! leave a table's advertised coverage silently smaller than its declaration,
//! which is the invisible-gap failure mode the whole harness is built against.
//!
//! Consequences, all intended:
//!
//!   * a refused column is NOT counted among the compared cells (the census
//!     number shrinks, and that smaller number is the true one);
//!   * a refusal is NOT a [`super::KnownGap`]: a known gap says "this is a
//!     recorded PRODUCT defect that still reproduces", a refusal says "this
//!     HARNESS cannot represent this shape". There is deliberately no
//!     `ExpectedFailure::UnsupportedRepresentation`, so a refusal can never be
//!     recorded into a gap's expected failure set — it always shows up as an
//!     unrecorded extra and fails the case;
//!   * the way to clear one is to TEACH the harness the representation (or drop
//!     the column from the case), never to record it. That is a work item, which
//!     is the point.

#![allow(dead_code)]

use super::cql_type::CqlTypeSpec;

/// ONE representation the harness declines to compare, as a stable TOKEN plus
/// the reason.
///
/// `representation` is part of the failure's SIGNATURE, so it is a token and not
/// prose; `why` is diagnostic and deliberately excluded from the signature (the
/// identity of a refusal is which representation was refused, on which column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsupported {
    pub representation: &'static str,
    pub why: &'static str,
}

/// A UDT's Arrow `Struct` FIELD TYPES cannot be checked.
///
/// A [`super::ParityCase`] declares a UDT by NAME only — the value projection
/// never needs the field types, because sstabledump types the field values for
/// it — so there is no independently declared field schema to validate the
/// Struct's children against. The expectation used to be "an Arrow `Struct`,
/// fields unconstrained", which ACCEPTS a Struct whose CQL `int` field was
/// exported as `Int64`: a width defect the value comparison cannot see either
/// (canonicalization folds every integer width into one `Int`), so nothing in
/// the harness would have caught it.
///
/// The harness therefore refuses to claim the Struct validated. What it still
/// DOES measure is the negative: a UDT exported as anything that is NOT a
/// `Struct` (#3556's `Utf8` flattening) is a normal type MISMATCH, because that
/// is an affirmative measurement. Retiring this refusal means giving a case a
/// way to declare its UDTs' field types (copied from the committed
/// `test-data/schemas/*.cql`, never from CQLite's mapping — #3041) and
/// validating the Struct's children recursively.
pub const UDT_STRUCT_FIELD_TYPES: Unsupported = Unsupported {
    representation: "udt-struct-field-types",
    why: "a case declares a UDT by NAME only, so the harness has no independently declared \
          field schema to validate the Arrow Struct's field types against — and a UDT field \
          exported at the wrong width round-trips its VALUES unchanged, so accepting any \
          Struct would be a pass the harness never measured",
};

/// A CQL `tuple`'s VALUES cannot be compared.
///
/// sstabledump writes a tuple as a JSON ARRAY, which canonicalizes to
/// `CanonicalValue::List`; the Arrow side decodes the exported `Struct` to
/// `CanonicalValue::Tuple` (name/value pairs). The two sides are in different
/// canonical representations, so the comparison would be measuring the harness's
/// own projection rather than the export.
///
/// Deliberately NOT "fixed" by converting one side into the other: the tuple's
/// Arrow field NAMES (`field_0`, …) are a convention with no counterpart in the
/// golden's positional array, so a conversion has to invent the very facts the
/// comparison would then check. Retiring this refusal means deciding the
/// positional-vs-named question explicitly and asserting it against
/// Cassandra-written bytes (#3042), which is a work item and not a one-line
/// coercion.
pub const CQL_TUPLE_VALUES: Unsupported = Unsupported {
    representation: "cql-tuple-values",
    why: "the sstabledump golden holds a CQL tuple as a positional JSON array (canonical \
          List) while the exported Arrow Struct decodes to a canonical Tuple of named \
          fields — two different representations, so comparing them would measure the \
          harness's projection rather than the export",
};

/// The representation refusal that applies to a declared column's VALUES, if
/// any.
///
/// Keyed strictly on the DECLARED CQL type (never on an observed value or Arrow
/// type — that would be the byte-pattern guessing the no-heuristics mandate
/// forbids), and recursive: a `tuple` nested inside a collection is refused just
/// as a top-level one is, because the element values reach the same comparison.
///
/// NOTE the deliberate narrowness: an Arrow `Interval` duration also decodes to
/// a `CanonicalValue::Tuple`, and it is NOT refused — its declared type is the
/// scalar `duration`, both sides are reconciled onto the same (months, days,
/// nanos) triple by `spelling.rs`, and that IS a measured comparison. The
/// refusal is about the declared CQL `tuple` type only.
pub fn refused_value_representation(spec: &CqlTypeSpec) -> Option<Unsupported> {
    match spec {
        CqlTypeSpec::Tuple(_) => Some(CQL_TUPLE_VALUES),
        CqlTypeSpec::Seq { elem, .. } => refused_value_representation(elem),
        CqlTypeSpec::Map { key, value } => {
            refused_value_representation(key).or_else(|| refused_value_representation(value))
        }
        // A UDT's VALUES are comparable (sstabledump types the field values and
        // both sides land on a named Tuple); it is the Arrow TYPE claim that is
        // unmeasurable — see `UDT_STRUCT_FIELD_TYPES`.
        CqlTypeSpec::Udt(_) | CqlTypeSpec::Scalar(_) => None,
    }
}
