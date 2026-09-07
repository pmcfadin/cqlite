//! Which COMPONENT of a multicell collection's declared type a CELL PATH holds,
//! and the ONE resolver that names it — shared by the READ and WRITE sides
//! (issues #3805, #4106).
//!
//! # Why this is one module and not two
//! Cassandra decides both collection kinds with ONE line. At `cassandra-5.0.8`,
//! `schema/ColumnMetadata.java:457-467` validates every collection cell path
//! with `((CollectionType)type).nameComparator().validate(path.get(0))`, and
//! `nameComparator()` is the KEYS type of a `MapType` (`db/marshal/MapType.java`)
//! and the ELEMENTS type of a `SetType` (`db/marshal/SetType.java:101-104`). So
//! "which component does the cell path hold" is one question with two answers,
//! and both the writer's admission gate
//! (`writer::data_writer::cell_path::admit_empty_cell_path`) and the reader's
//! (`reader::parsing::row_decoder::complex_column::cell_path_empty`) need it.
//!
//! It lived only on the WRITE side until #4106 roborev job 449, and that
//! asymmetry was the finding — see [`resolve_declared_cell_path_type`]'s
//! "THE SEAM" section for the exact divergence and its cause.

use crate::schema::CqlType;
use crate::storage::sstable::reader::parsing::row_decoder::V5CompressedLegacyParser;

/// Which component of a multicell collection's declared type a cell path holds.
///
/// The two are the same question — Cassandra answers both with
/// `nameComparator()` (see the module header) — so they share ONE resolver
/// rather than two near-identical ones able to drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CellPathComponent {
    /// A `map<K,V>`'s KEY (`MapType.nameComparator()` == the keys type).
    MapKey,
    /// A `set<T>`'s ELEMENT (`SetType.nameComparator()` == the elements type).
    SetElement,
}

impl CellPathComponent {
    /// The two declared-type spellings this component is resolvable from, for a
    /// diagnostic — the message tells the caller exactly what would have worked.
    ///
    /// Gated to match its callers, NOT `allow(dead_code)`. Its only caller is
    /// `writer::data_writer::cell_path`, and `pub mod writer`
    /// (`storage/sstable/mod.rs`) is itself `#[cfg(feature = "write-support")]`.
    /// `write-support` is a DEFAULT feature, so it survives every ordinary build
    /// and vanishes only under `--no-default-features` — which is exactly the
    /// gate's `feature-iso-parquet` lane
    /// (`--no-default-features --features all-compression,parquet`), where the
    /// sole caller disappears and this becomes dead code under `-D warnings`.
    /// An `allow(dead_code)` would silence the signal instead of expressing the
    /// invariant: this diagnostic exists only where something can write.
    #[cfg(feature = "write-support")]
    pub(crate) fn declared_shapes(self) -> &'static str {
        match self {
            CellPathComponent::MapKey => {
                "the CQL spelling `map<K,V>` nor the Cassandra marshal one \
                 `org.apache.cassandra.db.marshal.MapType(K,V)`"
            }
            CellPathComponent::SetElement => {
                "the CQL spelling `set<T>` nor the Cassandra marshal one \
                 `org.apache.cassandra.db.marshal.SetType(T)`"
            }
        }
    }
}

/// The DECLARED type of the cell-path component of a multicell collection
/// column, from the column's COMPLETE declared type string in EITHER spelling —
/// `None` when the string denotes no collection of that kind at all.
///
/// # NEITHER resolution is written here (one fact, one parser)
///  * the CQL spelling is [`CqlType::parse`];
///  * the marshal spelling is
///    [`V5CompressedLegacyParser::parse_cassandra_type`], whose name table is
///    derived arm-by-arm from `cql3/CQL3Type.java`'s `Native` enum at
///    `cassandra-5.0.8` and which enforces the marshal PACKAGE rule (a
///    third-party `com.acme.Int32Type` is refused, not read as `int`).
///
/// The string->string `convert_marshal_type_to_cql` in
/// `parser::enhanced_statistics_parser` is deliberately NOT used: it maps
/// `IntegerType` to `int` where Cassandra binds it to `varint`, and a
/// one-argument `MapType(V)` to `map<text, V>`, so reusing it would decide an
/// admission against a component type Cassandra does not agree with.
///
/// # THE SEAM this function exists to close — a COMPLETE type, never a COMPONENT
/// # NAME (issue #4106, roborev job 449 finding B1)
///
/// Two reviewers reported adjacent facts that look contradictory and are not.
/// The marshal TABLE is shared — `native_marshal_to_cql_type` is the crate's one
/// marshal-name authority and `primitive_marshal_to_cql_short` is a projection of
/// it — but the ENTRY CONDITION differs, and that is where read and write
/// diverged:
///
///  * every consumer that receives an ISOLATED COMPONENT NAME consults the table
///    only when the string already `contains("org.apache.cassandra.db.marshal.")`
///    — `cell_path_key_cql_type` and the value decoder
///    (`raw_value/reporting.rs`) both spell it that way;
///  * this function receives the COMPLETE DECLARED TYPE, so it may hand the whole
///    string to `parse_cassandra_type`, which KNOWS its input is a marshal type
///    string and therefore applies Cassandra's own `TypeParser` rule
///    (`TypeParser.java:450`: inside a marshal type string an UNQUALIFIED name IS
///    a class in the marshal package).
///
/// The divergence is CONTEXT, not table membership. `org.apache.cassandra.db.
/// marshal.SetType(Int32Type)` — package on the OUTER name, bare inner element
/// name — is a legal `TypeParser` spelling that the WRITER accepted (it resolves
/// the component from this complete string) while the READER refused: it split
/// `Int32Type` out first, and by then the string had LOST the fact that it came
/// from a marshal type string, so the `contains(package)` guard declined and the
/// empty cell path decoded as an opaque blob instead of `Value::Empty(Int)`.
///
/// The fix is to resolve from the COMPLETE type, restoring the context the split
/// discarded. **Synthesising the package onto a bare component name is NOT the
/// fix and must never be reintroduced**: #3612 roborev round 9 finding 2 removed
/// exactly that, because `format!("org.apache.cassandra.db.marshal.{t}")` made a
/// foreign `com.acme.CustomBytesType` match `BytesType` and suppressed the
/// diagnostic that exists for unmodelled custom types — and deciding type
/// identity from a name suffix is name-pattern inference that #28 forbids. That
/// rule has a committed regression suite
/// (`row_decoder::udt::regression_3631_marshal_package_rule_tests`).
///
/// # What stays refused, on purpose
/// A `FrozenType(MapType(K,V))` / `FrozenType(SetType(T))` resolves to
/// [`CqlType::Frozen`] and NOT to [`CqlType::Map`] / [`CqlType::Set`], so it is
/// refused here: a frozen collection is ONE inline length-prefixed cell with no
/// CellPath at all, so its empty component is the inline-element case owned by
/// `require_fixed_width` (#3847/#4071), not this one. A non-collection
/// declaration, a foreign-package class, and a spelling neither parser models
/// are all refused for the reason this function exists.
pub(crate) fn resolve_declared_cell_path_type(
    declared: &str,
    component: CellPathComponent,
) -> Option<CqlType> {
    fn pick(ty: CqlType, component: CellPathComponent) -> Option<CqlType> {
        match (ty, component) {
            (CqlType::Map(key, _), CellPathComponent::MapKey) => Some(*key),
            (CqlType::Set(element), CellPathComponent::SetElement) => Some(*element),
            _ => None,
        }
    }
    if let Ok(ty) = CqlType::parse(declared) {
        if let Some(resolved) = pick(ty, component) {
            return Some(resolved);
        }
    }
    match V5CompressedLegacyParser::parse_cassandra_type(declared) {
        Ok(ty) => pick(ty, component),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE B1 CASE (roborev job 449): package on the OUTER name, BARE inner
    /// element/key name — a legal `TypeParser` spelling
    /// (`TypeParser.java:450`) that the component-name classifiers cannot
    /// resolve and this one can, because it still has the marshal CONTEXT.
    #[test]
    fn a_bare_inner_marshal_name_resolves_from_the_complete_declared_type() {
        assert_eq!(
            resolve_declared_cell_path_type(
                "org.apache.cassandra.db.marshal.SetType(Int32Type)",
                CellPathComponent::SetElement
            ),
            Some(CqlType::Int)
        );
        assert_eq!(
            resolve_declared_cell_path_type(
                "org.apache.cassandra.db.marshal.MapType(Int32Type,Int32Type)",
                CellPathComponent::MapKey
            ),
            Some(CqlType::Int)
        );
    }

    #[test]
    fn both_spellings_resolve_and_the_component_kinds_do_not_cross() {
        for (declared, expected) in [
            ("set<int>", CqlType::Int),
            (
                "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)",
                CqlType::Int,
            ),
            ("set<decimal>", CqlType::Decimal),
        ] {
            assert_eq!(
                resolve_declared_cell_path_type(declared, CellPathComponent::SetElement),
                Some(expected.clone()),
                "{declared} as a SET element"
            );
            // A set declaration has no MAP KEY, and vice versa.
            assert_eq!(
                resolve_declared_cell_path_type(declared, CellPathComponent::MapKey),
                None,
                "{declared} must not resolve a MAP KEY"
            );
        }
        assert_eq!(
            resolve_declared_cell_path_type("map<bigint,text>", CellPathComponent::MapKey),
            Some(CqlType::BigInt)
        );
        assert_eq!(
            resolve_declared_cell_path_type("map<bigint,text>", CellPathComponent::SetElement),
            None
        );
    }

    /// THE PACKAGE RULE SURVIVES the widening: a foreign inner class is still
    /// refused rather than read as the marshal type it resembles (#3631 roborev
    /// job 76 / round 9 finding 2, #28).
    #[test]
    fn a_foreign_package_inner_name_is_refused_not_resolved() {
        for declared in [
            "org.apache.cassandra.db.marshal.SetType(com.acme.Int32Type)",
            "org.apache.cassandra.db.marshal.SetType(notorg.apache.cassandra.db.marshal.Int32Type)",
            "com.acme.SetType(org.apache.cassandra.db.marshal.Int32Type)",
        ] {
            assert_eq!(
                resolve_declared_cell_path_type(declared, CellPathComponent::SetElement),
                None,
                "{declared} must NOT resolve to a marshal type"
            );
        }
    }

    /// A FROZEN collection has no CellPath at all, so it resolves to no
    /// component here — its empty component is the inline case owned by
    /// `require_fixed_width` (#3847/#4071).
    #[test]
    fn frozen_and_non_collection_declarations_resolve_to_nothing() {
        for declared in [
            "frozen<set<int>>",
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type))",
            "int",
            "list<int>",
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
            "",
        ] {
            assert_eq!(
                resolve_declared_cell_path_type(declared, CellPathComponent::SetElement),
                None,
                "{declared} must resolve no SET element"
            );
            assert_eq!(
                resolve_declared_cell_path_type(declared, CellPathComponent::MapKey),
                None,
                "{declared} must resolve no MAP key"
            );
        }
    }
}
