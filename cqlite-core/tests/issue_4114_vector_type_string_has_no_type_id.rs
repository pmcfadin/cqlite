//! Issue #4114, blocker 3 (found by the lead while auditing the defect class): the
//! STRING-based `schema::cql_type_to_type_id` must not answer `CqlTypeId::Udt` for a
//! vector type string.
//!
//! # The defect this file pins
//!
//! `schema::cql_parser::cql_type_to_type_id(&str)` handles `list<`, `set<`, `map<`,
//! `tuple<` and `frozen<`, but had no `vector<` arm — so `"vector<float, 3>"` fell
//! through the primitive match to `_ => Ok(CqlTypeId::Udt)` ("assume it's a UDT").
//!
//! That is worse than a blob: `CqlTypeId::Udt` routes a value to
//! `parse_udt_enhanced`, which reads a UDT STRUCTURE over what are actually `4 * n`
//! raw big-endian binary32 bytes (`VectorType.java:94-96`, pinned `cassandra-5.0.8`:
//! no length prefix, no element count). It is also a #28 no-heuristics violation — a
//! type inferred from the ABSENCE of a match — and contradicts AC4, which requires a
//! named refusal for what is out of scope rather than a fallback decode.
//!
//! # Reach — stated accurately, and NOT overclaimed
//!
//! This is a PUBLIC-API boundary. `SchemaManager::cql_type_to_internal`
//! (`schema/mod.rs`, `pub fn`) delegates straight to it, and the function itself is
//! re-exported as `cqlite_core::schema::cql_type_to_type_id`. NO internal read-path
//! caller of either was found, so the demonstrated exposure is API consumers
//! (bindings, embedders) — NOT the measured `read-sstable` path, which decodes
//! vectors through `CqlType`/`vector_type` and never through this function.
//!
//! The test calls the re-exported function directly rather than
//! `SchemaManager::cql_type_to_internal`, because that method is `async` and
//! constructs a `StorageEngine`; its body is a single delegating call to exactly the
//! function under test here.
//!
//! # Why a refusal rather than `CqlTypeId::Custom`
//!
//! The sibling `CqlType`-taking `parser::types::cql_type_to_type_id` maps a vector to
//! `CqlTypeId::Custom` and relies on its callers intercepting `CqlType::Vector`
//! first — all three of them do. That option does not exist for THIS function: its
//! input is a string and its only output is the id, so a caller handed `Custom` has
//! nothing left to intercept — the element type and dimension are already gone.

use cqlite_core::parser::types::CqlTypeId;
use cqlite_core::schema::cql_type_to_type_id;

/// Every parameterised vector spelling — well-formed or malformed — is refused, and
/// the refusal names the type. Before the fix each of these returned
/// `Ok(CqlTypeId::Udt)`.
#[test]
fn a_vector_type_string_is_refused_and_never_answers_udt() {
    for type_str in [
        "vector<float, 3>",
        // CQL type keywords are case-insensitive.
        "VECTOR<FLOAT,3>",
        // A vector is always frozen; the `frozen<...>` arm recurses into it.
        "frozen<vector<float, 3>>",
        // Element types outside #4114's scope have no type id either.
        "vector<double, 2>",
        // Malformed: unterminated parameters. Not a type id, and not a UDT.
        "vector<float, 3",
        // Malformed: a dimension Cassandra rejects at construction
        // (`VectorType.java:89-90` — "vectors may only have positive dimensions").
        "vector<float, 0>",
    ] {
        let result = cql_type_to_type_id(type_str);
        assert!(
            result.is_err(),
            "'{type_str}' must be REFUSED, got {result:?} — `Udt` in particular would \
             read 4*n raw float bytes as a UDT structure"
        );
        let message = match result {
            Err(e) => e.to_string(),
            Ok(_) => unreachable!("asserted err above"),
        };
        assert!(
            message.contains("vector"),
            "the refusal must name what it refused: {message}"
        );
    }
}

/// The boundary the refusal must NOT cross: `vector` is not a reserved word in CQL,
/// so an UNPARAMETERISED `vector` may be a genuine UDT name and must still resolve
/// as one. This is the single case where the "assume it's a UDT" fallback is right.
#[test]
fn an_unparameterised_vector_is_still_a_udt_name() {
    for type_str in ["vector", "vector_column", "my_vector"] {
        assert_eq!(
            cql_type_to_type_id(type_str).expect("a UDT name must resolve"),
            CqlTypeId::Udt,
            "'{type_str}' is a plausible UDT name, not a vector type"
        );
    }
}

/// The refusal is scoped to vectors: the sibling type strings this function already
/// handled must be unaffected by the new arm.
#[test]
fn the_vector_arm_does_not_disturb_the_sibling_type_strings() {
    for (type_str, expected) in [
        ("text", CqlTypeId::Varchar),
        ("float", CqlTypeId::Float),
        ("list<float>", CqlTypeId::List),
        ("set<uuid>", CqlTypeId::Set),
        ("map<text, int>", CqlTypeId::Map),
        ("tuple<int, text>", CqlTypeId::Tuple),
        ("frozen<set<uuid>>", CqlTypeId::Set),
    ] {
        assert_eq!(
            cql_type_to_type_id(type_str).unwrap_or_else(|e| panic!("'{type_str}': {e}")),
            expected
        );
    }
}
