//! Issue #2339 (roborev F1): the EFFECTIVE UDT keyspace is ONE answer, shared by
//! the Arrow column metadata and merged-read reassembly.
//!
//! In-crate (not `tests/`) because `with_udt_keyspace` / `udt_scope` are
//! `pub(crate)`: the property under test is that those two seams agree, which the
//! public surface can only observe indirectly.
//!
//! The service chains `with_udt_keyspace` THEN `with_udt_registry`; these tests
//! pin that BOTH orders produce identical Arrow metadata, so a future reorder at
//! the call site cannot silently reintroduce the resolve-under-`"default"` defect
//! (the end-to-end, ticket-level assertion is
//! `tests/issue_2339_ticket_keyspace_udt_metadata.rs`).

use arrow::datatypes::DataType;
use cqlite_core::schema::{parse_cql_schema, udt_registry_from_cql};

use crate::filter::ScanSpec;
use crate::producer::MergeProducer;

/// An unqualified `CREATE TABLE` — `parse_cql_schema` gives it the placeholder
/// keyspace `"default"` — plus the `CREATE TYPE` the collection element needs.
const DDL: &str = "\
CREATE TABLE collections_with_udts (\
id int PRIMARY KEY, \
contacts set<frozen<contact_info>>); \
CREATE TYPE contact_info (email text, phone text);";

/// The keyspace the registry is built under: NOT the parsed schema's placeholder.
const KEYSPACE: &str = "test_collections";

fn base() -> MergeProducer {
    let schema = parse_cql_schema(DDL).expect("ticket DDL parses");
    assert_eq!(
        schema.keyspace, "default",
        "precondition: an unqualified CREATE TABLE parses to the placeholder keyspace, \
         which is what makes the two roles diverge"
    );
    MergeProducer::with_spec(schema, 64, ScanSpec::default()).expect("producer")
}

fn contacts_element(producer: &MergeProducer) -> DataType {
    let schema = producer.arrow_schema().expect("arrow schema");
    match schema
        .field_with_name("contacts")
        .expect("contacts column")
        .data_type()
    {
        DataType::List(inner) | DataType::LargeList(inner) => inner.data_type().clone(),
        other => panic!("contacts must be a LIST, got {other:?}"),
    }
}

/// The production order: establish the effective keyspace, then apply the registry.
#[test]
fn keyspace_then_registry_resolves_the_collection_element() {
    let producer = base()
        .with_udt_keyspace(KEYSPACE)
        .with_udt_registry(udt_registry_from_cql(DDL, KEYSPACE));
    assert!(
        matches!(contacts_element(&producer), DataType::Struct(_)),
        "the frozen UDT element must resolve to an Arrow Struct"
    );
    assert_eq!(
        producer.udt_scope().expect("scope").keyspace,
        KEYSPACE,
        "reassembly must resolve under the SAME keyspace the metadata did"
    );
}

/// The reversed order must produce the IDENTICAL Arrow metadata: applying the
/// registry first resolved against `schema.keyspace` (`"default"`), which found
/// nothing and left the element opaque until the keyspace was established.
#[test]
fn registry_then_keyspace_resolves_identically() {
    let production = base()
        .with_udt_keyspace(KEYSPACE)
        .with_udt_registry(udt_registry_from_cql(DDL, KEYSPACE));
    let reversed = base()
        .with_udt_registry(udt_registry_from_cql(DDL, KEYSPACE))
        .with_udt_keyspace(KEYSPACE);
    assert_eq!(
        contacts_element(&reversed),
        contacts_element(&production),
        "the resolved Arrow element type must not depend on builder call order"
    );
    assert_eq!(
        reversed.udt_scope().expect("scope").keyspace,
        KEYSPACE,
        "the established keyspace survives a later registry application"
    );
}

/// With NO keyspace established, resolution falls back to `schema.keyspace` — the
/// documented behaviour for direct `MergeProducer` callers that build schema and
/// registry under one keyspace. Here that placeholder legitimately misses, so the
/// element stays opaque: the fallback is preserved, not silently widened.
#[test]
fn without_an_established_keyspace_the_schema_keyspace_is_used() {
    let producer = base().with_udt_registry(udt_registry_from_cql(DDL, KEYSPACE));
    assert_eq!(
        producer.udt_scope().expect("scope").keyspace,
        "default",
        "no established keyspace ⇒ the parsed schema's own keyspace"
    );
    assert!(
        !matches!(contacts_element(&producer), DataType::Struct(_)),
        "a registry keyed under another keyspace must NOT resolve here — the \
         fallback is unchanged, and the composite path fails closed downstream"
    );
}

/// **Issue #3960 (roborev job 115) — order independence when the SAME UDT name
/// exists in BOTH keyspaces.**
///
/// The tests above cannot catch this, and the reason is the whole point. There the
/// schema keyspace is the placeholder `"default"`, which holds no `contact_info`, so
/// the registry-first resolution FAILS and leaves the element opaque — and a later
/// re-resolution therefore legitimately changes it. The defect needs the first
/// resolution to SUCCEED: once a column is bound to `ks_a`'s `contact_info`,
/// `UdtRegistry::resolve_type` is idempotent on that resolved node, so re-resolving
/// under `ks_b` is a NO-OP and the column keeps the WRONG definition. The Arrow
/// metadata then promises a type the reassembler does not produce.
///
/// The discriminator is the STRUCT FIELD COUNT: `ks_a`'s definition has one field,
/// `ks_b`'s has three, so a stale binding is unambiguous rather than a judgement call.
///
/// RED BEFORE THE FIX: the reversed order yielded 1 field instead of 3.
mod conflicting_same_named_udt {
    use super::*;

    const TABLE_DDL: &str = "\
CREATE TABLE ks_a.collections_with_udts (\
id int PRIMARY KEY, \
contacts set<frozen<contact_info>>);";

    /// ONE registry carrying a same-named UDT in two keyspaces with DIFFERENT shapes.
    const BOTH_KEYSPACES_DDL: &str = "\
CREATE TYPE ks_a.contact_info (email text); \
CREATE TYPE ks_b.contact_info (email text, phone text, note text);";

    const OTHER_KEYSPACE: &str = "ks_b";

    fn base_in_ks_a() -> MergeProducer {
        let schema = parse_cql_schema(TABLE_DDL).expect("qualified ticket DDL parses");
        assert_eq!(
            schema.keyspace, "ks_a",
            "precondition: the schema must resolve in a keyspace that DOES hold a \
             same-named UDT, or the first resolution fails and the defect is masked"
        );
        MergeProducer::with_spec(schema, 64, ScanSpec::default()).expect("producer")
    }

    fn element_field_count(producer: &MergeProducer) -> usize {
        match contacts_element(producer) {
            DataType::Struct(fields) => fields.len(),
            other => panic!("the frozen UDT element must resolve to a Struct, got {other:?}"),
        }
    }

    #[test]
    fn keyspace_then_registry_binds_the_selected_keyspaces_definition() {
        let producer = base_in_ks_a()
            .with_udt_keyspace(OTHER_KEYSPACE)
            .with_udt_registry(udt_registry_from_cql(BOTH_KEYSPACES_DDL, OTHER_KEYSPACE));
        assert_eq!(
            element_field_count(&producer),
            3,
            "keyspace-first must bind ks_b's 3-field contact_info"
        );
    }

    #[test]
    fn registry_then_keyspace_rebinds_instead_of_keeping_the_schema_keyspaces_definition() {
        let producer = base_in_ks_a()
            .with_udt_registry(udt_registry_from_cql(BOTH_KEYSPACES_DDL, OTHER_KEYSPACE))
            .with_udt_keyspace(OTHER_KEYSPACE);
        assert_eq!(
            element_field_count(&producer),
            3,
            "registry-first bound ks_a's 1-field contact_info; establishing ks_b MUST \
             re-bind to its 3-field definition. Keeping 1 is issue #3960: resolve_type \
             is idempotent on an already-resolved node, so the reset to the DECLARED \
             type is what makes re-resolution able to change the binding"
        );
    }

    /// Both orders must agree — the property the module header claims.
    #[test]
    fn both_orders_agree_on_the_conflicting_name() {
        let a = base_in_ks_a()
            .with_udt_keyspace(OTHER_KEYSPACE)
            .with_udt_registry(udt_registry_from_cql(BOTH_KEYSPACES_DDL, OTHER_KEYSPACE));
        let b = base_in_ks_a()
            .with_udt_registry(udt_registry_from_cql(BOTH_KEYSPACES_DDL, OTHER_KEYSPACE))
            .with_udt_keyspace(OTHER_KEYSPACE);
        assert_eq!(
            contacts_element(&a),
            contacts_element(&b),
            "call-order independence is documented on with_udt_keyspace; it must hold \
             for a name present in BOTH keyspaces, not only for one absent from the \
             schema keyspace"
        );
    }
}
