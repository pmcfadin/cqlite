//! Issue #2339 (roborev F1): the ARROW METADATA a client is promised must resolve
//! UDT references under the SAME keyspace merged-read reassembly resolves them
//! under — the TICKET's keyspace, not the parsed schema's placeholder.
//!
//! ## The defect this pins
//!
//! `parse_cql_schema` gives an UNQUALIFIED `CREATE TABLE` — which is what every
//! connector ticket carries — the literal placeholder keyspace `"default"`, while
//! the ticket's `CREATE TYPE` statements are registered under the TICKET's
//! keyspace (`udt_registry_from_cql(&ticket.ddl, &ticket.keyspace)`). The service
//! applied the registry BEFORE establishing that effective keyspace, so
//! `with_udt_registry` resolved every column's `cql_type` against `"default"`,
//! found nothing, and left a `set<frozen<contact_info>>` element as an opaque
//! `Custom` → Arrow `Utf8`. Merged-read reassembly, which resolves through
//! `udt_scope` (the effective keyspace), meanwhile produced a STRUCTURED
//! `Value::Udt` — an Arrow schema/array disagreement, promised to the client by
//! `get_schema`/`get_flight_info` and read by the Trino connector.
//!
//! ## Why this is a SERVICE-level test
//!
//! The ordering bug lives in `CqliteFlightService::build_producer`, so it is only
//! observable through a ticket: a directly-built `MergeProducer` has one keyspace
//! for both roles and cannot express the divergence. The assertion therefore goes
//! through the public `get_schema` RPC — no SSTables required, since the Arrow
//! schema comes from the ticket DDL alone.

use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::FlightDescriptor;
use cqlite_flight::service::CqliteFlightService;
use tonic::Request;

/// A connector-shaped ticket DDL: UNQUALIFIED `CREATE TABLE` first (the schema
/// parser reads the table from the head of the statement list), then the
/// `CREATE TYPE` that makes the collection element resolvable.
const DDL: &str = "\
CREATE TABLE collections_with_udts (\
id int PRIMARY KEY, \
contacts set<frozen<contact_info>>); \
CREATE TYPE contact_info (email text, phone text, verified boolean);";

/// The ticket keyspace — deliberately NOT `"default"`, which is the placeholder
/// `parse_cql_schema` assigns an unqualified `CREATE TABLE`. The whole defect is
/// invisible when the two coincide.
const KEYSPACE: &str = "test_collections";

/// The on-the-wire ticket is JSON: `FlightTicket` is `#[non_exhaustive]` and only
/// constructible inside the crate, so build the bytes a connector would send.
fn descriptor(keyspace: &str) -> FlightDescriptor {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "keyspace": keyspace,
        "table": "collections_with_udts",
        "ddl": DDL,
    }))
    .expect("ticket json");
    FlightDescriptor::new_cmd(bytes)
}

async fn arrow_schema_for_keyspace(keyspace: &str) -> ArrowSchema {
    // No SSTables are read: `get_schema` derives the Arrow schema from the ticket
    // DDL, so a bare temp dir is a sufficient (and honest) data root.
    let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
    let resp = svc
        .get_schema(Request::new(descriptor(keyspace)))
        .await
        .expect("get_schema");
    (&resp.into_inner())
        .try_into()
        .expect("decode SchemaResult")
}

/// The `contacts set<frozen<contact_info>>` field must be a LIST of STRUCT
/// carrying the UDT's declared fields — proving the ticket keyspace, not the
/// `"default"` placeholder, was used to resolve the column's `cql_type`.
#[tokio::test]
async fn ticket_keyspace_resolves_udt_collection_metadata_to_struct() {
    let schema = arrow_schema_for_keyspace(KEYSPACE).await;
    let field = schema
        .field_with_name("contacts")
        .expect("contacts column present");

    let element = match field.data_type() {
        DataType::List(inner) | DataType::LargeList(inner) => inner.data_type().clone(),
        other => panic!(
            "contacts must be a LIST of the frozen UDT element, got {other:?} — \
             an opaque type here means the registry was applied before the \
             effective UDT keyspace was established (issue #2339 F1)"
        ),
    };

    let fields = match &element {
        DataType::Struct(fields) => fields.clone(),
        other => panic!(
            "contacts element must be an Arrow Struct (the resolved \
             frozen<contact_info>), got {other:?} — the UDT reference did not \
             resolve, i.e. the lookup ran under the \"default\" placeholder \
             keyspace instead of the ticket's \"{KEYSPACE}\" (issue #2339 F1)"
        ),
    };

    let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["email", "phone", "verified"],
        "the struct must carry contact_info's DECLARED fields, in declaration order"
    );
}

/// The control: the SAME DDL under the `"default"` keyspace — where the ticket
/// keyspace and the parsed schema's placeholder coincide — resolves identically.
/// So the assertion above is about the ticket keyspace being HONOURED, not about
/// one keyspace happening to work; a fix that hard-coded `"default"` would pass
/// this one and fail the other.
#[tokio::test]
async fn default_keyspace_ticket_resolves_the_same_metadata() {
    let qualified = arrow_schema_for_keyspace(KEYSPACE).await;
    let placeholder = arrow_schema_for_keyspace("default").await;
    assert_eq!(
        qualified
            .field_with_name("contacts")
            .expect("contacts")
            .data_type(),
        placeholder
            .field_with_name("contacts")
            .expect("contacts")
            .data_type(),
        "the resolved Arrow type must not depend on which keyspace the ticket names"
    );
}
