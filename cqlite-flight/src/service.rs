//! Arrow Flight gRPC service.
//!
//! Phase 1 implements the read path: `get_flight_info` / `get_schema` (Arrow
//! schema from the ticket DDL) and `do_get` (compaction-merge → Arrow stream).
//! The remaining Flight RPCs are intentionally unimplemented.
//!
//! Clients address a table by sending a [`FlightTicket`] JSON as the
//! `FlightDescriptor.cmd` (for `get_flight_info`/`get_schema`) or as the
//! `Ticket.ticket` bytes (for `do_get`).

// The Flight gRPC trait mandates `tonic::Status` as the error type, which is
// large; helpers returning `Result<_, Status>` mirror it so they compose with the
// trait methods. Boxing would only add churn at every call site.
#![allow(clippy::result_large_err)]

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::Stream;
use futures::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use cqlite_core::schema::{parse_cql_schema, TableSchema};

use crate::filter::{FilterError, ScanSpec};
use crate::producer::{DirSource, MergeProducer, ProducerError};
use crate::ticket::{FlightTicket, TicketError};

/// Boxed server response stream alias.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Map a ticket-decode failure to a client (`invalid_argument`) gRPC error so the
/// Java connector can distinguish bad input from server faults.
impl From<TicketError> for Status {
    fn from(e: TicketError) -> Self {
        Status::invalid_argument(e.to_string())
    }
}

/// Map a producer failure to the appropriate gRPC status code, preserving the
/// error's message (and its source chain via `thiserror` `Display`).
impl From<ProducerError> for Status {
    fn from(e: ProducerError) -> Self {
        let msg = e.to_string();
        match e {
            ProducerError::InvalidColumnType { .. } => Status::invalid_argument(msg),
            ProducerError::Discovery { source, .. }
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Status::not_found(msg)
            }
            ProducerError::Discovery { .. }
            | ProducerError::Merge(_)
            | ProducerError::Convert(_)
            | ProducerError::Predicate(_) => Status::internal(msg),
        }
    }
}

/// Bad filter input (unknown column, type mismatch, malformed operand) is a
/// client error.
impl From<FilterError> for Status {
    fn from(e: FilterError) -> Self {
        Status::invalid_argument(e.to_string())
    }
}

/// Flight service over a node-local SSTable data directory.
#[derive(Clone)]
pub struct CqliteFlightService {
    /// Root holding `<keyspace>/<table>[-<uuid>]/` SSTable directories.
    data_dir: PathBuf,
    /// Max rows per emitted Arrow record batch.
    batch_size: usize,
}

impl CqliteFlightService {
    /// Create a service serving SSTables under `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>, batch_size: usize) -> Self {
        Self {
            data_dir: data_dir.into(),
            batch_size: batch_size.max(1),
        }
    }

    /// Parse the table schema from the ticket's CQL DDL.
    fn parse_schema(ticket: &FlightTicket) -> Result<TableSchema, Status> {
        parse_cql_schema(&ticket.ddl)
            .map_err(|e| Status::invalid_argument(format!("invalid ddl: {e}")))
    }

    /// Build a producer for a ticket, applying its token-range/predicate/projection
    /// filters. Used by every RPC so the Arrow schema reflects the projection.
    fn build_producer(&self, ticket: &FlightTicket) -> Result<MergeProducer, Status> {
        let schema = Self::parse_schema(ticket)?;
        let spec = ScanSpec::from_ticket(ticket, &schema)?;
        Ok(MergeProducer::with_spec(schema, self.batch_size, spec)?)
    }

    /// Arrow schema for a ticket (no SSTable access required).
    fn arrow_schema_for(&self, ticket: &FlightTicket) -> Result<ArrowSchema, Status> {
        Ok(self.build_producer(ticket)?.arrow_schema()?)
    }
}

#[tonic::async_trait]
impl FlightService for CqliteFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket = FlightTicket::from_bytes(&descriptor.cmd)?;
        let arrow_schema = self.arrow_schema_for(&ticket)?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(descriptor.cmd.clone()));
        let info = FlightInfo::new()
            .try_with_schema(&arrow_schema)
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let ticket = FlightTicket::from_bytes(&descriptor.cmd)?;
        let arrow_schema = self.arrow_schema_for(&ticket)?;

        let options = IpcWriteOptions::default();
        let schema_result: SchemaResult = SchemaAsIpc::new(&arrow_schema, &options)
            .try_into()
            .map_err(|e: arrow::error::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = FlightTicket::from_bytes(&request.into_inner().ticket)?;
        let producer = self.build_producer(&ticket)?;
        let schema_ref = Arc::new(producer.arrow_schema()?);
        let source = DirSource::resolve(&self.data_dir, &ticket.keyspace, &ticket.table);

        // The merge drains SSTables into memory and is CPU-bound — run it off the
        // async runtime so it cannot stall the gRPC reactor. A missing table
        // directory surfaces as `not_found`; an existing table with no SSTables
        // yields an empty result (schema only).
        let batches = tokio::task::spawn_blocking(move || producer.produce(&source))
            .await
            .map_err(|e| Status::internal(format!("merge task panicked: {e}")))??;

        // `with_schema` emits the Arrow schema as the first Flight message even
        // when no record batches follow, so an empty result still carries the schema.
        let input = futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, FlightError>));
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema_ref)
            .build(input)
            .map(|res| res.map_err(|e| Status::internal(e.to_string())));

        Ok(Response::new(Box::pin(encoded)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake is not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not yet supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not supported"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put is not supported (read-only server)"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not supported"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions is not supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{build_sstables, simple_schema, total_rows, write_row, KS, SIMPLE_DDL, TBL};
    use arrow::array::Array;
    use arrow_flight::decode::FlightRecordBatchStream;

    fn ticket(keyspace: &str, table: &str) -> FlightTicket {
        FlightTicket {
            keyspace: keyspace.into(),
            table: table.into(),
            ddl: SIMPLE_DDL.into(),
            ..Default::default()
        }
    }

    fn cmd_descriptor(ticket: &FlightTicket) -> FlightDescriptor {
        FlightDescriptor::new_cmd(ticket.to_bytes().unwrap())
    }

    async fn decode(stream: <CqliteFlightService as FlightService>::DoGetStream) -> Vec<RecordBatch> {
        let mapped = stream.map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
        let mut rb = FlightRecordBatchStream::new_from_flight_data(mapped);
        let mut out = Vec::new();
        while let Some(batch) = rb.next().await {
            out.push(batch.expect("decode batch"));
        }
        out
    }

    // Plain `#[test]`: `build_sstables` drives its own runtime to flush, so we
    // construct the data set first, then enter a fresh runtime to drive `do_get`.
    // Using `#[tokio::test]` here would nest runtimes and panic.
    #[test]
    fn do_get_streams_merged_rows() {
        let schema = simple_schema();
        // SSTable A: id=1 (old). SSTable B: id=1 (new, wins) + id=2.
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "old", 1, 100)],
                vec![write_row(1, "new", 2, 200), write_row(2, "b", 3, 200)],
            ],
        );
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let batches = rt.block_on(async {
            let bytes = ticket(KS, TBL).to_bytes().unwrap();
            let resp = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("do_get");
            decode(resp.into_inner()).await
        });

        assert_eq!(total_rows(&batches), 2, "two partitions after LWW merge");
        let names = batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let values: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert!(values.contains(&"new"), "newer write wins, got {values:?}");
        assert!(!values.contains(&"old"));
    }

    #[tokio::test]
    async fn get_schema_returns_declared_columns() {
        // No SSTables needed — schema comes from the ticket DDL.
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let resp = svc
            .get_schema(Request::new(cmd_descriptor(&ticket(KS, TBL))))
            .await
            .expect("get_schema");
        let schema: ArrowSchema = (&resp.into_inner())
            .try_into()
            .expect("decode schema result");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "score"]);
    }

    #[tokio::test]
    async fn get_flight_info_carries_schema_and_endpoint() {
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let resp = svc
            .get_flight_info(Request::new(cmd_descriptor(&ticket(KS, TBL))))
            .await
            .expect("get_flight_info");
        let info = resp.into_inner();
        assert_eq!(info.endpoint.len(), 1, "one endpoint with the ticket");
        assert!(
            !info.endpoint[0].ticket.as_ref().unwrap().ticket.is_empty(),
            "endpoint carries a non-empty ticket"
        );
    }

    #[tokio::test]
    async fn invalid_ddl_is_invalid_argument() {
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let bad = FlightTicket {
            keyspace: KS.into(),
            table: TBL.into(),
            ddl: "this is not valid CQL".into(),
            ..Default::default()
        };
        let err = svc
            .get_schema(Request::new(cmd_descriptor(&bad)))
            .await
            .expect_err("invalid ddl must error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn do_get_missing_table_is_not_found() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) = build_sstables(&schema, vec![vec![write_row(1, "x", 1, 100)]]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let result = rt.block_on(async {
            let bytes = ticket(KS, "missing_table").to_bytes().unwrap();
            svc.do_get(Request::new(Ticket::new(bytes))).await
        });
        let err = match result {
            Ok(_) => panic!("missing table must error"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::NotFound, "got: {err:?}");
    }

    #[test]
    fn do_get_applies_predicate_pushdown() {
        use crate::ticket::{Predicate, PredicateOp};
        let schema = simple_schema();
        let rows = (1..=5)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100)) // scores 10..50
            .collect::<Vec<_>>();
        let (_temp, data_dir, _dir) = build_sstables(&schema, vec![rows]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let mut t = ticket(KS, TBL);
        t.predicates = vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Gte,
            value: serde_json::json!(40),
        }];

        let batches = rt.block_on(async {
            let resp = svc
                .do_get(Request::new(Ticket::new(t.to_bytes().unwrap())))
                .await
                .expect("do_get");
            decode(resp.into_inner()).await
        });
        // score >= 40 → 40, 50.
        assert_eq!(total_rows(&batches), 2);
    }

    #[tokio::test]
    async fn do_get_unknown_predicate_column_is_invalid_argument() {
        use crate::ticket::{Predicate, PredicateOp};
        let svc = CqliteFlightService::new(std::env::temp_dir(), 1024);
        let mut t = ticket(KS, TBL);
        t.predicates = vec![Predicate {
            column: "nonexistent".into(),
            op: PredicateOp::Equal,
            value: serde_json::json!(1),
        }];
        // get_schema also runs the ticket through build_producer → ScanSpec.
        let err = svc
            .get_schema(Request::new(cmd_descriptor(&t)))
            .await
            .expect_err("unknown predicate column must error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn do_get_empty_table_emits_schema_only() {
        // An existing table directory with no SSTables → valid empty result that
        // still carries the Arrow schema (via FlightDataEncoder `with_schema`).
        let temp = tempfile::TempDir::new().unwrap();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(data_dir.join(KS).join(TBL)).unwrap();
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let (schema, batches) = rt.block_on(async {
            let bytes = ticket(KS, TBL).to_bytes().unwrap();
            let resp = svc
                .do_get(Request::new(Ticket::new(bytes)))
                .await
                .expect("do_get");
            let mapped = resp
                .into_inner()
                .map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
            let mut rb = FlightRecordBatchStream::new_from_flight_data(mapped);
            let mut out = Vec::new();
            while let Some(b) = rb.next().await {
                out.push(b.expect("decode"));
            }
            let schema = rb.schema().cloned();
            (schema, out)
        });

        assert_eq!(total_rows(&batches), 0, "no rows for an empty table");
        let schema = schema.expect("schema must be present even when empty");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "score"]);
    }
}
