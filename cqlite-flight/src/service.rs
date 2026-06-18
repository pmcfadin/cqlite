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

use crate::producer::{DirSource, MergeProducer, SstableSource};
use crate::ticket::FlightTicket;

/// Boxed server response stream alias.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

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

    /// Build a producer from a parsed schema.
    fn make_producer(&self, schema: TableSchema) -> Result<MergeProducer, Status> {
        MergeProducer::new(schema, self.batch_size).map_err(|e| Status::internal(e.to_string()))
    }

    /// Resolve the directory holding a table's SSTables.
    ///
    /// Supports both the write-engine layout (`<data>/<ks>/<table>`) and the
    /// Cassandra layout (`<data>/<ks>/<table>-<uuid>`). Snapshot resolution is
    /// added in Phase 3.
    fn table_dir(&self, keyspace: &str, table: &str) -> PathBuf {
        let base = self.data_dir.join(keyspace);
        let exact = base.join(table);
        if exact.is_dir() {
            return exact;
        }
        if let Ok(entries) = std::fs::read_dir(&base) {
            let prefix = format!("{table}-");
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir()
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|n| n.starts_with(&prefix))
                {
                    return path;
                }
            }
        }
        exact
    }

    /// Arrow schema for a ticket (no SSTable access required).
    fn arrow_schema_for(&self, ticket: &FlightTicket) -> Result<ArrowSchema, Status> {
        let schema = Self::parse_schema(ticket)?;
        let producer = self.make_producer(schema)?;
        producer
            .arrow_schema()
            .map_err(|e| Status::internal(e.to_string()))
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
        let ticket = FlightTicket::from_bytes(&descriptor.cmd)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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
        let ticket = FlightTicket::from_bytes(&descriptor.cmd)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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
        let ticket = FlightTicket::from_bytes(&request.into_inner().ticket)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let schema = Self::parse_schema(&ticket)?;
        let producer = self.make_producer(schema)?;
        let schema_ref = Arc::new(
            producer
                .arrow_schema()
                .map_err(|e| Status::internal(e.to_string()))?,
        );
        let paths = DirSource::new(self.table_dir(&ticket.keyspace, &ticket.table))
            .data_paths()
            .map_err(|e| Status::internal(e.to_string()))?;

        // The merge drains SSTables into memory and is CPU-bound — run it off the
        // async runtime so it cannot stall the gRPC reactor.
        let mut batches = tokio::task::spawn_blocking(move || producer.produce_from_paths(paths))
            .await
            .map_err(|e| Status::internal(format!("merge task panicked: {e}")))?
            .map_err(|e| Status::internal(e.to_string()))?;

        // Always emit the schema, even when the result is empty.
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(schema_ref.clone()));
        }

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

    fn ticket(data_keyspace: &str, table: &str) -> FlightTicket {
        FlightTicket {
            keyspace: data_keyspace.into(),
            table: table.into(),
            ddl: SIMPLE_DDL.into(),
            snapshot: None,
            token_start: None,
            token_end: None,
            wraparound: false,
            columns: None,
            predicates: vec![],
        }
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
        let mut descriptor = FlightDescriptor::new_cmd(ticket(KS, TBL).to_bytes().unwrap());
        descriptor.r#type = arrow_flight::flight_descriptor::DescriptorType::Cmd as i32;

        let resp = svc
            .get_schema(Request::new(descriptor))
            .await
            .expect("get_schema");
        let schema: ArrowSchema = (&resp.into_inner())
            .try_into()
            .expect("decode schema result");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "score"]);
    }

    #[test]
    fn do_get_missing_table_errors_cleanly() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) = build_sstables(&schema, vec![vec![write_row(1, "x", 1, 100)]]);
        let svc = CqliteFlightService::new(data_dir, 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Point at a table with no SSTable dir → surfaces a Status, not a panic.
        let resp = rt.block_on(async {
            let bytes = ticket(KS, "missing_table").to_bytes().unwrap();
            svc.do_get(Request::new(Ticket::new(bytes))).await
        });
        assert!(resp.is_err(), "missing table dir surfaces an error, not a panic");
    }
}
