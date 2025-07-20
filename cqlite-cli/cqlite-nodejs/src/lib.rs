use napi::{
  bindgen_prelude::*,
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  Result as NapiResult,
};
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::Mutex;
use std::sync::Arc;

// Import our core cqlite functionality
use cqlite_core::{
  storage::sstable::SSTableReader as CoreSSTableReader,
  query::planner::QueryPlanner,
  schema::Schema,
  parser::cql::CQLParser,
};

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableOptions {
  pub schema: String,
  pub compression: Option<String>,
  pub cache_size: Option<u32>,
  pub enable_bloom_filter: Option<bool>,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptions {
  pub limit: Option<u32>,
  pub timeout: Option<u32>,
  pub streaming: Option<bool>,
  pub skip_bloom_filter: Option<bool>,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
  pub rows: Vec<HashMap<String, serde_json::Value>>,
  pub row_count: u32,
  pub execution_time: u32,
  pub stats: Option<QueryStats>,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
  pub blocks_read: u32,
  pub cache_hits: u32,
  pub cache_misses: u32,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
  pub table: String,
  pub columns: Vec<ColumnDefinition>,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
  pub name: String,
  pub r#type: String,
  pub primary_key: Option<bool>,
  pub clustering_key: Option<bool>,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableStats {
  pub file_size: u64,
  pub estimated_rows: u64,
  pub compression: String,
  pub bloom_filter_present: bool,
}

/// Main SSTable Reader class for NodeJS
#[napi]
pub struct SSTableReader {
  inner: Arc<Mutex<CoreSSTableReader>>,
  schema: Arc<Schema>,
  parser: Arc<CQLParser>,
  planner: Arc<QueryPlanner>,
  options: SSTableOptions,
}

#[napi]
impl SSTableReader {
  /// Create a new SSTable reader instance
  #[napi(constructor)]
  pub fn new(path: String, options: SSTableOptions) -> NapiResult<Self> {
    // Validate the SSTable file exists
    if !Path::new(&path).exists() {
      return Err(napi::Error::new(
        napi::Status::InvalidArg,
        format!("SSTable file not found: {}", path),
      ));
    }

    // Load schema from file
    let schema_path = &options.schema;
    if !Path::new(schema_path).exists() {
      return Err(napi::Error::new(
        napi::Status::InvalidArg,
        format!("Schema file not found: {}", schema_path),
      ));
    }

    // Initialize core components
    let schema = Arc::new(
      Schema::from_file(schema_path)
        .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?,
    );

    let core_reader = CoreSSTableReader::new(&path)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;

    let parser = Arc::new(CQLParser::new());
    let planner = Arc::new(QueryPlanner::new(schema.clone()));

    Ok(Self {
      inner: Arc::new(Mutex::new(core_reader)),
      schema,
      parser,
      planner,
      options,
    })
  }

  /// Execute a SELECT query on the SSTable
  #[napi]
  pub async fn query(&self, sql: String, options: Option<QueryOptions>) -> NapiResult<QueryResult> {
    let start_time = std::time::Instant::now();
    let query_opts = options.unwrap_or_default();

    // Parse the SQL query
    let parsed_query = self
      .parser
      .parse_select(&sql)
      .map_err(|e| napi::Error::new(napi::Status::InvalidArg, format!("Query parse error: {}", e)))?;

    // Validate it's a SELECT statement
    if !sql.trim().to_uppercase().starts_with("SELECT") {
      return Err(napi::Error::new(
        napi::Status::InvalidArg,
        "Only SELECT statements are supported".to_string(),
      ));
    }

    // Create execution plan
    let plan = self
      .planner
      .plan_query(&parsed_query)
      .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("Planning error: {}", e)))?;

    // Execute the query
    let mut reader = self.inner.lock().await;
    let mut results = Vec::new();
    let mut row_count = 0u32;
    let mut stats = QueryStats {
      blocks_read: 0,
      cache_hits: 0,
      cache_misses: 0,
    };

    // Apply limit if specified
    let limit = query_opts.limit.unwrap_or(u32::MAX);

    // Execute the plan and collect results
    match reader.execute_plan(&plan) {
      Ok(iterator) => {
        for result in iterator {
          if row_count >= limit {
            break;
          }

          match result {
            Ok(row) => {
              // Convert row to HashMap for JavaScript
              let mut js_row = HashMap::new();
              for (column, value) in row.columns() {
                js_row.insert(
                  column.clone(),
                  convert_cql_value_to_json(value)?,
                );
              }
              results.push(js_row);
              row_count += 1;
            }
            Err(e) => {
              return Err(napi::Error::new(
                napi::Status::GenericFailure,
                format!("Row processing error: {}", e),
              ));
            }
          }
        }
      }
      Err(e) => {
        return Err(napi::Error::new(
          napi::Status::GenericFailure,
          format!("Query execution error: {}", e),
        ));
      }
    }

    let execution_time = start_time.elapsed().as_millis() as u32;

    Ok(QueryResult {
      rows: results,
      row_count,
      execution_time,
      stats: Some(stats),
    })
  }

  /// Get schema information for the SSTable
  #[napi]
  pub async fn get_schema(&self) -> NapiResult<SchemaDefinition> {
    let table_name = self.schema.table_name().to_string();
    let mut columns = Vec::new();

    for column in self.schema.columns() {
      columns.push(ColumnDefinition {
        name: column.name().to_string(),
        r#type: column.cql_type().to_string(),
        primary_key: Some(column.is_primary_key()),
        clustering_key: Some(column.is_clustering_key()),
      });
    }

    Ok(SchemaDefinition {
      table: table_name,
      columns,
    })
  }

  /// Get statistics about the SSTable
  #[napi]
  pub async fn get_stats(&self) -> NapiResult<SSTableStats> {
    let reader = self.inner.lock().await;
    
    Ok(SSTableStats {
      file_size: reader.file_size(),
      estimated_rows: reader.estimated_row_count(),
      compression: reader.compression_algorithm().to_string(),
      bloom_filter_present: reader.has_bloom_filter(),
    })
  }

  /// Close the SSTable reader and free resources
  #[napi]
  pub async fn close(&self) -> NapiResult<()> {
    // The Rust reader will be automatically closed when dropped
    Ok(())
  }

  /// Create a streaming query iterator (internal method)
  #[napi]
  pub fn query_stream_internal(&self, sql: String, callback: JsFunction) -> NapiResult<()> {
    let tsfn: ThreadsafeFunction<HashMap<String, serde_json::Value>, ErrorStrategy::CalleeHandled> =
      callback.create_threadsafe_function(0, |ctx| {
        Ok(vec![ctx.env.create_object_from_hashmap(&ctx.value)?])
      })?;

    // Spawn async task for streaming
    let reader = self.inner.clone();
    let parser = self.parser.clone();
    let planner = self.planner.clone();

    tokio::spawn(async move {
      // Parse and execute query in streaming mode
      match parser.parse_select(&sql) {
        Ok(parsed_query) => {
          match planner.plan_query(&parsed_query) {
            Ok(plan) => {
              let mut reader_guard = reader.lock().await;
              match reader_guard.execute_plan(&plan) {
                Ok(iterator) => {
                  for result in iterator {
                    match result {
                      Ok(row) => {
                        let mut js_row = HashMap::new();
                        for (column, value) in row.columns() {
                          if let Ok(json_value) = convert_cql_value_to_json(value) {
                            js_row.insert(column.clone(), json_value);
                          }
                        }
                        
                        tsfn.call(js_row, ThreadsafeFunctionCallMode::NonBlocking);
                      }
                      Err(_) => break,
                    }
                  }
                }
                Err(_) => {}
              }
            }
            Err(_) => {}
          }
        }
        Err(_) => {}
      }
      
      // Signal end of stream
      tsfn.call(HashMap::new(), ThreadsafeFunctionCallMode::NonBlocking);
    });

    Ok(())
  }
}

/// Utility functions
#[napi]
pub fn validate_query(sql: String) -> NapiResult<bool> {
  let parser = CQLParser::new();
  match parser.parse_select(&sql) {
    Ok(_) => Ok(true),
    Err(e) => Err(napi::Error::new(
      napi::Status::InvalidArg,
      format!("Invalid query: {}", e),
    )),
  }
}

#[napi]
pub async fn parse_schema(schema_path: String) -> NapiResult<SchemaDefinition> {
  let schema = Schema::from_file(&schema_path)
    .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;

  let mut columns = Vec::new();
  for column in schema.columns() {
    columns.push(ColumnDefinition {
      name: column.name().to_string(),
      r#type: column.cql_type().to_string(),
      primary_key: Some(column.is_primary_key()),
      clustering_key: Some(column.is_clustering_key()),
    });
  }

  Ok(SchemaDefinition {
    table: schema.table_name().to_string(),
    columns,
  })
}

/// Error classes for JavaScript
#[napi]
pub fn create_cqlite_error(message: String, code: String) -> NapiResult<JsObject> {
  // This will be wrapped in JavaScript
  Ok(napi::Env::create_object()?)
}

impl Default for QueryOptions {
  fn default() -> Self {
    Self {
      limit: None,
      timeout: Some(30000), // 30 second default timeout
      streaming: Some(false),
      skip_bloom_filter: Some(false),
    }
  }
}

/// Helper function to convert CQL values to JSON
fn convert_cql_value_to_json(value: &cqlite_core::types::CQLValue) -> NapiResult<serde_json::Value> {
  use cqlite_core::types::CQLValue;
  
  match value {
    CQLValue::Text(s) => Ok(serde_json::Value::String(s.clone())),
    CQLValue::Int(i) => Ok(serde_json::Value::Number((*i).into())),
    CQLValue::BigInt(i) => Ok(serde_json::Value::Number((*i).into())),
    CQLValue::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
    CQLValue::Float(f) => Ok(serde_json::json!(*f)),
    CQLValue::Double(d) => Ok(serde_json::json!(*d)),
    CQLValue::Null => Ok(serde_json::Value::Null),
    CQLValue::List(items) => {
      let mut json_array = Vec::new();
      for item in items {
        json_array.push(convert_cql_value_to_json(item)?);
      }
      Ok(serde_json::Value::Array(json_array))
    }
    CQLValue::Set(items) => {
      let mut json_array = Vec::new();
      for item in items {
        json_array.push(convert_cql_value_to_json(item)?);
      }
      Ok(serde_json::Value::Array(json_array))
    }
    CQLValue::Map(map) => {
      let mut json_obj = serde_json::Map::new();
      for (key, value) in map {
        if let CQLValue::Text(key_str) = key {
          json_obj.insert(key_str.clone(), convert_cql_value_to_json(value)?);
        }
      }
      Ok(serde_json::Value::Object(json_obj))
    }
    _ => Ok(serde_json::Value::String(format!("{:?}", value))),
  }
}