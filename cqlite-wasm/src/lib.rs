//! WASM bindings for CQLite
//! 
//! This module provides JavaScript-compatible bindings for the CQLite database engine,
//! enabling use in web browsers and Node.js environments.

#![deny(missing_docs)]

mod utils;
mod database;
mod query;
mod types;
mod storage;

use wasm_bindgen::prelude::*;

// Import the `console.log` function from the Web API
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

// Define a macro for easier console logging
macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

// Use `wee_alloc` as the global allocator if feature is enabled
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

/// Initialize the CQLite WASM module
/// 
/// This function should be called once when the module is loaded.
/// It sets up panic handling and other global state.
#[wasm_bindgen(start)]
pub fn init() {
    // Set up better panic messages in debug builds
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
    
    console_log!("CQLite WASM module initialized");
}

/// Get the version of CQLite
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Check if CQLite features are available in this environment
#[wasm_bindgen]
pub fn check_features() -> JsValue {
    let features = utils::detect_features();
    serde_wasm_bindgen::to_value(&features).unwrap_or(JsValue::NULL)
}

/// CQLite database configuration for WASM
#[wasm_bindgen]
#[derive(Clone)]
pub struct WasmConfig {
    config: cqlite_core::Config,
}

#[wasm_bindgen]
impl WasmConfig {
    /// Create a new configuration with default values
    #[wasm_bindgen(constructor)]
    pub fn new() -> WasmConfig {
        Self {
            config: cqlite_core::Config::wasm_optimized(),
        }
    }

    /// Create configuration optimized for memory usage
    #[wasm_bindgen]
    pub fn memory_optimized() -> WasmConfig {
        Self {
            config: cqlite_core::Config::memory_optimized(),
        }
    }

    /// Set maximum memory usage in bytes
    #[wasm_bindgen]
    pub fn set_max_memory(&mut self, bytes: u64) {
        self.config.memory.max_memory = bytes;
        #[cfg(target_arch = "wasm32")]
        {
            self.config.wasm.max_memory = bytes;
        }
    }

    /// Enable or disable IndexedDB storage
    #[wasm_bindgen]
    pub fn set_use_indexeddb(&mut self, enabled: bool) {
        #[cfg(target_arch = "wasm32")]
        {
            self.config.wasm.use_indexeddb = enabled;
        }
    }

    /// Enable or disable SIMD optimizations
    #[wasm_bindgen]
    pub fn set_enable_simd(&mut self, enabled: bool) {
        #[cfg(target_arch = "wasm32")]
        {
            self.config.wasm.enable_simd = enabled;
        }
    }

    /// Enable or disable Web Workers
    #[wasm_bindgen]
    pub fn set_enable_workers(&mut self, enabled: bool) {
        #[cfg(target_arch = "wasm32")]
        {
            self.config.wasm.enable_workers = enabled;
        }
    }

    /// Set maximum number of Web Workers
    #[wasm_bindgen]
    pub fn set_max_workers(&mut self, count: usize) {
        #[cfg(target_arch = "wasm32")]
        {
            self.config.wasm.max_workers = count;
        }
    }

    /// Convert to JSON string
    #[wasm_bindgen]
    pub fn to_json(&self) -> Result<String, JsValue> {
        serde_json::to_string_pretty(&self.config)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Create from JSON string
    #[wasm_bindgen]
    pub fn from_json(json: &str) -> Result<WasmConfig, JsValue> {
        let config: cqlite_core::Config = serde_json::from_str(json)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(Self { config })
    }
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// CQLite database handle for WASM
#[wasm_bindgen]
pub struct CQLiteDB {
    database: database::WasmDatabase,
}

#[wasm_bindgen]
impl CQLiteDB {
    /// Create a new database instance
    /// 
    /// # Arguments
    /// 
    /// * `name` - Database name (used for storage identification)
    /// * `config` - Optional configuration (uses default if not provided)
    #[wasm_bindgen(constructor)]
    pub fn new(name: String, config: Option<WasmConfig>) -> CQLiteDB {
        let config = config.unwrap_or_default();
        let database = database::WasmDatabase::new(name, config.config);
        
        Self { database }
    }

    /// Open the database
    /// 
    /// This is an async operation that initializes the database storage.
    #[wasm_bindgen]
    pub async fn open(&mut self) -> Result<(), JsValue> {
        self.database.open().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Execute a SQL statement
    /// 
    /// # Arguments
    /// 
    /// * `sql` - SQL statement to execute
    /// 
    /// # Returns
    /// 
    /// A Promise that resolves to the query result
    #[wasm_bindgen]
    pub async fn execute(&self, sql: String) -> Result<JsValue, JsValue> {
        let result = self.database.execute(&sql).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Prepare a SQL statement
    /// 
    /// # Arguments
    /// 
    /// * `sql` - SQL statement to prepare
    /// 
    /// # Returns
    /// 
    /// A prepared statement handle
    #[wasm_bindgen]
    pub async fn prepare(&self, sql: String) -> Result<PreparedStatement, JsValue> {
        let stmt = self.database.prepare(&sql).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(PreparedStatement { statement: stmt })
    }

    /// Insert data into a table
    /// 
    /// # Arguments
    /// 
    /// * `table` - Table name
    /// * `data` - Data object to insert
    #[wasm_bindgen]
    pub async fn insert(&self, table: String, data: JsValue) -> Result<JsValue, JsValue> {
        let result = self.database.insert(&table, data).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Select data from a table
    /// 
    /// # Arguments
    /// 
    /// * `table` - Table name
    /// * `conditions` - Optional conditions object
    /// * `limit` - Optional result limit
    #[wasm_bindgen]
    pub async fn select(
        &self,
        table: String,
        conditions: Option<JsValue>,
        limit: Option<u32>,
    ) -> Result<JsValue, JsValue> {
        let result = self.database.select(&table, conditions, limit).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Update data in a table
    /// 
    /// # Arguments
    /// 
    /// * `table` - Table name
    /// * `data` - Data to update
    /// * `conditions` - Conditions for which rows to update
    #[wasm_bindgen]
    pub async fn update(
        &self,
        table: String,
        data: JsValue,
        conditions: JsValue,
    ) -> Result<JsValue, JsValue> {
        let result = self.database.update(&table, data, conditions).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Delete data from a table
    /// 
    /// # Arguments
    /// 
    /// * `table` - Table name
    /// * `conditions` - Conditions for which rows to delete
    #[wasm_bindgen]
    pub async fn delete(&self, table: String, conditions: JsValue) -> Result<JsValue, JsValue> {
        let result = self.database.delete(&table, conditions).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Create a table
    /// 
    /// # Arguments
    /// 
    /// * `name` - Table name
    /// * `schema` - Table schema definition
    #[wasm_bindgen]
    pub async fn create_table(&self, name: String, schema: JsValue) -> Result<(), JsValue> {
        self.database.create_table(&name, schema).await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Drop a table
    /// 
    /// # Arguments
    /// 
    /// * `name` - Table name
    #[wasm_bindgen]
    pub async fn drop_table(&self, name: String) -> Result<(), JsValue> {
        self.database.drop_table(&name).await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// List all tables
    #[wasm_bindgen]
    pub async fn list_tables(&self) -> Result<JsValue, JsValue> {
        let tables = self.database.list_tables().await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&tables)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get database statistics
    #[wasm_bindgen]
    pub async fn stats(&self) -> Result<JsValue, JsValue> {
        let stats = self.database.stats().await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&stats)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Flush all pending writes
    #[wasm_bindgen]
    pub async fn flush(&self) -> Result<(), JsValue> {
        self.database.flush().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Perform manual compaction
    #[wasm_bindgen]
    pub async fn compact(&self) -> Result<(), JsValue> {
        self.database.compact().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Close the database
    #[wasm_bindgen]
    pub async fn close(&mut self) -> Result<(), JsValue> {
        self.database.close().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Export database to JSON
    #[wasm_bindgen]
    pub async fn export_json(&self) -> Result<String, JsValue> {
        self.database.export_json().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Import database from JSON
    #[wasm_bindgen]
    pub async fn import_json(&self, json: String) -> Result<(), JsValue> {
        self.database.import_json(&json).await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Create a transaction
    #[wasm_bindgen]
    pub async fn begin_transaction(&self) -> Result<Transaction, JsValue> {
        let tx = self.database.begin_transaction().await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(Transaction { transaction: tx })
    }
}

/// Prepared statement for WASM
#[wasm_bindgen]
pub struct PreparedStatement {
    statement: query::WasmPreparedStatement,
}

#[wasm_bindgen]
impl PreparedStatement {
    /// Execute the prepared statement with parameters
    /// 
    /// # Arguments
    /// 
    /// * `params` - Array of parameter values
    #[wasm_bindgen]
    pub async fn execute(&self, params: Option<JsValue>) -> Result<JsValue, JsValue> {
        let params = params.unwrap_or(JsValue::NULL);
        let result = self.statement.execute(params).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

/// Transaction for WASM
#[wasm_bindgen]
pub struct Transaction {
    transaction: database::WasmTransaction,
}

#[wasm_bindgen]
impl Transaction {
    /// Execute a statement within the transaction
    #[wasm_bindgen]
    pub async fn execute(&self, sql: String) -> Result<JsValue, JsValue> {
        let result = self.transaction.execute(&sql).await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&result)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Commit the transaction
    #[wasm_bindgen]
    pub async fn commit(&self) -> Result<(), JsValue> {
        self.transaction.commit().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Rollback the transaction
    #[wasm_bindgen]
    pub async fn rollback(&self) -> Result<(), JsValue> {
        self.transaction.rollback().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

/// Database iterator for WASM
#[wasm_bindgen]
pub struct Iterator {
    iterator: database::WasmIterator,
}

#[wasm_bindgen]
impl Iterator {
    /// Move to the next item
    #[wasm_bindgen]
    pub async fn next(&mut self) -> Result<bool, JsValue> {
        self.iterator.next().await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the current key
    #[wasm_bindgen]
    pub fn key(&self) -> Result<JsValue, JsValue> {
        let key = self.iterator.key()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&key)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the current value
    #[wasm_bindgen]
    pub fn value(&self) -> Result<JsValue, JsValue> {
        let value = self.iterator.value()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_wasm_bindgen::to_value(&value)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

/// Utility functions for WASM
#[wasm_bindgen]
pub struct Utils;

#[wasm_bindgen]
impl Utils {
    /// Check if running in Node.js environment
    #[wasm_bindgen]
    pub fn is_node() -> bool {
        utils::is_node_environment()
    }

    /// Check if running in browser environment
    #[wasm_bindgen]
    pub fn is_browser() -> bool {
        utils::is_browser_environment()
    }

    /// Check if IndexedDB is available
    #[wasm_bindgen]
    pub fn has_indexeddb() -> bool {
        utils::has_indexeddb()
    }

    /// Check if SIMD is available
    #[wasm_bindgen]
    pub fn has_simd() -> bool {
        utils::has_simd()
    }

    /// Check if Web Workers are available
    #[wasm_bindgen]
    pub fn has_workers() -> bool {
        utils::has_web_workers()
    }

    /// Get available memory (estimate)
    #[wasm_bindgen]
    pub fn available_memory() -> u32 {
        utils::estimate_available_memory()
    }

    /// Set log level for debugging
    #[wasm_bindgen]
    pub fn set_log_level(level: String) {
        utils::set_log_level(&level);
    }
}

// Re-export types for easier access
pub use types::*;
pub use storage::*;