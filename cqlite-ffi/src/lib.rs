//! C FFI bindings for CQLite
//! 
//! This module provides a C-compatible API for the CQLite database engine,
//! enabling integration with other programming languages like Python, Node.js, Go, etc.

#![deny(missing_docs)]
#![allow(clippy::missing_safety_doc)]

mod types;
mod error;
mod database;
mod query;
mod schema;
mod iterator;
mod utils;

pub use types::*;
pub use error::*;

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::Arc;

use tokio::runtime::Runtime;

/// Initialize the CQLite library
/// 
/// This function must be called before using any other CQLite functions.
/// It initializes the async runtime and sets up global state.
/// 
/// Returns:
/// - `CQLITE_OK` on success
/// - Error code on failure
#[no_mangle]
pub extern "C" fn cqlite_init() -> c_int {
    // Initialize global runtime if not already done
    if let Err(_) = utils::get_or_create_runtime() {
        return CQLITE_ERROR_INIT;
    }
    
    CQLITE_OK
}

/// Clean up the CQLite library
/// 
/// This function should be called when finished using CQLite.
/// It cleans up global state and shuts down the async runtime.
#[no_mangle]
pub extern "C" fn cqlite_cleanup() {
    utils::cleanup_runtime();
}

/// Get the version string of CQLite
/// 
/// Returns a null-terminated string containing the version.
/// The caller should not free the returned string.
#[no_mangle]
pub extern "C" fn cqlite_version() -> *const c_char {
    static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
    VERSION.as_ptr() as *const c_char
}

/// Open a CQLite database
/// 
/// # Arguments
/// 
/// * `path` - Path to the database directory (null-terminated string)
/// * `config_json` - Configuration as JSON string (null-terminated), or NULL for default
/// * `db` - Output parameter for database handle
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
/// 
/// # Safety
/// 
/// The `path` parameter must be a valid null-terminated string.
/// The `db` parameter must be a valid pointer to a `cqlite_db_t` pointer.
#[no_mangle]
pub unsafe extern "C" fn cqlite_open(
    path: *const c_char,
    config_json: *const c_char,
    db: *mut *mut cqlite_db_t,
) -> c_int {
    if path.is_null() || db.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => return CQLITE_ERROR_INVALID_UTF8,
    };

    let config = if config_json.is_null() {
        cqlite_core::Config::default()
    } else {
        match CStr::from_ptr(config_json).to_str() {
            Ok(json_str) => {
                match serde_json::from_str::<cqlite_core::Config>(json_str) {
                    Ok(config) => config,
                    Err(_) => return CQLITE_ERROR_INVALID_CONFIG,
                }
            }
            Err(_) => return CQLITE_ERROR_INVALID_UTF8,
        }
    };

    match database::open_database(path_str, config) {
        Ok(database_handle) => {
            *db = Box::into_raw(Box::new(database_handle));
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Close a CQLite database
/// 
/// # Arguments
/// 
/// * `db` - Database handle to close
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
/// 
/// # Safety
/// 
/// The `db` parameter must be a valid database handle returned by `cqlite_open`.
/// After this call, the handle becomes invalid and should not be used.
#[no_mangle]
pub unsafe extern "C" fn cqlite_close(db: *mut cqlite_db_t) -> c_int {
    if db.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    let database_handle = Box::from_raw(db);
    match database::close_database(*database_handle) {
        Ok(()) => CQLITE_OK,
        Err(error_code) => error_code,
    }
}

/// Execute a SQL statement
/// 
/// # Arguments
/// 
/// * `db` - Database handle
/// * `sql` - SQL statement (null-terminated string)
/// * `result` - Output parameter for query result, or NULL if not needed
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
/// 
/// # Safety
/// 
/// The `db` parameter must be a valid database handle.
/// The `sql` parameter must be a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn cqlite_execute(
    db: *mut cqlite_db_t,
    sql: *const c_char,
    result: *mut *mut cqlite_result_t,
) -> c_int {
    if db.is_null() || sql.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return CQLITE_ERROR_INVALID_UTF8,
    };

    match query::execute_query(&*db, sql_str) {
        Ok(query_result) => {
            if !result.is_null() {
                *result = Box::into_raw(Box::new(query_result));
            }
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Prepare a SQL statement for repeated execution
/// 
/// # Arguments
/// 
/// * `db` - Database handle
/// * `sql` - SQL statement (null-terminated string)
/// * `stmt` - Output parameter for prepared statement handle
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_prepare(
    db: *mut cqlite_db_t,
    sql: *const c_char,
    stmt: *mut *mut cqlite_stmt_t,
) -> c_int {
    if db.is_null() || sql.is_null() || stmt.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(_) => return CQLITE_ERROR_INVALID_UTF8,
    };

    match query::prepare_statement(&*db, sql_str) {
        Ok(prepared_stmt) => {
            *stmt = Box::into_raw(Box::new(prepared_stmt));
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Execute a prepared statement
/// 
/// # Arguments
/// 
/// * `stmt` - Prepared statement handle
/// * `params` - Parameter values array, or NULL if no parameters
/// * `param_count` - Number of parameters
/// * `result` - Output parameter for query result, or NULL if not needed
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_execute_prepared(
    stmt: *mut cqlite_stmt_t,
    params: *const cqlite_value_t,
    param_count: usize,
    result: *mut *mut cqlite_result_t,
) -> c_int {
    if stmt.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    let params_slice = if params.is_null() || param_count == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(params, param_count)
    };

    match query::execute_prepared(&*stmt, params_slice) {
        Ok(query_result) => {
            if !result.is_null() {
                *result = Box::into_raw(Box::new(query_result));
            }
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Free a prepared statement
/// 
/// # Arguments
/// 
/// * `stmt` - Prepared statement handle to free
/// 
/// # Safety
/// 
/// The statement handle becomes invalid after this call.
#[no_mangle]
pub unsafe extern "C" fn cqlite_stmt_free(stmt: *mut cqlite_stmt_t) {
    if !stmt.is_null() {
        let _ = Box::from_raw(stmt);
    }
}

/// Free a query result
/// 
/// # Arguments
/// 
/// * `result` - Query result handle to free
/// 
/// # Safety
/// 
/// The result handle becomes invalid after this call.
#[no_mangle]
pub unsafe extern "C" fn cqlite_result_free(result: *mut cqlite_result_t) {
    if !result.is_null() {
        let _ = Box::from_raw(result);
    }
}

/// Get the number of rows in a query result
/// 
/// # Arguments
/// 
/// * `result` - Query result handle
/// 
/// # Returns
/// 
/// Number of rows, or 0 if result is NULL
#[no_mangle]
pub unsafe extern "C" fn cqlite_result_row_count(result: *const cqlite_result_t) -> usize {
    if result.is_null() {
        return 0;
    }
    query::get_row_count(&*result)
}

/// Get the number of columns in a query result
/// 
/// # Arguments
/// 
/// * `result` - Query result handle
/// 
/// # Returns
/// 
/// Number of columns, or 0 if result is NULL
#[no_mangle]
pub unsafe extern "C" fn cqlite_result_column_count(result: *const cqlite_result_t) -> usize {
    if result.is_null() {
        return 0;
    }
    query::get_column_count(&*result)
}

/// Get column metadata from a query result
/// 
/// # Arguments
/// 
/// * `result` - Query result handle
/// * `column_index` - Zero-based column index
/// * `column_info` - Output parameter for column information
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_result_column_info(
    result: *const cqlite_result_t,
    column_index: usize,
    column_info: *mut cqlite_column_info_t,
) -> c_int {
    if result.is_null() || column_info.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    match query::get_column_info(&*result, column_index) {
        Ok(info) => {
            *column_info = info;
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Get a value from a query result
/// 
/// # Arguments
/// 
/// * `result` - Query result handle
/// * `row_index` - Zero-based row index
/// * `column_index` - Zero-based column index
/// * `value` - Output parameter for the value
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_result_get_value(
    result: *const cqlite_result_t,
    row_index: usize,
    column_index: usize,
    value: *mut cqlite_value_t,
) -> c_int {
    if result.is_null() || value.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    match query::get_result_value(&*result, row_index, column_index) {
        Ok(val) => {
            *value = val;
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Get the last error message
/// 
/// Returns a null-terminated string containing the last error message.
/// The string is valid until the next CQLite function call.
#[no_mangle]
pub extern "C" fn cqlite_error_message() -> *const c_char {
    error::get_last_error_message()
}

/// Free a string returned by CQLite
/// 
/// # Arguments
/// 
/// * `str_ptr` - String pointer to free
/// 
/// # Safety
/// 
/// Only call this on strings returned by CQLite functions that explicitly
/// state the string should be freed by the caller.
#[no_mangle]
pub unsafe extern "C" fn cqlite_string_free(str_ptr: *mut c_char) {
    if !str_ptr.is_null() {
        let _ = CString::from_raw(str_ptr);
    }
}

/// Create an iterator for scanning table data
/// 
/// # Arguments
/// 
/// * `db` - Database handle
/// * `table_name` - Table name (null-terminated string)
/// * `start_key` - Start key for scan, or NULL for beginning
/// * `end_key` - End key for scan, or NULL for end
/// * `iterator` - Output parameter for iterator handle
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_iterator_create(
    db: *mut cqlite_db_t,
    table_name: *const c_char,
    start_key: *const c_char,
    end_key: *const c_char,
    iterator: *mut *mut cqlite_iterator_t,
) -> c_int {
    if db.is_null() || table_name.is_null() || iterator.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    let table_str = match CStr::from_ptr(table_name).to_str() {
        Ok(s) => s,
        Err(_) => return CQLITE_ERROR_INVALID_UTF8,
    };

    let start_key_str = if start_key.is_null() {
        None
    } else {
        match CStr::from_ptr(start_key).to_str() {
            Ok(s) => Some(s),
            Err(_) => return CQLITE_ERROR_INVALID_UTF8,
        }
    };

    let end_key_str = if end_key.is_null() {
        None
    } else {
        match CStr::from_ptr(end_key).to_str() {
            Ok(s) => Some(s),
            Err(_) => return CQLITE_ERROR_INVALID_UTF8,
        }
    };

    match iterator::create_iterator(&*db, table_str, start_key_str, end_key_str) {
        Ok(iter) => {
            *iterator = Box::into_raw(Box::new(iter));
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Move iterator to next item
/// 
/// # Arguments
/// 
/// * `iterator` - Iterator handle
/// 
/// # Returns
/// 
/// * `CQLITE_OK` if next item exists
/// * `CQLITE_ERROR_EOF` if no more items
/// * Other error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_iterator_next(iterator: *mut cqlite_iterator_t) -> c_int {
    if iterator.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    iterator::next_item(&mut *iterator)
}

/// Get current key from iterator
/// 
/// # Arguments
/// 
/// * `iterator` - Iterator handle
/// * `key` - Output parameter for key value
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_iterator_key(
    iterator: *const cqlite_iterator_t,
    key: *mut cqlite_value_t,
) -> c_int {
    if iterator.is_null() || key.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    match iterator::get_current_key(&*iterator) {
        Ok(k) => {
            *key = k;
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Get current value from iterator
/// 
/// # Arguments
/// 
/// * `iterator` - Iterator handle
/// * `value` - Output parameter for value
/// 
/// # Returns
/// 
/// * `CQLITE_OK` on success
/// * Error code on failure
#[no_mangle]
pub unsafe extern "C" fn cqlite_iterator_value(
    iterator: *const cqlite_iterator_t,
    value: *mut cqlite_value_t,
) -> c_int {
    if iterator.is_null() || value.is_null() {
        return CQLITE_ERROR_NULL_POINTER;
    }

    match iterator::get_current_value(&*iterator) {
        Ok(v) => {
            *value = v;
            CQLITE_OK
        }
        Err(error_code) => error_code,
    }
}

/// Free an iterator
/// 
/// # Arguments
/// 
/// * `iterator` - Iterator handle to free
/// 
/// # Safety
/// 
/// The iterator handle becomes invalid after this call.
#[no_mangle]
pub unsafe extern "C" fn cqlite_iterator_free(iterator: *mut cqlite_iterator_t) {
    if !iterator.is_null() {
        let _ = Box::from_raw(iterator);
    }
}