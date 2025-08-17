//! FFI error handling
//!
//! This module contains error handling for the FFI interface.

use std::os::raw::c_int;

/// Success return code
pub const CQLITE_OK: c_int = 0;

/// Initialization error
pub const CQLITE_ERROR_INIT: c_int = -1;

/// Null pointer error
pub const CQLITE_ERROR_NULL_POINTER: c_int = -2;

/// Invalid UTF-8 error
pub const CQLITE_ERROR_INVALID_UTF8: c_int = -3;

/// Invalid configuration error
pub const CQLITE_ERROR_INVALID_CONFIG: c_int = -4;

/// Database error
pub const CQLITE_ERROR_DATABASE: c_int = -5;

/// Query error
pub const CQLITE_ERROR_QUERY: c_int = -6;

/// Network error
pub const CQLITE_ERROR_NETWORK: c_int = -7;

/// I/O error
pub const CQLITE_ERROR_IO: c_int = -8;

/// Out of memory error
pub const CQLITE_ERROR_OUT_OF_MEMORY: c_int = -9;

/// End of file error
pub const CQLITE_ERROR_EOF: c_int = -10;

thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Set the last error message
pub fn set_last_error(msg: String) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(msg);
    });
}

/// Get the last error message
pub fn get_last_error_message() -> Option<String> {
    LAST_ERROR.with(|e| e.borrow().clone())
}

/// Get the last error message as a C string
pub fn get_last_error_message_cstr() -> *const std::os::raw::c_char {
    use std::ffi::CString;
    if let Some(msg) = get_last_error_message() {
        if let Ok(c_string) = CString::new(msg) {
            return c_string.into_raw();
        }
    }
    std::ptr::null()
}
