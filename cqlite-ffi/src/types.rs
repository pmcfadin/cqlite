//! FFI type definitions
//!
//! This module contains type definitions for the FFI interface.

/// Opaque database handle
#[repr(C)]
pub struct cqlite_db_t {
    pub(crate) _private: [u8; 0],
}

/// Opaque statement handle
#[repr(C)]
pub struct cqlite_stmt_t {
    pub(crate) _private: [u8; 0],
}

/// Opaque result handle
#[repr(C)]
pub struct cqlite_result_t {
    pub(crate) _private: [u8; 0],
}

/// Value type for parameters and results
#[repr(C)]
pub struct cqlite_value_t {
    pub(crate) _private: [u8; 0],
}

/// Iterator handle
#[repr(C)]
pub struct cqlite_iterator_t {
    pub(crate) _private: [u8; 0],
}

/// Schema handle
#[repr(C)]
pub struct cqlite_schema_t {
    pub(crate) _private: [u8; 0],
}

/// Column info handle
#[repr(C)]
pub struct cqlite_column_info_t {
    pub(crate) _private: [u8; 0],
}
