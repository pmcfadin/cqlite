"""CQLite: Read Cassandra 5.0 SSTables without a cluster."""

from cqlite._cqlite import (
    __version__,
    version,
    # Test-support introspection (issue #1437)
    _built_with_panic_abort,
    # Test-support: direct DECIMAL rendering path (issue #1741)
    _decimal_from_parts,
    # Test-support: direct INET rendering path (issue #1453)
    _inet_from_bytes,
    # Test-support: direct VARINT decoding path (issue #1452)
    _varint_from_bytes,
    # Test-support: shared FFI error-contract conformance probe (issue #1451)
    _raise_mapped_core_error,
    # Test-support: committed cross-binding vector table (issue #1452)
    _ffi_common_render_vectors,
    # Exception types
    CqliteError,
    SchemaError,
    QueryError,
    ParseError,
    CancelledError,
    # Configuration
    StreamingConfig,
    memory_optimized,
    performance_optimized,
    validate_config,
    # Database
    Database,
    open,
    # Result types
    QueryResult,
    Row,
    ColumnInfo,
    StreamingIterator,
    # Exact temporal types (issue #1450)
    Duration,
    # UDT with out-of-band type identity (issue #3504)
    Udt,
    # Prepared statements
    PreparedStatement,
    # Statistics
    DatabaseStats,
    # Write API (Issue #390)
    WriteStats,
    MaintenanceReport,
    # Refresh (Issue #1749)
    RefreshReport,
)

__all__ = [
    "__version__",
    "version",
    # Test-support introspection (issue #1437)
    "_built_with_panic_abort",
    # Test-support: direct DECIMAL rendering path (issue #1741)
    "_decimal_from_parts",
    # Test-support: direct INET rendering path (issue #1453)
    "_inet_from_bytes",
    # Test-support: direct VARINT decoding path (issue #1452)
    "_varint_from_bytes",
    # Test-support: shared FFI error-contract conformance probe (issue #1451)
    "_raise_mapped_core_error",
    # Test-support: committed cross-binding vector table (issue #1452)
    "_ffi_common_render_vectors",
    # Exception types
    "CqliteError",
    "SchemaError",
    "QueryError",
    "ParseError",
    "CancelledError",
    # Configuration
    "StreamingConfig",
    "memory_optimized",
    "performance_optimized",
    "validate_config",
    # Database
    "Database",
    "open",
    # Result types
    "QueryResult",
    "Row",
    "ColumnInfo",
    "StreamingIterator",
    # Exact temporal types (issue #1450)
    "Duration",
    # UDT with out-of-band type identity (issue #3504)
    "Udt",
    # Prepared statements
    "PreparedStatement",
    # Statistics
    "DatabaseStats",
    # Write API (Issue #390)
    "WriteStats",
    "MaintenanceReport",
    # Refresh (Issue #1749)
    "RefreshReport",
]
