"""CQLite: Read Cassandra 5.0 SSTables without a cluster."""

from cqlite._cqlite import (
    __version__,
    version,
    # Test-support introspection (issue #1437)
    _built_with_panic_abort,
    # Exception types
    CqliteError,
    SchemaError,
    QueryError,
    ParseError,
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
    # Exception types
    "CqliteError",
    "SchemaError",
    "QueryError",
    "ParseError",
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
