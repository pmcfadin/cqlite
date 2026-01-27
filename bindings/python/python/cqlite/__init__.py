"""CQLite: Read Cassandra 5.0 SSTables without a cluster."""

from cqlite._cqlite import (
    __version__,
    version,
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
)

__all__ = [
    "__version__",
    "version",
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
]
