"""CQLite: Read Cassandra 5.0 SSTables without a cluster."""

from cqlite._cqlite import (
    __version__,
    version,
    # Exception types
    CqliteError,
    SchemaError,
    QueryError,
    ParseError,
)

__all__ = [
    "__version__",
    "version",
    # Exception types
    "CqliteError",
    "SchemaError",
    "QueryError",
    "ParseError",
]
