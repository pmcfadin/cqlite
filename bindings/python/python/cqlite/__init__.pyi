"""Type stubs for CQLite Python bindings.

CQLite provides read-only access to Cassandra 5.0 SSTables without a cluster.
"""

from pathlib import Path
from typing import Iterator, Any
from types import TracebackType

# Type alias for configuration (dict or JSON string)
Config = dict[str, Any] | str

# Version information
__version__: str
def version() -> str:
    """Return the CQLite version string."""
    ...

# Exception hierarchy
class CqliteError(Exception):
    """Base exception for all CQLite errors."""
    ...

class SchemaError(CqliteError):
    """Raised when schema parsing or validation fails."""
    ...

class QueryError(CqliteError):
    """Raised when query execution fails."""
    ...

class ParseError(CqliteError):
    """Raised when CQL query parsing fails."""
    ...

# Configuration
class StreamingConfig:
    """Configuration for streaming query execution.

    Attributes:
        buffer_size: Number of rows to buffer in flight (default: 1024)
        chunk_size: Number of rows per chunk (default: 10000)
    """
    buffer_size: int
    chunk_size: int

    def __init__(
        self,
        buffer_size: int = 1024,
        chunk_size: int = 10_000,
    ) -> None:
        """Create a streaming configuration.

        Args:
            buffer_size: Number of rows to buffer (must be > 0)
            chunk_size: Number of rows per chunk (must be > 0)

        Raises:
            ValueError: If buffer_size or chunk_size is <= 0
        """
        ...

    def __repr__(self) -> str: ...

def memory_optimized() -> dict[str, Any]:
    """Return a memory-optimized configuration preset.

    Returns:
        Configuration dict with max_memory = 256 MB
    """
    ...

def performance_optimized() -> dict[str, Any]:
    """Return a performance-optimized configuration preset.

    Returns:
        Configuration dict with max_memory = 4 GB
    """
    ...

def validate_config(config: Config) -> bool:
    """Validate a configuration dict or JSON string.

    Args:
        config: Configuration dict, JSON string, or preset name

    Returns:
        True if the configuration is valid

    Raises:
        ValueError: If the configuration is invalid
    """
    ...

# Database
class Database:
    """A connection to CQLite SSTable data.

    Database is the main entry point for querying Cassandra SSTables.
    It supports context manager protocol for automatic resource cleanup.

    Example:
        >>> with cqlite.open("path/to/sstables", schema="schema.cql") as db:
        ...     result = db.execute("SELECT * FROM keyspace.table LIMIT 10")
        ...     for row in result:
        ...         print(row["column_name"])

    Thread Safety:
        The database handle is thread-safe. Multiple threads can execute queries
        concurrently on the same Database instance.

        **GIL Release**: All blocking operations (open, execute, execute_streaming,
        prepare, stats, close) release the Python GIL, allowing other Python
        threads to run during I/O.

        **Iterator Usage**: Each thread should create its own StreamingIterator
        via execute_streaming(). Do not share iterators between threads.

        **Known Limitation**: Concurrent queries may experience a race condition
        in schema metadata access on first use. Workaround: Execute a warm-up
        query (e.g., ``SELECT * FROM table LIMIT 1``) before spawning parallel
        query threads.
    """

    @property
    def is_closed(self) -> bool:
        """True if the database connection is closed."""
        ...

    def execute(self, query: str) -> "QueryResult":
        """Execute a CQL query and return all results.

        Args:
            query: CQL SELECT query string

        Returns:
            QueryResult containing all matching rows

        Raises:
            QueryError: If query execution fails
            ParseError: If query syntax is invalid
            RuntimeError: If database is closed
        """
        ...

    def execute_streaming(
        self,
        query: str,
        config: StreamingConfig | None = None,
    ) -> "StreamingIterator":
        """Execute a CQL query with streaming results.

        Memory-efficient iteration over large result sets.

        Args:
            query: CQL SELECT query string
            config: Optional streaming configuration

        Returns:
            StreamingIterator for iterating over rows

        Raises:
            QueryError: If query execution fails
            ParseError: If query syntax is invalid
            RuntimeError: If database is closed

        Thread Safety:
            Each thread should use its own StreamingIterator. Do not share
            a single iterator between threads - create separate iterators
            per thread instead.
        """
        ...

    def prepare(self, query: str) -> "PreparedStatement":
        """Prepare a query for analysis.

        Args:
            query: CQL query string

        Returns:
            PreparedStatement with query plan information

        Raises:
            ParseError: If query syntax is invalid
            RuntimeError: If database is closed
        """
        ...

    def stats(self) -> "DatabaseStats":
        """Get database statistics.

        Returns:
            DatabaseStats with storage, memory, and query metrics

        Raises:
            RuntimeError: If database is closed
        """
        ...

    def close(self) -> None:
        """Close the database connection.

        This method is idempotent and thread-safe.
        """
        ...

    def __enter__(self) -> "Database":
        """Enter context manager."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """Exit context manager, closing the database.

        Returns:
            False (never suppresses exceptions)
        """
        ...

def open(
    path: str | Path,
    *,
    schema: str | Path | None = None,
    config: Config | None = None,
) -> Database:
    """Open a database connection to SSTable data.

    This is the primary entry point for opening a CQLite database.

    Args:
        path: Path to the SSTable directory
        schema: Optional path to CQL schema file
        config: Optional configuration (dict, JSON string, or preset name)

    Returns:
        A Database instance

    Raises:
        IOError: If the path does not exist
        SchemaError: If schema parsing fails
        ValueError: If configuration is invalid

    Example:
        >>> import cqlite
        >>> db = cqlite.open("data/sstables", schema="schema.cql")
        >>> result = db.execute("SELECT * FROM test.users")
        >>> db.close()

    Thread Safety:
        This function releases the Python GIL during file I/O operations.
    """
    ...

# Result types
class QueryResult:
    """Result of a query execution containing all rows.

    QueryResult is iterable and supports len() for row count.

    Attributes:
        rows: List of all result rows
        rows_affected: Number of rows affected
        execution_time_ms: Query execution time in milliseconds
        columns: Column metadata
    """

    @property
    def rows(self) -> list["Row"]:
        """All result rows."""
        ...

    @property
    def rows_affected(self) -> int:
        """Number of rows affected by the query."""
        ...

    @property
    def execution_time_ms(self) -> int:
        """Query execution time in milliseconds."""
        ...

    @property
    def columns(self) -> list["ColumnInfo"]:
        """Column metadata for the result set."""
        ...

    def __len__(self) -> int:
        """Return the number of rows."""
        ...

    def __iter__(self) -> Iterator["Row"]:
        """Iterate over rows."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """Convert result to a dictionary.

        Returns:
            Dict with 'rows', 'rows_affected', 'execution_time_ms', 'columns'
        """
        ...

    def __repr__(self) -> str: ...

class Row:
    """A single row from a query result.

    Row provides dict-like access to column values by name.

    Example:
        >>> row["name"]
        'Alice'
        >>> row.get("age", 0)
        30
        >>> dict(row.items())
        {'name': 'Alice', 'age': 30}
    """

    def __getitem__(self, key: str) -> Any:
        """Get column value by name.

        Args:
            key: Column name

        Returns:
            Column value (Python native type)

        Raises:
            KeyError: If column does not exist
        """
        ...

    def __contains__(self, key: str) -> bool:
        """Check if column exists."""
        ...

    def __len__(self) -> int:
        """Return number of columns."""
        ...

    def get(self, key: str, default: Any = None) -> Any:
        """Get column value with default.

        Args:
            key: Column name
            default: Value to return if column not found

        Returns:
            Column value or default
        """
        ...

    def keys(self) -> list[str]:
        """Return list of column names."""
        ...

    def values(self) -> list[Any]:
        """Return list of column values."""
        ...

    def items(self) -> list[tuple[str, Any]]:
        """Return list of (name, value) tuples."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """Convert row to a dictionary."""
        ...

    def __repr__(self) -> str: ...

class ColumnInfo:
    """Metadata for a result column.

    Attributes:
        name: Column name
        data_type: CQL type as string (e.g., "Text", "Int", "List<Text>")
        nullable: Whether the column can be null
        position: 0-indexed position in result
        table_name: Original table name (may be None)
    """

    @property
    def name(self) -> str:
        """Column name."""
        ...

    @property
    def data_type(self) -> str:
        """CQL data type as string."""
        ...

    @property
    def nullable(self) -> bool:
        """Whether column can be null."""
        ...

    @property
    def position(self) -> int:
        """0-indexed position in result."""
        ...

    @property
    def table_name(self) -> str | None:
        """Original table name, if known."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        ...

    def __repr__(self) -> str: ...

class StreamingIterator:
    """Memory-efficient iterator for large result sets.

    StreamingIterator yields rows one at a time, minimizing memory usage.
    Early termination via break is safe; resources are cleaned up automatically.

    Attributes:
        rows_received: Number of rows received so far
        progress_percent: Progress percentage (None if total unknown)

    Example:
        >>> for row in db.execute_streaming("SELECT * FROM big_table"):
        ...     process(row)
        ...     if should_stop:
        ...         break  # Safe early termination

    Thread Safety:
        StreamingIterator is designed for single-thread use. Each Python thread
        should create its own iterator via Database.execute_streaming().
        Sharing an iterator between threads is not supported.
    """

    @property
    def rows_received(self) -> int:
        """Number of rows received so far."""
        ...

    @property
    def progress_percent(self) -> float | None:
        """Progress percentage, or None if total is unknown."""
        ...

    def __iter__(self) -> Iterator["Row"]:
        """Return self as iterator."""
        ...

    def __next__(self) -> "Row":
        """Get next row.

        Raises:
            StopIteration: When no more rows available
        """
        ...

    def __repr__(self) -> str: ...

# Prepared statements
class PreparedStatement:
    """A prepared query with plan information.

    PreparedStatement provides query analysis without execution.

    Attributes:
        query: Original CQL query text
        parameter_count: Number of query parameters
    """

    @property
    def query(self) -> str:
        """Original CQL query text."""
        ...

    @property
    def parameter_count(self) -> int:
        """Number of query parameters."""
        ...

    def stats(self) -> dict[str, Any]:
        """Get query plan statistics.

        Returns:
            Dict with 'parameter_count', 'plan_type', 'estimated_cost',
            'estimated_rows', 'cache_friendly'
        """
        ...

    def __repr__(self) -> str: ...

# Statistics
class DatabaseStats:
    """Database statistics for monitoring and debugging.

    Attributes:
        storage_stats: SSTable storage metrics
        memory_stats: Memory and cache metrics
        query_stats: Query execution metrics (may be None)
    """

    @property
    def storage_stats(self) -> dict[str, int]:
        """SSTable storage metrics.

        Keys: 'sstable_count', 'total_size', 'total_entries',
              'total_tables', 'average_size'
        """
        ...

    @property
    def memory_stats(self) -> dict[str, int]:
        """Memory and cache metrics.

        Keys: 'block_cache_hits', 'block_cache_misses', 'row_cache_hits',
              'row_cache_misses', 'total_memory_used', 'buffer_allocations',
              'buffer_deallocations'
        """
        ...

    @property
    def query_stats(self) -> dict[str, Any] | None:
        """Query execution metrics (when state_machine enabled).

        Keys: 'total_queries', 'error_queries', 'avg_execution_time_us',
              'cache_hit_ratio', 'rows_affected'
        """
        ...

    def to_dict(self) -> dict[str, Any]:
        """Convert all stats to nested dictionary."""
        ...

    def __repr__(self) -> str: ...
