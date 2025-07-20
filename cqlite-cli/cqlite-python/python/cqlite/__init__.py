"""
🚀 CQLite - The World's First Python Package for Direct SSTable Querying!

Execute SELECT statements directly on Cassandra SSTable files without running Cassandra.
Revolutionary Python library that brings SQL querying to SSTable files!

Key Features:
- Execute SELECT queries directly on SSTable files
- Automatic Python type conversion from CQL types  
- Pandas DataFrame integration
- Async query support
- Export to CSV, Parquet, JSON, Excel
- Zero-copy operations where possible
- Memory-efficient streaming for large datasets

Basic Usage:
    ```python
    import cqlite
    
    # Open SSTable with schema
    reader = cqlite.SSTableReader("users-big-Data.db", schema="schema.json")
    
    # Execute SELECT queries directly!
    results = reader.query("SELECT name, email FROM users WHERE age > 25")
    
    # Convert to pandas DataFrame
    df = reader.query_df("SELECT * FROM users LIMIT 1000")
    
    # Async iteration for large datasets
    async for row in reader.query_async("SELECT * FROM large_table"):
        process_row(row)
    
    # Export capabilities
    reader.export_csv("SELECT * FROM users", "output.csv")
    reader.export_parquet("SELECT * FROM analytics", "output.parquet")
    ```

Advanced Usage:
    ```python
    # Context manager support
    with cqlite.SSTableReader("data.db") as reader:
        df = reader.query_df("SELECT * FROM users")
    
    # Discover SSTable files
    sstables = cqlite.discover_sstables("/path/to/cassandra/data")
    
    # Batch processing multiple SSTable files
    processor = cqlite.AsyncBatchProcessor(sstable_paths)
    results = await processor.process_all("SELECT count(*) FROM users")
    
    # Schema introspection
    schema = reader.get_schema()
    stats = reader.get_stats()
    ```
"""

from ._core import (
    # Main reader class
    SSTableReader,
    
    # Exception types
    CQLiteError,
    SchemaError, 
    QueryError,
    SSTableError,
    
    # Utility functions
    discover_sstables,
    infer_schema,
    validate_sstable,
    
    # Version info
    __version__,
    __author__,
)

from .reader import (
    # High-level Python API wrappers
    open_sstable,
    query_sstable,
    query_sstable_df,
)

from .types import (
    # Type system
    CQLType,
    PYTHON_TYPE_MAPPING,
    infer_python_type,
    convert_cql_value,
)

from .utils import (
    # Helper utilities
    format_query_results,
    estimate_memory_usage,
    optimize_query,
    create_schema_from_cql,
)

from .async_support import (
    # Async support
    AsyncSSTableReader,
    AsyncBatchProcessor,
    stream_query_results,
)

# Version and metadata
__version__ = "0.1.0"
__author__ = "CQLite Team"
__email__ = "support@cqlite.dev"
__description__ = "🚀 FIRST EVER: Direct SSTable querying for Cassandra"
__url__ = "https://github.com/cqlite/cqlite"

# Public API
__all__ = [
    # Core classes
    "SSTableReader",
    "AsyncSSTableReader", 
    "AsyncBatchProcessor",
    
    # Exception types
    "CQLiteError",
    "SchemaError",
    "QueryError", 
    "SSTableError",
    
    # Main functions
    "open_sstable",
    "query_sstable",
    "query_sstable_df",
    "discover_sstables",
    "infer_schema",
    "validate_sstable",
    "stream_query_results",
    
    # Type utilities
    "CQLType",
    "PYTHON_TYPE_MAPPING",
    "infer_python_type",
    "convert_cql_value",
    
    # Helper utilities
    "format_query_results",
    "estimate_memory_usage", 
    "optimize_query",
    "create_schema_from_cql",
    
    # Metadata
    "__version__",
    "__author__",
    "__email__",
    "__description__",
    "__url__",
]

# Optional pandas integration
try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False

# Optional numpy integration  
try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False

# Optional pyarrow integration
try:
    import pyarrow as pa
    _PYARROW_AVAILABLE = True
except ImportError:
    _PYARROW_AVAILABLE = False

# Feature detection
def get_available_features():
    """Get list of available optional features."""
    features = {
        "pandas": _PANDAS_AVAILABLE,
        "numpy": _NUMPY_AVAILABLE, 
        "pyarrow": _PYARROW_AVAILABLE,
        "async": True,  # Always available
        "streaming": True,  # Always available
    }
    return features

def check_dependencies():
    """Check and report on optional dependencies."""
    features = get_available_features()
    
    print("🚀 CQLite Feature Status:")
    print(f"   📊 Pandas DataFrame support: {'✅' if features['pandas'] else '❌'}")
    print(f"   🔢 NumPy array support: {'✅' if features['numpy'] else '❌'}")
    print(f"   📦 Parquet export support: {'✅' if features['pyarrow'] else '❌'}")
    print(f"   ⚡ Async query support: {'✅' if features['async'] else '❌'}")
    print(f"   🌊 Streaming support: {'✅' if features['streaming'] else '❌'}")
    
    if not features['pandas']:
        print("   💡 Install pandas: pip install pandas")
    if not features['numpy']:
        print("   💡 Install numpy: pip install numpy")
    if not features['pyarrow']:
        print("   💡 Install pyarrow: pip install pyarrow")

# Convenience function for quick usage
def quick_query(sstable_path: str, sql: str, **kwargs):
    """
    Quick one-liner for SSTable querying.
    
    Args:
        sstable_path: Path to SSTable Data.db file
        sql: SELECT statement to execute
        **kwargs: Additional arguments for SSTableReader
        
    Returns:
        List of query results as dictionaries
        
    Example:
        ```python
        results = cqlite.quick_query("users-Data.db", "SELECT * FROM users LIMIT 10")
        ```
    """
    with SSTableReader(sstable_path, **kwargs) as reader:
        return reader.query(sql)

def quick_query_df(sstable_path: str, sql: str, **kwargs):
    """
    Quick one-liner for SSTable querying with DataFrame output.
    
    Args:
        sstable_path: Path to SSTable Data.db file
        sql: SELECT statement to execute  
        **kwargs: Additional arguments for SSTableReader
        
    Returns:
        pandas.DataFrame with query results
        
    Example:
        ```python
        df = cqlite.quick_query_df("users-Data.db", "SELECT * FROM users")
        ```
    """
    if not _PANDAS_AVAILABLE:
        raise ImportError("pandas is required for DataFrame output. Install with: pip install pandas")
    
    with SSTableReader(sstable_path, **kwargs) as reader:
        return reader.query_df(sql)

# Add convenience functions to __all__
__all__.extend([
    "get_available_features",
    "check_dependencies", 
    "quick_query",
    "quick_query_df",
])

# Banner for interactive use
def _show_banner():
    """Show welcome banner when imported interactively."""
    import sys
    if hasattr(sys, 'ps1'):  # Interactive mode
        print("🚀 Welcome to CQLite - The World's First Python SSTable Querying Library!")
        print("   📖 Documentation: https://docs.cqlite.dev")
        print("   💡 Quick start: cqlite.check_dependencies()")
        print("   🔥 Example: cqlite.quick_query('data.db', 'SELECT * FROM users LIMIT 5')")

# Show banner on import
_show_banner()