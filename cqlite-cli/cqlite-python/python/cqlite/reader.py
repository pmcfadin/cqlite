"""
High-level Python API for SSTable reading and querying.

This module provides convenient Python wrappers around the core Rust implementation,
making it even easier to work with SSTable files from Python.
"""

import os
import json
from typing import List, Dict, Any, Optional, Union, Iterator, AsyncIterator
from pathlib import Path

from ._core import SSTableReader as _CoreSSTableReader
from ._core import discover_sstables, infer_schema, validate_sstable
from .types import CQLType, convert_cql_value
from .utils import format_query_results, estimate_memory_usage


class SSTableReader(_CoreSSTableReader):
    """
    Enhanced SSTable reader with additional Python convenience methods.
    
    This class extends the core Rust implementation with Python-specific
    features and more intuitive APIs for Python developers.
    """
    
    def __init__(
        self,
        sstable_path: str,
        schema: Optional[Union[str, Dict[str, Any]]] = None,
        cache_enabled: bool = True,
        max_memory_mb: int = 1024,
        auto_detect_schema: bool = True,
    ):
        """
        Create a new SSTableReader with enhanced Python features.
        
        Args:
            sstable_path: Path to the SSTable Data.db file
            schema: Path to schema JSON file or schema dictionary
            cache_enabled: Enable query result caching
            max_memory_mb: Maximum memory usage in MB
            auto_detect_schema: Automatically detect schema if not provided
        """
        # Auto-detect schema if not provided and requested
        if schema is None and auto_detect_schema:
            try:
                schema = infer_schema(sstable_path)
            except Exception:
                # Continue without schema if detection fails
                pass
        
        # Convert schema dict to JSON string if needed
        if isinstance(schema, dict):
            schema = json.dumps(schema)
        
        # Initialize core reader
        super().__init__(sstable_path, schema, cache_enabled, max_memory_mb)
        
        # Python-specific attributes
        self._sstable_path = sstable_path
        self._schema_cache = None
        self._stats_cache = None
    
    @property
    def sstable_path(self) -> str:
        """Get the SSTable file path."""
        return self._sstable_path
    
    @property
    def table_name(self) -> str:
        """Get the table name extracted from SSTable filename."""
        return self.get_table_name()
    
    @property
    def schema(self) -> Dict[str, Any]:
        """Get the schema information (cached)."""
        if self._schema_cache is None:
            self._schema_cache = self.get_schema()
        return self._schema_cache
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get SSTable statistics (cached)."""
        if self._stats_cache is None:
            self._stats_cache = self.get_stats()
        return self._stats_cache
    
    def query_one(self, sql: str) -> Optional[Dict[str, Any]]:
        """
        Execute a query and return only the first result.
        
        Args:
            sql: SELECT statement to execute
            
        Returns:
            First result row as dictionary, or None if no results
        """
        results = self.query(sql, limit=1)
        return results[0] if results else None
    
    def query_count(self, sql: str) -> int:
        """
        Execute a query and return the count of results.
        
        Args:
            sql: SELECT statement to execute (COUNT will be applied)
            
        Returns:
            Number of rows that match the query
        """
        # Convert SELECT to COUNT query
        if sql.strip().upper().startswith("SELECT"):
            # Basic transformation: SELECT * FROM table -> SELECT COUNT(*) FROM table
            parts = sql.split(" FROM ", 1)
            if len(parts) == 2:
                count_sql = f"SELECT COUNT(*) FROM {parts[1]}"
                result = self.query_one(count_sql)
                if result and 'count' in result:
                    return result['count']
        
        # Fallback: execute original query and count results
        results = self.query(sql)
        return len(results)
    
    def query_exists(self, sql: str) -> bool:
        """
        Check if a query returns any results.
        
        Args:
            sql: SELECT statement to execute
            
        Returns:
            True if query returns at least one row
        """
        results = self.query(sql, limit=1)
        return len(results) > 0
    
    def query_columns(self, *columns: str, where: str = None, limit: int = None) -> List[Dict[str, Any]]:
        """
        Query specific columns with optional WHERE clause.
        
        Args:
            *columns: Column names to select
            where: Optional WHERE clause (without "WHERE" keyword)
            limit: Optional LIMIT
            
        Returns:
            Query results
            
        Example:
            ```python
            # Query specific columns
            results = reader.query_columns("name", "email", "age")
            
            # With WHERE clause
            results = reader.query_columns("name", "email", where="age > 25")
            
            # With LIMIT
            results = reader.query_columns("*", where="city = 'NYC'", limit=100)
            ```
        """
        # Build SELECT statement
        if not columns:
            columns = ("*",)
        
        column_list = ", ".join(columns)
        sql = f"SELECT {column_list} FROM {self.table_name}"
        
        if where:
            sql += f" WHERE {where}"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.query(sql)
    
    def query_columns_df(self, *columns: str, where: str = None, limit: int = None):
        """
        Query specific columns and return as pandas DataFrame.
        
        Args:
            *columns: Column names to select
            where: Optional WHERE clause
            limit: Optional LIMIT
            
        Returns:
            pandas.DataFrame with results
        """
        # Build SQL and execute
        sql = self._build_column_query(columns, where, limit)
        return self.query_df(sql)
    
    def sample(self, n: int = 10) -> List[Dict[str, Any]]:
        """
        Get a sample of rows from the SSTable.
        
        Args:
            n: Number of sample rows to return
            
        Returns:
            Sample rows
        """
        return self.query(f"SELECT * FROM {self.table_name} LIMIT {n}")
    
    def sample_df(self, n: int = 10):
        """
        Get a sample of rows as pandas DataFrame.
        
        Args:
            n: Number of sample rows
            
        Returns:
            pandas.DataFrame with sample data
        """
        return self.query_df(f"SELECT * FROM {self.table_name} LIMIT {n}")
    
    def head(self, n: int = 5) -> List[Dict[str, Any]]:
        """Get first n rows (alias for sample)."""
        return self.sample(n)
    
    def head_df(self, n: int = 5):
        """Get first n rows as DataFrame (alias for sample_df)."""
        return self.sample_df(n)
    
    def describe(self) -> Dict[str, Any]:
        """
        Get descriptive statistics about the SSTable.
        
        Returns:
            Dictionary with statistics and schema information
        """
        return {
            "table_name": self.table_name,
            "file_path": self.sstable_path,
            "schema": self.schema,
            "stats": self.stats,
            "sample_data": self.sample(3),
        }
    
    def export_all_formats(
        self, 
        sql: str, 
        output_base: str,
        formats: List[str] = None
    ) -> Dict[str, Any]:
        """
        Export query results to multiple formats simultaneously.
        
        Args:
            sql: SELECT statement to execute
            output_base: Base path for output files (extension will be added)
            formats: List of formats to export to
            
        Returns:
            Dictionary with export results for each format
        """
        if formats is None:
            formats = ["csv", "json", "parquet"]
        
        results = {}
        
        for fmt in formats:
            output_path = f"{output_base}.{fmt}"
            
            try:
                if fmt == "csv":
                    result = self.export_csv(sql, output_path)
                elif fmt == "json":
                    result = self.export_json(sql, output_path)
                elif fmt == "parquet":
                    result = self.export_parquet(sql, output_path)
                else:
                    raise ValueError(f"Unsupported format: {fmt}")
                
                results[fmt] = {
                    "success": True,
                    "output_path": output_path,
                    "stats": result,
                }
                
            except Exception as e:
                results[fmt] = {
                    "success": False,
                    "error": str(e),
                }
        
        return results
    
    def get_column_names(self) -> List[str]:
        """Get list of column names from schema."""
        schema = self.schema
        if "columns" in schema:
            return [col["name"] for col in schema["columns"]]
        return []
    
    def get_partition_keys(self) -> List[str]:
        """Get partition key column names."""
        schema = self.schema
        return schema.get("partition_keys", [])
    
    def get_clustering_keys(self) -> List[str]:
        """Get clustering key column names."""
        schema = self.schema
        return schema.get("clustering_keys", [])
    
    def get_column_type(self, column_name: str) -> Optional[str]:
        """
        Get the CQL type for a specific column.
        
        Args:
            column_name: Name of the column
            
        Returns:
            CQL type string, or None if column not found
        """
        schema = self.schema
        if "columns" in schema:
            for col in schema["columns"]:
                if col["name"] == column_name:
                    return col.get("cql_type")
        return None
    
    def validate_query(self, sql: str) -> Dict[str, Any]:
        """
        Validate a SQL query without executing it.
        
        Args:
            sql: SELECT statement to validate
            
        Returns:
            Validation results with any errors or warnings
        """
        try:
            # This would use the query parser to validate syntax
            # For now, basic validation
            sql_upper = sql.strip().upper()
            
            errors = []
            warnings = []
            
            if not sql_upper.startswith("SELECT"):
                errors.append("Only SELECT statements are supported")
            
            if "DELETE" in sql_upper or "INSERT" in sql_upper or "UPDATE" in sql_upper:
                errors.append("Only read-only SELECT operations are allowed")
            
            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings,
            }
            
        except Exception as e:
            return {
                "valid": False,
                "errors": [str(e)],
                "warnings": [],
            }
    
    def _build_column_query(self, columns, where, limit):
        """Build SQL query from column parameters."""
        if not columns:
            columns = ("*",)
        
        column_list = ", ".join(columns)
        sql = f"SELECT {column_list} FROM {self.table_name}"
        
        if where:
            sql += f" WHERE {where}"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return sql


def open_sstable(
    sstable_path: str,
    schema: Optional[Union[str, Dict[str, Any]]] = None,
    **kwargs
) -> SSTableReader:
    """
    Open an SSTable file for querying.
    
    Args:
        sstable_path: Path to SSTable Data.db file
        schema: Optional schema file path or dictionary
        **kwargs: Additional arguments for SSTableReader
        
    Returns:
        SSTableReader instance
        
    Example:
        ```python
        reader = cqlite.open_sstable("users-Data.db")
        results = reader.query("SELECT * FROM users LIMIT 10")
        ```
    """
    return SSTableReader(sstable_path, schema=schema, **kwargs)


def query_sstable(
    sstable_path: str,
    sql: str,
    schema: Optional[Union[str, Dict[str, Any]]] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Execute a query on an SSTable file (convenience function).
    
    Args:
        sstable_path: Path to SSTable Data.db file
        sql: SELECT statement to execute
        schema: Optional schema file path or dictionary
        **kwargs: Additional arguments for SSTableReader
        
    Returns:
        Query results as list of dictionaries
        
    Example:
        ```python
        results = cqlite.query_sstable("users-Data.db", "SELECT * FROM users WHERE age > 25")
        ```
    """
    with SSTableReader(sstable_path, schema=schema, **kwargs) as reader:
        return reader.query(sql)


def query_sstable_df(
    sstable_path: str,
    sql: str,
    schema: Optional[Union[str, Dict[str, Any]]] = None,
    **kwargs
):
    """
    Execute a query on an SSTable file and return pandas DataFrame.
    
    Args:
        sstable_path: Path to SSTable Data.db file  
        sql: SELECT statement to execute
        schema: Optional schema file path or dictionary
        **kwargs: Additional arguments for SSTableReader
        
    Returns:
        pandas.DataFrame with query results
        
    Example:
        ```python
        df = cqlite.query_sstable_df("users-Data.db", "SELECT * FROM users")
        print(df.head())
        ```
    """
    with SSTableReader(sstable_path, schema=schema, **kwargs) as reader:
        return reader.query_df(sql)