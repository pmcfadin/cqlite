"""
Utility functions for CQLite Python bindings.

This module provides helper functions for common tasks when working with
SSTable files and query results.
"""

import json
import sys
from typing import List, Dict, Any, Optional, Union
from pathlib import Path


def format_query_results(
    results: List[Dict[str, Any]], 
    format_type: str = "table",
    max_rows: Optional[int] = None,
    max_width: int = 80
) -> str:
    """
    Format query results for display.
    
    Args:
        results: Query results as list of dictionaries
        format_type: Format type ("table", "json", "csv")
        max_rows: Maximum number of rows to display
        max_width: Maximum width for table format
        
    Returns:
        Formatted string representation
    """
    if not results:
        return "No results found."
    
    if max_rows:
        results = results[:max_rows]
    
    if format_type == "json":
        return json.dumps(results, indent=2, default=str)
    
    elif format_type == "csv":
        if not results:
            return ""
        
        # Get headers from first row
        headers = list(results[0].keys())
        lines = [",".join(headers)]
        
        for row in results:
            values = [str(row.get(h, "")) for h in headers]
            lines.append(",".join(values))
        
        return "\n".join(lines)
    
    elif format_type == "table":
        return _format_as_table(results, max_width)
    
    else:
        raise ValueError(f"Unsupported format type: {format_type}")


def _format_as_table(results: List[Dict[str, Any]], max_width: int) -> str:
    """Format results as a ASCII table."""
    if not results:
        return "No results found."
    
    # Get all unique columns
    all_columns = set()
    for row in results:
        all_columns.update(row.keys())
    columns = sorted(all_columns)
    
    # Calculate column widths
    col_widths = {}
    for col in columns:
        # Start with column name length
        col_widths[col] = len(col)
        
        # Check data widths
        for row in results:
            value_str = str(row.get(col, ""))
            col_widths[col] = max(col_widths[col], len(value_str))
    
    # Limit column widths to fit max_width
    total_width = sum(col_widths.values()) + len(columns) * 3 + 1
    if total_width > max_width:
        # Reduce column widths proportionally
        available_width = max_width - len(columns) * 3 - 1
        scale_factor = available_width / sum(col_widths.values())
        for col in columns:
            col_widths[col] = max(5, int(col_widths[col] * scale_factor))
    
    # Build table
    lines = []
    
    # Header
    header_parts = []
    separator_parts = []
    for col in columns:
        width = col_widths[col]
        header_parts.append(f" {col:<{width}} ")
        separator_parts.append("-" * (width + 2))
    
    lines.append("|" + "|".join(header_parts) + "|")
    lines.append("+" + "+".join(separator_parts) + "+")
    
    # Data rows
    for row in results:
        row_parts = []
        for col in columns:
            value = row.get(col, "")
            value_str = str(value)
            width = col_widths[col]
            
            # Truncate if too long
            if len(value_str) > width:
                value_str = value_str[:width-3] + "..."
            
            row_parts.append(f" {value_str:<{width}} ")
        
        lines.append("|" + "|".join(row_parts) + "|")
    
    return "\n".join(lines)


def estimate_memory_usage(
    row_count: int,
    columns: List[Dict[str, Any]],
    compression_ratio: float = 0.3
) -> Dict[str, Any]:
    """
    Estimate memory usage for query results.
    
    Args:
        row_count: Number of rows
        columns: Column definitions with types
        compression_ratio: Estimated compression ratio for SSTable
        
    Returns:
        Dictionary with memory estimates
    """
    # Rough estimates for different CQL types (bytes per value)
    type_sizes = {
        "text": 50,       # Average string length
        "varchar": 50,
        "ascii": 30,
        "int": 4,
        "smallint": 2,
        "tinyint": 1,
        "bigint": 8,
        "varint": 8,
        "float": 4,
        "double": 8,
        "decimal": 16,
        "boolean": 1,
        "blob": 100,      # Average blob size
        "uuid": 16,
        "timeuuid": 16,
        "timestamp": 8,
        "date": 4,
        "time": 8,
        "inet": 16,
        "list": 200,      # Average collection size
        "set": 200,
        "map": 300,
        "tuple": 100,
        "udt": 200,
        "counter": 8,
    }
    
    # Calculate row size
    row_size = 0
    for col in columns:
        col_type = col.get("cql_type", "text").split("<")[0].lower()
        row_size += type_sizes.get(col_type, 50)  # Default to 50 bytes
    
    # Add overhead for Python objects
    python_overhead = row_size * 2  # Python dict/object overhead
    total_row_size = row_size + python_overhead
    
    # Calculate totals
    raw_size = row_count * total_row_size
    compressed_size = int(raw_size * compression_ratio)
    
    return {
        "row_count": row_count,
        "estimated_row_size_bytes": total_row_size,
        "estimated_total_size_bytes": raw_size,
        "estimated_compressed_size_bytes": compressed_size,
        "estimated_total_size_mb": raw_size / (1024 * 1024),
        "estimated_compressed_size_mb": compressed_size / (1024 * 1024),
        "compression_ratio": compression_ratio,
        "memory_efficiency_tips": _get_memory_tips(raw_size),
    }


def _get_memory_tips(size_bytes: int) -> List[str]:
    """Get memory optimization tips based on estimated size."""
    tips = []
    
    size_mb = size_bytes / (1024 * 1024)
    
    if size_mb > 1000:  # > 1GB
        tips.extend([
            "Use query_async() for streaming large datasets",
            "Consider adding LIMIT clause to reduce memory usage",
            "Use specific column selection instead of SELECT *",
            "Export directly to files instead of loading in memory",
        ])
    elif size_mb > 100:  # > 100MB
        tips.extend([
            "Consider using streaming with query_async()",
            "Use pandas DataFrame for efficient memory usage",
            "Consider limiting the number of rows returned",
        ])
    
    return tips


def optimize_query(sql: str, available_columns: List[str]) -> Dict[str, Any]:
    """
    Analyze and suggest optimizations for a SQL query.
    
    Args:
        sql: SQL query to analyze
        available_columns: List of available column names
        
    Returns:
        Dictionary with optimization suggestions
    """
    sql_upper = sql.strip().upper()
    suggestions = []
    issues = []
    
    # Check for SELECT *
    if "SELECT *" in sql_upper:
        suggestions.append("Consider selecting specific columns instead of * for better performance")
    
    # Check if query has LIMIT
    if "LIMIT" not in sql_upper:
        suggestions.append("Consider adding LIMIT clause for large tables")
    
    # Check for column existence (basic check)
    sql_columns = _extract_columns_from_query(sql)
    for col in sql_columns:
        if col != "*" and col.lower() not in [c.lower() for c in available_columns]:
            issues.append(f"Column '{col}' may not exist in the table")
    
    # Check for potentially slow operations
    if any(keyword in sql_upper for keyword in ["LIKE", "CONTAINS"]):
        suggestions.append("String matching operations may be slow on large datasets")
    
    # Suggest using partition keys in WHERE clauses
    suggestions.append("Use partition key columns in WHERE clause for best performance")
    
    return {
        "original_query": sql,
        "suggestions": suggestions,
        "issues": issues,
        "optimized": len(issues) == 0,
    }


def _extract_columns_from_query(sql: str) -> List[str]:
    """Extract column names from SELECT clause (basic parsing)."""
    sql_upper = sql.strip().upper()
    
    if not sql_upper.startswith("SELECT"):
        return []
    
    # Find SELECT and FROM
    select_pos = sql_upper.find("SELECT") + 6
    from_pos = sql_upper.find(" FROM ")
    
    if from_pos == -1:
        return []
    
    # Extract column part
    columns_part = sql[select_pos:from_pos].strip()
    
    # Simple parsing (doesn't handle complex expressions)
    if columns_part == "*":
        return ["*"]
    
    # Split by comma and clean up
    columns = []
    for col in columns_part.split(","):
        col = col.strip()
        # Remove aliases (basic)
        if " AS " in col.upper():
            col = col.split(" AS ")[0].strip()
        if " " in col:
            col = col.split()[0].strip()
        columns.append(col)
    
    return columns


def create_schema_from_cql(cql_statement: str) -> Dict[str, Any]:
    """
    Parse a CREATE TABLE statement and extract schema information.
    
    Args:
        cql_statement: CQL CREATE TABLE statement
        
    Returns:
        Schema dictionary
    """
    # This is a simplified parser - a real implementation would use a proper CQL parser
    lines = cql_statement.strip().split("\n")
    
    schema = {
        "keyspace": "unknown",
        "table": "unknown",
        "columns": [],
        "partition_keys": [],
        "clustering_keys": [],
    }
    
    # Extract table name
    for line in lines:
        line = line.strip().upper()
        if line.startswith("CREATE TABLE"):
            # Basic extraction: CREATE TABLE keyspace.table
            parts = line.split()
            if len(parts) >= 3:
                table_part = parts[2]
                if "." in table_part:
                    keyspace, table = table_part.split(".", 1)
                    schema["keyspace"] = keyspace.strip("(")
                    schema["table"] = table.strip("(")
                else:
                    schema["table"] = table_part.strip("(")
            break
    
    # Extract columns (very basic parsing)
    in_columns = False
    for line in lines:
        line = line.strip()
        if "(" in line and not in_columns:
            in_columns = True
            continue
        
        if in_columns and ")" in line:
            break
        
        if in_columns and line and not line.startswith("PRIMARY KEY"):
            # Parse column definition: column_name column_type,
            parts = line.split()
            if len(parts) >= 2:
                col_name = parts[0].strip(",")
                col_type = parts[1].strip(",")
                
                schema["columns"].append({
                    "name": col_name,
                    "cql_type": col_type,
                    "python_type": "str",  # Would be determined by type mapping
                    "nullable": True,
                    "is_partition_key": False,
                    "is_clustering_key": False,
                })
    
    return schema


def validate_sstable_path(sstable_path: str) -> Dict[str, Any]:
    """
    Validate that an SSTable path is valid.
    
    Args:
        sstable_path: Path to SSTable file
        
    Returns:
        Validation result dictionary
    """
    path = Path(sstable_path)
    
    errors = []
    warnings = []
    
    # Check if file exists
    if not path.exists():
        errors.append(f"File does not exist: {sstable_path}")
        return {"valid": False, "errors": errors, "warnings": warnings}
    
    # Check if it's a file
    if not path.is_file():
        errors.append(f"Path is not a file: {sstable_path}")
    
    # Check file extension
    if not path.name.endswith("-Data.db"):
        errors.append("File does not have SSTable Data.db extension")
    
    # Check file size
    try:
        size = path.stat().st_size
        if size == 0:
            warnings.append("SSTable file is empty")
        elif size < 1024:
            warnings.append("SSTable file is very small (< 1KB)")
    except OSError as e:
        warnings.append(f"Could not check file size: {e}")
    
    # Look for companion files
    base_name = path.name[:-8]  # Remove "-Data.db"
    parent_dir = path.parent
    
    companion_files = {
        "Index.db": f"{base_name}-Index.db",
        "Statistics.db": f"{base_name}-Statistics.db", 
        "CompressionInfo.db": f"{base_name}-CompressionInfo.db",
        "Filter.db": f"{base_name}-Filter.db",
        "Summary.db": f"{base_name}-Summary.db",
    }
    
    missing_companions = []
    for file_type, filename in companion_files.items():
        companion_path = parent_dir / filename
        if not companion_path.exists():
            missing_companions.append(file_type)
    
    if missing_companions:
        warnings.append(f"Missing companion files: {', '.join(missing_companions)}")
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "file_size_bytes": path.stat().st_size if path.exists() else 0,
        "missing_companions": missing_companions,
    }


def get_system_info() -> Dict[str, Any]:
    """Get system information relevant to CQLite performance."""
    import platform
    import psutil
    
    return {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": psutil.cpu_count(),
        "memory_total_gb": psutil.virtual_memory().total / (1024**3),
        "memory_available_gb": psutil.virtual_memory().available / (1024**3),
        "disk_usage": {
            path: {
                "total_gb": psutil.disk_usage(path).total / (1024**3),
                "free_gb": psutil.disk_usage(path).free / (1024**3),
            }
            for path in ["/", "/tmp"] if Path(path).exists()
        }
    }


def benchmark_query_performance(reader, sql: str, iterations: int = 3) -> Dict[str, Any]:
    """
    Benchmark query performance.
    
    Args:
        reader: SSTableReader instance
        sql: SQL query to benchmark
        iterations: Number of iterations to run
        
    Returns:
        Performance metrics
    """
    import time
    
    times = []
    results_count = None
    
    for i in range(iterations):
        start_time = time.time()
        results = reader.query(sql)
        end_time = time.time()
        
        execution_time = end_time - start_time
        times.append(execution_time)
        
        if results_count is None:
            results_count = len(results)
    
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    
    return {
        "sql": sql,
        "iterations": iterations,
        "results_count": results_count,
        "avg_time_seconds": avg_time,
        "min_time_seconds": min_time,
        "max_time_seconds": max_time,
        "rows_per_second": results_count / avg_time if avg_time > 0 else 0,
        "all_times": times,
    }


# Utility for pretty printing results
def pprint_results(results: List[Dict[str, Any]], max_rows: int = 20):
    """Pretty print query results to console."""
    if not results:
        print("No results found.")
        return
    
    print(f"\nQuery Results ({len(results)} rows):")
    print("=" * 50)
    
    formatted = format_query_results(results, "table", max_rows)
    print(formatted)
    
    if len(results) > max_rows:
        print(f"\n... and {len(results) - max_rows} more rows")


if __name__ == "__main__":
    # Test utility functions
    sample_results = [
        {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
    ]
    
    print("Table format:")
    print(format_query_results(sample_results, "table"))
    
    print("\nJSON format:")
    print(format_query_results(sample_results, "json"))
    
    print("\nCSV format:")
    print(format_query_results(sample_results, "csv"))