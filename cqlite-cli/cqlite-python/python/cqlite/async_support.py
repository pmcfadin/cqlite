"""
Async support for CQLite Python bindings.

This module provides high-level async Python APIs on top of the core
async functionality implemented in Rust.
"""

import asyncio
from typing import List, Dict, Any, Optional, AsyncIterator, Union
from ._core import AsyncQueryIterator, AsyncQueryExecutor, AsyncBatchProcessor


class AsyncSSTableReader:
    """
    Async wrapper for SSTable reading operations.
    
    This class provides async Python APIs for memory-efficient processing
    of large SSTable files.
    """
    
    def __init__(
        self,
        sstable_path: str,
        schema: Optional[Union[str, Dict[str, Any]]] = None,
        max_concurrent: int = 4,
        chunk_size: int = 1000,
    ):
        """
        Create async SSTable reader.
        
        Args:
            sstable_path: Path to SSTable Data.db file
            schema: Optional schema file path or dictionary
            max_concurrent: Maximum concurrent operations
            chunk_size: Default chunk size for streaming
        """
        self.sstable_path = sstable_path
        self.schema = schema
        self.max_concurrent = max_concurrent
        self.chunk_size = chunk_size
        
        # Initialize executor
        self.executor = AsyncQueryExecutor(sstable_path, max_concurrent)
    
    async def query(self, sql: str, timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Execute async query with optional timeout.
        
        Args:
            sql: SELECT statement to execute
            timeout: Query timeout in seconds
            
        Returns:
            Query results
        """
        if timeout:
            return await self.executor.execute_with_timeout(sql, timeout)
        else:
            # Use executor directly for simple query
            return await self.executor.execute_with_timeout(sql, 300.0)  # Default 5 min timeout
    
    async def query_streaming(
        self, 
        sql: str, 
        chunk_size: Optional[int] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Execute query with streaming results.
        
        Args:
            sql: SELECT statement to execute
            chunk_size: Number of rows per chunk
            
        Yields:
            Individual result rows
        """
        chunk_size = chunk_size or self.chunk_size
        iterator = AsyncQueryIterator(self.sstable_path, sql, chunk_size)
        
        async for row in iterator:
            yield row
    
    async def query_chunks(
        self, 
        sql: str, 
        chunk_size: Optional[int] = None
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Execute query and yield chunks of results.
        
        Args:
            sql: SELECT statement to execute
            chunk_size: Number of rows per chunk
            
        Yields:
            Chunks of result rows
        """
        chunk_size = chunk_size or self.chunk_size
        iterator = AsyncQueryIterator(self.sstable_path, sql, chunk_size)
        
        chunk = []
        async for row in iterator:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        
        # Yield final partial chunk
        if chunk:
            yield chunk
    
    async def query_with_progress(
        self, 
        sql: str, 
        progress_callback: Optional[callable] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute query with progress reporting.
        
        Args:
            sql: SELECT statement to execute
            progress_callback: Function called with (progress, message)
            
        Returns:
            Query results
        """
        if progress_callback is None:
            # Default progress callback
            progress_callback = lambda p, m: print(f"Progress: {p:.1%} - {m}")
        
        return await self.executor.execute_with_progress(sql, progress_callback)
    
    async def count(self, sql: str) -> int:
        """
        Get count of rows matching query.
        
        Args:
            sql: SELECT statement (will be converted to COUNT)
            
        Returns:
            Number of matching rows
        """
        # Convert to COUNT query
        if sql.strip().upper().startswith("SELECT"):
            parts = sql.split(" FROM ", 1)
            if len(parts) == 2:
                count_sql = f"SELECT COUNT(*) FROM {parts[1]}"
                results = await self.query(count_sql)
                if results and 'count' in results[0]:
                    return results[0]['count']
        
        # Fallback: count results
        results = await self.query(sql)
        return len(results)
    
    async def exists(self, sql: str) -> bool:
        """
        Check if query returns any results.
        
        Args:
            sql: SELECT statement to check
            
        Returns:
            True if query returns at least one row
        """
        results = await self.query(f"{sql} LIMIT 1")
        return len(results) > 0
    
    async def sample(self, n: int = 10) -> List[Dict[str, Any]]:
        """
        Get sample rows from SSTable.
        
        Args:
            n: Number of sample rows
            
        Returns:
            Sample rows
        """
        return await self.query(f"SELECT * FROM table LIMIT {n}")
    
    async def export_async(
        self, 
        sql: str, 
        output_path: str, 
        format: str = "csv"
    ) -> Dict[str, Any]:
        """
        Export query results asynchronously.
        
        Args:
            sql: SELECT statement to execute
            output_path: Output file path
            format: Export format ("csv", "json", "parquet")
            
        Returns:
            Export statistics
        """
        # This would use async file operations
        # For now, delegate to sync operations
        import aiofiles
        
        results = await self.query(sql)
        
        if format == "csv":
            return await self._export_csv_async(results, output_path)
        elif format == "json":
            return await self._export_json_async(results, output_path)
        else:
            raise ValueError(f"Unsupported async export format: {format}")
    
    async def _export_csv_async(self, results: List[Dict[str, Any]], output_path: str) -> Dict[str, Any]:
        """Export results to CSV asynchronously."""
        import aiofiles
        import csv
        import io
        
        if not results:
            return {"rows_written": 0, "file_size_bytes": 0}
        
        # Build CSV content in memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
        
        csv_content = output.getvalue()
        
        # Write to file asynchronously
        async with aiofiles.open(output_path, 'w') as f:
            await f.write(csv_content)
        
        return {
            "format": "csv",
            "output_path": output_path,
            "rows_written": len(results),
            "file_size_bytes": len(csv_content.encode('utf-8')),
        }
    
    async def _export_json_async(self, results: List[Dict[str, Any]], output_path: str) -> Dict[str, Any]:
        """Export results to JSON asynchronously."""
        import aiofiles
        import json
        
        json_content = json.dumps(results, indent=2, default=str)
        
        async with aiofiles.open(output_path, 'w') as f:
            await f.write(json_content)
        
        return {
            "format": "json",
            "output_path": output_path,
            "rows_written": len(results),
            "file_size_bytes": len(json_content.encode('utf-8')),
        }
    
    async def __aenter__(self):
        """Async context manager support."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager cleanup."""
        # Cleanup if needed
        pass


async def stream_query_results(
    sstable_path: str,
    sql: str,
    chunk_size: int = 1000,
    max_memory_mb: int = 100
) -> AsyncIterator[Dict[str, Any]]:
    """
    Stream query results with automatic memory management.
    
    Args:
        sstable_path: Path to SSTable file
        sql: SELECT statement to execute
        chunk_size: Number of rows per chunk
        max_memory_mb: Maximum memory usage
        
    Yields:
        Individual result rows
    """
    async with AsyncSSTableReader(sstable_path, chunk_size=chunk_size) as reader:
        async for row in reader.query_streaming(sql, chunk_size):
            yield row


async def process_multiple_sstables(
    sstable_paths: List[str],
    sql: str,
    max_concurrent: int = 4,
    aggregate: bool = False
) -> Union[List[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Process the same query across multiple SSTable files.
    
    Args:
        sstable_paths: List of SSTable file paths
        sql: SELECT statement to execute on each
        max_concurrent: Maximum concurrent operations
        aggregate: Whether to aggregate results or return separately
        
    Returns:
        Results from each SSTable (or aggregated if aggregate=True)
    """
    processor = AsyncBatchProcessor(sstable_paths, max_concurrent)
    
    if aggregate:
        return await processor.process_all(sql)
    else:
        # Process each file separately
        tasks = []
        for path in sstable_paths:
            async with AsyncSSTableReader(path, max_concurrent=1) as reader:
                task = asyncio.create_task(reader.query(sql))
                tasks.append(task)
        
        return await asyncio.gather(*tasks)


async def parallel_query_execution(
    sstable_path: str,
    queries: List[str],
    max_concurrent: int = 4
) -> List[List[Dict[str, Any]]]:
    """
    Execute multiple queries in parallel on the same SSTable.
    
    Args:
        sstable_path: Path to SSTable file
        queries: List of SELECT statements
        max_concurrent: Maximum concurrent queries
        
    Returns:
        Results for each query
    """
    executor = AsyncQueryExecutor(sstable_path, max_concurrent)
    return await executor.execute_concurrent(queries)


async def benchmark_async_performance(
    sstable_path: str,
    sql: str,
    iterations: int = 3,
    concurrent: bool = False
) -> Dict[str, Any]:
    """
    Benchmark async query performance.
    
    Args:
        sstable_path: Path to SSTable file
        sql: Query to benchmark
        iterations: Number of iterations
        concurrent: Whether to run iterations concurrently
        
    Returns:
        Performance metrics
    """
    import time
    
    async def single_query():
        async with AsyncSSTableReader(sstable_path) as reader:
            start_time = time.time()
            results = await reader.query(sql)
            end_time = time.time()
            return end_time - start_time, len(results)
    
    if concurrent:
        # Run all iterations concurrently
        tasks = [single_query() for _ in range(iterations)]
        results = await asyncio.gather(*tasks)
        times = [r[0] for r in results]
        result_count = results[0][1] if results else 0
    else:
        # Run iterations sequentially
        times = []
        result_count = 0
        for _ in range(iterations):
            exec_time, count = await single_query()
            times.append(exec_time)
            result_count = count
    
    avg_time = sum(times) / len(times)
    
    return {
        "sql": sql,
        "iterations": iterations,
        "concurrent": concurrent,
        "result_count": result_count,
        "avg_time_seconds": avg_time,
        "min_time_seconds": min(times),
        "max_time_seconds": max(times),
        "rows_per_second": result_count / avg_time if avg_time > 0 else 0,
        "all_times": times,
    }


# Example usage functions
async def example_streaming_usage():
    """Example of streaming large dataset processing."""
    sstable_path = "large-table-Data.db"
    sql = "SELECT * FROM large_table WHERE created_date > '2023-01-01'"
    
    print("Processing large dataset with streaming...")
    
    row_count = 0
    async for row in stream_query_results(sstable_path, sql, chunk_size=1000):
        # Process each row
        row_count += 1
        
        if row_count % 10000 == 0:
            print(f"Processed {row_count} rows...")
    
    print(f"Total rows processed: {row_count}")


async def example_batch_processing():
    """Example of batch processing multiple SSTable files."""
    sstable_paths = [
        "users-shard1-Data.db",
        "users-shard2-Data.db", 
        "users-shard3-Data.db",
    ]
    
    sql = "SELECT COUNT(*) as user_count FROM users"
    
    print("Processing multiple SSTable files...")
    
    # Get results from each shard
    results = await process_multiple_sstables(sstable_paths, sql, aggregate=False)
    
    total_users = 0
    for i, shard_result in enumerate(results):
        count = shard_result[0]['user_count'] if shard_result else 0
        print(f"Shard {i+1}: {count} users")
        total_users += count
    
    print(f"Total users across all shards: {total_users}")


async def example_concurrent_queries():
    """Example of running multiple queries concurrently."""
    sstable_path = "analytics-Data.db"
    
    queries = [
        "SELECT COUNT(*) FROM events WHERE event_type = 'login'",
        "SELECT COUNT(*) FROM events WHERE event_type = 'purchase'", 
        "SELECT COUNT(*) FROM events WHERE event_type = 'signup'",
        "SELECT AVG(session_duration) FROM events WHERE event_type = 'session'",
    ]
    
    print("Running concurrent analytics queries...")
    
    results = await parallel_query_execution(sstable_path, queries, max_concurrent=4)
    
    metrics = ["Login events", "Purchase events", "Signup events", "Avg session duration"]
    for i, (metric, result) in enumerate(zip(metrics, results)):
        value = result[0] if result else {"value": "N/A"}
        print(f"{metric}: {value}")


if __name__ == "__main__":
    # Run examples
    async def main():
        print("🚀 CQLite Async Examples")
        print("=" * 40)
        
        try:
            await example_streaming_usage()
            print()
            await example_batch_processing()
            print()
            await example_concurrent_queries()
        except Exception as e:
            print(f"Example requires actual SSTable files: {e}")
    
    asyncio.run(main())