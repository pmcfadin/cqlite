#!/usr/bin/env python3
"""
🚀 CQLite Basic Usage Examples

This script demonstrates the revolutionary capability of querying Cassandra
SSTable files directly with Python, without running Cassandra!

This is the FIRST EVER Python library to provide this functionality!
"""

import cqlite
import tempfile
import json
from pathlib import Path


def create_mock_sstable():
    """Create a mock SSTable file for demonstration."""
    temp_dir = tempfile.mkdtemp()
    sstable_path = Path(temp_dir) / "users-big-Data.db"
    
    # Create empty file (in real usage, this would be an actual SSTable file)
    sstable_path.touch()
    
    return str(sstable_path)


def example_basic_querying():
    """Demonstrate basic SSTable querying."""
    print("🚀 Example 1: Basic SSTable Querying")
    print("=" * 50)
    
    # In real usage, you would point to an actual SSTable file:
    # sstable_path = "/path/to/cassandra/data/keyspace/table/users-big-Data.db"
    sstable_path = create_mock_sstable()
    
    try:
        # Open SSTable file for querying
        with cqlite.SSTableReader(sstable_path) as reader:
            print(f"📁 Opened SSTable: {sstable_path}")
            print(f"📊 Table: {reader.table_name}")
            
            # Execute SELECT queries directly on the SSTable!
            print("\n🔍 Executing: SELECT * FROM users LIMIT 5")
            results = reader.query("SELECT * FROM users LIMIT 5")
            
            print(f"✅ Found {len(results)} rows:")
            for i, row in enumerate(results, 1):
                print(f"   Row {i}: {row}")
            
            # Query with WHERE clause
            print("\n🔍 Executing: SELECT name, email FROM users WHERE age > 25")
            results = reader.query("SELECT name, email FROM users WHERE age > 25")
            
            print(f"✅ Found {len(results)} users over 25:")
            for row in results:
                print(f"   {row}")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_pandas_integration():
    """Demonstrate pandas DataFrame integration."""
    print("🐼 Example 2: Pandas DataFrame Integration")
    print("=" * 50)
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Get results as pandas DataFrame
            print("🔍 Executing: SELECT * FROM users")
            df = reader.query_df("SELECT * FROM users")
            
            print("✅ DataFrame created!")
            print(f"   Shape: {df.shape}")
            print(f"   Columns: {list(df.columns)}")
            
            # Use pandas operations
            print("\n📊 DataFrame operations:")
            print(f"   First 5 rows:\n{df.head()}")
            
            if 'age' in df.columns:
                print(f"   Average age: {df['age'].mean():.1f}")
                print(f"   Age distribution:\n{df['age'].value_counts().head()}")
    
    except ImportError:
        print("❌ Pandas not available. Install with: pip install pandas")
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_schema_discovery():
    """Demonstrate schema discovery and validation."""
    print("🔍 Example 3: Schema Discovery and Validation")
    print("=" * 50)
    
    sstable_path = create_mock_sstable()
    
    try:
        # Discover SSTable files in directory
        directory = Path(sstable_path).parent
        print(f"📁 Scanning directory: {directory}")
        
        sstables = cqlite.discover_sstables(str(directory))
        print(f"✅ Found {len(sstables)} SSTable files:")
        
        for sstable in sstables:
            print(f"   📄 {sstable['name']} ({sstable['size_bytes']} bytes)")
            
            # Validate each SSTable
            validation = cqlite.validate_sstable(sstable['path'])
            if validation['valid']:
                print(f"      ✅ Valid SSTable")
            else:
                print(f"      ❌ Issues: {validation['errors']}")
        
        # Get schema information
        with cqlite.SSTableReader(sstable_path) as reader:
            print(f"\n📋 Schema for {reader.table_name}:")
            schema = reader.get_schema()
            
            print(f"   Keyspace: {schema.get('keyspace', 'unknown')}")
            print(f"   Table: {schema.get('table', 'unknown')}")
            print(f"   Columns: {len(schema.get('columns', []))}")
            
            # Get column names
            columns = reader.get_column_names()
            print(f"   Column names: {columns}")
            
            # Get partition keys
            partition_keys = reader.get_partition_keys()
            print(f"   Partition keys: {partition_keys}")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_export_formats():
    """Demonstrate exporting to different formats."""
    print("📤 Example 4: Export to Multiple Formats")
    print("=" * 50)
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Export to CSV
            print("📄 Exporting to CSV...")
            csv_result = reader.export_csv(
                "SELECT * FROM users", 
                "/tmp/users.csv"
            )
            print(f"   ✅ CSV: {csv_result}")
            
            # Export to JSON
            print("📄 Exporting to JSON...")
            json_result = reader.export_json(
                "SELECT * FROM users", 
                "/tmp/users.json"
            )
            print(f"   ✅ JSON: {json_result}")
            
            # Export to multiple formats at once
            print("📄 Exporting to multiple formats...")
            multi_result = reader.export_all_formats(
                "SELECT * FROM users WHERE active = true",
                "/tmp/active_users",
                formats=["csv", "json"]
            )
            
            print("   ✅ Multi-format export:")
            for format_name, result in multi_result.items():
                status = "✅" if result['success'] else "❌"
                print(f"      {status} {format_name.upper()}: {result}")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_query_optimization():
    """Demonstrate query optimization features."""
    print("⚡ Example 5: Query Optimization")
    print("=" * 50)
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Validate query before execution
            sql = "SELECT name, email FROM users WHERE age > 25 AND city = 'NYC'"
            print(f"🔍 Validating query: {sql}")
            
            validation = reader.validate_query(sql)
            print(f"   Valid: {validation['valid']}")
            
            if validation['errors']:
                print(f"   Errors: {validation['errors']}")
            
            if validation['warnings']:
                print(f"   Warnings: {validation['warnings']}")
            
            # Get optimization suggestions
            from cqlite.utils import optimize_query
            
            available_columns = reader.get_column_names()
            optimization = optimize_query(sql, available_columns)
            
            print(f"\n💡 Optimization suggestions:")
            for suggestion in optimization['suggestions']:
                print(f"   • {suggestion}")
            
            if optimization['issues']:
                print(f"\n⚠️  Issues found:")
                for issue in optimization['issues']:
                    print(f"   • {issue}")
            
            # Benchmark query performance
            print(f"\n📊 Performance benchmark:")
            from cqlite.utils import benchmark_query_performance
            
            perf = benchmark_query_performance(reader, sql, iterations=3)
            print(f"   Average time: {perf['avg_time_seconds']:.3f} seconds")
            print(f"   Rows per second: {perf['rows_per_second']:.0f}")
            print(f"   Results: {perf['results_count']} rows")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_convenience_functions():
    """Demonstrate convenience functions."""
    print("🛠️  Example 6: Convenience Functions")
    print("=" * 50)
    
    sstable_path = create_mock_sstable()
    
    try:
        # Quick one-liner queries
        print("🔍 Quick query (one-liner):")
        results = cqlite.quick_query(sstable_path, "SELECT * FROM users LIMIT 3")
        print(f"   ✅ Found {len(results)} rows")
        
        # Feature detection
        print("\n🔍 Available features:")
        features = cqlite.get_available_features()
        for feature, available in features.items():
            status = "✅" if available else "❌"
            print(f"   {status} {feature}")
        
        # Check dependencies
        print("\n📦 Dependency check:")
        cqlite.check_dependencies()
        
        # Get system info for performance optimization
        print("\n💻 System information:")
        from cqlite.utils import get_system_info
        sys_info = get_system_info()
        print(f"   Platform: {sys_info['platform']}")
        print(f"   Python: {sys_info['python_version']}")
        print(f"   CPU cores: {sys_info['cpu_count']}")
        print(f"   Memory: {sys_info['memory_total_gb']:.1f} GB total")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_advanced_usage():
    """Demonstrate advanced usage patterns."""
    print("🔥 Example 7: Advanced Usage Patterns")
    print("=" * 50)
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Enhanced query methods
            print("🔍 Enhanced query methods:")
            
            # Query one row
            first_user = reader.query_one("SELECT * FROM users LIMIT 1")
            print(f"   First user: {first_user}")
            
            # Query count
            user_count = reader.query_count("SELECT * FROM users")
            print(f"   Total users: {user_count}")
            
            # Check if results exist
            has_admins = reader.query_exists("SELECT * FROM users WHERE role = 'admin'")
            print(f"   Has admin users: {has_admins}")
            
            # Query specific columns with convenience method
            results = reader.query_columns("name", "email", where="age > 21", limit=5)
            print(f"   Adult users: {len(results)} found")
            
            # Get table statistics
            print(f"\n📊 Table statistics:")
            stats = reader.get_stats()
            print(f"   File size: {stats.get('file_size_mb', 0):.1f} MB")
            print(f"   Estimated rows: {stats.get('estimated_rows', 0)}")
            
            # Get comprehensive description
            print(f"\n📋 Table description:")
            description = reader.describe()
            print(f"   Table: {description['table_name']}")
            print(f"   Schema columns: {len(description['schema'].get('columns', []))}")
            print(f"   Sample data: {len(description['sample_data'])} rows")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def main():
    """Run all examples."""
    print("🚀 CQLite Python - Revolutionary SSTable Querying Examples")
    print("🎯 The WORLD'S FIRST Python library for direct SSTable querying!")
    print("=" * 70)
    print()
    
    # Check if CQLite is properly installed
    try:
        print(f"📦 CQLite version: {cqlite.__version__}")
        print(f"👨‍💻 Author: {cqlite.__author__}")
        print(f"📖 Description: {cqlite.__description__}")
        print()
    except AttributeError:
        print("❌ CQLite metadata not available")
        print()
    
    # Run examples
    example_basic_querying()
    example_pandas_integration()
    example_schema_discovery()
    example_export_formats()
    example_query_optimization()
    example_convenience_functions()
    example_advanced_usage()
    
    print("🎉 All examples completed!")
    print("\n💡 Next steps:")
    print("   1. Try with real Cassandra SSTable files")
    print("   2. Explore async querying for large datasets")
    print("   3. Integrate with your data analysis workflows")
    print("   4. Export data to your preferred formats")
    print("\n📚 Documentation: https://docs.cqlite.dev")
    print("🐛 Issues: https://github.com/cqlite/cqlite/issues")
    print("⭐ Star us: https://github.com/cqlite/cqlite")


if __name__ == "__main__":
    main()