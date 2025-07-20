"""
Basic tests for CQLite Python bindings.

These tests verify core functionality of the revolutionary SSTable querying library.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch

# Import CQLite components
import cqlite
from cqlite import SSTableReader, CQLiteError, QueryError, SSTableError
from cqlite.types import CQLValue, CQLiteRow, convert_cql_value


class TestSSTableReader:
    """Test the main SSTableReader class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create a temporary mock SSTable file
        self.temp_dir = tempfile.mkdtemp()
        self.mock_sstable_path = os.path.join(self.temp_dir, "test-users-ka-1-Data.db")
        
        # Create empty file to satisfy existence checks
        Path(self.mock_sstable_path).touch()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_sstable_reader_creation(self):
        """Test creating SSTableReader instance."""
        # This will create the reader but the actual query execution
        # will be mocked since we don't have real SSTable data
        reader = SSTableReader(self.mock_sstable_path)
        
        assert reader.sstable_path == self.mock_sstable_path
        assert reader.table_name  # Should extract some table name
    
    def test_invalid_sstable_path(self):
        """Test error handling for invalid SSTable paths."""
        with pytest.raises(SSTableError):
            SSTableReader("/nonexistent/path/file.db")
    
    def test_invalid_sstable_extension(self):
        """Test error handling for files without proper extension."""
        invalid_path = os.path.join(self.temp_dir, "not-sstable.txt")
        Path(invalid_path).touch()
        
        with pytest.raises(SSTableError):
            SSTableReader(invalid_path)
    
    @patch('cqlite._core.SSTableReader.query')
    def test_basic_query_execution(self, mock_query):
        """Test basic query execution."""
        # Mock the query response
        mock_query.return_value = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25}
        ]
        
        reader = SSTableReader(self.mock_sstable_path)
        results = reader.query("SELECT * FROM users")
        
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["age"] == 25
        
        # Verify query was called
        mock_query.assert_called_once()
    
    @patch('cqlite._core.SSTableReader.query')
    def test_query_with_limit(self, mock_query):
        """Test query execution with LIMIT."""
        mock_query.return_value = [{"id": 1, "name": "Alice"}]
        
        reader = SSTableReader(self.mock_sstable_path)
        results = reader.query("SELECT * FROM users LIMIT 1")
        
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
    
    def test_context_manager_support(self):
        """Test using SSTableReader as context manager."""
        with SSTableReader(self.mock_sstable_path) as reader:
            assert reader is not None
            assert hasattr(reader, 'query')
    
    @patch('cqlite._core.SSTableReader.get_schema')
    def test_schema_access(self, mock_get_schema):
        """Test accessing schema information."""
        mock_schema = {
            "keyspace": "test_ks",
            "table": "users", 
            "columns": [
                {"name": "id", "cql_type": "int", "is_partition_key": True},
                {"name": "name", "cql_type": "text", "is_partition_key": False}
            ]
        }
        mock_get_schema.return_value = mock_schema
        
        reader = SSTableReader(self.mock_sstable_path)
        schema = reader.schema
        
        assert schema["keyspace"] == "test_ks"
        assert schema["table"] == "users"
        assert len(schema["columns"]) == 2
    
    @patch('cqlite._core.SSTableReader.get_stats')
    def test_stats_access(self, mock_get_stats):
        """Test accessing SSTable statistics."""
        mock_stats = {
            "file_size_bytes": 1024000,
            "estimated_rows": 1000,
            "compression": "lz4"
        }
        mock_get_stats.return_value = mock_stats
        
        reader = SSTableReader(self.mock_sstable_path)
        stats = reader.stats
        
        assert stats["file_size_bytes"] == 1024000
        assert stats["estimated_rows"] == 1000


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_discover_sstables(self):
        """Test SSTable discovery in directory."""
        # Create temporary directory with mock SSTable files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock SSTable files
            sstable_files = [
                "users-ka-1-Data.db",
                "posts-ka-2-Data.db",
                "comments-ka-1-Data.db"
            ]
            
            for filename in sstable_files:
                Path(temp_dir, filename).touch()
            
            # Also create non-SSTable files to ensure they're ignored
            Path(temp_dir, "not-sstable.txt").touch()
            Path(temp_dir, "other-file.db").touch()
            
            # Discover SSTable files
            discovered = cqlite.discover_sstables(temp_dir)
            
            assert len(discovered) == 3
            
            # Check that discovered files have correct structure
            for sstable in discovered:
                assert "path" in sstable
                assert "name" in sstable
                assert sstable["name"].endswith("-Data.db")
    
    def test_validate_sstable(self):
        """Test SSTable file validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create valid SSTable file
            valid_sstable = Path(temp_dir, "test-table-ka-1-Data.db")
            valid_sstable.write_text("mock sstable data")
            
            result = cqlite.validate_sstable(str(valid_sstable))
            
            assert result["valid"] is True
            assert result["file_size"] > 0
            assert len(result["errors"]) == 0
    
    def test_validate_invalid_sstable(self):
        """Test validation of invalid SSTable file."""
        result = cqlite.validate_sstable("/nonexistent/file.db")
        
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert "not found" in result["errors"][0].lower()


class TestTypeConversion:
    """Test CQL to Python type conversion."""
    
    def test_simple_type_conversion(self):
        """Test conversion of simple CQL types."""
        # Text types
        assert convert_cql_value("hello", "text") == "hello"
        assert convert_cql_value("world", "varchar") == "world"
        
        # Numeric types
        assert convert_cql_value("123", "int") == 123
        assert convert_cql_value("456", "bigint") == 456
        assert convert_cql_value("3.14", "float") == 3.14
        assert convert_cql_value("2.718", "double") == 2.718
        
        # Boolean
        assert convert_cql_value("true", "boolean") is True
        assert convert_cql_value("false", "boolean") is False
        assert convert_cql_value("1", "boolean") is True
        assert convert_cql_value("0", "boolean") is False
    
    def test_uuid_conversion(self):
        """Test UUID type conversion."""
        import uuid
        
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        result = convert_cql_value(uuid_str, "uuid")
        
        assert isinstance(result, uuid.UUID)
        assert str(result) == uuid_str
    
    def test_timestamp_conversion(self):
        """Test timestamp conversion."""
        import datetime
        
        # ISO format
        iso_timestamp = "2023-12-25T10:30:00Z"
        result = convert_cql_value(iso_timestamp, "timestamp")
        
        assert isinstance(result, datetime.datetime)
    
    def test_collection_type_conversion(self):
        """Test conversion of collection types."""
        # List
        list_data = [1, 2, 3]
        result = convert_cql_value(list_data, "list<int>")
        assert result == [1, 2, 3]
        assert all(isinstance(x, int) for x in result)
        
        # Set
        set_data = [1, 2, 3, 2]  # Input as list with duplicates
        result = convert_cql_value(set_data, "set<int>")
        assert isinstance(result, set)
        assert result == {1, 2, 3}
        
        # Map
        map_data = {"key1": "value1", "key2": "value2"}
        result = convert_cql_value(map_data, "map<text,text>")
        assert isinstance(result, dict)
        assert result == map_data
    
    def test_null_value_handling(self):
        """Test handling of NULL values."""
        assert convert_cql_value(None, "text") is None
        assert convert_cql_value(None, "int") is None
        assert convert_cql_value(None, "list<text>") is None


class TestCQLiteRow:
    """Test CQLiteRow functionality."""
    
    def test_row_creation(self):
        """Test creating and manipulating CQLiteRow."""
        from cqlite.types import CQLiteRow, CQLValue
        
        row = CQLiteRow()
        
        # Add columns
        row.add_column("id", CQLValue.Int(1))
        row.add_column("name", CQLValue.Text("Alice"))
        row.add_column("age", CQLValue.Int(30))
        
        assert row.column_count() == 3
        assert row.get_column("name") is not None
        assert row.get_column("nonexistent") is None
        
        # Check column names
        column_names = row.column_names()
        assert "id" in column_names
        assert "name" in column_names
        assert "age" in column_names


class TestQuickUtilities:
    """Test quick utility functions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_sstable_path = os.path.join(self.temp_dir, "test-Data.db")
        Path(self.mock_sstable_path).touch()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('cqlite.SSTableReader.query')
    def test_quick_query(self, mock_query):
        """Test quick query utility function."""
        mock_query.return_value = [{"id": 1, "name": "Alice"}]
        
        results = cqlite.quick_query(self.mock_sstable_path, "SELECT * FROM test")
        
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
    
    @patch('cqlite.SSTableReader.query_df')
    def test_quick_query_df(self, mock_query_df):
        """Test quick DataFrame query utility."""
        # Mock pandas DataFrame
        mock_df = Mock()
        mock_df.shape = (1, 2)
        mock_query_df.return_value = mock_df
        
        df = cqlite.quick_query_df(self.mock_sstable_path, "SELECT * FROM test")
        
        assert df is not None
        mock_query_df.assert_called_once()


class TestErrorHandling:
    """Test error handling and exceptions."""
    
    def test_cqlite_error_creation(self):
        """Test CQLiteError exception."""
        error = CQLiteError("Test error message", "TEST_CODE")
        
        assert str(error) == "[TEST_CODE] Test error message"
        assert error.message == "Test error message"
        assert error.error_code == "TEST_CODE"
    
    def test_query_error_with_sql(self):
        """Test QueryError with SQL context."""
        sql = "SELECT invalid_column FROM table"
        error = QueryError("Column not found", sql, 7)
        
        error_str = str(error)
        assert "Column not found" in error_str
        assert sql in error_str
        assert "position 7" in error_str
    
    def test_sstable_error_with_path(self):
        """Test SSTableError with file path."""
        error = SSTableError("File corrupted", "/path/to/file.db", "corruption")
        
        error_str = str(error)
        assert "File corrupted" in error_str
        assert "/path/to/file.db" in error_str
        assert "corruption" in error_str


class TestFeatureDetection:
    """Test feature detection functionality."""
    
    def test_get_available_features(self):
        """Test feature detection."""
        features = cqlite.get_available_features()
        
        assert isinstance(features, dict)
        assert "pandas" in features
        assert "numpy" in features
        assert "pyarrow" in features
        assert "async" in features
        assert "streaming" in features
        
        # async and streaming should always be True
        assert features["async"] is True
        assert features["streaming"] is True
    
    def test_check_dependencies(self, capsys):
        """Test dependency checking output."""
        cqlite.check_dependencies()
        
        captured = capsys.readouterr()
        assert "CQLite Feature Status" in captured.out
        assert "Pandas DataFrame support" in captured.out
        assert "Async query support" in captured.out


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])