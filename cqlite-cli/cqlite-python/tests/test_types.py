"""
Tests for CQL type system and conversion functionality.
"""

import pytest
import uuid
import datetime
import decimal
import ipaddress
from typing import List, Dict, Set, Tuple

from cqlite.types import (
    CQLType, 
    PYTHON_TYPE_MAPPING,
    convert_cql_value,
    infer_python_type,
    create_type_hint_string,
    get_cql_type_info,
    CQLValue,
    CQLiteRow,
    CQLiteSchema,
    CQLTypeInfo
)


class TestCQLTypeEnum:
    """Test CQLType enumeration."""
    
    def test_cql_type_values(self):
        """Test that all expected CQL types are defined."""
        expected_types = [
            "text", "varchar", "ascii", "int", "smallint", "tinyint", 
            "bigint", "varint", "float", "double", "decimal", "boolean",
            "blob", "uuid", "timeuuid", "timestamp", "date", "time",
            "duration", "inet", "list", "set", "map", "tuple", "udt", "counter"
        ]
        
        for type_name in expected_types:
            assert hasattr(CQLType, type_name.upper())
            assert CQLType[type_name.upper()].value == type_name


class TestPythonTypeMapping:
    """Test CQL to Python type mapping."""
    
    def test_basic_type_mappings(self):
        """Test basic type mappings."""
        mappings = [
            (CQLType.TEXT, str),
            (CQLType.INT, int),
            (CQLType.BIGINT, int),
            (CQLType.FLOAT, float),
            (CQLType.DOUBLE, float),
            (CQLType.BOOLEAN, bool),
            (CQLType.BLOB, bytes),
            (CQLType.UUID, uuid.UUID),
            (CQLType.TIMESTAMP, datetime.datetime),
            (CQLType.DATE, datetime.date),
            (CQLType.TIME, datetime.time),
            (CQLType.DECIMAL, decimal.Decimal),
        ]
        
        for cql_type, python_type in mappings:
            assert PYTHON_TYPE_MAPPING[cql_type] == python_type
    
    def test_collection_type_mappings(self):
        """Test collection type mappings."""
        assert PYTHON_TYPE_MAPPING[CQLType.LIST] == list
        assert PYTHON_TYPE_MAPPING[CQLType.SET] == set
        assert PYTHON_TYPE_MAPPING[CQLType.MAP] == dict
        assert PYTHON_TYPE_MAPPING[CQLType.TUPLE] == tuple


class TestTypeInference:
    """Test type inference functionality."""
    
    def test_infer_simple_types(self):
        """Test inferring simple Python types from CQL type strings."""
        test_cases = [
            ("text", str),
            ("varchar", str),
            ("int", int),
            ("bigint", int),
            ("float", float),
            ("double", float),
            ("boolean", bool),
            ("blob", bytes),
            ("uuid", uuid.UUID),
            ("timestamp", datetime.datetime),
            ("date", datetime.date),
            ("time", datetime.time),
            ("decimal", decimal.Decimal),
        ]
        
        for cql_type_str, expected_type in test_cases:
            result = infer_python_type(cql_type_str)
            assert result == expected_type
    
    def test_infer_collection_types(self):
        """Test inferring collection types."""
        # Collection types should return the base collection type
        assert infer_python_type("list<int>") == list
        assert infer_python_type("set<text>") == set
        assert infer_python_type("map<text,int>") == dict
        assert infer_python_type("tuple<int,text>") == tuple
    
    def test_infer_unknown_type(self):
        """Test handling of unknown types."""
        # Unknown types should default to str
        assert infer_python_type("unknown_type") == str
        assert infer_python_type("custom_udt") == str


class TestValueConversion:
    """Test CQL value conversion to Python values."""
    
    def test_text_conversion(self):
        """Test text type conversion."""
        assert convert_cql_value("hello", "text") == "hello"
        assert convert_cql_value("world", "varchar") == "world"
        assert convert_cql_value("ascii", "ascii") == "ascii"
        assert convert_cql_value(123, "text") == "123"
    
    def test_numeric_conversion(self):
        """Test numeric type conversion."""
        # Integer types
        assert convert_cql_value("123", "int") == 123
        assert convert_cql_value("456", "smallint") == 456
        assert convert_cql_value("789", "tinyint") == 789
        assert convert_cql_value("9876543210", "bigint") == 9876543210
        assert convert_cql_value("99999999999999999999", "varint") == 99999999999999999999
        
        # Float types
        assert convert_cql_value("3.14", "float") == 3.14
        assert convert_cql_value("2.718", "double") == 2.718
        
        # Already numeric values
        assert convert_cql_value(42, "int") == 42
        assert convert_cql_value(3.14159, "double") == 3.14159
    
    def test_boolean_conversion(self):
        """Test boolean conversion."""
        # String values
        assert convert_cql_value("true", "boolean") is True
        assert convert_cql_value("false", "boolean") is False
        assert convert_cql_value("TRUE", "boolean") is True
        assert convert_cql_value("FALSE", "boolean") is False
        assert convert_cql_value("1", "boolean") is True
        assert convert_cql_value("0", "boolean") is False
        assert convert_cql_value("yes", "boolean") is True
        assert convert_cql_value("no", "boolean") is False
        
        # Boolean values
        assert convert_cql_value(True, "boolean") is True
        assert convert_cql_value(False, "boolean") is False
        
        # Numeric values  
        assert convert_cql_value(1, "boolean") is True
        assert convert_cql_value(0, "boolean") is False
    
    def test_uuid_conversion(self):
        """Test UUID conversion."""
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        result = convert_cql_value(uuid_str, "uuid")
        
        assert isinstance(result, uuid.UUID)
        assert str(result) == uuid_str
        
        # Test with existing UUID object
        uuid_obj = uuid.UUID(uuid_str)
        result = convert_cql_value(uuid_obj, "uuid")
        assert result == uuid_obj
    
    def test_timestamp_conversion(self):
        """Test timestamp conversion."""
        # ISO format string
        iso_str = "2023-12-25T10:30:00"
        result = convert_cql_value(iso_str, "timestamp")
        assert isinstance(result, datetime.datetime)
        
        # Timestamp with timezone
        iso_tz = "2023-12-25T10:30:00+00:00"
        result = convert_cql_value(iso_tz, "timestamp")
        assert isinstance(result, datetime.datetime)
        
        # Unix timestamp
        timestamp = 1703509800  # 2023-12-25 10:30:00 UTC
        result = convert_cql_value(timestamp, "timestamp")
        assert isinstance(result, datetime.datetime)
        
        # Float timestamp
        timestamp_float = 1703509800.123
        result = convert_cql_value(timestamp_float, "timestamp")
        assert isinstance(result, datetime.datetime)
    
    def test_date_conversion(self):
        """Test date conversion."""
        date_str = "2023-12-25"
        result = convert_cql_value(date_str, "date")
        
        assert isinstance(result, datetime.date)
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
    
    def test_time_conversion(self):
        """Test time conversion."""
        time_str = "10:30:45"
        result = convert_cql_value(time_str, "time")
        
        assert isinstance(result, datetime.time)
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 45
    
    def test_decimal_conversion(self):
        """Test decimal conversion."""
        decimal_str = "123.456789"
        result = convert_cql_value(decimal_str, "decimal")
        
        assert isinstance(result, decimal.Decimal)
        assert str(result) == "123.456789"
    
    def test_inet_conversion(self):
        """Test IP address conversion."""
        # IPv4
        ipv4_str = "192.168.1.1"
        result = convert_cql_value(ipv4_str, "inet")
        assert isinstance(result, ipaddress.IPv4Address)
        assert str(result) == ipv4_str
        
        # IPv6
        ipv6_str = "2001:db8::1"
        result = convert_cql_value(ipv6_str, "inet")
        assert isinstance(result, ipaddress.IPv6Address)
        assert str(result) == ipv6_str
    
    def test_blob_conversion(self):
        """Test binary data conversion."""
        # Hex string
        hex_str = "deadbeef"
        result = convert_cql_value(hex_str, "blob")
        assert isinstance(result, bytes)
        assert result == bytes.fromhex(hex_str)
        
        # Bytes object
        bytes_obj = b"hello world"
        result = convert_cql_value(bytes_obj, "blob")
        assert result == bytes_obj
    
    def test_list_conversion(self):
        """Test list conversion."""
        # Simple list
        list_data = [1, 2, 3]
        result = convert_cql_value(list_data, "list<int>")
        assert isinstance(result, list)
        assert result == [1, 2, 3]
        assert all(isinstance(x, int) for x in result)
        
        # Mixed type list
        mixed_list = ["1", "2", "3"]
        result = convert_cql_value(mixed_list, "list<int>")
        assert result == [1, 2, 3]
        
        # Empty list
        result = convert_cql_value([], "list<text>")
        assert result == []
        
        # None value
        result = convert_cql_value(None, "list<text>")
        assert result == []
    
    def test_set_conversion(self):
        """Test set conversion."""
        # List with duplicates
        list_data = [1, 2, 3, 2, 1]
        result = convert_cql_value(list_data, "set<int>")
        assert isinstance(result, set)
        assert result == {1, 2, 3}
        
        # Set input
        set_data = {1, 2, 3}
        result = convert_cql_value(set_data, "set<int>")
        assert result == {1, 2, 3}
        
        # Empty set
        result = convert_cql_value([], "set<text>")
        assert result == set()
    
    def test_map_conversion(self):
        """Test map/dict conversion."""
        # String keys and values
        map_data = {"key1": "value1", "key2": "value2"}
        result = convert_cql_value(map_data, "map<text,text>")
        assert isinstance(result, dict)
        assert result == map_data
        
        # Mixed types
        mixed_map = {"1": "100", "2": "200"}
        result = convert_cql_value(mixed_map, "map<int,int>")
        expected = {1: 100, 2: 200}
        assert result == expected
        
        # Empty map
        result = convert_cql_value({}, "map<text,int>")
        assert result == {}
    
    def test_tuple_conversion(self):
        """Test tuple conversion."""
        # Simple tuple
        tuple_data = [1, "hello", 3.14]
        result = convert_cql_value(tuple_data, "tuple<int,text,double>")
        assert isinstance(result, tuple)
        assert result == (1, "hello", 3.14)
        
        # Tuple input
        tuple_input = (1, "hello")
        result = convert_cql_value(tuple_input, "tuple<int,text>")
        assert result == (1, "hello")
        
        # Empty tuple
        result = convert_cql_value([], "tuple<>")
        assert result == tuple()
    
    def test_udt_conversion(self):
        """Test user-defined type conversion."""
        udt_data = {"field1": "value1", "field2": 42}
        result = convert_cql_value(udt_data, "udt")
        assert isinstance(result, dict)
        assert result == udt_data
    
    def test_null_value_conversion(self):
        """Test null value handling."""
        for cql_type in ["text", "int", "boolean", "list<int>", "map<text,int>"]:
            result = convert_cql_value(None, cql_type)
            assert result is None


class TestTypeHints:
    """Test type hint generation."""
    
    def test_simple_type_hints(self):
        """Test type hints for simple types."""
        test_cases = [
            ("text", "str"),
            ("int", "int"),
            ("boolean", "bool"),
            ("uuid", "uuid.UUID"),
            ("timestamp", "datetime.datetime"),
            ("decimal", "decimal.Decimal"),
            ("inet", "Union[ipaddress.IPv4Address, ipaddress.IPv6Address]"),
        ]
        
        for cql_type, expected_hint in test_cases:
            result = create_type_hint_string(cql_type)
            assert result == expected_hint
    
    def test_collection_type_hints(self):
        """Test type hints for collection types."""
        test_cases = [
            ("list<int>", "List[int]"),
            ("set<text>", "Set[str]"),
            ("map<text,int>", "Dict[str, int]"),
            ("tuple<int,text>", "Tuple[int, str]"),
            ("list<list<int>>", "List[List[int]]"),  # Nested collections
        ]
        
        for cql_type, expected_hint in test_cases:
            result = create_type_hint_string(cql_type)
            assert result == expected_hint


class TestCQLTypeInfo:
    """Test CQL type information functionality."""
    
    def test_get_type_info(self):
        """Test getting comprehensive type information."""
        info = get_cql_type_info("text")
        
        assert info["cql_type"] == "text"
        assert info["base_type"] == "text"
        assert info["python_type"] == "str"
        assert info["type_hint"] == "str"
        assert info["is_collection"] is False
        assert info["is_numeric"] is False
        assert info["is_temporal"] is False
        assert info["nullable"] is True
    
    def test_numeric_type_info(self):
        """Test type info for numeric types."""
        for cql_type in ["int", "bigint", "float", "double", "decimal"]:
            info = get_cql_type_info(cql_type)
            assert info["is_numeric"] is True
            assert info["is_collection"] is False
            assert info["is_temporal"] is False
    
    def test_temporal_type_info(self):
        """Test type info for temporal types."""
        for cql_type in ["timestamp", "date", "time"]:
            info = get_cql_type_info(cql_type)
            assert info["is_temporal"] is True
            assert info["is_numeric"] is False
            assert info["is_collection"] is False
    
    def test_collection_type_info(self):
        """Test type info for collection types."""
        for cql_type in ["list<int>", "set<text>", "map<text,int>", "tuple<int,text>"]:
            info = get_cql_type_info(cql_type)
            assert info["is_collection"] is True
            assert info["is_numeric"] is False
            assert info["is_temporal"] is False


class TestCQLValue:
    """Test CQLValue enumeration and functionality."""
    
    def test_cql_value_creation(self):
        """Test creating CQLValue instances."""
        # Text value
        text_val = CQLValue.Text("hello")
        assert isinstance(text_val, CQLValue)
        
        # Integer value
        int_val = CQLValue.Int(42)
        assert isinstance(int_val, CQLValue)
        
        # Boolean value
        bool_val = CQLValue.Boolean(True)
        assert isinstance(bool_val, CQLValue)
    
    def test_cql_value_equality(self):
        """Test CQLValue equality comparison."""
        # Same values should be equal
        assert CQLValue.Text("hello") == CQLValue.Text("hello")
        assert CQLValue.Int(42) == CQLValue.Int(42)
        assert CQLValue.Boolean(True) == CQLValue.Boolean(True)
        
        # Different values should not be equal
        assert CQLValue.Text("hello") != CQLValue.Text("world")
        assert CQLValue.Int(42) != CQLValue.Int(43)
        assert CQLValue.Boolean(True) != CQLValue.Boolean(False)
        
        # Different types should not be equal
        assert CQLValue.Text("42") != CQLValue.Int(42)
    
    def test_cql_value_hashing(self):
        """Test CQLValue hashing for use in sets/dicts."""
        values = [
            CQLValue.Text("hello"),
            CQLValue.Int(42),
            CQLValue.Boolean(True),
            CQLValue.Text("hello"),  # Duplicate
        ]
        
        # Should be able to create set (requires hashing)
        value_set = set(values)
        assert len(value_set) == 3  # Duplicates removed


class TestCQLiteRow:
    """Test CQLiteRow functionality."""
    
    def test_row_creation_and_manipulation(self):
        """Test creating and manipulating rows."""
        row = CQLiteRow()
        
        # Add columns
        row.add_column("id", CQLValue.Int(1))
        row.add_column("name", CQLValue.Text("Alice"))
        row.add_column("active", CQLValue.Boolean(True))
        
        assert row.column_count() == 3
        
        # Get columns
        id_val = row.get_column("id")
        assert id_val == CQLValue.Int(1)
        
        name_val = row.get_column("name")
        assert name_val == CQLValue.Text("Alice")
        
        # Non-existent column
        assert row.get_column("nonexistent") is None
        
        # Column names
        names = row.column_names()
        assert "id" in names
        assert "name" in names
        assert "active" in names
    
    def test_row_default_creation(self):
        """Test default row creation."""
        row = CQLiteRow.default()
        assert row.column_count() == 0
        assert len(row.column_names()) == 0


class TestCQLiteSchema:
    """Test schema representation."""
    
    def test_schema_creation(self):
        """Test creating schema objects."""
        columns = [
            CQLTypeInfo(
                name="id",
                cql_type="int", 
                python_type="int",
                nullable=False,
                is_partition_key=True,
                is_clustering_key=False,
                clustering_order=None
            ),
            CQLTypeInfo(
                name="name",
                cql_type="text",
                python_type="str", 
                nullable=True,
                is_partition_key=False,
                is_clustering_key=False,
                clustering_order=None
            )
        ]
        
        schema = CQLiteSchema(
            keyspace="test_ks",
            table="users",
            columns=columns
        )
        
        assert schema.keyspace == "test_ks"
        assert schema.table == "users"
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "id"
        assert schema.columns[1].name == "name"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])