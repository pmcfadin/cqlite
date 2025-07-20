"""
CQL type system and Python type conversion utilities.

This module provides comprehensive mapping between Cassandra CQL types and Python types,
enabling seamless data conversion when querying SSTable files.
"""

import uuid
import datetime
import decimal
import ipaddress
from typing import Any, Dict, List, Set, Tuple, Union, Optional, Type
from enum import Enum


class CQLType(Enum):
    """Enumeration of supported CQL data types."""
    
    # Simple types
    TEXT = "text"
    VARCHAR = "varchar" 
    ASCII = "ascii"
    INT = "int"
    SMALLINT = "smallint"
    TINYINT = "tinyint"
    BIGINT = "bigint"
    VARINT = "varint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    BLOB = "blob"
    
    # UUID types
    UUID = "uuid"
    TIMEUUID = "timeuuid"
    
    # Date/time types
    TIMESTAMP = "timestamp"
    DATE = "date"
    TIME = "time"
    DURATION = "duration"
    
    # Network types
    INET = "inet"
    
    # Collection types
    LIST = "list"
    SET = "set"
    MAP = "map"
    TUPLE = "tuple"
    
    # User-defined types
    UDT = "udt"
    
    # Counter type
    COUNTER = "counter"


# Mapping from CQL types to Python types
PYTHON_TYPE_MAPPING: Dict[CQLType, Type] = {
    # Text types -> str
    CQLType.TEXT: str,
    CQLType.VARCHAR: str,
    CQLType.ASCII: str,
    
    # Integer types -> int
    CQLType.INT: int,
    CQLType.SMALLINT: int,
    CQLType.TINYINT: int,
    CQLType.BIGINT: int,
    CQLType.VARINT: int,
    CQLType.COUNTER: int,
    
    # Float types -> float
    CQLType.FLOAT: float,
    CQLType.DOUBLE: float,
    
    # Decimal -> decimal.Decimal
    CQLType.DECIMAL: decimal.Decimal,
    
    # Boolean -> bool
    CQLType.BOOLEAN: bool,
    
    # Binary data -> bytes
    CQLType.BLOB: bytes,
    
    # UUID types -> uuid.UUID
    CQLType.UUID: uuid.UUID,
    CQLType.TIMEUUID: uuid.UUID,
    
    # Date/time types
    CQLType.TIMESTAMP: datetime.datetime,
    CQLType.DATE: datetime.date,
    CQLType.TIME: datetime.time,
    
    # Network types
    CQLType.INET: Union[ipaddress.IPv4Address, ipaddress.IPv6Address],
    
    # Collection types (generic, actual types are parameterized)
    CQLType.LIST: list,
    CQLType.SET: set,
    CQLType.MAP: dict,
    CQLType.TUPLE: tuple,
    
    # UDT -> dict (or custom dataclass)
    CQLType.UDT: dict,
}

# Reverse mapping for type detection
PYTHON_TO_CQL_MAPPING: Dict[Type, CQLType] = {
    str: CQLType.TEXT,
    int: CQLType.BIGINT,  # Use bigint as default for int
    float: CQLType.DOUBLE,  # Use double as default for float
    bool: CQLType.BOOLEAN,
    bytes: CQLType.BLOB,
    uuid.UUID: CQLType.UUID,
    datetime.datetime: CQLType.TIMESTAMP,
    datetime.date: CQLType.DATE,
    datetime.time: CQLType.TIME,
    decimal.Decimal: CQLType.DECIMAL,
    list: CQLType.LIST,
    set: CQLType.SET,
    dict: CQLType.MAP,
    tuple: CQLType.TUPLE,
}


def infer_python_type(cql_type_string: str) -> Type:
    """
    Infer Python type from CQL type string.
    
    Args:
        cql_type_string: CQL type string (e.g., "text", "list<int>", "map<text,int>")
        
    Returns:
        Corresponding Python type
        
    Examples:
        >>> infer_python_type("text")
        <class 'str'>
        >>> infer_python_type("list<int>")
        <class 'list'>
        >>> infer_python_type("map<text,int>")
        <class 'dict'>
    """
    # Parse base type (handle parameterized types)
    base_type = cql_type_string.split('<')[0].lower().strip()
    
    try:
        cql_type = CQLType(base_type)
        return PYTHON_TYPE_MAPPING.get(cql_type, str)  # Default to str
    except ValueError:
        # Unknown type, default to str
        return str


def convert_cql_value(value: Any, cql_type_string: str) -> Any:
    """
    Convert a raw value to appropriate Python type based on CQL type.
    
    Args:
        value: Raw value from SSTable
        cql_type_string: CQL type string
        
    Returns:
        Converted Python value
        
    Examples:
        >>> convert_cql_value("123", "int")
        123
        >>> convert_cql_value("2023-12-25", "date")
        datetime.date(2023, 12, 25)
    """
    if value is None:
        return None
    
    base_type = cql_type_string.split('<')[0].lower().strip()
    
    try:
        cql_type = CQLType(base_type)
    except ValueError:
        # Unknown type, return as-is
        return value
    
    # Convert based on CQL type
    if cql_type in (CQLType.TEXT, CQLType.VARCHAR, CQLType.ASCII):
        return str(value)
    
    elif cql_type in (CQLType.INT, CQLType.SMALLINT, CQLType.TINYINT, CQLType.BIGINT, CQLType.COUNTER):
        return int(value)
    
    elif cql_type == CQLType.VARINT:
        # Handle very large integers
        return int(value)
    
    elif cql_type == CQLType.FLOAT:
        return float(value)
    
    elif cql_type == CQLType.DOUBLE:
        return float(value)
    
    elif cql_type == CQLType.DECIMAL:
        if isinstance(value, str):
            return decimal.Decimal(value)
        return decimal.Decimal(str(value))
    
    elif cql_type == CQLType.BOOLEAN:
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)
    
    elif cql_type == CQLType.BLOB:
        if isinstance(value, str):
            # Assume hex string
            return bytes.fromhex(value)
        return bytes(value)
    
    elif cql_type in (CQLType.UUID, CQLType.TIMEUUID):
        if isinstance(value, str):
            return uuid.UUID(value)
        return value
    
    elif cql_type == CQLType.TIMESTAMP:
        if isinstance(value, str):
            # Try to parse ISO format
            try:
                return datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                # Try parsing as timestamp
                return datetime.datetime.fromtimestamp(float(value))
        elif isinstance(value, (int, float)):
            return datetime.datetime.fromtimestamp(value)
        return value
    
    elif cql_type == CQLType.DATE:
        if isinstance(value, str):
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        return value
    
    elif cql_type == CQLType.TIME:
        if isinstance(value, str):
            return datetime.datetime.strptime(value, '%H:%M:%S').time()
        return value
    
    elif cql_type == CQLType.INET:
        if isinstance(value, str):
            return ipaddress.ip_address(value)
        return value
    
    elif cql_type == CQLType.LIST:
        if isinstance(value, (list, tuple)):
            # Parse element type from cql_type_string
            element_type = _extract_element_type(cql_type_string)
            return [convert_cql_value(item, element_type) for item in value]
        return list(value) if value is not None else []
    
    elif cql_type == CQLType.SET:
        if isinstance(value, (list, tuple, set)):
            element_type = _extract_element_type(cql_type_string)
            return {convert_cql_value(item, element_type) for item in value}
        return set(value) if value is not None else set()
    
    elif cql_type == CQLType.MAP:
        if isinstance(value, dict):
            key_type, value_type = _extract_map_types(cql_type_string)
            return {
                convert_cql_value(k, key_type): convert_cql_value(v, value_type)
                for k, v in value.items()
            }
        return dict(value) if value is not None else {}
    
    elif cql_type == CQLType.TUPLE:
        if isinstance(value, (list, tuple)):
            element_types = _extract_tuple_types(cql_type_string)
            converted = []
            for i, item in enumerate(value):
                elem_type = element_types[i] if i < len(element_types) else "text"
                converted.append(convert_cql_value(item, elem_type))
            return tuple(converted)
        return tuple(value) if value is not None else tuple()
    
    elif cql_type == CQLType.UDT:
        # UDT should be a dict-like structure
        return dict(value) if value is not None else {}
    
    # Default: return as-is
    return value


def _extract_element_type(cql_type_string: str) -> str:
    """Extract element type from list<type> or set<type>."""
    if '<' in cql_type_string and '>' in cql_type_string:
        start = cql_type_string.find('<') + 1
        end = cql_type_string.rfind('>')
        return cql_type_string[start:end].strip()
    return "text"  # Default


def _extract_map_types(cql_type_string: str) -> Tuple[str, str]:
    """Extract key and value types from map<key_type,value_type>."""
    if '<' in cql_type_string and '>' in cql_type_string:
        start = cql_type_string.find('<') + 1
        end = cql_type_string.rfind('>')
        inner = cql_type_string[start:end].strip()
        
        # Split on comma, but be careful of nested types
        parts = _split_type_params(inner)
        if len(parts) >= 2:
            return parts[0].strip(), parts[1].strip()
    
    return "text", "text"  # Default


def _extract_tuple_types(cql_type_string: str) -> List[str]:
    """Extract element types from tuple<type1,type2,...>."""
    if '<' in cql_type_string and '>' in cql_type_string:
        start = cql_type_string.find('<') + 1
        end = cql_type_string.rfind('>')
        inner = cql_type_string[start:end].strip()
        
        return [t.strip() for t in _split_type_params(inner)]
    
    return ["text"]  # Default


def _split_type_params(params_string: str) -> List[str]:
    """Split type parameters, handling nested angle brackets."""
    parts = []
    current = ""
    depth = 0
    
    for char in params_string:
        if char == '<':
            depth += 1
            current += char
        elif char == '>':
            depth -= 1
            current += char
        elif char == ',' and depth == 0:
            parts.append(current.strip())
            current = ""
        else:
            current += char
    
    if current.strip():
        parts.append(current.strip())
    
    return parts


def create_type_hint_string(cql_type_string: str) -> str:
    """
    Create a Python type hint string from CQL type.
    
    Args:
        cql_type_string: CQL type string
        
    Returns:
        Python type hint string
        
    Examples:
        >>> create_type_hint_string("text")
        "str"
        >>> create_type_hint_string("list<int>")
        "List[int]"
        >>> create_type_hint_string("map<text,int>")
        "Dict[str, int]"
    """
    base_type = cql_type_string.split('<')[0].lower().strip()
    
    # Handle simple types
    simple_mappings = {
        "text": "str",
        "varchar": "str", 
        "ascii": "str",
        "int": "int",
        "smallint": "int",
        "tinyint": "int",
        "bigint": "int",
        "varint": "int",
        "counter": "int",
        "float": "float",
        "double": "float",
        "decimal": "decimal.Decimal",
        "boolean": "bool",
        "blob": "bytes",
        "uuid": "uuid.UUID",
        "timeuuid": "uuid.UUID",
        "timestamp": "datetime.datetime",
        "date": "datetime.date",
        "time": "datetime.time",
        "inet": "Union[ipaddress.IPv4Address, ipaddress.IPv6Address]",
    }
    
    if base_type in simple_mappings:
        return simple_mappings[base_type]
    
    # Handle collection types
    if base_type == "list":
        element_type = _extract_element_type(cql_type_string)
        element_hint = create_type_hint_string(element_type)
        return f"List[{element_hint}]"
    
    elif base_type == "set":
        element_type = _extract_element_type(cql_type_string)
        element_hint = create_type_hint_string(element_type)
        return f"Set[{element_hint}]"
    
    elif base_type == "map":
        key_type, value_type = _extract_map_types(cql_type_string)
        key_hint = create_type_hint_string(key_type)
        value_hint = create_type_hint_string(value_type)
        return f"Dict[{key_hint}, {value_hint}]"
    
    elif base_type == "tuple":
        element_types = _extract_tuple_types(cql_type_string)
        element_hints = [create_type_hint_string(t) for t in element_types]
        return f"Tuple[{', '.join(element_hints)}]"
    
    elif base_type == "udt":
        return "Dict[str, Any]"  # UDTs are represented as dicts
    
    # Unknown type
    return "Any"


def get_cql_type_info(cql_type_string: str) -> Dict[str, Any]:
    """
    Get comprehensive information about a CQL type.
    
    Args:
        cql_type_string: CQL type string
        
    Returns:
        Dictionary with type information
    """
    python_type = infer_python_type(cql_type_string)
    type_hint = create_type_hint_string(cql_type_string)
    
    base_type = cql_type_string.split('<')[0].lower().strip()
    is_collection = base_type in ("list", "set", "map", "tuple")
    is_numeric = base_type in ("int", "smallint", "tinyint", "bigint", "varint", 
                              "float", "double", "decimal", "counter")
    is_temporal = base_type in ("timestamp", "date", "time")
    
    return {
        "cql_type": cql_type_string,
        "base_type": base_type,
        "python_type": python_type.__name__,
        "type_hint": type_hint,
        "is_collection": is_collection,
        "is_numeric": is_numeric,
        "is_temporal": is_temporal,
        "nullable": True,  # Most CQL types are nullable
    }


# Example usage and testing
if __name__ == "__main__":
    # Test type conversion
    test_cases = [
        ("text", "hello", str),
        ("int", "123", int),
        ("boolean", "true", bool),
        ("list<int>", [1, 2, 3], list),
        ("map<text,int>", {"a": 1, "b": 2}, dict),
        ("uuid", "550e8400-e29b-41d4-a716-446655440000", uuid.UUID),
    ]
    
    for cql_type, value, expected_type in test_cases:
        converted = convert_cql_value(value, cql_type)
        print(f"{cql_type}: {value} -> {converted} ({type(converted).__name__})")
        
        type_info = get_cql_type_info(cql_type)
        print(f"  Type info: {type_info}")
        print()