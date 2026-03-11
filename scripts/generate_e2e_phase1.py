#!/usr/bin/env python3
"""
Generate E2E Phase 1 test mutations for 9 tables covering all 24 primitive CQL types.

Creates 100 rows per table with deterministic values derived from row index.
Output: e2e_phase1/{table_name}.jsonl
"""

import json
import os
import struct
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


def make_uuid(index: int) -> List[int]:
    """Generate deterministic UUID v4 bytes from index."""
    # Format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx (version 4, variant 1)
    bytes_list = []
    for i in range(16):
        bytes_list.append((index + i * 17) % 256)
    # Set version to 4 (random UUID)
    bytes_list[6] = (bytes_list[6] & 0x0F) | 0x40  # Version 4
    # Set variant to 2 (RFC 4122)
    bytes_list[8] = (bytes_list[8] & 0x3F) | 0x80  # Variant bits 10
    return bytes_list


def make_timeuuid(index: int) -> List[int]:
    """Generate deterministic TIMEUUID (v1 UUID) bytes from index."""
    bytes_list = []
    for i in range(16):
        bytes_list.append((index + i * 23) % 256)
    # Set version to 1 (time-based UUID)
    bytes_list[6] = (bytes_list[6] & 0x0F) | 0x10  # Version 1
    # Set variant to 2 (RFC 4122)
    bytes_list[8] = (bytes_list[8] & 0x3F) | 0x80  # Variant bits 10
    return bytes_list


def make_ipv4(index: int) -> List[int]:
    """Generate IPv4 address bytes."""
    return [
        192,
        168,
        (index // 256) % 256,
        index % 256
    ]


def make_ipv6(index: int) -> List[int]:
    """Generate IPv6 address bytes."""
    return [
        0x20, 0x01, 0x0d, 0xb8,  # 2001:db8::
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        (index >> 24) & 0xFF,
        (index >> 16) & 0xFF,
        (index >> 8) & 0xFF,
        index & 0xFF
    ]


def make_decimal(value: float, scale: int) -> Dict[str, Any]:
    """
    Create Decimal value with proper big-endian 2's complement encoding.

    Example: 150.25 with scale=2 → unscaled=15025 → bytes=[0x3A, 0xB1]
    """
    unscaled = int(value * (10 ** scale))

    # Convert to big-endian 2's complement bytes
    if unscaled == 0:
        unscaled_bytes = [0]
    elif unscaled > 0:
        # Positive: convert to minimal big-endian bytes
        hex_str = hex(unscaled)[2:]  # Remove '0x'
        if len(hex_str) % 2:
            hex_str = '0' + hex_str
        unscaled_bytes = [int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2)]
        # Ensure positive (high bit clear) - add 0x00 if high bit set
        if unscaled_bytes[0] & 0x80:
            unscaled_bytes = [0] + unscaled_bytes
    else:
        # Negative: use 2's complement
        # Convert absolute value to bytes, then invert and add 1
        abs_val = abs(unscaled)
        hex_str = hex(abs_val)[2:]
        if len(hex_str) % 2:
            hex_str = '0' + hex_str
        pos_bytes = [int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2)]

        # Two's complement: invert bits and add 1
        inverted = [(~b) & 0xFF for b in pos_bytes]
        carry = 1
        for i in range(len(inverted) - 1, -1, -1):
            inverted[i] += carry
            if inverted[i] > 255:
                inverted[i] &= 0xFF
                carry = 1
            else:
                carry = 0
                break

        unscaled_bytes = inverted
        # Ensure negative (high bit set) - add 0xFF if high bit clear
        if not (unscaled_bytes[0] & 0x80):
            unscaled_bytes = [0xFF] + unscaled_bytes

    return {
        "scale": scale,
        "unscaled": unscaled_bytes
    }


def make_varint(value: int) -> List[int]:
    """Convert integer to big-endian 2's complement varint bytes."""
    if value == 0:
        return [0]
    elif value > 0:
        hex_str = hex(value)[2:]
        if len(hex_str) % 2:
            hex_str = '0' + hex_str
        bytes_list = [int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2)]
        # Ensure positive (high bit clear)
        if bytes_list[0] & 0x80:
            bytes_list = [0] + bytes_list
        return bytes_list
    else:
        # Negative: 2's complement
        abs_val = abs(value)
        hex_str = hex(abs_val)[2:]
        if len(hex_str) % 2:
            hex_str = '0' + hex_str
        pos_bytes = [int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2)]

        inverted = [(~b) & 0xFF for b in pos_bytes]
        carry = 1
        for i in range(len(inverted) - 1, -1, -1):
            inverted[i] += carry
            if inverted[i] > 255:
                inverted[i] &= 0xFF
                carry = 1
            else:
                carry = 0
                break

        # Ensure negative (high bit set)
        if not (inverted[0] & 0x80):
            inverted = [0xFF] + inverted
        return inverted


def make_duration(index: int) -> Dict[str, Any]:
    """Generate Duration value."""
    return {
        "months": (index % 12),
        "days": (index % 30),
        "nanos": (index * 1000000000) % (24 * 3600 * 1000000000)
    }


def make_blob(size: int, seed: int) -> List[int]:
    """Generate blob bytes of specified size."""
    return [(seed + i) % 256 for i in range(size)]


def make_mutation(
    keyspace: str,
    table: str,
    partition_key: List[tuple],
    clustering_key: Optional[List[tuple]],
    operations: List[Dict],
    timestamp_micros: int = 1704067200000000,
    ttl_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a mutation JSON object."""
    return {
        "table": {"keyspace": keyspace, "table": table},
        "partition_key": {"columns": partition_key},
        "clustering_key": {"columns": clustering_key} if clustering_key else None,
        "operations": operations,
        "timestamp_micros": timestamp_micros,
        "ttl_seconds": ttl_seconds,
        "partition_tombstone": None,
        "range_tombstones": []
    }


def generate_simple_table() -> List[Dict]:
    """test_basic.simple_table - UUID PK, no CK, 19 columns covering all primitive types."""
    mutations = []

    for i in range(100):
        partition_key = [["id", {"Uuid": make_uuid(i)}]]

        operations = [
            {"Write": {"column": "name", "value": {"Text": f"User_{i}"}}},
            {"Write": {"column": "age", "value": {"Integer": 20 + (i % 60)}}},
            {"Write": {"column": "salary", "value": {"BigInt": 50000 + i * 1000}}},
            {"Write": {"column": "height", "value": {"Float32": 150.0 + (i % 50)}}},
            {"Write": {"column": "weight", "value": {"Float": 50.0 + (i % 80)}}},
            {"Write": {"column": "active", "value": {"Boolean": i % 2 == 0}}},
            {"Write": {"column": "created", "value": {"Timestamp": 1704067200000 + i * 3600000}}},
            {"Write": {"column": "birth_date", "value": {"Date": -365 * 30 + i * 10}}},  # Days since epoch
            {"Write": {"column": "work_time", "value": {"Time": (8 * 3600 + i * 60) * 1000000000}}},  # Nanos since midnight
            {"Write": {"column": "description", "value": {"Blob": make_blob(32, i)}}},
            {"Write": {"column": "account_balance", "value": {"Decimal": make_decimal(1000.50 + i * 10.25, 2)}}},
            {"Write": {"column": "session_id", "value": {"Uuid": make_timeuuid(i)}}},
            {"Write": {"column": "ip_address", "value": {"Inet": make_ipv4(i) if i % 2 == 0 else make_ipv6(i)}}},
            {"Write": {"column": "small_number", "value": {"TinyInt": (i % 128) - 64}}},  # -64 to 63
            {"Write": {"column": "medium_number", "value": {"SmallInt": (i % 1000) - 500}}},
            {"Write": {"column": "duration_val", "value": {"Duration": make_duration(i)}}},
            {"Write": {"column": "varchar_field", "value": {"Text": f"varchar_{i}"}}},
            {"Write": {"column": "ascii_field", "value": {"Text": f"ascii_{i}"}}},
        ]

        mutations.append(make_mutation("test_basic", "simple_table", partition_key, None, operations))

    return mutations


def generate_composite_key_table() -> List[Dict]:
    """test_basic.composite_key_table - UUID PK, TIMESTAMP+TEXT CK."""
    mutations = []

    # 10 partitions × 10 rows each
    for partition_idx in range(10):
        partition_uuid = make_uuid(partition_idx)

        for row_idx in range(10):
            global_idx = partition_idx * 10 + row_idx

            partition_key = [["partition_key", {"Uuid": partition_uuid}]]
            clustering_key = [
                ["clustering_key1", {"Timestamp": 1704067200000 + global_idx * 3600000}],
                ["clustering_key2", {"Text": f"cluster_{global_idx}"}]
            ]

            operations = [
                {"Write": {"column": "data", "value": {"Text": f"data_{global_idx}"}}},
                {"Write": {"column": "value", "value": {"Integer": 1000 + global_idx}}},
            ]

            mutations.append(make_mutation("test_basic", "composite_key_table", partition_key, clustering_key, operations))

    return mutations


def generate_multi_partition_table() -> List[Dict]:
    """test_basic.multi_partition_table - (UUID+UUID) PK, TEXT+TIMEUUID CK."""
    mutations = []

    # 10 tenant/user combinations × 10 rows each
    for tenant_idx in range(5):
        for user_idx in range(2):
            partition_base = tenant_idx * 2 + user_idx

            for row_idx in range(10):
                global_idx = partition_base * 10 + row_idx

                partition_key = [
                    ["tenant_id", {"Uuid": make_uuid(tenant_idx)}],
                    ["user_id", {"Uuid": make_uuid(100 + user_idx)}]
                ]

                clustering_key = [
                    ["category", {"Text": f"cat_{global_idx % 5}"}],
                    ["item_id", {"Uuid": make_timeuuid(global_idx)}]
                ]

                operations = [
                    {"Write": {"column": "name", "value": {"Text": f"item_{global_idx}"}}},
                    {"Write": {"column": "value", "value": {"BigInt": 5000 + global_idx * 100}}},
                    {"Write": {"column": "metadata", "value": {"Text": f"meta_{global_idx}"}}},
                ]

                mutations.append(make_mutation("test_basic", "multi_partition_table", partition_key, clustering_key, operations))

    return mutations


def generate_static_columns_table() -> List[Dict]:
    """test_basic.static_columns_table - UUID PK, TIMESTAMP CK, static column."""
    mutations = []

    # 10 partitions × 10 rows each
    for partition_idx in range(10):
        partition_uuid = make_uuid(200 + partition_idx)

        for row_idx in range(10):
            global_idx = partition_idx * 10 + row_idx

            partition_key = [["partition_key", {"Uuid": partition_uuid}]]
            clustering_key = [
                ["clustering_key", {"Timestamp": 1704067200000 + global_idx * 3600000}]
            ]

            operations = [
                {"Write": {"column": "row_data", "value": {"Text": f"row_{global_idx}"}}},
                {"Write": {"column": "row_value", "value": {"Integer": 2000 + global_idx}}},
            ]

            # Static column: set once per partition (on first row)
            if row_idx == 0:
                operations.insert(0, {
                    "Write": {"column": "static_data", "value": {"Text": f"static_partition_{partition_idx}"}}
                })

            mutations.append(make_mutation("test_basic", "static_columns_table", partition_key, clustering_key, operations))

    return mutations


def generate_ttl_test_table() -> List[Dict]:
    """test_basic.ttl_test_table - UUID PK, TTL columns."""
    mutations = []

    for i in range(100):
        partition_key = [["id", {"Uuid": make_uuid(300 + i)}]]

        # Mix of normal writes and TTL writes
        if i % 3 == 0:
            # Use WriteWithTtl for some cells
            operations = [
                {"WriteWithTtl": {"column": "temporary_data", "value": {"Text": f"temp_{i}"}, "ttl_seconds": 86400}},
                {"WriteWithTtl": {"column": "expiring_value", "value": {"Integer": 3000 + i}, "ttl_seconds": 86400}},
                {"Write": {"column": "session_info", "value": {"Text": f"session_{i}"}}},
            ]
        else:
            operations = [
                {"Write": {"column": "temporary_data", "value": {"Text": f"temp_{i}"}}},
                {"Write": {"column": "expiring_value", "value": {"Integer": 3000 + i}}},
                {"Write": {"column": "session_info", "value": {"Text": f"session_{i}"}}},
            ]

        mutations.append(make_mutation("test_basic", "ttl_test_table", partition_key, None, operations))

    return mutations


def generate_sensor_data() -> List[Dict]:
    """test_timeseries.sensor_data - UUID PK, TIMESTAMP CK DESC."""
    mutations = []

    # 10 sensors × 10 readings each
    for sensor_idx in range(10):
        sensor_uuid = make_uuid(400 + sensor_idx)

        for reading_idx in range(10):
            global_idx = sensor_idx * 10 + reading_idx

            partition_key = [["sensor_id", {"Uuid": sensor_uuid}]]
            clustering_key = [
                ["timestamp", {"Timestamp": 1704067200000 + global_idx * 600000}]  # 10-min intervals
            ]

            operations = [
                {"Write": {"column": "temperature", "value": {"Float32": 20.0 + (global_idx % 30)}}},
                {"Write": {"column": "humidity", "value": {"Float32": 40.0 + (global_idx % 40)}}},
                {"Write": {"column": "pressure", "value": {"Float": 1013.25 + (global_idx % 20)}}},
                {"Write": {"column": "battery_level", "value": {"TinyInt": 100 - (global_idx % 100)}}},
                {"Write": {"column": "location", "value": {"Text": f"zone_{sensor_idx}"}}},
                {"Write": {"column": "status", "value": {"Text": "active" if global_idx % 5 != 0 else "idle"}}},
            ]

            mutations.append(make_mutation("test_timeseries", "sensor_data", partition_key, clustering_key, operations))

    return mutations


def generate_stock_prices() -> List[Dict]:
    """test_timeseries.stock_prices - (TEXT+DATE) PK, TIMESTAMP CK ASC."""
    mutations = []

    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

    # 5 symbols × 20 timestamps each
    for symbol_idx, symbol in enumerate(symbols):
        for day_idx in range(20):
            global_idx = symbol_idx * 20 + day_idx

            partition_key = [
                ["symbol", {"Text": symbol}],
                ["trading_day", {"Date": 19000 + day_idx}]  # Days since epoch
            ]

            clustering_key = [
                ["timestamp", {"Timestamp": 1704067200000 + global_idx * 3600000}]
            ]

            # Realistic stock prices with scale=2
            base_price = 150.0 + symbol_idx * 50.0
            operations = [
                {"Write": {"column": "open_price", "value": {"Decimal": make_decimal(base_price + day_idx * 0.50, 2)}}},
                {"Write": {"column": "high_price", "value": {"Decimal": make_decimal(base_price + day_idx * 0.50 + 2.00, 2)}}},
                {"Write": {"column": "low_price", "value": {"Decimal": make_decimal(base_price + day_idx * 0.50 - 1.50, 2)}}},
                {"Write": {"column": "close_price", "value": {"Decimal": make_decimal(base_price + day_idx * 0.50 + 0.25, 2)}}},
                {"Write": {"column": "volume", "value": {"BigInt": 1000000 + global_idx * 10000}}},
                {"Write": {"column": "adjusted_close", "value": {"Decimal": make_decimal(base_price + day_idx * 0.50 + 0.15, 2)}}},
            ]

            mutations.append(make_mutation("test_timeseries", "stock_prices", partition_key, clustering_key, operations))

    return mutations


def generate_wide_partition_table() -> List[Dict]:
    """test_wide_rows.wide_partition_table - UUID PK, 5-column CK."""
    mutations = []

    # 5 partitions × 20 rows each
    for partition_idx in range(5):
        partition_uuid = make_uuid(500 + partition_idx)

        for row_idx in range(20):
            global_idx = partition_idx * 20 + row_idx

            partition_key = [["partition_key", {"Uuid": partition_uuid}]]
            clustering_key = [
                ["clustering_col1", {"Timestamp": 1704067200000 + global_idx * 3600000}],
                ["clustering_col2", {"Text": f"ck2_{global_idx}"}],
                ["clustering_col3", {"Integer": 5000 + global_idx}],
                ["clustering_col4", {"Uuid": make_uuid(600 + global_idx)}],
                ["clustering_col5", {"Date": 19000 + global_idx}]
            ]

            operations = [
                {"Write": {"column": "data_column", "value": {"Text": f"data_{global_idx}"}}},
                {"Write": {"column": "value_column", "value": {"BigInt": 10000 + global_idx * 100}}},
                {"Write": {"column": "blob_column", "value": {"Blob": make_blob(64, global_idx)}}},
                {"Write": {"column": "json_column", "value": {"Text": f'{{\"id\":{global_idx},\"active\":true}}'}}},
            ]

            mutations.append(make_mutation("test_wide_rows", "wide_partition_table", partition_key, clustering_key, operations))

    return mutations


def generate_large_blob_table() -> List[Dict]:
    """test_wide_rows.large_blob_table - UUID PK, INT CK ASC, large blobs."""
    mutations = []

    # 10 files × 10 chunks each
    for file_idx in range(10):
        file_uuid = make_uuid(700 + file_idx)
        total_chunks = 10

        for chunk_idx in range(total_chunks):
            global_idx = file_idx * 10 + chunk_idx

            partition_key = [["file_id", {"Uuid": file_uuid}]]
            clustering_key = [
                ["chunk_id", {"Integer": chunk_idx}]
            ]

            # 1KB+ blobs
            chunk_data = make_blob(1024 + chunk_idx * 128, global_idx)

            operations = [
                {"Write": {"column": "file_name", "value": {"Text": f"file_{file_idx}.dat"}}},
                {"Write": {"column": "mime_type", "value": {"Text": "application/octet-stream"}}},
                {"Write": {"column": "chunk_data", "value": {"Blob": chunk_data}}},
                {"Write": {"column": "chunk_size", "value": {"Integer": len(chunk_data)}}},
                {"Write": {"column": "total_chunks", "value": {"Integer": total_chunks}}},
                {"Write": {"column": "checksum", "value": {"Text": f"sha256_{global_idx}"}}},
            ]

            mutations.append(make_mutation("test_wide_rows", "large_blob_table", partition_key, clustering_key, operations))

    return mutations


def main():
    """Generate all mutation files."""
    output_dir = Path("e2e_phase1")
    output_dir.mkdir(exist_ok=True)

    tables = [
        ("simple_table", generate_simple_table),
        ("composite_key_table", generate_composite_key_table),
        ("multi_partition_table", generate_multi_partition_table),
        ("static_columns_table", generate_static_columns_table),
        ("ttl_test_table", generate_ttl_test_table),
        ("sensor_data", generate_sensor_data),
        ("stock_prices", generate_stock_prices),
        ("wide_partition_table", generate_wide_partition_table),
        ("large_blob_table", generate_large_blob_table),
    ]

    total_mutations = 0

    for table_name, generator_func in tables:
        mutations = generator_func()
        output_file = output_dir / f"{table_name}.jsonl"

        with output_file.open("w") as f:
            for mutation in mutations:
                f.write(json.dumps(mutation) + "\n")

        print(f"Generated {len(mutations):3d} mutations for {table_name:30s} -> {output_file}")
        total_mutations += len(mutations)

    print(f"\nTotal: {total_mutations} mutations across {len(tables)} tables")
    print(f"Output directory: {output_dir.absolute()}")


if __name__ == "__main__":
    main()
