#!/usr/bin/env python3
"""Generate 100 mutations for E2E testing of CQLite write support.

Creates a JSONL file with mutations for simple_table in test_basic keyspace.
Schema: id UUID PRIMARY KEY, name TEXT, age INT
"""
import json

# Base timestamp: 2024-01-01 00:00:00 UTC in microseconds
base_ts = 1704067200000000

output_file = "mutations.jsonl"

with open(output_file, "w") as f:
    for i in range(1, 101):
        # Create a deterministic UUID: 00000000-0000-0000-0000-00000000XXXX
        # where XXXX is the row number
        uuid_bytes = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, i // 256, i % 256]

        mutation = {
            "table": {
                "keyspace": "test_basic",
                "table": "simple_table"
            },
            "partition_key": {
                "columns": [
                    ["id", {"Uuid": uuid_bytes}]
                ]
            },
            "clustering_key": None,
            "operations": [
                {"Write": {"column": "name", "value": {"Text": f"User_{i}"}}},
                {"Write": {"column": "age", "value": {"Integer": 20 + (i % 50)}}}
            ],
            "timestamp_micros": base_ts + i * 1000,
            "ttl_seconds": None,
            "partition_tombstone": None,
            "range_tombstones": []
        }
        f.write(json.dumps(mutation) + "\n")

print(f"Generated 100 mutations to {output_file}")
