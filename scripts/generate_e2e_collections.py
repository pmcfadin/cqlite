#!/usr/bin/env python3
"""
Generate E2E Phase 2 (Collections) test mutations for 16 tables covering all collection types.

Tables covered:
  test_collections (8): collection_table, nested_collections_table, large_collections_table,
    collections_with_udts, frozen_collections_table, typed_collections_table,
    empty_collections_table, collection_clustering_table
  test_timeseries (4): app_metrics, user_activity, event_store, user_sessions
  test_wide_rows (4): chat_messages, document_versions, product_catalog, sparse_data_table

Output: e2e_collections/{table_name}.jsonl (16 files, 10 rows each)
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---- Helper functions (shared with Phase 1) ----

def make_uuid(index: int) -> List[int]:
    """Generate deterministic UUID v4 bytes from index."""
    bytes_list = [(index + i * 17) % 256 for i in range(16)]
    bytes_list[6] = (bytes_list[6] & 0x0F) | 0x40  # Version 4
    bytes_list[8] = (bytes_list[8] & 0x3F) | 0x80  # Variant 1
    return bytes_list


def make_timeuuid(index: int) -> List[int]:
    """Generate deterministic TIMEUUID (v1) bytes from index."""
    bytes_list = [(index + i * 23) % 256 for i in range(16)]
    bytes_list[6] = (bytes_list[6] & 0x0F) | 0x10  # Version 1
    bytes_list[8] = (bytes_list[8] & 0x3F) | 0x80  # Variant 1
    return bytes_list


def make_ipv4(index: int) -> List[int]:
    return [192, 168, (index // 256) % 256, index % 256]


def make_blob(size: int, seed: int) -> List[int]:
    return [(seed + i) % 256 for i in range(size)]


def make_decimal(value: float, scale: int) -> Dict[str, Any]:
    """Create Decimal with big-endian 2's complement encoding."""
    unscaled = int(value * (10 ** scale))
    if unscaled == 0:
        return {"scale": scale, "unscaled": [0]}
    elif unscaled > 0:
        hex_str = hex(unscaled)[2:]
        if len(hex_str) % 2:
            hex_str = '0' + hex_str
        unscaled_bytes = [int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2)]
        if unscaled_bytes[0] & 0x80:
            unscaled_bytes = [0] + unscaled_bytes
        return {"scale": scale, "unscaled": unscaled_bytes}
    else:
        abs_val = abs(unscaled)
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
        if not (inverted[0] & 0x80):
            inverted = [0xFF] + inverted
        return {"scale": scale, "unscaled": inverted}


BASE_TS = 1704067200000  # 2024-01-01T00:00:00Z millis
BASE_TS_MICROS = 1704067200000000


def make_mutation(
    keyspace: str,
    table: str,
    partition_key: List[tuple],
    clustering_key: Optional[List[tuple]],
    operations: List[Dict],
    timestamp_micros: int = BASE_TS_MICROS,
) -> Dict[str, Any]:
    return {
        "table": {"keyspace": keyspace, "table": table},
        "partition_key": {"columns": partition_key},
        "clustering_key": {"columns": clustering_key} if clustering_key else None,
        "operations": operations,
        "timestamp_micros": timestamp_micros,
        "ttl_seconds": None,
        "partition_tombstone": None,
        "range_tombstones": [],
    }


# ---- UDT helpers ----

def make_address_udt(index: int) -> Dict[str, Any]:
    """Create address_type UDT value."""
    streets = ["Main St", "Oak Ave", "Elm Dr", "Pine Ln", "Maple Ct"]
    cities = ["Springfield", "Portland", "Austin", "Denver", "Seattle"]
    states = ["IL", "OR", "TX", "CO", "WA"]
    zips = ["62701", "97201", "73301", "80201", "98101"]
    return {
        "Udt": {
            "type_name": "address_type",
            "keyspace": "test_collections",
            "fields": [
                {"name": "street", "value": {"Text": f"{100 + index} {streets[index % 5]}"}},
                {"name": "city", "value": {"Text": cities[index % 5]}},
                {"name": "state", "value": {"Text": states[index % 5]}},
                {"name": "zip_code", "value": {"Text": zips[index % 5]}},
                {"name": "country", "value": {"Text": "USA"}},
            ],
        }
    }


def make_contact_info_udt(index: int) -> Dict[str, Any]:
    """Create contact_info UDT value (contains nested frozen address_type)."""
    return {
        "Udt": {
            "type_name": "contact_info",
            "keyspace": "test_collections",
            "fields": [
                {"name": "email", "value": {"Text": f"user{index}@example.com"}},
                {"name": "phone", "value": {"Text": f"+1-555-{1000 + index:04d}"}},
                {"name": "address", "value": {"Frozen": make_address_udt(index)}},
            ],
        }
    }


# ---- test_collections generators ----

def generate_collection_table() -> List[Dict]:
    """test_collections.collection_table - basic SET/LIST/MAP collections."""
    mutations = []
    for i in range(10):
        pk = [["id", {"Uuid": make_uuid(i)}]]
        ops = [
            {"Write": {"column": "tags", "value": {"Set": [
                {"Text": f"tag_{i}_{j}"} for j in range(3 + i % 4)
            ]}}},
            {"Write": {"column": "scores", "value": {"List": [
                {"Integer": 10 * (i + 1) + j} for j in range(2 + i % 3)
            ]}}},
            {"Write": {"column": "properties", "value": {"Map": [
                [{"Text": f"key_{j}"}, {"Text": f"val_{i}_{j}"}] for j in range(2 + i % 3)
            ]}}},
            {"Write": {"column": "numbers_set", "value": {"Set": [
                {"Integer": i * 100 + j} for j in range(3)
            ]}}},
            {"Write": {"column": "ordered_values", "value": {"List": [
                {"Timestamp": BASE_TS + (i * 10 + j) * 3600000} for j in range(3)
            ]}}},
            {"Write": {"column": "metadata_map", "value": {"Map": [
                [{"Text": f"metric_{j}"}, {"BigInt": 1000000 + i * 1000 + j}] for j in range(2)
            ]}}},
        ]
        mutations.append(make_mutation("test_collections", "collection_table", pk, None, ops))
    return mutations


def generate_nested_collections_table() -> List[Dict]:
    """test_collections.nested_collections_table - MAP with frozen inner collections."""
    mutations = []
    for i in range(10):
        pk = [["id", {"Uuid": make_uuid(100 + i)}]]
        ops = [
            # MAP<TEXT, FROZEN<SET<TEXT>>>
            {"Write": {"column": "tags_by_category", "value": {"Map": [
                [{"Text": f"cat_{j}"}, {"Frozen": {"Set": [
                    {"Text": f"tag_{i}_{j}_{k}"} for k in range(2)
                ]}}] for j in range(2 + i % 2)
            ]}}},
            # MAP<TEXT, FROZEN<LIST<INT>>>
            {"Write": {"column": "scores_by_game", "value": {"Map": [
                [{"Text": f"game_{j}"}, {"Frozen": {"List": [
                    {"Integer": i * 10 + j + k} for k in range(3)
                ]}}] for j in range(2)
            ]}}},
            # MAP<TEXT, FROZEN<MAP<TEXT, TEXT>>>
            {"Write": {"column": "user_preferences", "value": {"Map": [
                [{"Text": f"pref_{j}"}, {"Frozen": {"Map": [
                    [{"Text": "theme"}, {"Text": "dark" if (i + j) % 2 == 0 else "light"}],
                    [{"Text": "lang"}, {"Text": "en" if j == 0 else "es"}],
                ]}}] for j in range(2)
            ]}}},
            # MAP<DATE, FROZEN<LIST<TIMESTAMP>>>
            {"Write": {"column": "time_series_data", "value": {"Map": [
                [{"Date": 19700 + i * 10 + j}, {"Frozen": {"List": [
                    {"Timestamp": BASE_TS + (i * 100 + j * 10 + k) * 60000} for k in range(3)
                ]}}] for j in range(2)
            ]}}},
        ]
        mutations.append(make_mutation("test_collections", "nested_collections_table", pk, None, ops))
    return mutations


def generate_large_collections_table() -> List[Dict]:
    """test_collections.large_collections_table - UUID PK, INT CK, larger collections."""
    mutations = []
    for p in range(2):
        for c in range(5):
            i = p * 5 + c
            pk = [["partition_key", {"Uuid": make_uuid(200 + p)}]]
            ck = [["clustering_key", {"Integer": c}]]
            ops = [
                {"Write": {"column": "huge_set", "value": {"Set": [
                    {"Text": f"item_{i}_{j}"} for j in range(8 + i % 5)
                ]}}},
                {"Write": {"column": "massive_list", "value": {"List": [
                    {"Uuid": make_uuid(1000 + i * 10 + j)} for j in range(6 + i % 4)
                ]}}},
                {"Write": {"column": "giant_map", "value": {"Map": [
                    [{"Text": f"blob_key_{j}"}, {"Blob": make_blob(16, i * 10 + j)}]
                    for j in range(5 + i % 3)
                ]}}},
            ]
            mutations.append(make_mutation("test_collections", "large_collections_table", pk, ck, ops))
    return mutations


def generate_collections_with_udts() -> List[Dict]:
    """test_collections.collections_with_udts - collections containing UDT values."""
    mutations = []
    for i in range(10):
        pk = [["user_id", {"Uuid": make_uuid(300 + i)}]]
        ops = [
            # LIST<FROZEN<address_type>>
            {"Write": {"column": "addresses", "value": {"List": [
                {"Frozen": make_address_udt(i * 3 + j)} for j in range(2)
            ]}}},
            # SET<FROZEN<contact_info>>
            {"Write": {"column": "contacts", "value": {"Set": [
                {"Frozen": make_contact_info_udt(i * 2 + j)} for j in range(2)
            ]}}},
            # MAP<DATE, FROZEN<address_type>>
            {"Write": {"column": "locations_visited", "value": {"Map": [
                [{"Date": 19700 + i * 30 + j * 10}, {"Frozen": make_address_udt(i + j + 20)}]
                for j in range(2)
            ]}}},
            # MAP<TEXT, FROZEN<contact_info>>
            {"Write": {"column": "emergency_contacts", "value": {"Map": [
                [{"Text": "primary"}, {"Frozen": make_contact_info_udt(i + 40)}],
                [{"Text": "secondary"}, {"Frozen": make_contact_info_udt(i + 50)}],
            ]}}},
        ]
        mutations.append(make_mutation("test_collections", "collections_with_udts", pk, None, ops))
    return mutations


def generate_frozen_collections_table() -> List[Dict]:
    """test_collections.frozen_collections_table - frozen vs non-frozen collections."""
    mutations = []
    for i in range(10):
        pk = [["id", {"Uuid": make_uuid(400 + i)}]]
        ops = [
            # FROZEN<SET<TEXT>> - single cell
            {"Write": {"column": "frozen_tags", "value": {"Frozen": {"Set": [
                {"Text": f"ftag_{i}_{j}"} for j in range(3)
            ]}}}},
            # FROZEN<LIST<INT>> - single cell
            {"Write": {"column": "frozen_scores", "value": {"Frozen": {"List": [
                {"Integer": i * 10 + j} for j in range(4)
            ]}}}},
            # FROZEN<MAP<TEXT, TEXT>> - single cell
            {"Write": {"column": "frozen_properties", "value": {"Frozen": {"Map": [
                [{"Text": f"fk_{j}"}, {"Text": f"fv_{i}_{j}"}] for j in range(2)
            ]}}}},
            # SET<TEXT> - complex column (multi-cell)
            {"Write": {"column": "regular_tags", "value": {"Set": [
                {"Text": f"rtag_{i}_{j}"} for j in range(3)
            ]}}},
        ]
        mutations.append(make_mutation("test_collections", "frozen_collections_table", pk, None, ops))
    return mutations


def generate_typed_collections_table() -> List[Dict]:
    """test_collections.typed_collections_table - collections with diverse element types."""
    mutations = []
    for i in range(10):
        pk = [["id", {"Uuid": make_uuid(500 + i)}]]
        ops = [
            # SET<UUID>
            {"Write": {"column": "uuid_set", "value": {"Set": [
                {"Uuid": make_uuid(2000 + i * 3 + j)} for j in range(3)
            ]}}},
            # LIST<TIMESTAMP>
            {"Write": {"column": "timestamp_list", "value": {"List": [
                {"Timestamp": BASE_TS + (i * 10 + j) * 3600000} for j in range(3)
            ]}}},
            # MAP<TEXT, BOOLEAN>
            {"Write": {"column": "boolean_map", "value": {"Map": [
                [{"Text": f"flag_{j}"}, {"Boolean": (i + j) % 2 == 0}] for j in range(3)
            ]}}},
            # SET<DECIMAL>
            {"Write": {"column": "decimal_set", "value": {"Set": [
                {"Decimal": make_decimal(10.0 + i + j * 0.25, 2)} for j in range(3)
            ]}}},
            # LIST<BLOB>
            {"Write": {"column": "blob_list", "value": {"List": [
                {"Blob": make_blob(8, i * 10 + j)} for j in range(2)
            ]}}},
            # MAP<TEXT, INET>
            {"Write": {"column": "inet_map", "value": {"Map": [
                [{"Text": f"server_{j}"}, {"Inet": make_ipv4(i * 10 + j)}] for j in range(2)
            ]}}},
        ]
        mutations.append(make_mutation("test_collections", "typed_collections_table", pk, None, ops))
    return mutations


def generate_empty_collections_table() -> List[Dict]:
    """test_collections.empty_collections_table - empty and null collections."""
    mutations = []
    for i in range(10):
        pk = [["id", {"Uuid": make_uuid(600 + i)}]]
        if i < 5:
            # Rows 0-4: empty collections (0 elements)
            ops = [
                {"Write": {"column": "empty_set", "value": {"Set": []}}},
                {"Write": {"column": "null_list", "value": {"List": []}}},
                {"Write": {"column": "sparse_map", "value": {"Map": []}}},
                {"Write": {"column": "optional_tags", "value": {"Set": []}}},
            ]
        else:
            # Rows 5-9: only write some columns (others null/absent)
            ops = [
                {"Write": {"column": "sparse_map", "value": {"Map": [
                    [{"Text": f"key_{i}"}, {"Text": f"val_{i}"}]
                ]}}},
            ]
            if i % 2 == 0:
                ops.append({"Write": {"column": "optional_tags", "value": {"Set": [
                    {"Text": f"opt_{i}"}
                ]}}})
        mutations.append(make_mutation("test_collections", "empty_collections_table", pk, None, ops))
    return mutations


def generate_collection_clustering_table() -> List[Dict]:
    """test_collections.collection_clustering_table - FROZEN<LIST<TEXT>> as CK."""
    mutations = []
    for p in range(2):
        for c in range(5):
            i = p * 5 + c
            pk = [["partition_key", {"Uuid": make_uuid(700 + p)}]]
            # Frozen list as clustering key
            ck = [["clustering_key", {"Frozen": {"List": [
                {"Text": f"ck_{i}_{j}"} for j in range(2 + c % 3)
            ]}}]]
            ops = [
                {"Write": {"column": "data", "value": {"Text": f"data_{i}"}}},
                {"Write": {"column": "value", "value": {"Integer": 1000 + i}}},
            ]
            mutations.append(make_mutation("test_collections", "collection_clustering_table", pk, ck, ops))
    return mutations


# ---- test_timeseries generators (tables with MAP columns) ----

def generate_app_metrics() -> List[Dict]:
    """test_timeseries.app_metrics - (TEXT,TEXT) PK, TIMESTAMP CK, MAP<TEXT,TEXT> tags."""
    mutations = []
    apps = ["web-frontend", "api-backend"]
    metrics = ["cpu_usage"]
    for a_idx, app in enumerate(apps):
        for m_idx, metric in enumerate(metrics):
            for t in range(5):
                i = a_idx * 5 + t
                pk = [
                    ["application_id", {"Text": app}],
                    ["metric_name", {"Text": metric}],
                ]
                ck = [["timestamp", {"Timestamp": BASE_TS + i * 60000}]]
                ops = [
                    {"Write": {"column": "value", "value": {"Float": 45.0 + i * 2.5}}},
                    {"Write": {"column": "unit", "value": {"Text": "percent"}}},
                    {"Write": {"column": "tags", "value": {"Map": [
                        [{"Text": "env"}, {"Text": "prod" if a_idx == 0 else "staging"}],
                        [{"Text": "region"}, {"Text": "us-east-1"}],
                        [{"Text": "instance"}, {"Text": f"i-{i:04d}"}],
                    ]}}},
                ]
                mutations.append(make_mutation("test_timeseries", "app_metrics", pk, ck, ops))
    return mutations


def generate_user_activity() -> List[Dict]:
    """test_timeseries.user_activity - (UUID,DATE) PK, TIMESTAMP CK, MAP<TEXT,TEXT> metadata."""
    mutations = []
    for u in range(2):
        for t in range(5):
            i = u * 5 + t
            pk = [
                ["user_id", {"Uuid": make_uuid(800 + u)}],
                ["activity_date", {"Date": 19700 + i}],
            ]
            ck = [["activity_time", {"Timestamp": BASE_TS + i * 600000}]]
            activity_types = ["page_view", "click", "search", "purchase", "logout"]
            ops = [
                {"Write": {"column": "activity_type", "value": {"Text": activity_types[i % 5]}}},
                {"Write": {"column": "page_url", "value": {"Text": f"https://example.com/page/{i}"}}},
                {"Write": {"column": "session_id", "value": {"Uuid": make_uuid(900 + i)}}},
                {"Write": {"column": "duration_ms", "value": {"Integer": 500 + i * 100}}},
                {"Write": {"column": "metadata", "value": {"Map": [
                    [{"Text": "browser"}, {"Text": "Chrome"}],
                    [{"Text": "os"}, {"Text": "macOS" if u == 0 else "Windows"}],
                ]}}},
            ]
            mutations.append(make_mutation("test_timeseries", "user_activity", pk, ck, ops))
    return mutations


def generate_event_store() -> List[Dict]:
    """test_timeseries.event_store - UUID PK, BIGINT CK, MAP<TEXT,TEXT> metadata."""
    mutations = []
    for a in range(2):
        for v in range(5):
            i = a * 5 + v
            pk = [["aggregate_id", {"Uuid": make_uuid(1000 + a)}]]
            ck = [["version", {"BigInt": v + 1}]]
            event_types = ["Created", "Updated", "Published", "Archived", "Deleted"]
            ops = [
                {"Write": {"column": "event_id", "value": {"Uuid": make_timeuuid(i)}}},
                {"Write": {"column": "event_type", "value": {"Text": event_types[v]}}},
                {"Write": {"column": "event_data", "value": {"Text": f'{{"action":"{event_types[v].lower()}","index":{i}}}'}}},
                {"Write": {"column": "metadata", "value": {"Map": [
                    [{"Text": "source"}, {"Text": "api"}],
                    [{"Text": "user"}, {"Text": f"user_{a}"}],
                    [{"Text": "ip"}, {"Text": f"10.0.{a}.{v}"}],
                ]}}},
                {"Write": {"column": "created_at", "value": {"Timestamp": BASE_TS + i * 3600000}}},
            ]
            mutations.append(make_mutation("test_timeseries", "event_store", pk, ck, ops))
    return mutations


def generate_user_sessions() -> List[Dict]:
    """test_timeseries.user_sessions - UUID PK, MAP<TEXT,TEXT> device_info."""
    mutations = []
    for i in range(10):
        pk = [["session_id", {"Uuid": make_uuid(1100 + i)}]]
        ops = [
            {"Write": {"column": "user_id", "value": {"Uuid": make_uuid(1200 + i)}}},
            {"Write": {"column": "start_time", "value": {"Timestamp": BASE_TS + i * 7200000}}},
            {"Write": {"column": "last_activity", "value": {"Timestamp": BASE_TS + i * 7200000 + 3600000}}},
            {"Write": {"column": "ip_address", "value": {"Inet": make_ipv4(i)}}},
            {"Write": {"column": "user_agent", "value": {"Text": f"Mozilla/5.0 (test agent {i})"}}},
            {"Write": {"column": "device_info", "value": {"Map": [
                [{"Text": "os"}, {"Text": "macOS" if i % 3 == 0 else "Windows" if i % 3 == 1 else "Linux"}],
                [{"Text": "browser"}, {"Text": "Chrome" if i % 2 == 0 else "Firefox"}],
                [{"Text": "resolution"}, {"Text": "1920x1080"}],
            ]}}},
            {"Write": {"column": "is_active", "value": {"Boolean": i % 3 != 2}}},
        ]
        mutations.append(make_mutation("test_timeseries", "user_sessions", pk, None, ops))
    return mutations


# ---- test_wide_rows generators (tables with collection columns) ----

def generate_chat_messages() -> List[Dict]:
    """test_wide_rows.chat_messages - LIST<TEXT>, MAP<TEXT, FROZEN<SET<UUID>>>, MAP<TEXT,TEXT>."""
    mutations = []
    for ch in range(2):
        for m in range(5):
            i = ch * 5 + m
            pk = [["channel_id", {"Uuid": make_uuid(1300 + ch)}]]
            ck = [
                ["message_timestamp", {"Timestamp": BASE_TS + i * 60000}],
                ["message_id", {"Uuid": make_timeuuid(i)}],
            ]
            ops = [
                {"Write": {"column": "user_id", "value": {"Uuid": make_uuid(1400 + i % 3)}}},
                {"Write": {"column": "username", "value": {"Text": f"user_{i % 3}"}}},
                {"Write": {"column": "message_content", "value": {"Text": f"Message {i}: Hello from channel {ch}!"}}},
                # LIST<TEXT> attachments
                {"Write": {"column": "attachments", "value": {"List": [
                    {"Text": f"https://files.example.com/file_{i}_{j}.png"} for j in range(i % 3)
                ] if i % 3 > 0 else [{"Text": "https://files.example.com/default.png"}]}}},
                # MAP<TEXT, FROZEN<SET<UUID>>> reactions
                {"Write": {"column": "reactions", "value": {"Map": [
                    [{"Text": "thumbs_up"}, {"Frozen": {"Set": [
                        {"Uuid": make_uuid(1500 + i * 2 + j)} for j in range(1 + i % 2)
                    ]}}],
                    [{"Text": "heart"}, {"Frozen": {"Set": [
                        {"Uuid": make_uuid(1600 + i)}
                    ]}}],
                ]}}},
                {"Write": {"column": "thread_id", "value": {"Uuid": make_uuid(1700 + ch)}}},
                {"Write": {"column": "reply_count", "value": {"Integer": i * 2}}},
                {"Write": {"column": "edited_at", "value": {"Timestamp": BASE_TS + i * 60000 + 30000}}},
                # MAP<TEXT, TEXT> metadata
                {"Write": {"column": "metadata", "value": {"Map": [
                    [{"Text": "client"}, {"Text": "web"}],
                    [{"Text": "version"}, {"Text": "2.1.0"}],
                ]}}},
            ]
            mutations.append(make_mutation("test_wide_rows", "chat_messages", pk, ck, ops))
    return mutations


def generate_document_versions() -> List[Dict]:
    """test_wide_rows.document_versions - SET<TEXT>, MAP<TEXT,TEXT>."""
    mutations = []
    for d in range(2):
        for v in range(5):
            i = d * 5 + v
            pk = [["document_id", {"Uuid": make_uuid(1800 + d)}]]
            ck = [["version_number", {"Integer": v + 1}]]
            ops = [
                {"Write": {"column": "created_at", "value": {"Timestamp": BASE_TS + i * 86400000}}},
                {"Write": {"column": "author_id", "value": {"Uuid": make_uuid(1900 + i % 3)}}},
                {"Write": {"column": "title", "value": {"Text": f"Document {d} v{v + 1}"}}},
                {"Write": {"column": "content", "value": {"Text": f"Content for version {v + 1} of document {d}."}}},
                # SET<TEXT> tags
                {"Write": {"column": "tags", "value": {"Set": [
                    {"Text": f"tag_{j}"} for j in range(2 + v % 3)
                ]}}},
                # MAP<TEXT, TEXT> metadata
                {"Write": {"column": "metadata", "value": {"Map": [
                    [{"Text": "format"}, {"Text": "markdown"}],
                    [{"Text": "reviewer"}, {"Text": f"reviewer_{i % 4}"}],
                ]}}},
                {"Write": {"column": "word_count", "value": {"Integer": 500 + v * 200}}},
                {"Write": {"column": "character_count", "value": {"Integer": 3000 + v * 1200}}},
                {"Write": {"column": "change_summary", "value": {"Text": f"Revision {v + 1} changes"}}},
            ]
            mutations.append(make_mutation("test_wide_rows", "document_versions", pk, ck, ops))
    return mutations


def generate_product_catalog() -> List[Dict]:
    """test_wide_rows.product_catalog - many collection types."""
    mutations = []
    for cat in range(2):
        for prod in range(5):
            i = cat * 5 + prod
            pk = [["category_id", {"Uuid": make_uuid(2000 + cat)}]]
            ck = [["product_id", {"Uuid": make_uuid(2100 + i)}]]
            ops = [
                {"Write": {"column": "product_name", "value": {"Text": f"Product {i}"}}},
                {"Write": {"column": "description", "value": {"Text": f"Short description for product {i}"}}},
                {"Write": {"column": "long_description", "value": {"Text": f"Detailed description for product {i}. Features include high quality, durability, and great value."}}},
                # MAP<TEXT, TEXT> specifications
                {"Write": {"column": "specifications", "value": {"Map": [
                    [{"Text": "color"}, {"Text": "blue" if i % 2 == 0 else "red"}],
                    [{"Text": "size"}, {"Text": "medium"}],
                    [{"Text": "material"}, {"Text": "aluminum"}],
                ]}}},
                # LIST<TEXT> images
                {"Write": {"column": "images", "value": {"List": [
                    {"Text": f"https://img.example.com/prod_{i}_{j}.jpg"} for j in range(3)
                ]}}},
                # SET<TEXT> tags
                {"Write": {"column": "tags", "value": {"Set": [
                    {"Text": f"tag_{j}"} for j in range(2 + i % 3)
                ]}}},
                {"Write": {"column": "price", "value": {"Decimal": make_decimal(29.99 + i * 10.0, 2)}}},
                {"Write": {"column": "currency", "value": {"Text": "USD"}}},
                {"Write": {"column": "availability_count", "value": {"Integer": 100 + i * 10}}},
                {"Write": {"column": "weight", "value": {"Float32": 0.5 + i * 0.2}}},
                # MAP<TEXT, FLOAT> dimensions
                {"Write": {"column": "dimensions", "value": {"Map": [
                    [{"Text": "height"}, {"Float32": 10.0 + i}],
                    [{"Text": "width"}, {"Float32": 5.0 + i * 0.5}],
                    [{"Text": "depth"}, {"Float32": 2.0 + i * 0.3}],
                ]}}},
                # MAP<TEXT, DOUBLE> reviews_summary
                {"Write": {"column": "reviews_summary", "value": {"Map": [
                    [{"Text": "avg_rating"}, {"Float": 3.5 + (i % 5) * 0.3}],
                    [{"Text": "total_reviews"}, {"Float": float(50 + i * 10)}],
                ]}}},
                # MAP<TEXT, FROZEN<SET<TEXT>>> attributes
                {"Write": {"column": "attributes", "value": {"Map": [
                    [{"Text": "colors"}, {"Frozen": {"Set": [
                        {"Text": "blue"}, {"Text": "green"}, {"Text": "red"}
                    ]}}],
                    [{"Text": "sizes"}, {"Frozen": {"Set": [
                        {"Text": "L"}, {"Text": "M"}, {"Text": "S"}
                    ]}}],
                ]}}},
                # SET<UUID> related_products
                {"Write": {"column": "related_products", "value": {"Set": [
                    {"Uuid": make_uuid(2200 + (i + j) % 10)} for j in range(2)
                ]}}},
                {"Write": {"column": "created_at", "value": {"Timestamp": BASE_TS + i * 86400000}}},
                {"Write": {"column": "updated_at", "value": {"Timestamp": BASE_TS + i * 86400000 + 3600000}}},
            ]
            mutations.append(make_mutation("test_wide_rows", "product_catalog", pk, ck, ops))
    return mutations


def generate_sparse_data_table() -> List[Dict]:
    """test_wide_rows.sparse_data_table - sparse rows with optional collection columns."""
    mutations = []
    attribute_names = ["profile", "status", "metrics", "event_time", "payload"]

    for entity_index in range(2):
        entity_id = make_uuid(2300 + entity_index)
        for attr_index, attribute_name in enumerate(attribute_names):
            i = entity_index * len(attribute_names) + attr_index
            pk = [["entity_id", {"Uuid": entity_id}]]
            ck = [["attribute_name", {"Text": attribute_name}]]

            if attribute_name == "profile":
                ops = [
                    {"Write": {"column": "string_value", "value": {"Text": f"profile_{entity_index}"}}},
                    {"Write": {"column": "set_value", "value": {"Set": [
                        {"Text": "blue"},
                        {"Text": "green"},
                        {"Text": f"tier_{entity_index}"},
                    ]}}},
                    {"Write": {"column": "map_value", "value": {"Map": [
                        [{"Text": "region"}, {"Text": "us-west-2" if entity_index == 0 else "us-east-1"}],
                        [{"Text": "segment"}, {"Text": f"segment_{entity_index}"}],
                    ]}}},
                ]
            elif attribute_name == "status":
                ops = [
                    {"Write": {"column": "boolean_value", "value": {"Boolean": entity_index == 0}}},
                    {"Write": {"column": "list_value", "value": {"List": [
                        {"Text": "active"},
                        {"Text": "verified"},
                        {"Text": f"step_{i}"},
                    ]}}},
                ]
            elif attribute_name == "metrics":
                ops = [
                    {"Write": {"column": "numeric_value", "value": {"Float": 98.5 + i * 1.25}}},
                    {"Write": {"column": "map_value", "value": {"Map": [
                        [{"Text": "cpu"}, {"Text": f"{40 + i}"}],
                        [{"Text": "memory"}, {"Text": f"{2048 + i * 128}"}],
                    ]}}},
                ]
            elif attribute_name == "event_time":
                ops = [
                    {"Write": {"column": "timestamp_value", "value": {"Timestamp": BASE_TS + i * 600000}}},
                    {"Write": {"column": "list_value", "value": {"List": [
                        {"Text": f"evt_{i}_0"},
                        {"Text": f"evt_{i}_1"},
                    ]}}},
                ]
            else:
                ops = [
                    {"Write": {"column": "json_value", "value": {"Text": f'{{"entity":{entity_index},"slot":"{attribute_name}"}}'}}},
                    {"Write": {"column": "blob_value", "value": {"Blob": make_blob(12, i)}}},
                    {"Write": {"column": "set_value", "value": {"Set": [
                        {"Text": "raw"},
                        {"Text": f"payload_{i}"},
                    ]}}},
                ]

            mutations.append(make_mutation("test_wide_rows", "sparse_data_table", pk, ck, ops))
    return mutations


# ---- Main ----

def main():
    output_dir = Path("e2e_collections")
    output_dir.mkdir(exist_ok=True)

    tables = [
        # test_collections (8)
        ("collection_table", generate_collection_table),
        ("nested_collections_table", generate_nested_collections_table),
        ("large_collections_table", generate_large_collections_table),
        ("collections_with_udts", generate_collections_with_udts),
        ("frozen_collections_table", generate_frozen_collections_table),
        ("typed_collections_table", generate_typed_collections_table),
        ("empty_collections_table", generate_empty_collections_table),
        ("collection_clustering_table", generate_collection_clustering_table),
        # test_timeseries (4)
        ("app_metrics", generate_app_metrics),
        ("user_activity", generate_user_activity),
        ("event_store", generate_event_store),
        ("user_sessions", generate_user_sessions),
        # test_wide_rows (4)
        ("chat_messages", generate_chat_messages),
        ("document_versions", generate_document_versions),
        ("product_catalog", generate_product_catalog),
        ("sparse_data_table", generate_sparse_data_table),
    ]

    total = 0
    for table_name, gen_func in tables:
        mutations = gen_func()
        output_file = output_dir / f"{table_name}.jsonl"
        with output_file.open("w") as f:
            for m in mutations:
                f.write(json.dumps(m) + "\n")
        print(f"Generated {len(mutations):3d} mutations for {table_name:35s} -> {output_file}")
        total += len(mutations)

    print(f"\nTotal: {total} mutations across {len(tables)} tables")
    print(f"Output directory: {output_dir.absolute()}")


if __name__ == "__main__":
    main()
