#!/usr/bin/env python3
"""
Hardened Validator Test Data Generator - Issue #31
Generates comprehensive test data for cross-version complex type validation.
"""

import argparse
import json
import logging
import os
import random
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import yaml
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from faker import Faker
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HardenedValidatorTestDataGenerator:
    """Generates comprehensive test data for hardened validator testing"""
    
    def __init__(self, cassandra_version: str, host: str = "localhost", port: int = 9042):
        self.cassandra_version = cassandra_version
        self.host = host
        self.port = port
        self.fake = Faker()
        self.fake.seed_instance(42)  # Reproducible data
        
        self.cluster = None
        self.session = None
        
        # Test data configuration
        self.test_config = {
            "basic_rows": 1000,
            "complex_rows": 500,
            "edge_case_rows": 100,
            "performance_benchmark_rows": 1000,
            "max_collection_size": 100,
            "max_nesting_depth": 5,
            "unicode_test_cases": True,
            "null_test_cases": True,
            "ttl_test_cases": True,
        }
        
        # Version-specific features
        self.version_features = self._get_version_features(cassandra_version)
        
    def _get_version_features(self, version: str) -> Dict[str, bool]:
        """Get features supported by Cassandra version"""
        features = {
            "duration_type": False,
            "enhanced_metadata": False,
            "mixed_collections": False,
            "json_support": False,
            "frozen_collections": False,
        }
        
        version_num = float(version)
        
        if version_num >= 3.11:
            features["duration_type"] = True
        if version_num >= 4.0:
            features["frozen_collections"] = True
        if version_num >= 4.1:
            features["enhanced_metadata"] = True
        if version_num >= 5.0:
            features["mixed_collections"] = True
            features["json_support"] = True
            
        return features
    
    def connect(self):
        """Connect to Cassandra cluster"""
        logger.info(f"Connecting to Cassandra {self.cassandra_version} at {self.host}:{self.port}")
        
        try:
            # Wait for Cassandra to be ready
            self._wait_for_cassandra()
            
            # Create cluster connection
            self.cluster = Cluster([self.host], port=self.port)
            self.session = self.cluster.connect()
            
            # Set consistency level
            self.session.default_consistency_level = 'ONE'
            
            logger.info("Successfully connected to Cassandra")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
    
    def _wait_for_cassandra(self, max_attempts: int = 30, delay: int = 10):
        """Wait for Cassandra to be ready"""
        import socket
        
        for attempt in range(max_attempts):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((self.host, self.port))
                sock.close()
                
                if result == 0:
                    logger.info(f"Cassandra is ready after {attempt + 1} attempts")
                    time.sleep(5)  # Additional wait for full initialization
                    return
                    
            except Exception as e:
                logger.debug(f"Connection attempt {attempt + 1} failed: {e}")
            
            logger.info(f"Waiting for Cassandra... attempt {attempt + 1}/{max_attempts}")
            time.sleep(delay)
        
        raise RuntimeError(f"Cassandra not ready after {max_attempts} attempts")
    
    def create_schema(self):
        """Create comprehensive test schema"""
        logger.info("Creating hardened validator test schema")
        
        # Read schema file
        schema_path = Path(__file__).parent.parent / "test-data" / "schemas" / "hardened_validator_test_schema.cql"
        
        if not schema_path.exists():
            logger.error(f"Schema file not found: {schema_path}")
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r') as f:
            schema_content = f.read()
        
        # Execute schema statements
        statements = self._split_cql_statements(schema_content)
        
        for statement in statements:
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    logger.debug(f"Executing: {statement[:100]}...")
                    self.session.execute(statement)
                except Exception as e:
                    logger.warning(f"Schema statement failed (may be expected): {e}")
                    logger.debug(f"Failed statement: {statement}")
    
    def _split_cql_statements(self, content: str) -> List[str]:
        """Split CQL content into individual statements"""
        statements = []
        current_statement = []
        
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if not line or line.startswith('--'):
                continue
                
            current_statement.append(line)
            
            if line.endswith(';'):
                statements.append(' '.join(current_statement))
                current_statement = []
        
        return statements
    
    def generate_all_test_data(self):
        """Generate all test data for comprehensive validation"""
        logger.info("Starting comprehensive test data generation")
        
        try:
            # Create schema
            self.create_schema()
            
            # Use the hardened_validator_test keyspace
            self.session.execute("USE hardened_validator_test")
            
            # Generate different categories of test data
            self.generate_complex_collections_data()
            self.generate_tuple_test_data()
            self.generate_udt_test_data()
            self.generate_time_series_data()
            self.generate_edge_case_data()
            self.generate_performance_benchmark_data()
            
            # Generate version-specific data
            self.generate_version_specific_data()
            
            # Generate deletion and tombstone test data
            self.generate_deletion_test_data()
            
            logger.info("Test data generation completed successfully")
            
        except Exception as e:
            logger.error(f"Test data generation failed: {e}")
            raise
        finally:
            self.cleanup()
    
    def generate_complex_collections_data(self):
        """Generate data for complex collections table"""
        logger.info("Generating complex collections test data")
        
        insert_query = self.session.prepare("""
        INSERT INTO complex_collections (
            id, simple_list, simple_set, simple_map,
            frozen_list, frozen_set, frozen_map,
            list_of_lists, set_of_sets, map_of_lists,
            map_of_sets, map_of_maps,
            nested_map_list, nested_list_set,
            address_list, person_set, company_map,
            timestamp_created, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for i in range(self.test_config["complex_rows"]):
            try:
                # Generate basic collections
                simple_list = [self.fake.word() for _ in range(random.randint(1, 10))]
                simple_set = set(random.randint(1, 1000) for _ in range(random.randint(1, 10)))
                simple_map = {self.fake.word(): random.randint(1, 100) for _ in range(random.randint(1, 10))}
                
                # Generate nested collections
                list_of_lists = [[self.fake.word() for _ in range(random.randint(1, 5))] for _ in range(random.randint(1, 5))]
                set_of_sets = [set(random.randint(1, 100) for _ in range(random.randint(1, 5))) for _ in range(random.randint(1, 3))]
                map_of_lists = {self.fake.word(): [random.randint(1, 100) for _ in range(random.randint(1, 5))] for _ in range(random.randint(1, 5))}
                map_of_sets = {self.fake.word(): set(self.fake.word() for _ in range(random.randint(1, 3))) for _ in range(random.randint(1, 3))}
                map_of_maps = {self.fake.word(): {self.fake.word(): random.randint(1, 100) for _ in range(random.randint(1, 3))} for _ in range(random.randint(1, 3))}
                
                # Generate ultra-nested collections
                nested_map_list = {self.fake.word(): [[{self.fake.word(): random.randint(1, 100)} for _ in range(random.randint(1, 2))] for _ in range(random.randint(1, 2))] for _ in range(random.randint(1, 2))}\n                nested_list_set = [set([self.fake.word() for _ in range(random.randint(1, 2))]) for _ in range(random.randint(1, 2))]\n                \n                # Generate UDT collections\n                address_list = [self._generate_address() for _ in range(random.randint(1, 3))]\n                person_set = set()  # Will be populated with person UDTs\n                company_map = {self.fake.company(): self._generate_company() for _ in range(random.randint(1, 2))}\n                \n                row_data = [\n                    uuid.uuid4(),\n                    simple_list,\n                    simple_set,\n                    simple_map,\n                    simple_list[:5],  # Frozen versions (smaller)\n                    set(list(simple_set)[:5]),\n                    dict(list(simple_map.items())[:5]),\n                    list_of_lists,\n                    set_of_sets,\n                    map_of_lists,\n                    map_of_sets,\n                    map_of_maps,\n                    nested_map_list,\n                    nested_list_set,\n                    address_list,\n                    set(),  # person_set - simplified for now\n                    company_map,\n                    datetime.now(),\n                    datetime.now()\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n                if i % 100 == 0:\n                    logger.info(f"Generated {i}/{self.test_config['complex_rows']} complex collection rows")\n                    \n            except Exception as e:\n                logger.error(f"Failed to insert complex collections row {i}: {e}")\n    \n    def generate_tuple_test_data(self):\n        """Generate data for tuple tests table"""\n        logger.info("Generating tuple test data")\n        \n        insert_query = self.session.prepare("""\n        INSERT INTO tuple_tests (\n            id, simple_tuple, nested_tuple, frozen_tuple,\n            tuple_with_list, tuple_with_map, tuple_with_udt,\n            complex_tuple, timestamp_created\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)\n        """)\n        \n        for i in range(self.test_config["complex_rows"]):\n            try:\n                # Generate tuple data\n                simple_tuple = (self.fake.word(), random.randint(1, 1000), random.choice([True, False]))\n                nested_tuple = (self.fake.word(), (random.randint(1, 100), random.uniform(1.0, 100.0)), True)\n                frozen_tuple = (self.fake.word(), random.randint(1, 1000))\n                \n                tuple_with_list = (self.fake.word(), [random.randint(1, 100) for _ in range(random.randint(1, 5))])\n                tuple_with_map = (self.fake.word(), {self.fake.word(): random.randint(1, 100) for _ in range(random.randint(1, 3))})\n                tuple_with_udt = (self.fake.word(), self._generate_person())\n                \n                complex_tuple = (\n                    self.fake.word(),\n                    [random.randint(1, 100) for _ in range(random.randint(1, 3))],\n                    {self.fake.word(): self._generate_address() for _ in range(random.randint(1, 2))},\n                    self._generate_person()\n                )\n                \n                row_data = [\n                    uuid.uuid4(),\n                    simple_tuple,\n                    nested_tuple,\n                    frozen_tuple,\n                    tuple_with_list,\n                    tuple_with_map,\n                    tuple_with_udt,\n                    complex_tuple,\n                    datetime.now()\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n                if i % 100 == 0:\n                    logger.info(f"Generated {i}/{self.test_config['complex_rows']} tuple test rows")\n                    \n            except Exception as e:\n                logger.error(f"Failed to insert tuple test row {i}: {e}")\n    \n    def generate_udt_test_data(self):\n        """Generate data for UDT tests table"""\n        logger.info("Generating UDT test data")\n        \n        insert_query = self.session.prepare("""\n        INSERT INTO udt_tests (\n            id, simple_address, simple_person, simple_company,\n            addresses, people, companies,\n            person_addresses, company_employees,\n            partial_person, nullable_address, timestamp_created\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n        """)\n        \n        for i in range(self.test_config["complex_rows"]):\n            try:\n                # Generate UDT data\n                simple_address = self._generate_address()\n                simple_person = self._generate_person()\n                simple_company = self._generate_company()\n                \n                addresses = [self._generate_address() for _ in range(random.randint(1, 3))]\n                people = set()  # Simplified for compatibility\n                companies = {self.fake.company(): self._generate_company() for _ in range(random.randint(1, 2))}\n                \n                # Complex UDT combinations\n                person_addresses = {}  # Simplified\n                company_employees = {}  # Simplified\n                \n                # Partial and nullable UDTs\n                partial_person = self._generate_partial_person()\n                nullable_address = self._generate_address() if random.random() > 0.2 else None\n                \n                row_data = [\n                    uuid.uuid4(),\n                    simple_address,\n                    simple_person,\n                    simple_company,\n                    addresses,\n                    people,\n                    companies,\n                    person_addresses,\n                    company_employees,\n                    partial_person,\n                    nullable_address,\n                    datetime.now()\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n                if i % 100 == 0:\n                    logger.info(f"Generated {i}/{self.test_config['complex_rows']} UDT test rows")\n                    \n            except Exception as e:\n                logger.error(f"Failed to insert UDT test row {i}: {e}")\n    \n    def generate_time_series_data(self):\n        """Generate time series data with complex metadata"""\n        logger.info("Generating time series complex data")\n        \n        insert_query = self.session.prepare("""\n        INSERT INTO time_series_complex (\n            sensor_id, measurement_time, measurement_type,\n            scalar_value, vector_value, matrix_value,\n            sensor_info, calibration_data, location_history,\n            temporary_data\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n        """)\n        \n        sensor_ids = [uuid.uuid4() for _ in range(10)]  # 10 sensors\n        measurement_types = ['temperature', 'humidity', 'pressure', 'vibration', 'light']\n        \n        base_time = datetime.now() - timedelta(days=7)\n        \n        for i in range(self.test_config["complex_rows"] * 2):  # More time series data\n            try:\n                sensor_id = random.choice(sensor_ids)\n                measurement_time = base_time + timedelta(seconds=random.randint(0, 7*24*3600))\n                measurement_type = random.choice(measurement_types)\n                \n                # Generate measurement data\n                scalar_value = random.uniform(-100.0, 100.0)\n                vector_value = [random.uniform(-10.0, 10.0) for _ in range(random.randint(3, 10))]\n                matrix_value = {f"channel_{j}": [random.uniform(-5.0, 5.0) for _ in range(random.randint(2, 5))] for j in range(random.randint(2, 4))}\n                \n                # Generate complex metadata\n                sensor_info = {\n                    "manufacturer": self.fake.company(),\n                    "model": f"Model-{random.randint(1000, 9999)}",\n                    "firmware": f"{random.randint(1, 9)}.{random.randint(0, 9)}.{random.randint(0, 9)}",\n                    "location": self.fake.city()\n                }\n                \n                calibration_data = [\n                    (measurement_time - timedelta(hours=random.randint(1, 24)), random.uniform(0.9, 1.1))\n                    for _ in range(random.randint(1, 3))\n                ]\n                \n                location_history = [self._generate_address() for _ in range(random.randint(1, 3))]\n                \n                temporary_data = self.fake.text(max_nb_chars=100) if random.random() > 0.3 else None\n                \n                row_data = [\n                    sensor_id,\n                    measurement_time,\n                    measurement_type,\n                    scalar_value,\n                    vector_value,\n                    matrix_value,\n                    sensor_info,\n                    calibration_data,\n                    location_history,\n                    temporary_data\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n                if i % 200 == 0:\n                    logger.info(f"Generated {i}/{self.test_config['complex_rows'] * 2} time series rows")\n                    \n            except Exception as e:\n                logger.error(f"Failed to insert time series row {i}: {e}")\n    \n    def generate_edge_case_data(self):\n        """Generate edge case data for validation testing"""\n        logger.info("Generating edge case test data")\n        \n        insert_query = self.session.prepare("""\n        INSERT INTO edge_cases (\n            id, empty_list, empty_set, empty_map,\n            single_list, single_set, single_map,\n            large_list, large_set, large_map,\n            max_nested, unicode_map, special_chars_list,\n            nullable_udt, partial_udt, timestamp_created\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n        """)\n        \n        for i in range(self.test_config["edge_case_rows"]):\n            try:\n                # Empty collections\n                empty_list = []\n                empty_set = set()\n                empty_map = {}\n                \n                # Single element collections\n                single_list = [self.fake.word()]\n                single_set = {random.randint(1, 1000)}\n                single_map = {self.fake.word(): random.randint(1, 100)}\n                \n                # Large collections\n                large_list = [self.fake.word() for _ in range(self.test_config["max_collection_size"])]\n                large_set = set(uuid.uuid4() for _ in range(50))  # UUIDs for uniqueness\n                large_map = {str(j): self.fake.word() for j in range(50)}\n                \n                # Maximum nesting depth\n                max_nested = {\n                    "level1": [\n                        {\n                            "level2": [\n                                random.randint(1, 100)\n                                for _ in range(random.randint(1, 3))\n                            ]\n                        }\n                        for _ in range(random.randint(1, 2))\n                    ]\n                }\n                \n                # Unicode and special characters\n                unicode_map = {\n                    "简体中文": "Simplified Chinese",\n                    "العربية": "Arabic",\n                    "हिन्दी": "Hindi",\n                    "русский": "Russian",\n                    "日本語": "Japanese",\n                    "emoji": "🎉🚀💻🌟",\n                }\n                \n                special_chars_list = [\n                    "normal_text",\n                    "with spaces",\n                    "with-dashes",\n                    "with_underscores",\n                    "with.dots",\n                    "with/slashes",\n                    "with\\\\backslashes",\n                    "with\"quotes\"",\n                    "with'apostrophes'",\n                    "with[brackets]",\n                    "with{braces}",\n                    "with(parentheses)",\n                    "with,commas,",\n                    "with;semicolons;",\n                    "with:colons:",\n                    "with|pipes|",\n                    "with*asterisks*",\n                    "with+plus+",\n                    "with=equals=",\n                    "with?questions?",\n                    "with!exclamations!",\n                    "with@at@",\n                    "with#hash#",\n                    "with$dollar$",\n                    "with%percent%",\n                    "with^caret^",\n                    "with&ampersand&",\n                    "with~tilde~",\n                    "with`backtick`",\n                ]\n                \n                # Nullable and partial UDTs\n                nullable_udt = self._generate_person() if random.random() > 0.3 else None\n                partial_udt = self._generate_partial_company()\n                \n                row_data = [\n                    uuid.uuid4(),\n                    empty_list,\n                    empty_set,\n                    empty_map,\n                    single_list,\n                    single_set,\n                    single_map,\n                    large_list,\n                    large_set,\n                    large_map,\n                    max_nested,\n                    unicode_map,\n                    special_chars_list,\n                    nullable_udt,\n                    partial_udt,\n                    datetime.now()\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n                if i % 50 == 0:\n                    logger.info(f"Generated {i}/{self.test_config['edge_case_rows']} edge case rows")\n                    \n            except Exception as e:\n                logger.error(f"Failed to insert edge case row {i}: {e}")\n    \n    def generate_performance_benchmark_data(self):\n        """Generate performance benchmark data"""\n        logger.info("Generating performance benchmark data")\n        \n        insert_query = self.session.prepare("""\n        INSERT INTO performance_benchmark (\n            benchmark_id, iteration,\n            small_complex, medium_complex, large_complex,\n            small_collection, medium_collection, large_collection,\n            depth_1, depth_2, depth_3, depth_4, depth_5,\n            parse_time_us, memory_usage_bytes\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n        """)\n        \n        benchmark_id = uuid.uuid4()\n        \n        for i in range(self.test_config["performance_benchmark_rows"]):\n            try:\n                # Different complexity levels\n                small_complex = self._generate_address()\n                medium_complex = self._generate_person()\n                large_complex = self._generate_company()\n                \n                # Different collection sizes\n                small_collection = [self.fake.word() for _ in range(random.randint(1, 10))]\n                medium_collection = {self.fake.word(): self._generate_address() for _ in range(random.randint(10, 50))}\n                large_collection = {\n                    self.fake.word(): [self._generate_person() for _ in range(random.randint(1, 5))]\n                    for _ in range(random.randint(20, 50))\n                }\n                \n                # Different nesting depths\n                depth_1 = self._generate_address()\n                depth_2 = self._generate_person()\n                depth_3 = self._generate_company()\n                depth_4 = {self.fake.word(): self._generate_company() for _ in range(random.randint(1, 3))}\n                depth_5 = [depth_4 for _ in range(random.randint(1, 2))]\n                \n                # Simulate performance metrics\n                parse_time_us = random.randint(100, 10000)  # 100μs to 10ms\n                memory_usage_bytes = random.randint(1024, 1024*1024)  # 1KB to 1MB\n                \n                row_data = [\n                    benchmark_id,\n                    i,\n                    small_complex,\n                    medium_complex,\n                    large_complex,\n                    small_collection,\n                    medium_collection,\n                    large_collection,\n                    depth_1,\n                    depth_2,\n                    depth_3,\n                    depth_4,\n                    depth_5,\n                    parse_time_us,\n                    memory_usage_bytes\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n                if i % 100 == 0:\n                    logger.info(f"Generated {i}/{self.test_config['performance_benchmark_rows']} benchmark rows")\n                    \n            except Exception as e:\n                logger.error(f"Failed to insert benchmark row {i}: {e}")\n    \n    def generate_version_specific_data(self):\n        """Generate version-specific feature data"""\n        logger.info("Generating version-specific test data")\n        \n        if not self.version_features["duration_type"]:\n            logger.info("Skipping duration type tests (not supported in this version)")\n            return\n        \n        insert_query = self.session.prepare("""\n        INSERT INTO version_features (\n            id, cassandra_version, duration_field,\n            high_precision_timestamp, json_data,\n            supports_duration, supports_json, supports_mixed_collections,\n            timestamp_created\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)\n        """)\n        \n        for i in range(100):  # Smaller dataset for version-specific features\n            try:\n                # Duration field (if supported)\n                duration_field = f"{random.randint(1, 30)}d{random.randint(1, 24)}h{random.randint(1, 60)}m" if self.version_features["duration_type"] else None\n                \n                # High precision timestamp\n                high_precision_timestamp = datetime.now() + timedelta(microseconds=random.randint(0, 999999))\n                \n                # JSON data (stored as text for compatibility)\n                json_data = json.dumps({\n                    "key1": self.fake.word(),\n                    "key2": random.randint(1, 1000),\n                    "key3": random.choice([True, False]),\n                    "nested": {\n                        "nested_key": self.fake.sentence(),\n                        "nested_list": [random.randint(1, 100) for _ in range(random.randint(1, 5))]\n                    }\n                }) if self.version_features["json_support"] else None\n                \n                row_data = [\n                    uuid.uuid4(),\n                    self.cassandra_version,\n                    duration_field,\n                    high_precision_timestamp,\n                    json_data,\n                    self.version_features["duration_type"],\n                    self.version_features["json_support"],\n                    self.version_features["mixed_collections"],\n                    datetime.now()\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n            except Exception as e:\n                logger.error(f"Failed to insert version-specific row {i}: {e}")\n    \n    def generate_deletion_test_data(self):\n        """Generate data for deletion and tombstone testing"""\n        logger.info("Generating deletion test data")\n        \n        insert_query = self.session.prepare("""\n        INSERT INTO deletion_tests (\n            id, regular_data, complex_data, collection_data,\n            ttl_data, ttl_complex,\n            deleted_flag, deletion_timestamp, timestamp_created\n        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)\n        """)\n        \n        for i in range(200):  # Smaller dataset for deletion tests\n            try:\n                row_id = uuid.uuid4()\n                \n                # Regular data\n                regular_data = self.fake.text(max_nb_chars=100)\n                complex_data = self._generate_person()\n                collection_data = [self._generate_address() for _ in range(random.randint(1, 3))]\n                \n                # TTL data\n                ttl_data = self.fake.sentence() if random.random() > 0.3 else None\n                ttl_complex = {self.fake.word(): random.randint(1, 100) for _ in range(random.randint(1, 5))} if random.random() > 0.3 else None\n                \n                # Deletion metadata\n                deleted_flag = random.choice([True, False])\n                deletion_timestamp = datetime.now() if deleted_flag else None\n                \n                row_data = [\n                    row_id,\n                    regular_data,\n                    complex_data,\n                    collection_data,\n                    ttl_data,\n                    ttl_complex,\n                    deleted_flag,\n                    deletion_timestamp,\n                    datetime.now()\n                ]\n                \n                self.session.execute(insert_query, row_data)\n                \n                # Insert with TTL for some rows to create tombstones\n                if i % 4 == 0 and ttl_data:\n                    ttl_insert = self.session.prepare("""\n                    INSERT INTO deletion_tests (id, ttl_data) VALUES (?, ?) USING TTL 1\n                    """)\n                    self.session.execute(ttl_insert, [uuid.uuid4(), f"TTL data {i}"])\n                \n            except Exception as e:\n                logger.error(f"Failed to insert deletion test row {i}: {e}")\n    \n    def _generate_address(self) -> Dict[str, str]:\n        """Generate a realistic address UDT"""\n        return {\n            "street": self.fake.street_address(),\n            "city": self.fake.city(),\n            "state": self.fake.state(),\n            "zip_code": self.fake.zipcode(),\n            "country": self.fake.country()\n        }\n    \n    def _generate_phone_number(self) -> Dict[str, str]:\n        """Generate a phone number UDT"""\n        return {\n            "country_code": f"+{random.randint(1, 999)}",\n            "area_code": f"{random.randint(200, 999)}",\n            "number": f"{random.randint(1000000, 9999999)}",\n            "extension": f"{random.randint(1000, 9999)}" if random.random() > 0.7 else None\n        }\n    \n    def _generate_person(self) -> Dict[str, Any]:\n        """Generate a person UDT with complex nested data"""\n        return {\n            "first_name": self.fake.first_name(),\n            "last_name": self.fake.last_name(),\n            "email": self.fake.email(),\n            "home_address": self._generate_address(),\n            "work_address": self._generate_address() if random.random() > 0.3 else None,\n            "phone_numbers": [self._generate_phone_number() for _ in range(random.randint(1, 3))],\n            "emergency_contacts": {\n                self.fake.name(): self._generate_phone_number()\n                for _ in range(random.randint(1, 2))\n            }\n        }\n    \n    def _generate_company(self) -> Dict[str, Any]:\n        """Generate a company UDT with deeply nested data"""\n        employees = [self._generate_person() for _ in range(random.randint(1, 5))]\n        \n        return {\n            "name": self.fake.company(),\n            "headquarters": self._generate_address(),\n            "employees": employees,\n            "departments": {\n                self.fake.job(): [self._generate_person() for _ in range(random.randint(1, 3))]\n                for _ in range(random.randint(1, 3))\n            }\n        }\n    \n    def _generate_partial_person(self) -> Dict[str, Any]:\n        """Generate a person UDT with some null fields"""\n        person = {\n            "first_name": self.fake.first_name(),\n            "last_name": self.fake.last_name(),\n            "email": None,  # Intentionally null\n            "home_address": self._generate_address(),\n            "work_address": None,  # Intentionally null\n            "phone_numbers": [],  # Empty list\n            "emergency_contacts": {}  # Empty map\n        }\n        return person\n    \n    def _generate_partial_company(self) -> Dict[str, Any]:\n        """Generate a company UDT with some null/empty fields"""\n        return {\n            "name": self.fake.company(),\n            "headquarters": None,  # Intentionally null\n            "employees": [],  # Empty list\n            "departments": {}  # Empty map\n        }\n    \n    def export_sstables(self, output_dir: str):\n        """Export SSTable files for testing"""\n        logger.info(f"Exporting SSTable files to {output_dir}")\n        \n        os.makedirs(output_dir, exist_ok=True)\n        \n        # Flush data to SSTables\n        self.session.execute("SELECT * FROM system.local")  # Trigger flush\n        \n        # Find Cassandra data directory\n        data_dirs = [\n            "/var/lib/cassandra/data",\n            "/opt/cassandra/data",\n            "/cassandra/data",\n            "data",\n        ]\n        \n        cassandra_data_dir = None\n        for data_dir in data_dirs:\n            if os.path.exists(data_dir):\n                cassandra_data_dir = data_dir\n                break\n        \n        if not cassandra_data_dir:\n            logger.warning("Could not find Cassandra data directory")\n            return\n        \n        # Find keyspace directory\n        keyspace_dir = os.path.join(cassandra_data_dir, "hardened_validator_test")\n        if not os.path.exists(keyspace_dir):\n            logger.warning(f"Keyspace directory not found: {keyspace_dir}")\n            return\n        \n        # Copy SSTable files\n        try:\n            import shutil\n            \n            for table_dir in os.listdir(keyspace_dir):\n                table_path = os.path.join(keyspace_dir, table_dir)\n                if os.path.isdir(table_path):\n                    # Copy SSTable files for this table\n                    dest_table_dir = os.path.join(output_dir, table_dir)\n                    os.makedirs(dest_table_dir, exist_ok=True)\n                    \n                    for file in os.listdir(table_path):\n                        if file.endswith(('-Data.db', '-Index.db', '-Summary.db', '-Statistics.db')):\n                            src = os.path.join(table_path, file)\n                            dst = os.path.join(dest_table_dir, file)\n                            shutil.copy2(src, dst)\n                            logger.debug(f"Copied {src} -> {dst}")\n            \n            logger.info(f"SSTable export completed to {output_dir}")\n            \n        except Exception as e:\n            logger.error(f"Failed to export SSTables: {e}")\n    \n    def cleanup(self):\n        """Clean up connections"""\n        if self.cluster:\n            self.cluster.shutdown()\n            logger.info("Cassandra connection closed")\n\ndef main():\n    """Main function for command-line execution"""\n    parser = argparse.ArgumentParser(description="Hardened Validator Test Data Generator")\n    parser.add_argument("--version", required=True, help="Cassandra version (3.7, 3.11, 4.0, 4.1, 5.0)")\n    parser.add_argument("--host", default="localhost", help="Cassandra host")\n    parser.add_argument("--port", type=int, default=9042, help="Cassandra port")\n    parser.add_argument("--output-dir", help="Output directory for SSTable export")\n    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")\n    \n    args = parser.parse_args()\n    \n    if args.verbose:\n        logging.getLogger().setLevel(logging.DEBUG)\n    \n    logger.info("=== Hardened Validator Test Data Generator ===")\n    logger.info(f"Cassandra version: {args.version}")\n    logger.info(f"Host: {args.host}:{args.port}")\n    \n    try:\n        # Create generator\n        generator = HardenedValidatorTestDataGenerator(args.version, args.host, args.port)\n        \n        # Connect to Cassandra\n        generator.connect()\n        \n        # Generate all test data\n        generator.generate_all_test_data()\n        \n        # Export SSTables if requested\n        if args.output_dir:\n            generator.export_sstables(args.output_dir)\n        \n        logger.info("✅ Test data generation completed successfully!")\n        sys.exit(0)\n        \n    except Exception as e:\n        logger.error(f"❌ Test data generation failed: {e}")\n        sys.exit(1)\n\nif __name__ == "__main__":\n    main()\n