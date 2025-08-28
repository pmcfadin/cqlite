#!/usr/bin/env python3

"""
Comprehensive Test Data Generator for CQLite - Issue #17

This script generates comprehensive test data across multiple Cassandra versions
to ensure CQLite compatibility and reliability.

CRITICAL SUCCESS FACTOR: Command-line test execution MUST work reliably!
"""

import argparse
import json
import logging
import os
import random
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
import numpy as np
import pandas as pd
from tqdm import tqdm
from cassandra.util import Duration
from decimal import Decimal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/generated/data_generation.log')
    ]
)
logger = logging.getLogger(__name__)

# Data generation configuration
class DataGenerationConfig:
    """Configuration for test data generation"""
    
    def __init__(self, scale: str = "COMPREHENSIVE"):
        self.scale = scale.upper()
        self.scale_multipliers = {
            "SMALL": 1,
            "MEDIUM": 5,
            "COMPREHENSIVE": 10,
            "LARGE": 50
        }
        
        base_multiplier = self.scale_multipliers.get(scale, 10)
        
        # Row counts per table type
        self.basic_table_rows = 1000 * base_multiplier
        self.collection_table_rows = 500 * base_multiplier
        self.time_series_rows = 2000 * base_multiplier
        self.wide_table_rows = 100 * base_multiplier
        
        # Data characteristics
        self.max_string_length = min(1000, 100 * base_multiplier)
        self.max_collection_size = min(100, 10 * base_multiplier)
        self.max_blob_size = min(1024 * 1024, 64 * 1024 * base_multiplier)  # Up to 1MB
        
        # Performance settings
        self.batch_size = min(100, 10 * base_multiplier)
        self.parallel_threads = min(8, 2 * base_multiplier)
        
        logger.info(f"Data generation configured for {scale} scale")
        logger.info(f"Basic tables: {self.basic_table_rows} rows")
        logger.info(f"Collection tables: {self.collection_table_rows} rows")
        logger.info(f"Time series tables: {self.time_series_rows} rows")

class CassandraTestDataGenerator:
    """Main class for generating comprehensive test data"""
    
    def __init__(self, host: str, port: int, version: str, config: DataGenerationConfig):
        self.host = host
        self.port = port
        self.version = version
        self.config = config
        self.fake = Faker()
        self.fake.seed_instance(42)  # Reproducible data
        
        # Connect to Cassandra
        self.cluster = None
        self.session = None
        self.connect()
        
        # Track generation statistics
        self.stats = {
            'tables_created': 0,
            'rows_inserted': 0,
            'start_time': datetime.now(),
            'errors': []
        }
    
    def connect(self):
        """Connect to Cassandra cluster"""
        logger.info(f"Connecting to Cassandra {self.version} at {self.host}:{self.port}")
        
        try:
            # Wait for Cassandra to be ready
            self.wait_for_cassandra()
            
            # Create cluster connection
            self.cluster = Cluster([self.host], port=self.port)
            self.session = self.cluster.connect()
            
            # Set consistency level
            from cassandra import ConsistencyLevel
            self.session.default_consistency_level = ConsistencyLevel.ONE
            
            logger.info("Successfully connected to Cassandra")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
    
    def wait_for_cassandra(self, max_attempts: int = 30, delay: int = 10):
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
    
    def create_keyspace(self, keyspace_name: str = "cqlite_test"):
        """Create test keyspace"""
        logger.info(f"Creating keyspace: {keyspace_name}")
        
        create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH REPLICATION = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
        """
        
        self.session.execute(create_keyspace_query)
        self.session.set_keyspace(keyspace_name)
        logger.info(f"Keyspace {keyspace_name} created and selected")
    
    def create_basic_types_tables(self):
        """Create tables for testing basic CQL data types"""
        logger.info("Creating basic types tables...")
        
        # Simple table with all basic types
        simple_table_query = """
        CREATE TABLE IF NOT EXISTS simple_table (
            id UUID PRIMARY KEY,
            text_col TEXT,
            int_col INT,
            bigint_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE,
            boolean_col BOOLEAN,
            timestamp_col TIMESTAMP,
            date_col DATE,
            time_col TIME,
            blob_col BLOB,
            inet_col INET,
            uuid_col UUID,
            timeuuid_col TIMEUUID,
            decimal_col DECIMAL,
            varint_col VARINT
        )
        """
        
        # Composite key table
        composite_key_query = """
        CREATE TABLE IF NOT EXISTS composite_key_table (
            partition_key1 TEXT,
            partition_key2 INT,
            clustering_key1 TIMESTAMP,
            clustering_key2 TEXT,
            value_col TEXT,
            PRIMARY KEY ((partition_key1, partition_key2), clustering_key1, clustering_key2)
        ) WITH CLUSTERING ORDER BY (clustering_key1 DESC, clustering_key2 ASC)
        """
        
        # Table with static columns
        static_columns_query = """
        CREATE TABLE IF NOT EXISTS static_columns_table (
            partition_key TEXT,
            clustering_key INT,
            static_col TEXT STATIC,
            regular_col TEXT,
            PRIMARY KEY (partition_key, clustering_key)
        )
        """
        
        # Counter table
        counter_query = """
        CREATE TABLE IF NOT EXISTS counter_table (
            id UUID PRIMARY KEY,
            counter_col COUNTER
        )
        """
        
        # Compression test tables
        compression_queries = [
            """
            CREATE TABLE IF NOT EXISTS lz4_compressed_table (
                id UUID PRIMARY KEY,
                data TEXT
            ) WITH COMPRESSION = {'class': 'LZ4Compressor'}
            """,
            """
            CREATE TABLE IF NOT EXISTS snappy_compressed_table (
                id UUID PRIMARY KEY,
                data TEXT
            ) WITH COMPRESSION = {'class': 'SnappyCompressor'}
            """,
            """
            CREATE TABLE IF NOT EXISTS uncompressed_table (
                id UUID PRIMARY KEY,
                data TEXT
            ) WITH COMPRESSION = {'enabled': false}
            """
        ]
        
        # Execute all table creation queries
        tables = [
            simple_table_query,
            composite_key_query,
            static_columns_query,
            counter_query
        ] + compression_queries
        
        for query in tables:
            try:
                self.session.execute(query)
                self.stats['tables_created'] += 1
            except Exception as e:
                logger.error(f"Failed to create table: {e}")
                self.stats['errors'].append(str(e))
    
    def create_collection_tables(self):
        """Create tables for testing collection types"""
        logger.info("Creating collection types tables...")
        
        # Basic collections table
        collections_query = """
        CREATE TABLE IF NOT EXISTS collections_table (
            id UUID PRIMARY KEY,
            list_col LIST<TEXT>,
            set_col SET<INT>,
            map_col MAP<TEXT, INT>,
            frozen_list_col FROZEN<LIST<TEXT>>,
            frozen_set_col FROZEN<SET<INT>>,
            frozen_map_col FROZEN<MAP<TEXT, INT>>
        )
        """
        
        # Nested collections table
        nested_collections_query = """
        CREATE TABLE IF NOT EXISTS nested_collections_table (
            id UUID PRIMARY KEY,
            list_of_lists LIST<FROZEN<LIST<TEXT>>>,
            map_of_lists MAP<TEXT, FROZEN<LIST<INT>>>,
            set_of_maps SET<FROZEN<MAP<TEXT, INT>>>
        )
        """
        
        # Large collections performance test
        large_collections_query = """
        CREATE TABLE IF NOT EXISTS large_collections_table (
            id UUID PRIMARY KEY,
            large_list LIST<TEXT>,
            large_set SET<UUID>,
            large_map MAP<INT, TEXT>
        )
        """
        
        queries = [collections_query, nested_collections_query, large_collections_query]
        
        for query in queries:
            try:
                self.session.execute(query)
                self.stats['tables_created'] += 1
            except Exception as e:
                logger.error(f"Failed to create collection table: {e}")
                self.stats['errors'].append(str(e))
    
    def create_time_series_tables(self):
        """Create tables for time series data testing"""
        logger.info("Creating time series tables...")
        
        # Sensor data table
        sensor_data_query = """
        CREATE TABLE IF NOT EXISTS sensor_data (
            sensor_id UUID,
            timestamp TIMESTAMP,
            temperature FLOAT,
            humidity FLOAT,
            pressure DOUBLE,
            location TEXT,
            PRIMARY KEY (sensor_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """
        
        # Application metrics table with TTL
        app_metrics_query = """
        CREATE TABLE IF NOT EXISTS app_metrics (
            app_name TEXT,
            metric_timestamp TIMESTAMP,
            metric_name TEXT,
            metric_value DOUBLE,
            tags MAP<TEXT, TEXT>,
            PRIMARY KEY (app_name, metric_timestamp, metric_name)
        ) WITH CLUSTERING ORDER BY (metric_timestamp DESC)
          AND default_time_to_live = 604800
        """
        
        # User activity table
        user_activity_query = """
        CREATE TABLE IF NOT EXISTS user_activity (
            user_id UUID,
            activity_date DATE,
            activity_timestamp TIMESTAMP,
            activity_type TEXT,
            details TEXT,
            PRIMARY KEY (user_id, activity_date, activity_timestamp)
        ) WITH CLUSTERING ORDER BY (activity_date DESC, activity_timestamp DESC)
        """
        
        queries = [sensor_data_query, app_metrics_query, user_activity_query]
        
        for query in queries:
            try:
                self.session.execute(query)
                self.stats['tables_created'] += 1
            except Exception as e:
                logger.error(f"Failed to create time series table: {e}")
                self.stats['errors'].append(str(e))
    
    def create_wide_tables(self):
        """Create tables with wide rows for testing"""
        logger.info("Creating wide tables...")
        
        # Wide partition table
        wide_partition_query = """
        CREATE TABLE IF NOT EXISTS wide_partition_table (
            partition_key TEXT,
            clustering_key TIMEUUID,
            data_col TEXT,
            PRIMARY KEY (partition_key, clustering_key)
        )
        """
        
        # Many columns table
        many_columns_fields = ", ".join([f"col_{i} TEXT" for i in range(100)])
        many_columns_query = f"""
        CREATE TABLE IF NOT EXISTS many_columns_table (
            id UUID PRIMARY KEY,
            {many_columns_fields}
        )
        """
        
        # Large blob table
        large_blob_query = """
        CREATE TABLE IF NOT EXISTS large_blob_table (
            id UUID PRIMARY KEY,
            small_blob BLOB,
            medium_blob BLOB,
            large_blob BLOB,
            metadata TEXT
        )
        """
        
        queries = [wide_partition_query, many_columns_query, large_blob_query]
        
        for query in queries:
            try:
                self.session.execute(query)
                self.stats['tables_created'] += 1
            except Exception as e:
                logger.error(f"Failed to create wide table: {e}")
                self.stats['errors'].append(str(e))
    
    def populate_simple_table(self):
        """Populate simple table with test data"""
        logger.info("Populating simple_table...")
        
        insert_query = self.session.prepare("""
        INSERT INTO test_basic.simple_table (
            id, name, age, salary, height, weight, active, created, birth_date,
            work_time, description, account_balance, session_id, ip_address,
            small_number, medium_number, duration_val, varchar_field, ascii_field
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        batch_size = self.config.batch_size
        total_rows = self.config.basic_table_rows
        
        with tqdm(total=total_rows, desc="test_basic.simple_table") as pbar:
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                
                for i in range(batch_start, batch_end):
                    try:
                        row_data = [
                            uuid.uuid4(),
                            self.fake.name(),
                            random.randint(18, 80),
                            random.randint(30000, 200000),
                            round(random.uniform(1.5, 2.0), 2),
                            round(random.uniform(50.0, 120.0), 2),
                            random.choice([True, False]),
                            datetime.now(),
                            (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
                            datetime.now().time(),
                            bytes(random.getrandbits(8) for _ in range(random.randint(10, 1000))),
                            str(round(random.uniform(0, 100000), 2)),
                            uuid.uuid1(),
                            self.fake.ipv4(),
                            random.randint(0, 127),
                            random.randint(0, 32767),
                            Duration(0, 0, (random.randint(0,23)*3600 + random.randint(0,59)*60 + random.randint(0,59)) * 1_000_000_000),
                            self.fake.word(),
                            "ascii"
                        ]
                        
                        self.session.execute(insert_query, row_data)
                        self.stats['rows_inserted'] += 1
                        pbar.update(1)
                        
                    except Exception as e:
                        logger.error(f"Failed to insert row {i}: {e}")
                        self.stats['errors'].append(f"simple_table row {i}: {str(e)}")
    
    def populate_collections_table(self):
        """Populate collections table with test data"""
        logger.info("Populating test_collections.collection_table...")
        print("[generate] test_collections.collection_table")
        
        insert_query = self.session.prepare("""
        INSERT INTO test_collections.collection_table (
            id, tags, scores, properties, numbers_set, ordered_values, metadata_map
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)
        
        total_rows = self.config.collection_table_rows
        
        with tqdm(total=total_rows, desc="collections_table") as pbar:
            for i in range(total_rows):
                try:
                    tags = set(self.fake.word() for _ in range(random.randint(1, self.config.max_collection_size)))
                    scores = [random.randint(1, 100) for _ in range(random.randint(1, self.config.max_collection_size))]
                    properties = {self.fake.word(): self.fake.word() for _ in range(random.randint(1, self.config.max_collection_size))}
                    numbers_set = set(random.randint(1, 1000) for _ in range(random.randint(1, self.config.max_collection_size)))
                    ordered_values = [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(random.randint(1, 5))]
                    metadata_map = {self.fake.word(): random.randint(1, 1_000_000) for _ in range(random.randint(1, 5))}
                    
                    row_data = [
                        uuid.uuid4(),
                        tags,
                        scores,
                        properties,
                        numbers_set,
                        ordered_values,
                        metadata_map
                    ]
                    
                    self.session.execute(insert_query, row_data)
                    self.stats['rows_inserted'] += 1
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to insert collections row {i}: {e}")
                    self.stats['errors'].append(f"collections_table row {i}: {str(e)}")
    
    def populate_time_series_tables(self):
        """Populate time series tables with test data"""
        logger.info("Populating test_timeseries.sensor_data...")
        print("[generate] test_timeseries.sensor_data")
        
        # Populate sensor_data
        sensor_insert = self.session.prepare("""
        INSERT INTO test_timeseries.sensor_data (sensor_id, timestamp, temperature, humidity, pressure, battery_level, location, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        sensor_ids = [uuid.uuid4() for _ in range(10)]  # 10 sensors
        base_time = datetime.now() - timedelta(days=30)
        
        with tqdm(total=self.config.time_series_rows, desc="sensor_data") as pbar:
            for i in range(self.config.time_series_rows):
                try:
                    sensor_id = random.choice(sensor_ids)
                    timestamp = base_time + timedelta(seconds=random.randint(0, 30*24*3600))
                    
                    row_data = [
                        sensor_id,
                        timestamp,
                        random.uniform(-20.0, 50.0),  # temperature
                        random.uniform(0.0, 100.0),   # humidity
                        random.uniform(980.0, 1020.0), # pressure
                        random.randint(0, 100),        # battery_level
                        self.fake.city(),              # location
                        random.choice(['active','inactive','maintenance','error'])
                    ]
                    
                    self.session.execute(sensor_insert, row_data)
                    self.stats['rows_inserted'] += 1
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to insert sensor data row {i}: {e}")
                    self.stats['errors'].append(f"sensor_data row {i}: {str(e)}")
    
    def populate_wide_tables(self):
        """Populate wide tables with test data"""
        logger.info("Populating test_wide_rows.wide_partition_table...")
        print("[generate] test_wide_rows.wide_partition_table")
        
        # Populate wide_partition_table
        wide_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.wide_partition_table (
            partition_key, clustering_col1, clustering_col2, clustering_col3, clustering_col4, clustering_col5,
            data_column, value_column, blob_column, json_column
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        partitions = [f"partition_{i}" for i in range(10)]
        
        with tqdm(total=self.config.wide_table_rows, desc="wide_partition_table") as pbar:
            for i in range(self.config.wide_table_rows):
                try:
                    partition_key = uuid.uuid4()
                    clustering_col1 = datetime.utcnow() - timedelta(days=random.randint(0, 365))
                    clustering_col2 = self.fake.word()
                    clustering_col3 = int(random.randint(1, 1_000_000))
                    clustering_col4 = uuid.uuid4()
                    clustering_col5 = (datetime.utcnow() - timedelta(days=random.randint(0, 365))).date()
                    data_column = self.fake.text(max_nb_chars=500)
                    value_column = int(random.randint(1, 10_000_000))
                    blob_column = bytes(random.getrandbits(8) for _ in range(random.randint(10, 200)))
                    json_column = '{"k":"v"}'
                    
                    self.session.execute(wide_insert, [
                        partition_key, clustering_col1, clustering_col2, clustering_col3, clustering_col4, clustering_col5,
                        data_column, value_column, blob_column, json_column
                    ])
                    self.stats['rows_inserted'] += 1
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to insert wide partition row {i}: {e}")
                    logger.error("type debug: %s", [
                        type(partition_key).__name__, type(clustering_col1).__name__, type(clustering_col2).__name__,
                        type(clustering_col3).__name__, type(clustering_col4).__name__, type(clustering_col5).__name__,
                        type(data_column).__name__, type(value_column).__name__, type(blob_column).__name__, type(json_column).__name__
                    ])
                    self.stats['errors'].append(f"wide_partition_table row {i}: {str(e)}")
    
    def populate_basic_additional_tables(self):
        """Populate additional basic schema tables"""
        logger.info("Populating test_basic additional tables...")
        print("[generate] test_basic.{composite_key_table,multi_partition_table,compression_test_table,uncompressed_table,ttl_test_table,static_columns_table}")

        composite_insert = self.session.prepare("""
        INSERT INTO test_basic.composite_key_table (
            partition_key, clustering_key1, clustering_key2, data, value
        ) VALUES (?, ?, ?, ?, ?)
        """)

        multi_partition_insert = self.session.prepare("""
        INSERT INTO test_basic.multi_partition_table (
            tenant_id, user_id, category, item_id, name, value, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        compression_test_insert = self.session.prepare("""
        INSERT INTO test_basic.compression_test_table (
            id, large_text, repeated_data, random_data, compressed_json
        ) VALUES (?, ?, ?, ?, ?)
        """)

        uncompressed_insert = self.session.prepare("""
        INSERT INTO test_basic.uncompressed_table (
            id, data, value, timestamp_val
        ) VALUES (?, ?, ?, ?)
        """)

        ttl_test_insert = self.session.prepare("""
        INSERT INTO test_basic.ttl_test_table (
            id, temporary_data, expiring_value, session_info
        ) VALUES (?, ?, ?, ?)
        """)

        static_columns_insert = self.session.prepare("""
        INSERT INTO test_basic.static_columns_table (
            partition_key, clustering_key, static_data, row_data, row_value
        ) VALUES (?, ?, ?, ?, ?)
        """)

        rows = min(100, self.config.basic_table_rows // 10)
        with tqdm(total=rows * 6, desc="basic_additional") as pbar:
            for i in range(rows):
                try:
                    self.session.execute(composite_insert, [
                        uuid.uuid4(), datetime.utcnow(), self.fake.word(), self.fake.sentence(), random.randint(0, 1000)
                    ])
                except Exception as e:
                    logger.error(f"Failed composite_key_table row {i}: {e}")
                    self.stats['errors'].append(f"composite_key_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(multi_partition_insert, [
                        uuid.uuid4(), uuid.uuid4(), random.choice(["A","B","C"]), uuid.uuid1(),
                        self.fake.word(), random.randint(0, 1_000_000), self.fake.sentence()
                    ])
                except Exception as e:
                    logger.error(f"Failed multi_partition_table row {i}: {e}")
                    self.stats['errors'].append(f"multi_partition_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(compression_test_insert, [
                        uuid.uuid4(), self.fake.text(max_nb_chars=2000), self.fake.text(max_nb_chars=200),
                        bytes(random.getrandbits(8) for _ in range(random.randint(10, 1000))), json.dumps({"k":"v"})
                    ])
                except Exception as e:
                    logger.error(f"Failed compression_test_table row {i}: {e}")
                    self.stats['errors'].append(f"compression_test_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(uncompressed_insert, [
                        uuid.uuid4(), self.fake.text(max_nb_chars=200), random.randint(0, 1000), datetime.utcnow()
                    ])
                except Exception as e:
                    logger.error(f"Failed uncompressed_table row {i}: {e}")
                    self.stats['errors'].append(f"uncompressed_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(ttl_test_insert, [
                        uuid.uuid4(), self.fake.sentence(), random.randint(0, 1000), self.fake.word()
                    ])
                except Exception as e:
                    logger.error(f"Failed ttl_test_table row {i}: {e}")
                    self.stats['errors'].append(f"ttl_test_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(static_columns_insert, [
                        uuid.uuid4(), datetime.utcnow(), self.fake.word(), self.fake.sentence(), random.randint(0, 1000)
                    ])
                except Exception as e:
                    logger.error(f"Failed static_columns_table row {i}: {e}")
                    self.stats['errors'].append(f"static_columns_table row {i}: {e}")
                pbar.update(1)

                self.stats['rows_inserted'] += 6

    def populate_collections_additional_tables(self):
        """Populate additional collections schema tables"""
        logger.info("Populating test_collections additional tables...")
        print("[generate] test_collections.{nested_collections_table,collections_with_udts,frozen_collections_table,typed_collections_table,empty_collections_table,large_collections_table,collection_clustering_table}")

        nested_insert = self.session.prepare("""
        INSERT INTO test_collections.nested_collections_table (
            id, tags_by_category, scores_by_game, user_preferences, time_series_data
        ) VALUES (?, ?, ?, ?, ?)
        """)

        # Use NamedTuple UDT representations to allow usage in sets
        from typing import NamedTuple
        class AddressType(NamedTuple):
            street: str
            city: str
            state: str
            zip_code: str
            country: str
        class ContactInfo(NamedTuple):
            email: str
            phone: str
            address: AddressType

        udt_insert = self.session.prepare("""
        INSERT INTO test_collections.collections_with_udts (
            user_id, addresses, contacts, locations_visited, emergency_contacts
        ) VALUES (?, ?, ?, ?, ?)
        """)

        frozen_insert = self.session.prepare("""
        INSERT INTO test_collections.frozen_collections_table (
            id, frozen_tags, frozen_scores, frozen_properties, regular_tags
        ) VALUES (?, ?, ?, ?, ?)
        """)

        typed_insert = self.session.prepare("""
        INSERT INTO test_collections.typed_collections_table (
            id, uuid_set, timestamp_list, boolean_map, decimal_set, blob_list, inet_map
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        empty_insert = self.session.prepare("""
        INSERT INTO test_collections.empty_collections_table (
            id, empty_set, null_list, sparse_map, optional_tags
        ) VALUES (?, ?, ?, ?, ?)
        """)

        large_collections_insert = self.session.prepare("""
        INSERT INTO test_collections.large_collections_table (
            partition_key, clustering_key, huge_set, massive_list, giant_map
        ) VALUES (?, ?, ?, ?, ?)
        """)

        collection_clustering_insert = self.session.prepare("""
        INSERT INTO test_collections.collection_clustering_table (
            partition_key, clustering_key, data, value
        ) VALUES (?, ?, ?, ?)
        """)

        rows = min(50, self.config.collection_table_rows // 10)
        with tqdm(total=rows * 7, desc="collections_additional") as pbar:
            for i in range(rows):
                try:
                    tags_by_category = {self.fake.word(): set(self.fake.words(nb=random.randint(1, 3))) for _ in range(3)}
                    scores_by_game = {self.fake.word(): [random.randint(0, 100) for _ in range(3)] for _ in range(3)}
                    user_preferences = {self.fake.word(): {"opt": self.fake.word()} for _ in range(2)}
                    time_series_data = {(datetime.utcnow() - timedelta(days=d)).date(): [datetime.utcnow() - timedelta(hours=h) for h in range(3)] for d in range(2)}
                    self.session.execute(nested_insert, [uuid.uuid4(), tags_by_category, scores_by_game, user_preferences, time_series_data])
                except Exception as e:
                    logger.error(f"Failed nested_collections_table row {i}: {e}")
                    self.stats['errors'].append(f"nested_collections_table row {i}: {e}")
                pbar.update(1)

                try:
                    def make_address() -> AddressType:
                        return AddressType(
                            street=self.fake.street_address(),
                            city=self.fake.city(),
                            state=self.fake.state_abbr(),
                            zip_code=self.fake.postcode(),
                            country=self.fake.country(),
                        )
                    def make_contact() -> ContactInfo:
                        return ContactInfo(
                            email=self.fake.email(),
                            phone=self.fake.phone_number(),
                            address=make_address(),
                        )
                    addresses = [make_address() for _ in range(2)]  # LIST<FROZEN<address_type>>
                    contacts = {make_contact() for _ in range(2)}   # SET<FROZEN<contact_info>> requires hashable elements
                    locations_visited = {(datetime.utcnow() - timedelta(days=1)).date(): make_address()}
                    emergency_contacts = {self.fake.first_name(): make_contact()}
                    self.session.execute(udt_insert, [
                        uuid.uuid4(), addresses, contacts, locations_visited, emergency_contacts
                    ])
                except Exception as e:
                    logger.error(f"Failed collections_with_udts row {i}: {e}")
                    self.stats['errors'].append(f"collections_with_udts row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(frozen_insert, [
                        uuid.uuid4(), set(self.fake.words(nb=3)), [random.randint(0, 100) for _ in range(3)], {"k": "v"}, set(self.fake.words(nb=2))
                    ])
                except Exception as e:
                    logger.error(f"Failed frozen_collections_table row {i}: {e}")
                    self.stats['errors'].append(f"frozen_collections_table row {i}: {e}")
                pbar.update(1)

                try:
                    decimal_vals = {Decimal(str(round(random.random() * 100, 2))) for _ in range(2)}
                    blob_list = [bytes(random.getrandbits(8) for _ in range(32)) for _ in range(2)]
                    inet_map = {"home": self.fake.ipv4(), "office": self.fake.ipv4()}
                    self.session.execute(typed_insert, [
                        uuid.uuid4(), {uuid.uuid4() for _ in range(2)}, [datetime.utcnow() for _ in range(2)],
                        {"flag": random.choice([True, False])}, decimal_vals, blob_list, inet_map
                    ])
                except Exception as e:
                    logger.error(f"Failed typed_collections_table row {i}: {e}")
                    self.stats['errors'].append(f"typed_collections_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(empty_insert, [
                        uuid.uuid4(), set(), [], {"maybe": None if random.choice([True, False]) else ""}, set()
                    ])
                except Exception as e:
                    logger.error(f"Failed empty_collections_table row {i}: {e}")
                    self.stats['errors'].append(f"empty_collections_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(large_collections_insert, [
                        uuid.uuid4(), random.randint(0, 9999), set(self.fake.words(nb=5)), [uuid.uuid4() for _ in range(3)],
                        {self.fake.word(): bytes(random.getrandbits(8) for _ in range(16)) for _ in range(3)}
                    ])
                except Exception as e:
                    logger.error(f"Failed large_collections_table row {i}: {e}")
                    self.stats['errors'].append(f"large_collections_table row {i}: {e}")
                pbar.update(1)

                try:
                    clustering = [self.fake.word() for _ in range(3)]
                    self.session.execute(collection_clustering_insert, [
                        uuid.uuid4(), clustering, self.fake.sentence(), random.randint(0, 1000)
                    ])
                except Exception as e:
                    logger.error(f"Failed collection_clustering_table row {i}: {e}")
                    self.stats['errors'].append(f"collection_clustering_table row {i}: {e}")
                pbar.update(1)

                self.stats['rows_inserted'] += 7

    def populate_time_series_additional_tables(self):
        """Populate additional timeseries schema tables"""
        logger.info("Populating test_timeseries additional tables...")
        print("[generate] test_timeseries.{app_metrics,user_activity,stock_prices,log_entries,event_store,time_bucketed_counters,user_sessions,tick_data}")

        app_metrics_insert = self.session.prepare("""
        INSERT INTO test_timeseries.app_metrics (
            application_id, metric_name, timestamp, value, unit, tags
        ) VALUES (?, ?, ?, ?, ?, ?)
        """)

        user_activity_insert = self.session.prepare("""
        INSERT INTO test_timeseries.user_activity (
            user_id, activity_date, activity_time, activity_type, page_url, session_id, duration_ms, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        stock_prices_insert = self.session.prepare("""
        INSERT INTO test_timeseries.stock_prices (
            symbol, trading_day, timestamp, open_price, high_price, low_price, close_price, volume, adjusted_close
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        log_entries_insert = self.session.prepare("""
        INSERT INTO test_timeseries.log_entries (
            service_name, log_level, hour_bucket, log_id, message, source_file, line_number, thread_name, correlation_id, stack_trace
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        event_store_insert = self.session.prepare("""
        INSERT INTO test_timeseries.event_store (
            aggregate_id, version, event_id, event_type, event_data, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        # Counters use UPDATE with increments
        time_bucketed_counters_update = self.session.prepare("""
        UPDATE test_timeseries.time_bucketed_counters SET total_count = total_count + ?, error_count = error_count + ?, success_count = success_count + ? WHERE metric_name = ? AND time_bucket = ?
        """)

        user_sessions_insert = self.session.prepare("""
        INSERT INTO test_timeseries.user_sessions (
            session_id, user_id, start_time, last_activity, ip_address, user_agent, device_info, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        tick_data_insert = self.session.prepare("""
        INSERT INTO test_timeseries.tick_data (
            symbol, exchange, minute_bucket, tick_id, price, volume, bid_price, ask_price, trade_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        rows = min(200, self.config.time_series_rows // 10)
        with tqdm(total=rows * 8, desc="timeseries_additional") as pbar:
            base_time = datetime.utcnow() - timedelta(days=7)
            for i in range(rows):
                try:
                    self.session.execute(app_metrics_insert, [
                        self.fake.word(), self.fake.word(), base_time + timedelta(minutes=i),
                        random.random() * 100.0, "ms", {"env": random.choice(["dev","prod"]) }
                    ])
                except Exception as e:
                    logger.error(f"Failed app_metrics row {i}: {e}")
                    self.stats['errors'].append(f"app_metrics row {i}: {e}")
                pbar.update(1)

                try:
                    act_time = base_time + timedelta(minutes=i)
                    self.session.execute(user_activity_insert, [
                        uuid.uuid4(), act_time.date(), act_time, self.fake.word(),
                        f"https://{self.fake.domain_name()}/{self.fake.word()}", uuid.uuid4(), random.randint(0, 600000), {"ref": self.fake.word()}
                    ])
                except Exception as e:
                    logger.error(f"Failed user_activity row {i}: {e}")
                    self.stats['errors'].append(f"user_activity row {i}: {e}")
                pbar.update(1)

                try:
                    ts = base_time + timedelta(minutes=i)
                    self.session.execute(stock_prices_insert, [
                        random.choice(["AAPL","AMZN","GOOG"]), ts.date(), ts,
                        Decimal("100.00"), Decimal("101.00"), Decimal("99.50"), Decimal("100.50"), random.randint(1_000, 1_000_000), Decimal("100.45")
                    ])
                except Exception as e:
                    logger.error(f"Failed stock_prices row {i}: {e}")
                    self.stats['errors'].append(f"stock_prices row {i}: {e}")
                pbar.update(1)

                try:
                    hb = datetime(ts.year, ts.month, ts.day, ts.hour)
                    self.session.execute(log_entries_insert, [
                        self.fake.word(), random.choice(["INFO","WARN","ERROR"]), hb, uuid.uuid1(), self.fake.sentence(),
                        "app.py", random.randint(1, 1000), "main-thread", uuid.uuid4(), None
                    ])
                except Exception as e:
                    logger.error(f"Failed log_entries row {i}: {e}")
                    self.stats['errors'].append(f"log_entries row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(event_store_insert, [
                        uuid.uuid4(), i, uuid.uuid1(), "UserCreated", json.dumps({"i": i}), {"meta": "x"}, datetime.utcnow()
                    ])
                except Exception as e:
                    logger.error(f"Failed event_store row {i}: {e}")
                    self.stats['errors'].append(f"event_store row {i}: {e}")
                pbar.update(1)

                try:
                    tb = datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute // 5 * 5)
                    self.session.execute(time_bucketed_counters_update, [
                        1, 0 if i % 10 else 1, 1 if i % 10 else 0, "requests", tb
                    ])
                except Exception as e:
                    logger.error(f"Failed time_bucketed_counters row {i}: {e}")
                    self.stats['errors'].append(f"time_bucketed_counters row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(user_sessions_insert, [
                        uuid.uuid4(), uuid.uuid4(), ts, ts + timedelta(minutes=5), self.fake.ipv4(), self.fake.user_agent(), {"os": self.fake.word()}, True
                    ])
                except Exception as e:
                    logger.error(f"Failed user_sessions row {i}: {e}")
                    self.stats['errors'].append(f"user_sessions row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(tick_data_insert, [
                        random.choice(["AAPL","AMZN","GOOG"]), random.choice(["NASDAQ","NYSE"]), hb, uuid.uuid1(),
                        Decimal("100.10"), random.randint(1, 10000), Decimal("100.05"), Decimal("100.15"), random.choice(["BUY","SELL"])
                    ])
                except Exception as e:
                    logger.error(f"Failed tick_data row {i}: {e}")
                    self.stats['errors'].append(f"tick_data row {i}: {e}")
                pbar.update(1)

                self.stats['rows_inserted'] += 8

    def populate_wide_additional_tables(self):
        """Populate additional wide-rows schema tables"""
        logger.info("Populating test_wide_rows additional tables...")
        print("[generate] test_wide_rows.{large_blob_table,chat_messages,many_columns_table,product_catalog,document_versions,multi_metric_timeseries,sparse_data_table}")

        large_blob_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.large_blob_table (
            file_id, chunk_id, file_name, mime_type, chunk_data, chunk_size, total_chunks, checksum
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        chat_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.chat_messages (
            channel_id, message_timestamp, message_id, user_id, username, message_content, attachments,
            reactions, thread_id, reply_count, edited_at, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        many_columns_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.many_columns_table (
            id, col_001, col_011, col_021, col_031, col_041, col_046, col_051
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        product_catalog_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.product_catalog (
            category_id, product_id, product_name, description, price, currency, availability_count, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        document_versions_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.document_versions (
            document_id, version_number, created_at, author_id, title, content, tags, metadata, word_count, character_count, change_summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        multi_metric_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.multi_metric_timeseries (
            device_id, metric_timestamp, cpu_usage_percent, memory_usage_bytes, disk_io_read_bytes, disk_io_write_bytes, network_rx_bytes,
            network_tx_bytes, gpu_usage_percent, gpu_memory_bytes, temperature_celsius, fan_speed_rpm, power_consumption_watts, process_count,
            thread_count, handle_count, uptime_seconds, load_average_1min, load_average_5min, load_average_15min, disk_usage_percent,
            swap_usage_bytes, network_connections, active_sessions, error_count, warning_count, info_count, custom_metrics, status_flags, diagnostic_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        sparse_insert = self.session.prepare("""
        INSERT INTO test_wide_rows.sparse_data_table (
            entity_id, attribute_name, string_value, numeric_value, boolean_value, timestamp_value, json_value, blob_value, set_value, list_value, map_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        rows = min(50, self.config.wide_table_rows)
        with tqdm(total=rows * 6, desc="wide_additional") as pbar:
            for i in range(rows):
                try:
                    data = bytes(random.getrandbits(8) for _ in range(random.randint(256, 2048)))
                    self.session.execute(large_blob_insert, [
                        uuid.uuid4(), i, f"file_{i}.bin", "application/octet-stream", data, len(data), random.randint(1, 10), uuid.uuid4().hex
                    ])
                except Exception as e:
                    logger.error(f"Failed large_blob_table row {i}: {e}")
                    self.stats['errors'].append(f"large_blob_table row {i}: {e}")
                pbar.update(1)

                try:
                    reactions = {self.fake.word(): {uuid.uuid4() for _ in range(random.randint(0,3))} for _ in range(2)}
                    self.session.execute(chat_insert, [
                        uuid.uuid4(), datetime.utcnow(), uuid.uuid1(), uuid.uuid4(), self.fake.user_name(),
                        self.fake.sentence(), [self.fake.file_name() for _ in range(2)], reactions, uuid.uuid4(),
                        random.randint(0, 20), datetime.utcnow() if random.choice([True, False]) else None, {"lang": "en"}
                    ])
                except Exception as e:
                    logger.error(f"Failed chat_messages row {i}: {e}")
                    self.stats['errors'].append(f"chat_messages row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(many_columns_insert, [
                        uuid.uuid4(), self.fake.word(), random.randint(0, 1000), random.randint(0, 1_000_000),
                        random.random() * 100.0, datetime.utcnow(), uuid.uuid4(), bytes(random.getrandbits(8) for _ in range(8))
                    ])
                except Exception as e:
                    logger.error(f"Failed many_columns_table row {i}: {e}")
                    self.stats['errors'].append(f"many_columns_table row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(product_catalog_insert, [
                        uuid.uuid4(), uuid.uuid4(), self.fake.word(), self.fake.sentence(), Decimal("19.99"), "USD", random.randint(0, 1000), datetime.utcnow(), datetime.utcnow()
                    ])
                except Exception as e:
                    logger.error(f"Failed product_catalog row {i}: {e}")
                    self.stats['errors'].append(f"product_catalog row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(document_versions_insert, [
                        uuid.uuid4(), i, datetime.utcnow(), uuid.uuid4(), self.fake.sentence(), self.fake.text(max_nb_chars=200),
                        set(self.fake.words(nb=3)), {"k": "v"}, random.randint(100, 10000), random.randint(100, 20000), self.fake.sentence()
                    ])
                except Exception as e:
                    logger.error(f"Failed document_versions row {i}: {e}")
                    self.stats['errors'].append(f"document_versions row {i}: {e}")
                pbar.update(1)

                try:
                    ts = datetime.utcnow()
                    self.session.execute(multi_metric_insert, [
                        uuid.uuid4(), ts, random.random()*100, random.randint(1_000_000, 8_000_000), random.randint(1_000, 100_000),
                        random.randint(1_000, 100_000), random.randint(1_000, 100_000), random.randint(1_000, 100_000),
                        random.random()*100, random.randint(1_000_000, 8_000_000), random.random()*100, random.randint(500, 5000),
                        random.random()*500, random.randint(1, 500), random.randint(1, 1000), random.randint(1, 2000),
                        random.randint(1, 1_000_000), random.random()*5, random.random()*5, random.random()*5, random.random()*100,
                        random.randint(1_000, 1_000_000), random.randint(1, 1000), random.randint(1, 1000), random.randint(0, 100), random.randint(0, 100), random.randint(0, 100),
                        {"custom": 1.23}, set(["ok","warn"]), bytes(random.getrandbits(8) for _ in range(64))
                    ])
                except Exception as e:
                    logger.error(f"Failed multi_metric_timeseries row {i}: {e}")
                    self.stats['errors'].append(f"multi_metric_timeseries row {i}: {e}")
                pbar.update(1)

                try:
                    self.session.execute(sparse_insert, [
                        uuid.uuid4(), self.fake.word(), self.fake.word(), random.random()*100.0, random.choice([True, False]), datetime.utcnow(), json.dumps({"k":"v"}),
                        bytes(random.getrandbits(8) for _ in range(10)), set(self.fake.words(nb=2)), [self.fake.word() for _ in range(2)], {"k": "v"}
                    ])
                except Exception as e:
                    logger.error(f"Failed sparse_data_table row {i}: {e}")
                    self.stats['errors'].append(f"sparse_data_table row {i}: {e}")
                pbar.update(1)

                self.stats['rows_inserted'] += 6
    
    def generate_all_data(self):
        """Generate all test data"""
        logger.info(f"Starting comprehensive data generation for Cassandra {self.version}")
        
        try:
            # Create keyspace
            self.create_keyspace()
            
            # Create all tables
            self.create_basic_types_tables()
            self.create_collection_tables()
            self.create_time_series_tables()
            self.create_wide_tables()
            
            # Populate tables with data
            self.populate_simple_table()
            self.populate_collections_table()
            self.populate_time_series_tables()
            self.populate_wide_tables()
            
            # Generate final statistics
            self.generate_statistics()
            
            logger.info("Data generation completed successfully")
            
        except Exception as e:
            logger.error(f"Data generation failed: {e}")
            raise
        finally:
            self.cleanup()
    
    def generate_statistics(self):
        """Generate and save generation statistics"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        
        statistics = {
            'version': self.version,
            'host': self.host,
            'port': self.port,
            'scale': self.config.scale,
            'start_time': self.stats['start_time'].isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration.total_seconds(),
            'tables_created': self.stats['tables_created'],
            'rows_inserted': self.stats['rows_inserted'],
            'errors_count': len(self.stats['errors']),
            'errors': self.stats['errors'][:10],  # First 10 errors only
        }
        
        # Save statistics to file
        stats_file = f"/generated/v{self.version}/generation_statistics.json"
        os.makedirs(os.path.dirname(stats_file), exist_ok=True)
        
        with open(stats_file, 'w') as f:
            json.dump(statistics, f, indent=2)
        
        logger.info(f"Generation statistics saved to {stats_file}")
        logger.info(f"Tables created: {statistics['tables_created']}")
        logger.info(f"Rows inserted: {statistics['rows_inserted']}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Errors: {statistics['errors_count']}")
    
    def cleanup(self):
        """Clean up connections"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

    def populate_basic_counters(self):
        """Populate test_basic.counters with counter increments"""
        logger.info("Populating test_basic.counters (counters)...")
        print("[generate] test_basic.counters")

        update_stmt = self.session.prepare("""
        UPDATE test_basic.counters
        SET view_count = view_count + ?, like_count = like_count + ?, share_count = share_count + ?, total_interactions = total_interactions + ?
        WHERE id = ?
        """)

        # Use a small set of ids and random increments
        ids = ["home", "about", "products", "contact", "help"]
        rounds = 20
        with tqdm(total=len(ids) * rounds, desc="basic_counters") as pbar:
            for r in range(rounds):
                for cid in ids:
                    try:
                        views = random.randint(1, 10)
                        likes = random.randint(0, 5)
                        shares = random.randint(0, 3)
                        total = views + likes + shares
                        self.session.execute(update_stmt, [views, likes, shares, total, cid])
                    except Exception as e:
                        logger.error(f"Failed counters update {cid} r{r}: {e}")
                        self.stats['errors'].append(f"counters {cid} r{r}: {e}")
                    pbar.update(1)

        # counters are not counted as rows_inserted; but we note activity

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description="Comprehensive Test Data Generator for CQLite")
    parser.add_argument("--version", required=True, help="Cassandra version (3.7, 3.11, 4.0, 4.1, 5.0)")
    parser.add_argument("--host", default="localhost", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra port")
    parser.add_argument("--scale", default="COMPREHENSIVE", choices=["SMALL", "MEDIUM", "COMPREHENSIVE", "LARGE"],
                        help="Data generation scale")
    parser.add_argument("--rows-per-table", type=int, default=None, help="Uniform override for rows per table/group")
    parser.add_argument("--tables", default="basic,collections,timeseries,wide",
                        help="Comma-separated groups to populate: basic,collections,timeseries,wide")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=== CQLite Comprehensive Test Data Generator ===")
    logger.info(f"Cassandra version: {args.version}")
    logger.info(f"Host: {args.host}:{args.port}")
    logger.info(f"Scale: {args.scale}")
    logger.info(f"Tables: {args.tables}")
    
    try:
        # Create configuration
        config = DataGenerationConfig(args.scale)
        
        # Create generator
        generator = CassandraTestDataGenerator(args.host, args.port, args.version, config)
        
        # Generate selected groups (simple, while we restructure)
        selected = [t.strip() for t in args.tables.split(',') if t.strip()]
        if not selected:
            selected = ["basic","collections","timeseries","wide"]
        
        if "basic" in selected:
            generator.populate_simple_table()
            generator.populate_basic_additional_tables()
            generator.populate_basic_counters()
        if "collections" in selected:
            generator.populate_collections_table()
            generator.populate_collections_additional_tables()
        if "timeseries" in selected:
            generator.populate_time_series_tables()
            generator.populate_time_series_additional_tables()
        if "wide" in selected:
            generator.populate_wide_tables()
            generator.populate_wide_additional_tables()
        
        generator.generate_statistics()
        
        logger.info("✅ Data generation completed successfully!")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Data generation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()