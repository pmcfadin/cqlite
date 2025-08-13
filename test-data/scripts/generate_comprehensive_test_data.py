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
            self.session.default_consistency_level = 'ONE'
            
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
        INSERT INTO simple_table (
            id, text_col, int_col, bigint_col, float_col, double_col,
            boolean_col, timestamp_col, date_col, time_col, blob_col,
            inet_col, uuid_col, timeuuid_col, decimal_col, varint_col
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        batch_size = self.config.batch_size
        total_rows = self.config.basic_table_rows
        
        with tqdm(total=total_rows, desc="simple_table") as pbar:
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                
                for i in range(batch_start, batch_end):
                    try:
                        row_data = [
                            uuid.uuid4(),
                            self.fake.text(max_nb_chars=self.config.max_string_length),
                            random.randint(-2147483648, 2147483647),
                            random.randint(-9223372036854775808, 9223372036854775807),
                            random.uniform(-1e6, 1e6),
                            random.uniform(-1e6, 1e6),
                            random.choice([True, False]),
                            datetime.now() - timedelta(days=random.randint(0, 365)),
                            (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
                            random.randint(0, 86399999999999),  # nanoseconds in a day
                            bytes(random.getrandbits(8) for _ in range(random.randint(10, 1000))),
                            self.fake.ipv4(),
                            uuid.uuid4(),
                            uuid.uuid1(),
                            str(random.uniform(-1e6, 1e6)),
                            random.randint(-1000000, 1000000)
                        ]
                        
                        self.session.execute(insert_query, row_data)
                        self.stats['rows_inserted'] += 1
                        pbar.update(1)
                        
                    except Exception as e:
                        logger.error(f"Failed to insert row {i}: {e}")
                        self.stats['errors'].append(f"simple_table row {i}: {str(e)}")
    
    def populate_collections_table(self):
        """Populate collections table with test data"""
        logger.info("Populating collections_table...")
        
        insert_query = self.session.prepare("""
        INSERT INTO collections_table (
            id, list_col, set_col, map_col, frozen_list_col, frozen_set_col, frozen_map_col
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)
        
        total_rows = self.config.collection_table_rows
        
        with tqdm(total=total_rows, desc="collections_table") as pbar:
            for i in range(total_rows):
                try:
                    list_col = [self.fake.word() for _ in range(random.randint(1, self.config.max_collection_size))]
                    set_col = set(random.randint(1, 1000) for _ in range(random.randint(1, self.config.max_collection_size)))
                    map_col = {self.fake.word(): random.randint(1, 100) for _ in range(random.randint(1, self.config.max_collection_size))}
                    
                    row_data = [
                        uuid.uuid4(),
                        list_col,
                        set_col,
                        map_col,
                        list_col[:min(len(list_col), 10)],  # Frozen collections should be smaller
                        set(list(set_col)[:min(len(set_col), 10)]),
                        dict(list(map_col.items())[:min(len(map_col), 10)])
                    ]
                    
                    self.session.execute(insert_query, row_data)
                    self.stats['rows_inserted'] += 1
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to insert collections row {i}: {e}")
                    self.stats['errors'].append(f"collections_table row {i}: {str(e)}")
    
    def populate_time_series_tables(self):
        """Populate time series tables with test data"""
        logger.info("Populating time series tables...")
        
        # Populate sensor_data
        sensor_insert = self.session.prepare("""
        INSERT INTO sensor_data (sensor_id, timestamp, temperature, humidity, pressure, location)
        VALUES (?, ?, ?, ?, ?, ?)
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
                        self.fake.city()
                    ]
                    
                    self.session.execute(sensor_insert, row_data)
                    self.stats['rows_inserted'] += 1
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to insert sensor data row {i}: {e}")
                    self.stats['errors'].append(f"sensor_data row {i}: {str(e)}")
    
    def populate_wide_tables(self):
        """Populate wide tables with test data"""
        logger.info("Populating wide tables...")
        
        # Populate wide_partition_table
        wide_insert = self.session.prepare("""
        INSERT INTO wide_partition_table (partition_key, clustering_key, data_col)
        VALUES (?, ?, ?)
        """)
        
        partitions = [f"partition_{i}" for i in range(10)]
        
        with tqdm(total=self.config.wide_table_rows, desc="wide_partition_table") as pbar:
            for i in range(self.config.wide_table_rows):
                try:
                    partition_key = random.choice(partitions)
                    clustering_key = uuid.uuid1()
                    data_col = self.fake.text(max_nb_chars=1000)
                    
                    self.session.execute(wide_insert, [partition_key, clustering_key, data_col])
                    self.stats['rows_inserted'] += 1
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Failed to insert wide partition row {i}: {e}")
                    self.stats['errors'].append(f"wide_partition_table row {i}: {str(e)}")
    
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

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description="Comprehensive Test Data Generator for CQLite")
    parser.add_argument("--version", required=True, help="Cassandra version (3.7, 3.11, 4.0, 4.1, 5.0)")
    parser.add_argument("--host", default="localhost", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra port")
    parser.add_argument("--scale", default="COMPREHENSIVE", choices=["SMALL", "MEDIUM", "COMPREHENSIVE", "LARGE"],
                        help="Data generation scale")
    parser.add_argument("--keyspace", default="cqlite_test", help="Keyspace name")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=== CQLite Comprehensive Test Data Generator ===")
    logger.info(f"Cassandra version: {args.version}")
    logger.info(f"Host: {args.host}:{args.port}")
    logger.info(f"Scale: {args.scale}")
    logger.info(f"Keyspace: {args.keyspace}")
    
    try:
        # Create configuration
        config = DataGenerationConfig(args.scale)
        
        # Create generator
        generator = CassandraTestDataGenerator(args.host, args.port, args.version, config)
        
        # Generate all data
        generator.generate_all_data()
        
        logger.info("✅ Data generation completed successfully!")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Data generation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()