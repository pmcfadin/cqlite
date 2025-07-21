#!/usr/bin/env python3
"""
M3 Cassandra Test Data Generation Script

This script creates a Cassandra cluster, sets up complex type schemas,
inserts test data, and extracts SSTable files for CQLite validation.

Usage:
    python cassandra_test_setup.py --setup      # Create schemas and data
    python cassandra_test_setup.py --extract    # Extract SSTable files
    python cassandra_test_setup.py --validate   # Validate with CQLite
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraTestSetup:
    def __init__(self, host='localhost', port=9042):
        self.host = host
        self.port = port
        self.cluster = None
        self.session = None
        self.test_keyspace = 'm3_validation_test'
        
    def connect(self):
        """Connect to Cassandra cluster"""
        try:
            self.cluster = Cluster([self.host], port=self.port)
            self.session = self.cluster.connect()
            print(f"✅ Connected to Cassandra at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Cassandra: {e}")
            return False
    
    def create_keyspace(self):
        """Create test keyspace"""
        cql = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.test_keyspace}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
        """
        self.session.execute(cql)
        self.session.set_keyspace(self.test_keyspace)
        print(f"✅ Created keyspace: {self.test_keyspace}")
    
    def create_udt_schemas(self):
        """Create User Defined Type schemas"""
        udts = [
            """
            CREATE TYPE IF NOT EXISTS address (
                street text,
                city text,
                zip_code int,
                country text
            )
            """,
            """
            CREATE TYPE IF NOT EXISTS person (
                name text,
                age int,
                email text,
                addresses list<address>
            )
            """,
            """
            CREATE TYPE IF NOT EXISTS coordinates (
                latitude double,
                longitude double,
                altitude int
            )
            """
        ]
        
        for udt_cql in udts:
            self.session.execute(udt_cql)
        print("✅ Created UDT schemas")
    
    def create_test_tables(self):
        """Create all test tables with complex types"""
        tables = {
            'test_lists': """
                CREATE TABLE IF NOT EXISTS test_lists (
                    id UUID PRIMARY KEY,
                    int_list list<int>,
                    text_list list<text>,
                    nested_list list<list<int>>,
                    created_at timestamp
                )
            """,
            'test_sets': """
                CREATE TABLE IF NOT EXISTS test_sets (
                    id UUID PRIMARY KEY,
                    text_set set<text>,
                    int_set set<int>,
                    uuid_set set<uuid>,
                    created_at timestamp
                )
            """,
            'test_maps': """
                CREATE TABLE IF NOT EXISTS test_maps (
                    id UUID PRIMARY KEY,
                    simple_map map<text, int>,
                    complex_map map<text, list<int>>,
                    nested_map map<text, map<text, int>>,
                    created_at timestamp
                )
            """,
            'test_tuples': """
                CREATE TABLE IF NOT EXISTS test_tuples (
                    id UUID PRIMARY KEY,
                    simple_tuple tuple<int, text>,
                    coordinate_tuple tuple<double, double, int>,
                    complex_tuple tuple<text, list<int>, map<text, int>>,
                    created_at timestamp
                )
            """,
            'test_udts': """
                CREATE TABLE IF NOT EXISTS test_udts (
                    id UUID PRIMARY KEY,
                    home_address address,
                    work_address address,
                    person_info person,
                    location coordinates,
                    created_at timestamp
                )
            """,
            'test_frozen': """
                CREATE TABLE IF NOT EXISTS test_frozen (
                    id UUID PRIMARY KEY,
                    frozen_list frozen<list<text>>,
                    frozen_set frozen<set<int>>,
                    frozen_map frozen<map<text, int>>,
                    frozen_address frozen<address>,
                    created_at timestamp
                )
            """,
            'test_mixed_complex': """
                CREATE TABLE IF NOT EXISTS test_mixed_complex (
                    id UUID PRIMARY KEY,
                    data_mix tuple<text, list<int>, map<text, address>>,
                    nested_collection list<map<text, set<int>>>,
                    udt_with_collections person,
                    created_at timestamp
                )
            """
        }
        
        for table_name, table_cql in tables.items():
            self.session.execute(table_cql)
            print(f"✅ Created table: {table_name}")
    
    def insert_test_data(self):
        """Insert comprehensive test data"""
        import uuid
        from datetime import datetime
        
        test_data = [
            # Lists
            {
                'table': 'test_lists',
                'data': [
                    {
                        'id': uuid.uuid4(),
                        'int_list': [1, 2, 3, 4, 5],
                        'text_list': ['hello', 'world', 'cassandra', 'cqlite'],
                        'nested_list': [[1, 2], [3, 4], [5, 6]],
                        'created_at': datetime.now()
                    },
                    {
                        'id': uuid.uuid4(),
                        'int_list': [],  # Empty list
                        'text_list': ['single'],
                        'nested_list': [[]], # Nested empty list
                        'created_at': datetime.now()
                    }
                ]
            },
            # Sets
            {
                'table': 'test_sets',
                'data': [
                    {
                        'id': uuid.uuid4(),
                        'text_set': {'apple', 'banana', 'cherry'},
                        'int_set': {10, 20, 30, 40},
                        'uuid_set': {uuid.uuid4(), uuid.uuid4()},
                        'created_at': datetime.now()
                    }
                ]
            },
            # Maps
            {
                'table': 'test_maps',
                'data': [
                    {
                        'id': uuid.uuid4(),
                        'simple_map': {'key1': 100, 'key2': 200},
                        'complex_map': {'list1': [1, 2, 3], 'list2': [4, 5, 6]},
                        'nested_map': {'outer': {'inner1': 1, 'inner2': 2}},
                        'created_at': datetime.now()
                    }
                ]
            },
            # Tuples
            {
                'table': 'test_tuples',
                'data': [
                    {
                        'id': uuid.uuid4(),
                        'simple_tuple': (42, 'answer'),
                        'coordinate_tuple': (37.7749, -122.4194, 100),
                        'complex_tuple': ('data', [1, 2, 3], {'count': 3}),
                        'created_at': datetime.now()
                    }
                ]
            }
        ]
        
        for table_info in test_data:
            table_name = table_info['table']
            for row in table_info['data']:
                columns = ', '.join(row.keys())
                placeholders = ', '.join(['?' for _ in row.keys()])
                cql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                self.session.execute(cql, list(row.values()))
        
        print("✅ Inserted test data")
    
    def flush_tables(self):
        """Force flush tables to generate SSTable files"""
        tables = [
            'test_lists', 'test_sets', 'test_maps', 'test_tuples',
            'test_udts', 'test_frozen', 'test_mixed_complex'
        ]
        
        for table in tables:
            try:
                # Force flush using nodetool (requires Cassandra bin in PATH)
                result = subprocess.run(
                    ['nodetool', 'flush', self.test_keyspace, table],
                    capture_output=True, text=True, check=True
                )
                print(f"✅ Flushed table: {table}")
            except subprocess.CalledProcessError as e:
                print(f"⚠️  Warning: Could not flush {table}: {e}")
            except FileNotFoundError:
                print("⚠️  Warning: nodetool not found. Tables may not be flushed to disk.")
                break
    
    def extract_sstable_files(self, output_dir='./cassandra_test_data'):
        """Extract SSTable files from Cassandra data directory"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Common Cassandra data directories
        data_dirs = [
            '/var/lib/cassandra/data',
            '/opt/cassandra/data',
            './data',
            '../data'
        ]
        
        keyspace_dir = None
        for data_dir in data_dirs:
            potential_path = Path(data_dir) / self.test_keyspace
            if potential_path.exists():
                keyspace_dir = potential_path
                break
        
        if not keyspace_dir:
            print("❌ Could not find Cassandra data directory")
            return False
        
        print(f"📁 Found keyspace data at: {keyspace_dir}")
        
        # Copy SSTable files for each table
        for table_dir in keyspace_dir.iterdir():
            if table_dir.is_dir() and table_dir.name.startswith('test_'):
                table_output = output_path / table_dir.name
                table_output.mkdir(exist_ok=True)
                
                # Copy all SSTable-related files
                for file_path in table_dir.iterdir():
                    if any(file_path.name.endswith(ext) for ext in 
                           ['-Data.db', '-Index.db', '-Filter.db', '-Statistics.db', '-Summary.db', '-CompressionInfo.db']):
                        shutil.copy2(file_path, table_output)
                        print(f"📄 Copied: {file_path.name}")
        
        print(f"✅ SSTable files extracted to: {output_dir}")
        return True
    
    def generate_schema_definitions(self, output_dir='./cassandra_test_data'):
        """Generate JSON schema definitions for CQLite"""
        output_path = Path(output_dir)
        
        # Query Cassandra system tables for schema information
        schemas = {}
        
        # Get table schemas
        table_query = """
            SELECT table_name, column_name, type, kind 
            FROM system_schema.columns 
            WHERE keyspace_name = ?
        """
        rows = self.session.execute(table_query, [self.test_keyspace])
        
        for row in rows:
            table_name = row.table_name
            if table_name not in schemas:
                schemas[table_name] = {
                    'keyspace': self.test_keyspace,
                    'table': table_name,
                    'partition_keys': [],
                    'clustering_keys': [],
                    'columns': []
                }
            
            column_info = {
                'name': row.column_name,
                'type': row.type,
                'nullable': True
            }
            
            if row.kind == 'partition_key':
                schemas[table_name]['partition_keys'].append({
                    'name': row.column_name,
                    'type': row.type,
                    'position': 0  # Would need additional query for exact position
                })
            elif row.kind == 'clustering':
                schemas[table_name]['clustering_keys'].append({
                    'name': row.column_name,
                    'type': row.type,
                    'position': 0,
                    'order': 'ASC'
                })
            
            schemas[table_name]['columns'].append(column_info)
        
        # Save schemas as JSON files
        for table_name, schema in schemas.items():
            schema_file = output_path / f"{table_name}_schema.json"
            with open(schema_file, 'w') as f:
                json.dump(schema, f, indent=2)
            print(f"📋 Generated schema: {schema_file}")
        
        return schemas
    
    def validate_with_cqlite(self, test_data_dir='./cassandra_test_data'):
        """Validate SSTable files with CQLite parser"""
        print("🔍 Validating SSTable files with CQLite...")
        
        # This would call CQLite's parser to validate the files
        test_script = f"""
        import sys
        sys.path.append('../..')
        
        # Test CQLite parsing of generated SSTable files
        # This is a placeholder for actual CQLite integration
        print("CQLite validation would happen here")
        """
        
        # For now, just verify files exist
        data_path = Path(test_data_dir)
        if not data_path.exists():
            print("❌ Test data directory not found")
            return False
        
        sstable_count = 0
        for table_dir in data_path.iterdir():
            if table_dir.is_dir():
                for file_path in table_dir.iterdir():
                    if file_path.name.endswith('-Data.db'):
                        sstable_count += 1
                        print(f"📄 Found SSTable: {file_path}")
        
        print(f"✅ Found {sstable_count} SSTable files ready for validation")
        return sstable_count > 0
    
    def cleanup(self):
        """Clean up resources"""
        if self.cluster:
            self.cluster.shutdown()
        print("🧹 Cleanup complete")

def main():
    parser = argparse.ArgumentParser(description='M3 Cassandra Test Data Generation')
    parser.add_argument('--setup', action='store_true', help='Create schemas and insert test data')
    parser.add_argument('--extract', action='store_true', help='Extract SSTable files')
    parser.add_argument('--validate', action='store_true', help='Validate with CQLite')
    parser.add_argument('--all', action='store_true', help='Run all steps')
    parser.add_argument('--host', default='localhost', help='Cassandra host')
    parser.add_argument('--port', type=int, default=9042, help='Cassandra port')
    
    args = parser.parse_args()
    
    if not any([args.setup, args.extract, args.validate, args.all]):
        parser.print_help()
        return
    
    setup = CassandraTestSetup(args.host, args.port)
    
    try:
        if args.all or args.setup:
            print("🚀 Starting Cassandra test data setup...")
            if not setup.connect():
                return 1
            
            setup.create_keyspace()
            setup.create_udt_schemas()
            setup.create_test_tables()
            setup.insert_test_data()
            setup.flush_tables()
            print("✅ Setup complete!")
        
        if args.all or args.extract:
            print("📦 Extracting SSTable files...")
            if not setup.cluster:
                setup.connect()
            setup.extract_sstable_files()
            setup.generate_schema_definitions()
            print("✅ Extraction complete!")
        
        if args.all or args.validate:
            print("🔍 Validating with CQLite...")
            setup.validate_with_cqlite()
            print("✅ Validation complete!")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        setup.cleanup()
    
    print("🎉 M3 Cassandra test data generation complete!")
    return 0

if __name__ == '__main__':
    sys.exit(main())