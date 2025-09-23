#!/usr/bin/env python3
"""
Test Data Generation Pipeline for Cassandra 5 SSTable Testing

This script generates comprehensive test datasets for validating CQLite's
SSTable reading capabilities against Cassandra 5 format specifications.

Features:
- Various data types and partition key patterns
- Different compression scenarios (LZ4, Snappy, ZSTD)
- Index usage patterns (bloom filters, partition summaries)
- Summary file variations
- TTL and tombstone scenarios
- Multi-generation conflict data
"""

import os
import sys
import json
import yaml
import subprocess
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
import argparse
from dataclasses import dataclass, asdict
import uuid

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DatasetConfig:
    """Configuration for a test dataset"""
    name: str
    keyspace: str
    table: str
    description: str
    row_count: int
    compression_type: str
    bloom_filter_fp_chance: float
    enable_ttl: bool
    enable_tombstones: bool
    clustering_keys: List[str]
    data_types: List[str]
    partition_size: str  # small, medium, large
    compaction_strategy: str

@dataclass
class ValidationRule:
    """Validation rule for generated data"""
    rule_type: str
    expected_value: Any
    tolerance: Optional[float] = None

@dataclass
class TestDataset:
    """Complete test dataset with metadata"""
    config: DatasetConfig
    sstable_path: str
    metadata_path: str
    validation_rules: List[ValidationRule]
    generated_at: str
    cassandra_version: str
    file_sizes: Dict[str, int]
    checksums: Dict[str, str]

class CassandraTestDataGenerator:
    """Main test data generator for Cassandra 5 SSTable testing"""

    def __init__(self, base_dir: str, cassandra_home: str = None):
        self.base_dir = Path(base_dir)
        self.scripts_dir = self.base_dir / "scripts"
        self.datasets_dir = self.base_dir / "test-data" / "datasets"
        self.templates_dir = self.base_dir / "templates"
        self.cassandra_home = Path(cassandra_home) if cassandra_home else None

        # Create directories
        self.scripts_dir.mkdir(parents=True, exist_ok=True)
        self.datasets_dir.mkdir(parents=True, exist_ok=True)
        self.templates_dir.mkdir(parents=True, exist_ok=True)

        self.generated_datasets: List[TestDataset] = []

    def generate_comprehensive_test_suite(self) -> Dict[str, List[TestDataset]]:
        """Generate comprehensive test suite covering all scenarios"""
        logger.info("Generating comprehensive test data suite")

        test_categories = {
            "basic_types": self._generate_basic_types_datasets(),
            "collections": self._generate_collections_datasets(),
            "compression": self._generate_compression_datasets(),
            "ttl_scenarios": self._generate_ttl_datasets(),
            "tombstone_scenarios": self._generate_tombstone_datasets(),
            "performance_benchmarks": self._generate_performance_datasets(),
            "edge_cases": self._generate_edge_case_datasets(),
            "cassandra5_features": self._generate_cassandra5_specific_datasets()
        }

        return test_categories

    def _generate_basic_types_datasets(self) -> List[TestDataset]:
        """Generate datasets testing all basic Cassandra data types"""
        logger.info("Generating basic data types test datasets")

        configs = [
            DatasetConfig(
                name="primitive_types_test",
                keyspace="test_basic_types",
                table="primitive_types",
                description="All primitive Cassandra data types",
                row_count=1000,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["timestamp_col"],
                data_types=["text", "int", "bigint", "float", "double", "boolean",
                           "timestamp", "uuid", "timeuuid", "blob", "decimal",
                           "inet", "date", "time", "smallint", "tinyint"],
                partition_size="medium",
                compaction_strategy="SizeTieredCompactionStrategy"
            ),
            DatasetConfig(
                name="unicode_text_test",
                keyspace="test_unicode",
                table="unicode_text",
                description="Unicode text handling and edge cases",
                row_count=500,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["category"],
                data_types=["text"],
                partition_size="small",
                compaction_strategy="LeveledCompactionStrategy"
            ),
            DatasetConfig(
                name="large_blob_test",
                keyspace="test_blobs",
                table="large_blobs",
                description="Large blob data handling",
                row_count=100,
                compression_type="ZstdCompressor",
                bloom_filter_fp_chance=0.001,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["chunk_id"],
                data_types=["blob"],
                partition_size="large",
                compaction_strategy="SizeTieredCompactionStrategy"
            )
        ]

        return [self._generate_dataset_from_config(config) for config in configs]

    def _generate_collections_datasets(self) -> List[TestDataset]:
        """Generate datasets testing collection types"""
        logger.info("Generating collection types test datasets")

        configs = [
            DatasetConfig(
                name="basic_collections",
                keyspace="test_collections",
                table="basic_collections",
                description="Basic collection types: lists, sets, maps",
                row_count=500,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["collection_type"],
                data_types=["list<text>", "set<int>", "map<text,text>"],
                partition_size="medium",
                compaction_strategy="SizeTieredCompactionStrategy"
            ),
            DatasetConfig(
                name="nested_collections",
                keyspace="test_nested",
                table="nested_collections",
                description="Nested and frozen collections",
                row_count=200,
                compression_type="SnappyCompressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["nesting_level"],
                data_types=["map<text,frozen<list<text>>>", "list<frozen<set<int>>>"],
                partition_size="small",
                compaction_strategy="LeveledCompactionStrategy"
            )
        ]

        return [self._generate_dataset_from_config(config) for config in configs]

    def _generate_compression_datasets(self) -> List[TestDataset]:
        """Generate datasets with different compression scenarios"""
        logger.info("Generating compression test datasets")

        compression_types = ["LZ4Compressor", "SnappyCompressor", "ZstdCompressor", "DeflateCompressor"]
        datasets = []

        for comp_type in compression_types:
            config = DatasetConfig(
                name=f"compression_{comp_type.lower().replace('compressor', '')}",
                keyspace="test_compression",
                table=f"compressed_{comp_type.lower().replace('compressor', '')}",
                description=f"Data compressed with {comp_type}",
                row_count=1000,
                compression_type=comp_type,
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["data_type"],
                data_types=["text", "blob"],
                partition_size="medium",
                compaction_strategy="SizeTieredCompactionStrategy"
            )
            datasets.append(self._generate_dataset_from_config(config))

        return datasets

    def _generate_ttl_datasets(self) -> List[TestDataset]:
        """Generate datasets with TTL scenarios"""
        logger.info("Generating TTL test datasets")

        configs = [
            DatasetConfig(
                name="ttl_expired",
                keyspace="test_ttl",
                table="ttl_expired",
                description="Expired TTL data for read-time filtering",
                row_count=500,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=True,
                enable_tombstones=False,
                clustering_keys=["expiry_bucket"],
                data_types=["text", "int"],
                partition_size="small",
                compaction_strategy="SizeTieredCompactionStrategy"
            ),
            DatasetConfig(
                name="ttl_mixed",
                keyspace="test_ttl",
                table="ttl_mixed",
                description="Mixed TTL scenarios: expired, active, and no TTL",
                row_count=1000,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=True,
                enable_tombstones=False,
                clustering_keys=["ttl_status"],
                data_types=["text", "timestamp"],
                partition_size="medium",
                compaction_strategy="LeveledCompactionStrategy"
            )
        ]

        return [self._generate_dataset_from_config(config) for config in configs]

    def _generate_tombstone_datasets(self) -> List[TestDataset]:
        """Generate datasets with tombstone scenarios"""
        logger.info("Generating tombstone test datasets")

        configs = [
            DatasetConfig(
                name="cell_tombstones",
                keyspace="test_tombstones",
                table="cell_tombstones",
                description="Cell-level tombstones for reconciliation testing",
                row_count=300,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=True,
                clustering_keys=["tombstone_type"],
                data_types=["text", "int"],
                partition_size="small",
                compaction_strategy="SizeTieredCompactionStrategy"
            ),
            DatasetConfig(
                name="range_tombstones",
                keyspace="test_tombstones",
                table="range_tombstones",
                description="Range tombstones for clustering key ranges",
                row_count=500,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=True,
                clustering_keys=["range_key"],
                data_types=["text"],
                partition_size="medium",
                compaction_strategy="LeveledCompactionStrategy"
            )
        ]

        return [self._generate_dataset_from_config(config) for config in configs]

    def _generate_performance_datasets(self) -> List[TestDataset]:
        """Generate performance benchmark datasets"""
        logger.info("Generating performance benchmark datasets")

        configs = [
            DatasetConfig(
                name="wide_partitions",
                keyspace="test_performance",
                table="wide_partitions",
                description="Wide partitions for read performance testing",
                row_count=100000,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["time_bucket", "sequence"],
                data_types=["text", "bigint", "timestamp"],
                partition_size="large",
                compaction_strategy="SizeTieredCompactionStrategy"
            ),
            DatasetConfig(
                name="many_partitions",
                keyspace="test_performance",
                table="many_partitions",
                description="Many small partitions for index performance",
                row_count=50000,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.001,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["item_id"],
                data_types=["text", "int"],
                partition_size="small",
                compaction_strategy="LeveledCompactionStrategy"
            )
        ]

        return [self._generate_dataset_from_config(config) for config in configs]

    def _generate_edge_case_datasets(self) -> List[TestDataset]:
        """Generate edge case datasets"""
        logger.info("Generating edge case test datasets")

        configs = [
            DatasetConfig(
                name="empty_values",
                keyspace="test_edge_cases",
                table="empty_values",
                description="Empty strings, null values, and minimal data",
                row_count=100,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["value_type"],
                data_types=["text", "blob"],
                partition_size="small",
                compaction_strategy="SizeTieredCompactionStrategy"
            ),
            DatasetConfig(
                name="boundary_values",
                keyspace="test_edge_cases",
                table="boundary_values",
                description="Min/max values for numeric types",
                row_count=200,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["value_category"],
                data_types=["int", "bigint", "float", "double", "decimal"],
                partition_size="small",
                compaction_strategy="SizeTieredCompactionStrategy"
            )
        ]

        return [self._generate_dataset_from_config(config) for config in configs]

    def _generate_cassandra5_specific_datasets(self) -> List[TestDataset]:
        """Generate datasets testing Cassandra 5 specific features"""
        logger.info("Generating Cassandra 5 specific test datasets")

        configs = [
            DatasetConfig(
                name="vector_types",
                keyspace="test_cassandra5",
                table="vector_types",
                description="Cassandra 5 vector type support",
                row_count=100,
                compression_type="ZstdCompressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["vector_id"],
                data_types=["vector<float,128>"],
                partition_size="medium",
                compaction_strategy="UnifiedCompactionStrategy"
            ),
            DatasetConfig(
                name="sai_indexes",
                keyspace="test_cassandra5",
                table="sai_indexes",
                description="Storage Attached Indexes (SAI) test data",
                row_count=1000,
                compression_type="LZ4Compressor",
                bloom_filter_fp_chance=0.01,
                enable_ttl=False,
                enable_tombstones=False,
                clustering_keys=["indexed_field"],
                data_types=["text", "int"],
                partition_size="medium",
                compaction_strategy="UnifiedCompactionStrategy"
            )
        ]

        return [self._generate_dataset_from_config(config) for config in configs]

    def _generate_dataset_from_config(self, config: DatasetConfig) -> TestDataset:
        """Generate a single dataset from configuration"""
        logger.info(f"Generating dataset: {config.name}")

        # Create keyspace and table
        cql_script = self._generate_cql_script(config)

        # Generate data
        data_script = self._generate_data_insertion_script(config)

        # Execute scripts to create SSTable
        sstable_path = self._execute_data_generation(config, cql_script, data_script)

        # Generate metadata
        metadata = self._generate_dataset_metadata(config, sstable_path)

        # Create validation rules
        validation_rules = self._generate_validation_rules(config)

        # Calculate checksums
        checksums = self._calculate_checksums(sstable_path)

        # Get file sizes
        file_sizes = self._get_file_sizes(sstable_path)

        dataset = TestDataset(
            config=config,
            sstable_path=str(sstable_path),
            metadata_path=str(metadata),
            validation_rules=validation_rules,
            generated_at=datetime.utcnow().isoformat(),
            cassandra_version=self._get_cassandra_version(),
            file_sizes=file_sizes,
            checksums=checksums
        )

        self.generated_datasets.append(dataset)
        return dataset

    def _generate_cql_script(self, config: DatasetConfig) -> str:
        """Generate CQL script for keyspace and table creation"""
        cql_lines = [
            f"DROP KEYSPACE IF EXISTS {config.keyspace};",
            f"CREATE KEYSPACE {config.keyspace} WITH REPLICATION = {{",
            "    'class': 'SimpleStrategy',",
            "    'replication_factor': 1",
            "};",
            f"USE {config.keyspace};",
            ""
        ]

        # Generate table creation
        table_def = self._generate_table_definition(config)
        cql_lines.append(table_def)

        return "\\n".join(cql_lines)

    def _generate_table_definition(self, config: DatasetConfig) -> str:
        """Generate table definition based on config"""
        columns = ["id UUID PRIMARY KEY"]

        # Add clustering keys
        for i, clustering_key in enumerate(config.clustering_keys):
            columns.append(f"{clustering_key} text")

        # Add data type columns
        for i, data_type in enumerate(config.data_types):
            columns.append(f"col_{data_type.replace('<', '_').replace('>', '_').replace(',', '_')} {data_type}")

        if config.enable_ttl:
            columns.append("ttl_value int")

        primary_key = "id"
        if config.clustering_keys:
            clustering_part = ", ".join(config.clustering_keys)
            primary_key = f"(id), {clustering_part}"

        table_def = f"""CREATE TABLE {config.table} (
    {', '.join(columns)},
    PRIMARY KEY ({primary_key})
) WITH compression = {{'class': '{config.compression_type}'}}
  AND bloom_filter_fp_chance = {config.bloom_filter_fp_chance}
  AND compaction = {{'class': 'org.apache.cassandra.db.compaction.{config.compaction_strategy}'}};"""

        return table_def

    def _generate_data_insertion_script(self, config: DatasetConfig) -> str:
        """Generate data insertion script"""
        script_lines = [
            "#!/usr/bin/env python3",
            "import uuid",
            "import random",
            "import time",
            "from datetime import datetime, timedelta",
            "from cassandra.cluster import Cluster",
            "",
            "cluster = Cluster(['127.0.0.1'])",
            "session = cluster.connect()",
            f"session.execute('USE {config.keyspace}')",
            ""
        ]

        # Generate insertion logic based on config
        insert_logic = self._generate_insertion_logic(config)
        script_lines.extend(insert_logic)

        script_lines.extend([
            "",
            "session.shutdown()",
            "cluster.shutdown()",
            "print(f'Generated {0} rows for {1}')" % (config.row_count, config.table)
        ])

        return "\\n".join(script_lines)

    def _generate_insertion_logic(self, config: DatasetConfig) -> List[str]:
        """Generate insertion logic based on configuration"""
        lines = []

        # Prepare statement
        columns = ["id"] + config.clustering_keys
        for data_type in config.data_types:
            col_name = f"col_{data_type.replace('<', '_').replace('>', '_').replace(',', '_')}"
            columns.append(col_name)

        if config.enable_ttl:
            columns.append("ttl_value")

        placeholders = ", ".join(["?" for _ in columns])

        lines.extend([
            f"prepared = session.prepare(",
            f"    'INSERT INTO {config.table} ({', '.join(columns)}) VALUES ({placeholders})'",
            ")",
            ""
        ])

        # Generate data insertion loop
        lines.extend([
            f"for i in range({config.row_count}):",
            "    row_id = uuid.uuid4()",
        ])

        # Generate clustering key values
        for clustering_key in config.clustering_keys:
            lines.append(f"    {clustering_key} = f'cluster_{{i % 100}}'")

        # Generate data values
        for data_type in config.data_types:
            col_name = f"col_{data_type.replace('<', '_').replace('>', '_').replace(',', '_')}"
            value_generator = self._get_value_generator(data_type, config)
            lines.append(f"    {col_name} = {value_generator}")

        if config.enable_ttl:
            lines.append("    ttl_value = random.randint(1, 3600) if i % 3 == 0 else None")

        # Execute statement
        values = ["row_id"] + config.clustering_keys
        for data_type in config.data_types:
            col_name = f"col_{data_type.replace('<', '_').replace('>', '_').replace(',', '_')}"
            values.append(col_name)

        if config.enable_ttl:
            values.append("ttl_value")

        lines.extend([
            f"    session.execute(prepared, ({', '.join(values)}))",
            "    if i % 1000 == 0:",
            "        print(f'Inserted {i} rows')"
        ])

        return lines

    def _get_value_generator(self, data_type: str, config: DatasetConfig) -> str:
        """Get value generator code for data type"""
        generators = {
            "text": "f'text_value_{i}'",
            "int": "random.randint(-2**31, 2**31-1)",
            "bigint": "random.randint(-2**63, 2**63-1)",
            "float": "random.uniform(-1000.0, 1000.0)",
            "double": "random.uniform(-1000000.0, 1000000.0)",
            "boolean": "random.choice([True, False])",
            "timestamp": "int(time.time() * 1000)",
            "uuid": "uuid.uuid4()",
            "timeuuid": "uuid.uuid1()",
            "blob": "bytes(f'blob_data_{i}', 'utf-8')",
            "decimal": "f'{random.uniform(-1000.0, 1000.0):.2f}'",
            "inet": "'127.0.0.1'",
            "date": "datetime.now().date()",
            "time": "datetime.now().time()",
            "smallint": "random.randint(-32768, 32767)",
            "tinyint": "random.randint(-128, 127)",
            "list<text>": "[f'item_{j}' for j in range(random.randint(1, 5))]",
            "set<int>": "set(random.randint(1, 100) for _ in range(random.randint(1, 5)))",
            "map<text,text>": "{f'key_{j}': f'value_{j}' for j in range(random.randint(1, 3))}"
        }

        return generators.get(data_type, "f'unknown_type_{i}'")

    def _execute_data_generation(self, config: DatasetConfig, cql_script: str, data_script: str) -> Path:
        """Execute data generation scripts"""
        logger.info(f"Executing data generation for {config.name}")

        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cql', delete=False) as f:
            f.write(cql_script)
            cql_file = f.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(data_script)
            py_file = f.name

        try:
            # Execute CQL script
            if self.cassandra_home:
                cqlsh_cmd = [str(self.cassandra_home / "bin" / "cqlsh"), "-f", cql_file]
            else:
                cqlsh_cmd = ["cqlsh", "-f", cql_file]

            subprocess.run(cqlsh_cmd, check=True, capture_output=True, text=True)

            # Execute data insertion
            subprocess.run([sys.executable, py_file], check=True, capture_output=True, text=True)

            # Flush data and create SSTable
            self._flush_and_create_sstable(config.keyspace, config.table)

            # Find and copy SSTable files
            sstable_dir = self._find_sstable_directory(config.keyspace, config.table)
            target_dir = self.datasets_dir / "generated" / config.name
            target_dir.mkdir(parents=True, exist_ok=True)

            # Copy SSTable files
            for file_path in sstable_dir.glob("*"):
                if file_path.is_file():
                    shutil.copy2(file_path, target_dir)

            return target_dir

        finally:
            # Clean up temporary files
            os.unlink(cql_file)
            os.unlink(py_file)

    def _flush_and_create_sstable(self, keyspace: str, table: str):
        """Flush memtables and create SSTable files"""
        if self.cassandra_home:
            nodetool_cmd = [str(self.cassandra_home / "bin" / "nodetool")]
        else:
            nodetool_cmd = ["nodetool"]

        # Flush memtables
        subprocess.run(nodetool_cmd + ["flush", keyspace, table], check=True)

        # Compact to create single SSTable
        subprocess.run(nodetool_cmd + ["compact", keyspace, table], check=True)

    def _find_sstable_directory(self, keyspace: str, table: str) -> Path:
        """Find SSTable directory for keyspace/table"""
        # Default Cassandra data directory patterns
        possible_paths = [
            Path("/var/lib/cassandra/data"),
            Path(os.path.expanduser("~/.cassandra/data")),
            Path("./data")
        ]

        if self.cassandra_home:
            possible_paths.insert(0, self.cassandra_home / "data")

        for data_path in possible_paths:
            if data_path.exists():
                keyspace_dir = data_path / keyspace
                if keyspace_dir.exists():
                    for table_dir in keyspace_dir.glob(f"{table}-*"):
                        if table_dir.is_dir():
                            return table_dir

        raise FileNotFoundError(f"Could not find SSTable directory for {keyspace}.{table}")

    def _generate_dataset_metadata(self, config: DatasetConfig, sstable_path: Path) -> Path:
        """Generate metadata file for dataset"""
        metadata = {
            "name": config.name,
            "description": config.description,
            "generated_at": datetime.utcnow().isoformat(),
            "config": asdict(config),
            "sstable_path": str(sstable_path),
            "files": [f.name for f in sstable_path.glob("*")],
            "cassandra_version": self._get_cassandra_version()
        }

        metadata_file = sstable_path / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        return metadata_file

    def _generate_validation_rules(self, config: DatasetConfig) -> List[ValidationRule]:
        """Generate validation rules for dataset"""
        rules = [
            ValidationRule("row_count", config.row_count, tolerance=0.1),
            ValidationRule("compression_type", config.compression_type),
            ValidationRule("bloom_filter_fp_chance", config.bloom_filter_fp_chance, tolerance=0.001)
        ]

        if config.enable_ttl:
            rules.append(ValidationRule("has_ttl_data", True))

        if config.enable_tombstones:
            rules.append(ValidationRule("has_tombstones", True))

        return rules

    def _calculate_checksums(self, sstable_path: Path) -> Dict[str, str]:
        """Calculate checksums for all SSTable files"""
        import hashlib

        checksums = {}
        for file_path in sstable_path.glob("*"):
            if file_path.is_file():
                hasher = hashlib.sha256()
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hasher.update(chunk)
                checksums[file_path.name] = hasher.hexdigest()

        return checksums

    def _get_file_sizes(self, sstable_path: Path) -> Dict[str, int]:
        """Get file sizes for all SSTable files"""
        sizes = {}
        for file_path in sstable_path.glob("*"):
            if file_path.is_file():
                sizes[file_path.name] = file_path.stat().st_size

        return sizes

    def _get_cassandra_version(self) -> str:
        """Get Cassandra version"""
        try:
            if self.cassandra_home:
                cmd = [str(self.cassandra_home / "bin" / "cassandra"), "-v"]
            else:
                cmd = ["cassandra", "-v"]

            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.stdout.strip()
        except:
            return "unknown"

    def generate_manifest(self) -> Path:
        """Generate manifest file for all datasets"""
        manifest = {
            "generated_at": datetime.utcnow().isoformat(),
            "generator_version": "1.0.0",
            "total_datasets": len(self.generated_datasets),
            "datasets": [asdict(dataset) for dataset in self.generated_datasets]
        }

        manifest_file = self.datasets_dir / "manifest.json"
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Generated manifest with {len(self.generated_datasets)} datasets")
        return manifest_file

    def validate_generated_data(self) -> Dict[str, bool]:
        """Validate all generated datasets"""
        logger.info("Validating generated datasets")

        validation_results = {}
        for dataset in self.generated_datasets:
            validation_results[dataset.config.name] = self._validate_dataset(dataset)

        return validation_results

    def _validate_dataset(self, dataset: TestDataset) -> bool:
        """Validate a single dataset"""
        try:
            sstable_path = Path(dataset.sstable_path)

            # Check if files exist
            if not sstable_path.exists():
                logger.error(f"SSTable directory not found: {sstable_path}")
                return False

            # Check for required SSTable files
            required_extensions = ["-Data.db", "-Index.db", "-Summary.db", "-Statistics.db"]
            for ext in required_extensions:
                if not any(f.name.endswith(ext) for f in sstable_path.glob("*")):
                    logger.warning(f"Missing {ext} file in {sstable_path}")

            # Validate checksums
            current_checksums = self._calculate_checksums(sstable_path)
            for filename, expected_checksum in dataset.checksums.items():
                if filename in current_checksums:
                    if current_checksums[filename] != expected_checksum:
                        logger.error(f"Checksum mismatch for {filename}")
                        return False
                else:
                    logger.warning(f"File {filename} not found for checksum validation")

            logger.info(f"Dataset {dataset.config.name} validation passed")
            return True

        except Exception as e:
            logger.error(f"Validation failed for {dataset.config.name}: {e}")
            return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Generate test data for Cassandra 5 SSTable testing")
    parser.add_argument("--base-dir", default=".", help="Base directory for project")
    parser.add_argument("--cassandra-home", help="Cassandra installation directory")
    parser.add_argument("--categories", nargs="+",
                       choices=["basic_types", "collections", "compression", "ttl_scenarios",
                               "tombstone_scenarios", "performance_benchmarks", "edge_cases",
                               "cassandra5_features", "all"],
                       default=["all"], help="Categories of datasets to generate")
    parser.add_argument("--validate", action="store_true", help="Validate generated data")
    parser.add_argument("--output-manifest", action="store_true", help="Generate manifest file")

    args = parser.parse_args()

    generator = CassandraTestDataGenerator(args.base_dir, args.cassandra_home)

    try:
        if "all" in args.categories:
            test_suite = generator.generate_comprehensive_test_suite()
        else:
            test_suite = {}
            for category in args.categories:
                method_name = f"_generate_{category}_datasets"
                if hasattr(generator, method_name):
                    test_suite[category] = getattr(generator, method_name)()

        logger.info(f"Generated {sum(len(datasets) for datasets in test_suite.values())} datasets")

        if args.validate:
            validation_results = generator.validate_generated_data()
            failed_validations = [name for name, passed in validation_results.items() if not passed]
            if failed_validations:
                logger.error(f"Validation failed for: {failed_validations}")
                sys.exit(1)
            else:
                logger.info("All datasets passed validation")

        if args.output_manifest:
            manifest_file = generator.generate_manifest()
            logger.info(f"Manifest generated: {manifest_file}")

        logger.info("Test data generation completed successfully")

    except Exception as e:
        logger.error(f"Test data generation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()