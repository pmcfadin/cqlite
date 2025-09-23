#!/usr/bin/env python3
"""
Test Data Pipeline Manager for CQLite

This script manages the complete test data lifecycle:
- Data generation and versioning
- Validation and quality assurance
- Performance benchmark creation
- CI/CD integration
- Cleanup and regeneration

Features:
- Automated test data generation
- Version management with semantic versioning
- Quality gates and validation
- Performance regression detection
- Integration with CI/CD pipelines
"""

import os
import sys
import json
import yaml
import shutil
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import logging
import argparse
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DatasetVersion:
    """Represents a versioned dataset"""
    name: str
    version: str
    path: Path
    created_at: str
    checksum: str
    metadata: Dict[str, Any]
    validation_status: str

@dataclass
class PipelineConfig:
    """Configuration for the data pipeline"""
    base_dir: Path
    cassandra_home: Optional[Path]
    cqlite_binary: Optional[Path]
    max_parallel_tasks: int
    retention_days: int
    quality_gates: Dict[str, Any]
    performance_thresholds: Dict[str, float]

class DataPipelineManager:
    """Main pipeline manager for test data"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.datasets_dir = config.base_dir / "test-data" / "datasets"
        self.versions_dir = config.base_dir / "test-data" / "versions"
        self.benchmarks_dir = config.base_dir / "test-data" / "benchmarks"
        self.reports_dir = config.base_dir / "test-data" / "reports"

        # Create directories
        for directory in [self.datasets_dir, self.versions_dir, self.benchmarks_dir, self.reports_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        self.dataset_registry: Dict[str, List[DatasetVersion]] = {}
        self._load_registry()

    def _load_registry(self):
        """Load dataset registry from disk"""
        registry_file = self.versions_dir / "registry.json"
        if registry_file.exists():
            try:
                with open(registry_file, 'r') as f:
                    data = json.load(f)
                    for name, versions in data.items():
                        self.dataset_registry[name] = [
                            DatasetVersion(**version) for version in versions
                        ]
            except Exception as e:
                logger.warning(f"Failed to load registry: {e}")

    def _save_registry(self):
        """Save dataset registry to disk"""
        registry_file = self.versions_dir / "registry.json"
        try:
            data = {}
            for name, versions in self.dataset_registry.items():
                data[name] = [asdict(version) for version in versions]

            with open(registry_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save registry: {e}")

    def generate_full_test_suite(self, force_regenerate: bool = False) -> Dict[str, Any]:
        """Generate complete test data suite"""
        logger.info("Starting full test suite generation")

        start_time = time.time()

        # Check if we need to regenerate
        if not force_regenerate and self._is_current_suite_valid():
            logger.info("Current test suite is valid, skipping generation")
            return self._get_current_suite_info()

        # Generate datasets in parallel
        generation_results = self._run_parallel_generation()

        # Validate generated data
        validation_results = self._run_parallel_validation(generation_results)

        # Create performance benchmarks
        benchmark_results = self._create_performance_benchmarks(validation_results)

        # Generate golden reference data
        golden_data = self._generate_golden_reference_data(validation_results)

        # Update registry
        self._update_dataset_registry(validation_results)

        # Cleanup old versions
        self._cleanup_old_versions()

        generation_time = time.time() - start_time

        suite_info = {
            "generated_at": datetime.utcnow().isoformat(),
            "generation_time_seconds": generation_time,
            "total_datasets": len(validation_results),
            "valid_datasets": sum(1 for r in validation_results if r["is_valid"]),
            "benchmark_count": len(benchmark_results),
            "golden_reference_count": len(golden_data),
            "suite_version": self._calculate_suite_version()
        }

        logger.info(f"Test suite generation completed in {generation_time:.2f} seconds")
        return suite_info

    def _is_current_suite_valid(self) -> bool:
        """Check if current test suite is still valid"""
        suite_info_file = self.datasets_dir / "suite_info.json"
        if not suite_info_file.exists():
            return False

        try:
            with open(suite_info_file, 'r') as f:
                suite_info = json.load(f)

            # Check if suite is too old
            generated_at = datetime.fromisoformat(suite_info.get("generated_at", ""))
            if datetime.utcnow() - generated_at > timedelta(days=7):
                return False

            # Check if all expected datasets exist
            expected_datasets = suite_info.get("total_datasets", 0)
            actual_datasets = len(list(self.datasets_dir.glob("*/metadata.json")))

            return actual_datasets >= expected_datasets

        except Exception as e:
            logger.warning(f"Failed to validate current suite: {e}")
            return False

    def _get_current_suite_info(self) -> Dict[str, Any]:
        """Get information about current test suite"""
        suite_info_file = self.datasets_dir / "suite_info.json"
        try:
            with open(suite_info_file, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def _run_parallel_generation(self) -> List[Dict[str, Any]]:
        """Run dataset generation in parallel"""
        logger.info("Running parallel dataset generation")

        # Import the generator (assuming it's in the same directory)
        sys.path.append(str(self.config.base_dir / "scripts"))
        from test_data_generator import CassandraTestDataGenerator

        generator = CassandraTestDataGenerator(
            str(self.config.base_dir),
            str(self.config.cassandra_home) if self.config.cassandra_home else None
        )

        # Generate all test categories
        test_categories = generator.generate_comprehensive_test_suite()

        results = []
        for category, datasets in test_categories.items():
            for dataset in datasets:
                results.append({
                    "category": category,
                    "dataset": dataset,
                    "status": "generated"
                })

        return results

    def _run_parallel_validation(self, generation_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run validation in parallel"""
        logger.info("Running parallel validation")

        # Import the validator
        from validate_sstables import SSTableValidator

        validator = SSTableValidator(
            str(self.config.cqlite_binary) if self.config.cqlite_binary else None
        )

        validation_results = []

        with ThreadPoolExecutor(max_workers=self.config.max_parallel_tasks) as executor:
            future_to_dataset = {}

            for result in generation_results:
                dataset = result["dataset"]
                sstable_path = Path(dataset.sstable_path)

                future = executor.submit(self._validate_single_dataset, validator, sstable_path, dataset)
                future_to_dataset[future] = result

            for future in as_completed(future_to_dataset):
                result = future_to_dataset[future]
                try:
                    validation_result = future.result()
                    result.update(validation_result)
                    validation_results.append(result)
                except Exception as e:
                    logger.error(f"Validation failed for {result['dataset'].config.name}: {e}")
                    result.update({
                        "is_valid": False,
                        "validation_error": str(e)
                    })
                    validation_results.append(result)

        return validation_results

    def _validate_single_dataset(self, validator: 'SSTableValidator', sstable_path: Path, dataset: Any) -> Dict[str, Any]:
        """Validate a single dataset"""
        validation_results = validator.validate_sstable_directory(sstable_path)

        if not validation_results:
            return {"is_valid": False, "error": "No validation results"}

        validation_result = validation_results[0]  # Assuming one SSTable per directory

        # Apply quality gates
        passes_quality_gates = self._check_quality_gates(validation_result, dataset)

        return {
            "is_valid": validation_result.is_valid and passes_quality_gates,
            "validation_result": asdict(validation_result),
            "quality_gate_status": passes_quality_gates
        }

    def _check_quality_gates(self, validation_result: Any, dataset: Any) -> bool:
        """Check if dataset passes quality gates"""
        gates = self.config.quality_gates

        # Check error count
        max_errors = gates.get("max_errors", 0)
        error_count = sum(1 for issue in validation_result.issues if issue.severity == "ERROR")
        if error_count > max_errors:
            logger.warning(f"Dataset {dataset.config.name} failed quality gate: too many errors ({error_count} > {max_errors})")
            return False

        # Check warning count
        max_warnings = gates.get("max_warnings", 10)
        warning_count = sum(1 for issue in validation_result.issues if issue.severity == "WARNING")
        if warning_count > max_warnings:
            logger.warning(f"Dataset {dataset.config.name} failed quality gate: too many warnings ({warning_count} > {max_warnings})")
            return False

        # Check file size constraints
        min_data_size = gates.get("min_data_file_size", 1024)  # 1KB minimum
        if validation_result.components.data_file:
            data_size = validation_result.components.data_file.stat().st_size
            if data_size < min_data_size:
                logger.warning(f"Dataset {dataset.config.name} failed quality gate: data file too small ({data_size} < {min_data_size})")
                return False

        return True

    def _create_performance_benchmarks(self, validation_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create performance benchmarks for datasets"""
        logger.info("Creating performance benchmarks")

        benchmark_results = []

        for result in validation_results:
            if not result.get("is_valid", False):
                continue

            dataset = result["dataset"]
            benchmark = self._benchmark_dataset(dataset)
            benchmark_results.append(benchmark)

        # Save benchmark summary
        self._save_benchmark_summary(benchmark_results)

        return benchmark_results

    def _benchmark_dataset(self, dataset: Any) -> Dict[str, Any]:
        """Benchmark a single dataset"""
        if not self.config.cqlite_binary:
            return {"dataset": dataset.config.name, "benchmarks": {}, "error": "No CQLite binary"}

        sstable_path = Path(dataset.sstable_path)

        benchmarks = {}

        # Read performance benchmark
        try:
            start_time = time.time()
            result = subprocess.run([
                str(self.config.cqlite_binary), "read", str(sstable_path)
            ], capture_output=True, text=True, timeout=60)
            read_time = time.time() - start_time

            if result.returncode == 0:
                line_count = len(result.stdout.splitlines())
                benchmarks["read_time_seconds"] = read_time
                benchmarks["read_throughput_lines_per_second"] = line_count / read_time if read_time > 0 else 0
                benchmarks["output_lines"] = line_count
            else:
                benchmarks["read_error"] = result.stderr

        except subprocess.TimeoutExpired:
            benchmarks["read_error"] = "Timeout"
        except Exception as e:
            benchmarks["read_error"] = str(e)

        # Memory usage benchmark (if available)
        try:
            # This would need platform-specific implementation
            benchmarks["memory_usage"] = self._measure_memory_usage(sstable_path)
        except Exception as e:
            logger.debug(f"Memory measurement failed: {e}")

        return {
            "dataset": dataset.config.name,
            "benchmarks": benchmarks,
            "measured_at": datetime.utcnow().isoformat()
        }

    def _measure_memory_usage(self, sstable_path: Path) -> Dict[str, Any]:
        """Measure memory usage during SSTable reading"""
        # This is a placeholder - real implementation would need process monitoring
        return {
            "peak_memory_mb": 0,
            "measurement_method": "placeholder"
        }

    def _save_benchmark_summary(self, benchmark_results: List[Dict[str, Any]]):
        """Save benchmark summary"""
        summary = {
            "generated_at": datetime.utcnow().isoformat(),
            "total_benchmarks": len(benchmark_results),
            "benchmarks": benchmark_results
        }

        benchmark_file = self.benchmarks_dir / f"benchmarks_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(benchmark_file, 'w') as f:
            json.dump(summary, f, indent=2)

        # Also save as latest
        latest_file = self.benchmarks_dir / "latest_benchmarks.json"
        with open(latest_file, 'w') as f:
            json.dump(summary, f, indent=2)

    def _generate_golden_reference_data(self, validation_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate golden reference data for regression testing"""
        logger.info("Generating golden reference data")

        golden_data = []

        for result in validation_results:
            if not result.get("is_valid", False):
                continue

            dataset = result["dataset"]
            golden_ref = self._create_golden_reference(dataset)
            golden_data.append(golden_ref)

        # Save golden reference summary
        self._save_golden_reference_summary(golden_data)

        return golden_data

    def _create_golden_reference(self, dataset: Any) -> Dict[str, Any]:
        """Create golden reference for a dataset"""
        if not self.config.cqlite_binary:
            return {"dataset": dataset.config.name, "error": "No CQLite binary"}

        sstable_path = Path(dataset.sstable_path)

        try:
            # Read data with CQLite
            result = subprocess.run([
                str(self.config.cqlite_binary), "read", str(sstable_path)
            ], capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                # Create checksum of output
                output_checksum = hashlib.sha256(result.stdout.encode()).hexdigest()

                # Save reference output
                ref_dir = self.datasets_dir / "golden_references" / dataset.config.name
                ref_dir.mkdir(parents=True, exist_ok=True)

                ref_file = ref_dir / "reference_output.txt"
                with open(ref_file, 'w') as f:
                    f.write(result.stdout)

                checksum_file = ref_dir / "output_checksum.txt"
                with open(checksum_file, 'w') as f:
                    f.write(output_checksum)

                return {
                    "dataset": dataset.config.name,
                    "reference_file": str(ref_file),
                    "checksum": output_checksum,
                    "line_count": len(result.stdout.splitlines()),
                    "created_at": datetime.utcnow().isoformat()
                }
            else:
                return {
                    "dataset": dataset.config.name,
                    "error": f"CQLite read failed: {result.stderr}"
                }

        except Exception as e:
            return {
                "dataset": dataset.config.name,
                "error": str(e)
            }

    def _save_golden_reference_summary(self, golden_data: List[Dict[str, Any]]):
        """Save golden reference summary"""
        summary = {
            "generated_at": datetime.utcnow().isoformat(),
            "total_references": len(golden_data),
            "references": golden_data
        }

        ref_file = self.datasets_dir / "golden_references" / "summary.json"
        ref_file.parent.mkdir(parents=True, exist_ok=True)

        with open(ref_file, 'w') as f:
            json.dump(summary, f, indent=2)

    def _update_dataset_registry(self, validation_results: List[Dict[str, Any]]):
        """Update dataset registry with new versions"""
        for result in validation_results:
            if not result.get("is_valid", False):
                continue

            dataset = result["dataset"]
            version = self._calculate_dataset_version(dataset)

            dataset_version = DatasetVersion(
                name=dataset.config.name,
                version=version,
                path=Path(dataset.sstable_path),
                created_at=datetime.utcnow().isoformat(),
                checksum=self._calculate_dataset_checksum(Path(dataset.sstable_path)),
                metadata=asdict(dataset.config),
                validation_status="valid"
            )

            if dataset.config.name not in self.dataset_registry:
                self.dataset_registry[dataset.config.name] = []

            self.dataset_registry[dataset.config.name].append(dataset_version)

        self._save_registry()

    def _calculate_dataset_version(self, dataset: Any) -> str:
        """Calculate semantic version for dataset"""
        # Simple versioning based on timestamp for now
        # In a real implementation, this might be based on schema changes, data changes, etc.
        timestamp = datetime.utcnow().strftime("%Y%m%d.%H%M%S")
        return f"1.0.{timestamp}"

    def _calculate_dataset_checksum(self, sstable_path: Path) -> str:
        """Calculate checksum for entire dataset"""
        hasher = hashlib.sha256()

        for file_path in sorted(sstable_path.glob("*")):
            if file_path.is_file():
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hasher.update(chunk)

        return hasher.hexdigest()

    def _calculate_suite_version(self) -> str:
        """Calculate version for entire test suite"""
        timestamp = datetime.utcnow().strftime("%Y%m%d.%H%M%S")
        return f"2.0.{timestamp}"

    def _cleanup_old_versions(self):
        """Clean up old dataset versions"""
        logger.info("Cleaning up old dataset versions")

        cutoff_date = datetime.utcnow() - timedelta(days=self.config.retention_days)

        for name, versions in self.dataset_registry.items():
            versions_to_keep = []

            for version in versions:
                created_at = datetime.fromisoformat(version.created_at)
                if created_at >= cutoff_date:
                    versions_to_keep.append(version)
                else:
                    # Remove old dataset files
                    try:
                        if version.path.exists():
                            shutil.rmtree(version.path)
                        logger.info(f"Removed old version: {name} v{version.version}")
                    except Exception as e:
                        logger.warning(f"Failed to remove old version {name} v{version.version}: {e}")

            self.dataset_registry[name] = versions_to_keep

        self._save_registry()

    def run_regression_tests(self) -> Dict[str, Any]:
        """Run regression tests against golden reference data"""
        logger.info("Running regression tests")

        if not self.config.cqlite_binary:
            return {"error": "No CQLite binary available for testing"}

        golden_refs_dir = self.datasets_dir / "golden_references"
        if not golden_refs_dir.exists():
            return {"error": "No golden reference data available"}

        summary_file = golden_refs_dir / "summary.json"
        if not summary_file.exists():
            return {"error": "No golden reference summary available"}

        with open(summary_file, 'r') as f:
            golden_summary = json.load(f)

        regression_results = []

        for ref_data in golden_summary.get("references", []):
            if "error" in ref_data:
                continue

            result = self._run_single_regression_test(ref_data)
            regression_results.append(result)

        # Calculate summary
        total_tests = len(regression_results)
        passed_tests = sum(1 for r in regression_results if r.get("passed", False))

        regression_summary = {
            "tested_at": datetime.utcnow().isoformat(),
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "success_rate": passed_tests / total_tests if total_tests > 0 else 0,
            "results": regression_results
        }

        # Save regression test results
        self._save_regression_results(regression_summary)

        return regression_summary

    def _run_single_regression_test(self, ref_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run regression test for a single dataset"""
        dataset_name = ref_data["dataset"]
        reference_file = Path(ref_data["reference_file"])
        expected_checksum = ref_data["checksum"]

        # Find current dataset
        if dataset_name not in self.dataset_registry:
            return {
                "dataset": dataset_name,
                "passed": False,
                "error": "Dataset not found in registry"
            }

        latest_version = self.dataset_registry[dataset_name][-1]  # Get latest version

        try:
            # Run CQLite on current data
            result = subprocess.run([
                str(self.config.cqlite_binary), "read", str(latest_version.path)
            ], capture_output=True, text=True, timeout=60)

            if result.returncode != 0:
                return {
                    "dataset": dataset_name,
                    "passed": False,
                    "error": f"CQLite read failed: {result.stderr}"
                }

            # Calculate checksum of current output
            current_checksum = hashlib.sha256(result.stdout.encode()).hexdigest()

            # Compare with reference
            passed = current_checksum == expected_checksum

            return {
                "dataset": dataset_name,
                "passed": passed,
                "expected_checksum": expected_checksum,
                "actual_checksum": current_checksum,
                "tested_at": datetime.utcnow().isoformat()
            }

        except Exception as e:
            return {
                "dataset": dataset_name,
                "passed": False,
                "error": str(e)
            }

    def _save_regression_results(self, results: Dict[str, Any]):
        """Save regression test results"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        results_file = self.reports_dir / f"regression_test_{timestamp}.json"

        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)

        # Also save as latest
        latest_file = self.reports_dir / "latest_regression_test.json"
        with open(latest_file, 'w') as f:
            json.dump(results, f, indent=2)

        logger.info(f"Regression test results saved to: {results_file}")

def load_config(config_file: Optional[Path] = None) -> PipelineConfig:
    """Load pipeline configuration"""
    if config_file and config_file.exists():
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
    else:
        config_data = {}

    return PipelineConfig(
        base_dir=Path(config_data.get("base_dir", ".")),
        cassandra_home=Path(config_data["cassandra_home"]) if config_data.get("cassandra_home") else None,
        cqlite_binary=Path(config_data["cqlite_binary"]) if config_data.get("cqlite_binary") else None,
        max_parallel_tasks=config_data.get("max_parallel_tasks", 4),
        retention_days=config_data.get("retention_days", 30),
        quality_gates=config_data.get("quality_gates", {
            "max_errors": 0,
            "max_warnings": 10,
            "min_data_file_size": 1024
        }),
        performance_thresholds=config_data.get("performance_thresholds", {
            "max_read_time_seconds": 30.0,
            "min_throughput_lines_per_second": 1000.0
        })
    )

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Manage test data pipeline for CQLite")
    parser.add_argument("command", choices=["generate", "validate", "benchmark", "regression", "cleanup", "status"],
                       help="Command to execute")
    parser.add_argument("--config", type=Path, help="Configuration file")
    parser.add_argument("--force", action="store_true", help="Force regeneration even if current data is valid")
    parser.add_argument("--parallel", type=int, help="Number of parallel tasks")

    args = parser.parse_args()

    config = load_config(args.config)
    if args.parallel:
        config.max_parallel_tasks = args.parallel

    manager = DataPipelineManager(config)

    try:
        if args.command == "generate":
            result = manager.generate_full_test_suite(args.force)
            print(json.dumps(result, indent=2))

        elif args.command == "validate":
            # Run validation on existing data
            from validate_sstables import SSTableValidator
            validator = SSTableValidator(str(config.cqlite_binary) if config.cqlite_binary else None)
            results = validator.validate_sstable_directory(manager.datasets_dir)
            report = validator.generate_validation_report(results)

            report_file = manager.reports_dir / f"validation_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            validator.save_validation_report(report, report_file)
            print(f"Validation report saved to: {report_file}")

        elif args.command == "benchmark":
            # Create performance benchmarks
            validation_results = []  # This would need to be loaded from existing data
            benchmark_results = manager._create_performance_benchmarks(validation_results)
            print(f"Created {len(benchmark_results)} benchmarks")

        elif args.command == "regression":
            result = manager.run_regression_tests()
            print(json.dumps(result, indent=2))

            if result.get("failed_tests", 0) > 0:
                sys.exit(1)

        elif args.command == "cleanup":
            manager._cleanup_old_versions()
            print("Cleanup completed")

        elif args.command == "status":
            # Show pipeline status
            status = {
                "dataset_count": len(manager.dataset_registry),
                "datasets": {name: len(versions) for name, versions in manager.dataset_registry.items()},
                "last_generated": manager._get_current_suite_info().get("generated_at", "never")
            }
            print(json.dumps(status, indent=2))

        logger.info(f"Pipeline command '{args.command}' completed successfully")

    except Exception as e:
        logger.error(f"Pipeline command '{args.command}' failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()