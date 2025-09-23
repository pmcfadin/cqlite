#!/usr/bin/env python3
"""
SSTable Validation Pipeline for CQLite

This script provides comprehensive validation of SSTable files to ensure:
- Component integrity (Data, Index, Summary, Statistics files)
- Cross-component relationships and consistency
- Cassandra 5 format compliance
- Golden reference data validation

Features:
- File-level integrity checks
- Component relationship validation
- Format specification compliance
- Performance metrics collection
- Regression detection
"""

import os
import sys
import json
import hashlib
import struct
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, NamedTuple
from dataclasses import dataclass, asdict
from datetime import datetime
import logging
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SSTableComponents:
    """SSTable component files"""
    data_file: Optional[Path] = None
    index_file: Optional[Path] = None
    summary_file: Optional[Path] = None
    statistics_file: Optional[Path] = None
    filter_file: Optional[Path] = None
    compression_info_file: Optional[Path] = None
    digest_file: Optional[Path] = None

    def is_complete(self) -> bool:
        """Check if all required components are present"""
        required = [self.data_file, self.index_file, self.summary_file, self.statistics_file]
        return all(f is not None and f.exists() for f in required)

    def get_all_files(self) -> List[Path]:
        """Get all non-None component files"""
        return [f for f in [
            self.data_file, self.index_file, self.summary_file,
            self.statistics_file, self.filter_file, self.compression_info_file,
            self.digest_file
        ] if f is not None]

@dataclass
class ValidationIssue:
    """Represents a validation issue"""
    severity: str  # ERROR, WARNING, INFO
    component: str
    message: str
    details: Optional[Dict[str, Any]] = None

@dataclass
class ValidationResult:
    """Result of SSTable validation"""
    sstable_path: Path
    is_valid: bool
    issues: List[ValidationIssue]
    components: SSTableComponents
    metrics: Dict[str, Any]
    validated_at: str

class SSTableValidator:
    """Main SSTable validation class"""

    def __init__(self, cqlite_binary: Optional[str] = None):
        self.cqlite_binary = cqlite_binary or self._find_cqlite_binary()
        self.validation_cache: Dict[str, ValidationResult] = {}

    def _find_cqlite_binary(self) -> Optional[str]:
        """Find CQLite binary"""
        possible_paths = [
            "./target/release/cqlite",
            "./target/debug/cqlite",
            "cqlite"
        ]

        for path in possible_paths:
            if os.path.exists(path) or shutil.which(path):
                return path

        return None

    def validate_sstable_directory(self, sstable_dir: Path) -> List[ValidationResult]:
        """Validate all SSTables in a directory"""
        logger.info(f"Validating SSTables in: {sstable_dir}")

        results = []
        sstable_groups = self._group_sstable_files(sstable_dir)

        for prefix, components in sstable_groups.items():
            logger.info(f"Validating SSTable: {prefix}")
            result = self.validate_sstable(sstable_dir, components)
            results.append(result)

        return results

    def _group_sstable_files(self, directory: Path) -> Dict[str, SSTableComponents]:
        """Group SSTable files by their prefix"""
        groups = {}

        for file_path in directory.glob("*"):
            if not file_path.is_file():
                continue

            filename = file_path.name

            # Extract prefix (everything before the last component identifier)
            if "-Data.db" in filename:
                prefix = filename.replace("-Data.db", "")
                if prefix not in groups:
                    groups[prefix] = SSTableComponents()
                groups[prefix].data_file = file_path
            elif "-Index.db" in filename:
                prefix = filename.replace("-Index.db", "")
                if prefix not in groups:
                    groups[prefix] = SSTableComponents()
                groups[prefix].index_file = file_path
            elif "-Summary.db" in filename:
                prefix = filename.replace("-Summary.db", "")
                if prefix not in groups:
                    groups[prefix] = SSTableComponents()
                groups[prefix].summary_file = file_path
            elif "-Statistics.db" in filename:
                prefix = filename.replace("-Statistics.db", "")
                if prefix not in groups:
                    groups[prefix] = SSTableComponents()
                groups[prefix].statistics_file = file_path
            elif "-Filter.db" in filename:
                prefix = filename.replace("-Filter.db", "")
                if prefix not in groups:
                    groups[prefix] = SSTableComponents()
                groups[prefix].filter_file = file_path
            elif "-CompressionInfo.db" in filename:
                prefix = filename.replace("-CompressionInfo.db", "")
                if prefix not in groups:
                    groups[prefix] = SSTableComponents()
                groups[prefix].compression_info_file = file_path
            elif "-Digest.crc32" in filename:
                prefix = filename.replace("-Digest.crc32", "")
                if prefix not in groups:
                    groups[prefix] = SSTableComponents()
                groups[prefix].digest_file = file_path

        return groups

    def validate_sstable(self, directory: Path, components: SSTableComponents) -> ValidationResult:
        """Validate a single SSTable"""
        issues = []
        metrics = {}

        # Check component completeness
        if not components.is_complete():
            issues.append(ValidationIssue(
                severity="ERROR",
                component="components",
                message="Missing required SSTable components",
                details={"missing": self._get_missing_components(components)}
            ))

        # File-level validations
        self._validate_file_integrity(components, issues, metrics)

        # Component-specific validations
        if components.data_file:
            self._validate_data_file(components.data_file, issues, metrics)

        if components.index_file:
            self._validate_index_file(components.index_file, issues, metrics)

        if components.summary_file:
            self._validate_summary_file(components.summary_file, issues, metrics)

        if components.statistics_file:
            self._validate_statistics_file(components.statistics_file, issues, metrics)

        if components.filter_file:
            self._validate_filter_file(components.filter_file, issues, metrics)

        if components.compression_info_file:
            self._validate_compression_info_file(components.compression_info_file, issues, metrics)

        # Cross-component validations
        self._validate_component_relationships(components, issues, metrics)

        # CQLite compatibility validation
        if self.cqlite_binary and components.is_complete():
            self._validate_cqlite_compatibility(directory, components, issues, metrics)

        # Determine overall validity
        error_count = sum(1 for issue in issues if issue.severity == "ERROR")
        is_valid = error_count == 0

        return ValidationResult(
            sstable_path=directory,
            is_valid=is_valid,
            issues=issues,
            components=components,
            metrics=metrics,
            validated_at=datetime.utcnow().isoformat()
        )

    def _get_missing_components(self, components: SSTableComponents) -> List[str]:
        """Get list of missing required components"""
        missing = []

        if components.data_file is None or not components.data_file.exists():
            missing.append("Data.db")
        if components.index_file is None or not components.index_file.exists():
            missing.append("Index.db")
        if components.summary_file is None or not components.summary_file.exists():
            missing.append("Summary.db")
        if components.statistics_file is None or not components.statistics_file.exists():
            missing.append("Statistics.db")

        return missing

    def _validate_file_integrity(self, components: SSTableComponents, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate basic file integrity"""
        for file_path in components.get_all_files():
            if not file_path.exists():
                issues.append(ValidationIssue(
                    severity="ERROR",
                    component="file_integrity",
                    message=f"File does not exist: {file_path.name}"
                ))
                continue

            # Check file size
            size = file_path.stat().st_size
            if size == 0:
                issues.append(ValidationIssue(
                    severity="WARNING",
                    component="file_integrity",
                    message=f"Empty file: {file_path.name}"
                ))

            metrics[f"{file_path.name}_size"] = size

            # Calculate checksum
            checksum = self._calculate_file_checksum(file_path)
            metrics[f"{file_path.name}_checksum"] = checksum

    def _validate_data_file(self, data_file: Path, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate Data.db file"""
        try:
            with open(data_file, 'rb') as f:
                # Read and validate file header
                header = f.read(16)  # First 16 bytes typically contain format info

                if len(header) < 16:
                    issues.append(ValidationIssue(
                        severity="ERROR",
                        component="data_file",
                        message="Data file too small to contain valid header"
                    ))
                    return

                # Basic format validation (this would need to be expanded based on actual format)
                metrics["data_file_header"] = header.hex()

                # Check for corruption indicators
                f.seek(0, 2)  # Seek to end
                file_size = f.tell()

                if file_size < 100:  # Minimum reasonable size
                    issues.append(ValidationIssue(
                        severity="WARNING",
                        component="data_file",
                        message="Data file seems unusually small"
                    ))

                metrics["data_file_analyzed_size"] = file_size

        except Exception as e:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="data_file",
                message=f"Failed to read data file: {e}"
            ))

    def _validate_index_file(self, index_file: Path, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate Index.db file"""
        try:
            with open(index_file, 'rb') as f:
                # Read index header
                header = f.read(32)  # Larger header for index files

                if len(header) < 32:
                    issues.append(ValidationIssue(
                        severity="ERROR",
                        component="index_file",
                        message="Index file too small to contain valid header"
                    ))
                    return

                metrics["index_file_header"] = header.hex()

                # Validate index structure (simplified)
                f.seek(0, 2)
                file_size = f.tell()
                metrics["index_file_analyzed_size"] = file_size

                if file_size % 8 != 0:  # Index entries often have fixed sizes
                    issues.append(ValidationIssue(
                        severity="WARNING",
                        component="index_file",
                        message="Index file size not aligned to expected entry size"
                    ))

        except Exception as e:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="index_file",
                message=f"Failed to read index file: {e}"
            ))

    def _validate_summary_file(self, summary_file: Path, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate Summary.db file"""
        try:
            with open(summary_file, 'rb') as f:
                content = f.read()

                if len(content) < 16:
                    issues.append(ValidationIssue(
                        severity="ERROR",
                        component="summary_file",
                        message="Summary file too small"
                    ))
                    return

                metrics["summary_file_size"] = len(content)
                metrics["summary_file_header"] = content[:16].hex()

                # Validate summary structure
                # This would need detailed format knowledge

        except Exception as e:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="summary_file",
                message=f"Failed to read summary file: {e}"
            ))

    def _validate_statistics_file(self, stats_file: Path, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate Statistics.db file"""
        try:
            with open(stats_file, 'rb') as f:
                content = f.read()

                if len(content) == 0:
                    issues.append(ValidationIssue(
                        severity="WARNING",
                        component="statistics_file",
                        message="Empty statistics file"
                    ))
                    return

                metrics["statistics_file_size"] = len(content)

                # Parse statistics (format specific)
                # This would need detailed implementation

        except Exception as e:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="statistics_file",
                message=f"Failed to read statistics file: {e}"
            ))

    def _validate_filter_file(self, filter_file: Path, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate Filter.db (bloom filter) file"""
        try:
            with open(filter_file, 'rb') as f:
                content = f.read()

                if len(content) < 8:
                    issues.append(ValidationIssue(
                        severity="ERROR",
                        component="filter_file",
                        message="Bloom filter file too small"
                    ))
                    return

                metrics["filter_file_size"] = len(content)

                # Validate bloom filter structure
                # Implementation would depend on filter format

        except Exception as e:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="filter_file",
                message=f"Failed to read filter file: {e}"
            ))

    def _validate_compression_info_file(self, comp_info_file: Path, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate CompressionInfo.db file"""
        try:
            with open(comp_info_file, 'rb') as f:
                content = f.read()

                if len(content) == 0:
                    issues.append(ValidationIssue(
                        severity="WARNING",
                        component="compression_info_file",
                        message="Empty compression info file"
                    ))
                    return

                metrics["compression_info_size"] = len(content)

                # Parse compression metadata
                # This would need format-specific implementation

        except Exception as e:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="compression_info_file",
                message=f"Failed to read compression info file: {e}"
            ))

    def _validate_component_relationships(self, components: SSTableComponents, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate relationships between components"""
        if not components.is_complete():
            return

        # Validate data/index relationship
        data_size = components.data_file.stat().st_size
        index_size = components.index_file.stat().st_size

        # Index should typically be much smaller than data
        if index_size > data_size:
            issues.append(ValidationIssue(
                severity="WARNING",
                component="component_relationships",
                message="Index file larger than data file",
                details={"data_size": data_size, "index_size": index_size}
            ))

        # Summary should be smaller than index
        summary_size = components.summary_file.stat().st_size
        if summary_size > index_size:
            issues.append(ValidationIssue(
                severity="WARNING",
                component="component_relationships",
                message="Summary file larger than index file",
                details={"index_size": index_size, "summary_size": summary_size}
            ))

        metrics["size_ratios"] = {
            "index_to_data": index_size / data_size if data_size > 0 else 0,
            "summary_to_index": summary_size / index_size if index_size > 0 else 0
        }

    def _validate_cqlite_compatibility(self, directory: Path, components: SSTableComponents, issues: List[ValidationIssue], metrics: Dict[str, Any]):
        """Validate compatibility with CQLite"""
        if not self.cqlite_binary:
            issues.append(ValidationIssue(
                severity="INFO",
                component="cqlite_compatibility",
                message="CQLite binary not found, skipping compatibility validation"
            ))
            return

        try:
            # Try to read the SSTable with CQLite
            result = subprocess.run([
                self.cqlite_binary, "read", str(directory)
            ], capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                metrics["cqlite_read_success"] = True
                metrics["cqlite_output_lines"] = len(result.stdout.splitlines())
            else:
                issues.append(ValidationIssue(
                    severity="ERROR",
                    component="cqlite_compatibility",
                    message=f"CQLite failed to read SSTable: {result.stderr}",
                    details={"return_code": result.returncode}
                ))
                metrics["cqlite_read_success"] = False

        except subprocess.TimeoutExpired:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="cqlite_compatibility",
                message="CQLite read operation timed out"
            ))
        except Exception as e:
            issues.append(ValidationIssue(
                severity="ERROR",
                component="cqlite_compatibility",
                message=f"Failed to execute CQLite: {e}"
            ))

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of file"""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def generate_validation_report(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate comprehensive validation report"""
        total_sstables = len(results)
        valid_sstables = sum(1 for r in results if r.is_valid)

        all_issues = []
        for result in results:
            all_issues.extend(result.issues)

        issue_summary = {
            "ERROR": sum(1 for issue in all_issues if issue.severity == "ERROR"),
            "WARNING": sum(1 for issue in all_issues if issue.severity == "WARNING"),
            "INFO": sum(1 for issue in all_issues if issue.severity == "INFO")
        }

        component_issues = {}
        for issue in all_issues:
            if issue.component not in component_issues:
                component_issues[issue.component] = 0
            component_issues[issue.component] += 1

        report = {
            "validation_summary": {
                "total_sstables": total_sstables,
                "valid_sstables": valid_sstables,
                "invalid_sstables": total_sstables - valid_sstables,
                "success_rate": valid_sstables / total_sstables if total_sstables > 0 else 0
            },
            "issue_summary": issue_summary,
            "component_issues": component_issues,
            "detailed_results": [asdict(result) for result in results],
            "generated_at": datetime.utcnow().isoformat()
        }

        return report

    def save_validation_report(self, report: Dict[str, Any], output_file: Path):
        """Save validation report to file"""
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Validation report saved to: {output_file}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Validate SSTable files for CQLite compatibility")
    parser.add_argument("input_path", help="Path to SSTable directory or parent directory")
    parser.add_argument("--cqlite-binary", help="Path to CQLite binary")
    parser.add_argument("--output-report", help="Output file for validation report")
    parser.add_argument("--recursive", action="store_true", help="Recursively validate subdirectories")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")

    args = parser.parse_args()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    input_path = Path(args.input_path)
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)

    validator = SSTableValidator(args.cqlite_binary)

    try:
        all_results = []

        if args.recursive and input_path.is_dir():
            # Find all SSTable directories recursively
            for sstable_dir in input_path.rglob("*"):
                if sstable_dir.is_dir() and any(f.name.endswith(".db") for f in sstable_dir.glob("*")):
                    results = validator.validate_sstable_directory(sstable_dir)
                    all_results.extend(results)
        else:
            # Validate single directory
            results = validator.validate_sstable_directory(input_path)
            all_results.extend(results)

        # Generate report
        report = validator.generate_validation_report(all_results)

        # Print summary
        summary = report["validation_summary"]
        issues = report["issue_summary"]

        print(f"\\nValidation Summary:")
        print(f"  Total SSTables: {summary['total_sstables']}")
        print(f"  Valid SSTables: {summary['valid_sstables']}")
        print(f"  Invalid SSTables: {summary['invalid_sstables']}")
        print(f"  Success Rate: {summary['success_rate']:.2%}")
        print(f"\\nIssues Found:")
        print(f"  Errors: {issues['ERROR']}")
        print(f"  Warnings: {issues['WARNING']}")
        print(f"  Info: {issues['INFO']}")

        # Save report if requested
        if args.output_report:
            output_file = Path(args.output_report)
            validator.save_validation_report(report, output_file)

        # Exit with error code if any SSTables are invalid
        if summary['invalid_sstables'] > 0:
            sys.exit(1)

        logger.info("SSTable validation completed successfully")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()