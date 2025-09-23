#!/usr/bin/env python3
"""
CI/CD Integration for CQLite Test Data Pipeline

This script provides CI/CD integration for automated validation of test data
and regression testing. It integrates with GitHub Actions, Jenkins, and other
CI systems to ensure data quality and catch regressions early.

Features:
- Pre-commit hooks for data validation
- PR validation checks
- Automated regression testing
- Performance regression detection
- Quality gate enforcement
- Artifact publishing
"""

import os
import sys
import json
import yaml
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import logging
import argparse
import tempfile

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CIConfig:
    """CI/CD configuration"""
    base_dir: Path
    cqlite_binary: Optional[Path]
    github_token: Optional[str]
    slack_webhook: Optional[str]
    artifact_bucket: Optional[str]
    quality_gates: Dict[str, Any]
    performance_thresholds: Dict[str, float]

class CIIntegration:
    """CI/CD integration manager"""

    def __init__(self, config: CIConfig):
        self.config = config
        self.reports_dir = config.base_dir / "ci-reports"
        self.artifacts_dir = config.base_dir / "ci-artifacts"

        # Create directories
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def run_pr_validation(self, pr_number: Optional[int] = None) -> Dict[str, Any]:
        """Run validation checks for pull request"""
        logger.info(f"Running PR validation for PR #{pr_number}")

        validation_results = {
            "pr_number": pr_number,
            "started_at": datetime.utcnow().isoformat(),
            "checks": {},
            "overall_status": "pending"
        }

        try:
            # Run test data validation
            validation_results["checks"]["data_validation"] = self._run_data_validation_check()

            # Run regression tests
            validation_results["checks"]["regression_tests"] = self._run_regression_test_check()

            # Run performance tests
            validation_results["checks"]["performance_tests"] = self._run_performance_check()

            # Run quality gates
            validation_results["checks"]["quality_gates"] = self._run_quality_gate_check()

            # Determine overall status
            validation_results["overall_status"] = self._determine_overall_status(validation_results["checks"])

            # Generate summary
            validation_results["summary"] = self._generate_validation_summary(validation_results["checks"])

            validation_results["completed_at"] = datetime.utcnow().isoformat()

            # Save report
            self._save_pr_validation_report(validation_results)

            # Update PR status if GitHub integration is available
            if self.config.github_token and pr_number:
                self._update_github_pr_status(pr_number, validation_results)

            # Send notifications
            self._send_notifications(validation_results)

            return validation_results

        except Exception as e:
            logger.error(f"PR validation failed: {e}")
            validation_results["overall_status"] = "error"
            validation_results["error"] = str(e)
            validation_results["completed_at"] = datetime.utcnow().isoformat()
            return validation_results

    def _run_data_validation_check(self) -> Dict[str, Any]:
        """Run data validation check"""
        logger.info("Running data validation check")

        try:
            # Import and run validator
            sys.path.append(str(self.config.base_dir / "scripts"))
            from validate_sstables import SSTableValidator

            validator = SSTableValidator(
                str(self.config.cqlite_binary) if self.config.cqlite_binary else None
            )

            datasets_dir = self.config.base_dir / "test-data" / "datasets"
            results = validator.validate_sstable_directory(datasets_dir)
            report = validator.generate_validation_report(results)

            # Check against quality gates
            error_count = report["issue_summary"]["ERROR"]
            warning_count = report["issue_summary"]["WARNING"]

            max_errors = self.config.quality_gates.get("max_errors", 0)
            max_warnings = self.config.quality_gates.get("max_warnings", 10)

            passed = error_count <= max_errors and warning_count <= max_warnings

            return {
                "status": "passed" if passed else "failed",
                "error_count": error_count,
                "warning_count": warning_count,
                "max_errors": max_errors,
                "max_warnings": max_warnings,
                "details": report
            }

        except Exception as e:
            logger.error(f"Data validation check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    def _run_regression_test_check(self) -> Dict[str, Any]:
        """Run regression test check"""
        logger.info("Running regression test check")

        try:
            # Import and run pipeline manager
            from data_pipeline_manager import DataPipelineManager, load_config

            config = load_config()
            manager = DataPipelineManager(config)

            regression_results = manager.run_regression_tests()

            failed_tests = regression_results.get("failed_tests", 0)
            total_tests = regression_results.get("total_tests", 0)
            success_rate = regression_results.get("success_rate", 0)

            min_success_rate = self.config.quality_gates.get("min_regression_success_rate", 0.95)
            passed = success_rate >= min_success_rate

            return {
                "status": "passed" if passed else "failed",
                "failed_tests": failed_tests,
                "total_tests": total_tests,
                "success_rate": success_rate,
                "min_success_rate": min_success_rate,
                "details": regression_results
            }

        except Exception as e:
            logger.error(f"Regression test check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    def _run_performance_check(self) -> Dict[str, Any]:
        """Run performance check"""
        logger.info("Running performance check")

        try:
            # Load latest benchmark results
            benchmarks_dir = self.config.base_dir / "test-data" / "benchmarks"
            latest_file = benchmarks_dir / "latest_benchmarks.json"

            if not latest_file.exists():
                return {
                    "status": "skipped",
                    "reason": "No benchmark data available"
                }

            with open(latest_file, 'r') as f:
                benchmark_data = json.load(f)

            # Check performance against thresholds
            performance_issues = []

            for benchmark in benchmark_data.get("benchmarks", []):
                dataset_name = benchmark["dataset"]
                metrics = benchmark.get("benchmarks", {})

                # Check read time
                read_time = metrics.get("read_time_seconds")
                if read_time is not None:
                    max_read_time = self.config.performance_thresholds.get("max_read_time_seconds", 30.0)
                    if read_time > max_read_time:
                        performance_issues.append({
                            "dataset": dataset_name,
                            "issue": "read_time_exceeded",
                            "actual": read_time,
                            "threshold": max_read_time
                        })

                # Check throughput
                throughput = metrics.get("read_throughput_lines_per_second")
                if throughput is not None:
                    min_throughput = self.config.performance_thresholds.get("min_throughput_lines_per_second", 1000.0)
                    if throughput < min_throughput:
                        performance_issues.append({
                            "dataset": dataset_name,
                            "issue": "throughput_below_threshold",
                            "actual": throughput,
                            "threshold": min_throughput
                        })

            passed = len(performance_issues) == 0

            return {
                "status": "passed" if passed else "failed",
                "performance_issues": performance_issues,
                "total_benchmarks": len(benchmark_data.get("benchmarks", [])),
                "details": benchmark_data
            }

        except Exception as e:
            logger.error(f"Performance check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    def _run_quality_gate_check(self) -> Dict[str, Any]:
        """Run quality gate check"""
        logger.info("Running quality gate check")

        try:
            # Load dataset registry
            versions_dir = self.config.base_dir / "test-data" / "versions"
            registry_file = versions_dir / "registry.json"

            if not registry_file.exists():
                return {
                    "status": "failed",
                    "reason": "No dataset registry found"
                }

            with open(registry_file, 'r') as f:
                registry = json.load(f)

            # Check minimum dataset count
            min_datasets = self.config.quality_gates.get("min_datasets", 10)
            dataset_count = len(registry)

            if dataset_count < min_datasets:
                return {
                    "status": "failed",
                    "reason": f"Insufficient datasets: {dataset_count} < {min_datasets}",
                    "dataset_count": dataset_count,
                    "min_datasets": min_datasets
                }

            # Check for recent generation
            max_age_hours = self.config.quality_gates.get("max_dataset_age_hours", 168)  # 1 week

            old_datasets = []
            for name, versions in registry.items():
                if not versions:
                    continue

                latest_version = versions[-1]
                created_at = datetime.fromisoformat(latest_version["created_at"])
                age_hours = (datetime.utcnow() - created_at).total_seconds() / 3600

                if age_hours > max_age_hours:
                    old_datasets.append({
                        "dataset": name,
                        "age_hours": age_hours,
                        "created_at": latest_version["created_at"]
                    })

            max_old_datasets = self.config.quality_gates.get("max_old_datasets", 2)

            if len(old_datasets) > max_old_datasets:
                return {
                    "status": "failed",
                    "reason": f"Too many old datasets: {len(old_datasets)} > {max_old_datasets}",
                    "old_datasets": old_datasets,
                    "max_old_datasets": max_old_datasets
                }

            return {
                "status": "passed",
                "dataset_count": dataset_count,
                "old_datasets": len(old_datasets)
            }

        except Exception as e:
            logger.error(f"Quality gate check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    def _determine_overall_status(self, checks: Dict[str, Any]) -> str:
        """Determine overall validation status"""
        statuses = [check.get("status", "error") for check in checks.values()]

        if "error" in statuses:
            return "error"
        elif "failed" in statuses:
            return "failed"
        elif all(status in ["passed", "skipped"] for status in statuses):
            return "passed"
        else:
            return "pending"

    def _generate_validation_summary(self, checks: Dict[str, Any]) -> Dict[str, Any]:
        """Generate validation summary"""
        total_checks = len(checks)
        passed_checks = sum(1 for check in checks.values() if check.get("status") == "passed")
        failed_checks = sum(1 for check in checks.values() if check.get("status") == "failed")
        error_checks = sum(1 for check in checks.values() if check.get("status") == "error")
        skipped_checks = sum(1 for check in checks.values() if check.get("status") == "skipped")

        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "error_checks": error_checks,
            "skipped_checks": skipped_checks,
            "success_rate": passed_checks / total_checks if total_checks > 0 else 0
        }

    def _save_pr_validation_report(self, validation_results: Dict[str, Any]):
        """Save PR validation report"""
        pr_number = validation_results.get("pr_number", "unknown")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        report_file = self.reports_dir / f"pr_validation_{pr_number}_{timestamp}.json"

        with open(report_file, 'w') as f:
            json.dump(validation_results, f, indent=2)

        # Also save as latest for this PR
        if pr_number != "unknown":
            latest_file = self.reports_dir / f"pr_validation_{pr_number}_latest.json"
            with open(latest_file, 'w') as f:
                json.dump(validation_results, f, indent=2)

        logger.info(f"PR validation report saved to: {report_file}")

    def _update_github_pr_status(self, pr_number: int, validation_results: Dict[str, Any]):
        """Update GitHub PR status"""
        if not self.config.github_token:
            return

        try:
            import requests

            # This would need the actual GitHub repo info
            # For now, just log what would be done
            status = validation_results["overall_status"]
            summary = validation_results.get("summary", {})

            logger.info(f"Would update GitHub PR #{pr_number} with status: {status}")
            logger.info(f"Summary: {summary}")

            # Actual implementation would use GitHub API
            # headers = {"Authorization": f"token {self.config.github_token}"}
            # status_data = {
            #     "state": "success" if status == "passed" else "failure",
            #     "context": "cqlite/test-data-validation",
            #     "description": f"Test data validation: {status}"
            # }
            # requests.post(f"https://api.github.com/repos/{repo}/statuses/{commit_sha}",
            #               json=status_data, headers=headers)

        except Exception as e:
            logger.warning(f"Failed to update GitHub PR status: {e}")

    def _send_notifications(self, validation_results: Dict[str, Any]):
        """Send notifications about validation results"""
        status = validation_results["overall_status"]

        if status in ["failed", "error"] and self.config.slack_webhook:
            self._send_slack_notification(validation_results)

    def _send_slack_notification(self, validation_results: Dict[str, Any]):
        """Send Slack notification"""
        try:
            import requests

            status = validation_results["overall_status"]
            pr_number = validation_results.get("pr_number", "unknown")
            summary = validation_results.get("summary", {})

            message = {
                "text": f"CQLite Test Data Validation {status.upper()}",
                "attachments": [
                    {
                        "color": "danger" if status in ["failed", "error"] else "good",
                        "fields": [
                            {"title": "PR Number", "value": str(pr_number), "short": True},
                            {"title": "Status", "value": status, "short": True},
                            {"title": "Passed Checks", "value": str(summary.get("passed_checks", 0)), "short": True},
                            {"title": "Failed Checks", "value": str(summary.get("failed_checks", 0)), "short": True}
                        ]
                    }
                ]
            }

            response = requests.post(self.config.slack_webhook, json=message)
            response.raise_for_status()

            logger.info("Slack notification sent successfully")

        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")

    def setup_pre_commit_hooks(self) -> bool:
        """Setup pre-commit hooks"""
        logger.info("Setting up pre-commit hooks")

        try:
            hooks_dir = self.config.base_dir / ".git" / "hooks"
            hooks_dir.mkdir(parents=True, exist_ok=True)

            # Create pre-commit hook script
            pre_commit_script = hooks_dir / "pre-commit"

            hook_content = f"""#!/bin/bash
# CQLite Test Data Pre-commit Hook

echo "Running CQLite test data validation..."

# Run quick validation
python3 "{self.config.base_dir}/scripts/ci_integration.py" validate-quick

if [ $? -ne 0 ]; then
    echo "Test data validation failed. Commit aborted."
    echo "Run 'python3 scripts/ci_integration.py validate-quick' to see details."
    exit 1
fi

echo "Test data validation passed."
exit 0
"""

            with open(pre_commit_script, 'w') as f:
                f.write(hook_content)

            # Make executable
            os.chmod(pre_commit_script, 0o755)

            logger.info("Pre-commit hook installed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to setup pre-commit hooks: {e}")
            return False

    def run_quick_validation(self) -> Dict[str, Any]:
        """Run quick validation for pre-commit checks"""
        logger.info("Running quick validation")

        validation_results = {
            "started_at": datetime.utcnow().isoformat(),
            "checks": {},
            "overall_status": "pending"
        }

        try:
            # Quick file existence check
            validation_results["checks"]["file_existence"] = self._check_required_files()

            # Quick format validation
            validation_results["checks"]["format_validation"] = self._check_file_formats()

            # Determine overall status
            validation_results["overall_status"] = self._determine_overall_status(validation_results["checks"])
            validation_results["completed_at"] = datetime.utcnow().isoformat()

            return validation_results

        except Exception as e:
            logger.error(f"Quick validation failed: {e}")
            validation_results["overall_status"] = "error"
            validation_results["error"] = str(e)
            return validation_results

    def _check_required_files(self) -> Dict[str, Any]:
        """Check for required files"""
        required_files = [
            "test-data/datasets/metadata.yml",
            "test-data/datasets/references.yml"
        ]

        missing_files = []
        for file_path in required_files:
            full_path = self.config.base_dir / file_path
            if not full_path.exists():
                missing_files.append(file_path)

        return {
            "status": "passed" if not missing_files else "failed",
            "missing_files": missing_files,
            "checked_files": required_files
        }

    def _check_file_formats(self) -> Dict[str, Any]:
        """Check file format validity"""
        format_issues = []

        # Check YAML files
        yaml_files = [
            "test-data/datasets/metadata.yml",
            "test-data/datasets/references.yml"
        ]

        for yaml_file in yaml_files:
            full_path = self.config.base_dir / yaml_file
            if full_path.exists():
                try:
                    with open(full_path, 'r') as f:
                        yaml.safe_load(f)
                except yaml.YAMLError as e:
                    format_issues.append({
                        "file": yaml_file,
                        "error": str(e)
                    })

        # Check JSON files
        json_files = list((self.config.base_dir / "test-data").rglob("*.json"))
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    json.load(f)
            except json.JSONDecodeError as e:
                format_issues.append({
                    "file": str(json_file.relative_to(self.config.base_dir)),
                    "error": str(e)
                })

        return {
            "status": "passed" if not format_issues else "failed",
            "format_issues": format_issues,
            "checked_files": len(yaml_files) + len(json_files)
        }

    def generate_ci_config(self, ci_type: str) -> str:
        """Generate CI configuration file"""
        if ci_type == "github":
            return self._generate_github_actions_config()
        elif ci_type == "jenkins":
            return self._generate_jenkins_config()
        else:
            raise ValueError(f"Unsupported CI type: {ci_type}")

    def _generate_github_actions_config(self) -> str:
        """Generate GitHub Actions workflow"""
        config = f"""name: CQLite Test Data Validation

on:
  pull_request:
    paths:
      - 'test-data/**'
      - 'scripts/**'
      - 'cqlite-core/**'
  push:
    branches: [ main ]
  schedule:
    # Run daily at 2 AM UTC
    - cron: '0 2 * * *'

jobs:
  validate-test-data:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pyyaml requests

    - name: Build CQLite
      run: |
        cargo build --release

    - name: Validate test data
      run: |
        python3 scripts/ci_integration.py pr-validation \\
          --cqlite-binary ./target/release/cqlite
      env:
        GITHUB_TOKEN: ${{{{ secrets.GITHUB_TOKEN }}}}
        SLACK_WEBHOOK: ${{{{ secrets.SLACK_WEBHOOK }}}}

    - name: Upload validation reports
      uses: actions/upload-artifact@v3
      if: always()
      with:
        name: validation-reports
        path: ci-reports/

    - name: Upload test artifacts
      uses: actions/upload-artifact@v3
      if: always()
      with:
        name: test-artifacts
        path: ci-artifacts/

  regression-tests:
    runs-on: ubuntu-latest
    needs: validate-test-data

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pyyaml requests

    - name: Build CQLite
      run: |
        cargo build --release

    - name: Run regression tests
      run: |
        python3 scripts/data_pipeline_manager.py regression \\
          --config ci-config.yml

    - name: Upload regression reports
      uses: actions/upload-artifact@v3
      if: always()
      with:
        name: regression-reports
        path: test-data/reports/
"""
        return config

    def _generate_jenkins_config(self) -> str:
        """Generate Jenkins pipeline configuration"""
        config = f"""pipeline {{
    agent any

    environment {{
        CQLITE_BINARY = './target/release/cqlite'
    }}

    triggers {{
        cron('H 2 * * *')  // Daily at 2 AM
    }}

    stages {{
        stage('Checkout') {{
            steps {{
                checkout scm
            }}
        }}

        stage('Build CQLite') {{
            steps {{
                sh 'cargo build --release'
            }}
        }}

        stage('Validate Test Data') {{
            steps {{
                sh '''
                    python3 scripts/ci_integration.py pr-validation \\
                        --cqlite-binary $CQLITE_BINARY
                '''
            }}
            post {{
                always {{
                    archiveArtifacts artifacts: 'ci-reports/**/*', fingerprint: true
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'ci-reports',
                        reportFiles: '*.html',
                        reportName: 'Validation Report'
                    ])
                }}
            }}
        }}

        stage('Regression Tests') {{
            steps {{
                sh '''
                    python3 scripts/data_pipeline_manager.py regression \\
                        --config ci-config.yml
                '''
            }}
            post {{
                always {{
                    archiveArtifacts artifacts: 'test-data/reports/**/*', fingerprint: true
                }}
            }}
        }}
    }}

    post {{
        failure {{
            emailext (
                subject: "CQLite Test Data Validation Failed: ${{env.JOB_NAME}} - ${{env.BUILD_NUMBER}}",
                body: "Test data validation failed. Please check the build logs and reports.",
                to: "${{env.CHANGE_AUTHOR_EMAIL}}"
            )
        }}
    }}
}}"""
        return config

def load_ci_config(config_file: Optional[Path] = None) -> CIConfig:
    """Load CI configuration"""
    if config_file and config_file.exists():
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
    else:
        config_data = {}

    return CIConfig(
        base_dir=Path(config_data.get("base_dir", ".")),
        cqlite_binary=Path(config_data["cqlite_binary"]) if config_data.get("cqlite_binary") else None,
        github_token=os.environ.get("GITHUB_TOKEN") or config_data.get("github_token"),
        slack_webhook=os.environ.get("SLACK_WEBHOOK") or config_data.get("slack_webhook"),
        artifact_bucket=config_data.get("artifact_bucket"),
        quality_gates=config_data.get("quality_gates", {
            "max_errors": 0,
            "max_warnings": 10,
            "min_datasets": 10,
            "max_dataset_age_hours": 168,
            "max_old_datasets": 2,
            "min_regression_success_rate": 0.95
        }),
        performance_thresholds=config_data.get("performance_thresholds", {
            "max_read_time_seconds": 30.0,
            "min_throughput_lines_per_second": 1000.0
        })
    )

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="CI/CD integration for CQLite test data pipeline")
    parser.add_argument("command", choices=[
        "pr-validation", "validate-quick", "setup-hooks", "generate-config"
    ], help="Command to execute")
    parser.add_argument("--config", type=Path, help="Configuration file")
    parser.add_argument("--pr-number", type=int, help="Pull request number")
    parser.add_argument("--ci-type", choices=["github", "jenkins"], help="CI system type")
    parser.add_argument("--cqlite-binary", type=Path, help="Path to CQLite binary")

    args = parser.parse_args()

    config = load_ci_config(args.config)
    if args.cqlite_binary:
        config.cqlite_binary = args.cqlite_binary

    ci = CIIntegration(config)

    try:
        if args.command == "pr-validation":
            result = ci.run_pr_validation(args.pr_number)
            print(json.dumps(result, indent=2))

            if result["overall_status"] in ["failed", "error"]:
                sys.exit(1)

        elif args.command == "validate-quick":
            result = ci.run_quick_validation()
            print(json.dumps(result, indent=2))

            if result["overall_status"] in ["failed", "error"]:
                sys.exit(1)

        elif args.command == "setup-hooks":
            success = ci.setup_pre_commit_hooks()
            if not success:
                sys.exit(1)

        elif args.command == "generate-config":
            if not args.ci_type:
                print("--ci-type is required for generate-config command")
                sys.exit(1)

            config_content = ci.generate_ci_config(args.ci_type)
            print(config_content)

        logger.info(f"CI command '{args.command}' completed successfully")

    except Exception as e:
        logger.error(f"CI command '{args.command}' failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()