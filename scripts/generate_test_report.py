#!/usr/bin/env python3
"""
Comprehensive Test Report Generator for CQLite CLI Testing Framework
Aggregates results from unit, integration, E2E, and performance tests
"""

import argparse
import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any
import datetime


class TestReportGenerator:
    def __init__(self, input_dir: str, output_file: str, coverage_threshold: float = 90.0):
        self.input_dir = Path(input_dir)
        self.output_file = output_file
        self.coverage_threshold = coverage_threshold
        self.results = {
            'unit_tests': [],
            'integration_tests': [],
            'e2e_tests': [],
            'performance_tests': [],
            'coverage': {},
            'summary': {}
        }

    def generate_report(self):
        """Generate comprehensive test report"""
        print("🧪 Generating comprehensive test report...")
        
        # Collect test results
        self._collect_unit_test_results()
        self._collect_integration_test_results()
        self._collect_e2e_test_results()
        self._collect_performance_test_results()
        self._collect_coverage_results()
        
        # Generate summary
        self._generate_summary()
        
        # Create HTML report
        self._create_html_report()
        
        print(f"✅ Test report generated: {self.output_file}")

    def _collect_unit_test_results(self):
        """Collect unit test results from JUnit XML files"""
        print("📋 Collecting unit test results...")
        
        unit_test_files = list(self.input_dir.glob("**/unit-test-results-*/junit.xml"))
        
        for test_file in unit_test_files:
            try:
                tree = ET.parse(test_file)
                root = tree.getroot()
                
                test_result = {
                    'file': str(test_file),
                    'platform': self._extract_platform_from_path(str(test_file)),
                    'tests': int(root.get('tests', 0)),
                    'failures': int(root.get('failures', 0)),
                    'errors': int(root.get('errors', 0)),
                    'time': float(root.get('time', 0)),
                    'test_cases': []
                }
                
                for testcase in root.findall('testcase'):
                    case = {
                        'name': testcase.get('name'),
                        'classname': testcase.get('classname'),
                        'time': float(testcase.get('time', 0)),
                        'status': 'passed'
                    }
                    
                    if testcase.find('failure') is not None:
                        case['status'] = 'failed'
                        case['failure'] = testcase.find('failure').text
                    elif testcase.find('error') is not None:
                        case['status'] = 'error'
                        case['error'] = testcase.find('error').text
                    
                    test_result['test_cases'].append(case)
                
                self.results['unit_tests'].append(test_result)
                
            except Exception as e:
                print(f"⚠️ Error parsing {test_file}: {e}")

    def _collect_integration_test_results(self):
        """Collect integration test results"""
        print("🔗 Collecting integration test results...")
        
        integration_files = list(self.input_dir.glob("**/integration-test-results/junit.xml"))
        
        for test_file in integration_files:
            try:
                tree = ET.parse(test_file)
                root = tree.getroot()
                
                test_result = {
                    'file': str(test_file),
                    'tests': int(root.get('tests', 0)),
                    'failures': int(root.get('failures', 0)),
                    'errors': int(root.get('errors', 0)),
                    'time': float(root.get('time', 0)),
                    'workflows': []
                }
                
                for testcase in root.findall('testcase'):
                    workflow = {
                        'name': testcase.get('name'),
                        'time': float(testcase.get('time', 0)),
                        'status': 'passed' if testcase.find('failure') is None else 'failed'
                    }
                    test_result['workflows'].append(workflow)
                
                self.results['integration_tests'].append(test_result)
                
            except Exception as e:
                print(f"⚠️ Error parsing integration test {test_file}: {e}")

    def _collect_e2e_test_results(self):
        """Collect end-to-end test results"""
        print("🌐 Collecting E2E test results...")
        
        e2e_files = list(self.input_dir.glob("**/e2e-test-results/*.log"))
        
        for log_file in e2e_files:
            try:
                with open(log_file, 'r') as f:
                    content = f.read()
                
                # Parse E2E test log for scenarios
                scenarios = self._parse_e2e_scenarios(content)
                
                test_result = {
                    'file': str(log_file),
                    'scenarios': scenarios,
                    'total_scenarios': len(scenarios),
                    'passed_scenarios': len([s for s in scenarios if s['status'] == 'passed']),
                    'failed_scenarios': len([s for s in scenarios if s['status'] == 'failed'])
                }
                
                self.results['e2e_tests'].append(test_result)
                
            except Exception as e:
                print(f"⚠️ Error parsing E2E test {log_file}: {e}")

    def _collect_performance_test_results(self):
        """Collect performance test results"""
        print("⚡ Collecting performance test results...")
        
        perf_files = list(self.input_dir.glob("**/performance-test-results/*_benchmark.json"))
        
        for perf_file in perf_files:
            try:
                with open(perf_file, 'r') as f:
                    data = json.load(f)
                
                benchmark_name = perf_file.stem.replace('_benchmark', '')
                
                test_result = {
                    'name': benchmark_name,
                    'file': str(perf_file),
                    'results': data.get('results', []),
                    'summary': self._summarize_performance_data(data)
                }
                
                self.results['performance_tests'].append(test_result)
                
            except Exception as e:
                print(f"⚠️ Error parsing performance test {perf_file}: {e}")

    def _collect_coverage_results(self):
        """Collect code coverage results"""
        print("📊 Collecting coverage results...")
        
        lcov_files = list(self.input_dir.glob("**/lcov.info"))
        
        if lcov_files:
            try:
                # Parse LCOV file for coverage data
                coverage_data = self._parse_lcov_file(lcov_files[0])
                self.results['coverage'] = coverage_data
            except Exception as e:
                print(f"⚠️ Error parsing coverage file: {e}")

    def _generate_summary(self):
        """Generate test summary statistics"""
        print("📈 Generating test summary...")
        
        # Unit test summary
        unit_total = sum(test['tests'] for test in self.results['unit_tests'])
        unit_failures = sum(test['failures'] for test in self.results['unit_tests'])
        unit_errors = sum(test['errors'] for test in self.results['unit_tests'])
        unit_passed = unit_total - unit_failures - unit_errors
        
        # Integration test summary
        integration_total = sum(test['tests'] for test in self.results['integration_tests'])
        integration_failures = sum(test['failures'] for test in self.results['integration_tests'])
        integration_passed = integration_total - integration_failures
        
        # E2E test summary
        e2e_total = sum(test['total_scenarios'] for test in self.results['e2e_tests'])
        e2e_passed = sum(test['passed_scenarios'] for test in self.results['e2e_tests'])
        e2e_failed = sum(test['failed_scenarios'] for test in self.results['e2e_tests'])
        
        # Coverage summary
        coverage_percentage = self.results['coverage'].get('line_coverage', 0)
        coverage_status = 'passed' if coverage_percentage >= self.coverage_threshold else 'failed'
        
        # Overall status
        overall_passed = (
            unit_failures == 0 and unit_errors == 0 and
            integration_failures == 0 and
            e2e_failed == 0 and
            coverage_percentage >= self.coverage_threshold
        )
        
        self.results['summary'] = {
            'timestamp': datetime.datetime.now().isoformat(),
            'overall_status': 'passed' if overall_passed else 'failed',
            'unit_tests': {
                'total': unit_total,
                'passed': unit_passed,
                'failed': unit_failures,
                'errors': unit_errors,
                'success_rate': (unit_passed / unit_total * 100) if unit_total > 0 else 0
            },
            'integration_tests': {
                'total': integration_total,
                'passed': integration_passed,
                'failed': integration_failures,
                'success_rate': (integration_passed / integration_total * 100) if integration_total > 0 else 0
            },
            'e2e_tests': {
                'total': e2e_total,
                'passed': e2e_passed,
                'failed': e2e_failed,
                'success_rate': (e2e_passed / e2e_total * 100) if e2e_total > 0 else 0
            },
            'coverage': {
                'percentage': coverage_percentage,
                'status': coverage_status,
                'threshold': self.coverage_threshold
            },
            'performance': {
                'benchmarks_run': len(self.results['performance_tests']),
                'regressions_detected': self._count_performance_regressions()
            }
        }

    def _create_html_report(self):
        """Create HTML test report"""
        html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CQLite CLI - Comprehensive Test Report</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px 8px 0 0; }
        .header h1 { margin: 0; font-size: 2.5em; }
        .header .subtitle { margin-top: 10px; opacity: 0.9; font-size: 1.1em; }
        .summary { padding: 30px; border-bottom: 1px solid #eee; }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-top: 20px; }
        .summary-card { background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }
        .summary-card h3 { margin: 0 0 10px 0; color: #333; }
        .summary-card .number { font-size: 2em; font-weight: bold; margin: 10px 0; }
        .passed { color: #28a745; }
        .failed { color: #dc3545; }
        .warning { color: #ffc107; }
        .section { padding: 30px; border-bottom: 1px solid #eee; }
        .section h2 { margin-top: 0; color: #333; }
        .test-grid { display: grid; gap: 20px; }
        .test-item { background: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #28a745; }
        .test-item.failed { border-left-color: #dc3545; }
        .test-item h4 { margin: 0 0 10px 0; }
        .test-item .stats { display: flex; gap: 20px; margin-top: 10px; }
        .test-item .stat { text-align: center; }
        .test-item .stat .number { font-weight: bold; font-size: 1.2em; }
        .test-item .stat .label { font-size: 0.9em; color: #666; }
        .coverage-bar { background: #e9ecef; height: 20px; border-radius: 10px; overflow: hidden; margin: 10px 0; }
        .coverage-fill { background: linear-gradient(90deg, #28a745, #20c997); height: 100%; transition: width 0.3s ease; }
        .performance-item { background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0; }
        .performance-item h5 { margin: 0 0 10px 0; }
        .performance-metric { display: flex; justify-content: space-between; margin: 5px 0; }
        .footer { padding: 20px 30px; background: #f8f9fa; border-radius: 0 0 8px 8px; color: #666; text-align: center; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧪 CQLite CLI Test Report</h1>
            <div class="subtitle">Comprehensive Testing Framework Results</div>
            <div class="subtitle">Generated: {timestamp}</div>
        </div>

        <div class="summary">
            <h2>📊 Test Summary</h2>
            <div class="summary-grid">
                <div class="summary-card">
                    <h3>Overall Status</h3>
                    <div class="number {overall_status_class}">{overall_status_text}</div>
                </div>
                <div class="summary-card">
                    <h3>Unit Tests</h3>
                    <div class="number {unit_status_class}">{unit_passed}/{unit_total}</div>
                    <div>{unit_success_rate:.1f}% Success Rate</div>
                </div>
                <div class="summary-card">
                    <h3>Integration Tests</h3>
                    <div class="number {integration_status_class}">{integration_passed}/{integration_total}</div>
                    <div>{integration_success_rate:.1f}% Success Rate</div>
                </div>
                <div class="summary-card">
                    <h3>E2E Tests</h3>
                    <div class="number {e2e_status_class}">{e2e_passed}/{e2e_total}</div>
                    <div>{e2e_success_rate:.1f}% Success Rate</div>
                </div>
                <div class="summary-card">
                    <h3>Code Coverage</h3>
                    <div class="number {coverage_status_class}">{coverage_percentage:.1f}%</div>
                    <div class="coverage-bar">
                        <div class="coverage-fill" style="width: {coverage_percentage}%"></div>
                    </div>
                    <div>Threshold: {coverage_threshold}%</div>
                </div>
            </div>
        </div>

        {unit_tests_section}
        {integration_tests_section}
        {e2e_tests_section}
        {performance_tests_section}
        {coverage_section}

        <div class="footer">
            <p>Generated by CQLite CLI Comprehensive Testing Framework</p>
            <p>Issue #20: Implement comprehensive CLI testing framework</p>
        </div>
    </div>
</body>
</html>
        """

        # Format the template with data
        summary = self.results['summary']
        
        html_content = html_template.format(
            timestamp=summary['timestamp'],
            overall_status_class='passed' if summary['overall_status'] == 'passed' else 'failed',
            overall_status_text='✅ PASSED' if summary['overall_status'] == 'passed' else '❌ FAILED',
            unit_passed=summary['unit_tests']['passed'],
            unit_total=summary['unit_tests']['total'],
            unit_success_rate=summary['unit_tests']['success_rate'],
            unit_status_class='passed' if summary['unit_tests']['failed'] == 0 else 'failed',
            integration_passed=summary['integration_tests']['passed'],
            integration_total=summary['integration_tests']['total'],
            integration_success_rate=summary['integration_tests']['success_rate'],
            integration_status_class='passed' if summary['integration_tests']['failed'] == 0 else 'failed',
            e2e_passed=summary['e2e_tests']['passed'],
            e2e_total=summary['e2e_tests']['total'],
            e2e_success_rate=summary['e2e_tests']['success_rate'],
            e2e_status_class='passed' if summary['e2e_tests']['failed'] == 0 else 'failed',
            coverage_percentage=summary['coverage']['percentage'],
            coverage_threshold=summary['coverage']['threshold'],
            coverage_status_class=summary['coverage']['status'],
            unit_tests_section=self._generate_unit_tests_section(),
            integration_tests_section=self._generate_integration_tests_section(),
            e2e_tests_section=self._generate_e2e_tests_section(),
            performance_tests_section=self._generate_performance_tests_section(),
            coverage_section=self._generate_coverage_section()
        )

        with open(self.output_file, 'w') as f:
            f.write(html_content)

    # Helper methods for HTML sections
    def _generate_unit_tests_section(self):
        if not self.results['unit_tests']:
            return ""
        
        return """
        <div class="section">
            <h2>📋 Unit Tests</h2>
            <div class="test-grid">
                <!-- Unit test details would go here -->
            </div>
        </div>
        """

    def _generate_integration_tests_section(self):
        if not self.results['integration_tests']:
            return ""
        
        return """
        <div class="section">
            <h2>🔗 Integration Tests</h2>
            <div class="test-grid">
                <!-- Integration test details would go here -->
            </div>
        </div>
        """

    def _generate_e2e_tests_section(self):
        if not self.results['e2e_tests']:
            return ""
        
        return """
        <div class="section">
            <h2>🌐 End-to-End Tests</h2>
            <div class="test-grid">
                <!-- E2E test details would go here -->
            </div>
        </div>
        """

    def _generate_performance_tests_section(self):
        if not self.results['performance_tests']:
            return ""
        
        return """
        <div class="section">
            <h2>⚡ Performance Tests</h2>
            <div class="test-grid">
                <!-- Performance test details would go here -->
            </div>
        </div>
        """

    def _generate_coverage_section(self):
        return """
        <div class="section">
            <h2>📊 Code Coverage</h2>
            <div class="test-grid">
                <!-- Coverage details would go here -->
            </div>
        </div>
        """

    # Helper methods
    def _extract_platform_from_path(self, path: str) -> str:
        if 'ubuntu' in path:
            return 'ubuntu'
        elif 'windows' in path:
            return 'windows'
        elif 'macos' in path:
            return 'macos'
        return 'unknown'

    def _parse_e2e_scenarios(self, content: str) -> List[Dict]:
        # Simple parser for E2E scenarios
        scenarios = []
        lines = content.split('\n')
        
        for line in lines:
            if 'Scenario:' in line:
                scenarios.append({
                    'name': line.split('Scenario:')[1].strip(),
                    'status': 'passed' if '✅' in line else 'failed'
                })
        
        return scenarios

    def _summarize_performance_data(self, data: Dict) -> Dict:
        # Summarize performance benchmark data
        return {
            'mean_time': data.get('results', [{}])[0].get('mean', 0),
            'std_dev': data.get('results', [{}])[0].get('stddev', 0)
        }

    def _parse_lcov_file(self, lcov_file: Path) -> Dict:
        # Parse LCOV file for coverage data
        with open(lcov_file, 'r') as f:
            content = f.read()
        
        # Simple LCOV parser
        lines_found = 0
        lines_hit = 0
        
        for line in content.split('\n'):
            if line.startswith('LF:'):
                lines_found += int(line.split(':')[1])
            elif line.startswith('LH:'):
                lines_hit += int(line.split(':')[1])
        
        line_coverage = (lines_hit / lines_found * 100) if lines_found > 0 else 0
        
        return {
            'line_coverage': line_coverage,
            'lines_found': lines_found,
            'lines_hit': lines_hit
        }

    def _count_performance_regressions(self) -> int:
        # Count performance regressions
        return 0  # Placeholder


def main():
    parser = argparse.ArgumentParser(description='Generate comprehensive test report')
    parser.add_argument('--input', required=True, help='Input directory with test results')
    parser.add_argument('--output', required=True, help='Output HTML file')
    parser.add_argument('--coverage-threshold', type=float, default=90.0, help='Coverage threshold')
    
    args = parser.parse_args()
    
    generator = TestReportGenerator(args.input, args.output, args.coverage_threshold)
    generator.generate_report()


if __name__ == '__main__':
    main()