# CQLite Documentation Index

This document provides a comprehensive index of all documentation in the CQLite project, organized by category and purpose.

## 📁 Documentation Structure Overview

```
docs/
├── DOCUMENTATION_INDEX.md          # This file - comprehensive documentation catalog
├── README.md                       # Main documentation entry point
├── sstables-definitive-guide/      # PRIMARY REFERENCE: Cassandra 5.0 SSTable format
├── architecture/                   # Design notes and architecture decisions
├── development/                    # Development guides and specs
├── read-path/                      # Read path walkthrough series
├── technical/                      # Technical specifications and formats
├── testing/                        # Testing strategies and guides
├── user-guides/                    # End-user documentation
├── m5/                             # M5 write-support implementation notes
├── reports/                        # Project reports and analysis
├── plans/                          # Design plans and proposals
├── ci/                             # CI policy documentation
└── archive/                        # Historical documents (issue investigations)
```

## 📖 Primary Reference

| Document | Description | Status |
|----------|-------------|--------|
| [sstables-definitive-guide/README.md](sstables-definitive-guide/README.md) | The SSTable Definitive Guide — single source of truth for the Cassandra 5.0 format (Data.db, Index.db, BTI, encoding, known limitations) | Active |

## 🚀 Quick Start Documentation

| Document | Purpose | Target Audience |
|----------|---------|-----------------|
| [README.md](README.md) | Main documentation overview | All users |
| [user-guides/quick-start.md](user-guides/quick-start.md) | Getting started guide | New users |
| [user-guides/installation.md](user-guides/installation.md) | Installation instructions | Users |
| [user-guides/cli.md](user-guides/cli.md) | CLI usage guide | CLI users |
| [using-cqlite-core-as-a-dependency.md](using-cqlite-core-as-a-dependency.md) | Embedding the library in Rust projects | Rust developers |

## 🔧 Development Documentation

### Architecture & Design
| Document | Description | Status |
|----------|-------------|--------|
| [development/PRD.md](development/PRD.md) | Product Requirements Document | Active |
| [development/DEVELOPMENT.md](development/DEVELOPMENT.md) | Development setup and workflows | Active |
| [development/contributing.md](development/contributing.md) | Contribution guidelines | Active |
| [development/rust_developer_guide.md](development/rust_developer_guide.md) | Project Rust conventions and patterns | Active |
| [development/RELEASING.md](development/RELEASING.md) | Release process and checklist | Active |
| [development/ISSUE_EXECUTION_PLAYBOOK.md](development/ISSUE_EXECUTION_PLAYBOOK.md) | Issue workflow playbook | Active |
| [architecture/parser-overview.md](architecture/parser-overview.md) | How the SSTable parsers fit together | Active |
| [architecture/SCHEMA_PROPAGATION_DECISION.md](architecture/SCHEMA_PROPAGATION_DECISION.md) | Schema propagation design decision | Active |
| [architecture/sstabledump_parity_test_architecture.md](architecture/sstabledump_parity_test_architecture.md) | Parity test architecture | Active |

### Milestone Specs
| Document | Description | Status |
|----------|-------------|--------|
| [development/M2_CLI_SPEC.md](development/M2_CLI_SPEC.md) | M2 CLI specification | Complete |
| [development/M4_spec.md](development/M4_spec.md) | M4 Python bindings specification | Complete |
| [write-support.md](write-support.md) | M5 write support overview | Active |
| [write-engine-api.md](write-engine-api.md) | Write engine API reference | Active |
| [write-support-limitations.md](write-support-limitations.md) | Write support limitations | Active |

### Migration & Upgrades
| Document | Description | Status |
|----------|-------------|--------|
| [development/MIGRATION_GUIDE.md](development/MIGRATION_GUIDE.md) | Migration procedures | Active |

## 📚 Read Path Walkthrough

| Document | Description |
|----------|-------------|
| [read-path/00-overview.md](read-path/00-overview.md) | Read path overview |
| [read-path/01-query-engine.md](read-path/01-query-engine.md) | Query engine |
| [read-path/02-storage-engine.md](read-path/02-storage-engine.md) | Storage engine |
| [read-path/03-sstable-index-lookup.md](read-path/03-sstable-index-lookup.md) | Index lookup |
| [read-path/04-sstable-sequential-scan.md](read-path/04-sstable-sequential-scan.md) | Sequential scan |

## 🧪 Testing Documentation

### Core Testing Strategy
| Document | Description | Status |
|----------|-------------|--------|
| [TESTING_PRD.md](TESTING_PRD.md) | Testing Product Requirements | Active |
| [testing/comprehensive-testing-framework.md](testing/comprehensive-testing-framework.md) | Complete testing framework | Active |
| [testing/TESTING_ARCHITECTURE_SPECIFICATION.md](testing/TESTING_ARCHITECTURE_SPECIFICATION.md) | Testing architecture design | Active |
| [testing/TESTING_IMPLEMENTATION_GUIDE.md](testing/TESTING_IMPLEMENTATION_GUIDE.md) | Implementation guidelines | Active |
| [testing/TESTING_ARCHITECTURE.md](testing/TESTING_ARCHITECTURE.md) | Testing system architecture | Active |
| [test-infrastructure-architecture.md](test-infrastructure-architecture.md) | Test infrastructure architecture | Active |
| [test_data_pipeline_guide.md](test_data_pipeline_guide.md) | Test data pipeline guide | Active |

### Testing Guides & Best Practices
| Document | Description | Status |
|----------|-------------|--------|
| [testing/RUST_CLI_TESTING_BEST_PRACTICES.md](testing/RUST_CLI_TESTING_BEST_PRACTICES.md) | Rust CLI testing patterns | Active |
| [testing/REPL_TESTING_GUIDE.md](testing/REPL_TESTING_GUIDE.md) | REPL testing strategies | Active |
| [testing/E2E_WRITE_TESTING_GUIDE.md](testing/E2E_WRITE_TESTING_GUIDE.md) | End-to-end write testing | Active |
| [testing/MOCK_USAGE_POLICY.md](testing/MOCK_USAGE_POLICY.md) | Mock usage policy (real data preferred) | Active |

## 📊 Technical Documentation

### Format Specifications
| Document | Description | Status |
|----------|-------------|--------|
| [technical/BTI_FORMAT_SPECIFICATION.md](technical/BTI_FORMAT_SPECIFICATION.md) | BTI format details | Active |
| [technical/BTI_COMPLETE_ARCHITECTURE.md](technical/BTI_COMPLETE_ARCHITECTURE.md) | BTI architecture | Active |
| [technical/UDT_FORMAT_SPEC.md](technical/UDT_FORMAT_SPEC.md) | User Defined Types format | Active |
| [technical/CEP25_BYTE_COMPARABLE_ENCODER.md](technical/CEP25_BYTE_COMPARABLE_ENCODER.md) | CEP-25 byte-comparable encoding | Active |
| [comparator-type-api.md](comparator-type-api.md) | Comparator type API | Active |

### API & Architecture
| Document | Description | Status |
|----------|-------------|--------|
| [technical/api-specification.md](technical/api-specification.md) | API specification | Active |
| [technical/architecture.md](technical/architecture.md) | System architecture | Active |
| [technical/CASSANDRA_5_0_COMPATIBILITY_MATRIX.md](technical/CASSANDRA_5_0_COMPATIBILITY_MATRIX.md) | Compatibility matrix | Active |
| [technical/CASSANDRA_COMPATIBILITY_GUIDE.md](technical/CASSANDRA_COMPATIBILITY_GUIDE.md) | Compatibility guide | Active |
| [technical/CQL_PARSER_IMPLEMENTATION.md](technical/CQL_PARSER_IMPLEMENTATION.md) | CQL parser implementation | Active |
| [technical/ERROR_HANDLING_RECOVERY.md](technical/ERROR_HANDLING_RECOVERY.md) | Error handling and recovery | Active |

### Performance & Profiling
| Document | Description | Status |
|----------|-------------|--------|
| [performance.md](performance.md) | Benchmark methodology, reproducibility, CI perf gate | Active |
| [profiling.md](profiling.md) | pprof flamegraphs, dhat heap profiling, `scripts/profile.sh` loop | Active |

### Complex Types Documentation
| Document | Description | Status |
|----------|-------------|--------|
| [complex_types_documentation_index.md](complex_types_documentation_index.md) | Complex types overview | Active |
| [complex_types_api_reference.md](complex_types_api_reference.md) | API reference | Active |
| [complex_types_examples.md](complex_types_examples.md) | Usage examples | Active |
| [complex_types_performance_guide.md](complex_types_performance_guide.md) | Performance guide | Active |
| [complex_types_troubleshooting.md](complex_types_troubleshooting.md) | Troubleshooting guide | Active |

## 👥 User Documentation

| Document | Purpose | Audience |
|----------|---------|----------|
| [user-guides/installation.md](user-guides/installation.md) | Installation guide | End users |
| [user-guides/quick-start.md](user-guides/quick-start.md) | Getting started | New users |
| [user-guides/cli.md](user-guides/cli.md) | CLI usage guide | CLI users |
| [user-guides/troubleshooting.md](user-guides/troubleshooting.md) | Common issues | All users |
| [user-guides/UAT_QUICK_START.md](user-guides/UAT_QUICK_START.md) | User acceptance testing | QA users |
| [user-guides/demo_real_data.md](user-guides/demo_real_data.md) | Real data demos | Advanced users |

## 📈 Reports & Analysis

| Document | Description |
|----------|-------------|
| [reports/README.md](reports/README.md) | Reports index |
| [reports/milestone-reports/EXECUTIVE_SUMMARY.md](reports/milestone-reports/EXECUTIVE_SUMMARY.md) | Project executive summary |
| [reports/milestone-reports/M2_COMPLETION_REPORT.md](reports/milestone-reports/M2_COMPLETION_REPORT.md) | M2 completion report |
| [reports/bti-read-support-scoping.md](reports/bti-read-support-scoping.md) | BTI read support scoping |
| [reports/fixture-version-matrix.md](reports/fixture-version-matrix.md) | Fixture version matrix |
| [report/M5_FINAL_AUDIT_REPORT.md](report/M5_FINAL_AUDIT_REPORT.md) | M5 final audit |
| [coverage-analysis-jan-2026.md](coverage-analysis-jan-2026.md) | Coverage analysis (Jan 2026) |

## 🗂️ Archived Documentation

The `archive/` directory contains historical documents that are no longer actively maintained but kept for reference:

- [archive/issues/INDEX.md](archive/issues/INDEX.md) - Historical issue investigations and format-debugging deep dives

## 🔍 Finding Documentation

### By Purpose
- **Getting Started**: Start with [user-guides/quick-start.md](user-guides/quick-start.md)
- **SSTable Format**: Read [the definitive guide](sstables-definitive-guide/README.md)
- **Development Setup**: See [development/DEVELOPMENT.md](development/DEVELOPMENT.md)
- **Testing**: Begin with [TESTING_PRD.md](TESTING_PRD.md)
- **Performance**: See [performance.md](performance.md) and [profiling.md](profiling.md)
- **API Reference**: Check [technical/api-specification.md](technical/api-specification.md)
- **Troubleshooting**: Visit [user-guides/troubleshooting.md](user-guides/troubleshooting.md)

### By Audience
- **End Users**: Browse `user-guides/`
- **Developers**: Focus on `development/`, `technical/`, and `sstables-definitive-guide/`
- **QA Engineers**: Check `testing/` and `test-data/validation-matrix.md` (repo root)
- **Project Managers**: Review `reports/`

## 📋 Documentation Standards

All documentation in this project follows these standards:
- **Markdown Format**: All docs use `.md` extension
- **Clear Headings**: Hierarchical structure with descriptive headings
- **Table of Contents**: Complex documents include TOC
- **Status Indicators**: Documents indicate if they're active, archived, or moved
- **Cross-References**: Related documents are linked
- **Target Audience**: Each document specifies its intended audience

## 🔄 Maintenance

This documentation index is updated whenever:
- New documentation is added
- Documents are moved or restructured
- Archive policies are applied
- Major project milestones are reached

**Last Updated**: June 10, 2026 (link audit + profiling docs added; removed entries for deleted reports and renamed specs)
**Status**: Active and Comprehensive
