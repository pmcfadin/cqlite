# CQLite Documentation

## 📚 Complete Project Documentation

Welcome to the CQLite documentation hub. This directory holds technical documentation and user guides. The polished, published docs live at **[pmcfadin.github.io/cqlite](https://pmcfadin.github.io/cqlite/)**.

---

## 📖 Primary Reference

- **[The SSTable Definitive Guide](sstables-definitive-guide/README.md)** - Single source of truth for the Cassandra 5.0 SSTable format: Data.db row layout, Index.db/Summary.db lookups, BTI trie indexes, encoding cheat sheets, and known limitations.

---

## 🗂️ Documentation Structure

### 👥 User Documentation
- **[Installation Guide](user-guides/installation.md)** - Complete setup instructions for all platforms
- **[Quick Start Guide](user-guides/quick-start.md)** - Get started with CQLite in minutes
- **[CLI Guide](user-guides/cli.md)** - Command-line interface usage
- **[Troubleshooting Guide](user-guides/troubleshooting.md)** - Comprehensive problem resolution
- **[Using cqlite-core as a Dependency](using-cqlite-core-as-a-dependency.md)** - Embedding the library in your own Rust project

### ✍️ Write Support (M5)
- **[Write Support Overview](write-support.md)** - Mutations, memtable, WAL, and flush
- **[Write Engine API](write-engine-api.md)** - Programmatic write API reference
- **[Write Support Limitations](write-support-limitations.md)** - What write support does not cover yet

### 🎯 Complex Types Documentation
- **[Complex Types API Reference](complex_types_api_reference.md)** - Complete API for complex types
- **[Complex Types Documentation Index](complex_types_documentation_index.md)** - Documentation roadmap for complex types
- **[Complex Types Examples](complex_types_examples.md)** - Implementation examples and patterns
- **[Complex Types Performance Guide](complex_types_performance_guide.md)** - Performance optimization strategies
- **[Complex Types Troubleshooting](complex_types_troubleshooting.md)** - Common issues and solutions

### 🔧 Technical Documentation
- **[Architecture Overview](technical/architecture.md)** - System design and architecture
- **[API Specification](technical/api-specification.md)** - Complete API reference
- **[Cassandra 5.0 Compatibility Matrix](technical/CASSANDRA_5_0_COMPATIBILITY_MATRIX.md)** - Format compatibility matrix
- **[Cassandra Compatibility Guide](technical/CASSANDRA_COMPATIBILITY_GUIDE.md)** - Compatibility details and caveats
- **[BTI Format Specification](technical/BTI_FORMAT_SPECIFICATION.md)** - BTI index format details
- **[UDT Format Specification](technical/UDT_FORMAT_SPEC.md)** - User-defined type format details
- **[Parser Overview](architecture/parser-overview.md)** - How the SSTable parsers fit together

### ⚡ Performance & Profiling
- **[Performance Methodology](performance.md)** - Benchmark reproducibility and the CI perf gate
- **[Profiling Guide](profiling.md)** - Flamegraphs, heap profiling, and the `scripts/profile.sh` improvement loop

### 💻 Development Documentation
- **[Contributing Guide](development/contributing.md)** - How to contribute to CQLite
- **[Development Guide](development/DEVELOPMENT.md)** - Local development workflow
- **[Rust Developer Guide](development/rust_developer_guide.md)** - Project Rust conventions and patterns
- **[Releasing](development/RELEASING.md)** - Release process and checklist

### 📊 Project Reports
- **[Historical Issue Investigations](archive/issues/INDEX.md)** - Archived deep-dives from past format debugging

---

## 🚀 Quick Navigation

### For New Users
1. Start with [Installation Guide](user-guides/installation.md)
2. Follow the [Quick Start Guide](user-guides/quick-start.md)
3. Review [API Specification](technical/api-specification.md) for development

### For Developers
1. Read [Architecture Overview](technical/architecture.md)
2. Check [Contributing Guide](development/contributing.md)
3. Study [The SSTable Definitive Guide](sstables-definitive-guide/README.md) before touching format code

### For Performance Work
1. Read [Performance Methodology](performance.md) for what the CI gate enforces
2. Use the [Profiling Guide](profiling.md) to find and fix bottlenecks
3. Keep the [Troubleshooting Guide](user-guides/troubleshooting.md) handy

---

## 📈 Project Status

- **Current Version**: 0.12.0
- **Cassandra Compatibility**: 5.0+ with BTI (`da`) format read **and** write support
- **Milestones Complete**: M1 (Core Reading), M2 (CLI), M3 (Output Writers), M4 (Python & Node.js Bindings), M5 (Write Support + Compaction)
- **v0.12.0**: Byte-for-byte compaction parity vs Apache Cassandra · Arrow Flight + Trino connector · canonical BTI write/read · CDC delta-export · `WRITETIME()`/`TTL()` in `SELECT`
- **Next**: M6 (WebAssembly Bindings), M7 (Performance validation + v1.0)
- **Test Pass Rate**: 100% (33/33 tables vs sstabledump, see `test-data/validation-matrix.md`)

---

## 🔄 Document Maintenance

### Last Updated
- Documentation hub: 2026-06-10
- Performance & profiling docs: 2026-06-10
- Architecture docs: 2026-01-27
- User guides: 2026-01-27

### Contributing to Documentation
- Follow the [Documentation Standards](development/contributing.md#documentation-standards)
- Submit changes via pull request
- Ensure all links are functional
- Update the appropriate table of contents

---

## 📖 External Resources

- **GitHub Repository**: https://github.com/pmcfadin/cqlite
- **Community Discussions**: https://github.com/pmcfadin/cqlite/discussions
- **Issue Tracker**: https://github.com/pmcfadin/cqlite/issues
- **CQL Grammar Reference**: https://github.com/pmcfadin/cassandra-antlr4-grammar

---

*This documentation is maintained by the CQLite project team and community contributors. For questions or suggestions, please open an issue or discussion on GitHub.*
