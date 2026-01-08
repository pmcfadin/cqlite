# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- M3: Parquet output format (Issue #21) - in progress

## [0.2.0] - 2026-01-08 (M2 Complete)

### Added
- CLI one-shot query mode with `--schema`, `--data-dir`, `--query`, `--out` flags
- REPL mode with history, completion, and status display
- TUI mode (experimental)
- SELECT query support with WHERE clause (partition/clustering key equality)
- Output formats: Table, JSON, CSV, YAML
- M2SelectValidator for query validation

### Changed
- Query engine enabled by default (`state_machine` feature)
- Documentation updated for M2 completion

## [0.1.0] - 2025-12-18 (M1 Complete)

### Added
- Initial release of CQLite core library
- Cassandra 5.0 SSTable format support ('oa' format with BTI indexes)
- SSTable component parsing:
  - Data.db (row and cell data)
  - Index.db (partition index)
  - Summary.db (index summary)
  - Statistics.db (SSTable metadata)
  - TOC.txt (table of contents)
- Compression codec support:
  - LZ4
  - Snappy
  - Deflate
  - Zstd
- CQL type system implementation:
  - Primitive types (int, bigint, text, blob, uuid, timestamp, etc.)
  - Collection types (list, set, map)
  - User-defined types (UDT)
  - Frozen types
- Schema-aware decoding
- CLI tool with basic parsing commands
- Workspace structure:
  - `cqlite-core`: Core parsing library
  - `cqlite-cli`: Command-line interface
- 33/33 test tables passing (100% validation)

### Technical Details
- Zero-copy parsing where possible
- Memory-efficient design targeting <128MB for large files
- No external cluster dependencies required
- Real Cassandra SSTable test data validation

[Unreleased]: https://github.com/pmcfadin/cqlite/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/pmcfadin/cqlite/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pmcfadin/cqlite/releases/tag/v0.1.0
