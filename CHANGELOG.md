# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Query engine development (M2 milestone)
- CLI enhancements: TUI mode, REPL mode, one-shot query execution

### Changed
- Ongoing SSTable parsing improvements

## [0.1.0] - 2024-XX-XX

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
- Workspace structure with multiple crates:
  - `cqlite-core`: Core parsing library
  - `cqlite-cli`: Command-line interface
  - `cqlite-ffi`: C/C++ bindings (experimental)
  - `cqlite-wasm`: WebAssembly bindings (experimental)

### Technical Details
- Zero-copy parsing where possible
- Memory-efficient design targeting <128MB for large files
- No external cluster dependencies required
- Real Cassandra SSTable test data validation

[Unreleased]: https://github.com/pmcfadin/cqlite/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/pmcfadin/cqlite/releases/tag/v0.1.0
