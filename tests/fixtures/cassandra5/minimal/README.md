# Minimal Cassandra 5 Test Fixtures

## Overview

This directory contains minimal SSTable fixtures generated from Cassandra 5.x for testing CQLite's compatibility with the latest Cassandra format.

## Fixture Provenance

- **Source**: Cassandra 5.0.0 
- **Generated**: 2025-08-19
- **Purpose**: Minimal fixtures for header parsing and single-row reading tests
- **Size**: Kept minimal to avoid repository bloat

## Contents

### simple_table/
A minimal SSTable with one integer key and one text value:
- Schema: `CREATE TABLE test.simple (id int PRIMARY KEY, value text)`
- Data: Single row with id=1, value="test"
- Format: Cassandra 5.x native format

### Files Structure
Each fixture directory contains standard Cassandra SSTable components:
- `Data.db` - Main data file with rows
- `Index.db` - Primary key index
- `Summary.db` - Index summary for fast lookup
- `Statistics.db` - SSTable statistics
- `CompressionInfo.db` - Compression metadata (if compressed)
- `Filter.db` - Bloom filter
- `TOC.txt` - Table of contents listing all components
- `Digest.crc32` - CRC32 checksum of Data.db

## Usage

These fixtures are used by:
1. Header snapshot tests (`tests/cassandra5_header_tests.rs`)
2. Smoke tests for end-to-end compatibility
3. Format validation and regression testing

## Size Constraints

Each fixture is designed to be minimal:
- Single partition with one row
- Basic data types only
- No wide partitions or complex types
- Total size per fixture < 50KB

## Test Coverage

The fixtures validate:
- ✅ SSTable header parsing
- ✅ Metadata component reading  
- ✅ Single row extraction
- ✅ Format compatibility with Cassandra 5.x

## Generation Process

Fixtures were generated using:
1. Fresh Cassandra 5.0.0 installation
2. Simple schema creation
3. Single row insertion  
4. Flush and compaction to create minimal SSTable
5. File extraction and validation

These fixtures ensure CQLite can read real Cassandra 5 data without requiring large test datasets.