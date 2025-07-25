# Debug Tools

This directory contains compiled debug utilities and testing tools for CQLite development.

## Available Tools

### Variable Integer (VInt) Tools
- **`debug_vint2`** - Debug utility for Variable Integer encoding/decoding (version 2)
- **`debug_vint3`** - Debug utility for Variable Integer encoding/decoding (version 3)
- **`simple_vint_demo`** - Simple demonstration of VInt functionality

### SSTable Tools
- **`simple_sstable_test`** - Basic SSTable format testing and validation tool

### Integration Tools
- **`integration_test_demo`** - Demonstration of integration testing capabilities
- **`parser_integration_example`** - Example program showing parser integration

## Usage

All tools are compiled ARM64 Mach-O executables. Run them directly:

```bash
# VInt debugging
./debug_vint2
./debug_vint3
./simple_vint_demo

# SSTable testing
./simple_sstable_test

# Integration testing
./integration_test_demo
./parser_integration_example
```

## Notes

- These are standalone compiled binaries (no source dependencies)
- Built for ARM64 architecture (Apple Silicon)
- Created during various development phases for debugging and validation
- Executable permissions preserved during move

## Development Context

These tools were created to:
- Debug Variable Integer encoding/decoding issues
- Test SSTable format parsing
- Validate integration between components
- Demonstrate parser capabilities
- Support development workflow debugging

For source code or to rebuild these tools, check the `cqlite-core/src/` directories and test suites.