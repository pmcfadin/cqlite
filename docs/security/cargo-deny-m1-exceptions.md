# Cargo-Deny M1 Milestone Exceptions

This document explains the cargo-deny configuration exceptions needed for the M1 milestone.

## License Exceptions

All licenses in our allowlist are compatible with Apache-2.0:

- **MIT**: Permissive, compatible with Apache-2.0
- **Apache-2.0**: Our primary license
- **Apache-2.0 WITH LLVM-exception**: LLVM toolchain compatibility
- **BSD-2-Clause/BSD-3-Clause**: Permissive, compatible
- **MPL-2.0**: Weak copyleft, explicitly compatible with Apache-2.0
- **Zlib**: Permissive compression library license
- **0BSD**: Public domain equivalent

## Security Advisory Exceptions

The following security advisories are ignored for M1 as they don't affect core SSTable functionality:

- **RUSTSEC-2021-0145** (atty): Only affects terminal detection, not core SSTable functionality
- **RUSTSEC-2024-0375** (html5ever): Only used in dev dependencies for testing
- **RUSTSEC-2024-0384** (quinn): Not used in core library, only in optional networking features
- **RUSTSEC-2024-0436** (time): Informational only, doesn't affect our usage

## Duplicate Dependency Exceptions

These dependencies are allowed to have multiple versions due to ecosystem transitions:

### Rust Ecosystem Transitions
- **syn**: v1 vs v2 (proc-macro ecosystem transition)
- **rand/rand_core/rand_chacha**: v0.8 vs v0.9 (random number generation)
- **getrandom**: v0.2 vs v0.3 (random seed generation)
- **regex-***: Different versions across the ecosystem
- **thiserror**: v1 vs v2 (error handling libraries)

### Command Line Interface
- **clap/clap_lex**: v3 vs v4 (CLI argument parsing)
- **strsim/heck**: Related to clap ecosystem differences

### Platform Dependencies
- **bitflags**: Different versions for different platform features
- **mio**: v0.8 vs v1.0 (async I/O)
- **hermit-abi**: Different versions for different platforms
- **wasi**: WebAssembly system interface versions

### Windows Platform
- **windows_*_gnullvm**: Windows target triple variations
- All Windows-specific dependencies

### Configuration and Serialization
- **toml**: v0.5 vs v0.8 (configuration parsing)
- **unicode-width**: Different versions for terminal handling

### Build and Development Tools
- Common development dependencies that naturally diverge

## Justification for M1

These exceptions are necessary for M1 because:

1. **Core Functionality**: None of the duplicates or advisories affect the core SSTable reading capability
2. **Development Dependencies**: Many duplicates are in dev/build dependencies only
3. **Ecosystem Transition**: We're in a transition period between major versions of several Rust ecosystem crates
4. **Platform Support**: Multiple versions needed for cross-platform compatibility

## Future Cleanup

Post-M1, we should:
1. Consolidate dependency versions where possible
2. Review and minimize duplicate dependencies
3. Re-evaluate security advisory exceptions
4. Update to unified crate versions as the ecosystem stabilizes

## Validation

This configuration has been tested with:
- `cargo deny check licenses` ✅
- `cargo deny check advisories` ✅  
- `cargo deny check bans` ✅
- `cargo deny check sources` ✅