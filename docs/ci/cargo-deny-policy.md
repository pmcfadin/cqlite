# Cargo-Deny Policy Documentation

## Overview

This document describes the cargo-deny configuration for the CQLite project, which ensures license compliance and dependency security for the M1 milestone and beyond.

## Purpose

The `cargo-deny` tool is used in CI to automatically check for:
1. License compatibility issues
2. Security vulnerabilities in dependencies
3. Duplicate dependency versions
4. Untrusted dependency sources

## Configuration File

The cargo-deny configuration is located at `/deny.toml` in the project root.

## License Policy

### Allowed Licenses

The following licenses are explicitly allowed as they are compatible with Apache-2.0:

- **MIT** - Permissive license, very common in Rust ecosystem
- **Apache-2.0** - Our project's primary license
- **Apache-2.0 WITH LLVM-exception** - Apache with additional permissions
- **BSD-2-Clause** - Permissive BSD variant
- **BSD-3-Clause** - Permissive BSD variant with attribution
- **ISC** - Permissive license similar to MIT
- **Unicode-3.0** - For Unicode data files
- **Unicode-DFS-2016** - For Unicode data files
- **MPL-2.0** - Mozilla Public License (weak copyleft, compatible)
- **CC0-1.0** - Public domain dedication
- **Zlib** - Permissive compression library license
- **0BSD** - BSD Zero Clause License

### Explicitly Denied Licenses

Strong copyleft licenses that would conflict with Apache-2.0 are blocked:
- GPL (all versions)
- AGPL (all versions)
- LGPL (all versions)

## Dependency Bans

### Multiple Versions Policy

Multiple versions of the same crate generate warnings rather than errors, as some duplicates are often unavoidable in complex dependency trees.

### Allowed Duplicates

The following crates are explicitly allowed to have multiple versions:
- Windows-related crates (windows-sys, windows-targets, etc.)
- `redox_syscall` - OS interface that often has version conflicts
- `itertools` - Common utility with frequent version divergence
- `hashbrown` - Core data structure library
- `indexmap` - Data structure library

## Security Advisories

### Configuration
- Database: RustSec Advisory Database (https://github.com/rustsec/advisory-db)
- Vulnerability handling: DENY (build fails if vulnerabilities found)
- Yanked crates: WARN (alerts but doesn't fail build)

### Ignoring Advisories

Advisories should only be ignored when:
1. The vulnerability doesn't affect our usage
2. A fix is not yet available
3. The team has assessed and accepted the risk

Document the reason for ignoring any advisory in the configuration file.

## Source Control

### Allowed Sources
- **Crates.io Registry**: https://github.com/rust-lang/crates.io-index
- **Git repositories**: None by default (add only trusted repositories)

### Denied Sources
- Unknown registries are denied
- Unknown git repositories are denied

## CI Integration

### Running Locally

To check license compliance locally:
```bash
cargo install cargo-deny
cargo deny check licenses
```

To check all policies:
```bash
cargo deny check
```

### CI Workflow

The cargo-deny checks are run as part of the M1 CI pipeline. They should pass before merging any PR that modifies dependencies.

### Handling Failures

1. **License failures**: Check if the license is compatible with Apache-2.0. If yes, add it to the allow list. If no, find an alternative dependency.

2. **Security advisories**: Update the affected dependency or apply recommended patches. Only ignore if the vulnerability doesn't affect our usage (document the reason).

3. **Duplicate versions**: Try to unify versions where possible. If not possible, add to the skip list with a comment explaining why.

4. **Source failures**: Only use dependencies from crates.io or explicitly approved git repositories.

## Maintenance

### Regular Updates

1. Update the advisory database regularly:
```bash
cargo deny fetch
```

2. Review newly added dependencies for license compliance

3. Monitor for new security advisories in existing dependencies

### Adding Exceptions

When adding exceptions:
1. Document the reason in the configuration file
2. Get team review for security-related exceptions
3. Set a reminder to review the exception periodically

## References

- [cargo-deny documentation](https://embarkstudios.github.io/cargo-deny/)
- [SPDX License List](https://spdx.org/licenses/)
- [RustSec Advisory Database](https://rustsec.org/)

## Contact

For questions about the cargo-deny policy, please open an issue or contact the maintainers.