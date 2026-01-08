# Scripts

This directory contains shell scripts for CI/CD and local development.

## CI Scripts (`scripts/ci/`)

Scripts used by GitHub Actions workflows.

| Script | Used By | Purpose |
|--------|---------|---------|
| `ensure_real_dataset.sh` | m1-ci.yml, sstabledump-parity-gate.yml | Dataset provenance check (Issue #79) |
| `install-sstabledump.sh` | sstabledump-parity-gate.yml | Install Cassandra sstabledump tool |
| `test-sstabledump-parity-gate.sh` | sstabledump-parity-gate.yml | Parity validation against sstabledump |
| `validate-cleanup.sh` | ci.yml | Cleanup branch safety validation |
| `validate-structure.sh` | pre-commit | Project structure validation |

## Local Development (`scripts/local/`)

Scripts for local development and testing.

| Script | Purpose |
|--------|---------|
| `test-all-ci-locally.sh` | Run full CI pipeline locally |
| `test-m1-ci-locally.sh` | Run M1 CI locally (Apple Silicon) |
| `sstabledump-docker.sh` | Run sstabledump in Docker container |
| `audit-ci-commands.sh` | Audit CI command definitions |

## Test Data Scripts

See `test-data/scripts/` for test data generation and management scripts.

## Usage

### Pre-push validation
```bash
./scripts/ci/validate-cleanup.sh
```

### Run full CI locally
```bash
./scripts/local/test-all-ci-locally.sh
```

### Run M1 CI locally
```bash
./scripts/local/test-m1-ci-locally.sh
```
