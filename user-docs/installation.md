---
title: Installation
description: Install the CQLite CLI, Rust library, Python package, or Node.js module.
sidebar:
  label: Installation
  order: 2
---

# Installation

CQLite ships in four forms: a prebuilt CLI binary (the fastest way to start), a
Rust library crate, a Python package, and a Node.js package.

## Prebuilt CLI binaries

Each [GitHub release](https://github.com/pmcfadin/cqlite/releases) attaches a
prebuilt `cqlite` binary and a `.sha256` checksum sidecar for six platforms:

| Platform | Asset |
|----------|-------|
| macOS Apple Silicon | `cqlite-aarch64-apple-darwin.tar.gz` |
| macOS Intel | `cqlite-x86_64-apple-darwin.tar.gz` |
| Linux x86_64 (glibc) | `cqlite-x86_64-unknown-linux-gnu.tar.gz` |
| Linux x86_64 (musl/static) | `cqlite-x86_64-unknown-linux-musl.tar.gz` |
| Linux arm64 (glibc) | `cqlite-aarch64-unknown-linux-gnu.tar.gz` |
| Windows x86_64 | `cqlite-x86_64-pc-windows-gnu.zip` |

### macOS

```bash
# Apple Silicon
TARGET=aarch64-apple-darwin

# Intel Mac — uncomment this line instead:
# TARGET=x86_64-apple-darwin

curl -fsSLO https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-$TARGET.tar.gz
curl -fsSLO https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-$TARGET.tar.gz.sha256
shasum -a 256 -c cqlite-$TARGET.tar.gz.sha256
tar xzf cqlite-$TARGET.tar.gz
./cqlite --version
```

### Linux

```bash
# x86_64 glibc (most common, requires glibc >= 2.17)
TARGET=x86_64-unknown-linux-gnu

# x86_64 musl (fully static, runs everywhere including Alpine containers)
# TARGET=x86_64-unknown-linux-musl

# ARM64 (glibc)
# TARGET=aarch64-unknown-linux-gnu

curl -fsSLO https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-$TARGET.tar.gz
curl -fsSLO https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-$TARGET.tar.gz.sha256
sha256sum -c cqlite-$TARGET.tar.gz.sha256
tar xzf cqlite-$TARGET.tar.gz
sudo mv cqlite /usr/local/bin/
cqlite --version
```

### Windows

Download `cqlite-x86_64-pc-windows-gnu.zip` from the
[releases page](https://github.com/pmcfadin/cqlite/releases/latest), extract it,
and add the folder to your `PATH`. Then verify:

```powershell
cqlite --version
```

## Build from source (Rust)

Requires Rust 1.85+. Install Rust via [rustup.rs](https://rustup.rs) if needed.

```bash
git clone https://github.com/pmcfadin/cqlite.git
cd cqlite
cargo build --release

# The binary is at ./target/release/cqlite
./target/release/cqlite --version
```

To build with write support (M5 feature):

```bash
cargo build --package cqlite-cli --features write-support --release
```

## Python package

Requires Python 3.9+ and a supported platform (Linux x86_64/arm64, macOS x86_64/arm64,
Windows x86_64).

```bash
pip install cqlite-py
```

Verify the install:

```python
import cqlite
print(cqlite.__version__)
```

If the import fails after install, see [Troubleshooting](/cqlite/user-docs/troubleshooting/#python-import-errors-after-install).

### Build the Python package from source

Requires Rust 1.85+ and [maturin](https://www.maturin.rs/).

```bash
pip install maturin
cd bindings/python
maturin develop          # development build (editable)
# maturin build --release  # release wheel
```

## Node.js package

Requires Node.js 18+ and npm.

```bash
npm install @cqlite/node
```

Verify:

```javascript
const { Database } = require('@cqlite/node');
console.log('CQLite Node.js bindings loaded');
```

If the native module fails to load, check that your platform is in the supported
list (Linux x86_64/arm64, macOS x86_64/arm64, Windows x86_64) and file a
[bug report](https://github.com/pmcfadin/cqlite/issues) if it is.

### Build the Node.js package from source

```bash
cd bindings/node
npm install
npm run build
npm test
```

## Using CQLite as a Rust library

Add `cqlite-core` to your `Cargo.toml`:

```toml
[dependencies]
cqlite-core = { git = "https://github.com/pmcfadin/cqlite.git" }
```

Default features include `all-compression` (LZ4, Snappy, Deflate, Zstd) and
`state_machine` (query engine). For a minimal build without the query engine:

```toml
cqlite-core = { git = "…", default-features = false, features = ["all-compression"] }
```

## Feature flags (cqlite-core)

| Flag | Default | Description |
|------|---------|-------------|
| `all-compression` | yes | LZ4, Snappy, Deflate, Zstd support |
| `state_machine` | yes | Query engine and schema-based discovery |
| `cli-helpers` | no | CLI-specific ingestion and REPL API |
| `metrics` | no | Performance metrics collection |
| `experimental` | no | Experimental / unstable features |

## Verify your installation

Regardless of how you installed CQLite, verify it can parse real data:

```bash
# Fetch the test datasets (requires git clone)
bash test-data/scripts/fetch-datasets.sh

# Run a query against them
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --out json
```

If you see three JSON rows, you are ready to go.
If you see errors, check [Troubleshooting](/cqlite/user-docs/troubleshooting/).
