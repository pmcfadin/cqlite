---
title: Troubleshooting
description: Common problems and solutions when installing and using CQLite.
sidebar:
  label: Troubleshooting
  order: 5
---

# Troubleshooting

This page covers the most common problems users encounter with CQLite. If you
do not find your answer here, check the
[GitHub issues](https://github.com/pmcfadin/cqlite/issues) or open a new one.

## Installation

### `command not found: cqlite`

The binary is not on your `PATH`.

```bash
# Verify the binary exists
ls -la $(which cqlite 2>/dev/null || echo "NOT FOUND")

# Add the directory to PATH permanently (bash/zsh)
echo 'export PATH="/usr/local/bin:$PATH"' >> ~/.bashrc  # or ~/.zshrc
source ~/.bashrc

# Verify
cqlite --version
```

On macOS with Homebrew-style installations, the binary might be in
`/usr/local/bin` or `~/.local/bin`. Check both.

### Binary not compatible with my OS/architecture

Download the correct asset for your platform from the
[releases page](https://github.com/pmcfadin/cqlite/releases):

- macOS Apple Silicon: `cqlite-aarch64-apple-darwin.tar.gz`
- macOS Intel: `cqlite-x86_64-apple-darwin.tar.gz`
- Linux x86_64 (any distro): use the `musl` variant for maximum compatibility
- Linux ARM64: `cqlite-aarch64-unknown-linux-gnu.tar.gz`

See [Installation](/cqlite/user-docs/installation/) for the full platform table.

### Missing shared libraries on Linux

```
./cqlite: error while loading shared libraries: libssl.so.1.1: cannot open shared object file
```

Use the musl (static) build, which has no shared library dependencies:

```bash
TARGET=x86_64-unknown-linux-musl
curl -fsSLO https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-$TARGET.tar.gz
tar xzf cqlite-$TARGET.tar.gz
./cqlite --version
```

## Test data

### Query tests return 0 rows

The test datasets contain only the JSONL reference files by default.
The actual SSTable binary files (`.db`) must be fetched separately:

```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
bash test-data/scripts/fetch-datasets.sh
```

After fetching, retry your query. You should see results.

### Missing test data environment variable

Many integration tests look for `CQLITE_DATASETS_ROOT`. Set it before running:

```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
cargo test --package cqlite-core
```

## CLI

### Query returns no rows on your own SSTable data

1. **Wrong `--data-dir` path**: `--data-dir` should point to the directory that
   contains the keyspace subdirectories, not to a single SSTable directory.

   ```bash
   # Correct: the parent directory
   cqlite --data-dir /var/lib/cassandra/data ...

   # Incorrect: the table-specific directory
   cqlite --data-dir /var/lib/cassandra/data/my_ks/my_table-abc123 ...
   ```

2. **Schema mismatch**: ensure the keyspace and table names in the schema file
   exactly match the directory names on disk.

3. **Unsupported format**: CQLite only supports Cassandra 5.0 `nb-*-big-*` files.
   Check that your SSTable directory contains files named `nb-1-big-Data.db` (not
   `mc-*`, `md-*`, or `la-*`).

### `Unsupported SSTable format`

CQLite reads **Cassandra 5.0 BIG format** SSTables only. Files from Cassandra 3.x
(`mc-*`, `la-*`) and 4.x (`md-*`) are not supported. Upgrade your cluster to
Cassandra 5.0 and run `nodetool upgradesstables` to convert, or use
Cassandra's `sstabledump` tool to export to JSON first.

See [Limitations](/cqlite/user-docs/limitations/) for the full format-support matrix.

### `Parsing issues` or garbled values

Check `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`
for known parsing gaps. Some edge cases (for example, very wide partitions with
10 000+ rows, or BTI-format index files) may not produce correct results.

Enable debug logging to get more detail:

```bash
RUST_LOG=debug cqlite \
  --schema my.cql \
  --data-dir /path/to/data \
  --query "SELECT * FROM ks.tbl LIMIT 5"
```

### `--out` vs `--format` precedence

`--out` takes precedence over `--format` when both are provided. Use `--out` for
reliable behavior. You can also set a default via the environment variable:

```bash
export CQLITE_OUT=json
```

## Python bindings

### Python import errors after install

```bash
python3 --version  # Must be 3.9+
pip install --upgrade cqlite-py
```

If the binary wheel is missing for your platform, build from source:

```bash
pip install maturin
rustup update    # Requires Rust 1.85+
cd bindings/python && maturin develop
```

### Python tests skip or fail

Ensure the dataset is available:

```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
bash test-data/scripts/fetch-datasets.sh
pytest bindings/python/tests -v
```

### Concurrent query race condition

A known issue: concurrent queries on the **same** `Database` handle may race on
schema metadata access. Work around it by running a warm-up query before spawning
parallel threads:

```python
# Warm up — triggers schema metadata load
list(db.execute('SELECT * FROM ks.tbl LIMIT 1'))

# Now safe to use from multiple threads
```

## Node.js bindings

### Native module fails to load

Check that you are on a supported platform (Linux x86_64/arm64, macOS
x86_64/arm64, Windows x86_64). If you are on a supported platform and still see
an error like `Error: Cannot find module './cqlite.node'`, try rebuilding:

```bash
cd bindings/node
npm run build
npm test
```

### TypeScript types not found

The types are in `lib/index.d.ts`. Ensure `"types": "lib/index.d.ts"` is
referenced in your `tsconfig.json` or that `@cqlite/node` resolves correctly.
The package ships with complete TypeScript definitions — no `@types/` package is
needed.

## Clippy and CI failures

Run Clippy in CI mode to catch issues early:

```bash
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
```

## Common error quick reference

| Symptom | Likely cause | Quick fix |
|---------|-------------|-----------|
| `command not found: cqlite` | Binary not on PATH | Add install dir to PATH |
| Query returns 0 rows | Missing test data | Run `fetch-datasets.sh` |
| `Unsupported SSTable format` | Not Cassandra 5.0 format | Upgrade cluster, convert SSTables |
| `IO` error opening file | Wrong `--data-dir` path | Check path points to data root |
| Python `ImportError` | Old Python or missing wheel | Upgrade Python, rebuild bindings |
| `SCHEMA` error | Keyspace/table name mismatch | Check schema file vs directory names |
| `PARSE` error | Parsing limitation or corrupt file | Check limitations page; report bug |

For anything not listed here, open an issue at
https://github.com/pmcfadin/cqlite/issues with the output of:

```bash
cqlite --version
RUST_LOG=debug cqlite --schema <your.cql> --data-dir <dir> --query "<query>" 2>&1 | head -50
```
