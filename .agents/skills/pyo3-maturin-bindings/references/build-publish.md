# Building and Publishing

## Development Workflow

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install maturin
pip install maturin

# Build and install in development mode (fast iteration)
maturin develop

# Build with optimizations for testing performance
maturin develop --release

# Build wheel without installing
maturin build --release
```

## pyproject.toml Configuration

```toml
[build-system]
requires = ["maturin>=1.5,<2.0"]
build-backend = "maturin"

[project]
name = "cqlite"
version = "0.1.0"
description = "Fast CQL parser and SSTable reader"
readme = "README.md"
license = { file = "LICENSE" }
requires-python = ">=3.8"
authors = [{ name = "Your Name", email = "you@example.com" }]
classifiers = [
    "Development Status :: 4 - Beta",
    "Programming Language :: Rust",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]
keywords = ["cassandra", "cql", "parser", "sstable"]

[project.urls]
Homepage = "https://github.com/yourname/cqlite"
Documentation = "https://cqlite.readthedocs.io"
Repository = "https://github.com/yourname/cqlite"

[project.optional-dependencies]
dev = ["pytest", "pytest-benchmark", "mypy"]

[tool.maturin]
# Python source directory (for pure Python additions)
python-source = "python"

# Module name if different from package name
module-name = "cqlite._cqlite"

# Features to enable during build
features = ["pyo3/extension-module"]

# Strip symbols for smaller binaries
strip = true

# Build for specific Python versions (CI)
# python-packages = ["cqlite"]
```

## Cargo.toml Configuration

```toml
[package]
name = "cqlite"
version = "0.1.0"
edition = "2021"

[lib]
name = "cqlite"
crate-type = ["cdylib", "rlib"]  # cdylib for Python, rlib for Rust

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module"] }

[profile.release]
lto = true           # Link-time optimization
codegen-units = 1    # Better optimization, slower compile
strip = true         # Strip symbols
opt-level = 3        # Maximum optimization
```

## Publishing to PyPI

### 1. Test PyPI First

```bash
# Build wheels for current platform
maturin build --release

# Upload to Test PyPI
maturin upload --repository testpypi target/wheels/*.whl

# Test installation
pip install --index-url https://test.pypi.org/simple/ cqlite
```

### 2. Production PyPI

```bash
# Ensure PyPI token is configured
# ~/.pypirc or MATURIN_PYPI_TOKEN env var

maturin publish
```

### 3. CI/CD with GitHub Actions

```yaml
# .github/workflows/release.yml
name: Release

on:
  release:
    types: [published]

permissions:
  contents: read

jobs:
  linux:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64, aarch64]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          args: --release --out dist
          manylinux: auto
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-linux-${{ matrix.target }}
          path: dist

  macos:
    runs-on: macos-latest
    strategy:
      matrix:
        target: [x86_64, aarch64]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}-apple-darwin
          args: --release --out dist
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-macos-${{ matrix.target }}
          path: dist

  windows:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          args: --release --out dist
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-windows
          path: dist

  sdist:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build sdist
        uses: PyO3/maturin-action@v1
        with:
          command: sdist
          args: --out dist
      - uses: actions/upload-artifact@v4
        with:
          name: sdist
          path: dist

  publish:
    needs: [linux, macos, windows, sdist]
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write  # Trusted publishing
    steps:
      - uses: actions/download-artifact@v4
        with:
          path: dist
          merge-multiple: true
      - uses: pypa/gh-action-pypi-publish@release/v1
```

## Hybrid Packages (Rust + Pure Python)

```
cqlite/
├── Cargo.toml
├── pyproject.toml
├── src/
│   └── lib.rs              # Rust code -> cqlite._cqlite
└── python/
    └── cqlite/
        ├── __init__.py     # Re-exports from _cqlite
        ├── py.typed        # PEP 561 marker
        └── helpers.py      # Pure Python utilities
```

```python
# python/cqlite/__init__.py
from cqlite._cqlite import (
    parse,
    Statement,
    ParseError,
)

# Pure Python additions
from cqlite.helpers import format_statement

__all__ = ["parse", "Statement", "ParseError", "format_statement"]
```

## Type Stubs (.pyi)

Generate or write type stubs for better IDE support:

```python
# python/cqlite/__init__.pyi
from typing import List, Optional

class Statement:
    @property
    def query_type(self) -> str: ...
    @property
    def keyspace(self) -> Optional[str]: ...
    @property
    def table(self) -> Optional[str]: ...

def parse(cql: str) -> Statement: ...
def parse_all(cql: str) -> List[Statement]: ...

class ParseError(Exception): ...
```

## Version Management

```toml
# Cargo.toml - single source of truth
[package]
version = "0.1.0"

# pyproject.toml - read from Cargo.toml
[project]
dynamic = ["version"]

[tool.maturin]
# Version is read from Cargo.toml automatically
```

```python
# Expose version in Python
# python/cqlite/__init__.py
from importlib.metadata import version
__version__ = version("cqlite")
```
