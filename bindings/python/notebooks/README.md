# CQLite Python Bindings - Acceptance Testing Notebooks

Interactive Jupyter notebooks for testing and validating the CQLite Python bindings.

## Quick Start with uv

```bash
# From project root
cd /path/to/cqlite

# Fetch test data (required)
bash test-data/scripts/fetch-datasets.sh
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets

# Create isolated environment with uv
uv venv .venv-notebook
source .venv-notebook/bin/activate

# Install dependencies
uv pip install jupyter maturin

# Build and install cqlite bindings
cd bindings/python && maturin develop && cd ../..

# Launch Jupyter
jupyter notebook bindings/python/notebooks/acceptance-testing.ipynb
```

## Alternative: pip/venv setup

```bash
# From project root
python3 -m venv .venv-notebook
source .venv-notebook/bin/activate
pip install jupyter maturin

cd bindings/python && maturin develop && cd ../..
jupyter notebook bindings/python/notebooks/
```

## Notebooks

### acceptance-testing.ipynb

Comprehensive tour of all Python binding features:

1. **Environment Setup** - Import validation, path configuration
2. **Basic Queries** - open/execute/close workflow
3. **Type System** - All CQL-to-Python type conversions
4. **Streaming** - Memory-efficient large result processing
5. **Prepared Statements** - Query analysis and statistics
6. **Configuration** - Presets and custom config
7. **Error Handling** - Exception hierarchy
8. **All 33 Tables** - Smoke test every test table

## Troubleshooting

### "No module named 'cqlite'"
Rebuild the bindings:
```bash
cd bindings/python && maturin develop
```

### Empty query results
Fetch the SSTable data files:
```bash
bash test-data/scripts/fetch-datasets.sh
```

### Path errors
Set the environment variable:
```bash
export CQLITE_DATASETS_ROOT=/path/to/cqlite/test-data/datasets
```

### Kernel not found
Install ipykernel in your environment:
```bash
uv pip install ipykernel
python -m ipykernel install --user --name=cqlite-notebook
```
