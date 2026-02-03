# SuperTable Python SDK

This directory contains the official Python examples for SuperTable.

## Quick Setup

```bash
# 1. Create virtual environment
python3 -m venv venv

# 2. Activate it
source venv/bin/activate  # Linux/macOS
# or: venv\Scripts\activate  # Windows

# 3. Install SuperTable
pip install pysupertable

# 4. Install optional dependencies for examples
pip install polars datafusion pyarrow pandas
```

## Examples

| File | Description |
|------|-------------|
| `showcase.py` | Complete SDK demonstration |
| `requirements.txt` | All dependencies |

## Running Examples

```bash
# Activate venv first
source venv/bin/activate

# Run showcase
python showcase.py
```

## Building from Source

If you want to build the Python bindings from source:

```bash
cd ../../superbindings

# Create venv
python3 -m venv venv
source venv/bin/activate

# Install build tools
pip install maturin

# Build and install locally
maturin develop --release
```
