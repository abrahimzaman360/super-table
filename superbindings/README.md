# PySuperTable

<p align="center">
  <img src="https://raw.githubusercontent.com/abrahimzaman360/super-table/main/logo.png" alt="SuperTable Logo" width="200"/>
</p>

**The Rust-native open table format for Python** 🚀

PySuperTable provides Python bindings for [SuperTable](https://github.com/abrahimzaman360/super-table), bringing blazing-fast lakehouse operations to your Python workflows.

## Features

- ⚡ **Rust Performance**: Native Rust core with zero-copy Arrow integration
- 🔄 **ACID Transactions**: Full optimistic concurrency control
- 🕰️ **Time Travel**: Query any historical snapshot
- 🧬 **Schema Evolution**: Add, drop, rename columns safely
- 📊 **DataFusion & Polars**: Native query engine integration
- 🔌 **Iceberg Compatible**: REST Catalog API compatible

## Installation

```bash
pip install pysupertable
```

## Quick Start

```python
import supertable as st

# Create a table
schema = st.Schema([
    st.Field(1, "id", "long", required=True),
    st.Field(2, "name", "string", required=True),
    st.Field(3, "value", "double", required=False),
])

table = st.create_table("s3://my-bucket/warehouse/my_table", schema)

# Query with SQL
df = table.sql("SELECT * FROM my_table WHERE id > 100")

# Time travel
historical = table.snapshot_at("2024-01-01T00:00:00Z")

# Schema evolution
table.add_column("email", "string")
```

## Integration with Pandas

```python
import supertable as st
import pandas as pd

table = st.open_table("s3://my-bucket/warehouse/my_table")

# Read as Pandas DataFrame
df = table.to_pandas()

# Query with filters
df = table.sql("SELECT * FROM my_table WHERE value > 100").to_pandas()
```

## Integration with PyArrow

```python
import supertable as st
import pyarrow as pa

table = st.open_table("s3://my-bucket/warehouse/my_table")

# Read as Arrow Table
arrow_table = table.to_arrow()

# Write Arrow data
table.append(arrow_table)
```

## Why PySuperTable?

| Feature | PySuperTable | pyiceberg | delta-rs |
|---------|--------------|-----------|----------|
| Cold Start | ~10ms | ~500ms | ~200ms |
| Memory Overhead | Low | High (JVM) | Medium |
| Native Rust | ✅ | ❌ | ✅ |
| Zero-copy Arrow | ✅ | ✅ | ✅ |

## Requirements

- Python 3.8+
- Supported platforms: Linux, macOS, Windows

## License

Apache-2.0 OR MIT
