# ❄️ SuperTable

<p align="center">
  <img src="https://raw.githubusercontent.com/abrahimzaman360/super-table/main/logo.png" alt="SuperTable Logo" width="300"/>
  <br>
  <b>The Rust-native, Iceberg-compatible open table format for the modern data stack.</b>
</p>

<p align="center">
  <a href="https://github.com/abrahimzaman360/super-table/actions"><img src="https://img.shields.io/github/actions/workflow/status/abrahimzaman360/super-table/rust.yml?style=flat-square" alt="Build Status"></a>
  <a href="https://pypi.org/project/pysupertable/"><img src="https://img.shields.io/pypi/v/pysupertable?style=flat-square&color=blue" alt="PyPI"></a>
  <a href="https://crates.io/crates/supertable"><img src="https://img.shields.io/crates/v/supertable?style=flat-square&color=orange" alt="Crates.io"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="License"></a>
</p>

<p align="center">
  <b>Integrations:</b>
  <br>
  <img src="https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Polars-CD792C?style=for-the-badge&logo=polars&logoColor=white" alt="Polars">
  <img src="https://img.shields.io/badge/DataFusion-4D6F88?style=for-the-badge&logo=rust&logoColor=white" alt="DataFusion">
  <img src="https://img.shields.io/badge/Apache_Iceberg-223657?style=for-the-badge&logo=apache&logoColor=white" alt="Iceberg">
</p>

---

## 🚀 Overview

SuperTable is a high-performance table format built from the ground up in Rust. It provides **ACID guarantees**, **multi-engine interoperability**, and **advanced data management** features designed for high-performance data lakes.

It is fully compatible with the **Apache Iceberg REST Catalog** protocol, meaning it works out-of-the-box with existing tools while offering superior performance and resource efficiency.

👉 **[Read the Architecture Deep Dive](ARCHITECTURE.md)**

## ✨ Key Features

- **ACID Transactions**: Optimistic concurrency control (OCC) for reliable concurrent writes.
- **⚡ Rust Native**: Built for speed, memory safety, and zero-GC overhead.
- **🧊 Iceberg Compatibility**: Fully compatible with Iceberg REST catalog spec.
- **🐍 Python SDK**: Native `pysupertable` bindings with zero-copy Arrow integration.
- **🔌 Query Engine Integrations**:
  - **DataFusion**: Built-in `TableProvider` with predicate pushdown.
  - **Polars**: High-performance DataFrame integration.
  - **Spark**: Comprehensive read/write connector.
- **🛠️ Production Ready**:
  - **Z-Ordering**: Multi-dimensional spatial indexing.
  - **Compaction**: Native small-file merging.
  - **Schema Evolution**: Full support for adding, renaming, and updating columns.

## 📦 Installation

### Python
```bash
pip install pysupertable
```

### Rust
```toml
[dependencies]
supertable = "0.1.1"
```

## ⚡ Quick Start

### Python (with Polars)
```python
import supertable as st
import polars as pl

# 1. Create a table
schema = st.PySchema([
    st.PyField(1, "id", st.PyType.Long, required=True),
    st.PyField(2, "name", st.PyType.String, required=True),
])
table = st.create_table("s3://warehouse/users", schema)

# 2. Write data (via Polars)
df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
table.append(df)

# 3. Read back
print(table.to_pandas())
```

### Rust (with DataFusion)
```rust
use superfusion::SuperTableProvider;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    
    // Register SuperTable
    let table = SuperTableProvider::try_new("s3://warehouse/users").await?;
    ctx.register_table("users", Arc::new(table))?;

    // Query with SQL
    let df = ctx.sql("SELECT * FROM users WHERE id > 1").await?;
    df.show().await?;

    Ok(())
}
```

## 🗺️ Roadmap

- [x] Core Rust Implementation
- [x] Iceberg REST Catalog Compatibility
- [x] DataFusion & Polars Integration
- [x] Spark Connector (Read/Write)
- [x] Python Bindings (`pysupertable`)
- [x] Z-Ordering & Compaction
- [ ] Merge-on-Read (MoR)
- [ ] Merge/Upsert SQL Support
- [ ] Kafka Connect Integration

## 🤝 Contributing

We welcome contributions! Please check out [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system design before diving in.

## 📄 License

MIT (c) 2026 SuperTable Team
