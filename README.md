# ❄️ SuperTable: The Next-Gen Data Lake Format for Rust

SuperTable is a high-performance, **Iceberg-compatible** table format built from the ground up in Rust. It provides ACID guarantees, multi-engine interoperability, and advanced data management features designed for the modern data stack.

---

## 🚀 Key Features

- **ACID Transactions**: Optimistic concurrency control (OCC) for reliable concurrent writes.
- **Iceberg Compatibility**: Built on the Iceberg spec, enabling seamless integration with existing tools and catalogs.
- **Row-Level Operations**: Functional `DELETE`, `UPDATE`, and `MERGE` using a Copy-on-Write (CoW) strategy.
- **Query Engine Integrations**:
  - **DataFusion**: Native `TableProvider` with predicate pushdown and partition pruning.
  - **Polars**: (In Progress) High-performance DataFrame integration.
  - **PySpark**: (Planned) Seamless Python ecosystem support.
- **REST Catalog API**: A secure, Iceberg-compatible REST API (`superrest`) for metadata management.
- **Advanced Optimizations**:
  - **Z-Ordering**: Multi-dimensional clustering for efficient spatial-like pruning.
  - **Compaction**: Native background compaction to minimize small file problems.
- **Data Integrity**: Built-in `SchemaValidator` for strict schema enforcement.
- **Cloud Native**: Native support for S3, GCS, Azure Blob, and local filesystems.

---

## 📂 Project Architecture

SuperTable is organized as a modular workspace:

- [**supercore**](supercore/): The core library containing metadata management, transactions, and I/O.
- [**superrest**](superrest/): An Iceberg-compatible REST Catalog API server.
- [**superfusion**](superfusion/): DataFusion integration for distributed query execution.
- [**supercli**](supercli/): A command-line tool for table inspection and maintenance.
- [**supercatalog**](supercatalog/): Catalog implementations (REST, In-Memory, etc.).
- [**superbindings**](superbindings/): Python (PyO3) and Node.js bindings.

---

## 🛠️ Quick Start (Rust)

```rust
use superfusion::SuperTableProvider;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    
    // Register a SuperTable
    let table = SuperTableProvider::try_new("s3://my-bucket/warehouse/my_table").await?;
    ctx.register_table("users", Arc::new(table))?;

    // Query like a pro
    let df = ctx.sql("SELECT * FROM users WHERE age > 21").await?;
    df.show().await?;

    Ok(())
}
```

---

## 🔍 Why SuperTable?

Traditional data lake formats are often siloed or lack first-class Rust support. SuperTable brings the performance and safety of Rust to the Iceberg ecosystem, providing a lightweight, ultra-fast alternative to JVM-based implementations.

---

## 🗺️ Roadmap

- [x] Iceberg Metadata V1/V2 Support
- [x] DataFusion Predicate Pushdown
- [x] Row-level DELETE/UPDATE
- [x] REST Catalog Security (JWT)
- [x] Z-Ordering & Compaction
- [ ] PyPI & NPM Package Publishing
- [ ] Merge-on-Read (MoR) Implementation
- [ ] Full Schema & Partition Evolution
- [ ] Time Travel Queries (AS OF)

---

## 🤝 Contributing

We welcome your contributions!
Go ahead...
---

## 📄 License

MIT (c) 2026 SuperTable Team
