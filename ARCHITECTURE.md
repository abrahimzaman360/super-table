# SuperTable Architecture

SuperTable is designed as a modular, high-performance, Rust-native table format.

## System Architecture

```mermaid
flowchart TB
    subgraph Client["Client Layer"]
        Python[Python SDK]
        Rust[Rust API]
        REST[REST Client]
        Spark[Spark Connector]
    end
    
    subgraph Catalog["Catalog Layer"]
        InMem[InMemoryCatalog]
        RESTCat[REST Catalog Server]
        Iceberg[Iceberg REST Compat]
    end
    
    subgraph Table["Table Layer"]
        TM[TableMetadata]
        Schema[Schema & Evolution]
        Snapshot[Snapshots]
        Manifest[ManifestList]
        Partition[Partition Spec]
    end
    
    subgraph Transaction["Transaction Layer"]
        OCC[Optimistic Concurrency]
        Retry[Retry with Backoff]
        Conflict[Conflict Detection]
        Validation[Schema Validator]
    end
    
    subgraph Storage["Storage Layer"]
        ObjectStore[object_store]
        Parquet[Parquet Files (Data)]
        JSON[JSON/Avro (Metadata)]
    end
    
    subgraph Query["Query Engine Integration"]
        DF[DataFusion TableProvider]
        Polars[Polars LazyFrame]
        SparkDS[Spark DataSource V2]
    end
    
    subgraph Maintenance["Maintenance Services"]
        Compact[Compaction]
        ZOrder[Z-Ordering]
        Vacuum[Vacuum]
    end
    
    Client --> Catalog
    Catalog --> Table
    Table --> Transaction
    Transaction --> Storage
    Table --> Query
    Query --> Storage
    Maintenance --> Table
    Maintenance --> Storage
```

## Component Breakdown

### 1. **Client Layer (`superbindings`, `spark-connector`)**
- **Python SDK**: PyO3-based bindings providing zero-copy Arrow integration.
- **Rust API**: Native, async-first API for high-performance applications.
- **Spark Connector**: Scala-based DataSource V2 implementation for Spark integration.

### 2. **Catalog Layer (`supercatalog`, `superrest`)**
- Manages table references and namespaces.
- **REST Catalog**: A fully Iceberg-compatible REST catalog implementation (`superrest`) with JWT auth.
- **In-Memory Catalog**: For testing and ephemeral workloads.

### 3. **Table & Transaction Layer (`supercore`)**
- **TableMetadata**: The heart of the format, tracking schema, snapshots, and properties.
- **Optimistic Concurrency Control (OCC)**: Ensures ACID properties by validating state before commit.
- **Snapshot Isolation**: Readers see a consistent snapshot while writers append new data.
- **Schema Evolution**: Supports adding, dropping, renaming, and type-widening columns safely.

### 4. **Storage Layer**
- Uses `object_store` crate for cloud-native I/O (S3, GCS, Azure, Local).
- **Data**: Stored as Parquet files with Z-std/Snappy compression.
- **Metadata**: Stored as versioned JSON files (`vN.metadata.json`) and Avro manifest lists.

### 5. **Query Integration (`superfusion`, `supertable-spark`)**
- **DataFusion**: Native `TableProvider` implementation pushing down filters and projections.
- **Polars**: Zero-copy Arrow memory sharing for ultra-fast dataframe operations.
- **Spark**: Reads/Writes via standard Spark APIs.

### 6. **Maintenance**
- **Compaction**: Merges small files into larger ones to optimize read performance.
- **Z-Ordering**: Multi-dimensional clustering to speed up queries on specific columns.
- **Vacuum**: Cleans up old snapshots and orphaned files to reclaim space.
