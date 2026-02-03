# SuperTable Spark Connector

Apache Spark connector for SuperTable tables using the DataSource V2 API.

## Features

- ✅ Catalog integration (DDL operations)
- ✅ Batch reads via Parquet
- ✅ Batch writes with ACID transactions
- ✅ Schema evolution support
- ✅ Time travel queries
- ✅ REST Catalog communication

## Installation

### Scala/Java

```bash
# Build the JAR
sbt assembly

# Add to Spark
spark-shell --jars target/scala-2.12/spark-supertable-assembly-0.1.0.jar
```

### PySpark

```bash
pip install pysupertable[spark]
```

## Configuration

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.supertable", "io.supertable.spark.SuperTableCatalog") \
    .config("spark.sql.catalog.supertable.uri", "http://localhost:8080") \
    .getOrCreate()
```

## Usage

### SQL

```sql
-- Create a table
CREATE TABLE supertable.default.events (
    id BIGINT,
    event_type STRING,
    timestamp BIGINT
) PARTITIONED BY (event_type);

-- Query
SELECT * FROM supertable.default.events WHERE event_type = 'click';

-- Time travel
SELECT * FROM supertable.default.events VERSION AS OF 1234567890;

-- Schema evolution
ALTER TABLE supertable.default.events ADD COLUMN user_id BIGINT;

-- Optimize
OPTIMIZE supertable.default.events ZORDER BY (user_id, timestamp);

-- Vacuum
VACUUM supertable.default.events RETAIN 168 HOURS;
```

### PySpark API

```python
from supertable_spark import configure_spark, read_table, write_table, time_travel

# Configure Spark
spark = configure_spark(SparkSession.builder).getOrCreate()

# Read
df = read_table(spark, "supertable.default.events")

# Write
write_table(df, "supertable.default.events", mode="append")

# Time travel
historical_df = time_travel(spark, "supertable.default.events", snapshot_id=12345)
```

### Scala API

```scala
// Create table
spark.sql("""
  CREATE TABLE supertable.default.events (
    id BIGINT,
    event_type STRING
  )
""")

// Read
val df = spark.table("supertable.default.events")

// Write
df.write
  .mode("append")
  .saveAsTable("supertable.default.events")
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Spark Application                     │
├─────────────────────────────────────────────────────────┤
│    SuperTableCatalog (DDL, namespace, table mgmt)       │
├─────────────────────────────────────────────────────────┤
│  SuperTableSparkTable (read/write capabilities)         │
├─────────────────────────────────────────────────────────┤
│  SuperTableScan        │     SuperTableWrite            │
│  (partition planning)   │     (batch writes)            │
├─────────────────────────────────────────────────────────┤
│            SuperTableRestClient (HTTP/JSON)             │
├─────────────────────────────────────────────────────────┤
│                 SuperTable REST Catalog                  │
│                 (http://localhost:8080)                  │
└─────────────────────────────────────────────────────────┘
```

## Compatibility

| Spark Version | Status |
|---------------|--------|
| 3.3.x | ✅ |
| 3.4.x | ✅ |
| 3.5.x | ✅ |

## License

Apache-2.0 OR MIT
