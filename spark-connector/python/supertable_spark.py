"""
PySpark wrapper for SuperTable Spark Connector.

Usage:
    from pyspark.sql import SparkSession
    from supertable_spark import configure_spark, read_table, write_table
    
    spark = configure_spark(SparkSession.builder).getOrCreate()
    
    # Read from SuperTable
    df = read_table(spark, "supertable.default.events")
    
    # Write to SuperTable
    write_table(df, "supertable.default.events")
"""

from typing import Dict, Optional, Any


def configure_spark(builder, catalog_uri: str = "http://localhost:8080") -> Any:
    """
    Configure SparkSession builder with SuperTable catalog.
    
    Args:
        builder: SparkSession.builder instance
        catalog_uri: URI of the SuperTable REST catalog
        
    Returns:
        Configured SparkSession builder
    """
    return (builder
        .config("spark.sql.catalog.supertable", "io.supertable.spark.SuperTableCatalog")
        .config("spark.sql.catalog.supertable.uri", catalog_uri)
        .config("spark.jars", _get_connector_jar_path())
    )


def read_table(spark, table_name: str) -> Any:
    """
    Read a SuperTable table as a Spark DataFrame.
    
    Args:
        spark: SparkSession instance
        table_name: Fully qualified table name (e.g., "supertable.default.events")
        
    Returns:
        Spark DataFrame
    """
    return spark.table(table_name)


def write_table(
    df,
    table_name: str,
    mode: str = "append",
    partition_by: Optional[list] = None,
    properties: Optional[Dict[str, str]] = None
) -> None:
    """
    Write a Spark DataFrame to a SuperTable table.
    
    Args:
        df: Spark DataFrame to write
        table_name: Fully qualified table name
        mode: Write mode - "append", "overwrite", "error", "ignore"
        partition_by: Optional list of columns to partition by
        properties: Optional table properties
    """
    writer = df.write.mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    if properties:
        for key, value in properties.items():
            writer = writer.option(key, value)
    
    writer.saveAsTable(table_name)


def create_table(
    spark,
    table_name: str,
    schema: str,
    partition_by: Optional[list] = None,
    properties: Optional[Dict[str, str]] = None
) -> None:
    """
    Create a new SuperTable table.
    
    Args:
        spark: SparkSession instance
        table_name: Fully qualified table name
        schema: SQL schema definition (e.g., "id BIGINT, name STRING")
        partition_by: Optional list of columns to partition by
        properties: Optional table properties
    """
    partition_clause = ""
    if partition_by:
        partition_clause = f" PARTITIONED BY ({', '.join(partition_by)})"
    
    props_clause = ""
    if properties:
        props = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])
        props_clause = f" TBLPROPERTIES ({props})"
    
    spark.sql(f"""
        CREATE TABLE {table_name} ({schema}){partition_clause}{props_clause}
    """)


def time_travel(spark, table_name: str, snapshot_id: Optional[int] = None, timestamp: Optional[str] = None) -> Any:
    """
    Query a historical version of the table.
    
    Args:
        spark: SparkSession instance
        table_name: Fully qualified table name
        snapshot_id: Specific snapshot ID to query
        timestamp: Timestamp string for AS OF query
        
    Returns:
        Spark DataFrame
    """
    if snapshot_id is not None:
        return spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {snapshot_id}")
    elif timestamp is not None:
        return spark.sql(f"SELECT * FROM {table_name} TIMESTAMP AS OF '{timestamp}'")
    else:
        return spark.table(table_name)


def optimize_table(spark, table_name: str, z_order_by: Optional[list] = None) -> None:
    """
    Optimize (compact) a SuperTable table.
    
    Args:
        spark: SparkSession instance
        table_name: Fully qualified table name
        z_order_by: Optional columns for Z-ordering
    """
    if z_order_by:
        z_order_clause = f" ZORDER BY ({', '.join(z_order_by)})"
    else:
        z_order_clause = ""
    
    spark.sql(f"OPTIMIZE {table_name}{z_order_clause}")


def vacuum_table(spark, table_name: str, retention_hours: int = 168) -> None:
    """
    Remove old files from a SuperTable table.
    
    Args:
        spark: SparkSession instance
        table_name: Fully qualified table name
        retention_hours: Retention period in hours (default 7 days)
    """
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")


def _get_connector_jar_path() -> str:
    """Get the path to the SuperTable Spark connector JAR."""
    import os
    import pkg_resources
    
    try:
        # Try to find installed JAR
        jar_path = pkg_resources.resource_filename('supertable_spark', 'jars/spark-supertable.jar')
        if os.path.exists(jar_path):
            return jar_path
    except Exception:
        pass
    
    # Fallback: assume JAR is in current directory
    return "spark-supertable-assembly-0.1.0.jar"
