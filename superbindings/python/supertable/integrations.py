import importlib

def _check_import(module_name, extra_name):
    """Helper to check if a module is installed."""
    if importlib.util.find_spec(module_name) is None:
        raise ImportError(
            f"Missing dependency '{module_name}'. "
            f"Please install it with: pip install pysupertable[{extra_name}]"
        )

def to_polars(self):
    """
    Convert the table to a Polars DataFrame.
    """
    _check_import("polars", "polars")
    import polars as pl
    
    # Use zero-copy Arrow conversion
    arrow_table = self.to_arrow()
    return pl.from_arrow(arrow_table)

def to_pandas(self):
    """
    Convert the table to a Pandas DataFrame.
    """
    _check_import("pandas", "pandas")
    
    # Use zero-copy Arrow conversion
    arrow_table = self.to_arrow()
    return arrow_table.to_pandas()

def to_duckdb(self, connection=None):
    """
    Register the table as a DuckDB view or return a DuckDB relation.
    
    Args:
        connection: Optional DuckDB connection. If None, uses in-memory default.
    """
    _check_import("duckdb", "duckdb")
    import duckdb
    
    conn = connection or duckdb.connect()
    
    arrow_table = self.to_arrow()
    return conn.from_arrow(arrow_table)

def to_spark(self, spark):
    """
    Convert the table to a Spark DataFrame.
    
    Args:
        spark: The SparkSession object.
    
    Returns:
        pyspark.sql.DataFrame
    """
    try:
        # Approach 1: Native Read (Requires supertable-spark.jar)
        # This is preferred as it supports pushdown and parallelism
        return spark.read.format("supertable").load(self.location)
    except Exception:
        # Approach 2: Fallback to Arrow (Driver-side conversion)
        # Warning: This pulls all data to the driver!
        import warnings
        warnings.warn(
            "Native Spark reader failed (missing JAR?). Falling back to Arrow conversion. "
            "This may be slow for large datasets as it collects data to the driver."
        )
        _check_import("pandas", "pandas")
        arrow_table = self.to_arrow()
        # Spark < 3.3 might not support createDataFrame from Arrow directly, go via Pandas check?
        # Modern Spark supports Arrow.
        try:
           return spark.createDataFrame(arrow_table.to_pandas())
        except:
           # Last resort
           return spark.createDataFrame(arrow_table.to_pylist())
