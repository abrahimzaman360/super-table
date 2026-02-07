"""
PySuperTable - Python bindings for SuperTable
"""
from .supertable import *
from . import integrations

# Monkey pack integrations
PyTable.to_polars = integrations.to_polars
PyTable.to_pandas = integrations.to_pandas
PyTable.to_duckdb = integrations.to_duckdb
PyTable.to_spark = integrations.to_spark

__version__ = "0.1.1"
__all__ = [
    "Schema",
    "Field",
    "Table",
    "create_table",
    "open_table",
    "PyTable",
]
