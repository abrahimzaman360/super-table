"""
PySuperTable - Python bindings for SuperTable
"""
from .supertable import *

__version__ = "0.1.0"
__all__ = [
    "Schema",
    "Field",
    "Table",
    "create_table",
    "open_table",
    "PyTable",
]
