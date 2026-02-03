"""Type stubs for supertable Python bindings."""
from typing import Any, Dict, List, Optional, Union
import datetime

class Field:
    """Represents a schema field."""
    
    def __init__(
        self,
        id: int,
        name: str,
        field_type: str,
        required: bool = False,
        doc: Optional[str] = None,
    ) -> None: ...
    
    @property
    def id(self) -> int: ...
    @property
    def name(self) -> str: ...
    @property
    def field_type(self) -> str: ...
    @property
    def required(self) -> bool: ...

class Schema:
    """Represents a table schema."""
    
    def __init__(self, fields: List[Field]) -> None: ...
    
    @property
    def fields(self) -> List[Field]: ...
    
    def add_column(self, name: str, field_type: str, required: bool = False) -> "Schema": ...
    def drop_column(self, name: str) -> "Schema": ...
    def rename_column(self, old_name: str, new_name: str) -> "Schema": ...

class PyTable:
    """Represents a SuperTable table."""
    
    @property
    def location(self) -> str: ...
    
    @property
    def schema(self) -> Schema: ...
    
    @property
    def current_snapshot_id(self) -> Optional[int]: ...
    
    def sql(self, query: str) -> Any:
        """Execute a SQL query against the table."""
        ...
    
    def to_arrow(self) -> Any:
        """Convert table to PyArrow Table."""
        ...
    
    def to_pandas(self) -> Any:
        """Convert table to Pandas DataFrame."""
        ...
    
    def snapshot_at(self, timestamp: Union[str, datetime.datetime]) -> "PyTable":
        """Get a historical view of the table at the given timestamp."""
        ...
    
    def snapshot_by_id(self, snapshot_id: int) -> "PyTable":
        """Get a historical view of the table for the given snapshot ID."""
        ...
    
    def add_column(self, name: str, field_type: str, required: bool = False) -> None:
        """Add a new column to the table schema."""
        ...
    
    def drop_column(self, name: str) -> None:
        """Drop a column from the table schema."""
        ...
    
    def rename_column(self, old_name: str, new_name: str) -> None:
        """Rename a column in the table schema."""
        ...
    
    def append(self, data: Any) -> None:
        """Append data to the table."""
        ...
    
    def overwrite(self, data: Any) -> None:
        """Overwrite table data."""
        ...
    
    def delete(self, predicate: str) -> int:
        """Delete rows matching the predicate. Returns number of rows deleted."""
        ...
    
    def update(self, predicate: str, updates: Dict[str, Any]) -> int:
        """Update rows matching the predicate. Returns number of rows updated."""
        ...
    
    def compact(self) -> None:
        """Compact small files in the table."""
        ...
    
    def vacuum(self, older_than_days: int = 7) -> int:
        """Remove old snapshots and orphan files. Returns number of files removed."""
        ...

def create_table(
    location: str,
    schema: Schema,
    partition_by: Optional[List[str]] = None,
    properties: Optional[Dict[str, str]] = None,
) -> PyTable:
    """Create a new SuperTable at the given location."""
    ...

def open_table(location: str) -> PyTable:
    """Open an existing SuperTable at the given location."""
    ...
