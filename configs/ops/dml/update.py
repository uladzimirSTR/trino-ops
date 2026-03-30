from __future__ import annotations
from typing import Dict, Optional
from pydantic import BaseModel, Field

from .common import Where
from ...table import TrinoTableRef

class Update(BaseModel):
    """
    DML operation: update rows in a table.

    Typically rendered as:
      - `UPDATE <table> SET col1 = val1, col2 = val2 WHERE <predicate>`
      - If `where` is omitted: `UPDATE <table> SET ...` (full table update)

    Attributes:
      - table: Target table reference.
      - set: Mapping of column name -> value. Values are treated as "raw" Python
        objects that must be converted into SQL literals by the renderer.
      - where: Optional WHERE predicate wrapper.

    Notes:
      - Connector support varies. For Hive/S3 external tables, UPDATE is often not
        supported unless using table formats/connectors like Iceberg/Delta/Hudi.
      - Allowing `where=None` is intentional but dangerous; consider enforcing a
        safety policy in validators (e.g., forbid update without WHERE).
      - For complex updates, you may prefer a raw SQL `Query`.
    """
    table: TrinoTableRef
    set: Dict[str, object] = Field(default_factory=dict)  # col -> value (raw)
    where: Optional[Where] = None
