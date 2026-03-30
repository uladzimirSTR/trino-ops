from __future__ import annotations
from typing import Optional
from pydantic import BaseModel

from .common import Where
from ...table import TrinoTableRef

class Delete(BaseModel):
    """
    DML operation: delete rows from a table.

    Typically rendered as:
      - `DELETE FROM <catalog>.<schema>.<table> WHERE <predicate>`
      - If `where` is omitted: `DELETE FROM <...>` (full table delete)

    Notes:
      - Connector support varies. For Hive/S3 tables, DELETE is often not supported
        unless using table formats/connectors like Iceberg/Delta/Hudi.
      - Allowing `where=None` is intentional but dangerous; consider enforcing a
        safety policy in validators (e.g., forbid delete without WHERE).
    """
    table: TrinoTableRef
    where: Optional[Where] = None  # allow full delete if None (dangerous but real)
