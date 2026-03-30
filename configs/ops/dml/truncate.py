from __future__ import annotations
from pydantic import BaseModel
from ...table import TrinoTableRef

class Truncate(BaseModel):
    """
    DML operation: truncate (empty) a table.

    Typically rendered as:
      - `TRUNCATE TABLE <catalog>.<schema>.<table>`

    Notes:
      - TRUNCATE removes all rows from the table but keeps the table definition.
      - Connector support varies; some connectors may translate this to a fast
        metadata operation, while others may not support it at all.
      - Consider enforcing a safety policy in validators if truncation should be
        restricted in your environment.
    """
    table: TrinoTableRef
