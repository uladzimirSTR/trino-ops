from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, Field

from ...table import TrinoTableRef

class Merge(BaseModel):
    """
    DML operation: merge data from a source query into a target table.

    Typically rendered as:
      - `MERGE INTO <target> AS t`
      - `USING <source_sql> AS s`
      - `ON <predicate>`
      - `WHEN MATCHED THEN ...`
      - `WHEN NOT MATCHED THEN ...`

    Attributes:
      - target: The destination table to be updated/inserted into.
      - source_sql: A source relation, commonly a subquery in parentheses
        (e.g. `(SELECT ... )`) or a table reference rendered as SQL.
      - on: The join predicate between target and source (raw SQL).
      - when_matched: One or more actions to execute when rows match the `on`
        condition (e.g., `UPDATE SET ...`, `DELETE`).
      - when_not_matched: One or more actions to execute when rows do not match
        (e.g., `INSERT (...) VALUES (...)`).

    Notes:
      - MERGE support is connector/table-format dependent in Trino. It is commonly
        available for Iceberg/Delta/Hudi, but often unavailable for plain Hive/S3
        external tables.
      - This model is declarative and does not validate the SQL fragments inside
        `source_sql` / `on` / action clauses.
    """
    target: TrinoTableRef
    source_sql: str                  # e.g. "(SELECT ... )"
    on: str                          # join predicate
    when_matched: Optional[List[str]] = None     # e.g. ["UPDATE SET ...", "DELETE"]
    when_not_matched: Optional[List[str]] = None # e.g. ["INSERT (...) VALUES (...)"]
