from __future__ import annotations

from typing import List
from pydantic import BaseModel
from ...table import Column, TrinoTableRef

class AddColumns(BaseModel):
    """
    DDL operation: add one or more columns to an existing table.

    Notes:
      - This model is purely declarative (it describes the intent).
      - Rendering/execution is handled by the SQL renderer/executor layer.
      - Some connectors may restrict adding complex types or changing column order.
    """
    table: TrinoTableRef
    columns: List[Column]

class DropColumns(BaseModel):
    """
    DDL operation: drop one or more columns from an existing table.

    Notes:
      - Depending on connector capabilities, dropping columns may require one
        statement per column (the renderer can emit multiple statements).
      - Dropping columns is irreversible and may break downstream queries.
    """
    table: TrinoTableRef
    colnames: List[str]

class RenameColumn(BaseModel):
    """
    DDL operation: rename a column in an existing table.

    Notes:
      - Renaming a column changes the table schema and can break consumers.
      - Some connectors may have limitations on renaming columns.
    """
    table: TrinoTableRef
    old_name: str
    new_name: str
