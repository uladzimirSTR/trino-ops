from __future__ import annotations
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, model_validator

from ...table import TrinoTableRef

class InsertValues(BaseModel):
    """
    DML operation: insert explicit row values into a table.

    Typically rendered as:
      - `INSERT INTO <table> (c1, c2, ...) VALUES (v11, v12, ...), (v21, v22, ...)`

    Notes:
      - Correct SQL literal rendering is tricky (strings, timestamps, arrays, JSON,
        NULLs, escaping). Prefer `InsertSelect` when possible, or ensure your SQL
        renderer has a robust literal/value formatter.
      - This model enforces that each row has the same number of values as
        the `columns` list.
    """
    table: TrinoTableRef
    columns: List[str]
    rows: List[List[object]]  # rows aligned with columns

    @model_validator(mode="after")
    def _rows_match_columns(self):
        n = len(self.columns)
        bad = [i for i, r in enumerate(self.rows) if len(r) != n]
        if bad:
            raise ValueError(f"rows have different length than columns (bad rows indexes): {bad}")
        return self

class InsertSelect(BaseModel):
    """
    DML operation: insert the result of a SELECT query into a table.

    Typically rendered as:
      - `INSERT INTO <table> [(c1, c2, ...)] <select_sql>`

    Notes:
      - This is usually the most portable insert pattern in Trino, especially for
        external tables, because it avoids manual literal formatting.
      - If `columns` is provided, it must match the order and number of columns
        produced by the SELECT query.
    """
    table: TrinoTableRef
    columns: Optional[List[str]] = None
    select_sql: str  # simplest: raw select statement
