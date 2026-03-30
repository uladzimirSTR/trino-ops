from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, Field

from .common import OrderBy, Query, Where
from ...table import TrinoTableRef

class Select(BaseModel):
    """
    DML operation: build a simple SELECT query for a single table.

    Typically rendered as:
      - `SELECT <columns> FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT ...]`

    Attributes:
      - table: Fully qualified table reference (catalog + schema + table).
      - columns: List of column names or raw SQL expressions to select.
        Defaults to `["*"]`.
      - where: Optional WHERE predicate wrapper.
      - order_by: Optional ORDER BY configuration.
      - limit: Optional LIMIT value.

    Notes:
      - `columns` are treated as raw SQL fragments by the renderer. If you pass
        unquoted identifiers, they will be emitted as-is.
      - For complex queries (joins, CTEs, grouping, window functions), prefer
        `SelectQuery` (raw SQL) or introduce richer query models.
    """
    table: TrinoTableRef
    columns: List[str] = Field(default_factory=lambda: ["*"])
    where: Optional[Where] = None
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None

class SelectQuery(BaseModel):
    """
    DML operation: execute/render a raw SELECT (or any) SQL query.

    Use this when the query structure is too complex for the lightweight `Select`
    model (joins, CTEs, subqueries, grouping, etc.).

    Notes:
      - Parameters in `Query.params` are stored as intent; actual binding depends
        on your Trino client/driver.
    """
    query: Query
