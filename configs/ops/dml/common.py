from __future__ import annotations
from typing import Dict, List
from pydantic import BaseModel, Field

from ...table import TrinoTableRef

class SqlExpr(BaseModel):
    """
    A small wrapper for a raw SQL expression.

    This is intentionally lightweight: it does not try to parse SQL into an AST.
    It exists to make intent explicit in configs (e.g., computed columns,
    predicates, join conditions) while leaving correctness to the user and
    rendering/execution layers.
    """
    sql: str

class Query(BaseModel):
    """
    A raw SQL query with optional parameters.

    Notes:
      - Parameter binding is client-specific in Trino (different DB-API wrappers
        handle parameters differently). This model stores parameters as intent.
      - Prefer using parameters for values (when your client supports it) to avoid
        SQL injection and quoting issues.
    """
    sql: str
    params: Dict[str, object] = Field(default_factory=dict)

class Where(BaseModel):
    """
    A WHERE-clause predicate expressed as raw SQL.

    Example:
        Where(sql='"dt" >= DATE \'2026-01-01\' AND "country" = \'PL\'')
    """
    sql: str

class OrderBy(BaseModel):
    """
    ORDER BY configuration.

    Attributes:
      - cols: List of order-by terms (e.g., ['"ts"', '"event_id"'] or
        ['"ts" DESC', '"event_id" ASC'] if you want per-column direction).
      - otype: Default ordering direction applied by the renderer when `cols`
        contains only column names. Usually "ASC" or "DESC".

    Notes:
      - For advanced ordering (different directions per column, NULLS FIRST/LAST),
        it is often simplest to encode direction directly inside `cols`.
    """
    cols: List[str]  # "col", "col"
    otype: str = "ASC"  # Default ordering type (if not specified in items)
