from __future__ import annotations

from typing import Any

from trino.dbapi import Cursor

from .client import Client
from .typevar import C


class SQLClient(Client[C]):
    """
    Convenience extension of `Client` that executes SQL and returns results.

    This class provides a simple `execute()` helper built on top of the Trino
    DB-API cursor interface.

    Notes:
      - `execute()` fetches all rows into memory. For large result sets, consider
        adding streaming helpers (e.g., `fetchmany()` loops or an iterator API).
      - Transaction semantics depend on Trino/connector behavior; many operations
        are effectively autocommit at the engine/connector level.
    """

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        """
        Execute a SQL statement and return all rows.

        Args:
          sql: SQL string to execute.

        Returns:
          A list of result rows as tuples.

        Raises:
          Any exceptions raised by the underlying Trino client (connection errors,
          query failures, etc.) are propagated to the caller.
        """
        cur: Cursor = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
