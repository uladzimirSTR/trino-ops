
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


# from .interfaces import SqlExecutor
from trino_ops.db.executer import SQLClient


class ExecuteQueryMixin:
    """
    Mixin that provides a convenience method to execute a raw SQL string against Trino.

    The mixin opens an :class:`~trino_ops.db.executer.SQLClient` using ``self.conf``,
    executes the provided SQL, and returns all rows as a list of tuples.

    Expectations:
        - The inheriting class provides a ``conf`` attribute/property with the
          connection configuration accepted by ``SQLClient``.

    Args:
        sql: SQL statement to execute (query or command supported by the client).

    Returns:
        A list of result rows, where each row is a tuple of column values.

    Notes:
        - A new client session is created per call and is closed automatically via
          the context manager.
        - Any exceptions raised by the underlying client/driver are propagated.
    """
    def execute_query(self, sql: str) -> list[tuple[Any, ...]]:
        with SQLClient(self.conf) as c:
            return c.execute(sql)
