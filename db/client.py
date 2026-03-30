from __future__ import annotations

from typing import Generic, Optional

from trino.dbapi import Connection, connect
from .typevar import C


class Client(Generic[C]):
    """
    Lightweight Trino DB-API client wrapper with lazy connection management.

    This class stores a validated connection config and opens the underlying
    Trino connection only when it is first needed (`conn` property). It also
    supports usage as a context manager to guarantee cleanup.

    Type parameters:
      - C: A connection config type that provides `to_trino_kwargs() -> dict`
        compatible with `trino.dbapi.connect(**kwargs)`.
    """

    def __init__(self, cfg: C):
        """
        Initialize the client with a connection configuration.

        Args:
          cfg: Configuration object used to build keyword arguments for
               `trino.dbapi.connect`.
        """
        self.cfg: C = cfg
        self._conn: Optional[Connection] = None

    @property
    def conn(self) -> Connection:
        """
        Return an active Trino connection, creating it lazily if needed.

        Returns:
          A `trino.dbapi.Connection` instance.
        """
        if self._conn is None:
            self._conn = connect(**self.cfg.to_trino_kwargs())
        return self._conn

    def close(self) -> None:
        """
        Close the underlying connection if it is open.

        After calling this method, the next access to `conn` will create a new
        connection.
        """
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "Client[C]":
        """
        Enter context manager scope and ensure the connection is initialized.

        Returns:
          Self, with an established connection ready for use.
        """
        _ = self.conn
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """
        Exit context manager scope and close the connection.

        Args:
          exc_type: Exception type (if raised inside the context).
          exc: Exception instance (if raised inside the context).
          tb: Traceback (if raised inside the context).

        Notes:
          - Exceptions are not suppressed; this method always returns None.
        """
        self.close()
