from __future__ import annotations

# from .interfaces import SqlExecutor
from ..db.executer import SQLClient
from trino_crud.mixins import HasConf


class MakeClientMixin(HasConf):
    """
    Mixin that constructs an :class:`~..db.executer.SQLClient` using the instance's
    :pyattr:`conf` (expected to be a :class:`~..configs.auth.TrinoConnectionConfig`).

    The main purpose is to centralize client creation and ensure TLS verification
    is resolved into the concrete value expected by the underlying Trino/HTTP stack.

    Expectations:
        - The inheriting class provides a ``conf`` attribute/property returning a
          ``TrinoConnectionConfig`` instance.
        - ``TrinoConnectionConfig.resolved_verify()`` returns the effective ``verify``
          value (e.g. boolean or path to CA bundle) to be used for the connection.

    Methods:
        make_client(): Builds and returns a configured ``SQLClient`` instance.

    Notes:
        - A copied config is used (via ``model_copy``) to avoid mutating the original
          configuration object stored on the instance.
    """
    def make_client(self) -> "SQLClient":
        cfg = self.conf
        return SQLClient(cfg.model_copy(update={"verify": cfg.resolved_verify()}))
