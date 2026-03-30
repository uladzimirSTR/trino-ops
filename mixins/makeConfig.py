from __future__ import annotations

from typing import Optional, Union
from pathlib import Path


# from .interfaces import SqlExecutor
from ..configs.auth import TrinoConnectionConfig, BasicAuthConfig


class MakeConfigMixin:
    """
    Mixin that builds a :class:`~..configs.auth.TrinoConnectionConfig` from instance attributes.

    This class is intended to be inherited by clients/executors that store connection
    settings as plain attributes (e.g. ``host``, ``port``, ``user``) but want a single,
    canonical Pydantic config object via the :pyattr:`conf` property.

    Required attributes (must be provided by the subclass/instance):
        host: Trino coordinator hostname or IP.
        port: Trino coordinator port.
        user: Username used for the Trino session.
        catalog: Default Trino catalog to use.
        http_scheme: Connection scheme, typically ``"http"`` or ``"https"``.
        auth: Authentication config (e.g. :class:`~..configs.auth.BasicAuthConfig`).

    Optional TLS / verification attributes:
        verify: Certificate verification behavior. Use ``True``/``False`` or a path
            to a CA bundle as a string (depending on the underlying HTTP client).
        path_to_pem: Directory containing a PEM file used for TLS verification.
        file_name_pem: PEM file name within ``path_to_pem``.

    Notes:
        - The mixin does not validate that required attributes exist; that responsibility
          stays with the inheriting class (or your type checker).
        - The :pyattr:`conf` property creates a new config object on each access.
    """
    host: str
    port: int
    user: str
    catalog: str
    http_scheme: str
    auth: BasicAuthConfig

    verify: Optional[Union[bool, str]] = None
    path_to_pem: Optional[Path] = None
    file_name_pem: Optional[str] = None

    @property
    def conf(self) -> TrinoConnectionConfig:
        cfg = TrinoConnectionConfig(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            http_scheme=self.http_scheme,
            auth=self.auth, # .model_dump(),
            verify=self.verify,
            path_to_pem=self.path_to_pem,
            file_name_pem=self.file_name_pem,
        )
        return cfg
