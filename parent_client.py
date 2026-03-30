
from __future__ import annotations

from typing import Optional, Union

from pathlib import Path

# from .interfaces import SqlExecutor
from trino_ops_utils.configs.auth import BasicAuthConfig
from trino_ops_utils.mixins import (
    MakeConfigMixin,
    MakeClientMixin,
    ExecuteQueryMixin,
    MakeRenderMixin,
    MakeSchemaTableMixin,
    RenderExecMixin,
)
from trino_ops_utils.sql.validators import (
    ValidationContext,
    Capabilities
)


class ParentClient(
    MakeConfigMixin,
    MakeClientMixin,
    ExecuteQueryMixin,
    MakeRenderMixin,
    MakeSchemaTableMixin,
    RenderExecMixin,
):
    """
    High-level client composed from multiple mixins to simplify working with Trino.

    ``ParentClient`` is a convenience façade that bundles together:
      - connection config construction (:class:`MakeConfigMixin`)
      - SQL client creation (:class:`MakeClientMixin`)
      - raw SQL execution (:class:`ExecuteQueryMixin`)
      - operation rendering (:class:`MakeRenderMixin`)
      - schema/table object builders (:class:`MakeSchemaTableMixin`)
      - operation validation + execution (:class:`RenderExecMixin`)

    The instance stores connection parameters as plain attributes and exposes a
    canonical config via ``self.conf`` (provided by ``MakeConfigMixin``).

    Args:
        host: Trino coordinator hostname or IP.
        port: Trino coordinator port.
        user: Username used for the Trino session.
        catalog: Default Trino catalog to use.
        http_scheme: Connection scheme, typically ``"http"`` or ``"https"``.
        auth: Authentication configuration (e.g. ``BasicAuthConfig``).
        verify: TLS certificate verification behavior. Use ``True``/``False`` or a
            path to a CA bundle as a string (depending on the underlying HTTP client).
        path_to_pem: Optional directory containing a PEM file used for TLS verification.
        file_name_pem: Optional PEM file name within ``path_to_pem``.

    Notes:
        - This class intentionally keeps the constructor minimal and delegates most
          behaviors to mixins. The mixin order matters for method resolution (MRO).
        - ``verify``, ``path_to_pem`` and ``file_name_pem`` are passed through to the
          underlying connection config and may be resolved into an effective verify
          setting depending on your ``TrinoConnectionConfig`` implementation.
    """
    # Hive/S3 typical profile: UPDATE/DELETE/MERGE off
    ctx = ValidationContext(
        capabilities=Capabilities(
            allow_select=True,
            allow_insert=True,
            allow_ctas=True,
            allow_truncate=True,
            allow_delete=True,
            allow_update=True,
            allow_merge=True,
            allow_insert_values=True,
        )
    )

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        catalog: str,
        http_scheme: str,
        auth: BasicAuthConfig,
        verify: Optional[Union[bool, str]] = None,
        path_to_pem: Optional[Path] = None,
        file_name_pem: Optional[str] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.http_scheme = http_scheme
        self.auth = auth
        self.path_to_pem = path_to_pem
        self.file_name_pem = file_name_pem
        self.verify = verify
