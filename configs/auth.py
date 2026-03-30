from __future__ import annotations

from typing import Annotated, Literal, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator
from pathlib import Path

from trino.auth import (
    BasicAuthentication,
    KerberosAuthentication,
    JWTAuthentication,
    # LDAPAuthentication,
    # CustomAuthentication,
)


class BasicAuthConfig(BaseModel):
    """
    Configuration for Trino Basic authentication (username + password).

    This corresponds to `trino.auth.BasicAuthentication`.
    """
    type: Literal["basic"] = "basic"
    username: str
    password: str


class KerberosAuthConfig(BaseModel):
    """
    Configuration for Trino Kerberos authentication.

    This corresponds to `trino.auth.KerberosAuthentication`.

    Attributes:
      - config: Optional path to a Kerberos config (client/krb5 settings), depending
        on how your environment is set up.
      - service_name: Optional Kerberos service principal name (e.g., "trino").
    """
    type: Literal["kerberos"] = "kerberos"
    config: Optional[str] = None
    service_name: Optional[str] = None


class JWTAuthConfig(BaseModel):
    """
    Configuration for Trino JWT authentication.

    This corresponds to `trino.auth.JWTAuthentication`.

    Attributes:
      - token: Bearer token (JWT) used for authentication.
    """
    type: Literal["jwt"] = "jwt"
    token: str


class LDAPAuthConfig(BaseModel):
    """
    Configuration placeholder for LDAP authentication.

    Notes:
      - The official python `trino` client has historically focused on Basic/JWT/
        Kerberos. LDAP support may require a specific auth class or a custom
        authentication implementation in your codebase.
      - Keep this model only if you plan to implement/enable LDAP auth in the
        client layer.
    """
    type: Literal["ldap"] = "ldap"
    username: str
    password: str


class CustomAuthConfig(BaseModel):
    """
    Configuration for a custom authentication strategy.

    Use this when authentication is not covered by the built-in Trino auth
    implementations and you want to instantiate auth via a factory/registry.

    Attributes:
      - factory_name: Name/key used to resolve an auth factory in your code.
      - params: Arbitrary parameters passed to the factory.
    """
    type: Literal["custom"] = "custom"

    factory_name: str

    params: dict = Field(default_factory=dict)


AuthConfig = Annotated[
    Union[BasicAuthConfig, KerberosAuthConfig, JWTAuthConfig, LDAPAuthConfig],
    Field(discriminator="type"),
]


class TrinoConnectionConfig(BaseModel):
    """
    Connection configuration for the Trino Python client.

    This model is designed to be converted into keyword arguments compatible with:
      - `trino.dbapi.connect(**kwargs)`
      - `trino.client.TrinoRequest(...)` (via the same connect kwargs pattern)

    Attributes:
      - host: Trino coordinator hostname or IP.
      - port: Trino coordinator port (default: 443).
      - user: Trino user name (sent as the Trino session user).
      - catalog: Optional default catalog for the session.
      - schema_name: Optional default schema for the session. Aliased as `schema`
        for convenience in config files.
      - http_scheme: "http" or "https" (default: "https").
      - verify: TLS verification setting.
          * True  -> verify certificates using system trust store
          * False -> disable verification (not recommended)
          * str   -> path to a CA bundle file / directory
      - auth: Optional authentication config (discriminated union by `type`).

    Notes:
      - This model is intentionally transport-focused. Session properties,
        extra headers, roles, etc. should be modeled separately if needed.
    """
    host: str
    port: int = 443
    user: str
    catalog: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias='schema')
    http_scheme: Literal["http", "https"] = "https"
    auth: Optional[AuthConfig] = None
    verify: Optional[Union[bool, str]] = None
    
    path_to_pem: Optional[Path] = None
    file_name_pem: Optional[str] = None

    @model_validator(mode="after")
    def validate_pem_pair(self):
        a = self.path_to_pem is not None
        b = self.file_name_pem is not None
        if a ^ b:  # XOR
            raise ValueError("Fields 'path_to_pem' and 'file_name_pem' must be provided together.")
        return self

    @field_validator("verify", mode="before")
    def validate_verify(cls, v):
        """
        Validate the `verify` field to ensure it's a boolean or a valid file path.

        This allows users to specify TLS verification settings flexibly while
        ensuring that invalid configurations are caught early.

        Args:
          v: The value to validate (expected to be bool, str, or None).

        Returns:
          The validated value if it's valid.

        Raises:
          ValueError if the value is not a bool, str, or None, or if it's a string
          that does not correspond to an existing file path.
        """
        if isinstance(v, bool) or v is None:
            return v
        if isinstance(v, str):
            path = Path(v)
            if path.is_file() or path.is_dir():
                return v
            raise ValueError(f"Invalid 'verify' path: {v} does not exist as a file or directory.")
        raise ValueError(f"Invalid type for 'verify': expected bool, str, or None but got {type(v).__name__}.")


    def build_auth(self):
        """
        Build a `trino.auth.*Authentication` instance from the configured `auth`.

        Returns:
          - An authentication object accepted by `trino.dbapi.connect(auth=...)`,
            or None if `auth` is not configured.

        Raises:
          - ValueError if the auth type is unknown/unsupported by this builder.
        """
        if self.auth is None:
            return None

        a = self.auth
        if a.type == "basic":
            return BasicAuthentication(a.username, a.password)
        if a.type == "kerberos":
            return KerberosAuthentication(config=a.config, service_name=a.service_name)
        if a.type == "jwt":
            return JWTAuthentication(a.token)
        # if a.type == "ldap":
        #     return LDAPAuthentication(a.username, a.password)

        raise ValueError(f"Unsupported auth type: {getattr(a, 'type', None)}")
    
    def to_trino_kwargs(self) -> dict:
        """
        Build kwargs exactly for trino.dbapi.connect(**kwargs) / trino.client.TrinoRequest
        """
        kwargs = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "http_scheme": self.http_scheme,
            "verify": self.verify,
        }

        if self.catalog is not None:
            kwargs["catalog"] = self.catalog
        if self.schema_name is not None:
            kwargs["schema"] = self.schema_name
        
        if self.path_to_pem is not None and self.file_name_pem is not None:
            kwargs["verify"] = str(self.path_to_pem / self.file_name_pem)
        else:
            kwargs["verify"] = self.verify

        auth_obj = self.build_auth()
        if auth_obj is not None:
            kwargs["auth"] = auth_obj

        return kwargs
