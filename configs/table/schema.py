from __future__ import annotations
from pydantic import BaseModel, Field, field_validator


def _is_s3_location(loc: str) -> bool:
    """
    Return True if the given location string looks like an S3-style URI.

    This helper accepts common schemes used with Trino/Hive and Hadoop-style
    filesystems:
      - s3://
      - s3a://
      - s3n://

    Notes:
      - This is a lightweight syntactic check (prefix-only). It does not validate
        bucket names, paths, credentials, or connectivity.
    """
    # Accept typical Hadoop-ish + AWS-ish schemes used around Trino/Hive.
    return loc.startswith(("s3://", "s3a://", "s3n://"))


class TrinoSchema(BaseModel):
    """
    Trino schema (namespace) configuration.

    Attributes:
      - catalog: Trino catalog name (e.g., "hive", "datalake"). Defaults to
        "datalake".
      - name: Schema name inside the catalog (e.g., "analytics").
      - location: Default storage root for the schema. For Hive-like connectors
        on S3, this is commonly used as the base path for tables.

    Notes:
      - `location` is validated to be an S3-style URI (s3://, s3a://, s3n://).
      - This model is used by DDL operations (CREATE/DROP SCHEMA) and by table
        models to build fully qualified references and default external locations.
    """
    catalog: str = Field(default="datalake")
    name: str
    location: str = Field(default="s3a://prod-datalake/")

    @field_validator("location")
    @classmethod
    def _validate_location(cls, v: str) -> str:
        if not _is_s3_location(v):
            raise ValueError("location must be an S3 URI: s3://, s3a://, or s3n://")
        return v
