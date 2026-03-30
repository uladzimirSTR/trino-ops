from __future__ import annotations
from typing import List
from pydantic import BaseModel
from ...table import TrinoTableRef

class SetPartitioning(BaseModel):
    """
    DDL operation: update the table partitioning configuration.

    In Trino (Hive connector), partitioning is typically controlled via table
    properties (e.g., `WITH (...)` on CREATE or `ALTER TABLE ... SET PROPERTIES`).
    This model represents the *intent* to set partition keys, while the renderer
    decides how to express it in SQL for the target connector.

    Notes:
      - `partitioned_by` should contain existing column names.
      - Most setups expect partition columns to be primitive types (e.g., date,
        varchar, bigint). Complex types are usually not valid partition keys.
      - Connector support varies; treat this as declarative configuration rather
        than a guarantee of runtime support.
    """
    table: TrinoTableRef
    partitioned_by: List[str]

class SetTableLocation(BaseModel):
    """
    DDL operation: change the table's external storage location.

    For S3-backed Hive tables, this is usually expressed via the
    `external_location` table property.

    Notes:
      - The location should be a directory-like URI (no glob patterns like `*`).
      - Changing location affects where new data is read/written and can orphan
        existing files if not migrated.
    """
    table: TrinoTableRef
    external_location: str

class SetFileFormat(BaseModel):
    """
    DDL operation: change the table's storage file format.

    For Hive connector tables, this is typically the `format` property (e.g.,
    PARQUET, ORC, AVRO).

    Notes:
      - Changing the format does not convert existing data; it only affects how
        Trino interprets / writes files going forward (connector-dependent).
      - Prefer validating the allowed formats at the model layer (Literal/Enum).
    """
    table: TrinoTableRef
    format: str
