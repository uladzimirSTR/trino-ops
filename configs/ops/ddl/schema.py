from __future__ import annotations
from pydantic import BaseModel
from ...table import TrinoSchema

class CreateSchema(BaseModel):
    """
    DDL operation: create a schema (database/namespace) in Trino.

    Typically rendered as:
      - `CREATE SCHEMA [IF NOT EXISTS] <catalog>.<schema> WITH (location = '...')`

    Notes:
      - For Hive-like connectors on S3, the schema `location` is commonly used
        to define the default root path for tables created inside the schema.
      - This model is declarative; actual SQL generation and execution are handled
        elsewhere.
    """
    table_schema: TrinoSchema
    if_not_exists: bool = True

class DropSchema(BaseModel):
    """
    DDL operation: drop a schema (database/namespace) in Trino.

    Typically rendered as:
      - `DROP SCHEMA [IF EXISTS] <catalog>.<schema> [CASCADE]`

    Notes:
      - `cascade=True` usually drops dependent objects (tables/views) as well,
        but behavior can vary by connector and permissions.
      - Dropping a schema is destructive and may not remove underlying files
        in external storage automatically.
    """
    table_schema: TrinoSchema
    if_exists: bool = True
    cascade: bool = False
