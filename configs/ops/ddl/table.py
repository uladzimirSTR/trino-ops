from __future__ import annotations
from pydantic import BaseModel
from ...table import TrinoTableDDL, TrinoTableRef

class CreateTable(BaseModel):
    """
    DDL operation: create a table in Trino.

    Typically rendered as:
      - `CREATE TABLE [IF NOT EXISTS] <catalog>.<schema>.<table> ( ...columns... ) WITH ( ...properties... )`

    Notes:
      - For Hive/S3 tables, common properties include `format`, `external_location`,
        and `partitioned_by`.
      - This model describes the desired table definition; validation and SQL
        rendering are handled by separate layers.
    """
    table: TrinoTableDDL
    if_not_exists: bool = True

class DropTable(BaseModel):
    """
    DDL operation: drop an existing table in Trino.

    Typically rendered as:
      - `DROP TABLE [IF EXISTS] <catalog>.<schema>.<table>`

    Notes:
      - Dropping a table may remove only metadata (Hive metastore entry) and may
        not delete underlying files in external storage, depending on connector
        behavior and configuration.
    """
    table: TrinoTableRef
    if_exists: bool = True

class RenameTable(BaseModel):
    """
    DDL operation: rename an existing table in Trino.

    Typically rendered as:
      - `ALTER TABLE <catalog>.<schema>.<table> RENAME TO <new_table_name>`

    Notes:
      - This renames the table within the same schema/catalog (it is not a move).
      - Downstream queries and references must be updated accordingly.
      - Connector support and restrictions may vary.
    """
    table: TrinoTableRef
    new_table_name: str
