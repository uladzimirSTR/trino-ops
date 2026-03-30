from __future__ import annotations
from typing import Dict, Optional
from pydantic import BaseModel, Field

from ...table import TrinoSchema, TrinoTableRef

class CreateTableAs(BaseModel):
    """
    CTAS: CREATE TABLE ... AS SELECT ...
    """
    table_schema: TrinoSchema
    table_name: str
    select_sql: str
    properties: Dict[str, object] = Field(default_factory=dict)
    if_not_exists: bool = True
