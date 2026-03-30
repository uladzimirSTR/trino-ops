from __future__ import annotations

from pydantic import BaseModel, field_validator, Field, model_validator

from .schema import TrinoSchema
from .column import Column
from typing import List, Optional, Literal


class TrinoTableRef(BaseModel):
    """
    Reference (no columns). Useful for drop/rename/etc.
    """
    table_schema: TrinoSchema
    table_name: str

    @field_validator("table_name")
    @classmethod
    def _tbl_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("table_name must be non-empty")
        if " " in v or "." in v:
            raise ValueError("table_name must not contain spaces or dots")
        return v


# ---- File formats (Hive connector formats) ----
FileFormat = Literal["PARQUET", "ORC", "AVRO"]


class TableProp(BaseModel):
    """
    Storage-related properties for Hive connector.
    """
    format: FileFormat = "PARQUET"
    partitioned_by: Optional[List[str]] = None

    @field_validator("format")
    @classmethod
    def _format_upper(cls, v):
        return v.upper()
    
    # @field_validator("partitioned_by")
    # @classmethod
    # def _partitioned_by_upper(cls, v):
    #     return [col.upper() for col in v]

class TrinoTableDDL(TrinoTableRef):
    """
    Full table definition for CREATE TABLE.
    """
    columns: Optional[List[Column]] = None
    table_prop: TableProp = Field(default_factory=TableProp)

    # Table properties: Trino/Hive "WITH (...)" props
    # We'll keep it extensible (but you still validate key basics).
    # properties: Dict[str, object] = Field(default_factory=dict)
    # external_location: str = None

    # @field_validator("external_location", mode="after")
    # @classmethod
    # def _validate_location(cls, v):
    #     return f"{cls.table_schema.location}/{cls.table_schema.name}/{cls.table_name}"

    @model_validator(mode="after")
    def _validate_partitions(self):
        if self.columns is None:
            return self  # No columns, so skip partition validation (e.g. CTAS)

        colnames = {c.colname for c in self.columns}
        pcols = self.table_prop.partitioned_by
        if pcols:
            missing = [p for p in pcols if p not in colnames]
            if missing:
                raise ValueError(f"partitioned_by references missing columns: {missing}")

            # Keep partition columns sane: many formats/connectors dislike complex types as partitions.
            # You can loosen this later if you really want pain.
        
        
            complex_bases = {"array", "map", "row"}
            bad = []
            for c in self.columns:
                if c.colname in pcols:
                    if c.coltype in complex_bases:
                        bad.append(c.colname)
            if bad:
                raise ValueError(f"partition columns cannot be complex types (array/map/row): {bad}")

        return self
