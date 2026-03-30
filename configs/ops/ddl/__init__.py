from __future__ import annotations
from typing import Union

from .schema import CreateSchema, DropSchema
from .table import CreateTable, DropTable, RenameTable
from .columns import AddColumns, DropColumns, RenameColumn
from .properties import SetPartitioning, SetTableLocation, SetFileFormat

DdlOp = Union[
    CreateSchema, DropSchema,
    CreateTable, DropTable, RenameTable,
    AddColumns, DropColumns, RenameColumn,
    SetPartitioning, SetTableLocation, SetFileFormat,
]
