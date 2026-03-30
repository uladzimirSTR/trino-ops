from __future__ import annotations
from typing import Union

from .common import Query
from .select import Select, SelectQuery
from .insert import InsertValues, InsertSelect
from .delete import Delete
from .update import Update
from .merge import Merge
from .truncate import Truncate
from .ctas import CreateTableAs

DmlOp = Union[
    Select, SelectQuery,
    InsertValues, InsertSelect,
    Delete, Update, Merge,
    Truncate,
    CreateTableAs,
]
