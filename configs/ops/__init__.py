from __future__ import annotations
from typing import Union

from .ddl import DdlOp
from .dml import DmlOp

TrinoOp = Union[DdlOp, DmlOp]
