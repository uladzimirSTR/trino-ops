from __future__ import annotations

from typing import Any, Protocol, runtime_checkable, TypeVar

from trino_crud.configs import TrinoSchema, TrinoTableDDL
from trino_crud.configs.auth import TrinoConnectionConfig
from trino_crud.configs.ops.ddl import DdlOp
from trino_crud.configs.ops.dml import DmlOp
from trino_crud.sql.validators import ValidationContext


OpT = TypeVar("OpT", bound=Any)  # если захочешь ужесточить — сделаем Union[DdlOp, DmlOp]


@runtime_checkable
class HasConf(Protocol):
    @property
    def conf(self) -> TrinoConnectionConfig: ...


@runtime_checkable
class ExecutesSql(Protocol):
    def execute_query(self, sql: str) -> list[tuple[Any, ...]]: ...


@runtime_checkable
class RendersOp(Protocol):
    def render(self, op: Any) -> str: ...


@runtime_checkable
class BuildsOp(Protocol):
    def make_op(self, operation: type[DdlOp] | type[DmlOp], params: dict[str, Any]) -> Any: ...


@runtime_checkable
class ExecutesOp(Protocol):
    def execute_op(self, op: Any) -> Any: ...


@runtime_checkable
class HasSchemaTableObjects(Protocol):
    obj_schema: TrinoSchema
    obj_table: TrinoTableDDL


@runtime_checkable
class HasContext(Protocol):
    ctx: ValidationContext


@runtime_checkable
class CanRenderAndExecute(HasConf, ExecutesSql, RendersOp, BuildsOp, ExecutesOp, Protocol):
    """
    Composite protocol: an object that can build ops, validate/render them, and execute SQL.
    Useful for typing mixins that expect the full pipeline.
    """
    pass
