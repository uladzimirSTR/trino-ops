from __future__ import annotations

from typing import Any, TypeVar
from trino_crud.sql import SqlRenderer

from trino_crud.sql.validators import validate_op

from trino_crud.configs.ops.ddl import DdlOp
from trino_crud.configs.ops.dml import DmlOp
from trino_crud.mixins import (
    RendersOp,
    ExecutesSql,
    BuildsOp,
    HasContext,
)


PVar = TypeVar("PVar")


class MakeRenderMixin:
    """
    Mixin that provides a shared :class:`~trino_crud.sql.SqlRenderer` and a simple
    ``render()`` helper.

    The mixin turns operation objects (DDL/DML op instances) into SQL strings using
    ``trino_crud``'s renderer.

    Attributes:
        renderer: A reusable ``SqlRenderer`` instance shared by the class.

    Methods:
        render(op): Render the given operation object into a SQL string.
    """
    renderer = SqlRenderer()
    
    def render(self, op: Any) -> str:
        return self.renderer.render(op)


class RenderExecMixin(RendersOp, ExecutesSql, BuildsOp, HasContext):
    """
    Mixin that helps build, validate, render, and execute Trino CRUD operations.

    This mixin assumes the host class provides:
        - ``render(op) -> str`` (e.g. via :class:`MakeRenderMixin`)
        - ``execute_query(sql: str) -> list[tuple[Any, ...]]`` (e.g. via an executor/client mixin)

    Workflow:
        1) Build an op instance from an operation class and params (:meth:`make_op`).
        2) Validate the op against a predefined :class:`~trino_crud.sql.validators.ValidationContext`
           (:meth:`execute_op`).
        3) Render the op into SQL and execute it.

    Notes:
        - Validation uses the module-level ``ctx`` capabilities. Make sure those
          flags reflect what your Trino connector/environment actually supports.
        - Exceptions from validation, rendering, or execution are propagated.
    """
    
    def make_op(self, operation: type[DdlOp] | type[DmlOp], params: dict[str, PVar]) -> Any:
        """
        Instantiate an operation (DDL/DML) from the given operation class and parameters.

        Args:
            operation: Operation class to instantiate (subclass of ``DdlOp`` or ``DmlOp``).
            params: Keyword arguments passed into the operation constructor.

        Returns:
            The constructed operation instance.
        """
        op = operation(**params)
        return op

    def execute_op(self, op: Any) -> Any:
        """
        Validate, render, and execute an operation object.

        Args:
            op: Operation instance to validate and execute.

        Returns:
            The result of ``execute_query(sql)`` for the rendered SQL.
        """
        validate_op(op, ctx=self.ctx)
        sql = self.render(op)
        return self.execute_query(sql)
