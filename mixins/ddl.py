from __future__ import annotations

from typing import Any

from trino_ops.configs.ops.ddl.schema import (
    CreateSchema,
    DropSchema
)

from trino_ops.configs.ops.ddl.table import (
    CreateTable,
    DropTable,
    RenameTable
)

from trino_ops.mixins.interface import (
    HasSchemaTableObjects,
    BuildsOp,
    ExecutesOp
)


class CreateSchemaMixin(HasSchemaTableObjects, BuildsOp, ExecutesOp):
    """
    Mixin that adds a convenience method for creating a Trino schema.

    Expects the host class to provide:
        - ``self.obj_schema``: a :class:`~trino_ops.configs.TrinoSchema` instance
        - ``make_op(operation_cls, params)``: builds an op instance
        - ``execute_op(op)``: validates, renders, and executes an op

    Methods:
        create_schema(): Creates the schema referenced by ``self.obj_schema``.
    """

    def create_schema(self, if_not_exists: bool = True) -> Any:
        op = self.make_op(CreateSchema, {
            "table_schema": self.obj_schema,
            "if_not_exists": if_not_exists,
            })

        return self.execute_op(op)


class DropSchemaMixin(HasSchemaTableObjects, BuildsOp, ExecutesOp):
    """
    Mixin that adds a convenience method for dropping a Trino schema.

    Expects the host class to provide:
        - ``self.obj_schema``: a :class:`~trino_ops.configs.TrinoSchema` instance
        - ``make_op(...)`` and ``execute_op(...)`` helpers (see :class:`RenderExecMixin`)

    Methods:
        drop_schema(): Drops the schema referenced by ``self.obj_schema``.
    """

    def drop_schema(
        self,
        if_exists: bool = True,
        cascade: bool = False
    ) -> Any:
        op = self.make_op(DropSchema, {
            "table_schema": self.obj_schema,
            "if_exists": if_exists,
            "cascade": cascade,
            })

        return self.execute_op(op)


class CreateTableMixin(HasSchemaTableObjects, BuildsOp, ExecutesOp):
    """
    Mixin that adds a convenience method for creating a Trino table.

    Expects the host class to provide:
        - ``self.obj_table``: a :class:`~trino_ops.configs.TrinoTableDDL` instance
        - ``make_op(...)`` and ``execute_op(...)`` helpers (see :class:`RenderExecMixin`)

    Methods:
        create_table(): Creates the table referenced by ``self.obj_table``.
    """
    def create_table(
        self,
        if_not_exists: bool = True
    ) -> Any:
        op = self.make_op(CreateTable, {
            "table": self.obj_table,
            "if_not_exists": if_not_exists,
            })

        return self.execute_op(op)


class DropTableMixin(HasSchemaTableObjects, BuildsOp, ExecutesOp):
    """
    Mixin that adds a convenience method for dropping a Trino table.

    Expects the host class to provide:
        - ``self.obj_table``: a :class:`~trino_ops.configs.TrinoTableDDL` instance
        - ``make_op(...)`` and ``execute_op(...)`` helpers (see :class:`RenderExecMixin`)

    Methods:
        drop_table(): Drops the table referenced by ``self.obj_table``.
    """
    def drop_table(
        self,
        if_exists: bool = True
    ) -> Any:
        op = self.make_op(DropTable, {
            "table": self.obj_table,
            "if_exists": if_exists,
            })

        return self.execute_op(op)
