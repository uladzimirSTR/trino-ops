from __future__ import annotations

from typing import Any, Optional, Sequence

from trino_ops.configs import TrinoSchema
from trino_ops.configs.table import Column

from trino_ops.configs.ops.dml.insert import InsertValues, InsertSelect
from trino_ops.configs.ops.dml.ctas import CreateTableAs

from trino_ops.mixins.interface import (
    HasSchemaTableObjects,
    BuildsOp,
    ExecutesOp
)


class InsertMixin(HasSchemaTableObjects, BuildsOp, ExecutesOp):
    """
    Mixin that adds an ``INSERT ... VALUES`` helper for the current table.

    Expects the host class to provide:
        - ``self.obj_table`` with ``columns`` (list of ``Column``) and table metadata
        - ``make_op(...)`` and ``execute_op(...)`` helpers

    Methods:
        insert_values(): Inserts one or more rows into ``self.obj_table``.
    """
    
    def insert_values(
        self,
        rows: Optional[Sequence[Sequence[Any]]] = None,
        cols: Optional[list[Column]] = None,
    ) -> Any:
        """
        Insert rows into the current table using ``INSERT ... VALUES``.

        Args:
            rows: A sequence of rows, where each row is a sequence of values ordered
                to match ``cols`` (or the table's columns if ``cols`` is omitted).
            cols: Optional subset/order of columns to insert into. If omitted, all
                columns from ``self.obj_table.columns`` are used.

        Returns:
            The result of executing the rendered SQL, or ``None`` if ``rows`` is empty.

        Notes:
            - If ``rows`` is falsy (``None`` or empty), the method returns ``None`` and
              does not execute anything.
            - Values are passed through the ``InsertValues`` op and rendered by the SQL
              renderer; type handling depends on your renderer/connector rules.
        """
        if not rows:
            return None

        if cols is None:
            cols = self.obj_table.columns

        op = self.make_op(
            InsertValues,
            {
                "table": self.obj_table,
                "columns": [c.colname for c in cols],
                "rows": rows,
            },
        )
        return self.execute_op(op)


class InsertSelectMixin(HasSchemaTableObjects, BuildsOp, ExecutesOp):
    """
    Mixin that adds an ``INSERT INTO ... SELECT ...`` helper for the current table.

    Expects the host class to provide:
        - ``self.obj_table`` with ``columns`` (list of ``Column``) and table metadata
        - ``make_op(...)`` and ``execute_op(...)`` helpers

    Methods:
        insert_select(): Inserts query results into ``self.obj_table``.
    """

    def insert_select(
        self,
        cols: Optional[list[Column]] = None,
        sql: str = None
    ) -> Any:
        """
        Insert into the current table from a SELECT statement.

        Args:
            cols: Optional subset/order of target columns. If omitted, all columns
                from ``self.obj_table.columns`` are used.
            sql: The SELECT statement to insert from (without the surrounding
                ``INSERT INTO``).

        Returns:
            The result of executing the rendered SQL.

        Raises:
            ValueError: If ``sql`` is ``None``.
        """
        if sql is None:
            raise ValueError("sql is required for insert_select")
    
        if cols is None:
            cols = self.obj_table.columns
        
        op = self.make_op(InsertSelect, {
            "table": self.obj_table,
            "columns": [c.colname for c in cols],
            "select_sql": sql,
            })
        return self.execute_op(op)


class CreateTableAsMixin(HasSchemaTableObjects, BuildsOp, ExecutesOp):
    """
    Mixin that adds a CTAS (Create Table As Select) helper.

    Expects the host class to provide:
        - ``self.obj_schema`` (used as a source for catalog/location defaults)
        - ``make_op(...)`` and ``execute_op(...)`` helpers

    Methods:
        create_table_as(): Creates a new table from a SELECT query.
    """

    def create_table_as(
        self,
        table_schema: str,
        table_name: str,
        sql: str = None,
        properties: Optional[dict[str, object]] = None,
        if_not_exists: bool = True
    ) -> Any:
        """
        Create a table from a SELECT statement using CTAS.

        The new table is created in the same catalog as ``self.obj_schema`` and inherits
        ``location`` from it (if set). The schema name is provided explicitly via
        ``table_schema``.

        Args:
            table_schema: Target schema name for the new table (within the current catalog).
            table_name: Target table name to create.
            sql: The SELECT statement used to populate the table.
            properties: Optional CTAS properties (connector-specific), e.g. file format,
                partitioning, external location, etc.
            if_not_exists: If ``True``, render CTAS with an ``IF NOT EXISTS`` guard.

        Returns:
            The result of executing the rendered SQL.

        Raises:
            ValueError: If ``sql`` is ``None``.
        """
        if sql is None:
            raise ValueError("sql is required for create_table_as")

        table_schema = TrinoSchema(
            catalog=self.obj_schema.catalog,
            name=table_schema,
            location=self.obj_schema.location
        )

        op = self.make_op(CreateTableAs, {
            "table_schema": table_schema,
            "table_name": table_name,
            "select_sql": sql,
            "properties": properties or {},
            "if_not_exists": if_not_exists,
            })
        return self.execute_op(op)
