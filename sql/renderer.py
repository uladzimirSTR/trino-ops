from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Type

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..configs.ops.ddl.schema import CreateSchema, DropSchema
from ..configs.ops.ddl.table import CreateTable, DropTable, RenameTable
from ..configs.ops.ddl.columns import AddColumns, DropColumns, RenameColumn
from ..configs.ops.ddl.properties import SetPartitioning, SetTableLocation, SetFileFormat

from ..configs.ops.dml.select import Select, SelectQuery
from ..configs.ops.dml.insert import InsertValues, InsertSelect
from ..configs.ops.dml.delete import Delete
from ..configs.ops.dml.update import Update
from ..configs.ops.dml.merge import Merge
from ..configs.ops.dml.truncate import Truncate
from ..configs.ops.dml.ctas import CreateTableAs


# ---------- Template mapping (op class -> template path) ----------
OP_TEMPLATE: Dict[Type[Any], str] = {
    # DDL
    CreateSchema: "ddl/create_schema.sql.j2",
    DropSchema: "ddl/drop_schema.sql.j2",

    CreateTable: "ddl/create_table.sql.j2",
    DropTable: "ddl/drop_table.sql.j2",
    RenameTable: "ddl/rename_table.sql.j2",

    AddColumns: "ddl/add_columns.sql.j2",
    DropColumns: "ddl/drop_columns.sql.j2",
    RenameColumn: "ddl/rename_column.sql.j2",

    SetPartitioning: "ddl/set_partitioning.sql.j2",
    SetTableLocation: "ddl/set_table_location.sql.j2",
    SetFileFormat: "ddl/set_file_format.sql.j2",

    # DML
    Select: "dml/select.sql.j2",
    SelectQuery: "dml/select_query.sql.j2",

    InsertValues: "dml/insert_values.sql.j2",
    InsertSelect: "dml/insert_select.sql.j2",

    Delete: "dml/delete.sql.j2",
    Update: "dml/update.sql.j2",
    Merge: "dml/merge.sql.j2",
    Truncate: "dml/truncate.sql.j2",

    CreateTableAs: "dml/ctas.sql.j2",
}


def _default_templates_dir() -> Path:
    """
    Resolve the default templates directory.

    The renderer expects templates to live under:
      <package_root>/templates/

    This helper computes the path relative to this module file so the package can
    be installed and still find templates using a stable relative layout.
    """
    # trino_cfg/sql/renderer.py -> trino_cfg/templates
    return Path(__file__).resolve().parents[1] / "templates"


@dataclass(frozen=True)
class RenderConfig:
    """
    Configuration for SQL template rendering.

    Attributes:
      - templates_dir: Root directory containing the `ddl/`, `dml/`, and `_macros/`
        template folders.
      - trim_blocks: Jinja2 option to remove the first newline after a block.
      - lstrip_blocks: Jinja2 option to strip leading spaces and tabs from the
        start of a line to a block.
      - keep_trailing_newline: Whether to keep a trailing newline at the end of
        rendered output.
    """
    templates_dir: Path = _default_templates_dir()
    trim_blocks: bool = True
    lstrip_blocks: bool = True
    keep_trailing_newline: bool = False


class SqlRenderer:
    """
    Render Pydantic operation models (DDL/DML) into SQL using Jinja2 templates.

    This class:
      1) maps an op model type to a template file (`OP_TEMPLATE`)
      2) dumps the Pydantic model into a plain dict (`model_dump()`)
      3) renders the corresponding `.sql.j2` template

    Notes:
      - Templates are rendered with `StrictUndefined` so missing variables fail
        fast (useful for catching template/model mismatches).
      - This renderer is purely about SQL generation; it does not execute SQL.
    """

    def __init__(self, config: Optional[RenderConfig] = None) -> None:
        """
        Initialize the renderer and its Jinja2 environment.

        Args:
          config: Optional `RenderConfig`. If omitted, defaults are used.
        """
        self.config = config or RenderConfig()
        
        self.env = Environment(
            loader=FileSystemLoader(str(self.config.templates_dir)),
            undefined=StrictUndefined,  # fail fast on missing vars
            autoescape=False,
            trim_blocks=self.config.trim_blocks,
            lstrip_blocks=self.config.lstrip_blocks,
            keep_trailing_newline=self.config.keep_trailing_newline,
        )

    def template_for(self, op: Any) -> str:
        """
        Return the template path for a given operation model.

        Args:
          op: A Pydantic op instance (DDL or DML).

        Returns:
          Relative template path like `ddl/create_table.sql.j2`.

        Raises:
          KeyError: If there is no mapping for the op type in `OP_TEMPLATE`.
        """
        op_type = type(op)
        tpl = OP_TEMPLATE.get(op_type)
        if not tpl:
            raise KeyError(f"No template mapped for op type: {op_type.__name__}")
        return tpl

    def render(self, op: Any, extra_ctx: Optional[Dict[str, Any]] = None) -> str:
        """
        Render a single operation into a SQL string.

        Args:
          op: A Pydantic op instance.
          extra_ctx: Optional extra variables to inject into the template context.
                     If provided, these values override keys from `op.model_dump()`.

        Returns:
          Rendered SQL string (stripped of leading/trailing whitespace).
        """
        tpl_path = self.template_for(op)
        tpl = self.env.get_template(tpl_path)
        print(tpl)

        # Pydantic v2: model_dump
        payload = op.model_dump()

        # merge extra context (overrides allowed)
        if extra_ctx:
            payload.update(extra_ctx)

        sql = tpl.render(**payload).strip()
        return sql

    def render_many(self, ops: list[Any], extra_ctx: Optional[Dict[str, Any]] = None) -> list[str]:
        """
        Render a list of operations into a list of SQL statements.

        Args:
          ops: List of Pydantic op instances.
          extra_ctx: Optional shared template context injected for every op.

        Returns:
          List of rendered SQL strings.
        """
        return [self.render(op, extra_ctx=extra_ctx) for op in ops]
