from __future__ import annotations

from trino_ops_utils import ParentClient

from trino_ops_utils.mixins import (
    CreateSchemaMixin, 
    DropSchemaMixin,
    CreateTableMixin,
    DropTableMixin,
)


class DDLClient(
    ParentClient,
    CreateSchemaMixin, 
    DropSchemaMixin,
    CreateTableMixin,
    DropTableMixin,
):
    """
    Client extension that adds DDL-focused helpers on top of :class:`~trino_ops_utils.ParentClient`.

    ``DDLClient`` inherits the core pipeline from ``ParentClient`` (config building, validation,
    rendering, and execution) and mixes in common schema/table DDL operations:

      - ``CreateSchemaMixin``: create the schema referenced by ``self.obj_schema``.
      - ``DropSchemaMixin``: drop the schema referenced by ``self.obj_schema``.
      - ``CreateTableMixin``: create the table referenced by ``self.obj_table``.
      - ``DropTableMixin``: drop the table referenced by ``self.obj_table``.

    Expectations:
        - The host instance should have ``self.obj_schema`` and/or ``self.obj_table`` set before
          calling DDL helpers (typically via ``make_schema_and_table(..., make_objects=True)``
          or by assigning them manually).

    Notes:
        - This class is intentionally thin: it mainly composes behavior via mixins.
        - MRO matters if multiple mixins define methods with the same name; keep mixins
          narrowly scoped to avoid collisions.
    """
