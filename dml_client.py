from __future__ import annotations

from trino_ops import ParentClient

from trino_ops.mixins import (
    InsertMixin,
    InsertSelectMixin,
    CreateTableAsMixin
)


class DMLClient(
    ParentClient,
    InsertMixin,
    InsertSelectMixin,
    CreateTableAsMixin
):
    """
    Client extension that adds DML-focused helpers on top of :class:`~trino_ops.ParentClient`.

    ``DMLClient`` inherits the full “build → validate → render → execute” pipeline from
    ``ParentClient`` and additionally mixes in common data-manipulation operations:

      - ``InsertMixin``: ``INSERT ... VALUES`` into the current table.
      - ``InsertSelectMixin``: ``INSERT INTO ... SELECT ...`` into the current table.
      - ``CreateTableAsMixin``: CTAS (``CREATE TABLE AS SELECT``) for creating tables from queries.

    Expectations:
        - Before calling insert/create helpers, the client should have ``self.obj_schema`` and
          ``self.obj_table`` set (typically via ``make_schema_and_table(..., make_objects=True)``).

    Notes:
        - Method resolution order (MRO) is determined by the inheritance list; keep mixins
          small and side-effect free to avoid surprising interactions.
    """
