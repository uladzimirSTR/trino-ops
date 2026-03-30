from __future__ import annotations

from typing import Mapping, Sequence, Any, Optional, Union

from trino_ops.configs import TrinoSchema, TrinoTableDDL
from trino_ops.configs import Column, TableProp


class MakeSchemaTableMixin:
    """
    Helper mixin for constructing Trino schema/table configuration objects.

    This mixin provides small factory methods that turn simple Python inputs
    (strings, sequences, mappings) into strongly-typed config objects used by
    ``trino_ops``:

    - :class:`~trino_ops.configs.TrinoSchema` for schema references (optionally with a location)
    - :class:`~trino_ops.configs.TrinoTableDDL` for table DDL definitions
    - :class:`~trino_ops.configs.TableProp` for table properties (format, partitioning, etc.)

    Design goals:
        - Reduce repetitive boilerplate when building configs.
        - Allow convenient column input as either ``Column`` objects or ``(name, type)`` tuples.
        - Perform minimal sanity checks early (e.g., empty names, missing partition columns).

    Notes:
        - ``make_schema_and_table(..., make_objects=True)`` will also attach the created
          objects to ``self.obj_schema`` and ``self.obj_table`` for later reuse.
          These attributes are created dynamically; define them on your class if you
          want static type-checking support.
    """

    def make_schema(
        self,
        *,
        catalog: str,
        schema_name: str,
        location: Optional[str] = None,
    ) -> TrinoSchema:
        """
        Build a :class:`~trino_ops.configs.TrinoSchema`.

        Args:
            catalog: Trino catalog name (e.g. ``"datalake"``).
            schema_name: Schema name within the catalog (e.g. ``"analytics"``).
            location: Optional storage location for the schema (connector-specific),
                e.g. an S3/HDFS URI.

        Returns:
            A ``TrinoSchema`` instance.

        Raises:
            ValueError: If ``catalog`` or ``schema_name`` is empty.
        """
        if not catalog or not schema_name:
            raise ValueError("catalog and name are required for schema")
        if location is None:
            return TrinoSchema(catalog=catalog, name=schema_name)
        return TrinoSchema(catalog=catalog, name=schema_name, location=location)

    def make_table(
        self,
        *,
        schema: TrinoSchema,
        table_name: str,
        columns: Sequence[Union[Column, tuple[str, str]]],
        format: str = "ORC",
        partitioned_by: Optional[Sequence[str]] = None,
        # optional: extra table properties if your TableProp supports them
        extra_props: Optional[Mapping[str, Any]] = None,
    ) -> TrinoTableDDL:
        """        
        Build a :class:`~trino_ops.configs.TrinoTableDDL` definition.

        Args:
            schema: Schema object the table belongs to.
            table_name: Table name (unqualified).
            columns: Column definitions as either ``Column`` objects or
                ``(colname, coltype)`` tuples.
            format: Storage format (e.g. ``"ORC"``, ``"PARQUET"``), passed into
                :class:`~trino_ops.configs.TableProp`.
            partitioned_by: Optional list of column names to partition by. If provided,
                each name must exist among ``columns``.
            extra_props: Optional extra table properties passed into ``TableProp``.
                Use this for connector-specific options (compression, external_location,
                bucket_count, etc.) if your ``TableProp`` model supports them.

        Returns:
            A ``TrinoTableDDL`` instance.

        Raises:
            ValueError:
                - If ``table_name`` is empty.
                - If ``columns`` is empty.
                - If ``partitioned_by`` contains unknown column names.
        """
        if not table_name:
            raise ValueError("table_name is required")

        # Allow both Column objects and ("name","type") tuples for convenience
        cols: list[Column] = []
        for c in columns:
            if isinstance(c, Column):
                cols.append(c)
            else:
                colname, coltype = c
                cols.append(Column(colname=colname, coltype=coltype))

        if len(cols) == 0:
            raise ValueError("columns must not be empty")

        # Basic sanity: partition columns should exist
        part = list(partitioned_by or [])
        if part:
            known = {c.colname for c in cols}
            missing = [p for p in part if p not in known]
            if missing:
                raise ValueError(f"partitioned_by references unknown columns: {missing}")

        table_prop = TableProp(
            format=format,
            partitioned_by=part if part else None,
            **(dict(extra_props) if extra_props else {}),
        )

        return TrinoTableDDL(
            table_schema=schema,
            table_name=table_name,
            columns=cols,
            table_prop=table_prop,
        )

    def make_schema_and_table(
        self,
        *,
        catalog: str,
        schema_name: str,
        location: Optional[str],
        table_name: str,
        columns: Sequence[Union[Column, tuple[str, str]]],
        format: str = "ORC",
        partitioned_by: Optional[Sequence[str]] = None,
        extra_props: Optional[Mapping[str, Any]] = None,
        make_objects: bool = True
    ) -> tuple[TrinoSchema, TrinoTableDDL]:
        """
        Convenience method that builds both schema and table objects in one call.

        This is a small wrapper around :meth:`make_schema` and :meth:`make_table`.

        Args:
            catalog: Trino catalog name.
            schema_name: Schema name within the catalog.
            location: Optional schema location (connector-specific).
            table_name: Table name (unqualified).
            columns: Column definitions as either ``Column`` objects or
                ``(colname, coltype)`` tuples.
            format: Storage format for the table (passed into ``TableProp``).
            partitioned_by: Optional partition column names.
            extra_props: Optional extra properties for ``TableProp``.
            make_objects: If ``True``, also assign the created objects to
                ``self.obj_schema`` and ``self.obj_table``.

        Returns:
            A tuple ``(schema, table)`` containing the constructed objects.
        """
        schema = self.make_schema(
            catalog=catalog,
            schema_name=schema_name,
            location=location
        )
        
        table = self.make_table(
            schema=schema,
            table_name=table_name,
            columns=columns,
            format=format,
            partitioned_by=partitioned_by,
            extra_props=extra_props,
        )
        if make_objects:
            self.obj_schema = schema
            self.obj_table = table
        return schema, table
