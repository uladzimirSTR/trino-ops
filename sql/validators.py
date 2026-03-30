from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from ..configs import TrinoTableDDL

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
from trino_ops.sql.literals import (
    SqlExpr,
    Cast,
    RowLiteral,
    MapLiteral,
    JsonLiteral,
    ArrayLiteral,
)


_INSERT_WRAPPERS = (SqlExpr, Cast, RowLiteral, MapLiteral, JsonLiteral, ArrayLiteral)


class ValidationError(ValueError):
    """
    Raised when an operation or table definition violates validation rules.

    Validation rules are a combination of:
      - connector capabilities (what the target catalog supports)
      - safety policies (e.g., forbid UPDATE/DELETE without WHERE)
      - additional semantic checks beyond Pydantic field validation
    """
    pass


def _is_insert_wrapper(v: Any) -> bool:
    """
    Return ``True`` if *v* is one of the supported wrapper objects used to control
    SQL rendering for ``INSERT ... VALUES``.

    Wrapper objects are used to express intent explicitly (e.g. raw SQL, CAST, ARRAY,
    MAP, ROW, JSON) and must be handled by the SQL renderer.

    Notes:
        - Avoid relying on wrapper *class names* when possible; prefer ``isinstance``
          checks against the actual wrapper types to prevent false positives/negatives.
    """
    return isinstance(v, _INSERT_WRAPPERS)


def _is_allowed_insert_literal(v: Any) -> bool:
    """
    Validate whether a value is allowed inside ``INSERT ... VALUES`` rows.

    Allowed:
        - ``None`` (renders as NULL)
        - Scalars: ``bool``, ``int``, ``float``, ``str``
        - Explicit wrapper objects (SqlExpr/Cast/RowLiteral/MapLiteral/JsonLiteral/ArrayLiteral)
        - Nested lists/tuples *only if* all nested elements are allowed (used for arrays)

    Forbidden:
        - Raw ``dict`` values (must be wrapped explicitly as ``JsonLiteral`` or ``MapLiteral``)

    This function is intended to be conservative: it rejects ambiguous Python
    structures so the caller must make intent explicit via wrappers.

    Returns:
        ``True`` if the value is allowed, otherwise ``False``.
    """
    if v is None or isinstance(v, (bool, int, float, str)):
        return True

    if _is_insert_wrapper(v):
        return True

    if isinstance(v, (list, tuple)):
        return all(_is_allowed_insert_literal(x) for x in v)

    # force explicit intent for maps/json
    if isinstance(v, dict):
        return False

    return False


@dataclass(frozen=True)
class Capabilities:
    """
    Connector/catalog capability flags.

    These flags describe what the target Trino connector is expected to support.
    For a typical Hive-on-S3 setup you usually have:
      - SELECT: supported
      - INSERT INTO ... SELECT: supported
      - CTAS: supported
      - UPDATE/DELETE/MERGE: often NOT supported (unless using Iceberg/Delta/Hudi)

    Notes:
      - Capabilities are used by `validate_op()` to fail fast before rendering or
        executing unsupported operations.
    """
    allow_select: bool = True
    allow_insert: bool = True
    allow_ctas: bool = True
    allow_truncate: bool = True

    allow_delete: bool = False
    allow_update: bool = False
    allow_merge: bool = False

    allow_insert_values: bool = False  # default off: literals are tricky, safer to do INSERT..SELECT


@dataclass(frozen=True)
class ValidationContext:
    """
    Validation context controlling capabilities and safety policies.

    Attributes:
      - capabilities: What operations are allowed/supported for the target catalog.
      - forbid_delete_without_where: If True, `Delete(where=None)` is rejected.
      - forbid_update_without_where: If True, `Update(where=None)` is rejected.
      - forbid_truncate: If True, TRUNCATE is rejected even if capabilities allow it.
    """
    capabilities: Capabilities = Capabilities()
    # safety toggles
    forbid_delete_without_where: bool = True
    forbid_update_without_where: bool = True
    forbid_truncate: bool = False  # if you want extra safety

def _type_is_complex(t: str) -> bool:
    """
    Return True if the given type should be treated as "complex" for validation.

    This helper is used to prevent unsafe/unsupported configurations such as using
    complex types as partition keys.

    Note:
      - This is currently a simple string-based heuristic and assumes your column
        types are represented as strings like "array", "map", "row" or variants.
      - If you move to a structured type model (e.g., TypeSpec), update this
        function accordingly.
    """
    return t in ("array", "map", "row")

def validate_table_ddl(table: TrinoTableDDL) -> None:
    """
    Validate additional semantic rules for a table definition (TrinoTableDDL).

    This runs checks that are hard/awkward to express purely via field types, and
    aims to catch mistakes early, before SQL is rendered/executed.

    Current checks:
      - `partitioned_by` must reference existing column names
      - partition columns must not be complex types (array/map/row)
      - file format must be one of the supported values (PARQUET/ORC/AVRO)

    Raises:
      - ValidationError on any rule violation.
    """
    # extra paranoid checks (even though your model_validator already does part of it)
    colnames = {c.colname for c in table.columns}
    pcols = set(table.table_prop.partitioned_by or [])
    missing = pcols - colnames
    if missing:
        raise ValidationError(f"partitioned_by references missing columns: {sorted(missing)}")

    # partition columns should not be complex
    complex_partitions = []
    for c in table.columns:
        if c.colname in pcols and _type_is_complex(c.coltype):
            complex_partitions.append(c.colname)
    if complex_partitions:
        raise ValidationError(f"partition columns cannot be complex types: {complex_partitions}")

    # format validation is in your models (Literal), but keep a sanity fallback:
    fmt = (table.table_prop.format or "").upper()
    if fmt not in ("PARQUET", "ORC", "AVRO"):
        raise ValidationError(f"Unsupported file format: {fmt}")


def validate_op(op: Any, ctx: Optional[ValidationContext] = None) -> None:
    """
    Validate a single operation against connector capabilities and safety policies.

    Args:
      - op: A DDL/DML operation model instance.
      - ctx: Optional ValidationContext. If omitted, defaults are used.

    Raises:
      - ValidationError if the op is disallowed by capabilities or violates safety
        rules (e.g., DELETE without WHERE).
    """
    ctx = ctx or ValidationContext()
    caps = ctx.capabilities

    # ---------- DDL ----------
    if isinstance(op, CreateTable):
        validate_table_ddl(op.table)
        return

    if isinstance(op, (CreateSchema, DropSchema, DropTable, RenameTable, AddColumns, DropColumns, RenameColumn,
                       SetPartitioning, SetTableLocation, SetFileFormat)):
        return  # nothing extra (yet)

    # ---------- DML ----------
    if isinstance(op, (Select, SelectQuery)):
        if not caps.allow_select:
            raise ValidationError("SELECT operations are disabled by capabilities")
        return

    if isinstance(op, CreateTableAs):
        if not caps.allow_ctas:
            raise ValidationError("CTAS is disabled by capabilities")
        # if you want: validate properties like format/external_location/partitioned_by here too
        return

    if isinstance(op, InsertSelect):
        if not caps.allow_insert:
            raise ValidationError("INSERT is disabled by capabilities")
        return

    if isinstance(op, InsertValues):
        if not caps.allow_insert:
            raise ValidationError("INSERT is disabled by capabilities")
        if not caps.allow_insert_values:
            raise ValidationError("InsertValues is disabled (prefer INSERT..SELECT or enable allow_insert_values)")

        for r in op.rows:
            for v in r:
                if isinstance(v, dict):
                    raise ValidationError(
                        "Raw dict in InsertValues is not allowed. "
                        "Wrap it: MapLiteral.from_dict(d) OR JsonLiteral.from_obj(d) OR RowLiteral([...])."
                    )
                if not _is_allowed_insert_literal(v):
                    raise ValidationError(
                        f"InsertValues value has unsupported type: {type(v).__name__}. "
                        "Allow primitives, list/tuple (ARRAY), or wrappers (MapLiteral/RowLiteral/JsonLiteral/Cast/SqlExpr)."
                    )
        return

    if isinstance(op, Delete):
        if not caps.allow_delete:
            raise ValidationError("DELETE is disabled by capabilities (common for Hive/S3)")
        if ctx.forbid_delete_without_where and op.where is None:
            raise ValidationError("DELETE without WHERE is forbidden by safety policy")
        return

    if isinstance(op, Update):
        if not caps.allow_update:
            raise ValidationError("UPDATE is disabled by capabilities (common for Hive/S3)")
        if ctx.forbid_update_without_where and op.where is None:
            raise ValidationError("UPDATE without WHERE is forbidden by safety policy")
        return

    if isinstance(op, Merge):
        if not caps.allow_merge:
            raise ValidationError("MERGE is disabled by capabilities")
        return

    if isinstance(op, Truncate):
        if not caps.allow_truncate:
            raise ValidationError("TRUNCATE is disabled by capabilities")
        if ctx.forbid_truncate:
            raise ValidationError("TRUNCATE is forbidden by safety policy")
        return

    raise ValidationError(f"Unknown op type for validation: {type(op).__name__}")


def validate_plan(ops: list[Any], ctx: Optional[ValidationContext] = None) -> None:
    """
    Validate a sequence of operations.

    Args:
      - ops: List of operation model instances to validate.
      - ctx: Optional ValidationContext applied to every operation.

    Raises:
      - ValidationError on the first failing operation.
    """
    for op in ops:
        validate_op(op, ctx=ctx)
