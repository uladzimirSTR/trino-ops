from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence
import json

@dataclass(frozen=True)
class SqlExpr:
    """
    Wrapper for a raw SQL expression.

    Use this when you want to inject SQL as-is into a rendered statement, without
    quoting or value-literal formatting (e.g. ``CURRENT_TIMESTAMP``, ``uuid()``,
    ``date_trunc('day', ...)``).

    Attributes:
        sql: SQL snippet to be emitted verbatim by the renderer.
    """
    sql: str

@dataclass(frozen=True)
class ArrayLiteral:
    """
    Typed container representing an SQL ARRAY literal.

    The renderer is expected to serialize ``items`` into an ``ARRAY[...]`` (or
    connector-appropriate) literal, recursively rendering nested values.

    Attributes:
        items: Items contained in the array. Values may be plain Python scalars or
            other literal wrapper types.
    """
    items: Sequence[Any]

@dataclass(frozen=True)
class MapLiteral:
    """
    Typed container representing an SQL MAP literal.

    The renderer is expected to serialize ``keys`` and ``values`` into a Trino
    ``map(ARRAY[...], ARRAY[...])``-style expression (or equivalent).

    Attributes:
        keys: Sequence of map keys.
        values: Sequence of map values (same length as ``keys``).

    Classmethods:
        from_dict: Convenience constructor that converts a Python mapping into
            parallel ``keys`` and ``values`` sequences.
    """

    keys: Sequence[Any]
    values: Sequence[Any]

    @classmethod
    def from_dict(cls, d: Mapping[Any, Any]) -> "MapLiteral":
        """
        Build a ``MapLiteral`` from a Python mapping.

        Args:
            d: Mapping to convert.

        Returns:
            A ``MapLiteral`` with ``keys=list(d.keys())`` and ``values=list(d.values())``.

        Notes:
            - Iteration order follows the mapping's iteration order (in CPython 3.7+
              this is insertion order).
        """
        return cls(keys=list(d.keys()), values=list(d.values()))

@dataclass(frozen=True)
class RowLiteral:
    """
    Typed container representing an SQL ROW literal.

    The renderer is expected to serialize ``items`` into ``ROW(...)`` (or equivalent),
    recursively rendering nested values.

    Attributes:
        items: Row fields, in order.
    """
    items: Sequence[Any]

@dataclass(frozen=True)
class JsonLiteral:
    """
    Wrapper representing JSON text to be emitted as a JSON literal/expression.

    This is useful when you want to pass pre-serialized JSON (or serialize a Python
    object) and ensure the renderer treats it as JSON text rather than a plain string.

    Attributes:
        json_text: JSON document text.

    Classmethods:
        from_obj: Serialize a Python object to JSON text (unless it is already a string).
    """
    json_text: str

    @classmethod
    def from_obj(cls, obj: Any) -> "JsonLiteral":
        """
        Create a ``JsonLiteral`` from a Python object.

        Args:
            obj: Any JSON-serializable object (dict, list, primitives, etc.). If ``obj``
                is a string, it is assumed to already be JSON text and is used as-is.

        Returns:
            A ``JsonLiteral`` containing JSON text.

        Notes:
            - Uses ``ensure_ascii=False`` to keep Unicode readable.
            - Uses ``default=str`` as a fallback for non-serializable values.
        """
        # obj can be dict/list/str/anything json-serializable-ish
        if isinstance(obj, str):
            return cls(json_text=obj)
        return cls(json_text=json.dumps(obj, ensure_ascii=False, default=str))

@dataclass(frozen=True)
class Cast:
    """
    Typed container representing an explicit SQL CAST.

    The renderer is expected to serialize this into ``CAST(<expr> AS <type_sql>)``
    (or equivalent), where ``expr`` may itself be another literal wrapper.

    Attributes:
        expr: Expression/value to cast.
        type_sql: SQL type name/expression, e.g. ``"json"``, ``"map(varchar, integer)"``,
            ``"array(varchar)"``.
    """
    expr: Any
    type_sql: str  # e.g. "json", "map(varchar, integer)", "array(varchar)"
