from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    import numpy as np
except Exception:
    np = None

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None


def normalize_cell_for_values(cell: Any) -> Any:
    """
    Normalize Python/Pandas/Numpy values for SQL VALUES renderer:
      - NaN/NaT/NA -> None
      - numpy scalar -> python scalar
      - dict/list -> JSON string (so it becomes a scalar literal)
      - Path -> str
      - datetime/date -> ISO string (or keep datetime if your renderer supports it)
      - Decimal -> keep Decimal (renderer should handle) or str(cell)
    """
    # numpy scalar -> python scalar
    if isinstance(cell, np.generic):
        cell = cell.item()

    # pandas null-like values
    if pd is not None:
        # pandas.NA is tricky; check safely
        if cell is pd.NA:
            return None
        # Only scalar NaN/NaT
        if pd.api.types.is_scalar(cell) and pd.isna(cell):
            return None
    else:
        # fallback: numpy NaN
        try:
            if isinstance(cell, float) and np.isnan(cell):
                return None
        except Exception:
            pass

    if cell is None:
        return None

    # complex -> JSON string
    if isinstance(cell, (dict, list)):
        return json.dumps(cell, ensure_ascii=False)

    # bytes -> decode best-effort (or keep bytes if renderer supports)
    if isinstance(cell, (bytes, bytearray)):
        try:
            return cell.decode("utf-8")
        except Exception:
            return cell.hex()

    if isinstance(cell, Path):
        return str(cell)

    # dates/times: decide policy
    if isinstance(cell, (datetime, date)):
        # safest: string literal
        return cell.isoformat()

    # Decimal: depends on your vals.sql_value; usually ok to keep Decimal
    if isinstance(cell, Decimal):
        return cell

    return cell


def normalize_rows(rows: Sequence[Sequence[Any]]) -> list[list[Any]]:
    return [[normalize_cell_for_values(v) for v in r] for r in rows]
