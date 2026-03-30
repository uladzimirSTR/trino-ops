from pydantic import BaseModel
from typing import Literal, Optional


data_types = Literal[
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "real",                # float32
    "double",              # float64
    "decimal",             # decimal(p,s)
    "decimal(28, 4)",      # decimal(p,s)
    "decimal(7, 2)",       # decimal(p,s)
    "decimal(9, 0)",       # decimal(p,s)
    "decimal(28, 6)",
    "decimal(23, 2)",
    "decimal(18, 10)",
    "decimal(30, 2)",
    "decimal(38, 6)",
    "decimal(38, 18)",
    "decimal(38, 20)",
    "decimal(18, 5)",
    "boolean",
    "varchar",
    "char",
    "varbinary",
    # Date/Time
    "date",
    "time",         # optionally time(p)
    "timestamp",    # optionally timestamp(p) [with|without] time zone
    "timestamp with time zone",
    "time with time zone",
    # JSON / UUID / IP
    "json",
    "uuid",
    "ipaddress",
    "array",
    "map",
    "row",
    "array(varchar)",  # example of parametric type (array of varchar)
    "array(integer)",
    "array(double)",
    "array(real)",
    "array(timestamp)",
    "array(date)",
    "array(json)",
    "array(uuid)",
    "array(bigint)",
    "map(varchar, integer)",  # example of parametric type (map of varchar
    "map(varchar, varchar)",
    "map(varchar, json)",
    "row(col1 varchar, col2 integer)",  # example of parametric type (
    
]


class Column(BaseModel):
    """
    Column definition for a Trino table schema.

    Attributes:
      - colname: Column name (identifier). Quoting/escaping is handled by the SQL
        renderer (Jinja templates + identifier macros).
      - coltype: Trino data type as a constrained string literal.

    Notes:
      - `coltype` is intentionally restricted to a fixed allow-list via `Literal`
        to prevent accidental invalid types in configs.
      - This approach is simple but not fully generic: it does not naturally
        support arbitrary parametric types (e.g., `varchar(255)`, `decimal(12,3)`,
        nested `array(array(varchar))`, complex `row(...)`, etc.) unless they are
        explicitly added to the allow-list.
      - If you need full type expressiveness, consider replacing this `Literal`
        with a structured type model (e.g., `TypeSpec`) and a renderer that can
        generate correct Trino type strings.
    """
    colname: str
    coltype: data_types
    comment: Optional[str] = None
