# trino_ops_utils

> Unofficial utilities for declarative Trino DDL/DML modeling, validation, SQL rendering, and optional execution.

Declarative configs → validated operations → rendered SQL (Jinja2) → optional execution against Trino.

**trino_ops_utils** helps you model Trino DDL/DML actions as Pydantic objects, validate them against connector capabilities and safety rules, and render them into SQL using a template library. It is designed primarily for S3-backed tables (Hive-like catalogs) and focuses on repeatable, testable SQL generation.

> **Disclaimer**  
> This is an **unofficial** project and is **not affiliated with, endorsed by, or maintained by the Trino project or its maintainers**.  
> “Trino” is referenced only to describe compatibility and intended usage.

---

## Why this exists

- Keep schema and table changes in typed configs instead of ad-hoc SQL strings.
- Enforce guardrails through capabilities and safety policies before anything is executed.
- Render SQL via Jinja2 templates so SQL formatting and dialect details stay out of Python code.
- Enable plans of operations and render or execute them consistently.

---

## Key concepts

### 1) `table/` — models

Pydantic models for schemas, tables, columns, and storage properties:

- `TrinoSchema` — catalog, schema name, default S3 location
- `TrinoTableRef` — schema and table name reference
- `TrinoTableDDL` — table definition: columns and properties
- `Column` — name, type, optional comment
- `TableProp` — format, partitioned_by, external_location, and similar properties

> Types are currently modeled as a constrained allow-list (`Literal[...]`). This is simple and safe, but not fully generic for all parametric or nested types.

### 2) `ops/` — operations

Pydantic models that describe intent for DDL and DML, for example:

- DDL: `CreateSchema`, `CreateTable`, `AddColumns`, `SetPartitioning`
- DML: `Select`, `InsertSelect`, `InsertValues`, `Delete`, `Update`, `Merge`

These models do **not** execute SQL. They are inputs to the validator and renderer.

### 3) `sql/validators.py`

Validation is **two-layered**:

- Pydantic validates structure and types.
- `validate_op()` and `validate_table_ddl()` enforce semantic rules, capabilities, and safety policies.

Example policies:

- Hive/S3: allow `SELECT`, `INSERT`, `CTAS`; disallow `UPDATE`, `DELETE`, `MERGE` by default
- Forbid `DELETE` or `UPDATE` without `WHERE` when configured
- Require partition keys to exist and not use complex types

### 4) `sql/renderer.py`

Renders operations into SQL using Jinja2 templates:

- Maps operation class to a template path (`OP_TEMPLATE`)
- Uses `StrictUndefined` to fail fast on template and model mismatches
- Exposes `render(op)` and `render_many(ops)`

Templates live under `templates/`:

```text
templates/
  ddl/
  dml/
  _macros/
```

### 5) `db/` — optional execution

Basic DB-API wrapper around the Trino Python client:

- `Client` manages a lazy connection and supports the context manager protocol
- `SQLClient.execute(sql)` executes SQL and returns fetched rows

Execution is optional. This project is primarily a SQL generation library.

### 6) Managers — optional convenience API

Managers wrap common workflows:

- validate
- render
- optionally execute

Example: `TrinoDDLManager` for schema, table, column, and property operations.

---

## Installation

Expected dependencies:

- `pydantic>=2`
- `jinja2`
- `trino` (python client)

---

## Naming

The Python import name is:

```python
import trino_ops_utils
```

The distribution name can be published as:

```text
trino_ops_utils
```

---

## DDL examples

```python
from __future__ import annotations

from trino_ops_utils import DDLClient

conf = DDLClient(
    host="91.98.21.193",
    port=8443,
    user="airflow",
    catalog="datalake",
    http_scheme="https",
    auth={
        "type": "basic",
        "username": "airflow",
        "password": "SuperPAss",
    },
    path_to_pem=Path("./trino_ops_utils"),
    file_name_pem="trino.pem",
)
```

### Execute query

```python
conf.execute_query(
    "SELECT orderkey, orderstatus, totalprice, orderdate FROM _debug.orders"
)
```

### Create Table and Schema obj
```python
conf.make_schema_and_table(
    catalog="datalake",
    schema_name="stage",
    location="s3a://prod-datalake/",
    table_name="events",
    columns=[
        ("c", "varchar"),
        ("d", "array(varchar)"),
        ("a", "varchar"),
        ("b", "varchar"),
    ],
    format="ORC",
    partitioned_by=["a", "b"],
)
```

### Create schema

```python
conf.create_schema(if_not_exists=True)
```

### Drop schema

```python
conf.drop_schema(if_exists=True)
```

### Create table

```python
conf.create_table(if_not_exists=True)
```

### Drop table

```python
conf.drop_table(if_exists=True)
```

---

## DML examples

```python
conf = DMLClient(
    host="91.98.21.193",
    port=8443,
    user="airflow",
    catalog="datalake",
    http_scheme="https",
    auth={
        "type": "basic",
        "username": "airflow",
        "password": "SuperPassword",
    },
    path_to_pem=Path("./trino_ops_utils"),
    file_name_pem="trino.pem",
)
```
```python
conf.make_schema_and_table(
    catalog="datalake",
    schema_name="stage",
    location="s3a://prod-datalake/",
    table_name="events",
    columns=[
        ("c", "varchar"),
        ("d", "array(varchar)"),
        ("a", "varchar"),
        ("b", "varchar"),
    ],
    format="ORC",
    partitioned_by=["a", "b"],
)
```

### Insert values

```python
conf.insert_values(
    rows=[
        ["a", ["d1", "d2"], "a1", "b1"],
        ["a", ["d3", "d4"], "a2", "b2"],
        ["a", ["d3", "d4"], "a2", "b3"],
        ["a", ["d3", "d4"], "a2", "b3"],
        ["a", ["d3", "d4"], "a1", "b3"],
    ]
)
```

### Create table as

```python
sql = '''
SELECT c, d, a, b
FROM datalake.stage.events
'''
```

```python
conf.create_table_as(
    table_schema="stage",
    table_name="events_tmp",
    sql=sql,
    properties={
        "format": "ORC",
        "partitioned_by": ["a", "b"],
    },
)
```

### Insert select

```python
conf.insert_select(sql=sql)
```

---

## Notes

- This package is intended to make SQL generation safer and more repeatable, not to replace connector-specific knowledge.
- Validation rules should still reflect the capabilities of the target catalog and connector.
- Even if a Sheet, config, or upstream system looks clean, treat execution as a separate trust boundary. SQL has a talent for becoming creative at the worst possible moment.
