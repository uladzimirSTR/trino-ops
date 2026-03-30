# trino-ops

Declarative configs → validated operations → rendered SQL (Jinja2) → optional execution against Trino.

This project helps you **model Trino DDL/DML actions as Pydantic objects**, validate them against connector capabilities/safety rules, and render them into SQL using a template library. It is designed primarily for **S3-backed tables** (Hive-like catalogs) and focuses on **repeatable, testable SQL generation**.

---

## Why this exists

- Keep schema/table changes in **typed configs** instead of ad-hoc SQL strings.
- Enforce **guardrails** (capabilities + safety policies) before anything is executed.
- Render SQL via **Jinja2 templates** so SQL formatting and dialect details stay out of your Python code.
- Enable “plans” (lists of operations) and render/execute them consistently.

---

## Key concepts

### 1) `table/` (models)
Pydantic models for schemas, tables, columns, and storage properties:
- `TrinoSchema` (catalog, schema name, default S3 location)
- `TrinoTableRef` (schema + table name reference)
- `TrinoTableDDL` (table definition: columns + properties)
- `Column` (name, type, optional comment)
- `TableProp` (format, partitioned_by, external_location, …)

> Types are currently modeled as a constrained allow-list (`Literal[...]`). It’s simple and safe, but not fully generic for all parametric/nested types.

### 2) `ops/` (operations)
Pydantic models that describe *intent* (DDL/DML), e.g.:
- DDL: `CreateSchema`, `CreateTable`, `AddColumns`, `SetPartitioning`, …
- DML: `Select`, `InsertSelect`, `InsertValues`, `Delete`, `Update`, `Merge`, …

These models do **not** execute SQL. They are inputs to the validator and renderer.

### 3) `sql/validators.py`
Validation is **two-layered**:
- Pydantic validates structure/types.
- `validate_op()` and `validate_table_ddl()` enforce **semantic rules** + **capabilities** + **safety policies**.

Example policies:
- Hive/S3: allow SELECT/INSERT/CTAS, disallow UPDATE/DELETE/MERGE by default.
- Forbid DELETE/UPDATE without WHERE (optional).
- Partition keys must exist and not be complex types.

### 4) `sql/renderer.py`
Renders ops into SQL using Jinja2 templates:
- Maps op class → template path (`OP_TEMPLATE`)
- Uses `StrictUndefined` to fail fast on template/model mismatches
- `render(op)` / `render_many(ops)`

Templates live under `templates/`:
```
templates/
  ddl/
  dml/
  _macros/
```

### 5) Optional execution (`db/`)
Basic DB-API wrapper around the Trino python client:
- `Client` manages a lazy connection, supports context manager
- `SQLClient.execute(sql)` executes SQL and returns fetched rows

Execution is optional — this project is primarily a **SQL generator**.

### 6) Managers (optional convenience API)
Managers wrap common workflows (compile/apply):
- validate → render → optionally execute

Example: `TrinoDDLManager` for schema/table/columns/properties.

---

## Installation

Dependencies you should expect:
- `pydantic>=2`
- `jinja2`
- `trino` (python client)

---

## DDL examples
```python
from __future__ import annotations

from trino_ops import DDLClient

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
    path_to_pem=Path("./trino_ops"),
    file_name_pem="trino.pem",
)


```

### Execute query
```python
conf.execute_query("SELECT orderkey, orderstatus, totalprice, orderdate FROM _debug.orders")
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

### Create Table
```python
conf.create_table(if_not_exists=True)
```

### Drop Table
```python
conf.drop_table(if_exists=True)
```

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
    path_to_pem=Path("./trino_ops"),
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

### Insert Values
```python
conf.insert_values(rows=[
    ['a', ['d1', 'd2'], 'a1', 'b1'],
    ['a', ['d3', 'd4'], 'a2', 'b2'],
    ['a', ['d3', 'd4'], 'a2', 'b3'],
    ['a', ['d3', 'd4'], 'a2', 'b3'],
    ['a', ['d3', 'd4'], 'a1', 'b3'],  
])
```


### Create table AS

```python
sql = """
SELECT c, d, a, b
FROM datalake.stage.events
"""
```

```python
conf.create_table_as(
    table_schema="stage",
    table_name="events_tmp",
    sql=sql,
    properties={
        'format': "ORC",
        'partitioned_by': ["a", "b"],
    }
)
```

### Insert Select
```python
conf.insert_select(sql=sql)
```
