# `sql/` — Rendering and validation

The `sql/` package is the “engine room” of this project. It provides:

- **Validation** (`validators.py`): capability + safety checks for operations (`ops/*`)
- **Rendering** (`renderer.py`): conversion of validated ops into SQL using Jinja2 templates

This package intentionally does **not** execute SQL. Execution is handled elsewhere
(e.g., `db/` clients or a custom executor).

---

## Directory overview

```
trino_crud/sql/
  renderer.py
  validators.py
```

Templates live under the package `templates/` directory (sibling to `sql/`):

```
trino_crud/templates/
  ddl/
  dml/
  _macros/
```

---

## `renderer.py`

### Responsibilities
`SqlRenderer` takes a Pydantic op instance and produces a SQL string by:

1. Finding the template mapped to the op class (`OP_TEMPLATE`)
2. Calling `op.model_dump()` (Pydantic v2) to get a plain dict payload
3. Rendering the corresponding `.sql.j2` template with Jinja2

Key design choices:
- Uses **`StrictUndefined`** to fail fast when templates reference missing variables.
- Separates “what to do” (ops/models) from “how to write SQL” (templates/macros).

### Public API

```python
from trino_crud.sql import SqlRenderer

renderer = SqlRenderer()

sql = renderer.render(op)
sql_list = renderer.render_many([op1, op2, op3])
```

### RenderConfig

`RenderConfig` controls Jinja2 environment options and where templates are loaded from:

- `templates_dir`: templates root (defaults to `<package>/templates`)
- `trim_blocks`, `lstrip_blocks`: whitespace control
- `keep_trailing_newline`: preserve final newline if desired

Example:
```python
from trino_crud.sql.renderer import SqlRenderer, RenderConfig
from pathlib import Path

renderer = SqlRenderer(
    config=RenderConfig(
        templates_dir=Path("my_templates"),
        trim_blocks=True,
        lstrip_blocks=True,
    )
)
```

### Template mapping

Ops are mapped to templates by type:

```python
from trino_crud.sql.renderer import OP_TEMPLATE
# { CreateTable: "ddl/create_table.sql.j2", Select: "dml/select.sql.j2", ... }
```

If an op class has no mapping, rendering fails fast with a `KeyError`.

---

## `validators.py`

### Responsibilities
Validation is a separate step (and should happen **before** rendering/execution).

Validation is composed of:
1. **Pydantic validation**: field types, discriminated unions, model validators, etc.
2. **Semantic checks**: connector-specific rules, safety policies, and capability flags

### Capabilities

`Capabilities` describes what your target Trino connector/catalog supports.

Example: Hive on S3 is commonly:
- SELECT: yes
- INSERT ... SELECT: yes
- CTAS: yes
- UPDATE/DELETE/MERGE: often no (unless Iceberg/Delta/Hudi)

Example:
```python
from trino_crud.sql.validators import Capabilities, ValidationContext

ctx = ValidationContext(
    capabilities=Capabilities(
        allow_select=True,
        allow_insert=True,
        allow_ctas=True,
        allow_truncate=True,
        allow_delete=False,
        allow_update=False,
        allow_merge=False,
        allow_insert_values=False,
    )
)
```

### Safety policies

`ValidationContext` also provides safety toggles, for example:
- forbid DELETE without WHERE
- forbid UPDATE without WHERE
- forbid TRUNCATE (optional extra safety)

These are “guardrails” on top of pure capability support.

### Public API

```python
from trino_crud.sql.validators import validate_op, validate_plan

validate_op(op, ctx=ctx)          # validates a single op
validate_plan([op1, op2], ctx=ctx) # validates a list/plan of ops
```

If validation fails, a `ValidationError` is raised with a helpful message.

### Table DDL semantic checks

`validate_table_ddl(table: TrinoTableDDL)` adds extra schema rules such as:
- `partitioned_by` columns must exist in `columns`
- partition keys must not be complex types (array/map/row)
- file format must be one of PARQUET/ORC/AVRO (fallback sanity check)

> These checks complement (not replace) Pydantic-level field validation.

---

## Typical workflow

1) Build ops (Pydantic models)
2) Validate them using capabilities + safety policies
3) Render SQL via templates

```python
from trino_crud.sql import SqlRenderer
from trino_crud.sql.validators import validate_op, ValidationContext, Capabilities
from trino_crud.configs import TrinoSchema
from trino_crud.configs.ops.ddl.schema import CreateSchema

renderer = SqlRenderer()

ctx = ValidationContext(
    capabilities=Capabilities(
        allow_select=True,
        allow_insert=True,
        allow_ctas=True,
        allow_truncate=True,
        allow_delete=False,
        allow_update=False,
        allow_merge=False,
        allow_insert_values=False,
    )
)

op = CreateSchema(
    table_schema=TrinoSchema(
        catalog="datalake",
        name="analytics",
        location="s3a://prod-datalake/",
    ),
    if_not_exists=True,
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```

---

## Template tips (practical gotchas)

- Avoid dict attribute collisions in templates:
  - For dicts, prefer `obj["key"]` over `obj.key` when keys could collide with
    dict methods (e.g., `items`, `values`).
- Prefer helper macros for:
  - identifiers (`qident`, `fq_table`)
  - values (`sql_value`, `sql_array`) so lists become `ARRAY['a','b']`
  - safe path joining (`s3_join`) to avoid `//` in S3 URIs
- Do not use glob patterns like `*` in `external_location`; treat it as a directory.

---

## Testing idea (recommended)

A simple, high-value test suite is “golden SQL” snapshot tests:
- given op input → validate → render → compare to expected SQL

This catches template regressions quickly and keeps behavior stable.

---

## Roadmap ideas

- Add an optional linting step (SQL formatting policy)
- Add schema diff/sync planning (desired DDL → minimal ALTER plan)
- Improve type modeling (replace `Literal[...]` with structured type specs)
