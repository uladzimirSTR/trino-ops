# SQL Jinja Macros (Trino/Presto-style)

This folder contains small, reusable **Jinja2 macros** that generate valid SQL fragments for:
- quoting identifiers (catalog/schema/table names),
- rendering `WITH (...)` / `SET PROPERTIES (...)` blocks,
- converting Python values into SQL literals (`true`, `NULL`, strings, arrays, etc.).

They are meant to be imported into your operation templates (DDL/DML) so you don’t repeat boilerplate and don’t accidentally produce broken SQL.

---

## Files

### 1) `_macros/identifiers.sql.j2`

Macros for **quoting identifiers** and building **fully-qualified names**.

#### `qident(name)`
Wraps an identifier in double quotes.

```jinja2
{{ qident("my_table") }}
```

Output:
```sql
"my_table"
```
### Why:
- protects against reserved keywords (e.g., user, order)
- keeps exact casing
- special characters (within limits of your SQL engine)

Note: This macro does not escape double quotes inside name. If you need that, add escaping logic (`replace('"', '""')`).

### `fq_schema(schema)`
Renders a fully-qualified schema: "catalog"."schema".

Expected input shape:
- `schema.catalog`
- `schema.name`

Example:

```jinja2
{{ fq_schema(schema) }}
```

If:
- `schema.catalog` = "hive"
- `schema.name` = "analytics"

Output:
```sql
"hive"."analytics"
```

### `fq_table(table_ref)`
Renders a fully-qualified table: "catalog"."schema"."table".

Expected input shape:
- `table_ref.schema.catalog`
- `table_ref.schema.name`
- `table_ref.table_name`

Example:
```jinja2
{{ fq_table(table_ref) }}
```

If:
- `table_ref.schema.catalog` = "hive"
- `table_ref.schema.name` = "analytics"
- `table_ref.table_name` = "events"

Output:

```jinja2
"hive"."analytics"."events"
```

⸻

### 2) `_macros/values.sql.j2`

Macros for rendering Python values into SQL literals safely-ish (string escaping is handled; other types are best-effort).

### `sql_string(s)`
Renders a SQL single-quoted string, escaping ' by doubling it.

```jinj2
{{ sql_string("O'Reilly") }}
```

Output:
```sql
'O''Reilly'
```

`sql_value(v)`

Converts a value into SQL depending on its type:

`Python type` - `value	Output example`

- `True` / `False`	- `true` / `false`
- `numbers(1, 3.14)`- `1`, `3.14`
- `None` --	`NULL`
- `list`/`tuple`/`etc`. `(not string)`	`ARRAY[...]`
- everything else	string literal via `sql_string(...)`

Examples:

- `{{ sql_value(True) }}`         -- `true`
- `{{ sql_value(12) }}`           -- `12`
- `{{ sql_value(None) }}`         -- `NULL`
- `{{ sql_value("hello") }}`      -- `'hello'`
- `{{ sql_value(["a", "b"]) }}`   -- `ARRAY[a, b]   (see note below)`

Important note about arrays
Current implementation renders sequences like this:

`ARRAY[{{ v | map('string') | map('trim') | join(', ') }}]`

That means:
 - it does not quote string elements automatically.
- `["a", "b"]` becomes `ARRAY[a, b]` (invalid in many SQL dialects unless a and b are identifiers).

If you want proper SQL string arrays, change it to quote each element, e.g. conceptually:
- `ARRAY['a','b']`

Implementation idea (example approach):
- map each item through `sql_string` if it’s a string, else keep numeric/bool/null formatting.

⸻

3) ### `_macros/props.sql.j2`

Macros for rendering properties blocks used in Trino/Presto `DDL`/`ALTER` statements.

This file imports value rendering helpers:
```jinja2
{% import "_macros/values.sql.j2" as vals %}
```

`with_props(props)`

Renders:

```sql
WITH (
  k1 = v1,
  k2 = v2
)
```

Only renders anything if props exists and is non-empty.

Example:
```jinja2
{{ with_props({
  "format": "PARQUET",
  "partitioned_by": ["ds", "country"],
  "external_location": "s3://bucket/path/"
}) }}
```

Possible output (depends on vals.sql_value behavior, see array note above):
```sql
WITH (
  format = 'PARQUET',
  partitioned_by = ARRAY[ds, country],
  external_location = 's3://bucket/path/'
)
```

```set_props(props)```
Renders:
```sql
SET PROPERTIES
(
  k1 = v1,
  k2 = v2
)
```

Used for `ALTER ... SET PROPERTIES (...)`-style statements (depending on engine/object).

Example:
```jinja2
ALTER TABLE {{ fq_table(table_ref) }}
{{ set_props({
  "retention_days": 30,
  "comment": "hot table"
}) }}
```
Output:

```sql
ALTER TABLE "hive"."analytics"."events"
SET PROPERTIES
(
  retention_days = 30,
  comment = 'hot table'
)
```

⸻

Typical Usage

In a template that generates SQL:
```jinja2
{% import "_macros/identifiers.sql.j2" as id %}
{% import "_macros/props.sql.j2" as p %}

CREATE TABLE {{ id.fq_table(table_ref) }} (
  "id" BIGINT,
  "ds" VARCHAR
)
{{ p.with_props(props) }}
```
If props is empty / missing, WITH (...) is omitted cleanly.

⸻

Assumptions & Gotchas
- 1.	Identifier escaping
`qident()` wraps with "..." but does not escape internal quotes. If user input can contain ", add escaping.
- 2.	Arrays
`sql_value()` currently renders arrays without quoting strings. If you use string arrays in properties, you probably want to improve this macro.
- 3.	`SQL injection`
These macros reduce accidental syntax errors, but they are not a full security layer. Don’t pass untrusted raw strings as identifiers or properties without validation.
