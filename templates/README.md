# SQL Value Rendering (Trino) — Literals, Wrappers, and Jinja Macros

This module provides two layers that work together:

1) **Python wrapper classes** (dataclasses) that express *intent*  
2) **Jinja macros** that turn Python values + wrappers into correct Trino SQL literals/expressions

The goal: safely render complex values (arrays, maps, rows, JSON, casts, raw SQL) for `INSERT ... VALUES` and other generated statements.

---

## Concept: “Intent beats guessing”

Rendering SQL is all about intent:

- A Python `str` could be: a text literal, JSON text, or a raw SQL function name.
- A Python `list` could be: an array, or “a row of fields” (ambiguous).
- A Python `dict` could be: a map, or JSON, or something you never intended to serialize.

So we use **explicit wrappers** whenever meaning matters.

---

## Wrapper classes

### `SqlExpr(sql: str)`
Use when you want **raw SQL** emitted verbatim (no quoting).

```python
SqlExpr("current_timestamp")      # -> current_timestamp
SqlExpr("uuid()")                 # -> uuid()
SqlExpr("date_trunc('day', x)")   # -> date_trunc('day', x)
```

### `ArrayLiteral(items: Sequence[Any])`
Explicit “this is an ARRAY”.

```python
ArrayLiteral(["a", "b"])          # -> ARRAY['a','b']
ArrayLiteral([1, 2, 3])           # -> ARRAY[1,2,3]
```

### `RowLiteral(items: Sequence[Any])`
Explicit “this is a ROW(...)”.

```python
RowLiteral([1, "x", True])        # -> ROW(1,'x',TRUE)
```

### `MapLiteral(keys, values)` / `MapLiteral.from_dict(d)`
Explicit “this is a MAP”.

```python
MapLiteral.from_dict({"a": 1, "b": 2})
# -> MAP(ARRAY['a','b'], ARRAY[1,2])
```

### `JsonLiteral(json_text: str)` / `JsonLiteral.from_obj(obj)`
Explicit “this is JSON text”, rendered as `JSON_PARSE('...')`.

```python
JsonLiteral.from_obj({"a": 1, "b": ["x", "y"]})
# -> JSON_PARSE('{"a": 1, "b": ["x", "y"]}')
```

### `Cast(expr: Any, type_sql: str)`
Explicit “cast this expression/value to a specific SQL type”.

```python
Cast("123", "integer")            # -> CAST('123' AS integer)
Cast(ArrayLiteral(["a","b"]), "array(varchar)")
# -> CAST(ARRAY['a','b'] AS array(varchar))
```

---

## Jinja macros overview

### `sql_value(v)`
This is the main entrypoint used everywhere (arrays, rows, map values, etc.).

Rules (in order):

1. `bool` → `TRUE/FALSE`
2. `int/float` → printed as-is
3. `None` → `NULL`
4. Wrapper objects by class name:
   - `SqlExpr` → `v.sql`
   - `Cast` → `CAST(sql_value(v.expr) AS v.type_sql)`
   - `RowLiteral` → `ROW(...)`
   - `MapLiteral` → `MAP(ARRAY[...], ARRAY[...])`
   - `JsonLiteral` → `JSON_PARSE('...')`
   - `ArrayLiteral` → `ARRAY[...]`
5. Native Python containers:
   - `sequence and not string` → `ARRAY[...]`
6. `string` → `'...'` (with `'` escaped)
7. fallback → `{{ v }}` (last resort)

### Why a Python `list` becomes `ARRAY[...]`
Because of this clause:

```jinja2
{%- elif v is sequence and v is not string -%}
{{ sql_array(v) }}
```

In Jinja, a Python `list` is a `sequence`, so it gets rendered as a Trino `ARRAY[...]`.
Strings are excluded because strings are also sequences of characters.

---

## Examples

### 1) Plain scalars
```python
rows = [
  [1, "hello", True, None],
]
```

SQL:
```sql
(1, 'hello', TRUE, NULL)
```

### 2) Automatic arrays from Python lists/tuples
```python
rows = [
  ["a", ["d1", "d2"]],
]
```

SQL:
```sql
('a', ARRAY['d1', 'd2'])
```

Nested lists become nested arrays:
```python
rows = [
  [[[1, 2], [3, 4]]],
]
```

SQL:
```sql
(ARRAY[ARRAY[1, 2], ARRAY[3, 4]])
```

### 3) Use `ArrayLiteral` when you want to be explicit
This is functionally the same as passing a raw list, but documents intent.

```python
rows = [
  ["a", ArrayLiteral(["d1", "d2"])],
]
```

SQL:
```sql
('a', ARRAY['d1', 'd2'])
```

### 4) JSON values
If you pass a JSON string as a normal Python string, it becomes a SQL string:

```python
rows = [
  ['{"a":1}'],
]
# -> ('{"a":1}')   -- just text, not json
```

If you want JSON type, use `JsonLiteral`:

```python
rows = [
  [JsonLiteral.from_obj({"a": 1})],
]
```

SQL:
```sql
(JSON_PARSE('{"a": 1}'))
```

If the target column is `json`, this is usually what you want.

### 5) JSON + CAST
Sometimes you want to force the final type:

```python
rows = [
  [Cast(JsonLiteral.from_obj({"a": 1}), "json")],
]
```

SQL:
```sql
(CAST(JSON_PARSE('{"a": 1}') AS json))
```

### 6) MAP literals
Use `MapLiteral` (raw dicts are intentionally discouraged; see “Gotchas”).

```python
rows = [
  [MapLiteral.from_dict({"a": 1, "b": 2})],
]
```

SQL:
```sql
(MAP(ARRAY['a', 'b'], ARRAY[1, 2]))
```

### 7) ROW literals
Use `RowLiteral` to avoid ambiguity with arrays:

```python
rows = [
  [RowLiteral([1, "x", True])],
]
```

SQL:
```sql
(ROW(1, 'x', TRUE))
```

### 8) Raw SQL expressions
Use `SqlExpr` when you need a function or expression, not a string literal:

```python
rows = [
  ["id1", SqlExpr("current_timestamp")],
]
```

SQL:
```sql
('id1', current_timestamp)
```

---

## Gotchas and best practices

### 1) Avoid raw `dict` values
Your current Jinja macro does **not** define how to render a dict. If a dict reaches the fallback branch, you’ll likely get invalid SQL.

Best practice:
- Use `JsonLiteral.from_obj(d)` for JSON intent
- Use `MapLiteral.from_dict(d)` for MAP intent

### 2) Lists/tuples are treated as arrays by default
This line makes *every* non-string sequence an `ARRAY[...]`:

```jinja2
elif v is sequence and v is not string
```

That means:
- `tuple` becomes an array, not a row
- `bytes` (also a sequence) might behave unexpectedly

Best practice:
- Use `RowLiteral([...])` when you mean `ROW(...)`
- Use `ArrayLiteral([...])` when you want to be explicit and readable

### 3) Use `Cast` for type stability
Trino sometimes needs help inferring types (especially empty arrays or mixed arrays).

Examples:
```python
Cast(ArrayLiteral([]), "array(varchar)")          # empty array with explicit element type
Cast(JsonLiteral.from_obj({}), "json")            # make sure it is json
```

### 4) SQL injection safety
`sql_string()` escapes single quotes by doubling them:
- `"O'Reilly"` → `'O''Reilly'`

But **SqlExpr is raw SQL**. Treat it as “unsafe by design”:
- use it only with trusted expressions you control.

---

## Recommended usage patterns

### For “normal” inserts
- scalars as scalars (`str/int/bool/None`)
- arrays as lists (or `ArrayLiteral`)
- rows as `RowLiteral`
- JSON as `JsonLiteral` (optionally + `Cast(..., "json")`)
- maps as `MapLiteral`

### Example: everything together
```python
rows = [
  [
    "event_1",
    ArrayLiteral(["d1", "d2"]),
    MapLiteral.from_dict({"k": 1}),
    Cast(JsonLiteral.from_obj({"a": 1, "b": ["x"]}), "json"),
    SqlExpr("current_timestamp"),
    RowLiteral([1, "x", True]),
  ]
]
```

Renders roughly to:
```sql
(
  'event_1',
  ARRAY['d1', 'd2'],
  MAP(ARRAY['k'], ARRAY[1]),
  CAST(JSON_PARSE('{"a": 1, "b": ["x"]}') AS json),
  current_timestamp,
  ROW(1, 'x', TRUE)
)
```

---

## Notes for maintainers

### Wrapper detection in Jinja
Right now wrapper detection is done by class name:

```jinja2
v.__class__.__name__ == "SqlExpr"
```

This works but is fragile when refactoring. A more robust solution is to move wrapper detection into Python (normalize values before templating), or to expose wrapper classes/types into the Jinja environment and check against them explicitly.

If you keep the class-name approach, treat wrapper class names as part of the public API (renaming them becomes a breaking change).
