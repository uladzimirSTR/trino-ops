# Examples

```python
from trino_ops.sql import RenderConfig, SqlRenderer
from trino_ops.sql.validators import validate_op, validate_plan, ValidationContext, Capabilities

renderer = SqlRenderer()

# Hive/S3 типичный профиль: UPDATE/DELETE/MERGE отключены
ctx = ValidationContext(
    capabilities=Capabilities(
        allow_select=True,
        allow_insert=True,
        allow_ctas=True,
        allow_truncate=True,
        allow_delete=False,
        allow_update=False,
        allow_merge=False,
        allow_insert_values=True,
    )
)
```
```python
from trino_ops.configs.ops.dml.select import SelectQuery, Query
from trino_ops.configs.ops.dml.common import Where, OrderBy

from trino_ops.configs import TrinoTableDDL, Column, TrinoSchema, TableProp
from trino_ops.configs.ops.ddl.table import DropTable

schema = TrinoSchema(catalog="datalake", name="analytics", location="s3a://prod-datalake/")

ref = TrinoTableDDL(
    table_schema=schema,
    table_name="events",
)
```

# SELECT
```python
op = Select(
    table=ref,
    columns=['event_id', 'ts', 'dt'],
    where=Where(sql='"dt" >= DATE \'2026-01-01\''),
    order_by=OrderBy(
        cols=['ts'],
        otype="DESC",
    ),
    limit=100
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
SELECT
  event_id, ts, dt
FROM "datalake"."analytics"."events"
WHERE "dt" >= DATE '2026-01-01'ORDER BY ts DESC
LIMIT 100
```

```python
op = SelectQuery(
    query=Query(sql="select 1")
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```

```result
select 1
```

# CREATE AS TABLE
```python
from trino_ops.configs.ops.dml.ctas import CreateTableAs

op = CreateTableAs(
    table_schema=schema,
    table_name="events_2026",
    select_sql='SELECT * FROM "datalake"."analytics"."events" WHERE "dt" >= DATE \'2026-01-01\'',
    properties={
        "format": "PARQUET",
        "external_location": "s3a://prod-datalake/analytics/events_2026/",
        "partitioned_by": ["dt"],
    },
    if_not_exists=True
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Resutl
```sql
CREATE TABLE IF NOT EXISTS "datalake"."analytics"."events_2026"
WITH (  format = 'PARQUET'  external_location = 's3a://prod-datalake/analytics/events_2026/'  partitioned_by = ARRAY['dt'])
AS
SELECT * FROM "hive"."analytics"."events" WHERE "dt" >= DATE '2026-01-01'
```

# DELETE
```python
from trino_ops.configs.ops.dml.delete import Delete

op = Delete(
    table=ref,
    where=Where(sql='"dt" >= DATE \'2026-01-01\''),
)

# validate_op(op, ctx=ctx)
print(renderer.render(op))

```
### Result
```sql
DELETE FROM "datalake"."analytics"."events"
WHERE "dt" >= DATE '2026-01-01'
```

# INSERT
```python
from trino_ops.configs.ops.dml.insert import InsertValues

op = InsertValues(
    table=ref,
    columns=["col1", "col2"],
    rows=[["val1", "val2"], ["val3", "val4"]],
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```

### Result
```sql
INSERT INTO "datalake"."analytics"."events" (col1, col2)
VALUES  ("val1", "val2"),  ("val3", "val4")
```

# INSERT WITH QUERY
```python
from trino_ops.configs.ops.dml.insert import InsertSelect

op = InsertSelect(
    table=ref,
    # columns=["col1", "col2"],
    select_sql="Select 1"
)

validate_op(op, ctx=ctx)
print(renderer.render(op))

```
### Result
```sql
INSERT INTO "datalake"."analytics"."events"
Select 1
```

# TRUNCATE
```python
from trino_ops.configs.ops.dml.truncate import Truncate

op = Truncate(
    table=ref,
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
TRUNCATE TABLE "datalake"."analytics"."events"
```
