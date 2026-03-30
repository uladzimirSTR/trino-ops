# Examples

```python
from trino_crud.sql import RenderConfig, SqlRenderer
from trino_crud.sql.validators import validate_op, validate_plan, ValidationContext, Capabilities

renderer = SqlRenderer()

# Hive/S3 typical profile: UPDATE/DELETE/MERGE off
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

# Schema

### CREATE SCHEMA
```python
from trino_crud.configs import TrinoSchema
from trino_crud.configs.ops.ddl.schema import CreateSchema

op = CreateSchema(
    table_schema=TrinoSchema(
        catalog="datalake",
        name="analytics",
        location="s3a://prod-datalake/"
    ),
    if_not_exists=True
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
CREATE SCHEMA IF NOT EXISTS "datalake"."analytics"
WITH (  location = 's3a://prod-datalake/analytics')
```

### DROP SCHEMA
```python
from trino_crud.configs import TrinoSchema
from trino_crud.configs.ops.ddl.schema import DropSchema

op = DropSchema(
    table_schema=TrinoSchema(
        catalog="datalake",
        name="analytics",
        location="s3a://prod-datalake/"
    ),
    if_exists=True,
    # cascade=True
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
DROP SCHEMA IF EXISTS "datalake"."analytics"
```

# Table

### CREATE TABLE
```python
from trino_crud.configs import TrinoTableDDL, Column, TrinoSchema, TableProp
from trino_crud.configs.ops.ddl.table import CreateTable

schema = TrinoSchema(catalog="datalake", name="analytics", location="s3a://prod-datalake/")

table = TrinoTableDDL(
    table_schema=schema,
    table_name="events",
    columns=[
        Column(colname="a", coltype="varchar"),
        Column(colname="b", coltype="varchar"),
        Column(colname="c", coltype="varchar"),
        Column(colname="d", coltype="array"),
    ],
    table_prop=TableProp(
        format="ORC",
        partitioned_by=["a", "b"]
    ),
)

op = CreateTable(table=table, if_not_exists=True)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
CREATE TABLE IF NOT EXISTS "datalake"."analytics"."events" (  "a" varchar,  "b" varchar,  "c" varchar,  "d" array)
WITH (  format = 'ORC'  partitioned_by = ARRAY['a', 'b']  external_location = 's3a://prod-datalake/analytics/events/*')
```

### DROP TABLE
```python
from trino_crud.configs import TrinoTableDDL, Column, TrinoSchema, TableProp
from trino_crud.configs.ops.ddl.table import DropTable

schema = TrinoSchema(catalog="datalake", name="analytics", location="s3a://prod-datalake/")

table = TrinoTableDDL(
    table_schema=schema,
    table_name="events",
)

op = DropTable(table=table, if_exists=True)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
DROP TABLE IF EXISTS "datalake"."analytics"."events"
```

### RENAME TABLE
```python
from trino_crud.configs import TrinoTableDDL, Column, TrinoSchema, TableProp
from trino_crud.configs.ops.ddl.table import RenameTable

schema = TrinoSchema(catalog="datalake", name="analytics", location="s3a://prod-datalake/")

table = TrinoTableDDL(
    table_schema=schema,
    table_name="events",
)

op = RenameTable(table=table, new_table_name="new_events")

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
ALTER TABLE "datalake"."analytics"."events" RENAME TO "new_events"
```

# Columns

### ADD COLUMNS
```python
from trino_crud.configs.ops.ddl.columns import AddColumns
from trino_crud.configs import TrinoTableRef

ref = TrinoTableRef(table_schema=schema, table_name="events")

op = AddColumns(
    table=ref,
    columns=[
        Column(colname="a", coltype="varchar"),
        Column(colname="b", coltype="varchar"),
    ]
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
ALTER TABLE "datalake"."analytics"."events"
ADD COLUMN (  "a" varchar,  "b" varchar)
```

### DROP COLUMN
```python
from trino_crud.configs.ops.ddl.columns import DropColumns
from trino_crud.configs import TrinoTableRef

ref = TrinoTableRef(table_schema=schema, table_name="events")

op = DropColumns(
    table=ref,
    colnames=["a", "b"]
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
ALTER TABLE "datalake"."analytics"."events" DROP COLUMN "a";
ALTER TABLE "datalake"."analytics"."events" DROP COLUMN "b";
```

### RENAME COLUMN
```python

from trino_crud.configs.ops.ddl.columns import RenameColumn
from trino_crud.configs import TrinoTableRef

ref = TrinoTableRef(table_schema=schema, table_name="events")

op = RenameColumn(
    table=ref,
    old_name="a",
    new_name="b"
)

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
ALTER TABLE "datalake"."analytics"."events" RENAME COLUMN "a" TO "b"
```

# Properties

### SET PARTITION KEYS
```python
from trino_crud.configs import TrinoTableDDL, Column, TrinoSchema, TableProp
from trino_crud.configs.ops.ddl.properties import SetPartitioning

schema = TrinoSchema(catalog="datalake", name="analytics", location="s3a://prod-datalake/")

table = TrinoTableDDL(
    table_schema=schema,
    table_name="events",
)

op = SetPartitioning(table=table, partitioned_by=["a", "b"])

validate_op(op, ctx=ctx)
print(renderer.render(op))
```

### Result
```sql
ALTER TABLE "datalake"."analytics"."events"
SET PROPERTIES
(  partitioned_by = ARRAY['a', 'b'])
```

### SET TABLE LOCATION
```python
from trino_crud.configs import TrinoTableDDL, Column, TrinoSchema, TableProp
from trino_crud.configs.ops.ddl.properties import SetTableLocation

schema = TrinoSchema(catalog="datalake", name="analytics", location="s3a://prod-datalake/")

table = TrinoTableDDL(
    table_schema=schema,
    table_name="events",
)

op = SetTableLocation(table=table, external_location="s3a://prod-datalake/new_location/")

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
ALTER TABLE "datalake"."analytics"."events"
SET PROPERTIES
(  external_location = 's3a://prod-datalake/new_location/')
```

### SET FILE FORMAT
```python
from trino_crud.configs import TrinoTableDDL, Column, TrinoSchema, TableProp
from trino_crud.configs.ops.ddl.properties import SetFileFormat

schema = TrinoSchema(catalog="datalake", name="analytics", location="s3a://prod-datalake/")

table = TrinoTableDDL(
    table_schema=schema,
    table_name="events",
)

op = SetFileFormat(table=table, format="CSV")

validate_op(op, ctx=ctx)
print(renderer.render(op))
```
### Result
```sql
ALTER TABLE "datalake"."analytics"."events"
SET PROPERTIES
(  format = 'CSV')
```
