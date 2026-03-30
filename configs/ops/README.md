# ops/ — Operations (DDL + DML)

The ops/ package contains declarative Pydantic models that describe what you want to do in Trino, without executing anything by themselves.

These models are later:
-	validated (capabilities + safety rules)
-	rendered into SQL using Jinja2 templates
-	optionally executed by an external executor layer

---

## Directory layout

trino_ops/configs/ops/
```
- ddl/        # schema/table definition changes (metadata)
- dml/        # data manipulation queries
```

---

# DDL operations (ops/ddl)

### DDL models describe schema/table metadata changes.

## `Schema`
- `CreateSchema(table_schema, if_not_exists=True)`
- `DropSchema(table_schema, if_exists=True, cascade=False)`

## `Tables`
- `CreateTable(table, if_not_exists=True)`
- `DropTable(table, if_exists=True)`
- `RenameTable(table, new_table_name)`

## `Columns`
- `AddColumns(table, columns)`
- `DropColumns(table, colnames)`
- `RenameColumn(table, old_name, new_name)`

## `Properties`
- `SetPartitioning(table, partitioned_by)`
- `SetTableLocation(table, external_location)`
- `SetFileFormat(table, format)`

---

# DML operations (ops/dml)

### DML models describe queries and data changes.

## `Common helpers`
- `Where(sql)`
- `OrderBy(cols, otype="ASC")`
- `Query(sql, params={...})`
- `SqlExpr(sql) (raw SQL fragment wrapper)`

## `Queries`
- `Select(table, columns=["*"], where=None, order_by=None, limit=None)`
- `SelectQuery(query) (raw query wrapper)`

## `Inserts`
- `InsertValues(table, columns, rows)`
- `InsertSelect(table, columns=None, select_sql)`

## `Mutations (connector-dependent)`
- `Delete(table, where=None)`
- `Update(table, set={}, where=None)`
- `Merge(target, source_sql, on, when_matched=None, when_not_matched=None)`
- `Truncate(table)`

Note: `DELETE`/`UPDATE`/`MERGE`/`TRUNCATE` support depends on the connector/table format
(Hive/S3 often restricts these unless using Iceberg/Delta/Hudi). Use capabilities
validation to control what is allowed.
