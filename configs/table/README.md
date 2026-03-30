# table/ — Schema, Table and Column models

The table/ package contains Pydantic models that describe Trino schemas, table references, and table definitions. These models are used by ops/ (DDL/DML operations) and later rendered into SQL via Jinja2 templates.

This package is intentionally declarative: it does not connect to Trino or execute queries. It only describes metadata and constraints.

---

## What’s inside

# Typical models in this category:
- TrinoSchema — catalog + schema name + default S3 location
- TrinoTableRef — fully-qualified reference to a table (schema + table name)
- TrinoTableDDL — table definition (columns + table properties)
- Column — column name, data type, optional comment
- TableProp (or TableStorage) — table-level storage properties (format, partitioning, external location)
