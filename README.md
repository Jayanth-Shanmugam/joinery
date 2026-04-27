# joinery

A lightweight federated query engine for small-to-medium data teams.

## What it does

Joinery lets analysts write a single SQL query that references tables across
multiple databases as if they all belonged to one. The engine handles the
heavy lifting — opening connections, fetching the relevant data, and joining
the results — so analysts can focus on the SQL.

```sql
SELECT u.name, o.amount
FROM postgres_prod.users u
JOIN mysql_analytics.orders o ON u.id = o.user_id
WHERE o.amount > 100
```

The `postgres_prod.` and `mysql_analytics.` prefixes refer to entries in a
catalog file that maps each alias to a database connection.

## Why

Teams who don't need a distributed engine like Spark still spend a lot of
time writing glue code to pull data out of multiple databases, materialize
intermediate DataFrames, and join them in pandas. Joinery removes that
boilerplate and replaces it with a single `engine.query(sql)` call.

## How it works

```
catalog.yaml ──▶ Engine ──▶ Parser (sqlglot)
                              │
                              ▼
                            Planner ──▶ Connectors ──▶ Arrow tables
                                                            │
                                                            ▼
                                                   Executor (DuckDB)
                                                            │
                                                            ▼
                                                  Result (pandas / Arrow / DuckDB)
```

1. **Catalog** — a YAML file describes each database (type, connection details).
2. **Parser** — sqlglot extracts every `alias.table` reference from the query.
3. **Planner** — groups references by alias so each database is hit at most once.
4. **Connectors** — fetch the required tables and return them as Arrow tables.
5. **Executor** — registers the Arrow tables in an in-memory DuckDB instance,
   rewrites the query to drop alias prefixes, and executes the join locally.
6. **Result** — a thin wrapper that converts to pandas, Arrow, or a DuckDB relation
   on demand.

## Tech stack

- **sqlglot** — SQL parsing and rewriting
- **pyarrow** — universal in-memory data format between connectors and DuckDB
- **duckdb** — final join and aggregation execution
- **sqlalchemy** — universal database adapter (Postgres, MySQL, SQLite, MSSQL, …)
- **pyyaml** — catalog file parsing
- Python 3.12, managed with [uv](https://docs.astral.sh/uv/)

## Catalog example

```yaml
version: "1"
databases:
  postgres_prod:
    type: postgresql
    host: db.example.com
    port: 5432
    username: analyst
    password: ${POSTGRES_PASSWORD}
    database: analytics
    query:
      sslmode: require

  mysql_analytics:
    type: mysql
    url: "mysql+pymysql://user:${MYSQL_PASSWORD}@host:3306/db"

  sqlite_ref:
    type: sqlite
    path: "./reference.db"
```

`${VAR}` placeholders are interpolated from environment variables at load time.

## Status

Early development. The connector layer and catalog loader are implemented;
the parser, planner, executor, and top-level `Engine` are next.
