# Joinery: A Lightweight, Federated Query Engine in Python

A lightweight federated query engine in Python that allows users to write SQL as if all tables exist in a single unified database. The engine then parses the query, analyzes its predicates, and constructs individual sub‑queries for each backend database.

This project is built on top of **sqlglot** for SQL parsing, with a focus on extensibility and clear, modular internals.

---

## Current Features (Implemented)

### 1. Query Parsing with sqlglot
- Uses `sqlglot` to parse SQL queries into an AST.
- Supports extraction of referenced tables, columns, and predicates.
- Safely validates and normalizes user‑provided SQL.

### 2. Predicate Pushdown
- Breaks down the `WHERE` clause into conjunctive predicates.
- Determines which predicates can be pushed down to individual backend databases.
- Produces optimized per‑source query fragments.

### 3. Individual Query Building
- For each backend database involved, constructs:
  - A minimal `SELECT` clause containing only required columns.
  - Pushed‑down filters.
  - Properly isolated sub‑queries for distributed execution.
- Ensures consistency in aliasing and naming across fragment queries.

---

## Next Iteration (Planned for Upcoming Release)

### 1. Database Cataloging
- A catalog layer mapping tables to physical database locations.
- Allows the engine to know *where* each table lives.
- Enables metadata‑aware optimization.

### 2. Backend Connection Layer (SQLAlchemy)
- Connection pooling and session management via SQLAlchemy.
- Uniform interface for executing sub‑queries across heterogeneous SQL backends.
- Support for multi‑dialect compilation.

### 3. Result Manifesting with PyArrow
- Returned results from each database will be collected into Arrow tables.
- Enables efficient in‑memory processing.
- Paves the way for zero‑copy interoperability with analytical tools.

---

## Future Developments

### Database Explorer UI (Flask)
- A web‑based interface for browsing catalogs, schemas, and table metadata.
- Interactive preview of tables and columns.

### Query Versioning
- Automatic version tracking for user queries.
- Ability to inspect and reproduce past queries.
- Built‑in diffing and auditability.

### Query Editor
- A lightweight SQL editor UI built into the Flask app.
- Syntax highlighting, validation, auto‑completion.
- Integrated execution output panel.

---

## Project Philosophy
- **Modular**: Each phase—parsing, planning, execution—is isolated and testable.
- **Extensible**: New database engines and optimizations can be plugged in easily.
- **Transparent**: Users should understand exactly how queries get split and executed.
