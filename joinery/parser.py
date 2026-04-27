"""SQL parsing for joinery.

The parser's job is to convert a raw SQL string into a structured
representation that downstream layers can act on, without doing any
catalog or planning work itself. Specifically it returns:

  1. The sqlglot AST (preserved for the executor's eventual rewrite).
  2. A flat list of ScopeInfo objects describing every query scope
     (the root SELECT, every CTE body, every subquery), in root-first
     order with parent_id pointers.
  3. Within each ScopeInfo, the catalog-resolved TableRefs that the
     scope actually reads from.

Joinery is opinionated: every base table reference must be qualified
as ``db_alias.table`` (or ``db_alias.schema.table``). Unqualified
references that aren't CTE names raise UnqualifiedTableError. This
removes a class of ambiguity from the planner.

Predicate-to-table binding (i.e. discovering which WHERE clauses can
be pushed down to which source) is the planner's job; the parser only
hands the planner the scope structure that makes it tractable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import Scope, build_scope

DEFAULT_DIALECT = "duckdb"

ScopeKind = Literal["root", "cte", "subquery"]


# --- Errors -----------------------------------------------------------------


class InvalidSQLError(ValueError):
    """Raised when sqlglot cannot parse the input as SQL."""


class UnsupportedStatementError(ValueError):
    """Raised when the input is syntactically SQL but not a read query.

    Joinery is read-only; INSERT/UPDATE/DELETE/CREATE/DROP/etc. are
    rejected at parse time so the user gets a clear error rather than
    a downstream surprise.
    """


class UnqualifiedTableError(ValueError):
    """Raised when a base table reference is missing its db_alias prefix.

    CTE references and subquery aliases are not base tables and never
    trigger this error.
    """


# --- Result types -----------------------------------------------------------


@dataclass(frozen=True)
class TableRef:
    """A single catalog-resolved table reference inside the query.

    Attributes:
        alias: The catalog key (e.g. "postgres_prod"). This is the
            first qualifier in the user-written SQL.
        schema: Optional schema/namespace within the database (e.g.
            "public" for Postgres). Set when the user wrote a
            three-part name; None for two-part names.
        table: Unqualified table name (e.g. "users").
        ast_node: The original sqlglot Table node. Held so the planner
            can map predicates back to this reference and so the
            executor can rewrite it during query rewriting.
    """

    alias: str
    schema: str | None
    table: str
    ast_node: exp.Table


@dataclass
class ScopeInfo:
    """Information about a single query scope.

    A scope is one of: the root SELECT, a CTE body, or a subquery.
    Scopes form a tree via parent_id; we expose them as a flat list in
    root-first order so callers can iterate without recursion.

    Attributes:
        scope_id: Stable integer id, unique within a ParsedQuery. The
            root scope is always id 0.
        kind: Whether this scope is the root, a CTE body, or a
            subquery (collapsed from sqlglot's finer taxonomy).
        parent_id: scope_id of the enclosing scope, or None for the
            root. Lexical parent — used to resolve CTE visibility.
        table_refs: Catalog-resolved TableRefs read directly by this
            scope. Tables read via nested CTEs/subqueries belong to
            those scopes, not this one.
        cte_names: Names of CTEs *defined* in this scope (i.e. visible
            to this scope and its descendants).
        sqlglot_scope: The underlying sqlglot Scope object. Kept so
            the planner can access AST-level details (column refs,
            WHERE clauses, etc.) without re-walking the tree.
    """

    scope_id: int
    kind: ScopeKind
    parent_id: int | None
    table_refs: list[TableRef]
    cte_names: set[str]
    sqlglot_scope: Scope = field(repr=False)


@dataclass
class ParsedQuery:
    """The parser's full output for a single SQL string.

    Attributes:
        ast: The sqlglot AST. The executor uses this for query
            rewriting after intermediate tables are materialized.
        scopes: All scopes in the query, root first, with parent_id
            pointers giving the tree structure.
    """

    ast: exp.Expr
    scopes: list[ScopeInfo]


# --- Public entry point -----------------------------------------------------


def parse_query(sql: str) -> ParsedQuery:
    """Parse a SQL string into an AST and a flat list of scopes.

    Performs three steps in order:
        1. Parse the SQL with sqlglot (DuckDB dialect).
        2. Validate that the statement is a read query.
        3. Walk the scope tree, classifying every table reference and
           rejecting unqualified base tables.

    Args:
        sql: The full SQL query as written by the analyst.

    Returns:
        A ParsedQuery containing the AST and the scope list.

    Raises:
        InvalidSQLError: The input is not parseable SQL.
        UnsupportedStatementError: The input is a non-SELECT statement.
        UnqualifiedTableError: A base table reference lacks a
            db_alias qualifier.
    """
    ast = _parse_ast(sql)
    _validate_read_only(ast)
    scopes = _build_scope_list(ast)
    return ParsedQuery(ast=ast, scopes=scopes)


# --- Helpers ----------------------------------------------------------------


def _parse_ast(sql: str) -> exp.Expr:
    """Parse a SQL string into a sqlglot AST.

    Uses the DuckDB dialect because DuckDB is joinery's execution
    engine and its dialect is the most permissive superset for our
    purposes.

    Args:
        sql: The raw SQL string.

    Returns:
        The root sqlglot expression.

    Raises:
        InvalidSQLError: If sqlglot fails to parse the string, or if
            the parse returns None (empty input).
    """
    try:
        ast = sqlglot.parse_one(sql, dialect=DEFAULT_DIALECT)
    except ParseError as e:
        raise InvalidSQLError(f"Failed to parse SQL: {e}") from e
    if ast is None:
        raise InvalidSQLError("Input did not contain a parseable SQL statement.")
    return ast


def _validate_read_only(ast: exp.Expr) -> None:
    """Reject any AST that isn't a read query.

    Joinery materializes data through DuckDB; mutating statements have
    no defined semantics in a federated read engine. We accept Select
    expressions and any expression that wraps one (e.g. With for CTEs,
    Union/Intersect/Except for set operations, Subquery for a top-level
    parenthesized SELECT).

    Args:
        ast: The parsed sqlglot expression.

    Raises:
        UnsupportedStatementError: If the top-level node is a write,
            DDL, or otherwise non-read statement.
    """
    allowed = (exp.Select, exp.With, exp.Union, exp.Intersect, exp.Except, exp.Subquery)
    if not isinstance(ast, allowed):
        raise UnsupportedStatementError(
            f"Joinery only supports read queries (SELECT and set operations). "
            f"Received a {type(ast).__name__} statement."
        )


def _build_scope_list(ast: exp.Expr) -> list[ScopeInfo]:
    """Walk the sqlglot scope tree and produce a flat root-first list.

    Uses ``sqlglot.optimizer.scope.build_scope`` to derive the scope
    tree, then traverses it iteratively to assign stable scope_ids and
    parent_ids. For each scope, classifies every table node it owns
    into TableRefs, skipping CTE references (lexically scoped via
    ancestors) and rejecting unqualified base tables.

    Args:
        ast: The parsed sqlglot expression.

    Returns:
        A list of ScopeInfo objects. The root scope is always at
        index 0.

    Raises:
        InvalidSQLError: If sqlglot cannot construct a scope tree
            from the AST (e.g. for some edge-case statements).
        UnqualifiedTableError: Propagated from table classification.
    """
    root = build_scope(ast)
    if root is None:
        raise InvalidSQLError(
            "sqlglot could not derive a scope from this query. "
            "Joinery expects a SELECT or set-operation query at the top level."
        )

    scope_ids: dict[int, int] = {}  # id(Scope) -> scope_id
    parent_ids: dict[int, int | None] = {id(root): None}
    scopes_in_order: list[Scope] = []

    # Iterative breadth-first walk so the root is always id 0 and
    # parents always come before children in the output list.
    queue: list[Scope] = [root]
    while queue:
        current = queue.pop(0)
        scope_ids[id(current)] = len(scopes_in_order)
        scopes_in_order.append(current)
        # sqlglot exposes child scopes via the .subqueries iterator,
        # which covers CTE bodies, derived tables, and nested SELECTs.
        for child in current.subqueries:
            parent_ids[id(child)] = scope_ids[id(current)]
            queue.append(child)

    result: list[ScopeInfo] = []
    for sg_scope in scopes_in_order:
        sid = scope_ids[id(sg_scope)]
        pid = parent_ids[id(sg_scope)]
        kind = _classify_scope_kind(sg_scope, pid)
        visible_ctes = _collect_visible_ctes(sg_scope)
        cte_names = set(sg_scope.cte_sources.keys())
        table_refs = _classify_tables_in_scope(sg_scope, visible_ctes)
        result.append(
            ScopeInfo(
                scope_id=sid,
                kind=kind,
                parent_id=pid,
                table_refs=table_refs,
                cte_names=cte_names,
                sqlglot_scope=sg_scope,
            )
        )
    return result


def _classify_scope_kind(scope: Scope, parent_id: int | None) -> ScopeKind:
    """Collapse sqlglot's scope taxonomy into joinery's three kinds.

    Args:
        scope: The sqlglot Scope.
        parent_id: The scope_id of the parent, or None for the root.

    Returns:
        ``"root"`` if this is the outermost scope, ``"cte"`` if it is
        the body of a CTE, otherwise ``"subquery"`` (covers derived
        tables, scalar subqueries, lateral joins, etc.).
    """
    if parent_id is None:
        return "root"
    if scope.is_cte:
        return "cte"
    return "subquery"


def _collect_visible_ctes(scope: Scope) -> set[str]:
    """Collect every CTE name visible to this scope by lexical scoping.

    A CTE defined at scope level N is visible to N itself and all
    descendant scopes, but not to siblings or ancestors. We gather
    them by walking from the current scope up through every ancestor
    and unioning their cte_sources.

    Args:
        scope: The scope whose CTE visibility we want.

    Returns:
        The set of CTE names referencable from inside ``scope``.
    """
    visible: set[str] = set()
    current: Scope | None = scope
    while current is not None:
        visible.update(current.cte_sources.keys())
        current = current.parent
    return visible


def _classify_tables_in_scope(
    scope: Scope, visible_cte_names: set[str]
) -> list[TableRef]:
    """Walk every table node in a scope and emit catalog-resolved refs.

    Each ``exp.Table`` falls into exactly one of three buckets:

    1. Catalog-resolved — has a db_alias qualifier. We emit a
       TableRef.
    2. CTE reference — bare name that matches a visible CTE. Skipped.
    3. Unqualified base table — bare name with no matching CTE.
       Raises UnqualifiedTableError.

    Args:
        scope: The sqlglot Scope to classify.
        visible_cte_names: All CTE names in scope (this scope plus
            ancestors), used to distinguish bucket 2 from bucket 3.

    Returns:
        Catalog-resolved TableRefs from this scope, in source order.
        Duplicates are preserved here; deduplication is the planner's
        job since it owns the canonical ``(alias, schema, table)`` key.

    Raises:
        UnqualifiedTableError: If any table in the scope is an
            unqualified base table.
    """
    refs: list[TableRef] = []
    for table in scope.tables:
        # sqlglot's Table.db is the immediate qualifier; for `a.b` it
        # holds "a", and for `a.b.c` it holds "b" with .catalog == "a".
        # We treat the leftmost component as the catalog alias, so we
        # check .catalog first and fall back to .db.
        qualifier = table.catalog or table.db
        if not qualifier:
            if table.name in visible_cte_names:
                continue
            raise UnqualifiedTableError(
                f"Table '{table.name}' is not qualified with a database alias. "
                "Joinery requires every base table to be written as "
                "'db_alias.table' (or 'db_alias.schema.table')."
            )
        refs.append(_table_to_ref(table))
    return refs


def _table_to_ref(table: exp.Table) -> TableRef:
    """Map a catalog-resolved sqlglot Table node to a TableRef.

    Caller must have already verified the Table is qualified; this
    function does no validation. Handles both two-part
    (``db.table``) and three-part (``db.schema.table``) names by
    treating the leftmost component as the catalog alias.

    Args:
        table: A sqlglot Table node known to be qualified.

    Returns:
        A TableRef with alias/schema/table populated and the original
        AST node attached.
    """
    if table.catalog:
        # Three-part name: catalog.db.table -> alias=catalog, schema=db.
        alias = table.catalog
        schema = table.db or None
    else:
        # Two-part name: db.table -> alias=db, no schema.
        alias = table.db
        schema = None
    return TableRef(
        alias=alias,
        schema=schema,
        table=table.name,
        ast_node=table,
    )
