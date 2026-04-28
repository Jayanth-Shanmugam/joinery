"""Query planning for joinery.

The planner is the brain of the engine. It takes a ParsedQuery from
the parser and produces a list of ScanPlans — one per (scope, table)
pair — where each plan describes exactly what SQL to send to one
source database. Specifically, each ScanPlan carries:

  * The catalog entry for the source (so the executor knows where).
  * The columns that should be projected (projection pushdown).
  * The predicates that can safely be pushed down (predicate pushdown).
  * The pre-rendered source SQL string, dialect-correct for that source.

The executor then becomes mechanical: open a connector per alias,
execute each plan's ``generated_sql``, register the resulting Arrow
tables in DuckDB, rewrite the original AST to flat names, and run it.

v1 pushdown scope (deliberately conservative):
  * Projection pushdown: columns referenced anywhere in the scope.
  * Predicate pushdown: WHERE conjuncts whose every column reference
    resolves to a single source. Multi-source predicates, correlated
    subqueries, and predicates that traverse CTE boundaries are NOT
    pushed; the executor runs them in DuckDB after materialization.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator

import sqlglot.expressions as exp

from joinery.parser import ParsedQuery, ScopeInfo, TableRef

# Maps a catalog 'type' value to the sqlglot dialect used to render
# source SQL. Mirrors the map in connectors/sqlalchemy.py — duplicated
# here on purpose so the planner has no dependency on the connectors.
_DIALECT_MAP: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
    "duckdb": "duckdb",
    "mssql": "tsql",
    "oracle": "oracle",
}


# --- Errors -----------------------------------------------------------------


class UnknownAliasError(ValueError):
    """The catalog has no entry for an alias referenced by the query."""


class PlanningError(RuntimeError):
    """Generic planner failure (column resolution, SQL generation, etc.)."""


# --- Result type ------------------------------------------------------------


@dataclass
class ScanPlan:
    """Everything the executor needs to materialize one table from one scope.

    A ScanPlan is the planner's atomic unit of work. Each TableRef in
    each ScopeInfo produces exactly one ScanPlan; if the same table is
    referenced from two scopes, two ScanPlans are produced (because
    each scope may push different columns and predicates).

    Attributes:
        scope_id: The scope_id from ParsedQuery.scopes that owns this
            scan. Carried so the executor / debugger can correlate
            plans back to the AST.
        table_ref: The catalog-resolved TableRef the plan reads from.
            Holds the original sqlglot Table node so the executor's
            AST rewriter can locate it.
        catalog_entry: The catalog config dict for table_ref.alias.
            Carried inside the plan so the executor doesn't have to
            re-look-it-up.
        columns: The list of column names to fetch from the source,
            or None to fetch every column (``SELECT *``). An empty
            list is never produced — that would be meaningless SQL.
        predicates: WHERE-clause expressions safe to push to the
            source. Already deep-copied and normalized (table aliases
            stripped). May be empty.
        generated_sql: The fully-rendered SQL string in the source's
            own dialect. The executor passes this to
            ``connector.fetch_query(...)`` verbatim.
    """

    scope_id: int
    table_ref: TableRef
    catalog_entry: dict[str, Any]
    columns: list[str] | None
    predicates: list[exp.Expression]
    generated_sql: str


# --- Planner ----------------------------------------------------------------


class QueryPlanner:
    """Pipeline that turns a ParsedQuery into a list of ScanPlans.

    Stateless beyond the catalog passed at construction. Calling
    :meth:`plan` runs the full pipeline and returns a fresh list. Each
    private method handles one concern (column collection, predicate
    extraction, SQL rendering) so the pipeline is easy to read and
    test in isolation.
    """

    def __init__(self, catalog: dict[str, dict[str, Any]]) -> None:
        """Store the catalog used to resolve aliases to source configs.

        Args:
            catalog: The dict returned by ``joinery.catalog.load_catalog``.
                Keys are alias strings (e.g. ``"postgres_prod"``);
                values are the per-database config dicts the connector
                factory consumes.
        """
        self.catalog = catalog

    def plan(self, parsed: ParsedQuery) -> list[ScanPlan]:
        """Produce a ScanPlan for every TableRef in every scope.

        Two passes:
            1. Validate that every referenced alias is in the catalog.
               All missing aliases are reported in one error so the
               user fixes them in one round-trip rather than N.
            2. For each (scope, table_ref) pair, build a ScanPlan.

        Args:
            parsed: The output of ``QueryParser.parse``.

        Returns:
            One ScanPlan per (scope_id, TableRef) pair, in scope-order
            (root first) and source-order within each scope.

        Raises:
            UnknownAliasError: One or more aliases are missing from
                the catalog.
            PlanningError: Internal planner failure (e.g. SQL
                rendering produced an invalid string).
        """
        self._validate_aliases(parsed)

        plans: list[ScanPlan] = []
        for scope in parsed.scopes:
            for ref in scope.table_refs:
                plans.append(self._plan_scan(scope, ref))
        return plans

    def _validate_aliases(self, parsed: ParsedQuery) -> None:
        """Assert every referenced alias is in the catalog.

        Collects every missing alias before raising so the error
        message lists all of them at once.

        Args:
            parsed: The full ParsedQuery.

        Raises:
            UnknownAliasError: If any TableRef references an alias
                not present in ``self.catalog``.
        """
        missing: set[str] = set()
        for scope in parsed.scopes:
            for ref in scope.table_refs:
                if ref.alias not in self.catalog:
                    missing.add(ref.alias)
        if missing:
            known = ", ".join(sorted(self.catalog.keys())) or "(none)"
            raise UnknownAliasError(
                f"Query references {len(missing)} alias(es) not in the catalog: "
                f"{sorted(missing)}. Known aliases: {known}."
            )

    def _plan_scan(self, scope: ScopeInfo, table_ref: TableRef) -> ScanPlan:
        """Build a single ScanPlan for one (scope, table_ref) pair.

        Pipeline:
            1. Determine the SQL-level source name (alias or table name).
            2. Collect column references for projection pushdown.
            3. Extract pushable predicates from the scope's WHERE clause.
            4. Normalize predicates (strip the source-name qualifier).
            5. Render the source SQL in the source's dialect.

        Args:
            scope: The owning scope.
            table_ref: The catalog-resolved table reference.

        Returns:
            A complete ScanPlan ready for the executor to consume.

        Raises:
            PlanningError: If SQL rendering fails for any reason.
        """
        catalog_entry = self.catalog[table_ref.alias]
        source_name = self._source_name_for_ref(scope, table_ref)
        columns = self._collect_columns_for_table(scope, source_name)
        raw_predicates = self._extract_pushable_predicates(scope, source_name)
        predicates = [
            self._normalize_predicate(p, source_name) for p in raw_predicates
        ]
        sql = self._generate_source_sql(
            table_ref=table_ref,
            columns=columns,
            predicates=predicates,
            db_type=catalog_entry.get("type", ""),
        )
        return ScanPlan(
            scope_id=scope.scope_id,
            table_ref=table_ref,
            catalog_entry=catalog_entry,
            columns=columns,
            predicates=predicates,
            generated_sql=sql,
        )

    @staticmethod
    def _source_name_for_ref(scope: ScopeInfo, table_ref: TableRef) -> str:
        """Determine the name used to refer to this table within the scope's SQL.

        SQL allows two forms:
          * Aliased: ``FROM pg.users u`` — column refs use ``u.col``.
          * Unaliased: ``FROM pg.users``  — column refs use ``users.col``.

        sqlglot's ``Scope.sources`` dict maps the source name (alias
        if present, else bare table name) to the underlying Table
        node. We find the entry whose Table is ``table_ref.ast_node``.

        Args:
            scope: The owning scope.
            table_ref: The TableRef whose source name we want.

        Returns:
            The bare identifier the scope's expressions use to qualify
            columns from this table. Falls back to ``table_ref.table``
            if the TableRef can't be located in scope.sources (which
            should not happen in well-formed queries but is treated
            as a soft default).
        """
        for name, source in scope.sqlglot_scope.sources.items():
            if isinstance(source, exp.Table) and source is table_ref.ast_node:
                return name
        return table_ref.table

    @staticmethod
    def _collect_columns_for_table(
        scope: ScopeInfo, source_name: str
    ) -> list[str] | None:
        """Find every column from ``source_name`` referenced by this scope.

        Walks every ``exp.Column`` whose table qualifier matches
        ``source_name``. Returns ``None`` (meaning ``SELECT *``) when
        no qualified references are found — this is conservative but
        always correct, and avoids generating empty column lists.

        We only count columns that are *explicitly* qualified by
        ``source_name``. Bare columns (``WHERE country = 'US'``) and
        columns qualified by other sources are deliberately excluded.
        For v1 this means scopes that mostly use bare references will
        fall back to ``SELECT *``, which is correct but not optimal.

        Args:
            scope: The owning scope.
            source_name: The SQL-level name for our table within
                this scope (from ``_source_name_for_ref``).

        Returns:
            A sorted list of column names, or None when projection
            pushdown isn't safe.
        """
        columns: set[str] = set()
        for col in scope.sqlglot_scope.expression.find_all(exp.Column):
            if col.table == source_name and col.name:
                columns.add(col.name)
        return sorted(columns) if columns else None

    @classmethod
    def _extract_pushable_predicates(
        cls, scope: ScopeInfo, source_name: str
    ) -> list[exp.Expression]:
        """Return WHERE-clause conjuncts safe to push down to one source.

        A predicate is pushable iff every column reference inside it
        is explicitly qualified by ``source_name``. This deliberately
        excludes:
            * Multi-source predicates (joins, cross-source filters).
            * Bare-column predicates (we can't prove ownership).
            * Predicates with no column references (constants — they
              would be pushed to every source which is wasteful).

        Args:
            scope: The owning scope.
            source_name: The SQL-level name for our table.

        Returns:
            Deep-copied predicate expressions. The deep copy means
            the executor's later AST rewrite cannot mutate them out
            from under the planner.
        """
        where = scope.sqlglot_scope.expression.args.get("where")
        if where is None:
            return []
        pushable: list[exp.Expression] = []
        for predicate in cls._split_conjunction(where.this):
            if cls._is_predicate_pushable(predicate, source_name):
                pushable.append(predicate.copy())
        return pushable

    @staticmethod
    def _split_conjunction(expr: exp.Expression) -> Iterator[exp.Expression]:
        """Yield every conjunct of an AND-tree, left to right.

        ``A AND B AND C`` parses as ``And(And(A, B), C)``. We flatten
        it so the planner can decide pushability for each conjunct
        independently.

        Args:
            expr: A possibly-nested ``exp.And`` tree, or any other
                expression (yielded unchanged).

        Yields:
            Leaf expressions in original left-to-right order.
        """
        if isinstance(expr, exp.And):
            yield from QueryPlanner._split_conjunction(expr.this)
            yield from QueryPlanner._split_conjunction(expr.expression)
        else:
            yield expr

    @staticmethod
    def _is_predicate_pushable(
        predicate: exp.Expression, source_name: str
    ) -> bool:
        """Return True iff every column in ``predicate`` is from ``source_name``.

        A predicate must have at least one column reference and every
        column reference must explicitly name ``source_name``.
        Constants-only predicates are skipped — pushing them down
        would just send identical no-op WHERE clauses to every source.

        Args:
            predicate: A single conjunct from a WHERE clause.
            source_name: The SQL-level name for our table.

        Returns:
            True if the predicate can be safely sent to the source.
        """
        columns = list(predicate.find_all(exp.Column))
        if not columns:
            return False
        return all(col.table == source_name for col in columns)

    @staticmethod
    def _normalize_predicate(
        predicate: exp.Expression, source_name: str
    ) -> exp.Expression:
        """Strip the source-name qualifier from every column in the predicate.

        Predicates were written against the user's SQL (``u.country``)
        but will be sent to the source database, where the table is
        not aliased (``SELECT ... FROM "users" WHERE country = ...``).
        We rewrite ``u.country`` -> ``country`` so the predicate is
        valid in the source query.

        Args:
            predicate: A pushable predicate (already deep-copied).
            source_name: The qualifier to strip.

        Returns:
            A new expression with column references unqualified.
        """
        def _strip(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column) and node.table == source_name:
                stripped = node.copy()
                stripped.set("table", None)
                return stripped
            return node

        return predicate.transform(_strip)

    @classmethod
    def _generate_source_sql(
        cls,
        table_ref: TableRef,
        columns: list[str] | None,
        predicates: list[exp.Expression],
        db_type: str,
    ) -> str:
        """Render the per-source SQL string for a single ScanPlan.

        Builds ``SELECT <cols> FROM <schema?>.<table> [WHERE <preds>]``
        as a sqlglot expression, then renders it in the source's
        dialect so that identifier quoting and minor syntactic
        differences (e.g. backticks vs. double-quotes) come out right.

        Args:
            table_ref: The catalog-resolved table.
            columns: Column list, or None for ``SELECT *``.
            predicates: Pushable predicates (already normalized).
            db_type: The catalog 'type' string, used to look up the
                sqlglot dialect.

        Returns:
            A complete SQL statement string ready to send to the
            source database.

        Raises:
            PlanningError: If sqlglot cannot render the expression.
        """
        if columns:
            select_exprs: list[exp.Expression] = [
                exp.Column(this=exp.Identifier(this=col, quoted=True))
                for col in columns
            ]
        else:
            select_exprs = [exp.Star()]

        table = exp.Table(
            this=exp.Identifier(this=table_ref.table, quoted=True),
            db=(
                exp.Identifier(this=table_ref.schema, quoted=True)
                if table_ref.schema
                else None
            ),
        )
        select = exp.Select(expressions=select_exprs).from_(table)

        if predicates:
            combined = predicates[0]
            for pred in predicates[1:]:
                combined = exp.And(this=combined, expression=pred)
            select = select.where(combined)

        dialect = _DIALECT_MAP.get(db_type) or None
        try:
            return select.sql(dialect=dialect)
        except Exception as e:  # pragma: no cover - sqlglot rarely fails here
            raise PlanningError(
                f"Failed to render source SQL for "
                f"'{table_ref.alias}.{table_ref.table}': {e}"
            ) from e


# --- Module-level shorthand -------------------------------------------------


def plan_query(
    parsed: ParsedQuery, catalog: dict[str, dict[str, Any]]
) -> list[ScanPlan]:
    """Convenience wrapper around ``QueryPlanner(catalog).plan(parsed)``.

    Args:
        parsed: The output of ``QueryParser.parse``.
        catalog: The dict returned by ``load_catalog``.

    Returns:
        One ScanPlan per (scope_id, TableRef) pair.
    """
    return QueryPlanner(catalog).plan(parsed)
