# import sqlglot.errors
# from collections import defaultdict
# from sqlglot import parse_one, exp, transpile


# class query:
#     def __init__(self, query_string: str = ""):
#         self.query_string: str = query_string
#         self.tables: dict[str, str] = defaultdict(str)
#         self.columns: dict[str, str] = defaultdict(str)

#     def _split_query(self):
#         """
#         Split a query across different databases into individual queries
#         for each database

#         Parameters
#         query -> A query string that references multiple databases
#         Returns
#         List of individual queries for each database
#         """

#         try:
#             _ = transpile(self.query_string)
#         except sqlglot.errors.ParseError as e:
#             print(e.errors)

#         # Create a list of tables being referenced in the query
#         for table in parse_one(self.query_string).find_all(exp.Table):
#             self.tables[table.alias] = table.name

#         # Create a list of columns being referenced in the query
#         for column in parse_one(self.query_string).find_all(exp.Column):
#             self.columns[column.alias] = column.name


# if __name__ == "__main__":
#     from sqlglot import parse

#     sql = """SELECT e.eid, e.name FROM employees e WHERE e.eid > 100 AND e.name = "John";"""
#     statements = parse(sql)
#     for i, statement in enumerate(statements):
#         print(f"Statement {i + 1}")
#         print(statement.sql())
#         print("-" * 20)

# Requires: pip install sqlglot
from typing import Dict, List, Optional
import sqlglot
from sqlglot import expressions as exp


def _col_text(col_expr: exp.Expression) -> str:
    """
    Return the textual form of a Column expression, e.g. "a.id" or "id".
    Uses the node's SQL rendering which is robust across dialects.
    """
    return col_expr.sql(dialect="")  # empty dialect -> generic SQL text


def _columns_in_expr(expr: exp.Expression) -> List[str]:
    """Return list of column textual forms found inside expr (e.g. 'a.id', 'id')."""
    return [_col_text(c) for c in expr.find_all(exp.Column)]


def _qualifier_of_col_text(col_text: str) -> Optional[str]:
    """Return qualifier/table alias if present in 'a.id' else None"""
    if "." in col_text:
        return col_text.rsplit(".", 1)[0]
    return None


def extract_table_queries(sql: str) -> Dict[str, dict]:
    """
    Parse SQL and return a dictionary keyed by table alias (or table name if no alias).
    Each value contains:
      - original_table: fully qualified table name as seen in SQL
      - alias: alias used in query (or same as original_table)
      - projections: set of column texts referenced for that table (may include unqualified names)
      - pushdown_predicates: list of predicate SQL texts that reference only that table
      - per_table_sql: a generated SQL string that selects only needed columns and includes pushable predicates

    Note: This intentionally keeps text forms of columns/predicates. For full correctness
    (e.g., preserving quoting), you may re-construct expressions with sqlglot Expression builders.
    """
    tree = sqlglot.parse_one(sql)
    # 1) gather tables (Table nodes)
    tables = {}  # key -> {"original_table": "schema.table" or "table", "alias": alias}
    for t in tree.find_all(exp.Table):
        # textual form for the table (may include database/catalog)
        table_text = t.sql(dialect="")

        # alias: prefer explicit alias, else the table name
        alias = None
        if isinstance(t.parent, exp.Alias):
            # If Table node is directly wrapped in Alias, parent gives alias
            alias = t.parent.alias_or_name
        else:
            # try t.args.get("alias")
            try:
                alias = t.alias_or_name
            except Exception:
                alias = None

        if not alias:
            # fallback to bare table name (last part)
            alias = table_text.split(".")[-1]

        tables[alias] = {
            "original_table": table_text,
            "alias": alias,
            "projections": set(),
            "pushdown_predicates": [],
        }

    # If there were no explicit Table nodes (rare), bail out
    if not tables:
        raise ValueError("No tables found in query")

    # 2) collect all Column nodes and attribute them to tables (by qualifier if available)
    unqualified_columns = set()
    for col in tree.find_all(exp.Column):
        text = _col_text(col)  # e.g. "a.id" or "id"
        qual = _qualifier_of_col_text(text)
        if qual and qual in tables:
            tables[qual]["projections"].add(
                text.split(".", 1)[1]
            )  # store bare column name
        else:
            # unqualified column (or qualifier not matching known aliases)
            unqualified_columns.add(text)
            # attempt to assign unqualified column to a table if there's only one table
            if len(tables) == 1:
                only_alias = next(iter(tables))
                tables[only_alias]["projections"].add(text)

    # 3) gather predicates from WHERE, HAVING, and JOIN ON clauses
    predicate_sources = []
    where = tree.args.get("where")
    if where:
        # where.this is the expression representing the predicate
        predicate_sources.append(("WHERE", where.this))
    having = tree.args.get("having")
    if having:
        predicate_sources.append(("HAVING", having.this))
    # ON clauses: find all joins and their on expressions
    for join in tree.find_all(exp.Join):
        on_expr = join.args.get("on")
        if on_expr:
            predicate_sources.append(("JOIN_ON", on_expr))

    # Helper: determine if an expression references columns from exactly one alias
    def expr_single_table(expr: exp.Expression) -> Optional[str]:
        col_texts = _columns_in_expr(expr)
        quals = set(
            _qualifier_of_col_text(c)
            for c in col_texts
            if _qualifier_of_col_text(c) is not None
        )
        # If there are no qualified columns, but columns exist, and there's only one table in query, assign
        if not quals:
            if col_texts and len(tables) == 1:
                return next(iter(tables))
            return None
        # If all qualifiers map to a single known alias, return it
        if len(quals) == 1:
            qual = next(iter(quals))
            return qual if qual in tables else None
        return None

    # iterate predicates and push those that belong exclusively to a single table
    for src, expr in predicate_sources:
        # split conjunctions (AND) into top-level conjuncts for more pushdown
        # sqlglot represents AND as exp.And with left/right; use .flatten? we'll recursively collect
        def flatten_conj(e: exp.Expression):
            if isinstance(e, exp.And):
                yield from flatten_conj(e.args.get("this"))
                yield from flatten_conj(
                    e.args.get("expression")
                    or e.args.get("expression")
                    or e.args.get("right")
                )
            else:
                yield e

        for conjunct in flatten_conj(expr):
            alias = expr_single_table(conjunct)
            if alias:
                # safe to push to this alias
                tables[alias]["pushdown_predicates"].append(conjunct.sql(dialect=""))
            else:
                # cannot push (multi-table or ambiguous) — skip
                pass

    # 4) build per-table SQL strings
    per_table_results = {}
    for alias, meta in tables.items():
        proj = meta["projections"]
        # If no explicit projections discovered, don't expand '*' — use '*' to be safe
        if not proj:
            select_list = "*"
        else:
            # prefix columns with alias to be explicit (helps avoid ambiguity)
            select_list = ", ".join(f"{alias}.{c}" for c in sorted(proj))
        from_clause = meta["original_table"]
        # If alias differs from table name, include AS alias
        if alias != from_clause.split(".")[-1]:
            # preserve original alias usage if it was explicit
            from_clause = f"{from_clause} AS {alias}"

        where_clause = ""
        if meta["pushdown_predicates"]:
            where_clause = " WHERE " + " AND ".join(meta["pushdown_predicates"])

        per_sql = f"SELECT {select_list} FROM {from_clause}{where_clause}"
        per_table_results[alias] = {
            "original_table": meta["original_table"],
            "alias": alias,
            "projections": sorted(meta["projections"]),
            "pushdown_predicates": list(meta["pushdown_predicates"]),
            "per_table_sql": per_sql,
        }

    return per_table_results


if __name__ == "__main__":
    q = """
    SELECT a.id, a.name, b.dept_name
    FROM employee a
    JOIN department b ON a.dept_id = b.id AND b.active = true
    WHERE a.status = 'active' AND a.created_at > '2024-01-01' AND b.dept_name = 'Sales'
    """
    result = extract_table_queries(q)
    print(result)
    # import json
    # print(json.dumps(result, indent=2))
