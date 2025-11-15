from collections import defaultdict

import sqlglot.errors
from sqlglot import condition, exp, parse_one, select, transpile


class Query:
    def __init__(self, query_string: str = ""):
        self.query_string: str = query_string
        self.ast: sqlglot.Expression = self._validate_and_parse(query_string)
        self.tables: dict[str, list[str]] = defaultdict(list)
        self.columns: dict[str, list[str]] = defaultdict(list)
        self.per_table_queries: dict[str, list[str]] = defaultdict(list)
        self.pushable_predicates: dict[str, list[sqlglot.Expression]] = defaultdict(
            list
        )

    def _validate_and_parse(self, query: str) -> sqlglot.Expression:
        try:
            ast = parse_one(query)
        except sqlglot.errors.ParseError as e:
            raise ValueError(f"Invalid SQL query: {e}") from e

        return ast

    def _extract_tables(self):
        for table in self.ast.find_all(exp.Table):
            if not table.alias:
                raise ValueError(f"No alias defined for table {table.name}")

            self.tables[table.alias].append(table.name)

    def _extract_columns(self):
        for column in self.ast.find_all(exp.Column):
            self.columns[column.table].append(column.name)

    def _flatten_conjuncts(
        self, expr: sqlglot.Expression | None
    ) -> list[sqlglot.Expression]:
        if not expr:
            return []
        if isinstance(expr, exp.And):
            right: list[sqlglot.Expression] = self._flatten_conjuncts(
                expr.args.get("this")
            )
            left: list[sqlglot.Expression] = self._flatten_conjuncts(
                expr.args.get("expression")
            )

            return right + left

        return [expr]

    def _expr_tables(self, expr: sqlglot.Expression) -> set[str]:
        tables: set[str] = set()
        for c in expr.find_all(exp.Column):
            t = getattr(c, "table", None)
            if isinstance(t, exp.Identifier):
                t = t.name
            if t:
                tables.add(str(t))

        return tables

    def _pushdown_preds(self):
        where_node: exp.Where | None = self.ast.find(exp.Where)
        if where_node:
            condition = getattr(where_node, "this", None)
            for pred in self._flatten_conjuncts(condition):
                tbls = self._expr_tables(pred)
                if len(tbls) == 1:
                    t = next(iter(tbls))
                    self.pushable_predicates[t].append(pred)

    def build_query(self):
        self._extract_tables()
        self._extract_columns()
        self._pushdown_preds()

        for table in self.tables:
            if not self.columns.get(table):
                raise ValueError(f"Table {table} does not exist in columns!")
            else:
                cols: str = ", ".join(self.columns.get(table))
                preds: str | None = None
                if preds_list := self.pushable_predicates.get(table):
                    preds = " AND ".join([pred.sql() for pred in preds_list])
                    print(
                        f"SELECT {cols} FROM {self.tables[table][0]} WHERE 1=1 AND {preds}"
                    )
                else:
                    print(f"SELECT {cols} FROM {self.tables[table][0]}")


if __name__ == "__main__":
    query_str = """
        SELECT
            a.id, b.department
        FROM
            table1 a
        JOIN
            table2 b ON a.id = b.id
        WHERE
            a.building > 'Engineering' AND
            b.building = 'Newton' AND
            a.year = b.year AND
            a.status = 'ACTIVE';
    """
    # print(repr(parse_one(query_str)))
    query = Query(query_str)
    query.build_query()
