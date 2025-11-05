import sqlglot.errors
from collections import defaultdict
from sqlglot import parse_one, exp, transpile, select, condition


class query:
    def __init__(self, query_string: str = ""):
        self.query_string: str = query_string
        self.tables: dict[str, list[str]] = defaultdict(list)
        self.columns: dict[str, list[str]] = defaultdict(list)
        self.queries: dict[str, list[str]] = defaultdict(list)

    def extract_tables_and_columns(self):
        """
        Extract all tables and columns being referenced in the query

        Parameters:
            query -> A SQL query string
        Returns:
            None
        """

        try:
            _ = transpile(self.query_string)
        except sqlglot.errors.ParseError as e:
            print(e.errors)

        parsed_query = parse_one(self.query_string)

        # Create a list of tables being referenced in the query
        for table in parsed_query.find_all(exp.Table):
            self.tables[table.alias].append(table.name)

        # Create a list of columns being referenced in the query
        for column in parsed_query.find_all(exp.Column):
            self.columns[column.table].append(column.name)

    def build_query(self):
        """
        Build a SQL query from the extracted tables and columns

        Parameters:
            None
        Returns:
            None
        """

        select_stmnt = None
        for table, columns in self.columns.items():
            for column in list(set(columns)):
                if not select_stmnt:
                    select_stmnt = select(columns[0])
                else:
                    select_stmnt = select_stmnt.select(column)
            select_stmnt = select_stmnt.from_(table)
            print(select_stmnt)
            select_stmnt = None


if __name__ == "__main__":
    query_str = "SELECT a.id, b.department FROM table1 a JOIN table2 b ON a.id = b.id WHERE a.building = 'Engineering';"
    test_query = query(query_str)
    test_query.extract_tables_and_columns()
    print(test_query.columns)
    test_query.build_query()
