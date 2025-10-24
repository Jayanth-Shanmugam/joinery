import sqlglot.errors
from sqlglot import parse_one, exp, transpile


def _split_query(query: str) -> list[str]:
    """
    Split a federated query across different databases into individual queries
    for each database

    Parameters
      query -> A query string that references multiple databases
    Returns
      List of individual queries for each database
    """

    try:
        _ = transpile(query)
    except sqlglot.errors.ParseError as e:
        print(e.errors)

    # Create a list of tables being referenced in the query
    tables: list[str] = []
    for table in parse_one(query).find_all(exp.Table):
        tables.append(table.alias)

    # Create a list of columns being referenced in the query
    columns: list[str] = []
    for column in parse_one(query).find_all(exp.Column):
        columns.append(column.alias_or_name)

    return tables


if __name__ == "__main__":
    query = "SELECT * FROM department a JOIN employees b ON a.id = b.dept_id"
    print(_split_query(query))
