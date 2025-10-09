from typing import List
from sqlgot import parse_one, exp

def _split_query(query: str) -> List[str]:
  '''
  Split a federated query across different databases into individual queries
  for each database

  Parameters
    query -> A query string that references multiple databases
  Returns
    List of individual queries for each database
  '''

  if not string:
    raise ValueError("Empty SQL query!")

  
