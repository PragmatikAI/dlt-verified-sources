from typing import Iterable, List, Mapping, Any
from datetime import datetime, timedelta

def convert_schema_into_query(
  fields: Iterable[str],
  table_name: str,
  conditions: List[str] = None,
  order_field: str = None,
  limit: int = None,
) -> str:
  """
  Constructs a Google Ads query based on the provided parameters.

  Args:
  - fields (Iterable[str]): List of fields to be selected in the query.
  - table_name (str): Name of the table from which data will be selected.
  - conditions (List[str], optional): List of conditions to be applied in the WHERE clause. Defaults to None.
  - order_field (str, optional): Field by which the results should be ordered. Defaults to None.
  - limit (int, optional): Maximum number of results to be returned. Defaults to None.

  Returns:
  - str: Constructed Google Ads query.
  """

  query_template = f"SELECT {', '.join(fields)} FROM {table_name}"

  if conditions:
    query_template += " WHERE " + " AND ".join(conditions)

  if order_field:
    query_template += f" ORDER BY {order_field} ASC"

  if limit:
    query_template += f" LIMIT {limit}"

  return query_template


def get_fields_from_schema(schema: Mapping[str, Any]) -> List[str]:
  properties = schema.get("properties")
  return list(properties.keys())


def get_date_range(start_date: str, conversion_window: int, first_run: bool) -> List[str]: 
  today = datetime.now().strftime("%Y-%m-%d")
  if first_run:
    return [f"segments.date BETWEEN '{start_date}' AND '{today}'"]
  else:
    start_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=conversion_window)).strftime("%Y-%m-%d")
    return [f"segments.date BETWEEN '{start_date}' AND '{today}'"]

def get_date_range_slices(start_date: str, first_run: bool, n_days_ago: int = 90) -> List[str]:
    today = datetime.now()
    
    if first_run:
        # Get the later of start_date or 90 days ago
        date_days_ago = today - timedelta(days=n_days_ago)
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        effective_start = max(start_dt, date_days_ago)
        
        # Generate date range for each day from effective start to today
        date_ranges = []
        current = effective_start
        while current <= today:
            date_str = current.strftime("%Y-%m-%d")
            date_ranges.append(f"segments.date = '{date_str}'")
            current += timedelta(days=1)
            
        return date_ranges
        
    else:
        # For subsequent runs, just get last 2 days
        yesterday = today - timedelta(days=1)
        two_days_ago = today - timedelta(days=2)
        
        return [
            f"segments.date = '{yesterday.strftime('%Y-%m-%d')}'",
            f"segments.date = '{two_days_ago.strftime('%Y-%m-%d')}'"
        ]

