from typing import Any
import json
import os


class ResourceSchemaLoader:
  def __init__(self):
    pass

  def get_schema(self, name: str) -> dict[str, Any]:
    schema_filename = f"google_ads/schemas/{name}.json"

    if not os.path.exists(schema_filename):
      raise IOError(f"Cannot fine file {schema_filename}")

    with open(schema_filename, "r") as f:
      try:
        return json.load(f)
      except ValueError as err:
        raise RuntimeError(f"Invalid JSON file format for file {schema_filename}") from err


# schema_loader = ResourceSchemaLoader()
# print(schema_loader.get_schema("campaign"))