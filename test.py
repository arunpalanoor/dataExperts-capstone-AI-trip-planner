"""
Quick manual check that lakebase.py can reach Lakebase and read the schema.
Run from a Databricks notebook (%sh python test.py) or locally with a
Databricks CLI profile configured.
"""

import lakebase

TABLE = "trips"

rows = lakebase.run_query(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_name = %s ORDER BY ordinal_position",
    (TABLE,),
)

print(f"Columns in '{TABLE}':")
for row in rows:
    print(f"  {row['column_name']}: {row['data_type']}")
