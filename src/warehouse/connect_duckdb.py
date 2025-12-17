import duckdb
import os

DB_PATH = "warehouse/analytics.duckdb"

os.makedirs("warehouse", exist_ok=True)

con = duckdb.connect(DB_PATH)

print("Connected to DuckDB at:", DB_PATH)
print(con.execute("SELECT 1").fetchall())

con.close()
