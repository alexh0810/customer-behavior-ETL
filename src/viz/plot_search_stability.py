import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ----------------------------------
# Paths
# ----------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "src" / "warehouse" / "analytics.duckdb"

# ----------------------------------
# Load data
# ----------------------------------
con = duckdb.connect(str(DB_PATH))

df = con.execute(
    """
    SELECT trending_type, users, percentage
    FROM mart_search_stability
"""
).fetchdf()

con.close()

# ----------------------------------
# Plot
# ----------------------------------
plt.figure()
plt.bar(df["trending_type"], df["percentage"])
plt.title("Search Interest Stability vs Change")
plt.ylabel("Percentage of Users")
plt.xlabel("Trend Type")
plt.tight_layout()

# ----------------------------------
# Save image
# ----------------------------------
output_path = BASE_DIR / "docs" / "search_stability.png"
output_path.parent.mkdir(exist_ok=True)
plt.savefig(output_path)
plt.show()
