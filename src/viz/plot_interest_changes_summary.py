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
    SELECT
        category_june || ' → ' || category_july AS transition,
        users
    FROM mart_interest_change_summary
    WHERE trending_type = 'Changed'
    ORDER BY users DESC
    LIMIT 10
    """
).fetchdf()

con.close()

# ----------------------------------
# Plot
# ----------------------------------
plt.figure(figsize=(8, 4))
plt.barh(df["transition"], df["users"])
plt.gca().invert_yaxis()
plt.title("Top Search Interest Transitions")
plt.xlabel("Number of Users")
plt.tight_layout()

# ----------------------------------
# Save image (NON-BLOCKING)
# ----------------------------------
output_path = BASE_DIR / "docs" / "interest_transitions.png"
output_path.parent.mkdir(exist_ok=True)
plt.savefig(output_path)
plt.show()
