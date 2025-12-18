import duckdb
import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "src" / "warehouse" / "analytics.duckdb"
OUTPUT_DIR = BASE_DIR / "docs"
OUTPUT_DIR.mkdir(exist_ok=True)

con = duckdb.connect(str(DB_PATH))

df = con.execute(
    """
    SELECT engagement_level, COUNT(*) AS contracts
    FROM mart_contract_engagement
    GROUP BY engagement_level
    ORDER BY engagement_level
"""
).fetchdf()

con.close()

plt.figure()
plt.bar(df["engagement_level"], df["contracts"])
plt.title("Contract Engagement Distribution")
plt.ylabel("Number of Contracts")
plt.xlabel("Engagement Level")
plt.tight_layout()

plt.savefig(OUTPUT_DIR / "contract_engagement.png")
plt.show()
