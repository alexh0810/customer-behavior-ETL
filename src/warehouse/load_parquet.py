# src/warehouse/load_parquet.py
import duckdb
from pathlib import Path

# ============================================================
# Paths (robust)
# ============================================================
BASE_DIR = Path(__file__).resolve().parents[2]

WAREHOUSE_DIR = BASE_DIR / "src" / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = WAREHOUSE_DIR / "analytics.duckdb"

CONTENT_PARQUET = BASE_DIR / "output" / "content_interactions" / "*.parquet"
SEARCH_PARQUET = BASE_DIR / "output" / "search_interactions" / "*.parquet"


# ============================================================
# Load
# ============================================================
def main():
    con = duckdb.connect(str(DB_PATH))

    # ------------------------
    # Content interactions
    # ------------------------
    con.execute(
        f"""
        CREATE OR REPLACE TABLE fact_content_interactions AS
        SELECT *
        FROM read_parquet('{CONTENT_PARQUET}')
        """
    )

    # ------------------------
    # Search behavior
    # ------------------------
    con.execute(
        f"""
        CREATE OR REPLACE TABLE fact_search_behavior AS
        SELECT *
        FROM read_parquet('{SEARCH_PARQUET}')
        """
    )

    # ------------------------
    # Sanity checks
    # ------------------------
    print("\nTables loaded:")
    print(con.execute("SHOW TABLES").fetchall())

    print("\nRow counts:")
    print(
        con.execute(
            """
            SELECT 'fact_content_interactions' AS table, COUNT(*) FROM fact_content_interactions
            UNION ALL
            SELECT 'fact_search_behavior', COUNT(*) FROM fact_search_behavior
            """
        ).fetchall()
    )

    con.close()


if __name__ == "__main__":
    main()
