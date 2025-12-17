import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "src" / "warehouse" / "analytics.duckdb"


def main():
    con = duckdb.connect(str(DB_PATH))

    # dim_user
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_user AS
        SELECT DISTINCT user_id
        FROM fact_search_behavior
    """
    )

    # dim_keyword
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_keyword AS
        SELECT DISTINCT keyword
        FROM (
            SELECT most_search_June AS keyword FROM fact_search_behavior
            UNION
            SELECT most_search_July AS keyword FROM fact_search_behavior
        )
        WHERE keyword IS NOT NULL
    """
    )

    # dim_category
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_category AS
        SELECT DISTINCT category
        FROM (
            SELECT category_june AS category FROM fact_search_behavior
            UNION
            SELECT category_july AS category FROM fact_search_behavior
        )
        WHERE category IS NOT NULL
    """
    )

    # dim_trend_type
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_trend_type AS
        SELECT DISTINCT Trending_Type
        FROM fact_search_behavior
    """
    )

    # dim_period
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_period AS
        SELECT * FROM (
            VALUES
                ('June',  2022, 6),
                ('July',  2022, 7)
        ) AS t(period_name, year, month)
    """
    )

    print("Search dimensions created:")
    print(con.execute("SHOW TABLES").fetchall())

    con.close()


if __name__ == "__main__":
    main()
