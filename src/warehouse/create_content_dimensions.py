import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "src" / "warehouse" / "analytics.duckdb"


def main():
    con = duckdb.connect(str(DB_PATH))

    # dim_contract
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_contract AS
        SELECT DISTINCT Contract 
        FROM fact_content_interactions
    """
    )

    # dim_content_category
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_content_category AS
        SELECT * FROM (
            VALUES
                ('Giải Trí'),
                ('Phim Truyện'),
                ('Thiếu Nhi'),
                ('Thể Thao'),
                ('Truyền Hình')
        ) AS t(category)
    """
    )

    # dim_most_watched
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_most_watched AS
        SELECT DISTINCT Most_Watched AS category
        FROM fact_content_interactions
        WHERE Most_Watched IS NOT NULL
    """
    )

    # dim_device_profile
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_device_profile AS
        SELECT DISTINCT TotalDevices
        FROM fact_content_interactions
    """
    )

    # dim_activity_level
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_activity_level AS
        SELECT DISTINCT
            CASE
                WHEN ActiveDays >= 20 THEN 'Highly Active'
                WHEN ActiveDays >= 10 THEN 'Moderately Active'
                ELSE 'Low Activity'
            END AS activity_level
        FROM fact_content_interactions
    """
    )

    print("Content dimensions created:")
    print(con.execute("SHOW TABLES").fetchall())

    con.close()


if __name__ == "__main__":
    main()
