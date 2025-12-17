import duckdb
from pathlib import Path

# ---------------------------------------
# Paths
# ---------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "src" / "warehouse" / "analytics.duckdb"
SQL_BASE = BASE_DIR / "src" / "warehouse" / "sql"

DIMENSIONS_DIR = SQL_BASE / "dimensions"
ANALYTICS_DIR = SQL_BASE / "analytics"
MARTS_DIR = SQL_BASE / "marts"


def run_folder(con, folder: Path):
    for sql_file in sorted(folder.glob("*.sql")):
        print(f"▶ Running {sql_file.name}")
        con.execute(sql_file.read_text())


def main():
    con = duckdb.connect(str(DB_PATH))

    print("\n=== Dimensions ===")
    run_folder(con, DIMENSIONS_DIR)

    print("\n=== Analytics views ===")
    run_folder(con, ANALYTICS_DIR)

    print("\n=== Marts ===")
    run_folder(con, MARTS_DIR)

    con.close()
    print("\n✅ SQL runner finished successfully")


if __name__ == "__main__":
    main()
