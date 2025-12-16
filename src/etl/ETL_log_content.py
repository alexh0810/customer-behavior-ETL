#!/usr/bin/env python3
import os
import sys
import yaml
import logging
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql.functions import (
    col,
    lit,
    when,
    greatest,
    concat_ws,
    countDistinct,
    sum as f_sum,
)

from src.utils.spark_helpers import create_spark_session
from src.utils.content_log_transformer import transform_content_log


# ============================================================
# Logging Setup
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Content-ETL")


# ============================================================
# Load Config
# ============================================================
def load_config():
    if not os.path.exists("config.yaml"):
        logger.error("Missing config.yaml")
        sys.exit(1)

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    return config["content_interactions"], config.get("spark", {})


# ============================================================
# Helpers
# ============================================================
def generate_date_list(start, end):
    start_d = datetime.strptime(start, "%Y%m%d").date()
    end_d = datetime.strptime(end, "%Y%m%d").date()

    dates = []
    cur = start_d
    while cur <= end_d:
        dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)

    return dates


def process_daily(spark, base_path, date_str):
    path = os.path.join(base_path, f"{date_str}.json")
    if not os.path.exists(path):
        logger.warning(f"Missing {path}")
        return None

    df = transform_content_log(spark, path, verbose=False)
    return df.withColumn("Date", lit(date_str))


# ============================================================
# Main Pipeline
# ============================================================
def main():
    config, spark_config = load_config()

    BASE_PATH = config["base_path"]
    OUTPUT_PATH = config["output_path"]
    START_DATE = config["start_date"]
    END_DATE = config["end_date"]
    WRITE_OUTPUT = config["write_output"]
    CATEGORIES = config["categories"]

    spark = create_spark_session(
        app_name=spark_config.get("app_name", "ContentETL"),
        master=spark_config.get("master", "local[*]"),
        log_level=spark_config.get("log_level", "WARN"),
    )

    # --------------------------------------------------------
    # Read & process daily files
    # --------------------------------------------------------
    daily_dfs = []
    for date_str in generate_date_list(START_DATE, END_DATE):
        df = process_daily(spark, BASE_PATH, date_str)
        if df is not None:
            daily_dfs.append(df)

    if not daily_dfs:
        logger.error("No data processed")
        spark.stop()
        sys.exit(1)

    # --------------------------------------------------------
    # Union daily results
    # --------------------------------------------------------
    df_final = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        daily_dfs,
    )

    # --------------------------------------------------------
    # Active days (must be computed BEFORE aggregation)
    # --------------------------------------------------------
    active_days = df_final.groupBy("Contract").agg(
        countDistinct("Date").alias("ActiveDays")
    )

    # --------------------------------------------------------
    # Aggregate to ONE ROW per Contract
    # --------------------------------------------------------
    df_final = df_final.groupBy("Contract").agg(
        *[f_sum(col(c)).alias(c) for c in CATEGORIES]
    )

    # Join ActiveDays back
    df_final = df_final.join(active_days, "Contract", "left")

    # --------------------------------------------------------
    # Most watched category
    # --------------------------------------------------------
    cond = None
    for cat in CATEGORIES:
        cond = (
            when(greatest(*[col(c) for c in CATEGORIES]) == col(cat), cat)
            if cond is None
            else cond.when(greatest(*[col(c) for c in CATEGORIES]) == col(cat), cat)
        )

    df_final = df_final.withColumn("Most_Watched", cond)

    # --------------------------------------------------------
    # Customer taste
    # --------------------------------------------------------
    df_final = df_final.withColumn(
        "Taste",
        concat_ws(", ", *[when(col(c) > 0, lit(c)) for c in CATEGORIES]),
    ).withColumn("Taste", when(col("Taste") == "", "No watch").otherwise(col("Taste")))

    # --------------------------------------------------------
    # Output
    # --------------------------------------------------------
    df_final.show(20, truncate=False)

    if WRITE_OUTPUT:
        logger.info(f"Writing output to {OUTPUT_PATH}")
        df_final.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
