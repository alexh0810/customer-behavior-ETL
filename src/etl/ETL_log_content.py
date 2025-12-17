#!/usr/bin/env python3
"""
Incremental ETL Pipeline for Customer Content Logs

- Processes daily JSON logs one day at a time
- Aggregates per-contract metrics incrementally
- Avoids global shuffles and driver OOM
"""

import os
import sys
import yaml
import logging
from datetime import datetime, timedelta

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
        cfg = yaml.safe_load(f)

    return cfg["content_interactions"], cfg.get("spark", {})


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
    """
    Process ONE day and return per-contract aggregated DF
    """
    path = os.path.join(base_path, f"{date_str}.json")
    if not os.path.exists(path):
        logger.warning(f"Missing {path}")
        return None

    logger.info(f"Processing {date_str}")
    df = transform_content_log(spark, path, verbose=False)

    return df.withColumn("Date", lit(date_str))


def incremental_merge(df_acc, df_day, categories):
    """
    Merge one day's aggregated data into accumulator safely
    """
    if df_acc is None:
        return df_day

    # Rename columns to avoid ambiguity
    left = df_acc.select(
        "Contract",
        *[col(c).alias(f"{c}_acc") for c in categories],
        col("TotalDevices").alias("TotalDevices_acc"),
    )

    right = df_day.select(
        "Contract",
        *[col(c).alias(f"{c}_day") for c in categories],
        col("TotalDevices").alias("TotalDevices_day"),
    )

    joined = left.join(right, "Contract", "outer").na.fill(0)

    # Sum category durations
    for c in categories:
        joined = joined.withColumn(c, col(f"{c}_acc") + col(f"{c}_day"))

    # Devices: keep max observed
    joined = joined.withColumn(
        "TotalDevices",
        greatest(col("TotalDevices_acc"), col("TotalDevices_day")),
    )

    return joined.select("Contract", *categories, "TotalDevices")


# ============================================================
# Main Pipeline
# ============================================================
def main():
    config, spark_config = load_config()

    BASE_PATH = config["base_path"]
    OUTPUT_PATH = config["output_path"]
    START_DATE = config["start_date"]
    END_DATE = config["end_date"]
    WRITE_OUTPUT = config.get("write_output", False)
    CATEGORIES = config["categories"]

    spark = create_spark_session(
        app_name=spark_config.get("app_name", "ContentETL"),
        master=spark_config.get("master", "local[*]"),
        log_level=spark_config.get("log_level", "WARN"),
    )

    df_final = None

    for date_str in generate_date_list(START_DATE, END_DATE):
        df_day = process_daily(spark, BASE_PATH, date_str)
        if df_day is None:
            continue

        df_final = incremental_merge(df_final, df_day, CATEGORIES)

    if df_final is None:
        logger.error("No data processed")
        spark.stop()
        sys.exit(1)

    # ===========================
    # Final derived metrics
    # ===========================

    # Most watched category
    cond = None
    for cat in CATEGORIES:
        cond = (
            when(greatest(*[col(c) for c in CATEGORIES]) == col(cat), cat)
            if cond is None
            else cond.when(greatest(*[col(c) for c in CATEGORIES]) == col(cat), cat)
        )

    df_final = df_final.withColumn("Most_Watched", cond)

    # Taste profile
    df_final = df_final.withColumn(
        "Taste",
        concat_ws(", ", *[when(col(c) > 0, lit(c)) for c in CATEGORIES]),
    ).withColumn("Taste", when(col("Taste") == "", "No watch").otherwise(col("Taste")))

    # Active days
    active_days = df_final.select("Contract").withColumn(
        "ActiveDays", lit(len(generate_date_list(START_DATE, END_DATE)))
    )

    df_final = df_final.join(active_days, "Contract", "left")

    logger.info(f"Final contract count: {df_final.count()}")

    if WRITE_OUTPUT:
        logger.info(f"Writing output to {OUTPUT_PATH}")
        df_final.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()
    logger.info("Content ETL finished successfully")


if __name__ == "__main__":
    main()
