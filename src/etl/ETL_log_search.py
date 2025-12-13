#!/usr/bin/env python3
import os
import sys
import yaml
import logging
from typing import List, Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    count as f_count,
    when,
    lit,
    concat_ws,
    substring,
)
from pyspark.sql.window import Window

from src.llm.ask_llm import classify_keywords_batchwise


# ============================================================
# Logging Setup (Pretty Logging)
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("Customer-Behavior-ETL")


# ============================================================
# Load config (instead of CLI args)
# ============================================================
def load_config():
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        logger.error("Missing config.yaml file.")
        sys.exit(1)

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


config = load_config()

BASE_PATH = config["base_path"]
FINAL_OUTPUT_PATH = config["output_path"]
MAX_KEYWORDS = config["max_keywords"]
BATCH_SIZE = config["batch_size"]
WRITE_OUTPUT = config["write_output"]


# ============================================================
# Helpers
# ============================================================
def month_paths(prefix: str) -> List[str]:
    try:
        return [
            os.path.join(BASE_PATH, e)
            for e in os.listdir(BASE_PATH)
            if e.startswith(prefix)
        ]
    except FileNotFoundError:
        return []


def top_keyword_per_user(df):
    w = Window.partitionBy("user_id").orderBy(col("cnt").desc())
    df2 = df.groupBy("user_id", "keyword").count().withColumnRenamed("count", "cnt")
    return (
        df2.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("user_id", "keyword")
    )


def compute_top_keywords_list(final_df, max_keywords):
    all_keywords_df = (
        final_df.select(col("most_search_June").alias("keyword"))
        .union(final_df.select(col("most_search_July").alias("keyword")))
        .na.drop()
    )

    keyword_freq = (
        all_keywords_df.groupBy("keyword")
        .agg(f_count("*").alias("freq"))
        .orderBy(col("freq").desc())
    )

    top_keywords = keyword_freq.limit(max_keywords).select("keyword").collect()
    keywords_list = [r["keyword"] for r in top_keywords]
    return keywords_list, all_keywords_df


def build_mapping_df(spark, mapping: Dict[str, str]):
    rows = [(k, v) for k, v in mapping.items()]
    mapping_df = spark.createDataFrame(rows, ["keyword", "genre"])
    return mapping_df


def join_and_label(final_df, mapping_df):
    june_map = mapping_df.withColumnRenamed(
        "keyword", "most_search_June"
    ).withColumnRenamed("genre", "category_june")
    july_map = mapping_df.withColumnRenamed(
        "keyword", "most_search_July"
    ).withColumnRenamed("genre", "category_july")

    result = final_df.join(june_map, "most_search_June", "left").join(
        july_map, "most_search_July", "left"
    )

    result = result.withColumn(
        "Trending_Type",
        when(col("category_june") == col("category_july"), lit("Unchanged")).otherwise(
            lit("Changed")
        ),
    ).withColumn(
        "Previous",
        when(col("category_june") == col("category_july"), lit("Unchanged")).otherwise(
            concat_ws("-", col("category_june"), col("category_july"))
        ),
    )

    return result


# ============================================================
# Main Pipeline
# ============================================================
def main():
    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("KeywordGenreETL").getOrCreate()

    june_paths = month_paths("202206")
    july_paths = month_paths("202207")

    if not june_paths or not july_paths:
        logger.error("Missing 202206 or 202207 data folders inside BASE_PATH.")
        spark.stop()
        sys.exit(1)

    logger.info(f"Loading June data from: {june_paths}")
    logger.info(f"Loading July data from: {july_paths}")

    june = spark.read.parquet(*june_paths)
    july = spark.read.parquet(*july_paths)

    june_top = top_keyword_per_user(june).withColumnRenamed(
        "keyword", "most_search_June"
    )
    july_top = top_keyword_per_user(july).withColumnRenamed(
        "keyword", "most_search_July"
    )

    final_df = june_top.join(july_top, "user_id", "inner")
    logger.info(f"Users with valid June+July searches: {final_df.count()}")

    # Compute top keyword list
    keywords_list, all_keywords_df = compute_top_keywords_list(final_df, MAX_KEYWORDS)
    logger.info(f"Total distinct keywords: {all_keywords_df.distinct().count()}")
    logger.info(f"Sending top {len(keywords_list)} keywords to LLM...")

    if not keywords_list:
        logger.warning("No keywords found. Aborting pipeline.")
        spark.stop()
        sys.exit(0)

    # Call LLM
    mapping = classify_keywords_batchwise(keywords_list, BATCH_SIZE)
    logger.info(f"LLM returned {len(mapping)} classifications.")

    # Check missing
    missing = sorted(set(keywords_list) - set(mapping.keys()))
    logger.info(f"Missing mappings: {missing[:20]}")

    mapping_df = build_mapping_df(spark, mapping)
    result = join_and_label(final_df, mapping_df)
    result_filtered = result.filter(
        (col("category_june").isNotNull()) & (col("category_july").isNotNull())
    )

    # Show filtered results

    result_filtered.select(
        "user_id",
        "most_search_June",
        "category_june",
        "most_search_July",
        "category_july",
        "Trending_Type",
        "Previous",
    ).show(20)

    if WRITE_OUTPUT:
        logger.info(f"Writing output to {FINAL_OUTPUT_PATH}")
        result_filtered.write.mode("overwrite").parquet(FINAL_OUTPUT_PATH)

    logger.info("Pipeline finished successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
