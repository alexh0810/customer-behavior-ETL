#!/usr/bin/env python3
import os
import sys
import yaml
import logging
from typing import List, Dict

from pyspark.sql.functions import (
    col,
    row_number,
    count as f_count,
    when,
    lit,
    concat_ws,
)
from pyspark.sql.window import Window

from src.utils.spark_helpers import create_spark_session
from src.llm.ask_llm import classify_keywords_batchwise


# ============================================================
# Logging Setup
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Search-ETL")


# ============================================================
# Load Config
# ============================================================
def load_config():
    if not os.path.exists("config.yaml"):
        logger.error("Missing config.yaml")
        sys.exit(1)

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    return config["search"], config.get("spark", {})


# ============================================================
# Helpers
# ============================================================
def month_paths(base_path: str, prefix: str) -> List[str]:
    try:
        return [
            os.path.join(base_path, p)
            for p in os.listdir(base_path)
            if p.startswith(prefix)
        ]
    except FileNotFoundError:
        return []


def top_keyword_per_user(df):
    w = Window.partitionBy("user_id").orderBy(col("cnt").desc())
    return (
        df.groupBy("user_id", "keyword")
        .count()
        .withColumnRenamed("count", "cnt")
        .withColumn("rn", row_number().over(w))
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

    keywords = keyword_freq.limit(max_keywords).collect()
    return [r["keyword"] for r in keywords], all_keywords_df


def build_mapping_df(spark, mapping: Dict[str, str]):
    return spark.createDataFrame(
        [(k, v) for k, v in mapping.items()],
        ["keyword", "genre"],
    )


def join_and_label(final_df, mapping_df):
    june_map = mapping_df.withColumnRenamed(
        "keyword", "most_search_June"
    ).withColumnRenamed("genre", "category_june")

    july_map = mapping_df.withColumnRenamed(
        "keyword", "most_search_July"
    ).withColumnRenamed("genre", "category_july")

    result = (
        final_df.join(june_map, "most_search_June", "left")
        .join(july_map, "most_search_July", "left")
        .withColumn(
            "Trending_Type",
            when(
                col("category_june") == col("category_july"), lit("Unchanged")
            ).otherwise(lit("Changed")),
        )
        .withColumn(
            "Previous",
            when(
                col("category_june") == col("category_july"), lit("Unchanged")
            ).otherwise(concat_ws("-", col("category_june"), col("category_july"))),
        )
    )

    return result


# ============================================================
# Main Pipeline
# ============================================================
def main():
    config, spark_config = load_config()

    BASE_PATH = config["base_path"]
    OUTPUT_PATH = config["output_path"]
    MAX_KEYWORDS = config["max_keywords"]
    BATCH_SIZE = config["batch_size"]
    WRITE_OUTPUT = config["write_output"]

    spark = create_spark_session(
        app_name=spark_config.get("app_name", "SearchETL"),
        master=spark_config.get("master", "local[*]"),
        log_level=spark_config.get("log_level", "WARN"),
    )

    june_paths = month_paths(BASE_PATH, "202206")
    july_paths = month_paths(BASE_PATH, "202207")

    if not june_paths or not july_paths:
        logger.error("Missing June or July data")
        spark.stop()
        sys.exit(1)

    june = spark.read.parquet(*june_paths)
    july = spark.read.parquet(*july_paths)

    june_top = top_keyword_per_user(june).withColumnRenamed(
        "keyword", "most_search_June"
    )
    july_top = top_keyword_per_user(july).withColumnRenamed(
        "keyword", "most_search_July"
    )

    final_df = june_top.join(july_top, "user_id", "inner")

    keywords, all_keywords_df = compute_top_keywords_list(final_df, MAX_KEYWORDS)

    if not keywords:
        logger.warning("No keywords found")
        spark.stop()
        sys.exit(0)

    mapping = classify_keywords_batchwise(keywords, BATCH_SIZE)
    logger.info(f"Classified {len(mapping)} / {len(keywords)} keywords")

    if not mapping:
        logger.warning("LLM returned no valid keyword classifications. Skipping join.")
        spark.stop()
        sys.exit(0)

    mapping_df = build_mapping_df(spark, mapping)
    result = join_and_label(final_df, mapping_df)

    result_filtered = result.filter(
        col("category_june").isNotNull() & col("category_july").isNotNull()
    )

    result_filtered.show(20)

    if WRITE_OUTPUT:
        result_filtered.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
