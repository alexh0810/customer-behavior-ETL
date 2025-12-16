#!/usr/bin/env python3
"""
Content Log Transformation Module
Transforms daily content interaction JSON files (ElasticSearch format)

This module handles the transformation logic for processing customer
viewing behavior from log_content/ data files.

"""
import logging
from pyspark.sql.functions import when, col

logger = logging.getLogger(__name__)


def read_data_from_path(spark, path):
    """
    Read JSON data from path

    Args:
        spark: SparkSession
        path: Path to JSON file

    Returns:
        DataFrame with raw JSON data
    """
    logger.info(f"Reading data from {path}")
    df = spark.read.json(path)
    logger.info(f"  Loaded {df.count()} raw records")
    return df


def select_fields(df):
    """
    Extract nested _source fields from ElasticSearch JSON

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with flattened _source fields
    """
    logger.debug("Selecting nested _source fields")
    if "_source" in df.columns:
        df = df.select("_source.*")
    return df


def calculate_devices(df):
    """
    Calculate total unique devices per contract

    Args:
        df: DataFrame with Contract and Mac columns

    Returns:
        DataFrame with Contract and TotalDevices columns
    """
    logger.info("Calculating total devices per contract")
    total_devices = df.select("Contract", "Mac").groupBy("Contract").count()
    total_devices = total_devices.withColumnRenamed("count", "TotalDevices")
    logger.info(f"  Found devices for {total_devices.count()} contracts")
    return total_devices


def transform_category(df):
    """
    Map AppName to content categories

    Mapping:
    - CHANNEL, DSHD, KPLUS, KPlus → Truyền Hình (TV/Broadcast)
    - VOD, FIMS*, BHD*, DANET → Phim Truyện (Movies/Series)
    - RELAX → Giải Trí (Entertainment)
    - CHILD → Thiếu Nhi (Children)
    - SPORT → Thể Thao (Sports)

    Args:
        df: DataFrame with AppName column

    Returns:
        DataFrame with added Type column
    """
    logger.info("Transforming AppName to content categories")

    df = df.withColumn(
        "Type",
        when(
            (col("AppName") == "CHANNEL")
            | (col("AppName") == "DSHD")
            | (col("AppName") == "KPLUS")
            | (col("AppName") == "KPlus"),
            "Truyền Hình",
        )
        .when(
            (col("AppName") == "VOD")
            | (col("AppName") == "FIMS_RES")
            | (col("AppName") == "BHD_RES")
            | (col("AppName") == "VOD_RES")
            | (col("AppName") == "FIMS")
            | (col("AppName") == "BHD")
            | (col("AppName") == "DANET"),
            "Phim Truyện",
        )
        .when(col("AppName") == "RELAX", "Giải Trí")
        .when(col("AppName") == "CHILD", "Thiếu Nhi")
        .when(col("AppName") == "SPORT", "Thể Thao")
        .otherwise("Other"),
    )

    # Log category distribution
    if logger.isEnabledFor(logging.INFO):
        category_counts = df.groupBy("Type").count()
        logger.info("  Category distribution:")
        for row in category_counts.collect():
            logger.info(f"    {row['Type']}: {row['count']}")

    return df


def calculate_statistics(df):
    """
    Calculate total duration per contract and category
    Returns pivoted table with categories as columns

    Args:
        df: DataFrame with Contract, TotalDuration, and Type columns

    Returns:
        DataFrame with Contract and category columns (Giải Trí, Phim Truyện, etc.)
    """
    logger.info("Calculating viewing statistics by category")

    # Sum TotalDuration by Contract and Type
    statistics = (
        df.select("Contract", "TotalDuration", "Type").groupBy("Contract", "Type").sum()
    )

    statistics = statistics.withColumnRenamed("sum(TotalDuration)", "TotalDuration")

    # Pivot categories to columns and fill nulls with 0
    statistics = (
        statistics.groupBy("Contract").pivot("Type").sum("TotalDuration").na.fill(0)
    )

    logger.info(f"  Computed statistics for {statistics.count()} contracts")
    return statistics


def finalize_result(statistics, total_devices):
    """
    Join statistics with device counts

    Args:
        statistics: DataFrame with Contract and category columns
        total_devices: DataFrame with Contract and TotalDevices columns

    Returns:
        Final DataFrame with all metrics
    """
    logger.info("Joining statistics with device counts")
    result = statistics.join(total_devices, "Contract", "inner")
    logger.info(f"  Final result has {result.count()} contracts")
    return result


def transform_content_log(spark, path, verbose=False):
    """
    Main transformation function - processes a single day's content log JSON file

    This function applies all transformations to convert raw ElasticSearch
    JSON logs into aggregated customer viewing statistics.

    Args:
        spark: SparkSession
        path: Path to JSON file
        verbose: If True, show intermediate DataFrames

    Returns:
        Transformed DataFrame with columns:
        - Contract: Customer ID
        - Giải Trí: Entertainment duration
        - Phim Truyện: Movies/Series duration
        - Thiếu Nhi: Children's content duration
        - Thể Thao: Sports duration
        - Truyền Hình: TV/Broadcast duration
        - TotalDevices: Number of unique devices
    """
    logger.info("=" * 60)
    logger.info("Starting content log transformation")
    logger.info("=" * 60)

    # Read data
    df = read_data_from_path(spark, path)
    if verbose:
        logger.info("Raw data sample:")
        df.show(5)

    # Select fields
    df = select_fields(df)
    if verbose:
        logger.info("After selecting fields:")
        df.show(5)

    # Calculate devices
    total_devices = calculate_devices(df)
    if verbose:
        logger.info("Device counts:")
        total_devices.show(5)

    # Transform category
    df = transform_category(df)
    if verbose:
        logger.info("After category transformation:")
        df.show(5)

    # Calculate statistics
    statistics = calculate_statistics(df)
    if verbose:
        logger.info("Category statistics:")
        statistics.show(5)

    # Finalize result
    result = finalize_result(statistics, total_devices)
    if verbose:
        logger.info("Final result:")
        result.show(5)

    logger.info("Content log transformation completed successfully")
    logger.info("=" * 60)

    return result


# Backward compatibility aliases
def main(spark, path, verbose=False):
    """
    Backward compatible alias for transform_content_log()
    Maintains compatibility with code that imports etl_script.main()
    """
    return transform_content_log(spark, path, verbose)


def transform_single_file(spark, json_path):
    """
    Backward compatible wrapper
    """
    return transform_content_log(spark, json_path, verbose=False)
