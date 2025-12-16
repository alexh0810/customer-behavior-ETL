#!/usr/bin/env python3
"""
Shared Spark utilities for ETL pipelines
"""
import os
import logging
from typing import List
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: str = "ETL", master: str = "local[*]", log_level: str = "WARN"
) -> SparkSession:
    """
    Create a standardized Spark session with common configurations
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(log_level)
    logger.info(f"Spark session created: {app_name}")
    return spark


def list_data_paths(base_path: str, prefix: str = None) -> List[str]:
    """
    List all subdirectories in base_path, optionally filtered by prefix

    Args:
        base_path: Root directory to search
        prefix: Optional prefix to filter directories (e.g., "202206")

    Returns:
        List of full paths to matching directories
    """
    try:
        entries = os.listdir(base_path)
        if prefix:
            entries = [e for e in entries if e.startswith(prefix)]

        paths = [os.path.join(base_path, e) for e in entries]
        # Filter to only directories
        paths = [p for p in paths if os.path.isdir(p)]

        logger.info(f"Found {len(paths)} paths with prefix '{prefix}' in {base_path}")
        return paths
    except FileNotFoundError:
        logger.error(f"Base path not found: {base_path}")
        return []


def safe_read_parquet(spark: SparkSession, paths: List[str]):
    """
    Safely read parquet files with error handling
    """
    if not paths:
        raise ValueError("No paths provided to read")

    try:
        df = spark.read.parquet(*paths)
        logger.info(f"Successfully read {df.count()} rows from {len(paths)} path(s)")
        return df
    except Exception as e:
        logger.error(f"Error reading parquet files: {e}")
        raise


def validate_required_columns(df, required_columns: List[str]):
    """
    Validate that a DataFrame contains all required columns
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    logger.info(f"All required columns present: {required_columns}")
