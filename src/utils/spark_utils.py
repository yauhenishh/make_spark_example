"""Spark utilities for session management and data operations.

This module provides common Spark functionality including session creation
with optimized configurations and utilities for saving DataFrames in various formats.

The configurations are optimized for:
- Adaptive query execution for better performance
- Efficient shuffle operations
- Arrow-based data exchange between Spark and Python
"""

from pyspark.sql import DataFrame, SparkSession


def create_spark_session(app_name: str = "BillupsDataAnalysis") -> SparkSession:
    """
    Create a Spark session with optimized configurations.

    This function creates a Spark session with performance optimizations suitable
    for data analysis workloads. The configurations enable adaptive query execution
    and optimize data exchange between Spark and Python.

    Args:
        app_name: Name for the Spark application. Shows in Spark UI and logs.
                 Defaults to "BillupsDataAnalysis".

    Returns:
        SparkSession: Configured Spark session ready for use.

    Configuration details:
        - spark.sql.adaptive.enabled: Enables adaptive query execution for
          dynamic optimization of query plans based on runtime statistics
        - spark.sql.adaptive.coalescePartitions.enabled: Automatically combines
          small partitions to reduce overhead
        - spark.sql.shuffle.partitions: Default number of partitions for shuffles
          (200 is suitable for moderate data sizes)
        - spark.sql.execution.arrow.pyspark.enabled: Uses Apache Arrow for
          efficient data transfer between JVM and Python

    Example:
        >>> spark = create_spark_session("MyAnalysis")
        >>> df = spark.read.parquet("data.parquet")
        >>> spark.stop()
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def save_results(
    df: DataFrame, output_path: str, format: str = "csv", mode: str = "overwrite"
) -> None:
    """
    Save a DataFrame to disk in the specified format.

    This utility function provides a consistent interface for saving analysis
    results with appropriate optimizations for each format. CSV output is
    coalesced to a single file for easier consumption, while Parquet maintains
    partitioning for efficiency.

    Args:
        df: The DataFrame to save
        output_path: Destination path for the output file(s). Can be local or HDFS.
        format: Output format. Supported: "csv" or "parquet". Defaults to "csv".
        mode: Save mode. Options: "overwrite", "append", "error", "ignore".
              Defaults to "overwrite".

    Raises:
        ValueError: If an unsupported format is specified.

    Format-specific behaviors:
        - CSV: Coalesces to single partition for single output file with headers
        - Parquet: Maintains natural partitioning for performance

    Example:
        >>> df = spark.read.csv("input.csv", header=True)
        >>> save_results(df, "output/results.csv", format="csv")
        >>> save_results(df, "output/results.parquet", format="parquet")
    """
    if format == "csv":
        # Coalesce to single file for CSV to avoid multiple part files
        # This is acceptable for analysis results which are typically small
        df.coalesce(1).write.mode(mode).option("header", True).csv(output_path)
    elif format == "parquet":
        # Keep natural partitioning for Parquet as it handles multiple files well
        df.write.mode(mode).parquet(output_path)
    else:
        raise ValueError(f"Unsupported format: {format}. Supported formats: csv, parquet")
