"""Hive integration utilities for Spark-based data analysis.

This module provides functionality to create Hive-enabled Spark sessions and
manage data persistence in Hive tables. It supports both managed and external
tables, with optimizations for query performance.

Key features:
- Automatic database creation
- Table partitioning support
- Statistics computation for query optimization
- External table creation from existing Parquet files

Environment variables:
- HIVE_METASTORE_URI: Optional URI for external Hive metastore
"""

import os

from pyspark.sql import DataFrame, SparkSession


def get_spark_with_hive(app_name: str = "BillupsDataAnalysis") -> SparkSession:
    """
    Create a Spark session with Hive support enabled.

    This function creates a Spark session configured for Hive integration,
    allowing data to be persisted in Hive tables for long-term storage
    and cross-session access. It includes performance optimizations and
    automatic metastore configuration.

    Args:
        app_name: Name for the Spark application. Defaults to "BillupsDataAnalysis".

    Returns:
        SparkSession: Hive-enabled Spark session.

    Configuration details:
        - spark.sql.warehouse.dir: Location for managed table data (/warehouse)
        - spark.sql.catalogImplementation: Set to "hive" for Hive support
        - spark.sql.adaptive.*: Enables adaptive query execution
        - spark.sql.hive.metastore.uris: Configured from HIVE_METASTORE_URI env var

    Environment:
        Checks for HIVE_METASTORE_URI environment variable to connect to
        external metastore. If not set, uses embedded Derby metastore.

    Example:
        >>> spark = get_spark_with_hive("MyHiveApp")
        >>> spark.sql("SHOW DATABASES").show()
        >>> spark.stop()
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.warehouse.dir", "/warehouse")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # Add Hive metastore URI if available
    metastore_uri = os.environ.get("HIVE_METASTORE_URI")
    if metastore_uri:
        builder = builder.config("spark.sql.hive.metastore.uris", metastore_uri)

    # Enable Hive support
    return builder.enableHiveSupport().getOrCreate()


def write_to_hive(
    df: DataFrame,
    table_name: str,
    database: str = "billups_analytics",
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """
    Write a DataFrame to a Hive table with automatic optimization.

    This function handles the complete workflow of persisting data to Hive,
    including database creation, partitioning, and statistics computation
    for optimal query performance.

    Args:
        df: DataFrame to write to Hive
        table_name: Name of the target Hive table
        database: Target database name. Created if not exists.
                 Defaults to "billups_analytics".
        mode: Write mode - "overwrite", "append", "error", or "ignore".
              Defaults to "overwrite".
        partition_by: Optional list of column names to partition by.
                     Partitioning improves query performance for filtered queries.

    Side effects:
        - Creates database if it doesn't exist
        - Creates or overwrites the specified table
        - Computes table statistics for query optimization

    Example:
        >>> df = spark.read.parquet("transactions.parquet")
        >>> write_to_hive(df, "transactions_fact",
        ...              partition_by=["year", "month"])
        Data written to Hive table: billups_analytics.transactions_fact

    Note:
        Table statistics are automatically computed after write to ensure
        the Spark optimizer has accurate information for query planning.
    """
    # Create database if not exists
    spark: SparkSession = df.sql_ctx.sparkSession  # type: ignore
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Write to Hive table
    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(f"{database}.{table_name}")

    # Analyze table for better query performance
    spark.sql(f"ANALYZE TABLE {database}.{table_name} COMPUTE STATISTICS")

    print(f"Data written to Hive table: {database}.{table_name}")


def create_external_table_from_parquet(
    spark: SparkSession, table_name: str, parquet_path: str, database: str = "billups_analytics"
) -> None:
    """
    Create an external Hive table pointing to existing Parquet files.

    External tables are useful when you want to query existing data files
    through Hive without moving or copying the data. The table metadata
    is stored in Hive metastore while data remains in its original location.

    Args:
        spark: Active SparkSession with Hive support enabled
        table_name: Name for the new external table
        parquet_path: Path to existing Parquet file(s). Can be a directory
                     containing multiple Parquet files.
        database: Target database name. Created if not exists.
                 Defaults to "billups_analytics".

    Side effects:
        - Creates database if it doesn't exist
        - Creates external table definition in Hive metastore
        - Table points to original data location (no data copy)

    Example:
        >>> spark = get_spark_with_hive()
        >>> create_external_table_from_parquet(
        ...     spark, "historical_data",
        ...     "hdfs://data/historical/parquet/"
        ... )
        External table created: billups_analytics.historical_data

    Note:
        The Parquet schema is automatically inferred from the files.
        Dropping an external table only removes metadata, not the data files.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Read schema from Parquet
    df = spark.read.parquet(parquet_path)

    # Create external table
    df.write.mode("overwrite").option("path", parquet_path).saveAsTable(f"{database}.{table_name}")

    print(f"External table created: {database}.{table_name} from {parquet_path}")
