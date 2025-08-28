import os
import sys
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = Path(__file__).parent.parent.resolve()
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Also set PYTHONPATH environment variable if not set
if "PYTHONPATH" not in os.environ:
    os.environ["PYTHONPATH"] = str(parent_dir)
elif str(parent_dir) not in os.environ["PYTHONPATH"]:
    os.environ["PYTHONPATH"] = f"{parent_dir}:{os.environ['PYTHONPATH']}"

import shutil
import tempfile
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_merchants_schema():
    """Schema for merchants data."""
    return StructType(
        [
            StructField("merchant_id", StringType(), False),
            StructField("merchant_name", StringType(), True),
            StructField("city_id", IntegerType(), True),
            StructField("state_id", IntegerType(), True),
        ]
    )


@pytest.fixture
def sample_transactions_schema():
    """Schema for transactions data."""
    return StructType(
        [
            StructField("merchant_id", StringType(), False),
            StructField("purchase_amount", DoubleType(), False),
            StructField("purchase_date", TimestampType(), False),
            StructField("category", StringType(), True),
            StructField("installments", IntegerType(), True),
        ]
    )


@pytest.fixture
def sample_merchants_data():
    """Sample merchants data."""
    return [
        ("M001", "Merchant A", 1, 10),
        ("M002", "Merchant B", 2, 20),
        ("M003", "Merchant C", 1, 10),
        ("M004", None, 3, 30),  # Merchant without name
    ]


@pytest.fixture
def sample_transactions_data():
    """Sample transactions data."""
    return [
        ("M001", 1000.0, datetime(2023, 1, 15, 14, 30), "Electronics", 1),
        ("M001", 2000.0, datetime(2023, 1, 20, 15, 45), "Electronics", 2),
        ("M002", 500.0, datetime(2023, 2, 10, 10, 15), None, 1),  # Null category
        ("M003", 1500.0, datetime(2023, 1, 25, 16, 20), "Fashion", 3),
        ("M004", 3000.0, datetime(2023, 2, 5, 14, 00), "Home", 1),  # Merchant without name
        (
            "M005",
            750.0,
            datetime(2023, 1, 30, 11, 30),
            "Sports",
            1,
        ),  # Merchant not in merchants table
    ]


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)
