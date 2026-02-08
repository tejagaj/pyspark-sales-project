import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    """
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
    