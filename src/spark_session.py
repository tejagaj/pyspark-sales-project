from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
