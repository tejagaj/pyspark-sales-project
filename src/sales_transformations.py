from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_valid_sales(df: DataFrame) -> DataFrame:
    """
    Filter out invalid sales records.

    Valid records have quantity > 0 and unit_price > 0.

    Args:
        df (DataFrame): Input sales DataFrame

    Returns:
        DataFrame: Filtered DataFrame
    """
    return df.filter(
        (col("quantity") > 0) &
        (col("unit_price") > 0)
    )


def calculate_total_amount(df: DataFrame) -> DataFrame:
    """
    Calculate total_amount for each sale.

    Args:
        df (DataFrame): Sales DataFrame

    Returns:
        DataFrame: DataFrame with total_amount column
    """
    return df.withColumn(
        "total_amount",
        col("quantity") * col("unit_price")
    )
