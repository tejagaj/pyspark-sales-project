from src.spark_session import get_spark_session
from src.sales_transformations import (
    filter_valid_sales,
    calculate_total_amount
)


def run_sales_job(input_path: str, output_path: str) -> None:
    """
    Run sales ETL pipeline.

    Args:
        input_path (str): Source parquet path
        output_path (str): Target parquet path
    """
    spark = get_spark_session("Sales_ETL_Job")

    sales_df = spark.read.parquet(input_path)

    valid_sales_df = filter_valid_sales(sales_df)
    enriched_df = calculate_total_amount(valid_sales_df)

    enriched_df.write.mode("overwrite").parquet(output_path)
