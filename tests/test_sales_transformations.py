from src.sales_transformations import (
    filter_valid_sales,
    calculate_total_amount
)


def test_filter_valid_sales(spark):
    data = [
        (1, 10.0),
        (0, 5.0),
        (-1, 10.0)
    ]
    df = spark.createDataFrame(data, ["quantity", "unit_price"])

    result_df = filter_valid_sales(df)

    assert result_df.count() == 1


def test_calculate_total_amount(spark):
    data = [(2, 15.0)]
    df = spark.createDataFrame(data, ["quantity", "unit_price"])

    result = calculate_total_amount(df).collect()[0]

    assert result["total_amount"] == 30.0
    