from decimal import Decimal

import pyspark.sql.types as st
import pyspark.testing as spark_testing

from cubix_data_engineer_capstone.etl.gold.daily_product_category_metrics import (
    get_daily_product_category_metrics,
)


def test_get_daily_product_category_metrics(spark):
    """
    Positive test that the function get_daily_product_category_metrics returns the expected DataFrame.
    """
    wide_sales_test_data = [
        ("Bikes", Decimal("10.00"), Decimal("20.00")),
        ("Bikes", Decimal("16.00"), Decimal("26.00")),
        ("Clothing", Decimal("10.00"), Decimal("20.00")),
        ("Bikes", Decimal("20.00"), Decimal("40.00")),
        ("Bikes", Decimal("60.00"), Decimal("80.00")),
    ]

    wide_sales_schema = st.StructType([
        st.StructField("EnglishProductCategoryName", st.StringType(), True),
        st.StructField("SalesAmount", st.DecimalType(10, 2), True),
        st.StructField("Profit", st.DecimalType(10, 2), True),
    ])

    wide_sales_test = spark.createDataFrame(
        wide_sales_test_data,
        schema=wide_sales_schema
    )

    result = get_daily_product_category_metrics(wide_sales_test)

    expected_schema = st.StructType([
        st.StructField("EnglishProductCategoryName", st.StringType(), True),
        st.StructField("SalesAmountSum", st.DecimalType(10, 2), True),
        st.StructField("SalesAmountAvg", st.DecimalType(10, 2), True),
        st.StructField("ProfitSum", st.DecimalType(10, 2), True),
        st.StructField("ProfitAvg", st.DecimalType(10, 2), True),
    ])

    # Bikes:
    # SalesAmount: 10 + 16 + 20 + 60 = 106 → avg = 26.50
    # Profit:      20 + 26 + 40 + 80 = 166 → avg = 41.50
    expected_data = [
        ("Bikes", Decimal("106.00"), Decimal("26.50"), Decimal("166.00"), Decimal("41.50")),
        ("Clothing", Decimal("10.00"), Decimal("10.00"), Decimal("20.00"), Decimal("20.00")),
    ]

    expected = spark.createDataFrame(
        expected_data,
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)