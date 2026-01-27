from datetime import datetime
from decimal import Decimal

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.gold.daily_sales_metrics import get_daily_sales_metrics


def test_get_daily_sales_metrics(spark):
    """
    Positive test that the function get_daily_sales_metrics returns the expected DataFrame.
    """

    wide_sales_test_data = [
        (datetime(2017, 1, 1), Decimal("10.00"), Decimal("20.00")),
        (datetime(2017, 1, 1), Decimal("16.00"), Decimal("26.00")),
        (datetime(2017, 1, 1), Decimal("10.00"), Decimal("20.00")),
        (datetime(2017, 10, 1), Decimal("20.00"), Decimal("40.00")),
        (datetime(2017, 10, 1), Decimal("60.00"), Decimal("80.00")),
    ]

    wide_sales_schema = st.StructType([
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("SalesAmount", st.DecimalType(10, 2), True),
        st.StructField("Profit", st.DecimalType(10, 2), True),
    ])

    wide_sales_test = spark.createDataFrame(
        wide_sales_test_data,
        schema=wide_sales_schema
    )

    result = get_daily_sales_metrics(wide_sales_test)

    expected_schema = st.StructType([
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("SalesAmountSum", st.DecimalType(10, 2), True),
        st.StructField("SalesAmountAvg", st.DecimalType(10, 2), True),
        st.StructField("ProfitSum", st.DecimalType(10, 2), True),
        st.StructField("ProfitAvg", st.DecimalType(10, 2), True),
    ])

    expected_data = [
        (datetime(2017, 1, 1), Decimal("36.00"), Decimal("12.00"), Decimal("66.00"), Decimal("22.00")),
        (datetime(2017, 10, 1), Decimal("80.00"), Decimal("40.00"), Decimal("120.00"), Decimal("60.00")),
    ]

    expected = spark.createDataFrame(
        expected_data,
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)