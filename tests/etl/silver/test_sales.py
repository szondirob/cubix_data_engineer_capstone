from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.sales import get_sales

def test_get_sales(spark):
    """Validate that get_sales selects, casts, renames, and deduplicates sales data.

    This test builds a raw sales DataFrame with required columns plus an extra
    column and a duplicate row, then calls get_sales. It checks that:
    - Only the mapped columns (son, orderdate, pk, ck, dateofshipping, oquantity) are kept.
    - orderdate and dateofshipping are cast to dates, and pk, ck, oquantity are cast to integers.
    - Columns are renamed to SalesOrderNumber, OrderDate, ProductKey, CustomerKey,
      ShipDate, and OrderQuantity.
    - Duplicate source rows are removed.
    The result is compared to an expected DataFrame using spark_testing.assertDataFrameEqual.
    """

    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("son_1", "2023-01-01", "1", "1", "2023-01-06", "1", "extra_value"),
            # exclude - duplicate
            ("son_1", "2023-01-01", "1", "1", "2023-01-06", "1", "extra_value"),
        ],
        schema=[
            "son",
            "orderdate",
            "pk",
            "ck",
            "dateofshipping",
            "oquantity",
            "extra_col"
        ]      
    )

    result = get_sales(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("SalesOrderNumber", st.StringType(), True),
            st.StructField("OrderDate", st.DateType(), True),
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("CustomerKey", st.IntegerType(), True),
            st.StructField("ShipDate", st.DateType(), True),
            st.StructField("OrderQuantity", st.IntegerType(), True),
        ]
    )

    expected = spark.createDataFrame(
        [
            (
                "son_1",
                datetime(2023, 1, 1),
                1,
                1,
                datetime(2023, 1, 6),
                1,
            )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)