from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.sales import get_sales

def test_get_sales(spark):
    """
     Positive  test that the function get_sales returns the expected DataFrame.
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