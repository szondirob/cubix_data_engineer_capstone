from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.customers import get_customers


def test_get_customers(spark):
    """
    Positive test that the function get_customers returns the DataFrame
    """

    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("1", "name_1", "1980-01-01", "M", "F", "50000", "0", "occ_1", "1", "1", "addr_1", "addr_2", "000-000-000", "extra_value"),
            # exclude - duplicate
            ("1", "name_1", "1980-01-01", "M", "F", "50000", "0", "occ_1", "1", "1", "addr_1", "addr_2", "000-000-000", "extra_value"),
            # include - MaritalStatus / Gender = None, YearlyIncome = 50001
            ("2", "name_2", "1980-01-01", None, None, "50001", "0", "occ_2", "1", "1", "addr_3", "addr_4", "000-000-000", "extra_value"),
        ],
    schema=[
        "ck",
        "name",
        "bdate",
        "ms",
        "gender",
        "income",
        "childrenhome",
        "occ",
        "hof",
        "nco",
        "addr1",
        "addr2",
        "phone",
        "extra_col",
    ],
)

    result = get_customers(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("CustomerKey", st.IntegerType(), True),
            st.StructField("Name", st.StringType(), True),
            st.StructField("BirthDate", st.DateType(), True),
            st.StructField("MaritalStatus", st.IntegerType(), True),
            st.StructField("Gender", st.IntegerType(), True),
            st.StructField("YearlyIncome", st.IntegerType(), True),
            st.StructField("NumberChildrenAtHome", st.IntegerType(), True),
            st.StructField("Occupation", st.StringType(), True),
            st.StructField("HouseOwnerFlag", st.IntegerType(), True),
            st.StructField("NumberCarsOwned", st.IntegerType(), True),
            st.StructField("AddressLine1", st.StringType(), True),
            st.StructField("AddressLine2", st.StringType(), True),
            st.StructField("Phone", st.StringType(), True),
            st.StructField("FullAddress", st.StringType(), False),
            st.StructField("IncomeCategory", st.StringType(), False),
            st.StructField("BirthYear", st.IntegerType(), True),
        ]
    )

    expected = spark.createDataFrame(
        [
            (
            1,
            "name_1",
            datetime(1980, 1, 1),
            1,
            0,
            50000,
            0,
            "occ_1",
            1,
            1,
            "addr_1",
            "addr_2",
            "000-000-000",
            "addr_1, addr_2",
            "Low",
            1980
            ),
            (
            2,
            "name_2",
            datetime(1980, 1, 1),
            None,
            None,
            50001,
            0,
            "occ_2",
            1,
            1,
            "addr_3",
            "addr_4",
            "000-000-000",
            "addr_3, addr_4",
            "Medium",
            1980
            )
        ],
        schema=expected_schema,
    )

    spark_testing.assertDataFrameEqual(result, expected)