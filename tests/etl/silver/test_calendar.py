from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.calendar import get_calendar

def test_get_calendar(spark):
    """Validate that get_calendar casts, filters, and deduplicates calendar data correctly.

    This test builds a raw calendar DataFrame with all required columns plus an
    extra column and duplicate rows, then calls get_calendar. It verifies that:
    - Only the expected calendar columns are kept.
    - String-typed numeric fields are cast to the appropriate integer/date types.
    - Duplicate rows are removed so only one record remains.
    The result is compared to an expected DataFrame using
    spark_testing.assertDataFrameEqual.
    """

    test_data = spark.createDataFrame(
        [
            ("2017-01-01", "7", "Sunday", "January", "1", "1", "52", "1", "2017", "2016", "1", "1", "7", "1", "201701", "extra_value"),
            ("2017-01-01", "7", "Sunday", "January", "1", "1", "52", "1", "2017", "2016", "1", "1", "7", "1", "201701", "extra_value"),
        ],
        schema=[
            "Date",
            "DayNumberOfWeek",
            "DayName",
            "MonthName",
            "MonthNumberOfYear",
            "DayNumberOfYear",
            "WeekNumberOfYear",
            "CalendarQuarter",
            "CalendarYear",
            "FiscalYear",
            "FiscalSemester",
            "FiscalQuarter",
            "FinMonthNumberOfYear",
            "DayNumberOfMonth",
            "MonthID",
            "extra_col"
        ]      
    )

    result = get_calendar(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("Date", st.DateType(), True),
            st.StructField("DayNumberOfWeek", st.IntegerType(), True),
            st.StructField("DayName", st.StringType(), True),
            st.StructField("MonthName", st.StringType(), True),
            st.StructField("MonthNumberOfYear", st.IntegerType(), True),
            st.StructField("DayNumberOfYear", st.IntegerType(), True),
            st.StructField("WeekNumberOfYear", st.IntegerType(), True),
            st.StructField("CalendarQuarter", st.IntegerType(), True),
            st.StructField("CalendarYear", st.IntegerType(), True),
            st.StructField("FiscalYear", st.IntegerType(), True),
            st.StructField("FiscalSemester", st.IntegerType(), True),
            st.StructField("FiscalQuarter", st.IntegerType(), True),
            st.StructField("FinMonthNumberOfYear", st.IntegerType(), True),
            st.StructField("DayNumberOfMonth", st.IntegerType(), False),
            st.StructField("MonthID", st.IntegerType(), True)
        ]
    )

    expected = spark.createDataFrame(
        [
            (datetime(2017, 1, 1),
             7,
             "Sunday",
             "January",
             1,
             1,
             52,
             1,
             2017,
             2016,
             1,
             1,
             7,
             1,
             201701
             )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)