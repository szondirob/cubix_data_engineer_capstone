import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

def get_calendar(calendar_raw: DataFrame) -> DataFrame:
    """Clean and transform data type for calendar data.

    1. Select required columns.
    2. Cast them explicitly
    3. Drop duplicates

    Args:
        calendar_raw (DataFrame): Raw Calendar DataFrame

    Returns:
        DataFrame: Transformed Calendar DataFrame
    """

    return (
        calendar_raw
        .select(
            sf.col("Date").cast("date"),
            sf.col("DayNumberOfWeek").cast("int"),
            sf.col("DayName"),
            sf.col("MonthName"),
            sf.col("MonthNumberOfYear").cast("int"),
            sf.col("DayNumberOfYear").cast("int"),
            sf.col("WeekNumberOfYear").cast("int"),
            sf.col("CalendarQuarter").cast("int"),
            sf.col("CalendarYear").cast("int"),
            sf.col("FiscalYear").cast("int"),
            sf.col("FiscalSemester").cast("int"),
            sf.col("FiscalQuarter").cast("int"),
            sf.col("FinMonthNumberOfYear").cast("int"),
            sf.col("DayNumberOfMonth").cast("int"),
            sf.col("MonthID").cast("int"),
        )
        .dropDuplicates()
    )
