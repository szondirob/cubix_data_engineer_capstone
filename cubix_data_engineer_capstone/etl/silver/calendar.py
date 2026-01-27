import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

def get_calendar(calendar_raw: DataFrame) -> DataFrame:
    """Transform raw calendar data into a cleaned dimension table.

    The function:
    1. Selects relevant calendar columns.
    2. Casts columns to appropriate data types (date/int/string).
    3. Removes duplicate rows.

    Args:
        calendar_raw (DataFrame): Raw calendar DataFrame containing at least
            the following columns:
            - Date
            - DayNumberOfWeek
            - DayName
            - MonthName
            - MonthNumberOfYear
            - DayNumberOfYear
            - WeekNumberOfYear
            - CalendarQuarter
            - CalendarYear
            - FiscalYear
            - FiscalSemester
            - FiscalQuarter
            - FinMonthNumberOfYear
            - DayNumberOfMonth
            - MonthID

    Returns:
        DataFrame: Calendar dimension DataFrame with selected columns,
        properly cast types, and duplicate rows removed.
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
