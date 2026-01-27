import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def get_daily_sales_metrics(wide_sales: DataFrame) -> DataFrame:
    """Aggregate daily sales metrics from the wide sales DataFrame.

    The function:
    1. Groups the input data by ``OrderDate``.
    2. Computes daily totals and averages for sales and profit:
       - ``SalesAmountSum`` as the sum of ``SalesAmount`` per day.
       - ``SalesAmountAvg`` as the average ``SalesAmount`` per day,
         rounded to 2 decimal places.
       - ``ProfitSum`` as the sum of ``Profit`` per day.
       - ``ProfitAvg`` as the average ``Profit`` per day, rounded to
         2 decimal places.

    Args:
        wide_sales (DataFrame): Wide sales DataFrame containing at least
            ``OrderDate``, ``SalesAmount`` and ``Profit`` columns.

    Returns:
        DataFrame: Aggregated daily metrics with one row per ``OrderDate``
        and columns ``SalesAmountSum``, ``SalesAmountAvg``, ``ProfitSum``
        and ``ProfitAvg``.
    """

    return (
        wide_sales
        .groupBy(sf.col("OrderDate"))
        .agg(
            sf.sum(sf.col("SalesAmount")).alias("SalesAmountSum"),
            sf.round(sf.avg(sf.col("SalesAmount")), 2).alias("SalesAmountAvg"),
            sf.sum(sf.col("Profit")).alias("ProfitSum"),
            sf.round(sf.avg(sf.col("Profit")), 2).alias("ProfitAvg"),
        )
    )
