import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def get_daily_sales_metrics(wide_sales: DataFrame) -> DataFrame:
    """
    Calculates daily sales metrics from the wide sales DataFrame.

    Note: In order to get only two decimals for the averages, value is rounded.

    :param wide_sales: Input DataFrame containing wide sales data.
    :return: DataFrame with daily metrics including "SalesAmountSum", "SalesAmountAvg",
             "ProfitSum", and "ProfitAvg" grouped by "OrderDate".
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