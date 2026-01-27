import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def get_daily_product_category_metrics(wide_sales: DataFrame) -> DataFrame:
    """Aggregate sales metrics by product category.

    The function:
    1. Groups the input data by ``EnglishProductCategoryName``.
    2. Computes category-level totals and averages for sales and profit:
       - ``SalesAmountSum`` as the sum of ``SalesAmount`` per category.
       - ``SalesAmountAvg`` as the average ``SalesAmount`` per category,
         rounded to 2 decimal places.
       - ``ProfitSum`` as the sum of ``Profit`` per category.
       - ``ProfitAvg`` as the average ``Profit`` per category, rounded to
         2 decimal places.

    Args:
        wide_sales (DataFrame): Wide sales DataFrame containing at least
            ``EnglishProductCategoryName``, ``SalesAmount`` and ``Profit``
            columns.

    Returns:
        DataFrame: Aggregated metrics with one row per
        ``EnglishProductCategoryName`` and columns ``SalesAmountSum``,
        ``SalesAmountAvg``, ``ProfitSum`` and ``ProfitAvg``.
    """

    return (
        wide_sales
        .groupBy(sf.col("EnglishProductCategoryName"))
        .agg(
            sf.sum(sf.col("SalesAmount")).alias("SalesAmountSum"),
            sf.round(sf.avg(sf.col("SalesAmount")), 2).alias("SalesAmountAvg"),
            sf.sum(sf.col("Profit")).alias("ProfitSum"),
            sf.round(sf.avg(sf.col("Profit")), 2).alias("ProfitAvg"),
        )
    )
