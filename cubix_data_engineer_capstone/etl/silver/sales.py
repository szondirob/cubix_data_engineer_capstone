import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

SALES_MAPPING = {
    "son": "SalesOrderNumber",
    "orderdate": "OrderDate",
    "pk": "ProductKey",
    "ck": "CustomerKey",
    "dateofshipping": "ShipDate",
    "oquantity": "OrderQuantity",
}


def get_sales(sales_raw: DataFrame) -> DataFrame:
    """Map and filtered sales data.

    :param sales_raw: Raw sales data.
    :return:          Mapped and filtered Sales data.
    """

    return (
        sales_raw
        .select(
            sf.col("son"),
            sf.col("orderdate").cast("date"),
            sf.col("pk").cast("int"),
            sf.col("ck").cast("int"),
            sf.col("dateofshipping").cast("date"),
            sf.col("oquantity").cast("int"),
        )
        .withColumnsRenamed(SALES_MAPPING)
        .dropDuplicates()
    )