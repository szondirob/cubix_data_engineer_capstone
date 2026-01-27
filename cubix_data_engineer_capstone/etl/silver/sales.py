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
    """Transform raw sales data into a cleaned fact table.

    The function:
    1. Selects the relevant sales columns from the raw feed.
    2. Casts dates and keys to appropriate data types (date/int).
    3. Renames columns according to SALES_MAPPING.
    4. Removes duplicate rows.

    Args:
        sales_raw (DataFrame): Raw sales DataFrame containing at least:
            - son (sales order identifier)
            - orderdate (order date)
            - pk (product key)
            - ck (customer key)
            - dateofshipping (shipping date)
            - oquantity (ordered quantity)

    Returns:
        DataFrame: Cleaned sales fact DataFrame with columns:
            - SalesOrderNumber
            - OrderDate
            - ProductKey
            - CustomerKey
            - ShipDate
            - OrderQuantity
        All with appropriate types and duplicates removed.
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
