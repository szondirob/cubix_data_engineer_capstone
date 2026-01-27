import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType

PRODUCTS_MAPPING = {
    "pk": "ProductKey",
    "psck": "ProductSubCategoryKey",
    "name": "ProductName",
    "stancost": "StandardCost",
    "dealerprice": "DealerPrice",
    "listprice": "ListPrice",
    "color": "Color",
    "size": "Size",
    "range": "SizeRange",
    "weight": "Weight",
    "nameofmodel": "ModelName",
    "ssl": "SafetyStockLevel",
    "desc": "Description",
}


def get_products(products_raw: DataFrame) -> DataFrame:
    """Transform raw products data into a cleaned dimension table.

    The function:
    1. Selects relevant product columns from the raw feed.
    2. Casts numeric fields (keys, prices, weight, stock level) to proper types.
    3. Renames columns according to PRODUCTS_MAPPING.
    4. Derives a ProfitMargin column as ListPrice - DealerPrice.
    5. Replaces textual "NA" placeholders with nulls.
    6. Removes duplicate rows.

    Args:
        products_raw (DataFrame): Raw products DataFrame containing at least:
            - pk (product key)
            - psck (product subcategory key)
            - name (product name)
            - stancost (standard cost)
            - dealerprice (dealer price)
            - listprice (list price)
            - color
            - size
            - range (size range)
            - weight
            - nameofmodel (model name)
            - ssl (safety stock level)
            - desc (description)

    Returns:
        DataFrame: Products dimension DataFrame with:
            - ProductKey
            - ProductSubCategoryKey
            - ProductName
            - StandardCost
            - DealerPrice
            - ListPrice
            - Color
            - Size
            - SizeRange
            - Weight
            - ModelName
            - SafetyStockLevel
            - Description
            - ProfitMargin
        All with appropriate data types, cleaned values, and duplicates removed.
    """

    return (
        products_raw
        .select(
            sf.col("pk").cast("int"),
            sf.col("psck").cast("int"),
            sf.col("name"),
            sf.col("stancost").cast(DecimalType(10, 2)).alias("stancost"),
            sf.col("dealerprice").cast(DecimalType(10, 2)).alias("dealerprice"),
            sf.col("listprice").cast(DecimalType(10, 2)).alias("listprice"),
            sf.col("color"),
            sf.col("size").cast("int"),
            sf.col("range"),
            sf.col("weight").cast(DecimalType(10, 2)).alias("weight"),
            sf.col("nameofmodel"),
            sf.col("ssl").cast("int"),
            sf.col("desc"),
        )
        .withColumnsRenamed(PRODUCTS_MAPPING)
        .withColumn("ProfitMargin", sf.col("ListPrice") - sf.col("DealerPrice"))
        .replace("NA", None)
        .dropDuplicates()
    )
