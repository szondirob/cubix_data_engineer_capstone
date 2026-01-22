import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

PRODUCT_SUBCATEGORY_MAPPING = {
    "psk": "ProductSubCategoryKey",
    "pck": "ProductCategoryKey",
    "epsn": "EnglishProductSubcategoryName",
    "spsn": "SpanishProductSubcategoryName",
    "fpsn": "FrenchProductSubcategoryName",
}


def get_product_subcategory(product_subcategory_raw: DataFrame) -> DataFrame:
    """Transform and filter Product Subcategory data.

    1. Selecting needed columns, and cast the data types.
    2. Rename columns according to the mapping.

    :param product_subcategory_raw: Raw Product Subcategory data.
    :return:                        Cleaned, filtered and transformed Product Subcategory data.
    """
    
    return (
        product_subcategory_raw
        .select(
            sf.col("psk").cast("int"),
            sf.col("pck").cast("int"),
            sf.col("epsn"),
            sf.col("spsn"),
            sf.col("fpsn"),
        )
        .withColumnsRenamed(PRODUCT_SUBCATEGORY_MAPPING)
        .dropDuplicates()
    )
