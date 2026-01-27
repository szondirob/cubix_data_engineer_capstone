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
    """Transform raw product subcategory data into a cleaned dimension table.

    The function:
    1. Selects required product subcategory columns from the raw feed.
    2. Casts key columns to integer types.
    3. Renames columns according to PRODUCT_SUBCATEGORY_MAPPING.
    4. Removes duplicate rows.

    Args:
        product_subcategory_raw (DataFrame): Raw product subcategory DataFrame
            containing at least:
            - psk (product subcategory key)
            - pck (product category key)
            - epsn (English product subcategory name)
            - spsn (Spanish product subcategory name)
            - fpsn (French product subcategory name)

    Returns:
        DataFrame: Product subcategory dimension DataFrame with:
            - ProductSubCategoryKey
            - ProductCategoryKey
            - EnglishProductSubcategoryName
            - SpanishProductSubcategoryName
            - FrenchProductSubcategoryName
        All with appropriate data types and duplicates removed.
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
