import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

PRODUCT_CATEGORY_MAPPING = {
    "pck": "ProductCategoryKey",
    "epcn": "EnglishProductCategoryName",
    "spcn": "SpanishProductCategoryName",
    "fpcn": "FrenchProductCategoryName",
}


def get_product_category(product_category_raw: DataFrame) -> DataFrame:
    """Transform raw product category data into a cleaned dimension table.

    The function:
    1. Selects required category columns from the raw feed.
    2. Casts the category key to integer.
    3. Renames columns according to PRODUCT_CATEGORY_MAPPING.
    4. Removes duplicate rows.

    Args:
        product_category_raw (DataFrame): Raw product category DataFrame
            containing at least:
            - pck (product category key)
            - epcn (English product category name)
            - spcn (Spanish product category name)
            - fpcn (French product category name)

    Returns:
        DataFrame: Product category dimension DataFrame with:
            - ProductCategoryKey
            - EnglishProductCategoryName
            - SpanishProductCategoryName
            - FrenchProductCategoryName
        All with appropriate data types and duplicates removed.
    """
    return (
        product_category_raw
        .select(
            sf.col("pck").cast("int"),
            sf.col("epcn"),
            sf.col("spcn"),
            sf.col("fpcn"),
        )
        .withColumnsRenamed(PRODUCT_CATEGORY_MAPPING)
        .dropDuplicates()
    )
