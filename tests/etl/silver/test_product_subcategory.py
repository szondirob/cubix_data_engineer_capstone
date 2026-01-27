import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.product_subcategory import get_product_subcategory


def test_get_product_subcategory(spark):
    """Validate that get_product_subcategory selects, casts, renames, and deduplicates subcategories.

    This test builds a raw product_subcategory DataFrame with required columns,
    an extra column, and a duplicate row, then calls get_product_subcategory.
    It checks that:
    - Only the mapped columns (psk, pck, epsn, spsn, fpsn) are kept.
    - psk and pck are cast to integers and renamed to ProductSubCategoryKey
      and ProductCategoryKey.
    - Name columns are renamed to their English/Spanish/French subcategory names.
    - Duplicate source rows are removed.
    The result is compared to an expected DataFrame using
    spark_testing.assertDataFrameEqual.
    """

    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("1", "1", "english_name_1", "spanish_name_1", "french_name_1", "extra_value"),
            # exclude - duplicate
            ("1", "1", "english_name_1", "spanish_name_1", "french_name_1", "extra_value"),
        ],
        schema=[
            "psk",
            "pck",
            "epsn",
            "spsn",
            "fpsn",
            "extra_col",
        ],
    )

    result = get_product_subcategory(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductSubcategoryName", st.StringType(), True),
            st.StructField("SpanishProductSubcategoryName", st.StringType(), True),
            st.StructField("FrenchProductSubcategoryName", st.StringType(), True),
        ]
    )

    expected = spark.createDataFrame(
        [
            (
                1,
                1,
                "english_name_1",
                "spanish_name_1",
                "french_name_1"
            )
        ],
        schema=expected_schema,
    )

    spark_testing.assertDataFrameEqual(result, expected)