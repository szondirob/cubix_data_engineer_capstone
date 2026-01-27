import pyspark.sql.types as st
import pyspark.testing as spark_testing

from cubix_data_engineer_capstone.etl.silver.product_category import get_product_category


def test_get_product_category(spark):
    """Validate that get_product_category selects, casts, renames, and deduplicates categories.

    This test builds a raw product_category DataFrame containing required columns,
    extra columns, and duplicate rows, then calls get_product_category. It checks that:
    - Only the mapped columns (pck, epcn, spcn, fpcn) are kept.
    - pck is cast to integer and renamed to ProductCategoryKey.
    - Name columns are renamed to their English/Spanish/French counterparts.
    - Duplicate source rows are removed.
    The result is compared to an expected DataFrame using spark_testing.assertDataFrameEqual.
    """
    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("1", "1", "english_name_1", "spanish_name_1", "french_name_1", "extra_value"),
            # exclude - duplicate
            ("1", "1", "english_name_1", "spanish_name_1", "french_name_1", "extra_value"),
            # include - another row
            ("2", "2", "english_name_2", "spanish_name_2", "french_name_2", "extra_value"),
        ],
        schema=[
            "pck",
            "pcak",   # extra in source -> should be ignored
            "epcn",
            "spcn",
            "fpcn",
            "extra_col",
        ],
    )

    result = get_product_category(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductCategoryName", st.StringType(), True),
            st.StructField("SpanishProductCategoryName", st.StringType(), True),
            st.StructField("FrenchProductCategoryName", st.StringType(), True),
        ]
    )

    expected = spark.createDataFrame(
        [
            (1, "english_name_1", "spanish_name_1", "french_name_1"),
            (2, "english_name_2", "spanish_name_2", "french_name_2"),
        ],
        schema=expected_schema,
    )

    spark_testing.assertDataFrameEqual(result, expected)