import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

CUSTOMERS_MAPPING = {
    "ck": "CustomerKey",
    "name": "Name",
    "bdate": "BirthDate",
    "ms": "MaritalStatus",
    "gender": "Gender",
    "income": "YearlyIncome",
    "childrenhome": "NumberChildrenAtHome",
    "occ": "Occupation",
    "hof": "HouseOwnerFlag",
    "nco": "NumberCarsOwned",
    "addr1": "AddressLine1",
    "addr2": "AddressLine2",
    "phone": "Phone",
}


def get_customers(customers_raw: DataFrame) -> DataFrame:
    """Transform and filter customers data.

    1. Selecting needed columns.
    2. Apply the columns name mapping.
    3. Transform MaritalStatus.
    4. Transform Gender.
    5. Create FullAddress column.
    6. Create IncomeCategory column.
    7. Create BirthYear column.
    8. Remove duplicates.

    :param customers_raw: Raw customers data.
    :return:              Cleaned, filtered and transformed Customers data.
    """

    return (
        customers_raw
        .select(
            sf.col("ck").cast("int"),
            sf.col("name"),
            sf.col("bdate").cast("date"),
            sf.col("ms"),
            sf.col("gender"),
            sf.col("income").cast("int"),
            sf.col("childrenhome").cast("int"),
            sf.col("occ"),
            sf.col("hof").cast("int"),
            sf.col("nco").cast("int"),
            sf.col("addr1"),
            sf.col("addr2"),
            sf.col("phone"),
        )
        .withColumnsRenamed(CUSTOMERS_MAPPING)
        .withColumn(
            "MaritalStatus",
            sf.when(sf.col("MaritalStatus") == "M", 1)
            .when(sf.col("MaritalStatus") == "S", 0)
            .otherwise(None)
            .cast("int")
        )
        .withColumn(
            "Gender",
            sf.when(sf.col("Gender") == "M", 1)
            .when(sf.col("Gender") == "F", 0)
            .otherwise(None)
            .cast("int")
        )
        .withColumn(
            "FullAddress",
            sf.concat_ws(", ", sf.col("AddressLine1"), sf.col("AddressLine2"))
        )
        .withColumn(
            "IncomeCategory",
            sf.when(sf.col("YearlyIncome") <= 50000, "Low")
            .when((sf.col("YearlyIncome") <= 100000), "Medium")
            .otherwise("High")
            .cast("string")
        )
        .withColumn(
            "BirthYear",
            sf.year(sf.col("BirthDate"))
            .cast("int")
        )
        .dropDuplicates()
    )
