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
    """Transform raw customer data into a cleaned dimension table.

    The function:
    1. Selects relevant customer columns and casts them to proper types.
    2. Renames columns according to CUSTOMERS_MAPPING.
    3. Encodes marital status as an integer flag (M → 1, S → 0, else null).
    4. Encodes gender as an integer flag (M → 1, F → 0, else null).
    5. Builds a FullAddress field from address line columns.
    6. Derives an IncomeCategory (Low/Medium/High) from YearlyIncome.
    7. Derives BirthYear from BirthDate.
    8. Removes duplicate rows.

    Args:
        customers_raw (DataFrame): Raw customer DataFrame containing at least:
            - ck (customer key)
            - name (customer name)
            - bdate (birth date)
            - ms (marital status)
            - gender
            - income (yearly income)
            - childrenhome (number of children at home)
            - occ (occupation)
            - hof (house owner flag)
            - nco (number of cars owned)
            - addr1 (address line 1)
            - addr2 (address line 2)
            - phone

    Returns:
        DataFrame: Customer dimension DataFrame with renamed, typed, and
        enriched columns (including MaritalStatus, Gender, FullAddress,
        IncomeCategory, BirthYear) and duplicates removed.
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
