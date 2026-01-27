from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def scd1_uc(
    spark: SparkSession,
    table_name: str,
    new_data: DataFrame,
    primary_key: str,
) -> None:
    """Apply Slowly Changing Dimension Type 1 (SCD1) logic to a Delta table in UC.

    The function:
    1. Loads the existing Delta table registered as ``table_name``.
    2. Merges the ``new_data`` DataFrame into the master table on ``primary_key``.
    3. Updates all columns for matching keys (Type 1 overwrite of existing rows).
    4. Inserts all non-matching rows as new records.

    Args:
        spark (SparkSession): Active SparkSession used to access the Delta table.
        table_name (str): Fully qualified name of the target Delta table
            (e.g. ``catalog.schema.table``) already existing in Unity Catalog.
        new_data (DataFrame): DataFrame containing the latest snapshot of
            dimension data to be merged into the target table.
        primary_key (str): Column name used as the business/technical key
            for matching records between ``new_data`` and the target table.

    Returns:
        None: The function performs an in-place merge into the Delta table.
    """

    delta_master = DeltaTable.forName(spark, table_name)

    (
        delta_master
        .alias("master")
        .merge(
            new_data.alias("new_data"),
            f"master.{primary_key} = new_data.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
