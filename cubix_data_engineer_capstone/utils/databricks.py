
from pyspark.sql import DataFrame, SparkSession

def read_file_from_volume(full_path: str, format: str) -> DataFrame:
    """Read a file from a Unity Catalog volume into a Spark DataFrame.

    The function:
    1. Validates the requested file format.
    2. Uses the active SparkSession to build a DataFrame reader for the given format.
    3. Applies common options (e.g. header for CSV).
    4. Loads the data from the specified volume path.

    Args:
        full_path (str): Fully qualified path to the file within the UC volume
            (for example, "/Volumes/catalog/schema/volume/path/to/file").
        format (str): Storage format of the file. Must be one of
            "csv", "parquet", or "delta".

    Returns:
        DataFrame: Spark DataFrame containing the loaded data.

    Raises:
        ValueError: If an unsupported format is provided.
    """
    if format not in ["csv", "parquet", "delta"]:
        raise ValueError(f"Invalid format: {format}. Supported formated are: csv, parquet, delta.")

    spark = SparkSession.getActiveSession()

    reader = spark.read.format(format)
    if format == "csv":
        reader = reader.option("header", "true")

    return reader.load(full_path)

def write_file_to_volume(
        df: DataFrame,
        full_path: str,
        format: str,
        mode: str = "overwrite",
        partition_by: list[str] = None
) -> None:

    """Write a Spark DataFrame to a Unity Catalog volume.

    The function:
    1. Validates the requested output format.
    2. Configures a DataFrame writer with the given write mode and format.
    3. Optionally sets CSV header and partitioning behaviour.
    4. Persists the data to the target volume path.

    Args:
        df (DataFrame): Spark DataFrame to be written.
        full_path (str): Target path within the UC volume where the data
            should be stored (for example, "/Volumes/catalog/schema/volume/path").
        format (str): Output data format. Must be one of
            "csv", "parquet", or "delta".
        mode (str, optional): Save mode passed to the writer, such as
            "overwrite", "append", "error", or "ignore". Defaults to "overwrite".
        partition_by (list[str] | None, optional): List of column names used
            to partition the output dataset. If None, no partitioning is applied.

    Returns:
        None: Data is written to the specified location as a side effect.

    Raises:
        ValueError: If an unsupported format is provided.
    """
    if format not in ["csv", "parquet", "delta"]:
        raise ValueError(f"Invalid format: {format}. Supported formated are: csv, parquet, delta.")

    writer = df.write.mode(mode).format(format)
    if format == "csv":
        writer = writer.option("header", True)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(full_path)
