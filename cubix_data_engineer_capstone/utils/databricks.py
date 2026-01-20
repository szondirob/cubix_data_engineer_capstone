
from pyspark.sql import DataFrame, SparkSession

def read_file_from_volume(full_path: str, format: str) -> DataFrame:
    """Read a file form UC Volume and return it as a Spark DataFrame

    :param full_path: The path to the file on the volume.
    :param format:    The format of the file. Can be "csv", "parquet", "delta".
    :return:          DataFrame with the data
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
    
    """Writes a DataFrame to UC Volume as a parquet / csv / delta format.

    :param df:           DataFrame to be written.
    :param full_path:    The path to the file on the volume.
    :param format:       The format of the file ("csv", "json", "delta", "parquet").
    :param mode:         Default "overwrite", write mode.
    :param partition_by: List of column to partition by, default is None.
    """
    if format not in ["csv", "parquet", "delta"]:
        raise ValueError(f"Invalid format: {format}. Supported formated are: csv, parquet, delta.")

    writer = df.write.mode(mode).format(format)
    if format == "csv":
        writer = writer.option("header", True)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(full_path)
