from cubix_data_engineer_capstone.utils.databricks import read_file_from_volume, write_file_to_volume


def bronze_ingest_volume(
        source_path: str,
        bronze_path: str,
        file_name: str,
        partition_by: list[str] = None
):
    """Ingest a raw file from a source location into the Bronze layer.

    The function:
    1. Reads the specified source file from a Unity Catalog volume as CSV.
    2. Loads it into a Spark DataFrame.
    3. Writes the DataFrame to the Bronze volume path, optionally partitioned.

    Args:
        source_path (str): Base path of the source volume or directory
            containing the raw file (e.g. '/Volumes/.../raw').
        bronze_path (str): Target Bronze volume or directory where the
            ingested file should be stored (e.g. '/Volumes/.../bronze').
        file_name (str): Name of the file to ingest, including extension
            (e.g. 'sales.csv').
        partition_by (list[str] | None, optional): List of column names to
            partition the Bronze dataset by. If None, no partitioning is used.

    Returns:
        None: Data is written to the Bronze path as a side effect via
        write_file_to_volume.
    """

    df = read_file_from_volume(f"{source_path}/{file_name}", "csv")

    return write_file_to_volume(
        df=df,
        full_path=f"{bronze_path}/{file_name}",
        format="csv",
        mode="overwrite",
        partition_by=partition_by
    )
