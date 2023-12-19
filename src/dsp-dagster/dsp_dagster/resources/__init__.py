import os

# from dagster._utils import file_relative_path
from dagster import FilesystemIOManager
from dagster_duckdb_polars import DuckDBPolarsIOManager

# HERE YOU CAN CREATE RESOURCE (DICTS)
# FOR EXAMPLE: A DEV, ACCEPT AND PROD RESOURCE WITH DIFFERENT IO MANAGERS AND CONNECTIONS
database_name = os.getenv("DATABASE_NAME")


# Path is built relative to where `dagster dev` is run
LOCAL_RESOURCE = {
    "local_io_manager": FilesystemIOManager(base_dir="result/local/"),
    "database_io_manager": DuckDBPolarsIOManager(
        database=database_name
    ),  # Define the I/O manager here
    # "geospatial_pandas_io_manager": DuckDBPandasIOManager(database=database_name),
}
