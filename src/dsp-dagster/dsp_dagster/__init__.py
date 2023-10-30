import os
from dagster import (
    Definitions,
    load_assets_from_package_module,
    FilesystemIOManager,  # Update the imports at the top of the file to also include this
)
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dsp_dagster import assets


database_name = os.getenv("DATABASE_NAME")


data_assets = load_assets_from_package_module(
    assets,
    group_name="data_extraction",
)

local_io_manager = FilesystemIOManager(
    base_dir="data/file_system_IO",  # Path is built relative to where `dagster dev` is run
)

# Insert this section anywhere above your `defs = Definitions(...)`
database_io_manager = DuckDBPolarsIOManager(database=database_name)


# Update your Definitions
defs = Definitions(
    assets=data_assets,
    resources={
        "local_io_manager": local_io_manager,
        "database_io_manager": database_io_manager,  # Define the I/O manager here
    },
)
