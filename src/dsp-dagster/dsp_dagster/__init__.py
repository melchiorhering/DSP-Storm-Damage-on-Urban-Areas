import os
from dagster import (
    Definitions,
    define_asset_job,
    load_assets_from_package_module,
    FilesystemIOManager,  # Update the imports at the top of the file to also include this
)
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dsp_dagster import assets

# VARIABLES
# ======================================================
database_name = os.getenv("DATABASE_NAME")


# ASSET MODULES
# ======================================================
data_assets = load_assets_from_package_module(
    assets,
    group_name="data_assets",
)


# JOBS
# ======================================================
data_extraction = define_asset_job("data_extraction", selection=data_assets)

# IO MANAGERS
# ======================================================
local_io_manager = FilesystemIOManager(
    base_dir="result/local/",  # Path is built relative to where `dagster dev` is run
)
# Insert this section anywhere above your `defs = Definitions(...)`
database_io_manager = DuckDBPolarsIOManager(database=database_name)


# Update your Definitions
defs = Definitions(
    assets=data_assets,
    jobs=[data_extraction],
    resources={
        "local_io_manager": local_io_manager,
        "database_io_manager": database_io_manager,  # Define the I/O manager here
    },
)
