import os
from dagster import (
    Definitions,
    define_asset_job,
    load_assets_from_package_module,
    FilesystemIOManager,  # Update the imports at the top of the file to also include this
)
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dsp_dagster.etl import assets
from dsp_dagster.etl import transformations

# VARIABLES
# ======================================================
database_name = os.getenv("DATABASE_NAME")


# Dagster Assets/Nodes
# ======================================================
data_assets = load_assets_from_package_module(
    assets,
    group_name="data_assets",
)
transformation_assets = load_assets_from_package_module(
    transformations,
    group_name="transformation_assets",
)


# JOBS
# ======================================================
data_extract = define_asset_job("data_extract", selection=data_assets)
data_transform = define_asset_job("data_transform", selection=transformation_assets)

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
    jobs=[data_extract],
    resources={
        "local_io_manager": local_io_manager,
        "database_io_manager": database_io_manager,  # Define the I/O manager here
    },
)
