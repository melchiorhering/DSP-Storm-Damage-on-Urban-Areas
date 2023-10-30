import os
from dagster import (
    Definitions,
    load_assets_from_package_module,
    FilesystemIOManager,  # Update the imports at the top of the file to also include this
)
from dagster_duckdb_polars import DuckDBPolarsIOManager


from dsp_dagster import assets


data_assets = load_assets_from_package_module(
    assets,
    group_name="data_extraction",
)

# Load Data
# all_assets = load_assets_from_modules([load])

# Define a job that will materialize the assets
# extract_data = define_asset_job("extract_data", selection=AssetSelection.all())

# # Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
# hackernews_schedule = ScheduleDefinition(
#     job=hackernews_job,
#     cron_schedule="0 * * * *",  # every hour
# )

local_io_manager = FilesystemIOManager(
    base_dir="data",  # Path is built relative to where `dagster dev` is run
)

# Insert this section anywhere above your `defs = Definitions(...)`
database_io_manager = DuckDBPolarsIOManager(database="DSP_Project.db")


# Update your Definitions
defs = Definitions(
    assets=data_assets,
    # schedules=[hackernews_schedule],
    resources={
        "local_io_manager": local_io_manager,
        "database_io_manager": database_io_manager,  # Define the I/O manager here
    },
)
