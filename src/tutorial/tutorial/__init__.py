from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    FilesystemIOManager,  # Update the imports at the top of the file to also include this
)

from dagster_duckdb_polars import DuckDBPolarsIOManager
from .resources import DataGeneratorResource

from . import assets

all_assets = load_assets_from_modules([assets])

# Define a job that will materialize the assets
hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *",  # every hour
)

io_manager = FilesystemIOManager(
    base_dir="data",  # Path is built relative to where `dagster dev` is run
)

# Insert this section anywhere above your `defs = Definitions(...)`
database_io_manager = DuckDBPolarsIOManager(database="analytics.hackernews")


datagen = DataGeneratorResource()  # Make the resource


# Update your Definitions
defs = Definitions(
    assets=all_assets,
    schedules=[hackernews_schedule],
    resources={
        "io_manager": io_manager,
        "database_io_manager": database_io_manager,  # Define the I/O manager here
        "hackernews_api": datagen,  # Add the newly-made resource here
    },
)
