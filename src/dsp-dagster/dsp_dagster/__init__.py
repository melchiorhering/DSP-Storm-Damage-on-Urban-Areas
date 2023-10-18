from dagster import Definitions, load_assets_from_modules
from dagster_duckdb_polars import DuckDBPolarsIOManager


from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={"io_manager": DuckDBPolarsIOManager(database="DSP.duckdb")},
)
