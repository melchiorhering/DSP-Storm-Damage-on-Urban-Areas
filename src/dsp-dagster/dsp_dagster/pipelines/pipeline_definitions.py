from dagster import asset, Definitions
import polars as pl
from dagster_duckdb_polars import DuckDBPolarsIOManager


@asset(key_prefix=["my_schema"])  # will be used as the schema in DuckDB
def my_table() -> pl.DataFrame:  # the name of the asset will be the table name
    df = pl.read_excel("../../data/Stom_Data_Incidents.xlsx")


defs = Definitions(
    assets=[my_table],
    resources={"io_manager": DuckDBPolarsIOManager(database="my_db.duckdb")},
)


# def my_pipeline():
#     processed_data = process_data()
#     # Use the processed data for further processing or output
