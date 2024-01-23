import geopandas as gpd
import pandas as pd
import polars as pl
from dagster import AssetExecutionContext, AssetIn, asset, get_dagster_logger
from shapely.wkt import dumps, loads


def convert_to_geodf(polars_df: pl.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert a Polars DataFrame back to a using WKB transformation GeoDataFrame.
    """
    # Convert Polars DataFrame to Pandas DataFrame
    pandas_df = polars_df.to_pandas()

    # Convert geometry strings back to geometry objects
    # Ensure 'geometry' column is present and contains WKT strings
    if "geometry" in pandas_df.columns:
        pandas_df["geometry"] = pandas_df["geometry"].apply(loads)
    else:
        raise ValueError("No 'geometry' column found in the DataFrame")

    # Convert back to GeoDataFrame
    return gpd.GeoDataFrame(pandas_df, geometry="geometry")


def convert_to_polars(gdf: gpd.GeoDataFrame) -> pl.DataFrame:
    """
    Convert a GeoDataFrame to a Polars DataFrame.
    """
    # Efficiently convert geometries to string if needed
    gdf["geometry"] = gdf["geometry"].apply(dumps)  # Consider if this step is necessary
    return pl.from_pandas(pd.DataFrame(gdf))


@asset(
    name="aggregrate_buurten_trees",
    key_prefix="joined",
    ins={
        "buurten_trees": AssetIn(key="buurten_trees"),
    },
    io_manager_key="database_io_manager",
    description="Aggregrate tree data from the buurten_trees table",
)
def aggregrate_buurten_trees(
    context: AssetExecutionContext,
    buurten_trees: pl.DataFrame,
) -> pl.DataFrame:
    """
    Join CBS buurten on Storm incidents

    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame cbs_buurten: CBS data bout 'buurten'
    :param pl.DataFrame storm_incidents data: Storm incidents data
    :return pl.DataFrame: resulting joined table
    """
    logger = get_dagster_logger()

    logger.info(buurten_trees.drop_nulls().head())

    # df = convert_to_polars(joined_data)

    # context.add_output_metadata(
    #     metadata={
    #         "number_of_columns": MetadataValue.int(len(df.columns)),
    #         "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
    #         # The `MetadataValue` class has useful static methods to build Metadata
    #     }
    # )

    # return df
