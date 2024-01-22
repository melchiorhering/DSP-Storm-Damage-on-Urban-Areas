import geopandas as gpd
import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    asset,
    get_dagster_logger,
)
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
    name="incidents_buurten",
    key_prefix="joined",
    ins={
        "cbs_buurten": AssetIn(key="cbs_buurten"),
        "storm_incidents": AssetIn(key="cleaned_storm_incidents"),
    },
    io_manager_key="database_io_manager",
    description="Join cbs-buurten on storm incidents",
)
def incident_deployments_vehicles_wijken(
    context: AssetExecutionContext,
    storm_incidents: pl.DataFrame,
    cbs_buurten: pl.DataFrame,
) -> pl.DataFrame:
    """
    Join CBS buurten on Storm incidents

    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame cbs_buurten: CBS data bout 'buurten'
    :param pl.DataFrame storm_incidents data: Storm incidents data
    :return pl.DataFrame: resulting joined table
    """
    logger = get_dagster_logger()

    logger.info(storm_incidents.columns)

    logger.info(cbs_buurten.columns)

    # Create a GeoDataFrame directly using points_from_xy
    storm_incidents = gpd.GeoDataFrame(
        storm_incidents,
        geometry=gpd.points_from_xy(storm_incidents["LON"], storm_incidents["LAT"]),
    )
    # Create a GeoDataFrame from wkb format
    cbs_buurten = convert_to_geodf(cbs_buurten)

    if storm_incidents.crs != cbs_buurten.crs:
        storm_incidents = storm_incidents.to_crs(cbs_buurten.crs)

    joined_data = gpd.sjoin(storm_incidents, cbs_buurten, how="left", op="within")

    df = convert_to_polars(joined_data)

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df


# @asset(
#     name="incident_deployments",
#     key_prefix="joined",
#     ins={
#         "storm_deployments": AssetIn(key="storm_deployments"),
#         "storm_incidents": AssetIn(key="cleaned_storm_incidents"),
#     },
#     io_manager_key="database_io_manager",
#     description="Join storm_deployments on storm_incidents based on Incident_ID from the storm_deployments table",
# )
# def incident_deployments(
#     context: AssetExecutionContext,
#     storm_incidents: pl.DataFrame,
#     storm_deployments: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Join `storm_deployments` on `storm_incidents` based on  `Incident_ID` from the `storm_deployments` table
#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame storm_deployments: DataFrame 1
#     :param pl.DataFrame storm_incidents: DataFrame 2
#     :return pl.DataFrame: Combined DataFrame
#     """

#     df = storm_incidents.join(storm_deployments, on="Incident_ID")
#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )
#     return df


# @asset(
#     name="incident_deployments_vehicles",
#     key_prefix="joined",
#     ins={"fire_stations_and_vehicles": AssetIn(key="fire_stations_and_vehicles")},
#     deps=[incident_deployments],
#     io_manager_key="database_io_manager",
#     description="Join the vehicle information to combined incident data",
# )
# def incident_deployments_vehicles(
#     context: AssetExecutionContext,
#     incident_deployments: pl.DataFrame,
#     fire_stations_and_vehicles: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Joins some more vehicle information to the final DataFrame.

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame deployment_incident: FD data (combined)
#     :param pl.DataFrame fire_stations_and_vehicles: table
#     :return pl.DataFrame: Combined FD data table
#     """
#     df = incident_deployments.join(
#         fire_stations_and_vehicles, on=["Fire_Station", "Vehicle_Type"]
#     )

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df


# @asset(
#     name="incident_deployments_vehicles_wijken",
#     key_prefix="joined",
#     ins={"cbs_wijken": AssetIn(key="cbs_wijken")},
#     deps=[incident_deployments_vehicles],
#     io_manager_key="database_io_manager",
#     description="Join cbs-wijken info (minus geomtrics) to incidents",
# )
# def incident_deployments_vehicles_wijken(
#     context: AssetExecutionContext,
#     incident_deployments_vehicles: pl.DataFrame,
#     cbs_wijken: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Joins some more vehicle information to the final DataFrame.

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame cbs_wijkenm: CBS data bout 'wijken'
#     :param pl.DataFrame deployment_incident_vehicles data: Joined FD data
#     :return pl.DataFrame: Fire Department Data Table
#     """
#     get_dagster_logger()

#     # Convert to pandas DataFrames
#     incident_deployments_vehicles_pd = incident_deployments_vehicles.to_pandas()
#     cbs_wijken_pd = cbs_wijken.to_pandas()

#     # Convert to GeoDataFrames
#     incident_deployments_vehicles_gpd = gpd.GeoDataFrame(
#         incident_deployments_vehicles_pd,
#         geometry=gpd.points_from_xy(
#             incident_deployments_vehicles_pd.LON, incident_deployments_vehicles_pd.LAT
#         ),
#         crs="EPSG:4326",
#     )

#     cbs_wijken_gpd = gpd.GeoDataFrame(
#         cbs_wijken_pd,
#         geometry=gpd.GeoSeries.from_wkt(cbs_wijken_pd.geometry),
#         crs="EPSG:4326",
#     )

#     # Perform the spatial join
#     joined_df = gpd.sjoin(
#         incident_deployments_vehicles_gpd, cbs_wijken_gpd, how="left", op="intersects"
#     )

#     # Convert the Point geometry column to WKT string using dumps
#     joined_df["geometry"] = joined_df["geometry"].apply(dumps)

#     # Convert back to Polars DataFrame if needed
#     df = pl.from_pandas(joined_df)

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df


# @asset(
#     name="incident_deployments_vehicles_weather",
#     key_prefix="joined",
#     ins={"cleaned_knmi_weather_data": AssetIn(key="cleaned_knmi_weather_data")},
#     deps=[incident_deployments_vehicles],
#     io_manager_key="database_io_manager",
#     description="Join weather data to combined incident data",
# )
# def incident_deployments_vehicles_weather(
#     context: AssetExecutionContext,
#     incident_deployments_vehicles: pl.DataFrame,
#     cleaned_knmi_weather_data: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Joins some more vehicle information to the final DataFrame.

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame deployment_incident: FD data (combined)
#     :param pl.DataFrame fire_stations_and_vehicles: table
#     :return pl.DataFrame: Weather data with joined FD incidents data
#     """

#     df = cleaned_knmi_weather_data.join(
#         incident_deployments_vehicles,
#         left_on=["Date", "Hour"],
#         right_on=["Date", "Incident_Starttime_Hour"],
#         how="left",
#     )

#     # Checking for null values in each column
#     null_counts = df.select(
#         [
#             pl.col(column).is_null().sum().alias(column + "_null_count")
#             for column in df.columns
#         ]
#     )
#     context.add_output_metadata(
#         metadata={
#             "null_counts": MetadataValue.md(null_counts.to_pandas().to_markdown()),
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df
