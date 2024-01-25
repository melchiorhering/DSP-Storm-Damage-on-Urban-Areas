import geopandas as gpd
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from shapely import wkt


def convert_to_geodf(polars_df: pl.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert a Polars DataFrame to a GeoDataFrame using WKB or WKT transformation.
    """

    # Convert Polars DataFrame to Pandas DataFrame
    df = polars_df.to_pandas()

    # Convert geometry strings back to geometry objects
    if "geometry" in df.columns:
        df["geometry"] = df["geometry"].apply(wkt.loads)

    else:
        raise ValueError("No 'geometry' column found in the DataFrame")

    # Convert back to GeoDataFrame
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def convert_to_polars(gdf: gpd.GeoDataFrame) -> pl.DataFrame:
    """
    Convert a GeoDataFrame to a Polars DataFrame, converting geometries to WKB strings.
    """
    # If geometry conversion is necessary, uncomment the following line
    # gdf["geometry"] = gdf["geometry"].apply(lambda geom: wkb_dumps(geom, hex=True))
    gdf["geometry"] = gdf["geometry"].apply(wkt.dumps)

    # Convert to Polars DataFrame
    return pl.from_pandas(gdf)





# @asset(
#     name="incidents_nearest_tree",
#     key_prefix="joined",
#     ins={
#         "tree_data": AssetIn(key="tree_data"),
#         "storm_incidents": AssetIn(key="storm_incidents"),
#     },
#     io_manager_key="database_io_manager",
#     description="Join tree data to incidents_buurten ",
# )
# def incidents_nearest_tree(
#     context: AssetExecutionContext,
#     storm_incidents: pl.DataFrame,
#     tree_data: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Join CBS buurten on Storm incidents on Geometry data

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame storm_incidents: storm_incidents
#     :param pl.DataFrame storm_incidents data: Storm incidents data
#     :return pl.DataFrame: resulting joined table
#     """
#     logger = get_dagster_logger()


#     gdf_storm_incidents = convert_to_geodf(storm_incidents)

#     gdf_tree_data = convert_to_geodf(tree_data)

#     # Perform the spatial join
#     incident_nearest_tree = gdf_storm_incidents.sjoin_nearest(gdf_tree_data, distance_col="distance")

#     # Convert tot Polars
#     df = convert_to_polars(incident_nearest_tree)

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(
#                 df.drop("geometry").head().to_pandas().to_markdown()
#             ),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )
#
#    return df




# @asset(
#     name="buurten_trees",
#     key_prefix="joined",
#     ins={
#         "tree_data": AssetIn(key="tree_data"),
#         "cbs_buurten": AssetIn(key="cbs_buurten"),
#     },
#     io_manager_key="database_io_manager",
#     description="Join tree data to incidents_buurten ",
# )
# def buurten_trees(
#     context: AssetExecutionContext,
#     cbs_buurten: pl.DataFrame,
#     tree_data: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Join CBS buurten on Storm incidents on Geometry data

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame cbs_buurten: CBS data bout 'buurten'
#     :param pl.DataFrame storm_incidents data: Storm incidents data
#     :return pl.DataFrame: resulting joined table
#     """
#     logger = get_dagster_logger()

#     # Filter out Total Rows (CBS just adds them)
#     cbs_buurten = cbs_buurten.filter(pl.col("buurtnaam") != " ")
#     gdf_buurten = convert_to_geodf(cbs_buurten)

#     gdf_tree_data = convert_to_geodf(tree_data)
#     logger.info(tree_data.head(20))

#     # Do a spatial join
#     result = gdf_buurten.sjoin(gdf_tree_data)

#     # Convert tot Polars
#     df = convert_to_polars(result)

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(
#                 df.drop("geometry").head().to_pandas().to_markdown()
#             ),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df


# @asset(
#     name="buurten_incidents",
#     key_prefix="joined",
#     ins={
#         "cbs_buurten": AssetIn(key="cbs_buurten"),
#         "storm_incidents": AssetIn(key="cleaned_storm_incidents"),
#     },
#     io_manager_key="database_io_manager",
#     description="Join cbs-buurten on storm incidents",
# )
# def buurten_incidents(
#     context: AssetExecutionContext,
#     storm_incidents: pl.DataFrame,
#     cbs_buurten: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Join CBS buurten on Storm incidents

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame cbs_buurten: CBS data bout 'buurten'
#     :param pl.DataFrame storm_incidents data: Storm incidents data
#     :return pl.DataFrame: resulting joined table
#     """
#     # logger = get_dagster_logger()

#     # Filter out non `buurtnaam` rows (CBS just adds them)
#     cbs_buurten = cbs_buurten.filter(pl.col("buurtnaam") != " ")
#     gdf_buurten = convert_to_geodf(cbs_buurten)

#     # Create a GeoDataFrame from wkb format
#     gdf_storm_incidents = convert_to_geodf(storm_incidents)

#     result = gdf_buurten.sjoin(gdf_storm_incidents)

#     df = convert_to_polars(result)

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(
#                 df.drop("geometry").head().to_pandas().to_markdown()
#             ),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df


# @asset(
#     name="buurten_incidents_trees",
#     key_prefix="joined",
#     ins={
#         "buurten_trees": AssetIn(key="aggregated_buurten_trees"),
#         "buurten_incidents": AssetIn(key="aggregated_buurten_incidents"),
#     },
#     io_manager_key="database_io_manager",
#     description="Join the buurten_trees & buurten_incidents so we have a final buurten dataset",
# )
# def buurten_incidents_trees(
#     context: AssetExecutionContext,
#     buurten_trees: pl.DataFrame,
#     buurten_incidents: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Join CBS buurten on Storm incidents

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame cbs_buurten: CBS data bout 'buurten'
#     :param pl.DataFrame storm_incidents data: Storm incidents data
#     :return pl.DataFrame: resulting joined table
#     """
#     # logger = get_dagster_logger()

#     df = buurten_incidents.join(buurten_trees, on="buurtcode")

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df


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
