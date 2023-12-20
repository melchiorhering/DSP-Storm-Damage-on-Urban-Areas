from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    asset,
    get_dagster_logger,
)
import polars as pl


@asset(
    name="deployment_incident",
    key_prefix="joined",
    ins={
        "storm_deployments": AssetIn(key="storm_deployments"),
        "storm_incident_time_features": AssetIn(key="storm_incident_time_features"),
    },
    io_manager_key="database_io_manager",
    description="Join storm_deployments on storm_incidents based on  Incident_ID from the storm_deployments table",
)
def deployment_incident(
    context: AssetExecutionContext,
    storm_incident_time_features: pl.DataFrame,
    storm_deployments: pl.DataFrame,
) -> pl.DataFrame:
    """
    Join `storm_deployments` on `storm_incidents` based on  `Incident_ID` from the `storm_deployments` table
    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame storm_deployments: DataFrame 1
    :param pl.DataFrame storm_incidents: DataFrame 2
    :return pl.DataFrame: Combined DataFrame
    """

    df = storm_incident_time_features.join(storm_deployments, on="Incident_ID")
    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df


@asset(
    name="deployment_incident_vehicles",
    key_prefix="joined",
    ins={"fire_stations_and_vehicles": AssetIn(key="fire_stations_and_vehicles")},
    deps=[deployment_incident],
    io_manager_key="database_io_manager",
    description="Join the vehicle information to combined incident data",
)
def deployment_incident_vehicles(
    context: AssetExecutionContext,
    deployment_incident: pl.DataFrame,
    fire_stations_and_vehicles: pl.DataFrame,
) -> pl.DataFrame:
    """
    Joins some more vehicle information to the final DataFrame.

    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame deployment_incident: FD data (combined)
    :param pl.DataFrame fire_stations_and_vehicles: table
    :return pl.DataFrame: Combined FD data table
    """
    df = deployment_incident.join(
        fire_stations_and_vehicles, on=["Fire_Station", "Vehicle_Type"]
    )

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df


# @asset(
#     name="deployment_incident_vehicles_serviceareas",
#     key_prefix="joined",
#     ins={"service_areas": AssetIn(key="service_areas")},
#     deps=[deployment_incident_vehicles],
#     io_manager_key="database_io_manager",
#     description="Join service areas to deployment_incident_vehicles",
# )
# def deployment_incident_vehicles_serviceareas(
#     context: AssetExecutionContext,
#     deployment_incident_vehicles: pl.DataFrame,
#     service_areas: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Joins service areas to deployment_incident

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame deployment_incident_vehicles: FD data (combined)
#     :param pl.DataFrame service_areas: table
#     :return pl.DataFrame: Combined FD data table
#     """
#     df = deployment_incident_vehicles.join(
#         service_areas, how="left", left_on="Service_Area", right_on="Verzorgingsgebied", suffix="_service_area"
#     )

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df


@asset(
    name="deployment_incident_vehicles_weather",
    key_prefix="joined",
    ins={"adjust_knmi_data_types": AssetIn(key="adjust_knmi_data_types")},
    deps=[deployment_incident_vehicles],
    io_manager_key="database_io_manager",
    description="Join weather data to combined incident data",
)
def deployment_incident_vehicles_weather(
    context: AssetExecutionContext,
    deployment_incident_vehicles: pl.DataFrame,
    adjust_knmi_data_types: pl.DataFrame,
) -> pl.DataFrame:
    """
    Joins some more vehicle information to the final DataFrame.

    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame deployment_incident: FD data (combined)
    :param pl.DataFrame fire_stations_and_vehicles: table
    :return pl.DataFrame: Weather data with joined FD incidents data
    """
    logger = get_dagster_logger()

    df = adjust_knmi_data_types.join(
        deployment_incident_vehicles,
        left_on=["Date", "Hour"],
        right_on=["Date", "Incident_Starttime_Hour"],
        how="left",
    )

    logger.info(df.head())
    logger.info(df.schema)

    # Checking for null values in each column
    null_counts = df.select(
        [
            pl.col(column).is_null().sum().alias(column + "_null_count")
            for column in df.columns
        ]
    )

    logger.info(null_counts)

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df


# @asset(
#     name="deployment_incident_vehicles_cbs_wijken",
#     key_prefix="joined",
#     ins={"cbs_wijken": AssetIn(key="cbs_wijken")},
#     deps=[deployment_incident_vehicles],
#     io_manager_key="database_io_manager",
#     description="Join cbs-wijken data to combined incident data",
# )
# def deployment_incident_vehicles_cbs_wijken(
#     context: AssetExecutionContext,
#     deployment_incident_vehicles: pl.DataFrame,
#     cbs_wijken: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Joins some more vehicle information to the final DataFrame.

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame cbs_wijkenm: CBS data bout 'wijken'
#     :param pl.DataFrame deployment_incident_vehicles data: Joined FD data
#     :return pl.DataFrame: Fire Department Data Table
#     """
#     logger = get_dagster_logger()

#     depl_inc_veh_pd = deployment_incident_vehicles.to_pandas()
#     cbs_wijken_pd = cbs_wijken.to_pandas()

#     logger.info(depl_inc_veh_pd.head())
#     logger.info(depl_inc_veh_pd.columns)

#     logger.info(cbs_wijken_pd.head())
#     logger.info(cbs_wijken_pd.columns)

#     depl_inc_veh_gpd = gpd.GeoDataFrame(
#         depl_inc_veh_pd,
#         geometry=gpd.points_from_xy(depl_inc_veh_pd.LON, depl_inc_veh_pd.LAT),
#         crs="EPSG:4326",
#     )
#     logger.info(depl_inc_veh_gpd.head())

#     cbs_wijken_gpd = gpd.GeoDataFrame(
#         cbs_wijken_pd,
#         geometry=gpd.GeoSeries.from_wkt(cbs_wijken_pd.geometry),
#         crs="EPSG:4326",
#     )
#     logger.info(cbs_wijken_gpd.head())

#     gdf = cbs_wijken_gpd.sjoin(
#         depl_inc_veh_gpd,
#         how="left",
#     )

#     gdf["geometry"] = gdf["geometry"].apply(dumps)
#     logger.info(gdf.head())
#     df = pl.from_pandas(pd.DataFrame(gdf))

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df
