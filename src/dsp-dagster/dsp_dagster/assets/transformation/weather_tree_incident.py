import datetime
import geopandas as gpd
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from ...util.helpers import *


@asset(
    name="new_storm_incidents",
    ins={"storm_incidents": AssetIn(key="storm_incidents")},
    key_prefix="clean",
    io_manager_key="database_io_manager",
    description="Retrieve (time) features from storm_incidents",
)
def new_storm_incidents(
    context: AssetExecutionContext, storm_incidents: pl.DataFrame
) -> pl.DataFrame:
    """
    Retrieve features from storm_incident
    :return pl.DataFrame: storm_incidents with retrieved features
    """

    df = storm_incidents.with_columns(
        [
            pl.col("Incident_Starttime").dt.hour().alias("IncStrt_Hour"),
            # pl.col("Incident_Endtime").dt.hour().alias("Incident_Endtime_Hour"),
            # pl.col("Incident_Duration").dt.hour().alias("Incident_Duration_Hour"),
            # pl.col("Incident_Starttime").dt.minute().alias("Incident_Starttime_Minute"),
            # pl.col("Incident_Endtime").dt.minute().alias("Incident_Endtime_Minute"),
            # pl.col("Incident_Duration").dt.minute().alias("Incident_Duration_Minute"),
        ]
    )  # or .to_datetime("%+")

    # Assume 'df' is your existing Polars DataFrame
    # Specify the columns you want to move to the front
    front_columns = ['Incident_ID', 'Incident_Date', 'IncStrt_Hour']

    # Select the front columns and then the rest of the columns in sorted order
    df = df.select(
        front_columns + sorted([col for col in df.columns if col not in front_columns])
    )

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )
    return df


@asset(
    name="new_tree_data",
    ins={"tree_data": AssetIn(key="tree_data")},
    key_prefix="clean",
    io_manager_key="database_io_manager",
    description="Retrieve (time) features from storm_incidents",
)
def new_tree_data(
    context: AssetExecutionContext, tree_data: pl.DataFrame
) -> pl.DataFrame:
    """
    TO DO
    """

    df = tree_data.with_columns((datetime.datetime.now().year - pl.col("jaarVanAanleg")).alias("leeftijdBoom"))

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )
    return df



@asset(
    name="trees_buurten",
    ins={"cbs_buurten": AssetIn(key="cbs_buurten")},
    deps=[new_tree_data],
    key_prefix="joined",
    io_manager_key="database_io_manager",
)
def weather_trees(
    context: AssetExecutionContext, cbs_buurten: pl.DataFrame, new_tree_data:pl.DataFrame
) -> pl.DataFrame:
    """
    TO DO
    """
    logger = get_dagster_logger()

    gdf_tree_data = convert_to_geodf(new_tree_data)
    logger.info(gdf_tree_data.columns)
    logger.info(gdf_tree_data.dtypes)


    gdf_buurten = convert_to_geodf(cbs_buurten)
    logger.info(gdf_buurten.columns)
    logger.info(gdf_buurten.dtypes)


    gdf = gdf_tree_data.sjoin(gdf_buurten, how='inner')



    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )
    return df



@asset(
    name="tree_service_areas",
    ins={"service_areas": AssetIn(key="service_areas")},
    deps=[new_tree_data],
    key_prefix="joined",
    io_manager_key="database_io_manager",
)
def join_tree_service_areas(
    context: AssetExecutionContext, new_tree_data: pl.DataFrame, service_areas:pl.DataFrame
) -> pl.DataFrame:
    """
    TO DO
    """
    logger = get_dagster_logger()

    gdf_tree_data = convert_to_geodf(new_tree_data)
    logger.info(gdf_tree_data.columns)
    logger.info(gdf_tree_data.dtypes)


    gdf_service_areas = convert_to_geodf(service_areas)
    logger.info(gdf_service_areas.columns)
    logger.info(gdf_service_areas.dtypes)


    gdf = gdf_tree_data.sjoin(gdf_service_areas, how='inner')

    df = convert_to_polars(gdf).drop(["LAT", "LON"])
    df = df.drop(columns=["index_right"])

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.drop("geometry").head().to_pandas().to_markdown()),
        }
    )
    return df





@asset(
    name="weather_incident",
    ins={"knmi_weather_txt": AssetIn(key="knmi_weather_txt")},
    deps=[new_storm_incidents],
    key_prefix="joined",
    io_manager_key="database_io_manager",
)
def weather_incident(
    context: AssetExecutionContext, new_storm_incidents: pl.DataFrame, knmi_weather_txt:pl.DataFrame
) -> pl.DataFrame:
    """
    Retrieve features from storm_incident
    :return pl.DataFrame: storm_incidents with retrieved features
    """
    # logger = get_dagster_logger()

    df = knmi_weather_txt.join(new_storm_incidents, how="left", left_on=["YYYYMMDD", "HH"], right_on=["Incident_Date", "IncStrt_Hour"])


    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.drop("geometry").head().to_pandas().to_markdown()),
        }
    )
    return df







@asset(
    name="incident_tree",
    deps=[new_storm_incidents, new_tree_data],
    key_prefix="joined",
    io_manager_key="database_io_manager",
    description="Retrieve (time) features from storm_incidents",
)
def join_incident_tree(
    context: AssetExecutionContext, new_storm_incidents: pl.DataFrame, new_tree_data:pl.DataFrame
) -> pl.DataFrame:
    """
    TO DO
    """
    logger = get_dagster_logger()

    gdf_tree_data = convert_to_geodf(new_tree_data)
    logger.info(gdf_tree_data.columns)
    logger.info(gdf_tree_data.dtypes)


    gdf_storm_incidents = convert_to_geodf(new_storm_incidents)
    logger.info(gdf_tree_data.columns)
    logger.info(gdf_tree_data.dtypes)

    gdf = gdf_tree_data.sjoin_nearest(gdf_storm_incidents, distance_col="Distance", how="inner", exclusive=True)

    df = convert_to_polars(gdf)
    df = df.drop(columns=["index_right"])


    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.drop("geometry").head().to_pandas().to_markdown()),
        }
    )
    return df


