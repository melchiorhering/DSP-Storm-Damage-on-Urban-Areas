import datetime

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
    name="added_features_tree_data",
    ins={"tree_data": AssetIn(key="tree_data")},
    key_prefix="clean",
    io_manager_key="database_io_manager",
    description="Retrieve (time) features from storm_incidents",
)
def added_features_tree_data(
    context: AssetExecutionContext, tree_data: pl.DataFrame
) -> pl.DataFrame:
    """
    TO DO
    """

    df = tree_data.with_columns(
        (datetime.datetime.now().year - pl.col("jaarVanAanleg")).alias("leeftijdBoom")
    )

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )
    return df


@asset(
    name="added_features_incidents",
    ins={"storm_incidents": AssetIn(key="storm_incidents")},
    key_prefix="clean",
    io_manager_key="database_io_manager",
    description="Add new features to storm-incidents",
)
def added_features_incidents(
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

    # Specify the columns you want to move to the front
    front_columns = ["Incident_ID", "Incident_Date", "IncStrt_Hour"]

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
    name="weather_incident",
    ins={"knmi_weather_txt": AssetIn(key="knmi_weather_txt")},
    deps=[added_features_incidents],
    key_prefix="joined",
    io_manager_key="database_io_manager",
)
def weather_incident(
    context: AssetExecutionContext,
    added_features_incidents: pl.DataFrame,
    knmi_weather_txt: pl.DataFrame,
) -> pl.DataFrame:
    """
    Retrieve features from storm_incident
    :return pl.DataFrame: storm_incidents with retrieved features
    """
    # logger = get_dagster_logger()

    df = knmi_weather_txt.join(
        added_features_incidents,
        how="left",
        left_on=["YYYYMMDD", "HH"],
        right_on=["Incident_Date", "IncStrt_Hour"],
    )

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(
                df.drop("geometry").head().to_pandas().to_markdown()
            ),
        }
    )
    return df


@asset(
    name="incident_tree",
    deps=[added_features_incidents, added_features_tree_data],
    key_prefix="joined",
    io_manager_key="database_io_manager",
    description="Retrieve (time) features from storm_incidents",
)
def join_incident_tree(
    context: AssetExecutionContext,
    added_features_incidents: pl.DataFrame,
    added_features_tree_data: pl.DataFrame,
) -> pl.DataFrame:
    """
    TO DO
    """
    logger = get_dagster_logger()

    gdf_tree_data = convert_to_geodf(added_features_tree_data)
    logger.info(gdf_tree_data.columns)
    logger.info(gdf_tree_data.dtypes)

    gdf_storm_incidents = convert_to_geodf(added_features_incidents)
    logger.info(gdf_tree_data.columns)
    logger.info(gdf_tree_data.dtypes)

    gdf = gdf_tree_data.sjoin_nearest(
        gdf_storm_incidents, distance_col="Distance", how="inner", exclusive=True
    )

    df = convert_to_polars(gdf)
    df = df.drop(columns=["index_right"])

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(
                df.drop("geometry").head().to_pandas().to_markdown()
            ),
        }
    )
    return df


# @asset(
#     name="aggregated_buurten_trees",
#     key_prefix="joined",
#     ins={
#         "buurten_trees": AssetIn(key="buurten_trees"),
#     },
#     io_manager_key="database_io_manager",
#     description="Aggregate the buurten_trees table to create new features",
# )
# def aggregated_buurten_trees(
#     context: AssetExecutionContext,
#     buurten_trees: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Aggregate the buurten_trees data to create new features

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame buurten_trees: Tree data added to CBS buurten
#     :return pl.DataFrame: resulting joined table
#     """
#     logger = get_dagster_logger()

#     # List of columns from trees_data
#     columns_to_pivot = [
#         "boomhoogteklasseActueel",
#         "typeObject",
#         "soortnaamTop",
#         "standplaatsGedetailleerd",
#         "stamdiameterklasse",
#     ]

#     df = buurten_trees.group_by("buurtcode").agg(pl.col("id").count().alias("Totaal"))

#     for i, column in enumerate(columns_to_pivot):
#         temp_df = (
#             buurten_trees.group_by(["buurtcode", column])
#             .agg(pl.col(column).count().alias(f"{column}CntTotal"))
#             .drop_nulls()
#         )

#         # display(temp_df)
#         pivot_df = temp_df.pivot(
#             index="buurtcode", columns=column, values=f"{column}CntTotal"
#         ).fill_null(0)

#         pivot_df = pivot_df.rename(
#             {
#                 col: f"Trees_{column}_{'-'.join(col.split(' '))}"
#                 for col in pivot_df.columns
#                 if col != "buurtcode"
#             }
#         )
#         df = df.join(pivot_df, on="buurtcode", how="left").fill_null(0)
#         logger.info(df.head(5))

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df


# @asset(
#     name="aggregated_buurten_incidents",
#     key_prefix="joined",
#     ins={
#         "buurten_incidents": AssetIn(key="buurten_incidents"),
#     },
#     io_manager_key="database_io_manager",
#     description="Aggregate the buurten_incidents table to create new features",
# )
# def aggregated_buurten_incidents(
#     context: AssetExecutionContext,
#     buurten_incidents: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     Aggregate the buurten_incidents data to create new features

#     :param AssetExecutionContext context: Dagster context
#     :param pl.DataFrame buurten_incidents: Tree data added to CBS buurten
#     :return pl.DataFrame: resulting joined table
#     """
#     # logger = get_dagster_logger()

#     df = buurten_incidents.group_by(
#         [
#             "buurtcode",
#             "Date",
#             "Incident_Starttime_Hour",
#             "Damage_Type",
#         ]
#     ).agg(pl.col("Incident_ID").count().alias("Totaal"))

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )

#     return df
