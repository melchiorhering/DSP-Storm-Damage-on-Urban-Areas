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
    name="buurten_bomen",
    ins={
        "cbs_buurten": AssetIn(key="cbs_buurten"),
        "added_features_tree_data": AssetIn(key="added_features_tree_data"),
    },
    key_prefix="joined",
    io_manager_key="database_io_manager",
    description="Create a joined buurten & Bomen table",
)
def buurten_bomen(
    context: AssetExecutionContext,
    cbs_buurten: pl.DataFrame,
    added_features_tree_data: pl.DataFrame,
) -> pl.DataFrame:
    """
    TO-DO
    """
    logger = get_dagster_logger()

    # buurten Data Selection
    cbs_buurten_selection = cbs_buurten.select("buurtcode", "buurtnaam", "geometry")
    gdf_cbs_buurten_selection = convert_to_geodf(
        cbs_buurten_selection
    )  # Transform to GeoPandas
    logger.info(cbs_buurten_selection.head(5))

    # Tree Data Selection
    tree_data_selection = added_features_tree_data.select(
        "id",
        "boomhoogteklasseActueel",
        "standplaatsGedetailleerd",
        "typeObject",
        "soortnaamTop",
        "geometry",
    )  # "leeftijdBoom" removed since not alot of info
    tree_data_selection = convert_to_geodf(
        tree_data_selection
    )  # Transform to GeoPandas
    logger.info(tree_data_selection.head(5))

    joined_gdf = gdf_cbs_buurten_selection.sjoin(tree_data_selection).drop(
        columns=["geometry", "index_right"]
    )
    joined_df = pl.from_dataframe(joined_gdf)
    logger.info(joined_df.shape)
    logger.info(joined_df.head(5))

    # List of columns from trees_data
    columns_to_pivot = [
        "boomhoogteklasseActueel",
        "typeObject",
        "soortnaamTop",
        "standplaatsGedetailleerd",
    ]

    # Original DataFrame aggregation
    buurten_bomen = joined_df.group_by(["buurtnaam"]).agg(
        pl.col("id").count().alias("Trees_Totaal")
    )
    logger.info(f"Original DF shape:{buurten_bomen.shape}")

    pivot_dfs = []
    for column in columns_to_pivot:
        # Create a pivot table for each column
        temp_df = (
            joined_df.group_by(["buurtnaam", column])
            .agg(pl.col(column).count().alias(f"{column}CntTotal"))
            .drop_nulls()
        )

        pivot_df = temp_df.pivot(
            index="buurtnaam", columns=column, values=f"{column}CntTotal"
        ).fill_null(0)

        pivot_df = pivot_df.rename(
            {
                col: f"Trees_{column}_{'-'.join(col.split(' '))}"
                for col in pivot_df.columns
                if col != "buurtnaam"
            }
        )
        pivot_dfs.append(pivot_df)

    # Join pivot tables with the original DataFrame
    for pivot_df in pivot_dfs:
        buurten_bomen = buurten_bomen.join(
            pivot_df, on="buurtnaam", how="left"
        ).fill_null(0)

    logger.info(f"Final DF shape:{buurten_bomen.shape}")
    logger.info(buurten_bomen.head(5))

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(buurten_bomen.columns)),
            "preview": MetadataValue.md(buurten_bomen.head().to_pandas().to_markdown()),
        }
    )
    return buurten_bomen


@asset(
    name="buurten_weather_trees",
    ins={
        "cbs_buurten": AssetIn(key="cbs_buurten"),
        "knmi_weather_txt": AssetIn(key="knmi_weather_txt"),
    },
    deps=[buurten_bomen],
    key_prefix="joined",
    io_manager_key="database_io_manager",
    description="Create a joined buurten, Weather & Trees table",
)
def buurten_weather_trees(
    context: AssetExecutionContext,
    cbs_buurten: pl.DataFrame,
    knmi_weather_txt: pl.DataFrame,
    buurten_bomen: pl.DataFrame,
) -> pl.DataFrame:
    """
    TO-DO
    """
    logger = get_dagster_logger()

    # Select useful buurtcode and buurtnaam
    cbs_buurten_selection = cbs_buurten.select("buurtcode", "buurtnaam")
    logger.info(cbs_buurten_selection.shape)
    logger.info(cbs_buurten_selection.head(5))

    # Select all unique combinations of KNMI_WEATHER data based on wind variables
    knmi_weather_txt_selection = knmi_weather_txt.select(
        ["DD", "FH", "FF", "FX"]
    ).unique()
    logger.info(knmi_weather_txt_selection.shape)
    logger.info(knmi_weather_txt_selection.head(5))

    cross_join_buurten_weather = cbs_buurten_selection.join(
        knmi_weather_txt_selection, how="cross"
    )
    logger.info(cross_join_buurten_weather.shape)
    logger.info(cross_join_buurten_weather.head())

    logger.info(buurten_bomen.head())
    logger.info(cross_join_buurten_weather.shape)

    buurten_weather_trees = cross_join_buurten_weather.join(
        buurten_bomen, on="buurtnaam", how="inner"
    )

    logger.info(buurten_weather_trees.shape)
    logger.info(buurten_weather_trees.columns)
    logger.info(buurten_weather_trees.head())

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(buurten_weather_trees.columns)),
            "preview": MetadataValue.md(
                buurten_weather_trees.head().to_pandas().to_markdown()
            ),
        }
    )
    return buurten_weather_trees


# @asset(
#     name="buurten_weer_bomen_incidenten",
#     ins={
#         "weather_incident": AssetIn(key="weather_incident"),
#     },
#     deps=[buurten_weather_trees],
#     key_prefix="joined",
#     io_manager_key="database_io_manager",
#     description="Join buurten_weather_trees on weather_incident to create a final table to train on",
# )
# def buurten_weer_bomen_incidenten(
#     context: AssetExecutionContext,
#     buurten_weather_trees: pl.DataFrame,
#     weather_incident: pl.DataFrame,
# ) -> pl.DataFrame:
#     """
#     TO-DO
#     """
#     logger = get_dagster_logger()

#     # Do a aggregation of weather_incidents, count all incidents based on weather (wind) conditions and ""
#     groupby_weather_incident = weather_incident.group_by(
#         ["DD", "FH", "FF", "FX", "Damage_Type"]
#     ).agg(Total_Incidents=pl.col("Incident_ID").count())
#     # Fill NaN values in 'Damage_Type' column with 'No-Incident' and update the DataFrame
#     groupby_weather_incident = groupby_weather_incident.with_columns(
#         groupby_weather_incident["Damage_Type"].fill_null("No-Incident")
#     )
#     logger.info(groupby_weather_incident.shape)
#     logger.info(groupby_weather_incident.head(5))

#     buurten_weer_bomen_incidenten = buurten_weather_trees.join(
#         groupby_weather_incident, how="left", on=["DD", "FH", "FF", "FX"]
#     )

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(
#                 len(buurten_weer_bomen_incidenten.columns)
#             ),
#             "preview": MetadataValue.md(
#                 buurten_weer_bomen_incidenten.head().to_pandas().to_markdown()
#             ),
#         }
#     )
#     return buurten_weer_bomen_incidenten
