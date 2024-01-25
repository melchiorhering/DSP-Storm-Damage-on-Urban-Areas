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
    name="wijken_bomen",
    ins={
        "cbs_wijken": AssetIn(key="cbs_wijken"),
        "added_features_tree_data": AssetIn(key="added_features_tree_data"),
    },
    key_prefix="joined",
    io_manager_key="database_io_manager",
    description="Create a joined Wijken & Bomen table",
)
def wijken_bomen(
    context: AssetExecutionContext,
    cbs_wijken: pl.DataFrame,
    added_features_tree_data: pl.DataFrame,
) -> pl.DataFrame:
    """
    TO-DO
    """
    logger = get_dagster_logger()

    # Wijken Data Selection
    cbs_wijken_selection = cbs_wijken.select("wijkcode", "wijknaam", "geometry")
    gdf_cbs_wijken_selection = convert_to_geodf(
        cbs_wijken_selection
    )  # Transform to GeoPandas
    logger.info(cbs_wijken_selection.head(5))

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

    joined_gdf = gdf_cbs_wijken_selection.sjoin(tree_data_selection).drop(
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
    wijken_bomen = joined_df.group_by(["wijknaam"]).agg(
        pl.col("id").count().alias("Trees_Totaal")
    )
    logger.info(f"Original DF shape:{wijken_bomen.shape}")

    pivot_dfs = []
    for column in columns_to_pivot:
        # Create a pivot table for each column
        temp_df = (
            joined_df.group_by(["wijknaam", column])
            .agg(pl.col(column).count().alias(f"{column}CntTotal"))
            .drop_nulls()
        )

        pivot_df = temp_df.pivot(
            index="wijknaam", columns=column, values=f"{column}CntTotal"
        ).fill_null(0)

        pivot_df = pivot_df.rename(
            {
                col: f"Trees_{column}_{'-'.join(col.split(' '))}"
                for col in pivot_df.columns
                if col != "wijknaam"
            }
        )
        pivot_dfs.append(pivot_df)

    # Join pivot tables with the original DataFrame
    for pivot_df in pivot_dfs:
        wijken_bomen = wijken_bomen.join(pivot_df, on="wijknaam", how="left").fill_null(
            0
        )

    logger.info(f"Final DF shape:{wijken_bomen.shape}")
    logger.info(wijken_bomen.head(5))

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(wijken_bomen.columns)),
            "preview": MetadataValue.md(wijken_bomen.head().to_pandas().to_markdown()),
        }
    )
    return wijken_bomen


@asset(
    name="wijken_weather_trees",
    ins={
        "cbs_wijken": AssetIn(key="cbs_wijken"),
        "knmi_weather_txt": AssetIn(key="knmi_weather_txt"),
    },
    deps=[wijken_bomen],
    key_prefix="joined",
    io_manager_key="database_io_manager",
    description="Create a joined Wijken, Weather & Trees table",
)
def wijken_weather_trees(
    context: AssetExecutionContext,
    cbs_wijken: pl.DataFrame,
    knmi_weather_txt: pl.DataFrame,
    wijken_bomen: pl.DataFrame,
) -> pl.DataFrame:
    """
    TO-DO
    """
    logger = get_dagster_logger()

    # Select useful wijkcode and wijknaam
    cbs_wijken_selection = cbs_wijken.select("wijkcode", "wijknaam")
    logger.info(cbs_wijken_selection.shape)
    logger.info(cbs_wijken_selection.head(5))

    # Select all unique combinations of KNMI_WEATHER data based on wind variables
    knmi_weather_txt_selection = knmi_weather_txt.select(
        ["DD", "FH", "FF", "FX"]
    ).unique()
    logger.info(knmi_weather_txt_selection.shape)
    logger.info(knmi_weather_txt_selection.head(5))

    cross_join_wijken_weather = cbs_wijken_selection.join(
        knmi_weather_txt_selection, how="cross"
    )
    logger.info(cross_join_wijken_weather.shape)
    logger.info(cross_join_wijken_weather.head())

    logger.info(wijken_bomen.head())
    logger.info(cross_join_wijken_weather.shape)

    wijken_weather_trees = cross_join_wijken_weather.join(
        wijken_bomen, on="wijknaam", how="inner"
    )

    logger.info(wijken_weather_trees.shape)
    logger.info(wijken_weather_trees.columns)
    logger.info(wijken_weather_trees.head())

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(wijken_weather_trees.columns)),
            "preview": MetadataValue.md(
                wijken_weather_trees.head().to_pandas().to_markdown()
            ),
        }
    )
    return wijken_weather_trees


@asset(
    name="wijken_weer_bomen_incidenten",
    ins={
        "weather_incident": AssetIn(key="weather_incident"),
    },
    deps=[wijken_weather_trees],
    key_prefix="joined",
    io_manager_key="database_io_manager",
    description="Join wijken_weather_trees on weather_incident to create a final table to train on",
)
def wijken_weer_bomen_incidenten(
    context: AssetExecutionContext,
    wijken_weather_trees: pl.DataFrame,
    weather_incident: pl.DataFrame,
) -> pl.DataFrame:
    """
    TO-DO
    """
    logger = get_dagster_logger()

    # Do a aggregation of weather_incidents, count all incidents based on weather (wind) conditions and ""
    groupby_weather_incident = weather_incident.group_by(
        ["DD", "FH", "FF", "FX", "Damage_Type"]
    ).agg(Total_Incidents=pl.col("Incident_ID").count())
    # Fill NaN values in 'Damage_Type' column with 'No-Incident' and update the DataFrame
    groupby_weather_incident = groupby_weather_incident.with_columns(
        groupby_weather_incident["Damage_Type"].fill_null("No-Incident")
    )
    logger.info(groupby_weather_incident.shape)
    logger.info(groupby_weather_incident.head(5))

    wijken_weer_bomen_incidenten = wijken_weather_trees.join(
        groupby_weather_incident, how="left", on=["DD", "FH", "FF", "FX"]
    )

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(
                len(wijken_weer_bomen_incidenten.columns)
            ),
            "preview": MetadataValue.md(
                wijken_weer_bomen_incidenten.head().to_pandas().to_markdown()
            ),
        }
    )
    return wijken_weer_bomen_incidenten


@asset(
    name="categorical_data",
    deps=[wijken_weer_bomen_incidenten],
    key_prefix="pre_process",
    io_manager_key="database_io_manager",
    description="Pre-Process data for Tree models",
)
def pre_process_categorical_data(
    context: AssetExecutionContext, wijken_weer_bomen_incidenten: pl.DataFrame
) -> pl.DataFrame:
    """
    TO-DO
    """
    logger = get_dagster_logger()

    # SELECT ALL ROWS WITH INCIDENTS == 0
    selection_eq_0 = wijken_weer_bomen_incidenten.filter(
        pl.col("Total_Incidents") == 0
    ).drop("wijkcode", "Total_Incidents")
    logger.info(selection_eq_0.shape)
    logger.info(selection_eq_0.head(5))

    # SELECT ONLY ROWS WITH INCIDENTS> 0
    selection_gt_0 = wijken_weer_bomen_incidenten.filter(
        pl.col("Total_Incidents") > 0
    ).drop("wijkcode")
    logger.info(selection_gt_0.shape)
    logger.info(selection_gt_0.head(5))
    # 'EXPLODE' THOSE ROWS SO EACH ROW REPRESENTS AN Type of Inicdents
    selection_gt_0_explode = selection_gt_0.select(
        pl.exclude("Total_Incidents").repeat_by("Total_Incidents").explode()
    )
    logger.info(selection_gt_0_explode.shape)
    logger.info(selection_gt_0_explode.head(5))

    # CONCAT TO COMPLETE DF
    final_df = pl.concat([selection_gt_0_explode, selection_eq_0])
    logger.info(final_df.shape)
    logger.info(final_df.head(5))

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(final_df.columns)),
            "preview": MetadataValue.md(final_df.head().to_pandas().to_markdown()),
        }
    )
    return final_df
