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


@asset(
    name="aggregated_buurten_trees",
    key_prefix="joined",
    ins={
        "buurten_trees": AssetIn(key="buurten_trees"),
    },
    io_manager_key="database_io_manager",
    description="Aggregate the buurten_trees table to create new features",
)
def aggregated_buurten_trees(
    context: AssetExecutionContext,
    buurten_trees: pl.DataFrame,
) -> pl.DataFrame:
    """
    Aggregate the buurten_trees data to create new features

    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame buurten_trees: Tree data added to CBS buurten
    :return pl.DataFrame: resulting joined table
    """
    logger = get_dagster_logger()

    # List of columns from trees_data
    columns_to_pivot = [
        "boomhoogteklasseActueel",
        "typeObject",
        "soortnaamTop",
        "standplaatsGedetailleerd",
        "stamdiameterklasse",
    ]

    df = buurten_trees.group_by("buurtcode").agg(pl.col("id").count().alias("Totaal"))

    for i, column in enumerate(columns_to_pivot):
        temp_df = (
            buurten_trees.group_by(["buurtcode", column])
            .agg(pl.col(column).count().alias(f"{column}CntTotal"))
            .drop_nulls()
        )

        # display(temp_df)
        pivot_df = temp_df.pivot(
            index="buurtcode", columns=column, values=f"{column}CntTotal"
        ).fill_null(0)

        pivot_df = pivot_df.rename(
            {
                col: f"Trees_{column}_{'-'.join(col.split(' '))}"
                for col in pivot_df.columns
                if col != "buurtcode"
            }
        )
        df = df.join(pivot_df, on="buurtcode", how="left").fill_null(0)
        logger.info(df.head(5))

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df


@asset(
    name="aggregated_buurten_incidents",
    key_prefix="joined",
    ins={
        "buurten_incidents": AssetIn(key="buurten_incidents"),
    },
    io_manager_key="database_io_manager",
    description="Aggregate the buurten_incidents table to create new features",
)
def aggregated_buurten_incidents(
    context: AssetExecutionContext,
    buurten_incidents: pl.DataFrame,
) -> pl.DataFrame:
    """
    Aggregate the buurten_incidents data to create new features

    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame buurten_incidents: Tree data added to CBS buurten
    :return pl.DataFrame: resulting joined table
    """
    # logger = get_dagster_logger()

    df = buurten_incidents.group_by(
        [
            "buurtcode",
            "Date",
            "Incident_Starttime_Hour",
            "Damage_Type",
        ]
    ).agg(pl.col("Incident_ID").count().alias("Totaal"))

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df
