import geopandas as gpd
import pandas as pd
import polars as pl
from dagster import AssetExecutionContext, MetadataValue, asset
from shapely import wkt


@asset(
    name="storm_incidents",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_incidents(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local Storm Incidents

    :return pl.DataFrame: Storm Incidents Dataset
    """
    df = pl.read_csv(
        "./data/Storm_Data_Incidents.csv",
        use_pyarrow=True,
        null_values=["NULL"],
        try_parse_dates=True,
    )

    df = df.to_pandas()

    # Read LON and LAT
    df["geometry"] = gpd.points_from_xy(df["LON"], df["LAT"])
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326").drop(
        columns=["LON", "LAT"]
    )
    gdf["geometry"] = gdf["geometry"].apply(wkt.dumps)
    df = pl.from_pandas(pd.DataFrame(gdf))

    df = df.with_columns((pl.col("Date").dt.date()).alias("Incident_Date")).drop("Date")

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            "max_date": MetadataValue.text(
                df.select("Incident_Date")
                .max()
                .to_pandas()
                .iloc[0, 0]
                .strftime("%Y-%m-%d")
            ),
            "min_date": MetadataValue.text(
                df.select("Incident_Date")
                .min()
                .to_pandas()
                .iloc[0, 0]
                .strftime("%Y-%m-%d")
            ),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df


@asset(
    name="storm_deployments",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_deployments(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local storm deployment dataset

    :return pl.DataFrame: Deployment Dataset
    """
    df = pl.read_csv("./data/Storm_Data_Deployments.csv")
    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df


@asset(
    name="fire_stations_and_vehicles",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def fire_stations_and_vehicles(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local dataset about vehicles and their default fire-station

    :return pl.DataFrame: Fire Brigade Vehicles Dataset
    """
    df = pl.read_csv("./data/Fire_Stations_and_Vehicles.csv")

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df


@asset(
    name="service_areas",
    io_manager_key="database_io_manager",
)
def service_areas(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads the service areas and their homebases

    :return pl.DataFrame: Table with Service Area geometry and linked homebase
    """
    df = pl.read_csv(
        "./data/Service_Areas.csv",
    ).to_pandas()

    # Read Feature Collection to Geopandas df, then transform to Pandas
    df["geometry"] = gpd.GeoSeries.from_wkt(df["geomtext"], crs="EPSG:28992")
    gdf = gpd.GeoDataFrame(df, geometry="geometry")
    gdf = gdf.to_crs("EPSG:4326").drop(columns=["geomtext", "geom"])
    gdf["geometry"] = gdf["geometry"].apply(wkt.dumps)
    df = pl.from_pandas(pd.DataFrame(gdf))

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(
                df.drop("geometry").to_pandas().head().to_markdown()
            ),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df
