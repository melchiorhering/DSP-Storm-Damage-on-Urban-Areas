from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
)
import polars as pl


@asset(
    name="storm_incidents",
    # key_prefix="local_extraction",
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
    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            "max_date": MetadataValue.text(
                df.select("Date").max().to_pandas().iloc[0, 0].strftime("%Y-%m-%d")
            ),
            "min_date": MetadataValue.text(
                df.select("Date").min().to_pandas().iloc[0, 0].strftime("%Y-%m-%d")
            )
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df


@asset(
    name="storm_deployments",
    # key_prefix="local_extraction",
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
    # key_prefix="local_extraction",
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
