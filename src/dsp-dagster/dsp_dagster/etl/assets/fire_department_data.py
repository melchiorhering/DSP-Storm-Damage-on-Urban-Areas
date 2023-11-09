from dagster import asset, AssetExecutionContext, MetadataValue
import polars as pl


@asset(
    name="storm_data_incidents",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_incidents(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local Storm Incidents

    :return pl.DataFrame: Large Storm Dataset
    """
    df = pl.read_csv(
        "./data/Storm_Data_Incidents.csv", null_values=["NULL"]
    )
    context.add_output_metadata(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df


@asset(
    name="storm_data_deployments",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_deployments(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local storm deployment dataset

    :return pl.DataFrame: Deployment Dataset
    """
    df = pl.read_csv("./data/Storm_Data_Deployments.csv")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
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
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df
