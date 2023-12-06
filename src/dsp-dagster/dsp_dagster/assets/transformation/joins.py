from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    asset,
)
import polars as pl


@asset(
    name="deployment_incident",
    key_prefix="joined",
    ins={
        "storm_deployments": AssetIn(key="storm_deployments"),
        "storm_incidents": AssetIn(key="storm_incidents"),
    },
    io_manager_key="database_io_manager",
    description="Join storm_deployments on storm_incidents based on  Incident_ID from the storm_deployments table",
)
def deployment_incident(
    context: AssetExecutionContext,
    storm_incidents: pl.DataFrame,
    storm_deployments: pl.DataFrame,
) -> pl.DataFrame:
    """
    Join `storm_deployments` on `storm_incidents` based on  `Incident_ID` from the `storm_deployments` table
    :param AssetExecutionContext context: Dagster context
    :param pl.DataFrame storm_deployments: DataFrame 1
    :param pl.DataFrame storm_incidents: DataFrame 2
    :return pl.DataFrame: Combined DataFrame
    """

    df = storm_incidents.join(storm_deployments, on="Incident_ID")
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
    :return pl.DataFrame: Fire Department Data Table
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
    :return pl.DataFrame: Fire Department Data Table
    """

    df = deployment_incident_vehicles.join(adjust_knmi_data_types, left_on="Date", right_on="date", how="left")



    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df
