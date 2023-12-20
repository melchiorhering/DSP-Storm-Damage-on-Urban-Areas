from dagster import (
    AssetExecutionContext,
    AssetIn,
    asset,
    MetadataValue,
)
import polars as pl


@asset(
    name="cleaned_knmi_weather_data",
    ins={"knmi_weather_data": AssetIn(key="knmi_weather_data")},
    key_prefix="cleaned",
    io_manager_key="database_io_manager",
    description="Transform str (datetime) to date format",
)
def cleaned_knmi_weather_data(
    context: AssetExecutionContext, knmi_weather_data: pl.DataFrame
) -> pl.DataFrame:
    """
    Set the data types of the KNMI data
    :return pl.DataFrame: with adjusted data-types
    """

    # Change datetime-string to datetime format
    df = knmi_weather_data.with_columns(
        pl.col("date").str.to_date("%+"), pl.col("hour").cast(pl.Int8)
    )  # or .to_datetime("%+")

    # Capitalize col names
    df.columns = [col.capitalize() for col in df.columns]

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )
    return df


@asset(
    name="cleaned_storm_incidents",
    ins={"storm_incidents": AssetIn(key="storm_incidents")},
    key_prefix="cleaned",
    io_manager_key="database_io_manager",
    description="Retrieve (time) features from storm_incidents",
)
def cleaned_storm_incidents(
    context: AssetExecutionContext, storm_incidents: pl.DataFrame
) -> pl.DataFrame:
    """
    Retrieve features from storm_incident
    :return pl.DataFrame: storm_incidents with retrieved features
    """

    df = storm_incidents.with_columns(
        [
            pl.col("Incident_Starttime").dt.hour().alias("Incident_Starttime_Hour"),
            pl.col("Incident_Endtime").dt.hour().alias("Incident_Endtime_Hour"),
            pl.col("Incident_Duration").dt.hour().alias("Incident_Duration_Hour"),
            pl.col("Incident_Starttime").dt.minute().alias("Incident_Starttime_Minute"),
            pl.col("Incident_Endtime").dt.minute().alias("Incident_Endtime_Minute"),
            pl.col("Incident_Duration").dt.minute().alias("Incident_Duration_Minute"),
        ]
    )  # or .to_datetime("%+")

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )
    return df
