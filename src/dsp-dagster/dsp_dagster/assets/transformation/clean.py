from dagster import (
    AssetExecutionContext,
    AssetIn,
    asset,
    MetadataValue,
)
import polars as pl


@asset(
    name="adjust_knmi_data_types",
    ins={"knmi_weather_data": AssetIn(key="knmi_weather_data")},
    key_prefix="cleaned",
    io_manager_key="database_io_manager",
    description="Transform str (datetime) to date format",
)
def adjust_knmi_data_types(
    context: AssetExecutionContext, knmi_weather_data: pl.DataFrame
) -> pl.DataFrame:
    """
    Set the data types of the KNMI data
    :return pl.DataFrame: with adjusted data-types
    """

    df = knmi_weather_data.with_columns(
        pl.col("date").str.to_date("%+")
    )  # or .to_datetime("%+")

    context.add_output_metadata(
        metadata={
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )
    return df
