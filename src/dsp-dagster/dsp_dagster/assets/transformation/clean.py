from dagster import AssetExecutionContext, asset, AssetIn, MetadataValue

import polars as pl

# knmi_weather_data = SourceAsset(key=AssetKey("knmi_weather_data"))


@asset(
    name="set_dtypes",
    ins={"knmi_weather_data": AssetIn("knmi_weather_data")},
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_small(
    context: AssetExecutionContext, knmi_weather_data: pl.DataFrame
) -> pl.DataFrame:
    """
    Loads local Storm data (small dataset)

    :return pl.DataFrame: Small Storm Dataset
    """

    df = knmi_weather_data.with_columns(pl.col("date").str.to_datetime("%+"))

    context.add_output_metadata(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df
