from dagster import asset, AssetExecutionContext, MetadataValue
import polars as pl


@asset(
    name="join",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_small(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local Storm data (small dataset)

    :return pl.DataFrame: Small Storm Dataset
    """
    df = pl.read_excel("./data/Storm_Data_Small.xlsx")
    context.add_output_metadata(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df