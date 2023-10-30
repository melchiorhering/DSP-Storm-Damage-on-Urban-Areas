from dagster import asset, AssetExecutionContext, MetadataValue
import polars as pl


@asset(
    name="storm_data_small",
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


@asset(
    name="storm_data_large",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_large(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local Storm data (large dataset)

    :return pl.DataFrame: Large Storm Dataset
    """
    df = pl.read_excel(
        "./data/Storm_Data_Large.xlsx", read_csv_options={"infer_schema_length": 10000}
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
    df = pl.read_excel("./data/Storm_Data_Deployments.xlsx")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    return df


@asset(
    name="fire_department_stations",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def kazernes(context: AssetExecutionContext) -> pl.DataFrame:
    """
    Loads local data about 'Kazernes'

    :return pl.DataFrame: Kazernes Dataset
    """
    df = pl.read_excel("./data/Kazernes.xlsx")

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
    df = pl.read_excel("./data/Fire_Stations_and_Vehicles.xlsx")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df
