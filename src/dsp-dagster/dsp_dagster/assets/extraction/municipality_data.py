import asyncio

import geopandas as gpd
import httpx
import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from pydantic import Field
from shapely import wkt


def create_url(endpoint: str) -> str:
    """
    "Creates the final API url based on base-url and given endpoint"

    :param str endpoint: Given endpoint
    :return str: final API url
    """
    return f"https://api.data.amsterdam.nl{endpoint}"


class GemeenteAmsterdamAPI(Config):
    fmt: str = Field(
        title="Output-Format",
        description="Sets the output format of the API-endpoint",
        default="geojson",
    )
    count: str = Field(
        title="Give-Data-Count",
        description="Boolean (str) for returning the total number of items in the dataset and the total amount of pages (based on `_pageSize` value)",
        default="true",
    )

    pageSize: int = Field(
        title="Page-Size",
        description="Sets page size for the returned data",
        default=5000,
    )


class Trees(GemeenteAmsterdamAPI):
    url: str = Field(
        title="Bomen-API-url",
        description="Endpoint used for retrieving `Bomen` data; information can be found here: https://api.data.amsterdam.nl/v1/docs/datasets/bomen.html",
        default_factory=lambda: create_url("/v1/bomen/stamgegevens/"),
    )


class Grond(GemeenteAmsterdamAPI):
    url: str = Field(
        title="Grond-API-url",
        description="Endpoint used for retrieving `Grond` data; information can be found here: https://api.data.amsterdam.nl/v1/docs/datasets/bodem.html",
        default_factory=lambda: create_url("/v1/bodem/grond/"),
    )


class GrondWater(GemeenteAmsterdamAPI):
    url: str = Field(
        title="Grond Water-API-url",
        description="Endpoint used for retrieving `Grondwater` data; information can be found here: https://api.data.amsterdam.nl/v1/docs/datasets/bodem.html#grondwater",
        default_factory=lambda: create_url("/v1/bodem/grondwater/"),
    )


def convert_to_polars(gdf: gpd.GeoDataFrame) -> pl.DataFrame:
    """
    Convert a GeoDataFrame to a Polars DataFrame.
    """
    # Efficiently convert geometries to string if needed
    gdf["geometry"] = gdf["geometry"].apply(
        wkt.dumps
    )  # Consider if this step is necessary
    return pl.from_pandas(pd.DataFrame(gdf))


async def fetch_data(session, logger, url, params, page):
    """
    Asynchronously fetch data for a specific page.
    """
    response = await session.get(url, params={**params, "page": page})
    if response.status_code != 200:
        raise Failure(
            description=f"Received non-200 status code [{response.status_code}]",
            metadata={
                "api_url": MetadataValue.url(str(response.url)),
                "errored_page": MetadataValue.int(page),
                "params": MetadataValue.json(params),
            },
        )
    logger.info(f"[PAGE] {page} | [RETRIEVED]")
    return response.json()


async def fetch_data_chunk(session, logger, url, params, start_page, end_page):
    """
    Fetch a chunk of pages and aggregate their features.
    """
    tasks = [
        fetch_data(session, logger, url, params, page)
        for page in range(start_page, end_page + 1)
    ]
    chunk_results = await asyncio.gather(*tasks)

    # Aggregate features from each page's FeatureCollection
    aggregated_features = []
    for chunk in chunk_results:
        if chunk.get("type") == "FeatureCollection":
            aggregated_features.extend(chunk.get("features", []))

    return aggregated_features


async def fetch_all_data(
    session, logger, url, params, total_pages, initial_data, chunk_size=20, delay=10
):
    """
    Fetch all pages of data in chunks with a delay between each chunk, starting from the second page.
    :param initial_data: The data fetched from the first page.
    """
    feature_collection = (
        initial_data if initial_data.get("type") == "FeatureCollection" else None
    )

    if not feature_collection:
        raise ValueError("Initial data is not a FeatureCollection.")
    else:
        for start_page in range(2, total_pages + 1, chunk_size):  # Start from page 2
            end_page = min(start_page + chunk_size - 1, total_pages)
            logger.info(f"Fetching pages {start_page} to {end_page}")
            data_chunk = await fetch_data_chunk(
                session, logger, url, params, start_page, end_page
            )

            # Append features from each chunk to the initial FeatureCollection
            for chunk in data_chunk:
                if chunk.get("type") == "FeatureCollection":
                    feature_collection["features"].extend(chunk.get("features", []))

            # Wait for a specified delay time before fetching the next chunk
            if end_page < total_pages:
                await asyncio.sleep(delay)

        return feature_collection


@asset(
    name="tree_data",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
async def tree_data(context: AssetExecutionContext, config: Trees) -> pl.DataFrame:
    """
    Asynchronously retrieves tree data from the Gemeente Amsterdam API
    and converts it to a Polars DataFrame.
    """
    logger = get_dagster_logger()
    params = {
        "_format": config.fmt,
        "_count": config.count,
        "_pageSize": config.pageSize,
    }
    logger.info(f"Using {params}")

    async with httpx.AsyncClient(timeout=460) as session:
        initial_response = await session.get(config.url, params=params)
        if initial_response.status_code != 200:
            raise Failure(
                description=f"Received non-200 status code [{initial_response.status_code}]",
                metadata={
                    "api_url": MetadataValue.url(str(initial_response.url)),
                    "params": MetadataValue.json(params),
                },
            )

        total_pages = int(initial_response.headers["x-pagination-count"])
        logger.info(f"Total pages: {total_pages}")

        initial_data = initial_response.json()
        all_data = await fetch_all_data(
            session, logger, config.url, params, total_pages, initial_data
        )

        gdf = gpd.GeoDataFrame.from_features(all_data, crs="EPSG:4326")

        df = convert_to_polars(gdf)

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )

    return df


@asset(
    name="grond_data",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
async def grond_data(context: AssetExecutionContext, config: Grond) -> pl.DataFrame:
    """
    Asynchronously retrieves tree data from the Gemeente Amsterdam API
    and converts it to a Polars DataFrame.
    """
    logger = get_dagster_logger()
    params = {
        "_format": config.fmt,
        "_count": config.count,
        "_pageSize": config.pageSize,
    }
    logger.info(f"Using {params}")

    async with httpx.AsyncClient(timeout=460) as session:
        initial_response = await session.get(config.url, params=params)
        if initial_response.status_code != 200:
            raise Failure(
                description=f"Received non-200 status code [{initial_response.status_code}]",
                metadata={
                    "api_url": MetadataValue.url(str(initial_response.url)),
                    "params": MetadataValue.json(params),
                },
            )

        total_pages = int(initial_response.headers["x-pagination-count"])
        logger.info(f"Total pages: {total_pages}")

        initial_data = initial_response.json()
        all_data = await fetch_all_data(
            session, logger, config.url, params, total_pages, initial_data
        )

        gdf = gpd.GeoDataFrame.from_features(all_data, crs="EPSG:4326")
        df = convert_to_polars(gdf)

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
        }
    )

    return df
