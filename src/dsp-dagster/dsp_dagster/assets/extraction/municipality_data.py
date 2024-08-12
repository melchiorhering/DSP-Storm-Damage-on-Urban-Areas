import asyncio

import geopandas as gpd
import httpx
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

from ...util.helpers import *


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


async def fetch_data(session, logger, url, params, page):
    """
    Asynchronously fetch data for a specific page.
    """
    try:
        response = await session.get(url, params={**params, "page": page})
        response.raise_for_status()
        logger.info(f"Page {page} data retrieved.")
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
        raise Failure(
            description=f"HTTP error for page {page}: {e}",
            metadata={
                "api_url": MetadataValue.url(str(response.url)),
                "errored_page": MetadataValue.int(page),
                "params": MetadataValue.json(params),
            },
        )
    except httpx.RequestError as e:
        logger.error(f"Request error occurred: {e}")
        raise Failure(
            description=f"Request error for page {page}: {e}",
            metadata={
                "api_url": MetadataValue.url(url),
                "errored_page": MetadataValue.int(page),
                "params": MetadataValue.json(params),
            },
        )


async def fetch_data_chunk(session, logger, url, params, start_page, end_page):
    """
    Fetch a chunk of pages and aggregate their features.
    """
    tasks = [
        fetch_data(session, logger, url, params, page)
        for page in range(start_page, end_page + 1)
    ]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    aggregated_features = []
    for result in chunk_results:
        if isinstance(result, Exception):
            continue  # Skip failed tasks
        if result.get("type") == "FeatureCollection":
            aggregated_features.extend(result.get("features", []))

    return aggregated_features


async def fetch_all_data(
    session, logger, url, params, total_pages, initial_data, chunk_size=20, delay=10
):
    """
    Fetch all pages of data in chunks with a delay between each chunk.
    """
    if not initial_data.get("type") == "FeatureCollection":
        raise ValueError("Initial data is not a FeatureCollection.")

    feature_collection = initial_data

    for start_page in range(2, total_pages + 1, chunk_size):  # Start from page 2
        end_page = min(start_page + chunk_size - 1, total_pages)
        logger.info(f"Fetching pages {start_page} to {end_page}")

        data_chunk = await fetch_data_chunk(
            session, logger, url, params, start_page, end_page
        )
        feature_collection["features"].extend(data_chunk)

        if end_page < total_pages:
            logger.info(f"Waiting for {delay} seconds before next chunk.")
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

    logger = get_dagster_logger()
    params = {
        "_format": config.fmt,
        "_count": config.count,
        "_pageSize": config.pageSize,
    }
    logger.info(f"Using {params}")

    async with httpx.AsyncClient(timeout=460) as session:
        try:
            initial_response = await session.get(config.url, params=params)
            initial_response.raise_for_status()
            total_pages = int(initial_response.headers["x-pagination-count"])
            logger.info(f"Total pages: {total_pages}")

            initial_data = initial_response.json()

            logger.info(initial_data.keys())

            all_data = await fetch_all_data(
                session, logger, config.url, params, total_pages, initial_data
            )

            gdf = gpd.GeoDataFrame.from_features(all_data, crs="EPSG:4326")

            df = convert_to_polars(gdf)

            context.add_output_metadata(
                metadata={
                    "describe": MetadataValue.md(
                        df.to_pandas().describe().to_markdown()
                    ),
                    "number_of_columns": MetadataValue.int(len(df.columns)),
                    "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
                }
            )
            return df

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise Failure(
                description=f"HTTP error during initial request: {e}",
                metadata={
                    "api_url": MetadataValue.url(str(initial_response.url)),
                    "params": MetadataValue.json(params),
                },
            )
        except httpx.RequestError as e:
            logger.error(f"Request error occurred: {e}")
            raise Failure(
                description=f"Request error during initial request: {e}",
                metadata={
                    "api_url": MetadataValue.url(config.url),
                    "params": MetadataValue.json(params),
                },
            )


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
