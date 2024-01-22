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
        default=2000,
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
    Fetch a chunk of pages.
    """
    tasks = [
        fetch_data(session, logger, url, params, page)
        for page in range(start_page, end_page + 1)
    ]
    chunk_results = await asyncio.gather(*tasks)
    return [item for sublist in chunk_results for item in sublist]


async def fetch_all_data(
    session, logger, url, params, total_pages, chunk_size=15, delay=10
):
    """
    Fetch all pages of data in chunks with a delay between each chunk.
    """
    all_data = []
    for start_page in range(1, total_pages + 1, chunk_size):
        end_page = min(start_page + chunk_size - 1, total_pages)
        logger.info(f"Fetching pages {start_page} to {end_page}")
        data_chunk = await fetch_data_chunk(
            session, logger, url, params, start_page, end_page
        )
        all_data.extend(data_chunk)

        # Wait for a specified delay time before fetching the next chunk
        if end_page < total_pages:
            await asyncio.sleep(delay)

    return all_data


def convert_to_geopandas(data):
    """
    Convert data to a GeoPandas DataFrame and perform transformations.
    """
    gdf = gpd.GeoDataFrame.from_features(data, crs="EPSG:28992").to_crs("EPSG:4326")

    print(gdf.columns)
    print(gdf.head())
    gdf["latitude"] = gdf.geometry.y
    gdf["longitude"] = gdf.geometry.x
    gdf.drop(columns=["geometry"], inplace=True)
    return gdf


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

    async with httpx.AsyncClient(verify=False, timeout=460) as session:
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

        all_data = await fetch_all_data(
            session, logger, config.url, params, total_pages
        )

        gdf = convert_to_geopandas(all_data)
        pdf = pd.DataFrame(gdf)
        pl_df = pl.DataFrame(pdf)

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(pdf.describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(pl_df.columns)),
            "preview": MetadataValue.md(pl_df.head().to_pandas().to_markdown()),
        }
    )

    return pl_df


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

    async with httpx.AsyncClient(verify=False, timeout=460) as session:
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

        all_data = await fetch_all_data(
            session, logger, config.url, params, total_pages
        )

        gdf = convert_to_geopandas(all_data)
        pdf = pd.DataFrame(gdf)
        pl_df = pl.DataFrame(pdf)

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(pdf.describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(pl_df.columns)),
            "preview": MetadataValue.md(pl_df.head().to_pandas().to_markdown()),
        }
    )

    return pl_df
