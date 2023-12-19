from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
    Failure,
)
import polars as pl
import httpx
import asyncio
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
        default="json",
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
        description="Endpoint used for retrieving `Grondwater` data; information can be found here: https://api.data.amsterdam.nl/v1/docs/datasets/bodem.html",
        default_factory=lambda: create_url("/v1/bodem/grondwater/"),
    )


async def fetch_data(
    session, logger, complete_endpoint, params, current_page, total_pages
):
    params["page"] = current_page
    response = await session.get(complete_endpoint, params=params, timeout=180)
    if response.status_code == 200:
        logger.info(f"[PAGE] {current_page} | {total_pages} [RETRIEVED]")

        data = response.json()
        return data["_embedded"]["stamgegevens"]

    raise Failure(
        description=f"Received non-200 status code [{response.status_code}]",
        metadata={
            "api_url": MetadataValue.url(str(response.url)),
            "errored_page": MetadataValue.int(current_page),
            "params": MetadataValue.json(params),
        },
    )


@asset(
    name="tree_data",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
async def tree_data(context: AssetExecutionContext, config: Trees) -> pl.DataFrame:
    """
    Function that retrieves tree data from the Gemeente Amsterdam API

    :param AssetExecutionContext context: Dagster context
    :param GA_Bomen config: Parmas for the `Bomen` API
    :raises Failure: Failure when the async function fails
    :return pl.DataFrame: Amsterdam Trees dataset
    """
    logger = get_dagster_logger()

    params = {
        "_format": config.fmt,
        "_count": config.count,
        "_pageSize": config.pageSize,
    }

    logger.info(f"Using {params}")

    async with httpx.AsyncClient() as session:
        response = await session.get(config.url, params=params, timeout=180)
        if response.status_code == 200:
            logger.info(response.headers)
            current_page = int(response.headers["x-pagination-page"])
            total_count = int(response.headers["x-total-count"])
            total_pages = int(response.headers["x-pagination-count"])

            logger.info(f"Total number of data (trees): {total_count}")
            logger.info(
                f"Using _pageSize={params['_pageSize']} will result in {total_pages} pages"
            )

            data = response.json()
            final_data = data["_embedded"]["stamgegevens"]

            tasks = [
                fetch_data(session, logger, config.url, params, page, total_pages)
                for page in range(2, total_pages + 1)
            ]

            fetched_data = await asyncio.gather(*tasks)

            for data in fetched_data:
                final_data.extend(data)
        else:
            raise Failure(
                description=f"Received non-200 status code [{response.status_code}]",
                metadata={
                    "api_url": MetadataValue.url(str(response.url)),
                    "total-pages": MetadataValue.text(total_pages),
                    "total-count": MetadataValue.text(total_count),
                    "errored_page": MetadataValue.int(current_page),
                    "params": MetadataValue.json(params),
                },
            )

    df = pl.from_dicts(final_data)
    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(df.to_pandas().describe().to_markdown()),
            "number_of_columns": MetadataValue.int(len(df.columns)),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df
