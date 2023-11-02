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


class GemeenteAmsterdamAPI(Config):
    base_url: str = Field(
        title="Gemeente-Amsterdam-Base-URL",
        description="The base url for retrieving data from gemeente Amsterdam API endpoints",
        default="https://api.data.amsterdam.nl",
    )


class GA_Bomen(GemeenteAmsterdamAPI):
    bomen_endpoint: str = Field(
        title="Bomen-Endpoint",
        description="Endpoint used for retrieving `Bomen` data; information can be found here: https://api.data.amsterdam.nl/v1/docs/datasets/bomen.html, https://api.data.amsterdam.nl/v1/bomen/stamgegevens/",
        default="/v1/bomen/stamgegevens/",
    )
    fmt: str = Field(
        title="Output-Format",
        description="Sets the output format of the API-endpoint",
        default="json",
    )
    count: str = Field(
        title="Give-Data-Count",
        description="Boolean (str) for returning the total number of trees in the dataset and the total amount of pages (based on `_pageSize` value)",
        default="true",
    )

    pageSize: int = Field(
        title="Page-Size",
        description="Sets page size for the returned data",
        default=5000,
    )


async def fetch_data(
    session, logger, complete_endpoint, params, current_page, total_pages
):
    params["page"] = current_page
    response = await session.get(complete_endpoint, params=params, timeout=60)
    if response.status_code == 200:
        logger.info(f"Page --> {current_page} | {total_pages}")

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
    name="gemeente_ams_tree_data",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
async def get_gemeente_ams_tree_data(
    context: AssetExecutionContext, config: GA_Bomen
) -> pl.DataFrame:
    logger = get_dagster_logger()
    complete_endpoint = config.base_url + config.bomen_endpoint

    params = {
        "_format": config.fmt,
        "_count": config.count,
        "_pageSize": config.pageSize,
    }

    logger.info(f"Using {params}")

    async with httpx.AsyncClient() as session:
        response = await session.get(complete_endpoint, params=params, timeout=60)
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
                fetch_data(
                    session, logger, complete_endpoint, params, page, total_pages
                )
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
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )

    return df


# OLD
# @asset(
#     name="gemeente_ams_tree_data",
#     io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
# )
# def get_gemeente_ams_tree_data(
#     context: AssetExecutionContext, config: GA_Bomen
# ) -> pl.DataFrame:
#     logger = get_dagster_logger()

#     complete_endpoint = config.base_url + config.bomen_endpoint

#     params = {
#         "_format": config.fmt,
#         "_count": config.count,
#         "_pageSize": config.pageSize,
#     }

#     logger.info(f"Using {params}")
#     response = requests.request("GET", complete_endpoint, params=params, timeout=60)

#     if response.status_code == 200:
#         logger.info(response.headers)
#         current_page = int(response.headers["x-pagination-page"])
#         total_count = int(response.headers["x-total-count"])
#         total_pages = int(response.headers["x-pagination-count"])

#         logger.info(f"Total number of data (trees): {total_count}")
#         logger.info(
#             f"Using _pageSize={params['_pageSize']} will result in {total_pages} pages"
#         )

#         data = response.json()
#         final_data = data["_embedded"]["stamgegevens"]

#         while current_page <= total_pages:
#             logger.info(f"{current_page} | {total_pages} pages retrieved")
#             current_page += 1
#             params["page"] = current_page
#             response = requests.request(
#                 "GET", complete_endpoint, params=params, timeout=60
#             )
#             if response.status_code == 200:
#                 data = response.json()
#                 final_data += data["_embedded"]["stamgegevens"]
#             else:
#                 raise Failure(
#                     description=f"Received non-200 status code [{response.status_code}]",
#                     metadata={
#                         "api_url": MetadataValue.url(complete_endpoint),
#                         "total-pages": MetadataValue.text(total_pages),
#                         "total-count": MetadataValue.text(total_count),
#                         "errored_page": MetadataValue.int(current_page),
#                         "params": MetadataValue.json(params),
#                     },
#                 )
#     else:
#         raise Failure(
#             description=f"Received non-200 status code [{response.status_code}]",
#             metadata={
#                 "api_url": MetadataValue.url(complete_endpoint),
#                 "total-pages": MetadataValue.text(total_pages),
#                 "total-count": MetadataValue.text(total_count),
#                 "errored_page": MetadataValue.int(current_page),
#                 "params": MetadataValue.json(params),
#             },
#         )

#     df = pl.from_dicts(final_data)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(df),  # Metadata can be any key-value pair
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#             # The `MetadataValue` class has useful static methods to build Metadata
#         }
#     )
#     return df
