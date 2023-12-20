from dagster import (
    AssetExecutionContext,
    Config,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from pydantic import Field
from requests import request
from urllib.parse import quote
import geopandas as gpd
import pandas as pd
import polars as pl
from shapely.wkt import dumps


def create_url(endpoint: str) -> str:
    """
    "Creates the final API url based on base-url and given endpoint"
    WFS Documentatie: https://docs.geoserver.org/latest/en/user/services/wfs/reference.html#getfeature
    Meer Documentatie:https://pdok-ngr.readthedocs.io/services.html#web-feature-service-wfs"

    :param str endpoint: Given endpoint
    :return str: final API url
    """
    return f"https://service.pdok.nl{endpoint}"


class PDOK(Config):
    request: str = Field(
        title="Request Type",
        description="What to request: GetFeature, GetCapabilities etc",
        examples=["GetFeature", "GetCapabilities", "DescribeFeatureType"],
        default="GetFeature",
    )
    service: str = Field(
        title="Service Type",
        default="WFS",
    )
    version: str = Field(
        title="Version",
        default="1.1.0",
    )
    outputFormat: str = Field(
        title="Output-Format",
        examples=["application/json; subtype=geojson", "application/json"],
        default="application/json",
    )
    srsName: str = Field(
        title="CRS",
        examples=["EPSG:4326", "EPSG::28992"],
        default="EPSG:4326",
    )


class PDOK_BAG(PDOK):
    bag_url: str = Field(
        title="PDOK BAG Web Feature Service Endpoint",
        default_factory=lambda: create_url("/lv/bag/wfs/v2_0"),
    )
    typeName: str = Field(
        title="Type-Name",
        description="What type of BAG data to request",
        examples=[
            "bag:pand",
            "bag:ligplaats",
            "bag:woonplaats",
            "bag:standplaats",
            "bag:verbijlsobject",
        ],
        default="bag:pand",
    )


class PDOK_CBS(PDOK):
    cbs_url: str = Field(
        title="PDOK CBS Web Feature Service Endpoint",
        default_factory=lambda: create_url("/cbs/wijkenbuurten/2022/wfs/v1_0"),
    )
    # typeName: str = Field(
    #     title="Type-Name",
    #     description="What type of BAG data to request",
    #     examples=["wijkenbuurten:wijken", "wijkenbuurten:buurten"],
    # )


@asset(
    name="bag_panden",
    io_manager_key="database_io_manager",
)
def bag_panden(context, config: PDOK_BAG) -> pl.DataFrame:
    """
    Retrieve data from the PDOK BAG Web Feature Service (WFS)
    returns pl.DataFrame: A Table with geospatial data based on BAG-Panden
    """

    logger = get_dagster_logger()
    params = {
        "request": config.request,
        "service": config.service,
        "version": config.version,
        "outputFormat": config.outputFormat,
        "typeName": config.typeName,
        "srsName": config.srsName,
    }

    response = request("GET", config.bag_url, params=params, timeout=240)
    logger.info(response.url)

    if response.status_code == 200:
        data = response.json()
        # Read Feature Collection to Geopandas df, then transform to Pandas
        gdf = gpd.GeoDataFrame.from_features(data)
        gdf["geometry"] = gdf["geometry"].apply(dumps)
        logger.info(gdf.head())
        df = pl.from_pandas(pd.DataFrame(gdf))

        context.add_output_metadata(
            metadata={
                # "metadata": MetadataValue.json(data),
                # "describe": MetadataValue.md(df.describe().to_markdown()),
                "number_of_columns": MetadataValue.int(len(df.columns)),
                "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
                # The `MetadataValue` class has useful static methods to build Metadata
            }
        )

        return df
    response.raise_for_status()


@asset(
    name="cbs_wijken",
    io_manager_key="database_io_manager",
)
def cbs_wijken(context: AssetExecutionContext, config: PDOK_CBS) -> pl.DataFrame:
    """
    Retrieve data from the PDOK Web Feature Service (WFS)
    WFS Documentatie: https://docs.geoserver.org/latest/en/user/services/wfs/reference.html#getfeature
    Meer Documentatie: https://pdok-ngr.readthedocs.io/services.html#web-feature-service-wfs
    Documentatie over velden: https://www.cbs.nl/nl-nl/longread/diversen/2022/toelichting-wijk-en-buurtkaart-2020-2021-en-2022?onepage=true

    returns pl.DataFrame: A Table with geospatial data based on CBS-Wijken
    """
    logger = get_dagster_logger()

    # Construct the filter XML on Gemeente Amsterdam
    filter_xml = """
    <Filter>
        <PropertyIsEqualTo>
            <PropertyName>gemeentenaam</PropertyName>
            <Literal>Amsterdam</Literal>
        </PropertyIsEqualTo>
    </Filter>
    """

    # URL-encode the filter
    encoded_filter = quote(filter_xml)

    # Set up request parameters, including the filter
    params = {
        "request": config.request,
        "service": config.service,
        "version": config.version,
        "outputFormat": config.outputFormat,
        "typeName": "wijkenbuurten:wijken",
        "srsName": config.srsName,
        "filter": filter_xml,  # Add the encoded filter here
    }
    response = request("GET", config.cbs_url, params=params, timeout=240)
    logger.info(response.url)

    if response.status_code == 200:
        data = response.json()

        # Read Feature Collection to Geopandas df, then transform to Pandas
        gdf = gpd.GeoDataFrame.from_features(data)
        gdf["geometry"] = gdf["geometry"].apply(dumps)
        logger.info(gdf.head())
        df = pl.from_pandas(pd.DataFrame(gdf))

        context.add_output_metadata(
            metadata={
                # "metadata": MetadataValue.json(data),
                # "url_used": MetadataValue.text(response.url),
                # "describe": MetadataValue.md(df.describes().to_markdown()),
                "number_of_columns": MetadataValue.int(len(df.columns)),
                "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
                # The `MetadataValue` class has useful static methods to build Metadata
            }
        )
        return df

    response.raise_for_status()


@asset(
    name="cbs_buurten",
    io_manager_key="database_io_manager",
)
def cbs_buurten(context: AssetExecutionContext, config: PDOK_CBS) -> pl.DataFrame:
    """
    Retrieve data from the PDOK Web Feature Service (WFS)
    WFS Documentatie: https://docs.geoserver.org/latest/en/user/services/wfs/reference.html#getfeature
    Meer Documentatie: https://pdok-ngr.readthedocs.io/services.html#web-feature-service-wfs
    Documentatie over velden: https://www.cbs.nl/nl-nl/longread/diversen/2022/toelichting-wijk-en-buurtkaart-2020-2021-en-2022?onepage=true

    returns pl.DataFrame: A Table with geospatial data based on CBS-Wijken
    """
    logger = get_dagster_logger()

    # Construct the filter XML on Gemeente Amsterdam
    filter_xml = """
    <Filter>
        <PropertyIsEqualTo>
            <PropertyName>gemeentenaam</PropertyName>
            <Literal>Amsterdam</Literal>
        </PropertyIsEqualTo>
    </Filter>
    """

    # URL-encode the filter
    encoded_filter = quote(filter_xml)

    # Set up request parameters, including the filter
    params = {
        "request": config.request,
        "service": config.service,
        "version": config.version,
        "outputFormat": config.outputFormat,
        "typeName": "wijkenbuurten:buurten",
        "srsName": config.srsName,
        "filter": filter_xml,  # Add the encoded filter here
    }
    response = request("GET", config.cbs_url, params=params, timeout=240)
    logger.info(response.url)

    if response.status_code == 200:
        data = response.json()

        # Read Feature Collection to Geopandas df, then transform to Pandas
        gdf = gpd.GeoDataFrame.from_features(data)
        gdf["geometry"] = gdf["geometry"].apply(dumps)
        logger.info(gdf.head())
        df = pl.from_pandas(pd.DataFrame(gdf))

        context.add_output_metadata(
            metadata={
                # "metadata": MetadataValue.json(data),
                # "url_used": MetadataValue.text(response.url),
                # "describe": MetadataValue.md(df.describes().to_markdown()),
                "number_of_columns": MetadataValue.int(len(df.columns)),
                "preview": MetadataValue.md(df.to_pandas().head().to_markdown()),
                # The `MetadataValue` class has useful static methods to build Metadata
            }
        )
        return df

    response.raise_for_status()
