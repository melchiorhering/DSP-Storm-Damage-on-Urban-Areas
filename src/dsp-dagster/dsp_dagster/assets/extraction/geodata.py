import geopandas as gpd
import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    Config,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from pydantic import Field
from requests import request
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
        examples=["application/json subtype=geojson", "application/json"],
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


def fetch_wfs_data(url, params, logger):
    """
    Fetch data from a WFS endpoint.
    """
    response = request("GET", url, params=params, timeout=240)
    response.raise_for_status()
    logger.info(response.url)
    return response.json()


def convert_to_polars(gdf):
    """
    Convert a GeoDataFrame to a Polars DataFrame.
    """
    # Efficiently convert geometries to string if needed
    gdf["geometry"] = gdf["geometry"].apply(dumps)  # Consider if this step is necessary
    return pl.from_pandas(pd.DataFrame(gdf))


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

    data = fetch_wfs_data(config.bag_url, params, logger)
    gdf = gpd.GeoDataFrame.from_features(data)

    logger.info(gdf.head(10))
    df = convert_to_polars(gdf)
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
            <Or>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Zaanstad</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Diemen</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Ouder-Amstel</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Amstelveen</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Amsterdam</Literal>
                </PropertyIsEqualTo>
            </Or>
        </Filter>
        """

    # Set up request parameters, including the filter
    params = {
        "request": config.request,
        "service": config.service,
        "version": config.version,
        "outputFormat": config.outputFormat,
        "typeName": "wijkenbuurten:wijken",
        "srsName": config.srsName,
        "filter": filter_xml,  # URL-encode the filter
    }
    data = fetch_wfs_data(config.cbs_url, params, logger)
    gdf = gpd.GeoDataFrame.from_features(data)
    logger.info(gdf.head(10))
    df = convert_to_polars(gdf)

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
            <Or>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Zaanstad</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Diemen</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Ouder-Amstel</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Amstelveen</Literal>
                </PropertyIsEqualTo>
                <PropertyIsEqualTo>
                    <PropertyName>gemeentenaam</PropertyName>
                    <Literal>Amsterdam</Literal>
                </PropertyIsEqualTo>
            </Or>
        </Filter>
        """

    # Set up request parameters, including the filter
    params = {
        "request": config.request,
        "service": config.service,
        "version": config.version,
        "outputFormat": config.outputFormat,
        "typeName": "wijkenbuurten:buurten",
        "srsName": config.srsName,
        "filter": filter_xml,  # URL-encode the filter
    }
    data = fetch_wfs_data(config.cbs_url, params, logger)
    gdf = gpd.GeoDataFrame.from_features(data)
    logger.info(gdf.head(10))
    df = convert_to_polars(gdf)

    df = df.filter(pl.col("buurtnaam") != " ")

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
