from dagster import (
    AssetExecutionContext,
    Config,
    MetadataValue,
    asset,
)
from pydantic import Field
from requests import request
import polars as pl
import pandas as pd
import geopandas as gpd


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
        default="application/json; subtype=geojson",
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
    typeName: str = Field(
        title="Type-Name",
        description="What type of BAG data to request",
        examples=["wijkenbuurten:buurten", "wijkenbuurten:buurten"],
        default="wijkenbuurten:buurten",
    )


@asset(
    name="bag_panden",
    io_manager_key="geo_database_io_manager",
)
def bag_panden(context: AssetExecutionContext, config: PDOK_BAG) -> pd.DataFrame:
    """
    Retrieve data from the PDOK BAG Web Feature Service (WFS)

    :param AssetExecutionContext context: Dagster Context
    :param PDOK_BAG config: Config
    :return pl.DataFrame: Data
    """
    params = {
        "request": config.request,
        "service": config.service,
        "version": config.version,
        "outputFormat": config.outputFormat,
        "typeName": config.typeName,
        "srsName": config.srsName,
    }

    response = request("GET", config.bag_url, params=params, timeout=180)

    if response.status_code == 200:
        data = response.json()
        features = data.pop("features")
        df = gpd.GeoDataFrame(features)
        context.add_output_metadata(
            metadata={
                "metadata": MetadataValue.json(data),
                "describe": MetadataValue.md(df.describe().to_markdown()),
                "number_of_columns": MetadataValue.int(len(df.columns)),
                "preview": MetadataValue.md(df.head().to_markdown()),
                # The `MetadataValue` class has useful static methods to build Metadata
            }
        )
        return df

    response.raise_for_status()


@asset(
    name="cbs_wijken",
    io_manager_key="geo_database_io_manager",
)
def cbs_wijken(context: AssetExecutionContext, config: PDOK_CBS) -> pd.DataFrame:
    """
    Retrieve data from the PDOK Web Feature Service (WFS)
    WFS Documentatie: https://docs.geoserver.org/latest/en/user/services/wfs/reference.html#getfeature
    Meer Documentatie: https://pdok-ngr.readthedocs.io/services.html#web-feature-service-wfs


    """
    params = {
        "request": config.request,
        "service": config.service,
        "version": config.version,
        "outputFormat": config.outputFormat,
        "typeName": config.typeName,
        "srsName": config.srsName,
    }

    response = request("GET", config.cbs_url, params=params, timeout=180)

    if response.status_code == 200:
        data = response.json()
        features = data.pop("features")
        df = gpd.GeoDataFrame(features)
        context.add_output_metadata(
            metadata={
                "metadata": MetadataValue.json(data),
                "describe": MetadataValue.md(df.describe().to_markdown()),
                "number_of_columns": MetadataValue.int(len(df.columns)),
                "preview": MetadataValue.md(df.head().to_markdown()),
                # The `MetadataValue` class has useful static methods to build Metadata
            }
        )
        return df

    response.raise_for_status()
