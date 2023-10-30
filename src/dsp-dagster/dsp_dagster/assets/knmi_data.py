from datetime import datetime
import os
from typing import Optional
from dagster import asset, Config, get_dagster_logger
import polars as pl
import requests

from pydantic import Field


def datetime_now() -> int:
    return int(datetime.now().strftime("%Y%m%d%H"))


class KNMIAssetConfig(Config):
    base_url: str = os.getenv("KNMI_BASE_URL")
    start: int = Field(
        title="start",
        description="param field `start` that is the same as the complete start-date in the format: YYYYMMDDHH",
        examples=[2023010100, 2022010100],
        default=2023010100,
    )
    end: int = Field(
        title="end",
        description="param field `end` that is the same as the complete end-date in the format: YYYYMMDDHH",
        default_factory=datetime_now,
    )
    stns: Optional[int] = Field(
        title="stns",
        description="param field `stns` that represents a weather station as integer, defaults to 240 (schiphol). A list of weather stations can be found on https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script",
        default=240,
    )
    vars: Optional[str] = Field(
        title="vars",
        description="A list of variables that will be retrieved, separated with ':'. The list can be found on https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script",
        default="ALL",
    )
    fmt: Optional[str] = Field(
        title="fmt",
        description="Output format, this can be csv (default), json and xml",
        default="json",
    )


@asset(
    name="knmi_uurgegevens",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def get_knmi_uurgegevens(config: KNMIAssetConfig) -> pl.DataFrame:
    """
    Based from these KNMI API docs: https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script

    `start`: (verplicht)
        De volledige datum (start) in het format YYYYMMDDHH.

    `end`: (verplicht)
        De volledige datum-tijd (end) in het format YYYYMMDDHH.
        Het eerste en laatste uur in het HH-gedeelte bepalen de uren die voor elke dag geleverd worden (1-24), in overeeenkomst met het aan te geven dagdeel in de interactieve selectie. Dus start=2006060606, end=2008080808 resulteren voor elke dag steeds in het 6e, 7e en 8e uur, terwijl start=2006060622, end=2008080806 bv. de nachtelijke uren 22, 23 .... 5, 6 geeft.

    `vars`:
        Lijst van gewenste variabelen in willekeurige volgorde, aangeduid met hun acroniemen (zoals op de selectiepagina) gescheiden door ':', bijvoorbeeld 'DD:FH:FF:FX'.
        De voorgedefinieerde sets van variabelen zijn hier:
            WIND = DD:FH:FF:FX     Wind
            TEMP = T:T10N:TD       Temperatuur
            SUNR = SQ:Q            Zonneschijnduur en globale straling
            PRCP = DR:RH           Neerslag en potentiële verdamping
            VICL = VV:N:U          Zicht, bewolking en relatieve vochtigheid
            WEER = M:R:S:O:Y:WW    Weerverschijnselen, weertypen
            ALL alle variabelen
    `stns`:
        Lijst van gewenste stations (nummers) in willekeurige volgorde, gescheiden door ':'.
        Geen default waarde; Stations móeten zijn gespecificeerd.
        ALL staat voor álle stations.

    `fmt`:
        Standaard is de output van het script in CSV-formaat, met fmt=xml is de output in XML-formaat, met fmt=json in JSON-formaat.

    :return pl.DataFrame: DataFrame with KNMI data that will be stored in DuckDB database
    """
    logger = get_dagster_logger()

    params = {
        "start": config.start,
        "end": config.end,
        "vars": config.vars,
        "stns": config.stns,
        "fmt": config.fmt,
    }
    response = requests.request("GET", config.base_url, params=params, timeout=60)

    if response.status_code == 200:
        data = response.json()
        df = pl.from_dicts(data)
        return df

    else:
        logger.error(f"Received non-200 status code [{response.status_code}]")
