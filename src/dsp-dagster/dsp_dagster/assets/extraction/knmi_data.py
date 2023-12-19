from datetime import datetime, timedelta
from typing import Optional
from pydantic import Field
from dagster import (
    asset,
    Config,
    AssetExecutionContext,
    MetadataValue,
)

import polars as pl
import httpx
import asyncio


def datetime_now() -> int:
    return int(datetime.now().strftime("%Y%m%d%H"))


class KNMIAssetConfig(Config):
    knmi_endpoint: str = Field(
        title="KNMI-Endpoint",
        description="The KNMI API endpoint being used, defaults to `uurgegevens`-API",
        examples=[
            "https://www.daggegevens.knmi.nl/klimatologie/uurgegevens",
            "https://www.daggegevens.knmi.nl/klimatologie/daggegevens",
            "https://www.daggegevens.knmi.nl/klimatologie/monv/reeksen",
        ],
        default="https://www.daggegevens.knmi.nl/klimatologie/uurgegevens",
    )
    start: int = Field(
        title="start",
        description="param field `start` that is the same as the complete start-date in the format: YYYYMMDDHH",
        examples=[2023010100, 2022010100],
        default=2005010100,
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
    interval_months: Optional[int] = Field(
        title="Interval Months",
        description="Number of months for each data-fetching interval",
        default=6,
    )


@asset(
    name="knmi_weather_data",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def get_knmi_weather_data(
    context: AssetExecutionContext, config: KNMIAssetConfig
) -> pl.DataFrame:
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

    :return pl.DataFrame: DataFrame with KNMI data based on range
    """
    start_date = datetime.strptime(str(config.start), "%Y%m%d%H")
    end_date = datetime.strptime(str(config.end), "%Y%m%d%H")
    interval = timedelta(
        days=config.interval_months * 30
    )  # Approximation of months to days
    intervals = create_intervals(start_date, end_date, interval)

    loop = asyncio.get_event_loop()
    all_data = loop.run_until_complete(fetch_data_for_intervals(config, intervals))

    combined_df = pl.concat(all_data)

    context.add_output_metadata(
        metadata={
            "describe": MetadataValue.md(
                combined_df.to_pandas().describe().to_markdown()
            ),
            "number_of_columns": MetadataValue.int(len(combined_df.columns)),
            "preview": MetadataValue.md(combined_df.head().to_pandas().to_markdown()),
        }
    )
    return combined_df


def create_intervals(start, end, interval):
    intervals = []
    current_start = start
    while current_start < end:
        current_end = min(current_start + interval, end)
        intervals.append((current_start, current_end))
        current_start = current_end
    return intervals


async def fetch_data_for_interval(client, config, start, end):
    params = {
        "start": start.strftime("%Y%m%d%H"),
        "end": end.strftime("%Y%m%d%H"),
        "vars": config.vars,
        "stns": config.stns,
        "fmt": config.fmt,
    }
    response = await client.get(config.knmi_endpoint, params=params, timeout=180)
    response.raise_for_status()
    data = response.json()
    return pl.from_dicts(data)


async def fetch_data_for_intervals(config, intervals):
    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_data_for_interval(client, config, start, end)
            for start, end in intervals
        ]
        return await asyncio.gather(*tasks)
