import polars as pl
from dagster import AssetExecutionContext, AssetIn, MetadataValue, asset
from ...util.helpers import *



# @asset(
#     name="cleaned_knmi_weather_data_api",
#     ins={"knmi_weather_data_api": AssetIn(key="knmi_weather_api")},
#     key_prefix="cleaned",
#     io_manager_key="database_io_manager",
#     description="Clean API data source",
# )
# def cleaned_knmi_weather_data_api(
#     context: AssetExecutionContext, knmi_weather_data_api: pl.DataFrame
# ) -> pl.DataFrame:
#     return clean_data(knmi_weather_data_api)


# @asset(
#     name="cleaned_knmi_weather_data_txt",
#     ins={"knmi_weather_data_txt": AssetIn(key="knmi_weather_txt")},
#     key_prefix="cleaned",
#     io_manager_key="database_io_manager",
#     description="Clean TXT data source",
# )
# def cleaned_knmi_weather_data_txt(
#     context: AssetExecutionContext, knmi_weather_data_txt: pl.DataFrame
# ) -> pl.DataFrame:
#     return clean_data(knmi_weather_data_txt)


# @asset(
#     name="cleaned_storm_incidents",
#     ins={"storm_incidents": AssetIn(key="storm_incidents")},
#     key_prefix="cleaned",
#     io_manager_key="database_io_manager",
#     description="Retrieve (time) features from storm_incidents",
# )
# def cleaned_storm_incidents(
#     context: AssetExecutionContext, storm_incidents: pl.DataFrame
# ) -> pl.DataFrame:
#     """
#     Retrieve features from storm_incident
#     :return pl.DataFrame: storm_incidents with retrieved features
#     """

#     df = storm_incidents.with_columns(
#         [
#             pl.col("Incident_Starttime").dt.hour().alias("Incident_Starttime_Hour"),
#             # pl.col("Incident_Endtime").dt.hour().alias("Incident_Endtime_Hour"),
#             # pl.col("Incident_Duration").dt.hour().alias("Incident_Duration_Hour"),
#             pl.col("Incident_Starttime").dt.minute().alias("Incident_Starttime_Minute"),
#             # pl.col("Incident_Endtime").dt.minute().alias("Incident_Endtime_Minute"),
#             # pl.col("Incident_Duration").dt.minute().alias("Incident_Duration_Minute"),
#         ]
#     )  # or .to_datetime("%+")

#     context.add_output_metadata(
#         metadata={
#             "number_of_columns": MetadataValue.int(len(df.columns)),
#             "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
#         }
#     )
#     return df
