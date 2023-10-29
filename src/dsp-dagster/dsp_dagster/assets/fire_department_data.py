from dagster import (
    asset,
)
import polars as pl
import pandas as pd


@asset(
    name="storm_data_small",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_small() -> pl.DataFrame:  # modify return type signature
    df = pl.read_excel("./data/Storm_Data_Small.xlsx")
    return df


@asset(
    name="storm_data_large",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_large() -> pl.DataFrame:  # modify return type signature
    df = pl.read_excel("./data/Storm_Data_Large.xlsx", read_csv_options={"infer_schema_length": 10000})
    return df


@asset(
    name="storm_data_deployments",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def storm_data_deployments() -> pl.DataFrame:  # modify return type signature
    df = pl.read_excel("./data/Storm_Data_Deployments.xlsx")
    return df


@asset(
    name="fire_department_stations",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def kazernes() -> pl.DataFrame:  # modify return type signature
    df = pl.read_excel("./data/Kazernes.xlsx")
    return df


@asset(
    name="fire_stations_and_vehicles",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def fire_stations_and_vehicles() -> pl.DataFrame:  # modify return type signature
    df = pl.read_excel("./data/Fire_Stations_and_Vehicles.xlsx")
    return df
