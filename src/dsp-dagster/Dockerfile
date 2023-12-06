FROM python:3.10-slim

# Update and upgrade the package list
RUN apt-get update && apt-get upgrade -y


RUN pip install --upgrade pip
# Dagster specific
RUN pip install dagster dagster-webserver dagster-mlflow dagster-duckdb dagster-duckdb-pandas dagster-duckdb-polars xlsx2csv openpyxl asyncio httpx Faker geopandas





