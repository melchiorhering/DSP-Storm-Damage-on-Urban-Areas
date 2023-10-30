from dagster import (
    asset,
)
import polars as pl


import requests


url = "https://www.daggegevens.knmi.nl/klimatologie/uurgegevens?start=2023040100&end=2023100100&stns=240&vars=all&fmt=json"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)