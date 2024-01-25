import os

from dagster import Definitions

from .assets import extraction_assets, model_assets, transformation_assets
from .jobs import extract_data, modelling, transform_data
from .resources import LOCAL_RESOURCE

all_assets = [
    *extraction_assets,
    *transformation_assets,
    *model_assets,
]


resources_by_deployment_name = {"local": LOCAL_RESOURCE}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_name[deployment_name],
    jobs=[extract_data, transform_data, modelling]
    # schedules=[core_assets_schedule],
    # sensors=all_sensors,
)
