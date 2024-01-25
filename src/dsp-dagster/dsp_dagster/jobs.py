from dagster import AssetSelection, define_asset_job

from .assets import EXTRACTIONS, MODELS, TRANSFORMATIONS

extract_data = define_asset_job(
    "extract_data", selection=AssetSelection.groups(EXTRACTIONS)
)
transform_data = define_asset_job(
    "transform_data", selection=AssetSelection.groups(TRANSFORMATIONS)
)
modelling = define_asset_job("modelling", selection=AssetSelection.groups(MODELS))
