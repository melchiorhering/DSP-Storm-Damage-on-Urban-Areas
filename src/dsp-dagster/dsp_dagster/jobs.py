from dagster import AssetSelection, define_asset_job

from .assets import EXTRACTIONS, MODELS, TRANSFORMATIONS

extract_data = define_asset_job(
    "extract_data", selection=AssetSelection.groups(EXTRACTIONS)
)
transform_data = define_asset_job(
    "transform_data", selection=AssetSelection.groups(TRANSFORMATIONS)
)
run_models = define_asset_job("run_models", selection=AssetSelection.groups(MODELS))
