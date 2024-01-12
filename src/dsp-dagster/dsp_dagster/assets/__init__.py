from dagster import load_assets_from_package_module

from . import extraction, models, transformation

EXTRACTIONS = "extraction"
TRANSFORMATIONS = "transformation"
MODELS = "models"

extraction_assets = load_assets_from_package_module(
    package_module=extraction,
    group_name=EXTRACTIONS,
    # key_prefix=["prefix", EXTRACTIONS],
)

transformation_assets = load_assets_from_package_module(
    package_module=transformation,
    group_name=TRANSFORMATIONS,
    # key_prefix=["prefix", TRANSFORMATIONS],
)

model_assets = load_assets_from_package_module(
    package_module=models,
    group_name=MODELS,
    # key_prefix=["prefix", TRANSFORMATIONS],
)
