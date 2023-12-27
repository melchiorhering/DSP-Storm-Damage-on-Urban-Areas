#!/bin/sh
poetry update --no-interaction
poetry install --no-interaction --no-ansi --only main --no-root


poetry run mlflow server --backend-store-uri sqlite:///mlruns.db --default-artifact-root ./mlruns -h 0.0.0.0 -p 5001