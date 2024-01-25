#!/bin/sh
poetry install --no-interaction --no-ansi --only main --no-root
# poetry update --no-interaction

duckdb DSP.duckdb

export DAGSTER_HOME=/home/workspaces/DSP-compose/dsp-dagster/

poetry run dagster dev -h 0.0.0.0 -p 3000
