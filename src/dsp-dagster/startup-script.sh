#!/bin/sh
poetry update --no-interaction
poetry install --no-interaction --no-ansi --only main --no-root

export DAGSTER_HOME=/home/workspaces/DSP-compose/dsp-dagster/
poetry run dagster dev -h 0.0.0.0 -p 3000