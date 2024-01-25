#!/bin/sh
poetry install --no-interaction --no-ansi --no-root
# poetry update --no-interaction

# Install IPython kernel - useful if you're running Jupyter inside the container
poetry run ipython kernel install --user --name=DSP --display-name="DSP-Storm-Damage-on-Urban-Areas"

# Adds this repo to safe directories
git config --global --add safe.directory /home/workspaces/DSP-compose

# Keep the container running - this is a common pattern for Devcontainers
tail -f /dev/null
