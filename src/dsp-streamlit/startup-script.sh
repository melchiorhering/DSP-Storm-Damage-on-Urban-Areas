#!/bin/sh
poetry install --no-interaction --no-ansi --only main --no-root
# poetry update --no-interaction

poetry run streamlit run 🏠_Home.py
