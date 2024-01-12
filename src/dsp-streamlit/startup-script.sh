#!/bin/sh
poetry update --no-interaction
poetry install --no-interaction --no-ansi --only-main --no-root

poetry run streamlit run 🏠_Home.py
