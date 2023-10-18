from setuptools import find_packages, setup

setup(
    name="dsp_dagster",
    packages=find_packages(exclude=["dsp_dagster_tests"]),
    install_requires=["dagster", "dagster-cloud", "dagster-duckdb-polars", "plotly"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
