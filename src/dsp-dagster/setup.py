from setuptools import find_packages, setup


setup(
    name="dsp-dagster",
    version="0.1.0",
    author=["Stijn Hering", "etc", "etc"],
    description="TO-DO",
    packages=find_packages(
        exclude=["tests"]
    ),  # Include all Python packages in the project
    install_requires=[
        "dagster",
        "dagster-duckdb-polars",
        "dagster_duckdb",
        "Faker==18.4.0",
        "xlsx2csv",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
