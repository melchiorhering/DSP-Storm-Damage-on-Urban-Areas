FROM python:3.10-bullseye

# Update and upgrade the package list
RUN apt-get update && apt-get upgrade -y

# Install necessary packages (git, zsh, and curl)
RUN apt-get -y install git zsh curl

# Install oh-my-zsh and the dieter theme
RUN sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" && \
    sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="dieter"/' /root/.zshrc

RUN usermod -s $(which zsh) $(whoami)

RUN python -m ensurepip --upgrade

RUN pip install --upgrade pip

RUN pip install streamlit ruff plotly pylint[spelling] Faker xlsx2csv

RUN pip install dagster dagster-webserver dagster-mlflow dagster-duckdb-polars openpyxl