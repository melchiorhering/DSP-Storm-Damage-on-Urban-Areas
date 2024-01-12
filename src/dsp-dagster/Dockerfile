FROM python:3.10-slim

# Update and upgrade the package list
RUN apt-get update && apt-get upgrade -y

# Install pipx and prerequisites
RUN apt-get install -y curl python3-venv
RUN python3 -m pip install --upgrade pip
RUN pip install pipx
RUN python3 -m pipx ensurepath

# Install Poetry using pipx
RUN pipx install poetry

# Add pipx bin directory to PATH
ENV PATH="/root/.local/bin:$PATH"

# Disable creation of virtual environments by Poetry, as the container itself provides isolation
RUN poetry config virtualenvs.create false
