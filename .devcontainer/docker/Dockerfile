# MAIN DSP IMAGE
FROM python:3.10-bullseye

# Update and upgrade the package list
RUN apt-get update && apt-get upgrade -y

# Install necessary packages (git, zsh, curl, and sqlite3)
RUN apt-get -y install git zsh curl sqlite3 python3-venv

# Install oh-my-zsh and the dieter theme
RUN sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" && \
    sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="dieter"/' /root/.zshrc

RUN usermod -s $(which zsh) $(whoami)

# Install pipx and prerequisites
RUN python3 -m pip install --upgrade pip
RUN pip install pipx
RUN python3 -m pipx ensurepath

# Install Poetry using pipx
RUN pipx install poetry

# Add pipx bin directory to PATH
ENV PATH="/root/.local/bin:$PATH"

# Copy Dependencies
# COPY ../poetry.lock ../pyproject.toml /home/workspaces/DSP-compose/

# Set workdir so pyproject.toml and poetry.lock can be found
# WORKDIR /home/workspaces/DSP-compose

# Disable creation of virtual environments by Poetry, as the container itself provides isolation
RUN poetry config virtualenvs.create false
# RUN poetry update --no-interaction
# RUN poetry install --no-interaction --no-ansi --no-dev

# Install dependencies using Poetry
# RUN poetry install --no-dev
