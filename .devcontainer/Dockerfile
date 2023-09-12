FROM python:latest

# Update and upgrade the package list
RUN apt-get update && apt-get upgrade -y

# Install necessary packages (git, zsh, and curl)
RUN apt-get -y install git zsh curl

# Install oh-my-zsh and the dieter theme
RUN sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" && \
    sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="dieter"/' /root/.zshrc

RUN usermod -s $(which zsh) $(whoami)

RUN pip install dagster dagster-webserver streamlit polars ruff

