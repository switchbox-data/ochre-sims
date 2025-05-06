FROM mcr.microsoft.com/devcontainers/python:3.11

# Install additional OS packages
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    wget \
    curl \
    sudo \
    dpkg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv 
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="/usr/local/bin" sh

# Install DuckDB
RUN curl https://install.duckdb.org | sh