install:
    curl -LsSf https://astral.sh/uv/install.sh | sh
    curl https://install.duckdb.org | sh
    source $HOME/.local/bin/env
    uv venv --python 3.12
    source .venv/bin/activate
    uv pip install -r requirements.txt

env:
    source .venv/bin/activate