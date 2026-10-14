# AntSearch

[![tests](https://github.com/brooke006/AntSearch/actions/workflows/tests.yml/badge.svg)](https://github.com/brooke006/AntSearch/actions/workflows/tests.yml)

A Python volunteer-computing system for exoplanet searches using ant-inspired task scheduling.

## Setup

Requires Python 3.10 or newer.

```bash
git clone https://github.com/brooke006/AntSearch.git
cd AntSearch

python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

`-e` installs the package in editable mode, so changes under `src/antsearch/` take effect without reinstalling.

## Running tests

```bash
python -m pytest
```

GitHub Actions runs the same command on every push and pull request, on Python 3.10–3.13. See [.github/workflows/tests.yml](.github/workflows/tests.yml).

## Running the local cluster

With [Docker](https://docs.docker.com/get-docker/) running, start Redis, the coordinator stub and two workers:

```bash
docker compose up --build
```

Each worker logs a line like `worker worker-1 registered with coordinator at http://coordinator:8000` once it registers. The coordinator is published at http://localhost:8000 and records registered workers in Redis. To list them:

```bash
docker compose exec redis redis-cli SMEMBERS antsearch:workers
```

Press Ctrl+C to stop, or run `docker compose down` from another terminal. After changing code, rerun with `--build` so the images pick up your edits.

## Project layout

```
AntSearch/
├── src/antsearch/
│   ├── coordinator.py  # FastAPI coordinator stub (records worker IDs in Redis)
│   └── worker.py       # worker stub (registers, then idles)
├── tests/              # pytest tests (test_*.py)
├── pyproject.toml      # package metadata, dependencies, pytest config
├── Dockerfile          # one image for the coordinator and the workers
├── compose.yaml        # local cluster: redis, coordinator, worker-1, worker-2
└── .github/workflows/  # CI
```

To add a runtime dependency, list it under `dependencies` in `pyproject.toml`. Dev-only tools go under `[project.optional-dependencies] dev`. Then rerun `pip install -e ".[dev]"`.
