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
python -m pip install -e ".[dev,science]"
```

`-e` installs the package in editable mode, so changes under `src/antsearch/` take effect without reinstalling.

## Running tests

```bash
python -m pytest
```

To run only the pipeline tests, use `python -m pytest tests/test_pipeline.py`. They use a synthetic light curve, so they don't need network access. If the `science` extra isn't installed, they're skipped.

GitHub Actions runs the full suite on every push and pull request, on Python 3.10–3.13. See [.github/workflows/tests.yml](.github/workflows/tests.yml).

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

## Running the science check

`antsearch.pipeline` downloads one TESS light curve from MAST, flattens it, runs Box Least Squares and saves a phase-folded plot of the transit. By default it uses WASP-18 b (TESS sector 2, known period 0.94145 days):

```bash
python -m antsearch.pipeline
```

It logs the recovered period and its error against the known one, writes `output/folded_transit.png`, and exits with status 1 if the error is over 1%. To try another star, pass `--target`, `--known-period` and `--sector`, e.g. `--target "Pi Men" --known-period 6.2678 --sector 1`. Run with `--help` for the other options.

It flattens twice: the first BLS pass finds the transits, then the light curve is re-flattened with them masked so the detrending filter doesn't eat the dip.

## Project layout

```
AntSearch/
├── src/antsearch/
│   ├── coordinator.py  # FastAPI coordinator stub (records worker IDs in Redis)
│   ├── pipeline.py     # single-star science check: download, flatten, BLS, folded plot
│   └── worker.py       # worker stub (registers, then idles)
├── tests/              # pytest tests (test_*.py)
├── pyproject.toml      # package metadata, dependencies, pytest config
├── Dockerfile          # one image for the coordinator and the workers
├── compose.yaml        # local cluster: redis, coordinator, worker-1, worker-2
└── .github/workflows/  # CI
```

To add a runtime dependency, list it under `dependencies` in `pyproject.toml`. Dev-only tools go under `[project.optional-dependencies] dev`, and the light-curve libraries (lightkurve, astropy, matplotlib, numpy) under `science`. Then rerun `pip install -e ".[dev]"`.
