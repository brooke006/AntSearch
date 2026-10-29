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

`-e` installs the package in editable mode, so changes under `src/antsearch/` take effect without reinstalling. Once the venv exists, `make install` does the same install.

## Make targets

The [Makefile](Makefile) wraps the common commands. Run `make` on its own to list them. It uses `.venv/bin/python` if the venv exists, so you don't need to activate it first.

| Target | What it does |
|---|---|
| `make install` | Install the package with the `dev` and `science` extras |
| `make build` | Build the Docker image (`antsearch:dev`; override with `IMAGE=...`) |
| `make up` / `make down` | Start or stop the local cluster in the background |
| `make logs` / `make ps` | Follow the cluster's logs / list its containers |
| `make submit` | Queue the first 50 stars of the TESS list on the cluster and wait for the results (`STARS=...` for another count) |
| `make test` | Unit and science tests (everything that doesn't need Docker) |
| `make test-unit` | Fast tests only: coordinator, worker, package |
| `make test-science` | Light-curve pipeline tests |
| `make test-integration` | Start a throwaway cluster, test it over HTTP, tear it down |
| `make test-all` | `test` plus `test-integration` |
| `make coverage` | `make test` with a coverage report |
| `make clean` | Remove test caches and coverage output |

## Running tests

Tests come in three types, selected with pytest markers:

| Type | Command | Covers | Needs |
|---|---|---|---|
| Unit | `make test-unit` | Coordinator endpoints (against [fakeredis](https://github.com/cunla/fakeredis-py)), the worker loop, the submit script, two in-process workers processing 50 stars, package | Nothing; takes seconds |
| Science | `make test-science` | Pipeline: flatten, BLS, plotting, `run_task`, download retries and exit codes, using a synthetic light curve and faked MAST errors | The `science` extra (skipped without it); no network |
| Integration | `make test-integration` | The real cluster over HTTP: real Redis, the two worker containers processing 50 tasks, a full task round trip, a silent worker going offline and its task finished by another | Docker |

`make test` (or plain `python -m pytest`) runs the unit and science tests. The integration tests are skipped unless `ANTSEARCH_URL` points at a coordinator. `make test-integration` sets that for you: it starts a separate compose project (`antsearch-it`) on port 8001 with a 3-second heartbeat timeout, runs the tests, and removes it again, so it doesn't touch a cluster you started with `make up`. Its workers run simulated tasks (`WORKER_TASK=simulated`), so the tests don't download from MAST.

To run the same selections with pytest directly, use `-m "not science and not integration"`, `-m science` or `-m integration`.

### Coverage

```bash
make coverage
```

This prints a per-file summary with the uncovered line numbers and writes an HTML report to `htmlcov/index.html` (open it in a browser to see uncovered lines highlighted). It covers the unit and science tests. The integration tests run code inside containers, so they don't count towards coverage.

GitHub Actions runs `python -m pytest` (unit and science) on every push and pull request, on Python 3.10–3.13. See [.github/workflows/tests.yml](.github/workflows/tests.yml).

## Running the local cluster

With [Docker](https://docs.docker.com/get-docker/) running, start Redis, the coordinator and two workers:

```bash
make up          # or, in the foreground: docker compose up --build
```

The coordinator is published at http://localhost:8000 (set `ANTSEARCH_PORT` to use another port). Each worker registers, sends a heartbeat every third of the heartbeat timeout, and pulls tasks from the coordinator (see [Workers](#workers)). The timeout defaults to 30 s; set `HEARTBEAT_TIMEOUT` to change it. To see the workers, their status and the tasks they hold:

```bash
curl localhost:8000/workers
```

To give the workers something to do, queue stars from the bundled TESS list ([tess_data_mixed.csv](src/antsearch/datasets/tess_data_mixed.csv): 100 TOIs with confirmed planets, then 100 false positives) and wait for the results:

```bash
make submit              # first 50 stars; STARS=10 make submit for fewer
```

It prints a line per star (who ran it, and the period and depth found or the error), then the total time and how many tasks each worker ran. Each star takes about a minute, mostly searching MAST and downloading the light curve. `python -m antsearch.submit --help` lists the options, including `--stars` for another CSV and `--no-wait`.

`make down` stops the cluster. `make up` always rebuilds the images, so it picks up code changes.

## Workers

`python -m antsearch.worker` (what the compose workers run) joins a coordinator and works until it gets SIGINT or SIGTERM:

1. Register, then send heartbeats from a background thread so the worker stays online through long tasks.
2. Request a task. If the queue is empty, or the coordinator can't be reached, wait `POLL_INTERVAL` seconds and ask again.
3. Run the task: by default `pipeline.run_task`, the transit search on that star's first SPOC 2-minute light curve. A task that raises (say the star has no light curve on MAST) is reported with an `{"error", "message"}` result instead.
4. Report the result, retrying network errors. If the coordinator answers 409, the worker was marked offline and the task went to someone else, so the result is dropped.

If the coordinator forgets the worker or marks it offline, the worker registers again. On SIGTERM it finishes and reports the task it's running before it exits (compose gives it three minutes).

| Variable | Default | Meaning |
|---|---|---|
| `COORDINATOR_URL` | `http://localhost:8000` | Coordinator to join |
| `WORKER_ID` | the host name | Name to register as |
| `WORKER_TASK` | `transit` | `transit` runs the transit search; `simulated` waits `SIMULATED_TASK_SECONDS` and returns a fake result, for tests and scheduler experiments that shouldn't hit MAST |
| `SIMULATED_TASK_SECONDS` | `1` | How long a simulated task takes |
| `POLL_INTERVAL` | `2` | Seconds between task requests while the queue is empty |

To run a worker outside Docker against a cluster started with `make up`: `WORKER_ID=laptop python -m antsearch.worker`.

A transit-search result looks like this (TOI-102, known period 4.4119 d):

```json
{"star_id": "TOI-102", "sector": 1, "period": 4.41305, "t0": 1326.0756, "duration": 0.12,
 "depth": 0.0131, "power": 0.283, "download_seconds": 43.96, "compute_seconds": 6.31}
```

## Coordinator API

All state lives in Redis under `antsearch:` keys (the layout is described at the top of [coordinator.py](src/antsearch/coordinator.py)), so the coordinator can restart without losing workers or tasks.

| Endpoint | Body | Response |
|---|---|---|
| `POST /workers/register` | `{"worker_id": "w1"}` | `{"worker_id", "registered": true, "heartbeat_timeout"}` |
| `POST /workers/{id}/heartbeat` | none | `{"worker_id", "status": "online"}`; 404 if not registered |
| `GET /workers` | none | Every worker with its status, last heartbeat and assigned tasks |
| `POST /tasks` | `{"star_ids": ["WASP-18", ...]}` | 201, `{"task_ids": ["t1", ...]}` |
| `POST /tasks/request` | `{"worker_id": "w1"}` | `{"task_id", "star_id"}`; 204 if nothing is queued; 404 if not registered; 409 if offline |
| `POST /tasks/{id}/result` | `{"worker_id": "w1", "result": {...}}` | `{"task_id", "status": "done"}`; 409 if the task isn't assigned to that worker |
| `GET /tasks/{id}` | none | The task's star, status, worker, timestamps and result |
| `GET /health` | none | `{"status": "ok"}`, or 503 if Redis is unreachable |

Tasks are handed out first come, first served, until the scheduler (#5) replaces that.

**Joining and leaving.** A worker joins by registering. If it sends no heartbeat for longer than the timeout, the coordinator marks it `offline`. A background check runs every third of the timeout, and `GET /workers` and `POST /tasks/request` also check first. When a worker is marked offline, its unfinished tasks go back to the front of the queue for another worker. An offline worker gets 409 on task requests and result submissions until it sends a heartbeat or registers again. A worker that registers again after a restart gives up any tasks it held, since it lost them in the restart.

Try it against `make up`:

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json' -d '{"star_ids": ["WASP-18"]}'
curl -X POST localhost:8000/tasks/request -H 'content-type: application/json' -d '{"worker_id": "worker-1"}'
curl -X POST localhost:8000/tasks/t1/result -H 'content-type: application/json' \
     -d '{"worker_id": "worker-1", "result": {"period": 0.94136}}'
curl localhost:8000/tasks/t1
```

## Running the science check

`antsearch.pipeline` downloads one TESS light curve from MAST, flattens it, runs Box Least Squares and saves a phase-folded plot of the transit. By default it uses WASP-18 b (TESS sector 2, known period 0.94145 days):

```bash
python -m antsearch.pipeline
```

It logs the recovered period and its error against the known one and writes `output/folded_transit.png`. The exit status says how it went:

| Status | Meaning |
|---|---|
| 0 | Period recovered within 1% |
| 1 | Period recovered, but more than 1% off |
| 3 | No matching data on MAST, or the star name can't be resolved. Retrying won't help. |
| 4 | Download failed on every attempt (MAST unreachable or erroring). Try again later. |

Network and MAST errors are retried 3 times by default (`--attempts`), waiting 2 s and then 4 s. If a cached download is corrupt, usually because an earlier download was cut off, it's deleted and downloaded again. Lightkurve caches files under `~/.cache/lightkurve`.

To try another star, pass `--target`, `--known-period` and `--sector`, e.g. `--target "Pi Men" --known-period 6.2678 --sector 1`. Run with `--help` for the other options.

It flattens twice: the first BLS pass finds the transits, then the light curve is re-flattened with them masked so the detrending filter doesn't eat the dip.

## Project layout

```
AntSearch/
├── src/antsearch/
│   ├── coordinator.py  # FastAPI coordinator: workers, heartbeats, task queue, results (Redis)
│   ├── pipeline.py     # transit search: download, flatten, BLS, folded plot; run_task() for workers
│   ├── submit.py       # queue stars from a star list and wait for the results
│   ├── worker.py       # worker: registers, heartbeats, pulls tasks, runs them, reports results
│   └── datasets/       # TESS star list (tess_data_mixed.csv) and the script that rebuilt it
├── tests/              # pytest tests; test_integration.py needs a running cluster
├── Makefile            # build, run, test and coverage shortcuts (`make help`)
├── pyproject.toml      # package metadata, dependencies, pytest config
├── Dockerfile          # one image for the coordinator and the workers
├── compose.yaml        # local cluster: redis, coordinator, worker-1, worker-2
└── .github/workflows/  # CI
```

To add a runtime dependency, list it under `dependencies` in `pyproject.toml`. Dev-only tools go under `[project.optional-dependencies] dev`, and the light-curve libraries (lightkurve, astropy, matplotlib, numpy) under `science`. Then rerun `make install`.
