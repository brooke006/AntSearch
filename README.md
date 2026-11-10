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
| `make scenario` | Run a failure scenario on the cluster (`SCENARIO=scenarios/slow-worker.yaml`); see [Failure scenarios](#failure-scenarios) |
| `make metrics` | Completion time, throughput and wasted work of the newest run (`RUN=output/runs/<run>.jsonl` for another); see [Run logs and metrics](#run-logs-and-metrics) |
| `make test` | Unit and science tests (everything that doesn't need Docker) |
| `make test-unit` | Fast tests only: coordinator, worker, submit, scenarios, run logs, pheromone table, package |
| `make test-science` | Light-curve pipeline tests |
| `make test-integration` | Start a throwaway cluster, test it over HTTP, tear it down |
| `make test-all` | `test` plus `test-integration` |
| `make coverage` | `make test` with a coverage report |
| `make clean` | Remove test caches and coverage output |

## Running tests

Tests come in three types, selected with pytest markers:

| Type | Command | Covers | Needs |
|---|---|---|---|
| Unit | `make test-unit` | Coordinator endpoints (against [fakeredis](https://github.com/cunla/fakeredis-py)), the worker loop, the submit script, failure scenarios (against a fake Docker and clock), the pheromone table, two in-process workers processing 50 stars, package | Nothing; takes seconds |
| Science | `make test-science` | Pipeline: flatten, BLS, plotting, `run_task`, download retries and exit codes, using a synthetic light curve and faked MAST errors | The `science` extra (skipped without it); no network |
| Integration | `make test-integration` | The real cluster over HTTP: real Redis, the two worker containers processing 50 tasks, a full task round trip, a silent worker going offline and its task finished by another, a failure scenario slowing, killing and restarting a worker on schedule | Docker |

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

## Failure scenarios

A scenario file makes failures repeatable: it limits workers' CPUs, adds network delay, and kills and restarts workers at set times. Run one against the cluster from `make up`:

```bash
make scenario SCENARIO=scenarios/crash-and-recover.yaml
```

This writes one JSON line per event to `output/scenarios/crash-and-recover.jsonl`, with the planned time (`at`) and when it actually happened (`elapsed`). Run a scenario in one terminal and the workload (`make submit`) in another; the scenario holds after its last event until `duration`, then puts the workers back to normal.

```yaml
duration: 100               # optional: reset and exit this many seconds after the start (default: wait for Ctrl-C)
events:
  - {at: 0,  worker: worker-1, action: cpu, cpus: 0.25}   # Docker CPU limit; cpus: 0 lifts it
  - {at: 10, worker: worker-1, action: delay, ms: 300}    # delay on every packet the worker sends; ms: 0 removes it
  - {at: 20, worker: worker-2, action: kill}              # SIGKILL: the worker vanishes mid-task
  - {at: 60, worker: worker-2, action: start}             # start a killed worker again
  - {at: 80, worker: worker-1, action: restart}           # kill, then start straight away
```

`worker` is a compose service name. [scenarios/](scenarios/) has two examples: `slow-worker.yaml` and `crash-and-recover.yaml`.

What makes two runs of the same file match:

- **Same starting state.** Before the first event, every worker the scenario names is started if it isn't running, and its CPU limit and delay are removed.
- **Timed from the start, not from the last event.** A slow docker command makes its own event late but not the ones after it. Events at the same time run in file order.
- **Delays survive restarts.** A kill throws away the container's network settings, so the scenario sets the delay again when it starts that worker.

Run twice on the same stack, a six-event scenario applied every event within 5 ms of its planned time.

Some things to know:

- CPU limits slow the transit search, but not simulated tasks (`WORKER_TASK=simulated`), which only sleep.
- Network delay applies to everything the worker sends: heartbeats, task requests, results and MAST downloads.
- The worker containers need `NET_ADMIN` and `tc` for the delay. `compose.yaml` and the Dockerfile provide both, so run `make up` again after pulling this change.
- Other options: `python -m antsearch.scenario FILE --dry-run` prints the timeline without touching Docker, and `-p PROJECT` targets another compose project. `--help` lists the rest.

Exit status: 0 if every event applied, 1 if some failed (each is logged with its error and the scenario carries on), 3 if the workers couldn't be reached (is the cluster up?).

## Run logs and metrics

Each run writes one log file, `output/runs/<start time>-<scheduler>.jsonl`, and `make metrics` computes the results from it:

```bash
SCHEDULER=greedy make up
make submit
make metrics
make down
```

A run lasts from `make up` to `make down`. The run's name is kept in Redis, so a coordinator that restarts mid-run carries on with the same file, and `make down` clears it so the next `make up` starts a new one. Run `make down` before changing `SCHEDULER`, or one log will mix two schedulers (`make metrics` warns if it does). Set `RUN_LOG_DIR` to keep the logs somewhere else; a path for `make up` needs to start with `./` or `/`.

The coordinator writes one JSON line per event, with `time` in Unix seconds on its own clock:

| `event` | Other fields |
|---|---|
| `coordinator_started` | `scheduler`, `seed`, `heartbeat_timeout` |
| `worker_registered`, `worker_online` | `worker_id` |
| `worker_offline` | `worker_id`, `last_heartbeat` |
| `task_queued` | `task_id`, `star_id` |
| `task_assigned` | `task_id`, `star_id`, `worker_id` |
| `task_done` | `task_id`, `worker_id`, `result` (a failed run's result has an `error` key) |
| `task_requeued` | `task_id`, `worker_id`, `reason` (`offline` or `reregistered`), `last_heartbeat` |
| `result_rejected` | `task_id`, `worker_id`, `status`, `result` |

The metrics:

- **Completion time:** from the first task queued to the last task finished. `make metrics` also shows the mean turnaround, the same measure for each task on its own. With a full queue the schedulers have about the same choices, so to compare them, queue tasks gradually and look at turnaround.
- **Throughput:** tasks that succeeded per minute of completion time. Failed runs don't count, so a worker that fails quickly can't look fast.
- **Wasted work:** worker time that produced no answer, measured from assignment. For a task taken from a worker, that's up to the worker's last heartbeat, or up to its result if it turns out to be alive and reports late. For a failed run, it's up to the error result. It's shown as seconds and as a share of all worker time, with lost and failed work listed separately.

Times are spans on the coordinator's clock rather than what workers report, so they work for failed runs and don't depend on the workers' clocks. They include time on the network.

On Linux, the coordinator container's user (UID 1000) must be able to write to the log directory. `make up` creates it as you; `docker compose up` on its own would create it as root, and the coordinator would exit with a permission error.

## Coordinator API

Workers and tasks are stored in Redis under `antsearch:` keys (the layout is described at the top of [coordinator.py](src/antsearch/coordinator.py)), so the coordinator can restart without losing them. Scheduler state (the round-robin position, the random choice, the greedy estimates) is kept in memory and starts over when the coordinator restarts. Run one coordinator process.

| Endpoint | Body | Response |
|---|---|---|
| `POST /workers/register` | `{"worker_id": "w1"}` | `{"worker_id", "registered": true, "heartbeat_timeout"}` |
| `POST /workers/{id}/heartbeat` | none | `{"worker_id", "status": "online"}`; 404 if not registered |
| `GET /workers` | none | Every worker with its status, last heartbeat and assigned tasks |
| `POST /tasks` | `{"star_ids": ["WASP-18", ...]}` | 201, `{"task_ids": ["t1", ...]}` |
| `POST /tasks/request` | `{"worker_id": "w1"}` | `{"task_id", "star_id"}`; 204 if no task is available for this worker; 404 if not registered; 409 if offline |
| `POST /tasks/{id}/result` | `{"worker_id": "w1", "result": {...}}` | `{"task_id", "status": "done"}`; 409 if the task isn't assigned to that worker |
| `GET /tasks/{id}` | none | The task's star, status, worker, timestamps and result |
| `GET /health` | none | `{"status": "ok"}`, or 503 if Redis is unreachable |

The coordinator started with `python -m antsearch.coordinator` uses round robin unless `SCHEDULER` says otherwise (see [Schedulers](#schedulers)). Online workers without a running task take turns in worker-ID order. A worker requesting out of turn gets 204 and tries again after its normal polling interval. Busy and offline workers are skipped. A worker that finishes can keep taking work while the others are busy. An idle worker that stops requesting work but continues sending heartbeats can delay the others; it remains eligible until it takes work or goes offline.

`Scheduler.assign(worker_id, tasks, workers)` returns a pending task ID or `None`. It does not change Redis. The coordinator serializes scheduling and claims the selected task in a Redis transaction. `on_result(worker_id, result)` is called after an accepted result; round robin does not use the result to change its choices. The `create_app` factory accepts a scheduler argument; omitting it keeps the original FIFO behavior for existing callers and tests.

### Schedulers

Set `SCHEDULER` to choose one, for example `SCHEDULER=greedy make up`:

| `SCHEDULER` | Each task goes to |
|---|---|
| `round-robin` (default) | The next available worker in worker-ID order |
| `random` | An available worker chosen uniformly at random. Set `SCHEDULER_SEED` to an integer to repeat the same sequence of choices |
| `greedy` | The available worker with the shortest estimated task time |

"Available" means online with no running task, so a slower worker still gets work while a faster one is busy. All three make a worker that isn't chosen wait: it gets 204 and asks again after its polling interval.

The greedy estimate is a moving average of `download_seconds + compute_seconds` from a worker's results. Workers with no result yet go first, so each one gets measured, and workers whose results have all failed go last. Simulated tasks all report the same `compute_seconds`, so greedy sees no difference between simulated workers and always prefers the lowest worker ID.

**Joining and leaving.** A worker joins by registering. If it sends no heartbeat for longer than the timeout, the coordinator marks it `offline`. A background check runs every third of the timeout, and `GET /workers` and `POST /tasks/request` also check first. When a worker is marked offline, its unfinished tasks go back to the front of the queue for another worker. An offline worker gets 409 on task requests and result submissions until it sends a heartbeat or registers again. A worker that registers again after a restart gives up any tasks it held, since it lost them in the restart.

Try it against `make up`:

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json' -d '{"star_ids": ["WASP-18"]}'
curl -X POST localhost:8000/tasks/request -H 'content-type: application/json' -d '{"worker_id": "worker-1"}'
curl -X POST localhost:8000/tasks/t1/result -H 'content-type: application/json' \
     -d '{"worker_id": "worker-1", "result": {"period": 0.94136}}'
curl localhost:8000/tasks/t1
```

### Pheromone table

The ant colony scheduler will keep its scores in a pheromone table ([pheromone.py](src/antsearch/pheromone.py)): one level for each worker and task type, recording how well that worker handles that kind of star. The levels are stored in Redis, one hash per task type (`antsearch:pheromone:short` and `antsearch:pheromone:multi_sector`, mapping worker ID to level), so they survive a coordinator restart. `make down` clears them along with everything else in Redis.

| Task type | Stars with |
|---|---|
| `short` | One TESS sector or none (`num_sectors` ≤ 1 in the star list) |
| `multi_sector` | Two or more sectors |

`PheromoneTable(redis_client)` gives `get(worker, type)`, `levels(type, workers)` for a set of workers in one round trip, `set`, `add` (atomic in Redis, so deposits that arrive together all count), `table()`, `forget_worker` and `clear`. A pair that hasn't been written yet reads as the initial level (1.0 by default). The table only stores levels. Assigning tasks by pheromone and the deposit and evaporation rules come later.

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

Network and MAST errors are retried 3 times by default (`--attempts`), waiting 2 s and then 4 s. Downloads are cached under `~/.cache/lightkurve`, so running the same star and sector again reads the file from disk without contacting MAST. If a cached download is corrupt, usually because an earlier download was cut off, it's deleted and downloaded again.

To try another star, pass `--target`, `--known-period` and `--sector`, e.g. `--target "Pi Men" --known-period 6.2678 --sector 1`. Run with `--help` for the other options.

It flattens twice: the first BLS pass finds the transits, then the light curve is re-flattened with them masked so the detrending filter doesn't eat the dip.

For workers, the same search is packaged as `pipeline.run_task(star_id)`, which takes any name MAST resolves (such as `TOI-102`), searches the star's first SPOC 2-minute light curve and returns a JSON-ready dict: the best period, mid-transit time, duration and depth, the BLS `power` (its signal-to-noise objective), and `download_seconds` and `compute_seconds`. A cached star's download time drops to tens of milliseconds. For TOI-102 (known period 4.4119 d):

```json
{"star_id": "TOI-102", "sector": 1, "period": 4.41305, "t0": 1326.0756, "duration": 0.12,
 "depth": 0.0131, "power": 0.283, "download_seconds": 52.56, "compute_seconds": 7.40}
```

## Project layout

```
AntSearch/
├── src/antsearch/
│   ├── coordinator.py  # FastAPI coordinator: workers, heartbeats, task queue, results (Redis)
│   ├── metrics.py      # completion time, throughput and wasted work from a run log
│   ├── pheromone.py    # pheromone table: a level per worker and task type, stored in Redis
│   ├── pipeline.py     # transit search: download, flatten, BLS, folded plot; run_task() for workers
│   ├── runlog.py       # run log: one JSON line per assignment, result and failure
│   ├── scenario.py     # failure scenarios: CPU limits, network delay, kills and restarts on a timeline
│   ├── submit.py       # queue stars from a star list and wait for the results
│   ├── worker.py       # worker: registers, heartbeats, pulls tasks, runs them, reports results
│   └── datasets/       # TESS star list (tess_data_mixed.csv) and the script that rebuilt it
├── scenarios/          # example failure scenarios for antsearch.scenario
├── tests/              # pytest tests; test_integration.py needs a running cluster
├── Makefile            # build, run, test and coverage shortcuts (`make help`)
├── pyproject.toml      # package metadata, dependencies, pytest config
├── Dockerfile          # one image for the coordinator and the workers
├── compose.yaml        # local cluster: redis, coordinator, worker-1, worker-2
└── .github/workflows/  # CI
```

To add a runtime dependency, list it under `dependencies` in `pyproject.toml`. Dev-only tools go under `[project.optional-dependencies] dev`, and the light-curve libraries (lightkurve, astropy, matplotlib, numpy) under `science`. Then rerun `make install`.
