# AntSearch

[![tests](https://github.com/brooke006/AntSearch/actions/workflows/tests.yml/badge.svg)](https://github.com/brooke006/AntSearch/actions/workflows/tests.yml)

A Python volunteer-computing system for exoplanet searches using ant-inspired task scheduling.

See [docs/architecture.md](docs/architecture.md) for diagrams of the components, which file does what, and the system's inputs and outputs.

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
| `make coordinator` | Start only Redis and the coordinator, for workers on other machines; see [Running across several machines](#running-across-several-machines) |
| `make worker` | Run a worker on this machine (`COORDINATOR_URL=...` and `WORKER_ID=...`) |
| `make machines` | Start four worker containers with different CPU and memory limits that join `make coordinator`, to rehearse a multi-machine run |
| `make logs` / `make ps` | Follow the cluster's logs / list its containers |
| `make submit` | Queue the first 50 stars of the TESS list on the cluster and wait for the results (`STARS=...` for another count) |
| `make scenario` | Run a failure scenario on the cluster (`SCENARIO=scenarios/slow-worker.yaml`); see [Failure scenarios](#failure-scenarios) |
| `make metrics` | Completion time, throughput and wasted work of the newest run (`RUN=output/runs/<run>.jsonl` for another); see [Run logs and metrics](#run-logs-and-metrics) |
| `make recall` | Share of known planets the newest run (or `RUN=...`) found again, and strong signals on stars with no known planet; see [Planet recall](#planet-recall) |
| `make save-run` | Copy the newest run log (or `RUN=...`) into `docs/runs/`, with its metrics and recall next to it |
| `make sweep` | Simulate the ant scheduler over a grid of alpha, beta and evaporation (about 11 minutes on 10 cores); see [docs/parameter-sweep.md](docs/parameter-sweep.md) |
| `make test` | Unit and science tests (everything that doesn't need Docker) |
| `make test-unit` | Fast tests only: coordinator, replication, worker, submit, scenarios, run logs, pheromone table, package |
| `make test-science` | Light-curve pipeline tests |
| `make test-integration` | Start a throwaway cluster, test it over HTTP, tear it down |
| `make test-all` | `test` plus `test-integration` |
| `make coverage` | `make test` with a coverage report |
| `make clean` | Remove test caches and coverage output |

## Running tests

Tests come in three types, selected with pytest markers:

| Type | Command | Covers | Needs |
|---|---|---|---|
| Unit | `make test-unit` | Coordinator endpoints (against [fakeredis](https://github.com/cunla/fakeredis-py)), the worker loop, the submit script, failure scenarios (against a fake Docker and clock), the pheromone table, replication and result checks (including a worker that returns garbage), machine details and the per-worker report, two in-process workers processing 50 stars, package | Nothing; takes seconds |
| Science | `make test-science` | Pipeline: flatten, BLS, plotting, `run_task`, download retries and exit codes, using a synthetic light curve and faked MAST errors | The `science` extra (skipped without it); no network |
| Integration | `make test-integration` | The real cluster over HTTP: real Redis, the worker containers processing 50 tasks, a full task round trip, a garbage result outvoted and its worker flagged, a silent worker going offline and its task finished by another, a failure scenario slowing, killing and restarting a worker on schedule | Docker |

`make test` (or plain `python -m pytest`) runs the unit and science tests. The integration tests are skipped unless `ANTSEARCH_URL` points at a coordinator. `make test-integration` sets that for you: it starts a separate compose project (`antsearch-it`) on port 8001 with a 3-second heartbeat timeout, runs the tests, and removes it again, so it doesn't touch a cluster you started with `make up`. Its workers run simulated tasks (`WORKER_TASK=simulated`), so the tests don't download from MAST.

To run the same selections with pytest directly, use `-m "not science and not integration"`, `-m science` or `-m integration`.

### Coverage

```bash
make coverage
```

This prints a per-file summary with the uncovered line numbers and writes an HTML report to `htmlcov/index.html` (open it in a browser to see uncovered lines highlighted). It covers the unit and science tests. The integration tests run code inside containers, so they don't count towards coverage.

GitHub Actions runs `python -m pytest` (unit and science) on every push and pull request, on Python 3.10 to 3.13. See [.github/workflows/tests.yml](.github/workflows/tests.yml).

## Running the local cluster

With [Docker](https://docs.docker.com/get-docker/) running, start Redis, the coordinator and three workers:

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

Each star runs on two workers and is accepted once their results match (see [Replication](#replication)). It prints a line per star (whose result was accepted, and the period and depth found or the error), then the total time and how many accepted results came from each worker. Each star takes about a minute, mostly searching MAST and downloading the light curve. `python -m antsearch.submit --help` lists the options, including `--stars` for another CSV and `--no-wait`.

`make down` stops the cluster. `make up` always rebuilds the images, so it picks up code changes.

## Workers

`python -m antsearch.worker` (what the compose workers run) joins a coordinator and works until it gets SIGINT or SIGTERM:

1. Register, then send heartbeats from a background thread so the worker stays online through long tasks.
2. Request a task. If the queue is empty, or the coordinator can't be reached, wait `POLL_INTERVAL` seconds and ask again.
3. Run the task: by default `pipeline.run_task`, the transit search on that star's first SPOC 2-minute light curve. A task that raises (say the star has no light curve on MAST) is reported with an `{"error", "message"}` result instead.
4. Report the result, retrying network errors. If the coordinator answers 409, the worker was marked offline and the task went to someone else, so the result is dropped.

When it registers, the worker sends a short description of its machine: OS, CPU, core count, memory, and its Python, numpy, astropy and lightkurve versions. If the coordinator can't be reached at startup, it keeps trying. If the coordinator forgets the worker or marks it offline, the worker registers again. On SIGTERM it finishes and reports the task it's running before it exits (compose gives it three minutes).

| Variable | Default | Meaning |
|---|---|---|
| `COORDINATOR_URL` | `http://localhost:8000` | Coordinator to join |
| `WORKER_ID` | the host name | Name to register as |
| `WORKER_TASK` | `transit` | `transit` runs the transit search; `simulated` waits `SIMULATED_TASK_SECONDS` and returns a fake result, for tests and scheduler experiments that shouldn't hit MAST |
| `SIMULATED_TASK_SECONDS` | `1` | How long a simulated task takes |
| `POLL_INTERVAL` | `2` | Seconds between task requests while the queue is empty |

To run a worker outside Docker against a cluster started with `make up`: `make worker COORDINATOR_URL=http://localhost:8000 WORKER_ID=laptop`.

A transit-search result looks like this (TOI-102, known period 4.4119 d):

```json
{"star_id": "TOI-102", "sector": 1, "period": 4.41305, "t0": 1326.0756, "duration": 0.12,
 "depth": 0.0131, "power": 0.283, "snr": 317.0, "download_seconds": 43.96, "compute_seconds": 6.31}
```

## Running across several machines

To run one cluster across several computers, one machine runs the coordinator and every machine runs a worker. The workers need to reach the coordinator. That works on the same Wi-Fi or a phone hotspot. Teammates who are somewhere else can use [Tailscale](https://tailscale.com): everyone installs it and signs in to one tailnet, or the coordinator's owner shares that machine with them.

On the coordinator machine:

```bash
make coordinator        # Redis and the coordinator, no local workers
```

It prints the address to use, e.g. `http://192.168.1.20:8000`, and the Tailscale address too if the machine is on one. Then on every machine, including the coordinator's own, clone the repo, run the [setup](#setup), and start a worker with a name no other machine uses:

```bash
make worker COORDINATOR_URL=http://192.168.1.20:8000 WORKER_ID=daniel-laptop
```

On Windows without `make`, in PowerShell with the venv active: `$env:COORDINATOR_URL="http://192.168.1.20:8000"; $env:WORKER_ID="spare-pc"; python -m antsearch.worker`.

A machine without Python can run the Docker image instead. The worker then reports the Docker VM rather than the real hardware:

```bash
make build
docker run --rm --stop-timeout 180 -e COORDINATOR_URL=http://192.168.1.20:8000 -e WORKER_ID=spare-pc antsearch:dev
```

A worker in a container reports its CPU and memory limits, if it has them, instead of the VM's. To rehearse a run on one computer, `make machines` starts four worker containers sized like different computers, from a 4-CPU "desktop" down to a half-CPU "netbook". They join `make coordinator` through this machine's network address, the same way other machines would. Teammates can run it too, with `make machines COORDINATOR_URL=http://<address>:8000`. The container names start with each person's user name, e.g. `daniel-netbook`, so they don't clash. A real transit search mostly waits on MAST downloads, so the CPU limits barely change how fast they answer. With `make machines WORKER_TASK=simulated`, each one takes a different time per star (0.5 s to 4 s), which shows how the scheduler handles uneven workers. `make down` stops them along with the coordinator.

`curl http://192.168.1.20:8000/workers` shows who has joined and what each machine is. Once everyone is in, queue the stars from the coordinator machine with `make submit`. When the run is finished, `make save-run` copies the run log into `docs/runs/` with its metrics next to it. The metrics end with one line per worker: its runs, failures, outvoted results, lost runs, mean time per answer, and the machine it ran on.

Things that can go wrong:

- Campus and guest Wi-Fi often block traffic between devices, so the workers can't reach the coordinator. A phone hotspot or [Tailscale](https://tailscale.com) gets around that. On macOS, allow incoming connections for Docker if the firewall asks.
- Two machines with the same `WORKER_ID` would share one worker's tasks. The coordinator logs a warning if an ID registers from a second host while it's still online.
- A laptop that goes to sleep is marked offline and its task goes to another worker, as with any crash. Keep the coordinator machine plugged in and awake; on macOS, leave `caffeinate -i` running in a terminal during the run.
- Different numpy or astropy versions can change a transit search's result slightly, so honest machines can outvote each other. The versions are in the run log's `worker_registered` lines.
- There's no authentication. Only run the coordinator on a network you trust, and don't forward its port to the internet.

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

Before the first event, every worker the scenario names is started if needed and its CPU limit and delay are removed, so runs of the same file start from the same state. Events are timed from the start, so a slow docker command only delays its own event, and events at the same time run in file order. A kill throws away the container's network settings, so the scenario sets the delay again when it starts that worker. In our runs, every event landed within 5 ms of its planned time.

Notes:

- CPU limits slow the transit search, but not simulated tasks (`WORKER_TASK=simulated`), which only sleep.
- Network delay applies to everything the worker sends: heartbeats, task requests, results and MAST downloads.
- The worker containers need `NET_ADMIN` and `tc` for the delay. `compose.yaml` and the Dockerfile provide both.
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
| `coordinator_started` | `scheduler`, `seed`, `heartbeat_timeout`, `replicas`, and `alpha`, `beta`, `evaporation` and `late_factor` for the ant scheduler |
| `worker_registered` | `worker_id`, and `machine` (what the worker runs on) if the worker sent it |
| `worker_online` | `worker_id` |
| `worker_offline` | `worker_id`, `last_heartbeat` |
| `task_queued` | `task_id`, `star_id` |
| `task_assigned` | `task_id`, `star_id`, `worker_id`; one for each run of a task |
| `result_received` | `task_id`, `worker_id`, `result` (a failed run's result has an `error` key) |
| `task_rerun` | `task_id`, `runs`, `reason`: the results disagree, so more runs were queued |
| `result_disputed` | `task_id`, `worker_id`, `result`: a result the other workers outvoted |
| `task_done` | `task_id`, `result`, `outcome` (`accepted` or `unresolved`), `agreed` (the workers whose results matched), `worker_id` (the first of them), and for the ant scheduler `pheromone`: each run's worker mapped to `deposited`, `failed`, `late` or `disputed` |
| `task_requeued` | `task_id`, `worker_id`, `reason` (`offline` or `reregistered`), `last_heartbeat` |
| `result_rejected` | `task_id`, `worker_id`, `status`, `result` |

The metrics:

- **Completion time:** from the first task queued to the last task settled. `make metrics` also shows the mean turnaround, the same measure for each task on its own. With a full queue the schedulers have about the same choices, so to compare them, queue tasks gradually and look at turnaround.
- **Throughput:** tasks that succeeded per minute of completion time. Failed runs don't count, so a worker that fails quickly can't look fast.
- **Wasted work:** worker time that produced no answer, measured from assignment. For a task taken from a worker, that's up to the worker's last heartbeat, or up to its result if it turns out to be alive and reports late. For a failed run, it's up to the error result, and for an outvoted run, up to its result. It's shown as seconds and as a share of all worker time, with lost, failed and outvoted work listed separately. Worker time counts every run, so with replication it's at least twice the useful work; tasks are counted once each. `make metrics` also shows how many extra runs mismatches caused.
- **Per worker:** runs, failed runs, outvoted runs, lost runs, mean time per answer, and the worker's machine.

Times are spans on the coordinator's clock rather than what workers report, so they work for failed runs and don't depend on the workers' clocks. They include time on the network.

On Linux, the coordinator container's user (UID 1000) must be able to write to the log directory. `make up` creates it as you; `docker compose up` on its own would create it as root, and the coordinator would exit with a permission error.

## Coordinator API

Workers and tasks are stored in Redis under `antsearch:` keys (the layout is described at the top of [coordinator.py](src/antsearch/coordinator.py)), so the coordinator can restart without losing them. Scheduler state (the round-robin position, the random choice, the greedy and ant speed estimates) is kept in memory and starts over when the coordinator restarts; the ant scheduler's pheromone table is in Redis and survives it. Run one coordinator process.

| Endpoint | Body | Response |
|---|---|---|
| `POST /workers/register` | `{"worker_id": "w1"}`, optionally with `"machine": {...}`, flat key/value pairs describing the machine | `{"worker_id", "registered": true, "heartbeat_timeout"}` |
| `POST /workers/{id}/heartbeat` | none | `{"worker_id", "status": "online"}`; 404 if not registered |
| `GET /workers` | none | Every worker with its status, last heartbeat, assigned tasks, how many of its results were outvoted (`disputed`) whether it's `flagged`, and its `machine` |
| `POST /tasks` | `{"star_ids": ["WASP-18", ...]}`, optionally with `"num_sectors": [2, ...]`, one per star, to set each task's type | 201, `{"task_ids": ["t1", ...]}`; 422 if `num_sectors` doesn't match `star_ids` |
| `POST /tasks/request` | `{"worker_id": "w1"}` | `{"task_id", "star_id"}`; 204 if no task is available for this worker; 404 if not registered; 409 if offline |
| `POST /tasks/{id}/result` | `{"worker_id": "w1", "result": {...}}` | `{"task_id", "status"}`: `done` once enough results match, `assigned` or `pending` while it waits for more; 409 if the task isn't assigned to that worker |
| `GET /tasks/{id}` | none | The task's star and status, and its `runs` (each worker's status, timestamps, result and, once settled, verdict). Once done: the accepted `result`, the `worker_id` it came from, the `outcome` and `completed_at` |
| `GET /health` | none | `{"status": "ok"}`, or 503 if Redis is unreachable |

The coordinator started with `python -m antsearch.coordinator` uses round robin unless `SCHEDULER` says otherwise (see [Schedulers](#schedulers)). Online workers without a running task take turns in worker-ID order. A worker requesting out of turn gets 204 and tries again after its normal polling interval. Busy and offline workers are skipped. A worker that finishes can keep taking work while the others are busy. An idle worker that stops requesting work but continues sending heartbeats can delay the others; it remains eligible until it takes work or goes offline.

`Scheduler.assign(worker_id, tasks, workers)` returns a pending task ID or `None`. It does not change Redis. The coordinator serializes scheduling and claims the selected task in a Redis transaction. `on_result(worker_id, result, task_id, seconds, disputed)` is called for each run of a task once the task is settled, with `disputed` set if the run's result was outvoted (or nothing was agreed); round robin does not use it. `create_app` takes a `scheduler` argument; without one it hands out tasks in queue order.

### Replication

Each star runs on `REPLICAS` different workers (default 2) and is accepted once that many results match ([validation.py](src/antsearch/validation.py)). Two results match if they have the same keys and values. Timings (keys ending in `_seconds`) are ignored, and numbers may differ by a relative 1e-6, since two machines can round the same calculation differently. Failed runs match if they failed with the same error type, so a star MAST has no data for is accepted as failed.

When the results disagree, another worker that hasn't run the star gets a further run, up to `2 * REPLICAS - 1` runs (3 by default), so the majority wins. If still no two agree, the task is done with outcome `unresolved` and an `Unresolved` error as its result.

A worker whose answer is outvoted is flagged. `GET /workers` shows `"flagged": true` and how many of its results were `disputed`, the run log records `result_disputed`, and the ant scheduler gives it no pheromone for that run. A failed run also gets another run, but no flag: a download error isn't a wrong answer.

A worker never runs the same star twice, so settling a mismatch needs a worker that hasn't run it yet. That's why the compose stack has three workers. With only two, a disputed star waits until a third one joins. `REPLICAS=1 make up` turns replication off; each star then takes the first result, as before. The coordinator from `create_app` on its own doesn't replicate either.

### Schedulers

Set `SCHEDULER` to choose one, for example `SCHEDULER=greedy make up`:

| `SCHEDULER` | Each task goes to |
|---|---|
| `round-robin` (default) | The next available worker in worker-ID order |
| `random` | An available worker chosen uniformly at random. Set `SCHEDULER_SEED` to an integer to repeat the same sequence of choices |
| `greedy` | The available worker with the shortest estimated task time |
| `ant` | An available worker chosen at random, weighted by its pheromone for the task's type and its recent speed (Ant System). `SCHEDULER_ALPHA` and `SCHEDULER_BETA` set the weights, `SCHEDULER_EVAPORATION` and `SCHEDULER_LATE_FACTOR` how the pheromone is updated; `SCHEDULER_SEED` works as for `random` |

"Available" means online with no running task, so a slower worker still gets work while a faster one is busy. All of them make a worker that isn't chosen wait: it gets 204 and asks again after its polling interval.

The greedy estimate is a moving average of `download_seconds + compute_seconds` from a worker's results. Workers with no result yet go first, so each one gets measured, and workers whose results have all failed go last. Simulated tasks all report the same `compute_seconds`, so greedy sees no difference between simulated workers and always prefers the lowest worker ID.

The ant scheduler gives each available worker a chance of

```
pheromone ** alpha * speed ** beta / (the same, summed over the available workers)
```

`pheromone` is the worker's level in the [pheromone table](#pheromone-table) for the type of the task at the front of the queue, and `speed` is 1 / its estimated task time, the same moving average greedy uses. `alpha` (`SCHEDULER_ALPHA`, default 1) sets how much the learned pheromone counts, and `beta` (`SCHEDULER_BETA`, default 4) how much the current speed counts. The defaults come from the [parameter sweep](docs/parameter-sweep.md). With both at 0 the choice is uniform, like `random`. A slower worker keeps a smaller but non-zero chance, so it is still tried now and then. A worker with no timed result yet counts as fast as the fastest measured one, so new workers get tried; one whose results have all failed counts as slow as the slowest. Like `random`, the choice stands until that worker asks, so polling more often doesn't win more tasks. The run log's `coordinator_started` line records `alpha` and `beta` for an ant run.

#### Evaporation and deposit

Every run of a task is a round, applied once the task is settled. First every stored level evaporates: it's multiplied by `1 - evaporation` (`SCHEDULER_EVAPORATION`, default 0.2). Then the worker that finished earns a deposit for that task's type:

```
deposit = evaporation * typical / seconds
```

`seconds` is how long the task took on the coordinator's clock, from assignment to result, so a worker can't earn more by reporting false timings. `typical` is the recent average for that type of task. A task done at the usual speed earns `evaporation`, and one done twice as fast earns twice that. A worker that does every task at the usual speed therefore stays at 1, while one that is slower, or gets fewer tasks, settles lower. Some results earn nothing, and evaporation alone then wears the worker's level down:

| Result | Why it earns nothing |
|---|---|
| Failed | It has an `error` key |
| Late | It took more than `SCHEDULER_LATE_FACTOR` (default 3) times the typical time for its type |
| Disputed | Other workers outvoted its answer, or no answer was agreed (see [Replication](#replication)) |

Levels stay between 0.2 and 1, as in MAX-MIN Ant System (Stützle and Hoos, 2000). Without the ceiling, whichever worker wins the first few tasks earns more pheromone, so it wins more and soon takes almost everything (Ant System's "stagnation"). The floor keeps a punished worker in the running, so it is still tried now and then. A pair nobody has written yet reads as 1, so a new worker gets tried. With `SCHEDULER_EVAPORATION=0`, levels never change.

What to expect, with tasks handed out one at a time so both workers are free at every choice:

- A worker that slows down five-fold drops below a quarter of the choices within about 1 round, and within 4 in all 30 seeds tried.
- Two equal workers still drift apart for stretches: over 50 tasks, one's share ranged from 10% to 76% across the same seeds, against 32% to 64% with evaporation off. A lower evaporation rate is fairer to equal workers but punishes slower. The [parameter sweep](docs/parameter-sweep.md) found the rate makes almost no difference to turnaround, so it stays at 0.2.
- Simulated workers all report the same time, so between them only luck separates the levels.

#### Joining and leaving

A worker joins by registering. If it sends no heartbeat for longer than the timeout, the coordinator marks it `offline`. A background check runs every third of the timeout, and `GET /workers` and `POST /tasks/request` also check first. When a worker is marked offline, its unfinished tasks go back to the front of the queue for another worker. An offline worker gets 409 on task requests and result submissions until it sends a heartbeat or registers again. A worker that registers again after a restart gives up any tasks it held, since it lost them in the restart.

Try it against `make up`:

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json' -d '{"star_ids": ["WASP-18"]}'
curl -X POST localhost:8000/workers/register -H 'content-type: application/json' -d '{"worker_id": "me"}'
curl -X POST localhost:8000/tasks/request -H 'content-type: application/json' -d '{"worker_id": "me"}'
curl -X POST localhost:8000/tasks/t1/result -H 'content-type: application/json' \
     -d '{"worker_id": "me", "result": {"period": 0.94136}}'
curl localhost:8000/tasks/t1
```

The compose workers are asking for work too, so the request may get 204 (repeat it, or queue another star). Once the compose workers have run the star, their real results outvote the made-up one: `curl localhost:8000/tasks/t1` shows the verdict for each run, and `curl localhost:8000/workers` shows `me` flagged.

### Pheromone table

The ant scheduler keeps its scores in a pheromone table ([pheromone.py](src/antsearch/pheromone.py)): one level for each worker and task type, recording how well that worker handles that kind of star. The levels are stored in Redis, one hash per task type (`antsearch:pheromone:short` and `antsearch:pheromone:multi_sector`, mapping worker ID to level), so they survive a coordinator restart. `make down` clears them along with everything else in Redis.

| Task type | Stars with |
|---|---|
| `short` | One TESS sector or none (`num_sectors` ≤ 1 in the star list) |
| `multi_sector` | Two or more sectors |

`PheromoneTable(redis_client)` gives `get(worker, type)`, `levels(type, workers)` for a set of workers in one round trip, `set`, `add` (atomic in Redis, so deposits that arrive together all count), `table()`, `forget_worker` and `clear`. A pair that hasn't been written yet reads as the initial level (1.0 by default). `evaporate(rate, minimum)` scales every stored level in one go, and `ensure` stores a pair at the initial level so evaporation reaches it. The `ant` scheduler reads the levels to choose workers and updates them after every task (see [Schedulers](#schedulers)).

A task gets its type from the `num_sectors` it was queued with; `make submit` sends them from the star list's `num_sectors` column. A task queued without one counts as `short`, which is also what the transit search actually runs today: it searches only the star's first sector.

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

For workers, the same search is packaged as `pipeline.run_task(star_id)`, which takes any name MAST resolves (such as `TOI-102`), searches the star's first SPOC 2-minute light curve and returns a JSON-ready dict: the best period, mid-transit time, duration and depth, the BLS `power`, the signal-to-noise ratio `snr`, and `download_seconds` and `compute_seconds`. BLS runs without flux errors, so `power` is the signal-to-noise for errors of 1; `snr` divides it by the flattened light curve's scatter (1.4826 times the median absolute deviation) to give the real one. A cached star's download time drops to tens of milliseconds. For TOI-102 (known period 4.4119 d):

```json
{"star_id": "TOI-102", "sector": 1, "period": 4.41305, "t0": 1326.0756, "duration": 0.12,
 "depth": 0.0131, "power": 0.283, "snr": 317.0, "download_seconds": 52.56, "compute_seconds": 7.40}
```

## Planet recall

`make recall` checks a run's results against the star list: how many of its known planets the search found again, and which stars without a known planet showed a strong signal. It reads the newest run log by default, or `RUN=output/runs/<run>.jsonl`, and `make save-run` saves its report with the run as `<run>-recall.txt`, plus `<run>-recall.csv` with one row per star.

```bash
make recall
```

Each star in the list gets one verdict:

| Verdict | Meaning |
|---|---|
| `recovered` | Known planet, found period within 1% of the known one (`--tolerance`) |
| `harmonic` | Known planet, found at 2 or 3 times the period, or a half or a third of it |
| `missed` | Known planet, found at some other period |
| `unvetted` | No known planet, but a signal with SNR of at least 7.1 (`--min-snr`), the threshold the Kepler pipeline used |
| `quiet` | No known planet and no strong signal |
| `failed` | The search failed, usually because MAST has no 2-minute light curve |
| `unresolved` | The workers never agreed on a result |

Recall is recovered planets over the planet stars with a result (recovered, harmonic or missed). Harmonics don't count. Failed and unresolved stars are left out and reported separately. The report also gives recall over the planets whose known period is in the range searched, 0.5 to 15 days: a planet with a longer period can't be recovered, and with one sector of data it may show only one transit.

The stars with no known planet are TOIs that were ruled false positives, often eclipsing binaries, so many have a real signal. Each unvetted signal is listed with a note saying whether it's the same signal that was ruled out (within the tolerance of its listed period, or a harmonic of it) or a new one. None of them has been checked by eye, which is why they're unvetted rather than candidates.

Results for the whole star list are in [docs/planet-recall.md](docs/planet-recall.md).

The search only reports the strongest signal on each star. A known planet counts as missed if that signal is something else, for example a second planet in the system or stellar variability that flattening didn't remove.

## Project layout

```
AntSearch/
├── src/antsearch/
│   ├── coordinator.py  # FastAPI coordinator: workers, heartbeats, task queue, results (Redis)
│   ├── metrics.py      # completion time, throughput and wasted work from a run log
│   ├── pheromone.py    # pheromone table: a level per worker and task type, stored in Redis
│   ├── pipeline.py     # transit search: download, flatten, BLS, folded plot; run_task() for workers
│   ├── recall.py       # share of known planets a run found again, and unvetted strong signals
│   ├── runlog.py       # run log: one JSON line per assignment, result and failure
│   ├── scenario.py     # failure scenarios: CPU limits, network delay, kills and restarts on a timeline
│   ├── submit.py       # queue stars from a star list and wait for the results
│   ├── sweep.py        # parameter sweep: the ant scheduler on simulated workloads
│   ├── validation.py   # comparing results from different workers, and settling a task
│   ├── worker.py       # worker: registers, heartbeats, pulls tasks, runs them, reports results
│   └── datasets/       # TESS star list (tess_data_mixed.csv) and the script that rebuilt it
├── docs/               # architecture diagrams, parameter sweep write-up and results, references, saved run logs (runs/)
├── scenarios/          # example failure scenarios for antsearch.scenario
├── tests/              # pytest tests; test_integration.py needs a running cluster
├── Makefile            # build, run, test and coverage shortcuts (`make help`)
├── pyproject.toml      # package metadata, dependencies, pytest config
├── Dockerfile          # one image for the coordinator and the workers
├── compose.yaml        # local cluster: redis, coordinator, worker-1 to worker-3
├── compose.machines.yaml  # make machines: four worker containers sized like different computers
└── .github/workflows/  # CI
```

To add a runtime dependency, list it under `dependencies` in `pyproject.toml`. Dev-only tools go under `[project.optional-dependencies] dev`, and the light-curve libraries (lightkurve, astropy, matplotlib, numpy) under `science`. Then rerun `make install`.
