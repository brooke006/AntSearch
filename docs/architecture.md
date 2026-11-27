# Architecture

AntSearch searches TESS light curves for transiting planets across a cluster of volunteer machines. A coordinator holds the task queue and picks which worker gets each star; workers download a star's light curve, run the transit search and send back the result. Every run leaves a log that the metrics and recall scripts turn into results.

## Components, files, inputs and outputs

```mermaid
flowchart LR
    subgraph inputs["Inputs"]
        archive["NASA Exoplanet Archive<br/>TOI table"]
        stars["Star list<br/>datasets/tess_data_mixed.csv<br/>200 TOIs: has_planet, period, num_sectors"]
        mast["MAST<br/>TESS 2-minute light curves"]
        settings["Settings<br/>SCHEDULER, REPLICAS, HEARTBEAT_TIMEOUT,<br/>SCHEDULER_ALPHA / BETA / EVAPORATION"]
        scenarios["Failure scenarios<br/>scenarios/*.yaml"]
    end

    rebuild["datasets/reconstruct_tess_data_mixed.py<br/>rebuild the star list"]
    submit["submit.py<br/>queue stars, wait for results"]

    subgraph coordhost["Coordinator machine"]
        coordinator["coordinator.py<br/>FastAPI: workers, heartbeats,<br/>task queue, results"]
        scheduler["scheduler.py<br/>first-come, round-robin, random, greedy, ant"]
        pheromone["pheromone.py<br/>pheromone level per worker and task type"]
        validation["validation.py<br/>compare replicated results, settle a task"]
        runlog["runlog.py<br/>one JSON line per event"]
        redis[("Redis<br/>workers, tasks, queue,<br/>pheromone, run name")]
    end

    subgraph workerhost["Each worker machine"]
        worker["worker.py<br/>register with machine info,<br/>heartbeat, pull, report"]
        pipeline["pipeline.py<br/>download, flatten, BLS search"]
        cache[("lightkurve cache<br/>~/.cache/lightkurve")]
    end

    scenario["scenario.py<br/>CPU limits, network delay,<br/>kill and restart workers"]
    metrics["metrics.py<br/>completion time, throughput,<br/>wasted work, per-worker table"]
    recall["recall.py<br/>planet recall,<br/>unvetted signals"]
    sweep["sweep.py<br/>ant scheduler parameter sweep<br/>on simulated workers"]

    subgraph outputs["Outputs"]
        runs[("output/runs/*.jsonl<br/>one log per run")]
        saved["docs/runs/<br/>saved log, metrics .txt,<br/>-recall.txt and -recall.csv"]
        scenlog["output/scenarios/*.jsonl"]
        sweepdocs["docs/parameter-sweep-results.md<br/>docs/parameter-sweep-runs.csv"]
        plot["output/folded_transit.png<br/>science check"]
        console["Results table on the console"]
    end

    archive --> rebuild --> stars
    stars --> submit
    submit -- "POST /tasks<br/>GET /tasks/{id}" --> coordinator
    submit --> console
    settings --> coordinator
    coordinator <--> redis
    coordinator --> scheduler --> pheromone
    pheromone <--> redis
    coordinator --> validation
    coordinator --> runlog --> runs

    worker -- "POST /workers/register<br/>POST /workers/{id}/heartbeat<br/>POST /tasks/request<br/>POST /tasks/{id}/result" --> coordinator
    worker --> pipeline
    mast --> pipeline
    pipeline <--> cache
    pipeline --> plot

    scenarios --> scenario
    scenario -- "docker update, tc, kill, start" --> worker
    scenario --> scenlog

    runs --> metrics
    runs --> recall
    stars --> recall
    metrics --> saved
    recall --> saved

    sweep -.->|"runs create_app and the schedulers<br/>in process, on a fake clock"| coordinator
    stars --> sweep
    sweep --> sweepdocs
```

Solid arrows are data at run time. The dotted arrow is `sweep.py` reusing the coordinator's code in-process rather than talking to a running one.

## A task's life

Each star is one task. With `REPLICAS=2` (the default) it runs on two different workers, and the coordinator only accepts a result when both match.

```mermaid
sequenceDiagram
    participant S as submit.py
    participant C as coordinator.py
    participant Sch as scheduler.py
    participant V as validation.py
    participant R as Redis
    participant L as run log
    participant W as worker.py
    participant P as pipeline.py
    participant M as MAST

    S->>C: POST /tasks {star_ids, num_sectors}
    C->>R: queue one task per star
    C->>L: task_queued

    loop every worker, until stopped
        W->>C: POST /tasks/request {worker_id}
        C->>Sch: assign(worker, queued tasks, workers)
        Sch-->>C: a task, or none
        C->>R: mark the run assigned
        C->>L: task_assigned
        C-->>W: {task_id, star_id}, or 204 if nothing fits
        W->>P: run_task(star_id)
        P->>M: download the first SPOC 2-minute light curve
        M-->>P: light curve (cached on disk after the first time)
        P-->>W: period, t0, duration, depth, power, snr, timings
        W->>C: POST /tasks/{id}/result
        C->>L: result_received
        C->>V: decide(results so far, quorum)
        alt results match
            C->>Sch: on_result for each run (pheromone deposit, or penalty if outvoted)
            C->>L: task_done (and result_disputed for an outvoted run)
        else results disagree
            C->>R: queue a rerun on another worker
            C->>L: task_rerun
        end
    end

    Note over W,C: Workers heartbeat in the background. A worker that misses<br/>HEARTBEAT_TIMEOUT goes offline and its runs are requeued (task_requeued).

    S->>C: GET /tasks/{id} until every task is done
    C-->>S: results
```

## Deployment

The same Docker image runs the coordinator and the workers. The coordinator side always runs in Docker Compose; workers can be containers or plain Python processes on any machine that can reach the coordinator's port.

```mermaid
flowchart TB
    subgraph local["make up: one machine"]
        direction LR
        r1[("redis")] --- c1["coordinator :8000"]
        c1 --- w1["worker-1"]
        c1 --- w2["worker-2"]
        c1 --- w3["worker-3"]
    end

    subgraph multi["make coordinator + workers on several machines"]
        direction LR
        r2[("redis")] --- c2["coordinator :8000<br/>on the LAN or Tailscale address"]
        c2 --- n1["make worker<br/>teammate laptop"]
        c2 --- n2["make worker<br/>another laptop"]
        c2 --- d1["docker run antsearch:dev<br/>spare PC"]
        c2 --- m1["make machines<br/>5 sized containers"]
    end
```

## Files

| File | What it does | Inputs | Outputs | Run with |
|---|---|---|---|---|
| `src/antsearch/coordinator.py` | HTTP API for workers and clients; keeps all state in Redis; assigns tasks through the scheduler; settles replicated results | HTTP requests, settings from the environment, Redis | HTTP responses, Redis state, run log events | `make up`, `make coordinator` |
| `src/antsearch/scheduler.py` | Picks a task for a worker that asks: first-come, round-robin, random, greedy or ant (pheromone and recent speed) | Queued tasks, workers, pheromone levels | The chosen task; pheromone updates on each result | via the coordinator |
| `src/antsearch/pheromone.py` | Pheromone level per worker and task type (short or multi-sector), with evaporation and bounds | Deposits and penalties from the scheduler | Levels stored in Redis | via the scheduler |
| `src/antsearch/validation.py` | Compares results from different workers (ignoring timings, allowing rounding) and decides accept, wait, rerun or unresolved | Results of a task's runs | A decision | via the coordinator |
| `src/antsearch/runlog.py` | Appends one JSON line per event; reads logs back | Events from the coordinator | `output/runs/<run>.jsonl` | via the coordinator |
| `src/antsearch/worker.py` | Registers with its machine details, sends heartbeats, pulls tasks, runs them, reports results with retries | `COORDINATOR_URL`, `WORKER_ID`, `WORKER_TASK` | Results sent to the coordinator | `make worker`, compose workers, `make machines` |
| `src/antsearch/pipeline.py` | Transit search: download a light curve, flatten it, run BLS twice, return the best signal and its SNR; also a single-star science check | Star name, MAST, the download cache | Result dict; `output/folded_transit.png` from the science check | via workers; `python -m antsearch.pipeline` |
| `src/antsearch/submit.py` | Queues stars from the star list and waits for them | Star list CSV, `--limit` | Tasks on the coordinator; results table on the console | `make submit` |
| `src/antsearch/metrics.py` | Completion time, turnaround, throughput, wasted work and a line per worker | A run log | Report on the console; with `--save`, the log and `<run>.txt` in `docs/runs/` | `make metrics`, `make save-run` |
| `src/antsearch/recall.py` | Share of known planets found again; strong signals on stars with no known planet | A run log, the star list | Report on the console; with `--save`, `<run>-recall.txt` and `.csv` | `make recall`, `make save-run` |
| `src/antsearch/scenario.py` | Applies failures to compose workers on a timeline | `scenarios/*.yaml`, the running stack | Docker changes; `output/scenarios/<name>.jsonl` | `make scenario` |
| `src/antsearch/sweep.py` | Runs the coordinator and ant scheduler in-process on simulated workers over a grid of alpha, beta and evaporation | The star list, parameter grid | `docs/parameter-sweep-results.md`, `docs/parameter-sweep-runs.csv` | `make sweep` |
| `src/antsearch/datasets/tess_data_mixed.csv` | The 200-star list: 100 confirmed planets, 100 false positives | | Read by submit, recall and sweep | |
| `src/antsearch/datasets/reconstruct_tess_data_mixed.py` | Rebuilds the star list | NASA Exoplanet Archive TOI table, MAST sector counts | The star list CSV | `python` by hand |
| `compose.yaml` | Redis, the coordinator and three workers on one machine | Settings from the environment | Containers on port 8000 | `make up` |
| `compose.machines.yaml` | Five worker containers with different CPU and memory limits | `COORDINATOR_URL` | Containers that join a coordinator | `make machines` |
| `Dockerfile` | One image for the coordinator and workers, with the science libraries and `tc` | | `antsearch:dev` | `make build` |
| `Makefile` | Shortcuts for everything above | | | `make help` |
