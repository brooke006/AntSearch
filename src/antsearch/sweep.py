"""Parameter sweep for the ant scheduler: alpha, beta and evaporation rate.

    python -m antsearch.sweep                        # full sweep, writes to docs/
    python -m antsearch.sweep --seeds 2 --stars 60   # quick check

Runs the real coordinator against simulated workers on a fake clock, for each setting, workload
and seed, and scores each setting by mean turnaround relative to round-robin (lower is better).
See docs/parameter-sweep.md for the workloads and results.
"""

import argparse
import csv
import heapq
import itertools
import logging
import math
import random
import statistics
import sys
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fakeredis

from antsearch.coordinator import NewTasks, Registration, TaskRequest, TaskResult, create_app, task_type_lookup
from antsearch.pheromone import MULTI_SECTOR, SHORT, PheromoneTable, task_type
from antsearch.scheduler import DEFAULT_ALPHA, DEFAULT_BETA, DEFAULT_EVAPORATION, DEFAULT_LATE_FACTOR, make_scheduler
from antsearch.submit import read_sectors, read_stars

log = logging.getLogger("antsearch.sweep")

ALPHAS = (0.0, 0.5, 1.0, 2.0)
BETAS = (0.0, 1.0, 2.0, 4.0, 8.0)
EVAPORATIONS = (0.05, 0.1, 0.2, 0.4)
LOADS = (0.5, 0.8)
BASELINES = ("round-robin", "random", "greedy")
REFERENCE = "round-robin"
DEFAULT_SEEDS = 10
DEFAULT_OUT_DIR = Path("docs")
RESULTS_FILE = "parameter-sweep-results.md"
RUNS_FILE = "parameter-sweep-runs.csv"

TASK_SECONDS = {SHORT: 10.0, MULTI_SECTOR: 20.0}
NOISE = 0.2
POLL_INTERVAL = 2.0  # the worker's default


@dataclass(frozen=True)
class Worker:
    """A simulated worker. slows_at is a fraction of the way through the arrivals."""

    slowness: dict[str, float] = field(default_factory=lambda: {SHORT: 1.0, MULTI_SECTOR: 1.0})
    slows_at: float | None = None
    slows_by: float = 1.0
    failure_rate: float = 0.0
    fails_after: float = 0.5


WORKLOADS: dict[str, dict[str, Worker]] = {
    "mixed-speeds": {
        "worker-1": Worker(),
        "worker-2": Worker(),
        "worker-3": Worker(slowness={SHORT: 2.0, MULTI_SECTOR: 2.0}),
        "worker-4": Worker(slowness={SHORT: 1.0, MULTI_SECTOR: 4.0}),
    },
    "slowdown": {
        "worker-1": Worker(slows_at=1 / 3, slows_by=5.0),
        "worker-2": Worker(),
        "worker-3": Worker(),
        "worker-4": Worker(),
    },
    "flaky": {
        "worker-1": Worker(),
        "worker-2": Worker(),
        "worker-3": Worker(),
        "worker-4": Worker(failure_rate=0.5),
    },
}


@dataclass(frozen=True, order=True)
class Setting:
    """A baseline by name, or ant with its parameters. evaporation is None when alpha is 0 (it has no effect)."""

    scheduler: str
    alpha: float | None = None
    beta: float | None = None
    evaporation: float | None = None

    @property
    def label(self) -> str:
        if self.scheduler != "ant":
            return self.scheduler
        evaporation = "any" if self.evaporation is None else f"{self.evaporation:g}"
        return f"ant alpha={self.alpha:g} beta={self.beta:g} evaporation={evaporation}"


def ant_settings(alphas: Iterable[float] = ALPHAS, betas: Iterable[float] = BETAS, evaporations: Iterable[float] = EVAPORATIONS) -> list[Setting]:
    """Every combination, with one alpha-0 setting per beta rather than one per evaporation rate."""
    settings = []
    for alpha, beta in itertools.product(alphas, betas):
        for evaporation in [None] if alpha == 0 else evaporations:
            settings.append(Setting("ant", float(alpha), float(beta), evaporation))
    return settings


def coordinator(setting: Setting, seed: int, redis_client, clock: Callable[[], float]) -> dict[str, Callable]:
    """The real coordinator's POST endpoint functions, by path, running setting's scheduler."""
    kwargs: dict[str, Any] = {}
    if setting.scheduler == "ant":
        kwargs = {
            "pheromones": PheromoneTable(redis_client),
            "task_type_of": task_type_lookup(redis_client),
            "alpha": setting.alpha,
            "beta": setting.beta,
            "evaporation": DEFAULT_EVAPORATION if setting.evaporation is None else setting.evaporation,
            "late_factor": DEFAULT_LATE_FACTOR,
        }
    scheduler = make_scheduler(setting.scheduler, seed, **kwargs)
    # Heartbeats aren't simulated and nobody crashes, so nobody should time out.
    app = create_app(redis_client, heartbeat_timeout=math.inf, clock=clock, scheduler=scheduler)
    return {route.path: route.endpoint for route in app.routes if "POST" in getattr(route, "methods", ())}


def arrival_rate(workers: dict[str, Worker], stars: list[tuple[str, int]], load: float) -> float:
    """Stars per second that keep the healthy workers load busy on average."""
    share = {kind: sum(1 for _, sectors in stars if task_type(sectors) == kind) / len(stars) for kind in TASK_SECONDS}
    capacity = sum(
        1 / sum(share[kind] * TASK_SECONDS[kind] * worker.slowness[kind] for kind in TASK_SECONDS) for worker in workers.values()
    )
    return load * capacity


def simulate(setting: Setting, workload: str, load: float, seed: int, stars: list[tuple[str, int]]) -> dict[str, Any]:
    """Run stars through workload under setting's scheduler. Returns the mean turnaround and run counts."""
    workers = WORKLOADS[workload]
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    now = [0.0]
    endpoints = coordinator(setting, seed, redis_client, lambda: now[0])
    add_tasks, request_task = endpoints["/tasks"], endpoints["/tasks/request"]
    submit_result = endpoints["/tasks/{task_id}/result"]

    workload_random = random.Random(f"{workload}/{load}/{seed}")
    gaps = [workload_random.expovariate(arrival_rate(workers, stars, load)) for _ in stars]
    arrivals = list(itertools.accumulate(gaps))
    slowdown_from = arrivals[-1]
    events: list[tuple[float, int, str, Any]] = []
    order = itertools.count()

    def at(when: float, kind: str, data: Any) -> None:
        heapq.heappush(events, (when, next(order), kind, data))

    for worker_id in workers:
        endpoints["/workers/register"](Registration(worker_id=worker_id))
        at(workload_random.uniform(0, POLL_INTERVAL), "ask", worker_id)
    for index, when in enumerate(arrivals):
        at(when, "arrive", index)

    # One random stream per star, so every setting gets the same run times and failures.
    draws = [random.Random(f"{workload}/{load}/{seed}/{index}") for index in range(len(stars))]
    first_queued: dict[int, float] = {}
    succeeded_at: dict[int, float] = {}
    index_of: dict[str, int] = {}  # task ID -> star index
    runs = {worker_id: 0 for worker_id in workers}
    failed_runs = 0
    pending = 0

    def queue(index: int) -> None:
        nonlocal pending
        star, sectors = stars[index]
        task_id = add_tasks(NewTasks(star_ids=[star], num_sectors=[sectors]))["task_ids"][0]
        index_of[task_id] = index
        pending += 1

    while events and len(succeeded_at) < len(stars):
        now[0], _, kind, data = heapq.heappop(events)
        if kind == "arrive":
            first_queued[data] = now[0]
            queue(data)
        elif kind == "ask":
            worker_id = data
            # Asking with nothing queued changes nothing in the coordinator, so the call is skipped.
            answer = request_task(TaskRequest(worker_id=worker_id)) if pending else None
            if not isinstance(answer, dict):  # None or a 204 response
                at(now[0] + POLL_INTERVAL, "ask", worker_id)
                continue
            pending -= 1
            task_id = answer["task_id"]
            index = index_of[task_id]
            worker = workers[worker_id]
            kind_of_task = task_type(stars[index][1])
            seconds = TASK_SECONDS[kind_of_task] * worker.slowness[kind_of_task] * draws[index].uniform(1 - NOISE, 1 + NOISE)
            if worker.slows_at is not None and now[0] >= worker.slows_at * slowdown_from:
                seconds *= worker.slows_by
            failed = draws[index].random() < worker.failure_rate
            if failed:
                seconds *= worker.fails_after
            at(now[0] + seconds, "finish", (worker_id, task_id, seconds, failed))
        elif kind == "finish":
            worker_id, task_id, seconds, failed = data
            index = index_of[task_id]
            runs[worker_id] += 1
            star = stars[index][0]
            result = {"star_id": star, "error": "SimulatedFailure"} if failed else {"star_id": star, "compute_seconds": seconds}
            submit_result(task_id, TaskResult(worker_id=worker_id, result=result))
            if failed:
                failed_runs += 1
                queue(index)
            else:
                succeeded_at[index] = now[0]
            at(now[0], "ask", worker_id)

    if len(succeeded_at) < len(stars):
        raise RuntimeError(f"{setting.label} on {workload}: only {len(succeeded_at)} of {len(stars)} stars finished")
    return {
        "mean_turnaround": statistics.fmean(succeeded_at[index] - first_queued[index] for index in succeeded_at),
        "failed_runs": failed_runs,
        "runs": runs,
    }


@dataclass(frozen=True)
class Run:
    setting: Setting
    workload: str
    load: float
    seed: int
    mean_turnaround: float
    failed_runs: int


def _run(job: tuple[Setting, str, float, int, list[tuple[str, int]]]) -> Run:
    setting, workload, load, seed, stars = job
    result = simulate(setting, workload, load, seed, stars)
    return Run(setting, workload, load, seed, result["mean_turnaround"], result["failed_runs"])


def run_all(
    settings: list[Setting],
    stars: list[tuple[str, int]],
    seeds: int = DEFAULT_SEEDS,
    workloads: Iterable[str] = tuple(WORKLOADS),
    loads: Iterable[float] = LOADS,
    processes: int | None = None,
) -> list[Run]:
    """Every setting on every workload, load and seed. The reference baseline is added if missing."""
    if Setting(REFERENCE) not in settings:
        settings = [Setting(REFERENCE), *settings]
    jobs = [(setting, workload, load, seed, stars) for setting in settings for workload in workloads for load in loads for seed in range(seeds)]
    if processes == 1:
        return [_run(job) for job in jobs]
    with ProcessPoolExecutor(processes) as pool:
        return list(pool.map(_run, jobs, chunksize=4))


@dataclass(frozen=True)
class Summary:
    setting: Setting
    score: float  # mean of turnaround / round-robin's, over cells and seeds
    error: float  # standard error of score over seeds
    cells: dict[tuple[str, float], float]  # (workload, load) -> mean ratio
    failed_runs: float  # mean per run


def summarize(runs: list[Run]) -> list[Summary]:
    """One summary per setting, best score first."""
    reference = {(run.workload, run.load, run.seed): run.mean_turnaround for run in runs if run.setting == Setting(REFERENCE)}
    by_setting: dict[Setting, list[Run]] = {}
    for run in runs:
        by_setting.setdefault(run.setting, []).append(run)
    summaries = []
    for setting, its_runs in by_setting.items():
        ratios = {(run.workload, run.load, run.seed): run.mean_turnaround / reference[(run.workload, run.load, run.seed)] for run in its_runs}
        cells = sorted({(workload, load) for workload, load, _ in ratios})
        seeds = sorted({seed for _, _, seed in ratios})
        per_seed = [statistics.fmean(ratios[(workload, load, seed)] for workload, load in cells) for seed in seeds]
        summaries.append(
            Summary(
                setting=setting,
                score=statistics.fmean(per_seed),
                error=statistics.stdev(per_seed) / math.sqrt(len(per_seed)) if len(per_seed) > 1 else math.nan,
                cells={cell: statistics.fmean(ratios[(*cell, seed)] for seed in seeds) for cell in cells},
                failed_runs=statistics.fmean(run.failed_runs for run in its_runs),
            )
        )
    return sorted(summaries, key=lambda summary: (summary.score, summary.setting))


def paired_difference(runs: list[Run], first: Setting, second: Setting) -> tuple[float, float]:
    """first's score minus second's, and its standard error over seeds, pairing runs on the same work."""
    ratio = {}
    reference = {(run.workload, run.load, run.seed): run.mean_turnaround for run in runs if run.setting == Setting(REFERENCE)}
    for run in runs:
        if run.setting in (first, second):
            ratio[(run.setting, run.workload, run.load, run.seed)] = run.mean_turnaround / reference[(run.workload, run.load, run.seed)]
    cells = sorted({(workload, load) for _, workload, load, _ in ratio})
    seeds = sorted({seed for *_, seed in ratio})
    differences = [
        statistics.fmean(ratio[(first, *cell, seed)] - ratio[(second, *cell, seed)] for cell in cells) for seed in seeds
    ]
    error = statistics.stdev(differences) / math.sqrt(len(differences)) if len(differences) > 1 else math.nan
    return statistics.fmean(differences), error


def _number(value: float | None) -> str:
    return "any" if value is None else f"{value:g}"


def _table(summaries: list[Summary], cells: list[tuple[str, float]]) -> list[str]:
    header = ["scheduler", "alpha", "beta", "evaporation", "score", "±"] + [f"{workload} @{load:g}" for workload, load in cells] + ["failed runs"]
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join(["---"] * 4 + ["--:"] * (len(header) - 4)) + "|"]
    for summary in summaries:
        setting = summary.setting
        is_ant = setting.scheduler == "ant"
        row = [
            setting.scheduler,
            _number(setting.alpha) if is_ant else "",
            _number(setting.beta) if is_ant else "",
            _number(setting.evaporation) if is_ant else "",
            f"{summary.score:.3f}",
            "" if math.isnan(summary.error) else f"{summary.error:.3f}",
            *(f"{summary.cells[cell]:.3f}" for cell in cells),
            f"{summary.failed_runs:.1f}",
        ]
        lines.append("| " + " | ".join(row) + " |")
    return lines


def best_ant(summaries: list[Summary]) -> Summary:
    return next(summary for summary in summaries if summary.setting.scheduler == "ant")


def one_at_a_time(runs: list[Run], summaries: list[Summary], around: Setting) -> list[str]:
    """For each parameter, the score at each value with the other two held at around's."""
    by_setting = {summary.setting: summary for summary in summaries}
    lines = ["| parameter | value | score | ± | vs chosen, paired |", "|---|--:|--:|--:|--:|"]
    for name in ("alpha", "beta", "evaporation"):
        values = sorted({getattr(setting, name) for setting in by_setting if setting.scheduler == "ant"} - {None})
        for value in values:
            fields = {"alpha": around.alpha, "beta": around.beta, "evaporation": around.evaporation, name: value}
            if fields["alpha"] == 0:
                fields["evaporation"] = None
            candidate = Setting("ant", **fields)
            if candidate not in by_setting:
                continue
            summary = by_setting[candidate]
            if candidate == around:
                lines.append(f"| {name} | {value:g} (chosen) | {summary.score:.3f} | {summary.error:.3f} | |")
            else:
                difference, error = paired_difference(runs, candidate, around)
                lines.append(f"| {name} | {value:g} | {summary.score:.3f} | {summary.error:.3f} | {difference:+.3f} ± {error:.3f} |")
    return lines


def _describe(setting: Setting) -> str:
    return f"alpha={setting.alpha:g}, beta={setting.beta:g}, evaporation={_number(setting.evaporation)}"


def report(runs: list[Run], command: str) -> str:
    summaries = summarize(runs)
    by_setting = {summary.setting: summary for summary in summaries}
    cells = sorted(summaries[0].cells)
    seeds = len({run.seed for run in runs})
    best = best_ant(summaries)
    chosen = Setting("ant", DEFAULT_ALPHA, DEFAULT_BETA, DEFAULT_EVAPORATION)
    if chosen not in by_setting:
        chosen = best.setting
    lines = [
        "# Parameter sweep results",
        "",
        f"Generated by `{command}` ({seeds} seeds). Don't edit by hand; see",
        "[parameter-sweep.md](parameter-sweep.md) for the method and why the settings were chosen.",
        "",
        "Score: mean time from a star being queued to its first successful result, divided by",
        "round-robin's on the same work and averaged over workloads, loads and seeds. Lower is better.",
        "± is the standard error over seeds. Paired differences compare two settings seed by seed on",
        "the same work, which cancels most of the noise. The workload columns are the score for one",
        "workload at one load. Failed runs is the average per simulation.",
        "",
        f"Chosen (the defaults in scheduler.py): {_describe(chosen)}, score {by_setting[chosen].score:.3f} ± {by_setting[chosen].error:.3f}.",
    ]
    if best.setting != chosen:
        difference, error = paired_difference(runs, best.setting, chosen)
        lines.append(f"Best in this sweep: {_describe(best.setting)}, score {best.score:.3f}; {difference:+.3f} ± {error:.3f} against the chosen, paired.")
    lines += [
        "",
        "## One parameter at a time, around the chosen settings",
        "",
        *one_at_a_time(runs, summaries, chosen),
        "",
        "## Every setting, best first",
        "",
        *_table(summaries, cells),
        "",
    ]
    return "\n".join(lines)


def write_runs(runs: list[Run], path: Path) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["scheduler", "alpha", "beta", "evaporation", "workload", "load", "seed", "mean_turnaround_seconds", "failed_runs"])
        for run in sorted(runs, key=lambda run: (run.setting, run.workload, run.load, run.seed)):
            setting = run.setting
            writer.writerow(
                [
                    setting.scheduler,
                    "" if setting.alpha is None else setting.alpha,
                    "" if setting.beta is None else setting.beta,
                    "" if setting.evaporation is None else setting.evaporation,
                    run.workload,
                    run.load,
                    run.seed,
                    f"{run.mean_turnaround:.3f}",
                    run.failed_runs,
                ]
            )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seeds", type=int, default=DEFAULT_SEEDS, help=f"seeds per workload and load (default {DEFAULT_SEEDS})")
    parser.add_argument("--stars", type=int, help="use only the first STARS stars of the bundled list (default: all)")
    parser.add_argument("--processes", type=int, help="worker processes (default: one per CPU)")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help=f"where to write the results (default {DEFAULT_OUT_DIR})")
    args = parser.parse_args(argv)
    if args.seeds < 2:
        parser.error("--seeds must be at least 2, to estimate the noise")
    if args.stars is not None and args.stars < 1:
        parser.error("--stars must be at least 1")

    stars = list(zip(read_stars(limit=args.stars), read_sectors(limit=args.stars)))
    settings = [Setting(name) for name in BASELINES] + ant_settings(ALPHAS, BETAS, EVAPORATIONS)
    log.info("%d settings x %d workloads x %d loads x %d seeds on %d stars", len(settings), len(WORKLOADS), len(LOADS), args.seeds, len(stars))
    started = time.monotonic()
    runs = run_all(settings, stars, seeds=args.seeds, loads=LOADS, processes=args.processes)
    log.info("%d runs in %.0f s", len(runs), time.monotonic() - started)

    command = "python -m antsearch.sweep"
    if args.seeds != DEFAULT_SEEDS:
        command += f" --seeds {args.seeds}"
    if args.stars is not None:
        command += f" --stars {args.stars}"
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / RESULTS_FILE).write_text(report(runs, command))
    write_runs(runs, args.out_dir / RUNS_FILE)
    best = best_ant(summarize(runs))
    print(f"best ant setting: {best.setting.label} (score {best.score:.3f} ± {best.error:.3f})")
    print(f"wrote {args.out_dir / RESULTS_FILE} and {args.out_dir / RUNS_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
