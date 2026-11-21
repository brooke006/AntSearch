"""Compute a run's completion time, throughput and wasted work from its log.

    python -m antsearch.metrics                                 # the newest log in output/runs
    python -m antsearch.metrics output/runs/<run>.jsonl

Wasted work is worker time spent on runs that were lost, failed or outvoted. The README explains
how each metric is measured.
"""

import argparse
import json
import logging
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from antsearch.runlog import read_events

DEFAULT_LOG_DIR = "output/runs"


def compute(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Replay a run's events into its metrics. Times the run can't give yet are None."""
    schedulers = []
    queued = {}  # task ID -> time
    finished = {}  # task ID -> time
    running = {}  # (task ID, worker ID) -> time assigned
    taken_away = {}  # (task ID, worker ID) -> (time assigned, seconds already counted as lost)
    run_seconds = {}  # (task ID, worker ID) -> how long its finished run took
    succeeded = failed = lost_runs = late_results = disputed_runs = reruns = 0
    useful_seconds = failed_seconds = lost_seconds = disputed_seconds = 0.0

    def run_finished(key, time, result):
        nonlocal useful_seconds, failed_seconds
        seconds = time - running.pop(key, time)
        run_seconds[key] = seconds
        if "error" in result:
            failed_seconds += seconds
        else:
            useful_seconds += seconds

    for event in events:
        name, time = event["event"], event["time"]
        key = (event.get("task_id"), event.get("worker_id"))
        if name == "coordinator_started":
            if event["scheduler"] not in schedulers:
                schedulers.append(event["scheduler"])
        elif name == "task_queued":
            queued[event["task_id"]] = time
        elif name == "task_assigned":
            running[key] = time
        elif name == "result_received":
            run_finished(key, time, event["result"])
        elif name == "result_disputed" and key in run_seconds:
            seconds = run_seconds.pop(key)
            disputed_runs += 1
            disputed_seconds += seconds
            useful_seconds -= seconds
        elif name == "task_rerun":
            reruns += event["runs"]
        elif name == "task_done":
            if key in running:  # a log from before replication: this was also the run's result
                run_finished(key, time, event["result"])
            finished[event["task_id"]] = time
            if "error" in event["result"]:
                failed += 1
            else:
                succeeded += 1
        elif name == "task_requeued" and key in running:
            assigned = running.pop(key)
            seconds = max(0.0, event["last_heartbeat"] - assigned)
            lost_runs += 1
            lost_seconds += seconds
            taken_away[key] = (assigned, seconds)
        elif name == "result_rejected" and key in taken_away:
            # Retried reports of an accepted result are rejected too, but they never lost the task.
            assigned, counted = taken_away.pop(key)
            late_results += 1
            lost_seconds += time - assigned - counted

    completion = max(finished.values()) - min(queued.values()) if finished and queued else None
    turnarounds = [finished[task] - queued[task] for task in finished if task in queued]
    wasted = failed_seconds + lost_seconds + disputed_seconds
    worker_seconds = useful_seconds + wasted
    return {
        "schedulers": schedulers,
        "tasks_queued": len(queued),
        "tasks_succeeded": succeeded,
        "tasks_failed": failed,
        "tasks_unfinished": len(queued.keys() - finished.keys()),
        "completion_seconds": completion,
        "mean_turnaround_seconds": sum(turnarounds) / len(turnarounds) if turnarounds else None,
        "throughput_per_minute": succeeded / completion * 60 if completion else None,
        "worker_seconds": worker_seconds,
        "wasted_seconds": wasted,
        "wasted_fraction": wasted / worker_seconds if worker_seconds else None,
        "lost_runs": lost_runs,
        "late_results": late_results,
        "lost_seconds": lost_seconds,
        "failed_seconds": failed_seconds,
        "disputed_runs": disputed_runs,
        "disputed_seconds": disputed_seconds,
        "reruns": reruns,
    }


def report(metrics: dict[str, Any]) -> str:
    lines = [f"scheduler:       {', '.join(metrics['schedulers']) or 'unknown'}"]
    if len(metrics["schedulers"]) > 1:
        lines.append("                 (changed mid-run, so these numbers mix them; run make down between runs)")
    lines.append(
        f"tasks:           {metrics['tasks_queued']} queued, {metrics['tasks_succeeded']} succeeded, "
        f"{metrics['tasks_failed']} failed, {metrics['tasks_unfinished']} unfinished"
    )
    if metrics["completion_seconds"] is None:
        lines.append("no task has finished yet")
        return "\n".join(lines)
    if metrics["tasks_unfinished"]:
        lines.append("                 (still running or abandoned, so the times below cover the finished tasks only)")
    lines.append(f"completion time: {metrics['completion_seconds']:.1f} s")
    lines.append(f"mean turnaround: {metrics['mean_turnaround_seconds']:.1f} s per task")
    lines.append(f"throughput:      {metrics['throughput_per_minute'] or 0:.1f} succeeded tasks per minute")
    fraction = metrics["wasted_fraction"] or 0
    lines.append(f"wasted work:     {metrics['wasted_seconds']:.1f} s of {metrics['worker_seconds']:.1f} s worker time ({fraction:.1%})")
    lines.append(
        f"  lost with a worker: {metrics['lost_seconds']:.1f} s "
        f"(runs lost: {metrics['lost_runs']}, reported too late: {metrics['late_results']})"
    )
    lines.append(f"  failed runs:        {metrics['failed_seconds']:.1f} s (tasks failed: {metrics['tasks_failed']})")
    lines.append(f"  outvoted runs:      {metrics['disputed_seconds']:.1f} s (runs outvoted: {metrics['disputed_runs']})")
    lines.append(f"extra runs:      {metrics['reruns']} (queued because results disagreed)")
    return "\n".join(lines)


def newest_log(directory: str | Path) -> Path | None:
    return max(Path(directory).glob("*.jsonl"), key=lambda path: path.stat().st_mtime, default=None)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("log", nargs="?", type=Path, help="run log (default: the newest in RUN_LOG_DIR or output/runs)")
    args = parser.parse_args(argv)

    directory = os.environ.get("RUN_LOG_DIR") or DEFAULT_LOG_DIR
    path = args.log or newest_log(directory)
    if path is None:
        parser.error(f"no run logs in {directory}; start a cluster with make up, or name a log")
    try:
        events = read_events(path)
    except (OSError, json.JSONDecodeError) as exc:
        parser.error(f"can't read {path}: {exc}")
    print(path)
    print(report(compute(events)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
