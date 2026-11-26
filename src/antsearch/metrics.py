"""Compute a run's completion time, throughput and wasted work from its log.

    python -m antsearch.metrics                                 # the newest log in output/runs
    python -m antsearch.metrics output/runs/<run>.jsonl
    python -m antsearch.metrics --save docs/runs              # keep the log and its report in docs/runs

Wasted work is worker time spent on runs that were lost, failed or outvoted. The README explains
how each metric is measured. The report ends with a line per worker: its runs, its pheromone level
at the end of the run (ant scheduler only) and the machine it ran on.
"""

import argparse
import json
import logging
import os
import shutil
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
    workers = {}  # worker ID -> its own counts
    levels = {}  # worker ID -> {task type: pheromone level}, as of the last task the ant scheduler settled
    succeeded = failed = lost_runs = late_results = disputed_runs = reruns = 0
    useful_seconds = failed_seconds = lost_seconds = disputed_seconds = 0.0

    def worker(worker_id):
        return workers.setdefault(
            worker_id, {"machine": None, "runs": 0, "failed": 0, "outvoted": 0, "lost": 0, "answer_seconds": 0.0}
        )

    def run_finished(key, time, result):
        nonlocal useful_seconds, failed_seconds
        seconds = time - running.pop(key, time)
        run_seconds[key] = seconds
        counts = worker(key[1])
        counts["runs"] += 1
        if "error" in result:
            failed_seconds += seconds
            counts["failed"] += 1
        else:
            useful_seconds += seconds
            counts["answer_seconds"] += seconds

    for event in events:
        name, time = event["event"], event["time"]
        key = (event.get("task_id"), event.get("worker_id"))
        if name == "coordinator_started":
            if event["scheduler"] not in schedulers:
                schedulers.append(event["scheduler"])
        elif name == "worker_registered":
            worker(event["worker_id"])["machine"] = event.get("machine") or worker(event["worker_id"])["machine"]
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
            worker(key[1])["outvoted"] += 1
        elif name == "task_rerun":
            reruns += event["runs"]
        elif name == "task_done":
            if key in running:  # a log from before replication: this was also the run's result
                run_finished(key, time, event["result"])
            finished[event["task_id"]] = time
            levels = event.get("levels") or levels
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
            worker(key[1])["lost"] += 1
        elif name == "result_rejected" and key in taken_away:
            # Retried reports of an accepted result are rejected too, but they never lost the task.
            assigned, counted = taken_away.pop(key)
            late_results += 1
            lost_seconds += time - assigned - counted

    completion = max(finished.values()) - min(queued.values()) if finished and queued else None
    turnarounds = [finished[task] - queued[task] for task in finished if task in queued]
    for worker_id in levels:
        worker(worker_id)
    for worker_id, counts in workers.items():
        counts["pheromone"] = levels.get(worker_id, {})
        answers = counts["runs"] - counts["failed"]
        counts["mean_answer_seconds"] = counts.pop("answer_seconds") / answers if answers else None
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
        "workers": dict(sorted(workers.items())),
    }


def describe(machine: dict[str, Any] | None) -> str:
    """A short line for a worker's machine, e.g. "Darwin 24.1 arm64, Apple M2, 8 CPUs, 16 GB"."""
    if not machine:
        return "-"
    parts = [" ".join(str(machine[key]) for key in ("os", "arch") if machine.get(key))]
    if machine.get("cpu"):
        parts.append(str(machine["cpu"]))
    if machine.get("cpus"):
        parts.append(f"{machine['cpus']:g} CPU{'' if machine['cpus'] == 1 else 's'}")
    if machine.get("memory_gb"):
        parts.append(f"{machine['memory_gb']:g} GB")
    if machine.get("docker"):
        parts.append("in Docker")
    return ", ".join(part for part in parts if part)


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
        return "\n".join(lines + worker_table(metrics["workers"]))
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
    return "\n".join(lines + worker_table(metrics["workers"]))


def worker_table(workers: dict[str, dict[str, Any]]) -> list[str]:
    if not workers:
        return []
    width = max(len("worker"), *(len(worker_id) for worker_id in workers))
    # One pheromone column per task type the run used, short first; none if the scheduler wasn't ant.
    kinds = sorted({kind for counts in workers.values() for kind in counts["pheromone"]}, key=lambda kind: (kind != "short", kind))
    headers = ["pheromone" if kinds == ["short"] else f"pheromone {kind}" for kind in kinds]
    lines = ["", f"{'worker':<{width}}  runs  failed  outvoted  lost  mean answer  " + "".join(f"{h}  " for h in headers) + "machine"]
    for worker_id, counts in workers.items():
        mean = counts["mean_answer_seconds"]
        mean = f"{mean:.1f} s" if mean is not None else "-"
        levels = [counts["pheromone"].get(kind) for kind in kinds]
        cells = [f"{level:.2f}" if level is not None else "-" for level in levels]
        pheromone = "".join(f"{cell:>{len(header)}}  " for header, cell in zip(headers, cells))
        lines.append(
            f"{worker_id:<{width}}  {counts['runs']:>4}  {counts['failed']:>6}  {counts['outvoted']:>8}  "
            f"{counts['lost']:>4}  {mean:>11}  {pheromone}{describe(counts['machine'])}"
        )
    return lines


def newest_log(directory: str | Path) -> Path | None:
    return max(Path(directory).glob("*.jsonl"), key=lambda path: path.stat().st_mtime, default=None)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("log", nargs="?", type=Path, help="run log (default: the newest in RUN_LOG_DIR or output/runs)")
    parser.add_argument("--save", type=Path, metavar="DIR", help="copy the log into DIR, with the report next to it as <run>.txt")
    args = parser.parse_args(argv)

    directory = os.environ.get("RUN_LOG_DIR") or DEFAULT_LOG_DIR
    path = args.log or newest_log(directory)
    if path is None:
        parser.error(f"no run logs in {directory}; start a cluster with make up, or name a log")
    try:
        events = read_events(path)
    except (OSError, json.JSONDecodeError) as exc:
        parser.error(f"can't read {path}: {exc}")
    text = report(compute(events))
    if args.save:
        args.save.mkdir(parents=True, exist_ok=True)
        saved = args.save / path.name
        if saved.resolve() != path.resolve():
            shutil.copyfile(path, saved)
        saved.with_suffix(".txt").write_text(f"{saved}\n{text}\n")
        print(f"saved {saved} and {saved.with_suffix('.txt')}")
        path = saved
    print(path)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
