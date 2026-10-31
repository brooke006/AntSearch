"""Queue stars from a star list on the coordinator, then wait for the workers to finish them.

    python -m antsearch.submit --limit 50              # first 50 stars of the bundled TESS list
    python -m antsearch.submit --limit 50 --no-wait    # queue them and exit

The star list is a CSV with a star_id column of TOI numbers (see datasets/tess_data_mixed.csv);
each is queued as "TOI-<number>", a name MAST resolves.
"""

import argparse
import csv
import logging
import os
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

log = logging.getLogger("antsearch.submit")

DEFAULT_STARS = Path(__file__).parent / "datasets" / "tess_data_mixed.csv"


def read_stars(path: str | Path = DEFAULT_STARS, limit: int | None = None) -> list[str]:
    """MAST names for the first `limit` stars in a star list CSV (all of them if limit is None)."""
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    return [f"TOI-{row['star_id']}" for row in rows[:limit]]


def submit(client: httpx.Client, star_ids: list[str]) -> list[str]:
    """Queue one task per star and return the task IDs."""
    response = client.post("/tasks", json={"star_ids": star_ids})
    response.raise_for_status()
    return response.json()["task_ids"]


def wait_for(client: httpx.Client, task_ids: list[str], poll_interval: float = 5.0) -> list[dict]:
    """Poll until every task is done, logging progress as it changes, and return the tasks."""
    last_done = -1
    while True:
        tasks = []
        for task_id in task_ids:
            response = client.get(f"/tasks/{task_id}")
            response.raise_for_status()
            tasks.append(response.json())
        done = sum(task["status"] == "done" for task in tasks)
        if done != last_done:
            log.info("%d/%d tasks done", done, len(tasks))
            last_done = done
        if done == len(tasks):
            return tasks
        time.sleep(poll_interval)


def summarize(tasks: list[dict], elapsed: float) -> str:
    """One line per task, then the totals per worker."""
    lines = []
    for task in tasks:
        result = task["result"]
        if "error" in result:
            outcome = f"{result['error']}: {result.get('message', '')}"
        elif "period" in result:
            outcome = f"P = {result['period']:.5f} d, depth = {result['depth'] * 1e6:.0f} ppm"
        else:
            outcome = "done"
        lines.append(f"{task['task_id']:>6}  {task['star_id']:<12} {task['worker_id']:<12} {outcome}")
    per_worker = Counter(task["worker_id"] for task in tasks)
    failed = sum("error" in task["result"] for task in tasks)
    lines.append("")
    lines.append(f"{len(tasks)} tasks in {elapsed:.0f} s, {failed} failed")
    lines.extend(f"  {worker}: {count} tasks" for worker, count in sorted(per_worker.items()))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one request per task per progress check
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=os.environ.get("ANTSEARCH_URL", "http://localhost:8000"), help="coordinator URL")
    parser.add_argument("--stars", type=Path, default=DEFAULT_STARS, help="star list CSV")
    parser.add_argument("--limit", type=int, help="queue only the first LIMIT stars")
    parser.add_argument("--no-wait", dest="wait", action="store_false", help="exit once the stars are queued")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="seconds between progress checks")
    args = parser.parse_args(argv)

    star_ids = read_stars(args.stars, args.limit)
    with httpx.Client(base_url=args.url, timeout=10.0) as client:
        start = time.monotonic()
        task_ids = submit(client, star_ids)
        log.info("queued %d stars as tasks %s to %s", len(task_ids), task_ids[0], task_ids[-1])
        if args.wait:
            tasks = wait_for(client, task_ids, args.poll_interval)
            print(summarize(tasks, time.monotonic() - start))
    return 0


if __name__ == "__main__":
    sys.exit(main())
