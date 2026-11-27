"""A dashboard that shows which workers are assigned which tasks and their pheromone levels.

    docker compose exec coordinator python -m antsearch.dashboard

Redis isn't exposed outside the compose network, so the dashboard runs inside the
coordinator container, where Redis is already reachable. It refreshes every few seconds
during a run, so you can watch the scheduler adapt as pheromone levels change.
"""

import os
import time

import redis

from antsearch.coordinator import WORKERS_KEY, PENDING_KEY, worker_key, worker_tasks_key
from antsearch.pheromone import TASK_TYPES, PheromoneTable

REFRESH_SECONDS = 2
WORKER_WIDTH = 10
STATUS_WIDTH = 9
LEVEL_WIDTH = 14


def snapshot(redis_client: redis.Redis) -> dict:
    """Read the workers, their tasks, the pending tasks and 
    the pheromone levels from Redis, and return them as one dict.
    """
    worker_ids = sorted(redis_client.smembers(WORKERS_KEY))
    workers = {}

    for worker_id in worker_ids:
        tasks = sorted(redis_client.smembers(worker_tasks_key(worker_id)))
        info = redis_client.hgetall(worker_key(worker_id))
        workers[worker_id] = {"status": info.get("status"), "tasks": tasks}

    pending = redis_client.lrange(PENDING_KEY, 0, -1)
    pheromones = PheromoneTable(redis_client).table()

    return{"workers": workers, "pending": pending, "pheromones": pheromones}

def render(snap) -> str:
    """Turn a snapshot into the text shown on screen"""
    lines = []
    lines.append("AntSearch live scheduler view")

    header = f"{'Worker':<{WORKER_WIDTH}}{'Status':<{STATUS_WIDTH}}"
    for kind in TASK_TYPES:
        header += f"{kind:>{LEVEL_WIDTH}}"
    header += " Tasks"
    lines.append(header)

    if not snap["workers"]:
        lines.append("No workers registered yet")

    for worker_id, details in snap["workers"].items():
        row = f"{worker_id:<{WORKER_WIDTH}}{details['status']:<{STATUS_WIDTH}}"

        levels = snap["pheromones"].get(worker_id, {})
        for kind in TASK_TYPES:
            if kind in levels:
                row += f"{levels[kind]:>{LEVEL_WIDTH}.2f}"
            else:
                row += f"{'-':>{LEVEL_WIDTH}}"

        if details["tasks"]:
            tasks_text = ", ".join(details["tasks"])
        else:
            tasks_text = "idle"
        row += f" {tasks_text}"

        lines.append(row)
    
    lines.append("")
    lines.append(f"Pending: {len(snap['pending'])}")
    
    return "\n".join(lines)


