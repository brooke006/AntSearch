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


def snapshot(redis_client: redis.Redis) -> dict:
    # Read the workers, their tasks, the pending tasks and the pheromone levels from Redis, and return them as one dict.
    worker_ids = sorted(redis_client.smembers(WORKERS_KEY))
    workers = {}

    for worker_id in worker_ids:
        tasks = sorted(redis_client.smembers(worker_tasks_key(worker_id)))
        info = redis_client.hgetall(worker_key(worker_id))
        workers[worker_id] = {"status": info.get("status"), "tasks": tasks}

    pending = redis_client.lrange(PENDING_KEY, 0, -1)
    pheromones = PheromoneTable(redis_client).table()

    return{"workers": workers, "pending": pending, "pheromones": pheromones}


