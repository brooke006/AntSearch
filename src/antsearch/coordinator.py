"""Coordinator: a FastAPI service that tracks workers and hands out tasks, keeping all state in Redis.

Redis keys:
    antsearch:workers               set of every worker ID that has registered
    antsearch:worker:<id>           hash: status (online/offline), registered_at, last_heartbeat
    antsearch:worker:<id>:tasks     set of task IDs currently assigned to that worker
    antsearch:tasks:pending         list of task IDs waiting for a worker, next one first
    antsearch:task:<id>             hash: star_id, status (pending/assigned/done), worker_id,
                                    assigned_at, completed_at, result (JSON)
    antsearch:task_seq              counter used to number new tasks

A worker that misses heartbeats for longer than the heartbeat timeout is marked offline, and any
tasks it held go back to the front of the pending queue.
"""

import asyncio
import contextlib
import json
import logging
import os
import time
from collections.abc import Callable
from typing import Any

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

log = logging.getLogger("antsearch.coordinator")

WORKERS_KEY = "antsearch:workers"
PENDING_KEY = "antsearch:tasks:pending"
TASK_SEQ_KEY = "antsearch:task_seq"
DEFAULT_HEARTBEAT_TIMEOUT = 30.0  # seconds


def worker_key(worker_id: str) -> str:
    return f"antsearch:worker:{worker_id}"


def worker_tasks_key(worker_id: str) -> str:
    return f"antsearch:worker:{worker_id}:tasks"


def task_key(task_id: str) -> str:
    return f"antsearch:task:{task_id}"


class Registration(BaseModel):
    worker_id: str


class TaskRequest(BaseModel):
    worker_id: str


class NewTasks(BaseModel):
    star_ids: list[str]


class TaskResult(BaseModel):
    worker_id: str
    result: dict[str, Any]


def _queue_requeue(pipe: redis.client.Pipeline, worker_id: str) -> list[str]:
    """Read worker_id's tasks, then queue the commands that put them back at the front of the queue.

    pipe must be watching the worker's task set; this switches it to MULTI.
    """
    task_ids = sorted(pipe.smembers(worker_tasks_key(worker_id)))
    pipe.multi()
    for task_id in task_ids:
        pipe.hset(task_key(task_id), "status", "pending")
        pipe.hdel(task_key(task_id), "worker_id", "assigned_at")
        pipe.lpush(PENDING_KEY, task_id)
    pipe.delete(worker_tasks_key(worker_id))
    return task_ids


def requeue_tasks(redis_client: redis.Redis, worker_id: str) -> list[str]:
    """Put every task assigned to worker_id back at the front of the pending queue.

    Runs as a Redis transaction watching the worker's task set: if anything else changes the set
    between the read and the write (say a heartbeat check requeueing the same tasks), the write
    is dropped and retried against the new contents, so no task is queued twice.
    """
    task_ids = redis_client.transaction(
        lambda pipe: _queue_requeue(pipe, worker_id), worker_tasks_key(worker_id), value_from_callable=True
    )
    if task_ids:
        log.info("requeued tasks %s from worker %s", ", ".join(task_ids), worker_id)
    return task_ids


def mark_offline(redis_client: redis.Redis, now: float, timeout: float) -> list[str]:
    """Mark online workers with no heartbeat in the last `timeout` seconds as offline.

    Their tasks are requeued. Returns the IDs of the workers that were marked offline.

    Several checks can run at once (the background loop and requests, on different threads), so
    each worker's check, offline mark and requeue happen in one Redis transaction watching that
    worker's keys. If another check, a heartbeat or a re-registration changes them first, the
    transaction retries with fresh values: a worker is only marked offline once, its tasks are
    only requeued once, and a heartbeat that lands mid-check keeps the worker online.
    """

    def check(pipe: redis.client.Pipeline, worker_id: str):
        status, last_heartbeat = pipe.hmget(worker_key(worker_id), "status", "last_heartbeat")
        if status != "online" or now - float(last_heartbeat) <= timeout:
            return None
        task_ids = _queue_requeue(pipe, worker_id)
        pipe.hset(worker_key(worker_id), "status", "offline")
        return now - float(last_heartbeat), task_ids

    marked = []
    for worker_id in sorted(redis_client.smembers(WORKERS_KEY)):
        outcome = redis_client.transaction(
            lambda pipe: check(pipe, worker_id),
            worker_key(worker_id),
            worker_tasks_key(worker_id),
            value_from_callable=True,
        )
        if outcome is None:
            continue
        silent_for, task_ids = outcome
        log.warning("worker %s missed heartbeats for %.0f s; marked offline", worker_id, silent_for)
        if task_ids:
            log.info("requeued tasks %s from worker %s", ", ".join(task_ids), worker_id)
        marked.append(worker_id)
    return marked


def create_app(
    redis_client: redis.Redis,
    heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
    sweep_interval: float | None = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    """Build the coordinator app.

    If sweep_interval is set, a background loop checks for missed heartbeats that often. Listing
    workers and requesting a task also check first, so their answers are never stale.
    """

    def sweep() -> list[str]:
        return mark_offline(redis_client, clock(), heartbeat_timeout)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(sweep_forever(sweep, sweep_interval)) if sweep_interval else None
        yield
        if task:
            task.cancel()

    app = FastAPI(title="AntSearch coordinator", lifespan=lifespan)

    def require_worker(worker_id: str) -> dict[str, str]:
        worker = redis_client.hgetall(worker_key(worker_id))
        if not worker:
            raise HTTPException(status_code=404, detail=f"unknown worker {worker_id!r}; register first")
        return worker

    @app.get("/health")
    def health():
        try:
            redis_client.ping()
        except redis.RedisError as exc:
            raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}")
        return {"status": "ok"}

    @app.post("/workers/register")
    def register(registration: Registration):
        worker_id = registration.worker_id
        # A worker that restarts loses whatever it was running, so hand those tasks to someone else.
        requeue_tasks(redis_client, worker_id)
        now = clock()
        redis_client.sadd(WORKERS_KEY, worker_id)
        redis_client.hset(
            worker_key(worker_id), mapping={"status": "online", "registered_at": now, "last_heartbeat": now}
        )
        log.info("registered worker %s", worker_id)
        return {"worker_id": worker_id, "registered": True, "heartbeat_timeout": heartbeat_timeout}

    @app.post("/workers/{worker_id}/heartbeat")
    def heartbeat(worker_id: str):
        worker = require_worker(worker_id)
        redis_client.hset(worker_key(worker_id), mapping={"status": "online", "last_heartbeat": clock()})
        if worker["status"] == "offline":
            log.info("worker %s is back online", worker_id)
        return {"worker_id": worker_id, "status": "online"}

    @app.get("/workers")
    def list_workers():
        sweep()
        workers = []
        for worker_id in sorted(redis_client.smembers(WORKERS_KEY)):
            worker = redis_client.hgetall(worker_key(worker_id))
            workers.append(
                {
                    "worker_id": worker_id,
                    "status": worker["status"],
                    "last_heartbeat": float(worker["last_heartbeat"]),
                    "tasks": sorted(redis_client.smembers(worker_tasks_key(worker_id))),
                }
            )
        return {"workers": workers}

    @app.post("/tasks", status_code=201)
    def add_tasks(new_tasks: NewTasks):
        task_ids = []
        for star_id in new_tasks.star_ids:
            task_id = f"t{redis_client.incr(TASK_SEQ_KEY)}"
            redis_client.hset(task_key(task_id), mapping={"star_id": star_id, "status": "pending"})
            redis_client.rpush(PENDING_KEY, task_id)
            task_ids.append(task_id)
        log.info("queued %d tasks", len(task_ids))
        return {"task_ids": task_ids}

    @app.post("/tasks/request")
    def request_task(request: TaskRequest):
        worker_id = request.worker_id
        sweep()
        if require_worker(worker_id)["status"] != "online":
            raise HTTPException(status_code=409, detail=f"worker {worker_id!r} is offline; register again")
        task_id = redis_client.lpop(PENDING_KEY)
        if task_id is None:
            return Response(status_code=204)
        redis_client.hset(task_key(task_id), mapping={"status": "assigned", "worker_id": worker_id, "assigned_at": clock()})
        redis_client.sadd(worker_tasks_key(worker_id), task_id)
        star_id = redis_client.hget(task_key(task_id), "star_id")
        log.info("assigned task %s (star %s) to worker %s", task_id, star_id, worker_id)
        return {"task_id": task_id, "star_id": star_id}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        task = redis_client.hgetall(task_key(task_id))
        if not task:
            raise HTTPException(status_code=404, detail=f"unknown task {task_id!r}")
        if "result" in task:
            task["result"] = json.loads(task["result"])
        return {"task_id": task_id, **task}

    @app.post("/tasks/{task_id}/result")
    def submit_result(task_id: str, submission: TaskResult):
        task = redis_client.hgetall(task_key(task_id))
        if not task:
            raise HTTPException(status_code=404, detail=f"unknown task {task_id!r}")
        if task["status"] != "assigned" or task.get("worker_id") != submission.worker_id:
            # e.g. the worker was marked offline and the task went back on the queue
            raise HTTPException(status_code=409, detail=f"task {task_id!r} is not assigned to {submission.worker_id!r}")
        redis_client.hset(
            task_key(task_id),
            mapping={"status": "done", "completed_at": clock(), "result": json.dumps(submission.result)},
        )
        redis_client.srem(worker_tasks_key(submission.worker_id), task_id)
        log.info("task %s done by worker %s", task_id, submission.worker_id)
        return {"task_id": task_id, "status": "done"}

    return app


async def sweep_forever(sweep: Callable[[], list[str]], interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(sweep)
        except redis.RedisError as exc:
            log.error("heartbeat check failed: %s", exc)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    timeout = float(os.environ.get("HEARTBEAT_TIMEOUT", DEFAULT_HEARTBEAT_TIMEOUT))
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    uvicorn.run(
        create_app(client, heartbeat_timeout=timeout, sweep_interval=timeout / 3),
        host=os.environ.get("COORDINATOR_HOST", "127.0.0.1"),
        port=int(os.environ.get("COORDINATOR_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
