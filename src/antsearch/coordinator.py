"""Coordinator: a FastAPI service that tracks workers and hands out tasks, keeping all state in Redis.

Redis keys:
    antsearch:workers               set of every worker ID that has registered
    antsearch:worker:<id>           hash: status (online/offline), registered_at, last_heartbeat,
                                    disputed (how many of its results were outvoted)
    antsearch:worker:<id>:tasks     set of task IDs currently assigned to that worker
    antsearch:worker:<id>:history   set of task IDs that worker has reported a result for
    antsearch:tasks:pending         list of task IDs waiting for a worker, next one first; a task
                                    is listed once for each run it still needs
    antsearch:task:<id>             hash: star_id, status (pending until done), queued (runs on the
                                    pending list), task_type (short/multi_sector) if it was queued
                                    with a sector count, and once done: result (JSON), worker_id
                                    (whose result was accepted), outcome, completed_at
    antsearch:task:<id>:running     hash: worker ID -> assigned_at, for each run in progress
    antsearch:task:<id>:results     hash: worker ID -> its run as JSON (result, completed_at,
                                    seconds, and verdict once the task is done)
    antsearch:task_seq              counter used to number new tasks
    antsearch:run                   name of this run, and of its log file
    antsearch:pheromone:<type>      hash: worker ID -> pheromone level for that task type
                                    (see antsearch.pheromone)

Each task runs on `replicas` workers and is done when that many results match. A worker that
misses heartbeats is marked offline and its tasks go back on the queue.
"""

import asyncio
import contextlib
import copy
import json
import logging
import os
import time
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from antsearch.pheromone import SHORT, PheromoneTable, task_type
from antsearch.runlog import NO_RUN_LOG, RunLog
from antsearch.scheduler import (
    DEFAULT_ALPHA,
    DEFAULT_BETA,
    DEFAULT_EVAPORATION,
    DEFAULT_LATE_FACTOR,
    Scheduler,
    make_scheduler,
)
from antsearch.validation import decide

log = logging.getLogger("antsearch.coordinator")

WORKERS_KEY = "antsearch:workers"
PENDING_KEY = "antsearch:tasks:pending"
TASK_SEQ_KEY = "antsearch:task_seq"
RUN_KEY = "antsearch:run"
DEFAULT_HEARTBEAT_TIMEOUT = 30.0  # seconds
DEFAULT_RUN_LOG_DIR = "output/runs"
DEFAULT_REPLICAS = 2  # what main() uses; create_app on its own doesn't replicate


def worker_key(worker_id: str) -> str:
    return f"antsearch:worker:{worker_id}"


def worker_tasks_key(worker_id: str) -> str:
    return f"antsearch:worker:{worker_id}:tasks"


def worker_history_key(worker_id: str) -> str:
    return f"antsearch:worker:{worker_id}:history"


def task_key(task_id: str) -> str:
    return f"antsearch:task:{task_id}"


def task_running_key(task_id: str) -> str:
    return f"antsearch:task:{task_id}:running"


def task_results_key(task_id: str) -> str:
    return f"antsearch:task:{task_id}:results"


def task_status(redis_client: redis.Redis, task: dict[str, str], task_id: str) -> str:
    """done, assigned (a run is in progress) or pending."""
    if task.get("status") == "done":
        return "done"
    return "assigned" if redis_client.hlen(task_running_key(task_id)) else "pending"


class Registration(BaseModel):
    worker_id: str


class TaskRequest(BaseModel):
    worker_id: str


class NewTasks(BaseModel):
    star_ids: list[str]
    # Optional: how many TESS sectors each star has, in the same order, which sets each task's type.
    num_sectors: list[int] | None = None


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
        pipe.hdel(task_running_key(task_id), worker_id)
        pipe.hincrby(task_key(task_id), "queued", 1)
        pipe.lpush(PENDING_KEY, task_id)
    pipe.delete(worker_tasks_key(worker_id))
    return task_ids


def task_type_lookup(redis_client: redis.Redis) -> Callable[[str], str]:
    """A function giving a task's type; tasks queued without a sector count are short."""
    return lambda task_id: redis_client.hget(task_key(task_id), "task_type") or SHORT


def requeue_tasks(redis_client: redis.Redis, worker_id: str) -> list[str]:
    """Put every task assigned to worker_id back at the front of the pending queue."""
    task_ids = redis_client.transaction(
        lambda pipe: _queue_requeue(pipe, worker_id), worker_tasks_key(worker_id), value_from_callable=True
    )
    if task_ids:
        log.info("requeued tasks %s from worker %s", ", ".join(task_ids), worker_id)
    return task_ids


def record_requeued(run_log: RunLog, task_ids: list[str], worker_id: str, reason: str, last_heartbeat: float) -> None:
    for task_id in task_ids:
        run_log.record("task_requeued", task_id=task_id, worker_id=worker_id, reason=reason, last_heartbeat=last_heartbeat)


def mark_offline(redis_client: redis.Redis, now: float, timeout: float, run_log: RunLog = NO_RUN_LOG) -> list[str]:
    """Mark workers with no heartbeat in the last `timeout` seconds offline and requeue their tasks.

    Returns the workers marked offline. Each worker is checked in its own transaction, since the
    background loop and requests can check at the same time.
    """

    def check(pipe: redis.client.Pipeline, worker_id: str):
        status, last_heartbeat = pipe.hmget(worker_key(worker_id), "status", "last_heartbeat")
        if status != "online" or now - float(last_heartbeat) <= timeout:
            return None
        task_ids = _queue_requeue(pipe, worker_id)
        pipe.hset(worker_key(worker_id), "status", "offline")
        return float(last_heartbeat), task_ids

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
        last_heartbeat, task_ids = outcome
        log.warning("worker %s missed heartbeats for %.0f s; marked offline", worker_id, now - last_heartbeat)
        run_log.record("worker_offline", worker_id=worker_id, last_heartbeat=last_heartbeat)
        record_requeued(run_log, task_ids, worker_id, "offline", last_heartbeat)
        if task_ids:
            log.info("requeued tasks %s from worker %s", ", ".join(task_ids), worker_id)
        marked.append(worker_id)
    return marked


def create_app(
    redis_client: redis.Redis,
    heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
    sweep_interval: float | None = None,
    clock: Callable[[], float] = time.time,
    scheduler: Scheduler | None = None,
    run_log: RunLog = NO_RUN_LOG,
    replicas: int = 1,
) -> FastAPI:
    """Build the coordinator app. With sweep_interval, a background loop checks heartbeats that often."""
    if isinstance(replicas, bool) or not isinstance(replicas, int) or replicas < 1:
        raise ValueError(f"replicas must be a whole number >= 1, not {replicas!r}")
    max_runs = 2 * replicas - 1

    # Keep the factory's existing FIFO behavior unless a scheduler is supplied.
    # main() chooses one from SCHEDULER (round robin by default). One process owns scheduler state.
    active_scheduler = scheduler
    scheduler_lock = threading.Lock()

    def sweep() -> list[str]:
        return mark_offline(redis_client, clock(), heartbeat_timeout, run_log)

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
        last_heartbeat = redis_client.hget(worker_key(worker_id), "last_heartbeat")
        task_ids = requeue_tasks(redis_client, worker_id)
        record_requeued(run_log, task_ids, worker_id, "reregistered", float(last_heartbeat or 0))
        now = clock()
        redis_client.sadd(WORKERS_KEY, worker_id)
        redis_client.hset(
            worker_key(worker_id), mapping={"status": "online", "registered_at": now, "last_heartbeat": now}
        )
        run_log.record("worker_registered", worker_id=worker_id)
        log.info("registered worker %s", worker_id)
        return {"worker_id": worker_id, "registered": True, "heartbeat_timeout": heartbeat_timeout}

    @app.post("/workers/{worker_id}/heartbeat")
    def heartbeat(worker_id: str):
        worker = require_worker(worker_id)
        redis_client.hset(worker_key(worker_id), mapping={"status": "online", "last_heartbeat": clock()})
        if worker["status"] == "offline":
            run_log.record("worker_online", worker_id=worker_id)
            log.info("worker %s is back online", worker_id)
        return {"worker_id": worker_id, "status": "online"}

    @app.get("/workers")
    def list_workers():
        sweep()
        workers = []
        for worker_id in sorted(redis_client.smembers(WORKERS_KEY)):
            worker = redis_client.hgetall(worker_key(worker_id))
            disputed = int(worker.get("disputed", 0))
            workers.append(
                {
                    "worker_id": worker_id,
                    "status": worker["status"],
                    "last_heartbeat": float(worker["last_heartbeat"]),
                    "tasks": sorted(redis_client.smembers(worker_tasks_key(worker_id))),
                    "disputed": disputed,
                    "flagged": disputed > 0,
                }
            )
        return {"workers": workers}

    @app.post("/tasks", status_code=201)
    def add_tasks(new_tasks: NewTasks):
        sectors = new_tasks.num_sectors
        if sectors is not None:
            if len(sectors) != len(new_tasks.star_ids):
                raise HTTPException(status_code=422, detail="num_sectors needs one entry per star in star_ids")
            if any(count < 0 for count in sectors):
                raise HTTPException(status_code=422, detail="num_sectors can't be negative")
        task_ids = []
        for index, star_id in enumerate(new_tasks.star_ids):
            task_id = f"t{redis_client.incr(TASK_SEQ_KEY)}"
            task = {"star_id": star_id, "status": "pending", "queued": replicas}
            if sectors is not None:
                task["task_type"] = task_type(sectors[index])
            redis_client.hset(task_key(task_id), mapping=task)
            # Recorded before the push, since a worker can take the task as soon as it's on the queue.
            run_log.record("task_queued", task_id=task_id, star_id=star_id)
            redis_client.rpush(PENDING_KEY, *[task_id] * replicas)
            task_ids.append(task_id)
        log.info("queued %d tasks", len(task_ids))
        return {"task_ids": task_ids}

    @app.post("/tasks/request")
    def request_task(request: TaskRequest):
        nonlocal active_scheduler
        worker_id = request.worker_id
        sweep()
        if require_worker(worker_id)["status"] != "online":
            raise HTTPException(status_code=409, detail=f"worker {worker_id!r} is offline; register again")
        with scheduler_lock:
            while True:
                try:
                    with redis_client.pipeline() as pipe:
                        pipe.watch(PENDING_KEY, WORKERS_KEY, worker_tasks_key(worker_id), worker_history_key(worker_id))
                        worker_ids = sorted(pipe.smembers(WORKERS_KEY))
                        for item in worker_ids:
                            pipe.watch(worker_key(item), worker_tasks_key(item))
                        if pipe.hget(worker_key(worker_id), "status") != "online":
                            raise HTTPException(status_code=409, detail="worker is offline")
                        # A worker never runs a task twice: skip the ones it holds or has reported on.
                        skip = pipe.smembers(worker_tasks_key(worker_id)) | pipe.smembers(worker_history_key(worker_id))
                        tasks = list(dict.fromkeys(item for item in pipe.lrange(PENDING_KEY, 0, -1) if item not in skip))
                        workers = []
                        for item in worker_ids:
                            if pipe.hget(worker_key(item), "status") != "online":
                                continue
                            if active_scheduler is None:
                                workers.append(item)
                            elif pipe.scard(worker_tasks_key(item)) == 0 and not (tasks and pipe.sismember(worker_history_key(item), tasks[0])):
                                # Idle, and free to run the task the scheduler is choosing for.
                                workers.append(item)
                        candidate = copy.deepcopy(active_scheduler)
                        task_id = candidate.assign(worker_id, tasks, workers) if candidate else (tasks[0] if tasks else None)
                        if task_id is not None and task_id not in tasks:
                            raise RuntimeError("scheduler selected a task outside the pending queue")
                        if task_id is not None:
                            pipe.watch(task_key(task_id))
                            star_id = pipe.hget(task_key(task_id), "star_id")
                        pipe.multi()
                        if task_id is not None:
                            pipe.lrem(PENDING_KEY, 1, task_id)
                            pipe.hincrby(task_key(task_id), "queued", -1)
                            pipe.hset(task_running_key(task_id), worker_id, clock())
                            pipe.sadd(worker_tasks_key(worker_id), task_id)
                        else:
                            pipe.ping()
                        pipe.execute()
                        active_scheduler = candidate
                    break
                except redis.WatchError:
                    # Retry without advancing the scheduler's committed state.
                    continue
        if task_id is None:
            return Response(status_code=204)
        run_log.record("task_assigned", task_id=task_id, star_id=star_id, worker_id=worker_id)
        log.info("assigned task %s (star %s) to worker %s", task_id, star_id, worker_id)
        return {"task_id": task_id, "star_id": star_id}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        task = redis_client.hgetall(task_key(task_id))
        if not task:
            raise HTTPException(status_code=404, detail=f"unknown task {task_id!r}")
        task.pop("queued", None)
        task["status"] = task_status(redis_client, task, task_id)
        if "result" in task:
            task["result"] = json.loads(task["result"])
        runs = [
            {"worker_id": worker, "status": "done", **json.loads(run)}
            for worker, run in redis_client.hgetall(task_results_key(task_id)).items()
        ]
        runs.sort(key=lambda run: (run["completed_at"], run["worker_id"]))
        runs += [
            {"worker_id": worker, "status": "running", "assigned_at": float(assigned_at)}
            for worker, assigned_at in sorted(redis_client.hgetall(task_running_key(task_id)).items(), key=lambda item: float(item[1]))
        ]
        if runs:
            task["runs"] = runs
        return {"task_id": task_id, **task}

    def record_result(pipe: redis.client.Pipeline, task_id: str, worker_id: str, result: dict[str, Any]):
        """Store one run's result and settle the task if enough results are in."""
        task = pipe.hgetall(task_key(task_id))
        assigned_at = pipe.hget(task_running_key(task_id), worker_id)
        if task.get("status") == "done" or assigned_at is None:
            return None
        running = pipe.hgetall(task_running_key(task_id))
        now = clock()
        run = {"result": result, "completed_at": now, "seconds": now - float(assigned_at)}
        earlier = [(json.loads(stored), worker) for worker, stored in pipe.hgetall(task_results_key(task_id)).items()]
        earlier.sort(key=lambda item: (item[0]["completed_at"], item[1]))
        runs = earlier + [(run, worker_id)]
        outstanding = len(running) - 1 + int(task.get("queued", 0))
        decision = decide([item[0]["result"] for item in runs], outstanding, replicas, max_runs)

        pipe.multi()
        pipe.hdel(task_running_key(task_id), worker_id)
        pipe.srem(worker_tasks_key(worker_id), task_id)
        pipe.sadd(worker_history_key(worker_id), task_id)
        if decision.kind in ("waiting", "rerun"):
            pipe.hset(task_results_key(task_id), worker_id, json.dumps(run))
            if decision.runs:
                pipe.hincrby(task_key(task_id), "queued", decision.runs)
                pipe.lpush(PENDING_KEY, *[task_id] * decision.runs)
            return decision, runs

        for index, (each, worker) in enumerate(runs):
            if decision.kind == "accepted" and index in decision.agreed:
                each["verdict"] = "accepted"
            elif "error" in each["result"]:
                each["verdict"] = "failed"
            else:
                each["verdict"] = "disputed" if decision.kind == "accepted" else "unresolved"
            pipe.hset(task_results_key(task_id), worker, json.dumps(each))
            if each["verdict"] == "disputed":
                pipe.hincrby(worker_key(worker), "disputed", 1)
        if decision.kind == "accepted":
            first, winner = runs[decision.agreed[0]]
            done = {"result": json.dumps(first["result"]), "worker_id": winner, "outcome": "accepted"}
        else:
            message = f"no {replicas} of {len(runs)} runs agreed"
            done = {"result": json.dumps({"error": "Unresolved", "message": message}), "outcome": "unresolved"}
        pipe.hset(task_key(task_id), mapping={**done, "status": "done", "completed_at": now, "queued": 0})
        # Nothing else needs to run: drop queued runs, and let go of any still in progress.
        pipe.lrem(PENDING_KEY, 0, task_id)
        for worker in running:
            if worker != worker_id:
                pipe.srem(worker_tasks_key(worker), task_id)
        pipe.delete(task_running_key(task_id))
        return decision, runs

    @app.post("/tasks/{task_id}/result")
    def submit_result(task_id: str, submission: TaskResult):
        worker_id, result = submission.worker_id, submission.result
        if not redis_client.exists(task_key(task_id)):
            raise HTTPException(status_code=404, detail=f"unknown task {task_id!r}")
        outcome = redis_client.transaction(
            lambda pipe: record_result(pipe, task_id, worker_id, result),
            task_key(task_id),
            task_running_key(task_id),
            task_results_key(task_id),
            value_from_callable=True,
        )
        if outcome is None:
            # e.g. the worker was marked offline and the task went back on the queue
            status = task_status(redis_client, redis_client.hgetall(task_key(task_id)), task_id)
            run_log.record("result_rejected", task_id=task_id, worker_id=worker_id, status=status, result=result)
            raise HTTPException(status_code=409, detail=f"task {task_id!r} is not assigned to {worker_id!r}")
        decision, runs = outcome
        run_log.record("result_received", task_id=task_id, worker_id=worker_id, result=result)
        log.info("task %s: result from worker %s", task_id, worker_id)
        if decision.kind == "rerun":
            run_log.record("task_rerun", task_id=task_id, runs=decision.runs, reason="results disagree")
            log.info("task %s: results disagree, queued %d more run(s)", task_id, decision.runs)
        if decision.kind in ("waiting", "rerun"):
            return {"task_id": task_id, "status": task_status(redis_client, {}, task_id)}

        # Settled: now the scheduler learns from every run, knowing which ones were outvoted.
        pheromone = {}
        with scheduler_lock:
            if active_scheduler is not None:
                for run, worker in runs:
                    disputed = run["verdict"] in ("disputed", "unresolved")
                    effect = active_scheduler.on_result(worker, run["result"], task_id=task_id, seconds=run["seconds"], disputed=disputed)
                    if effect:
                        pheromone[worker] = effect
        for run, worker in runs:
            if run["verdict"] == "disputed":
                run_log.record("result_disputed", task_id=task_id, worker_id=worker, result=run["result"])
                log.warning("task %s: worker %s's result was outvoted; flagged", task_id, worker)
        agreed = [runs[index][1] for index in decision.agreed]
        task = redis_client.hgetall(task_key(task_id))
        details = {"pheromone": pheromone} if pheromone else {}
        run_log.record(
            "task_done",
            task_id=task_id,
            worker_id=task.get("worker_id"),
            result=json.loads(task["result"]),
            outcome=task["outcome"],
            agreed=agreed,
            **details,
        )
        log.info("task %s done (%s by %s)", task_id, task["outcome"], ", ".join(agreed) or "nobody")
        return {"task_id": task_id, "status": "done"}

    return app


async def sweep_forever(sweep: Callable[[], list[str]], interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(sweep)
        except redis.RedisError as exc:
            log.error("heartbeat check failed: %s", exc)


def open_run_log(redis_client: redis.Redis, directory: str | Path, scheduler_name: str, clock: Callable[[], float] = time.time) -> RunLog:
    """Open this run's log in directory. The run's name is kept in Redis, so a restart reuses the file."""
    started = time.strftime("%Y%m%d-%H%M%S", time.gmtime(clock()))
    redis_client.set(RUN_KEY, f"{started}-{scheduler_name}", nx=True)
    return RunLog(Path(directory) / f"{redis_client.get(RUN_KEY)}.jsonl", clock)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    timeout = float(os.environ.get("HEARTBEAT_TIMEOUT", DEFAULT_HEARTBEAT_TIMEOUT))
    scheduler_name = os.environ.get("SCHEDULER", "round-robin")
    seed = os.environ.get("SCHEDULER_SEED")
    alpha = float(os.environ.get("SCHEDULER_ALPHA") or DEFAULT_ALPHA)
    beta = float(os.environ.get("SCHEDULER_BETA") or DEFAULT_BETA)
    evaporation = float(os.environ.get("SCHEDULER_EVAPORATION") or DEFAULT_EVAPORATION)
    late_factor = float(os.environ.get("SCHEDULER_LATE_FACTOR") or DEFAULT_LATE_FACTOR)
    replicas = int(os.environ.get("REPLICAS") or DEFAULT_REPLICAS)
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    scheduler = make_scheduler(
        scheduler_name,
        int(seed) if seed else None,
        pheromones=PheromoneTable(client),
        task_type_of=task_type_lookup(client),
        alpha=alpha,
        beta=beta,
        evaporation=evaporation,
        late_factor=late_factor,
    )
    log.info("using the %s scheduler, %d replica(s) per task", scheduler_name, replicas)
    run_log = open_run_log(client, os.environ.get("RUN_LOG_DIR") or DEFAULT_RUN_LOG_DIR, scheduler_name)
    settings = {"alpha": alpha, "beta": beta, "evaporation": evaporation, "late_factor": late_factor} if scheduler_name == "ant" else {}
    run_log.record(
        "coordinator_started",
        scheduler=scheduler_name,
        seed=int(seed) if seed else None,
        heartbeat_timeout=timeout,
        replicas=replicas,
        **settings,
    )
    log.info("writing the run log to %s", run_log.path)
    uvicorn.run(
        create_app(
            client, heartbeat_timeout=timeout, sweep_interval=timeout / 3, scheduler=scheduler, run_log=run_log, replicas=replicas
        ),
        host=os.environ.get("COORDINATOR_HOST", "127.0.0.1"),
        port=int(os.environ.get("COORDINATOR_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
