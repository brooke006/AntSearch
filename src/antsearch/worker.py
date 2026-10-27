"""Worker: registers with the coordinator, then pulls tasks, runs them and reports the results until stopped.

Heartbeats go out from a background thread, so the worker stays online through long tasks.

Environment:
    COORDINATOR_URL         coordinator to join (default http://localhost:8000)
    WORKER_ID               name to register as (default: the host name)
    WORKER_TASK             transit (default) runs the TESS transit search; simulated just waits
                            SIMULATED_TASK_SECONDS (default 1) and returns a fake result, for
                            tests and experiments that shouldn't download from MAST
    POLL_INTERVAL           seconds between task requests when the queue is empty (default 2)
"""

import functools
import logging
import os
import signal
import socket
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx

log = logging.getLogger("antsearch.worker")


DEFAULT_HEARTBEAT_TIMEOUT = 30.0  # seconds; the coordinator's answer to register overrides it
DEFAULT_POLL_INTERVAL = 2.0  # seconds

Task = Callable[[str], dict[str, Any]]


def register(client: httpx.Client, worker_id: str) -> float:
    """Register with the coordinator and return its heartbeat timeout in seconds."""
    response = client.post("/workers/register", json={"worker_id": worker_id})
    response.raise_for_status()
    log.info("worker %s registered with coordinator at %s", worker_id, client.base_url)
    return float(response.json().get("heartbeat_timeout", DEFAULT_HEARTBEAT_TIMEOUT))


def send_heartbeats(client: httpx.Client, worker_id: str, interval: float, stop: threading.Event) -> None:
    """Send a heartbeat every interval seconds until stop is set.

    If the coordinator no longer knows this worker, register again. Network errors are logged and
    the next heartbeat tries again.
    """
    while not stop.wait(interval):
        try:
            response = client.post(f"/workers/{worker_id}/heartbeat")
            if response.status_code == 404:
                register(client, worker_id)
            else:
                response.raise_for_status()
        except httpx.HTTPError as exc:
            log.warning("heartbeat from worker %s failed: %s", worker_id, exc)


def request_task(client: httpx.Client, worker_id: str) -> dict[str, str] | None:
    """Ask for the next task. Returns {"task_id", "star_id"}, or None if the queue is empty.

    If the coordinator has forgotten this worker (404) or marked it offline (409), register again
    and ask once more.
    """
    response = client.post("/tasks/request", json={"worker_id": worker_id})
    if response.status_code in (404, 409):
        register(client, worker_id)
        response = client.post("/tasks/request", json={"worker_id": worker_id})
    response.raise_for_status()
    if response.status_code == 204:
        return None
    return response.json()


def execute(task: Task, star_id: str) -> dict[str, Any]:
    """Run task on star_id. A failure becomes an {"error", "message"} result, so it still gets reported."""
    try:
        return task(star_id)
    except Exception as exc:
        log.warning("task for star %s failed: %s: %s", star_id, type(exc).__name__, exc)
        return {"error": type(exc).__name__, "message": str(exc)}


def report_result(
    client: httpx.Client, worker_id: str, task_id: str, result: dict[str, Any], stop: threading.Event, retry_interval: float
) -> bool:
    """Send a task's result and return whether the coordinator accepted it.

    Network and server errors are retried every retry_interval seconds until stop is set. A 4xx
    answer is final: usually a 409 because this worker was marked offline and the task went back
    on the queue for someone else, so the result is dropped.
    """
    while True:
        try:
            response = client.post(f"/tasks/{task_id}/result", json={"worker_id": worker_id, "result": result})
            if response.is_client_error:
                log.warning("coordinator rejected the result for task %s: %s", task_id, response.text)
                return False
            response.raise_for_status()
            return True
        except httpx.HTTPError as exc:
            log.warning("reporting task %s failed: %s", task_id, exc)
        if stop.wait(retry_interval):
            return False


def run_tasks(client: httpx.Client, worker_id: str, task: Task, stop: threading.Event, poll_interval: float) -> None:
    """Request a task, run it and report the result, over and over until stop is set.

    When the queue is empty or the coordinator can't be reached, wait poll_interval seconds and ask
    again. A task already running when stop is set is finished and reported first.
    """
    while not stop.is_set():
        try:
            assignment = request_task(client, worker_id)
        except httpx.HTTPError as exc:
            log.warning("task request from worker %s failed: %s", worker_id, exc)
            assignment = None
        if assignment is None:
            stop.wait(poll_interval)
            continue
        task_id, star_id = assignment["task_id"], assignment["star_id"]
        log.info("worker %s running task %s (star %s)", worker_id, task_id, star_id)
        result = execute(task, star_id)
        if report_result(client, worker_id, task_id, result, stop, poll_interval):
            log.info("worker %s finished task %s", worker_id, task_id)


def simulated_task(star_id: str, seconds: float) -> dict[str, Any]:
    """Stand-in for the transit search that just waits, for tests and experiments without MAST."""
    time.sleep(seconds)
    return {"star_id": star_id, "simulated": True, "compute_seconds": seconds}


def load_task(name: str, simulated_seconds: float = 1.0) -> Task:
    """The task function WORKER_TASK names: transit or simulated."""
    if name == "transit":
        from antsearch.pipeline import run_task  # needs the science extra, so only imported here

        return run_task
    if name == "simulated":
        return functools.partial(simulated_task, seconds=simulated_seconds)
    raise ValueError(f"unknown WORKER_TASK {name!r}; use transit or simulated")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # a request every POLL_INTERVAL while idle, plus heartbeats
    coordinator_url = os.environ.get("COORDINATOR_URL", "http://localhost:8000")
    worker_id = os.environ.get("WORKER_ID") or socket.gethostname()
    poll_interval = float(os.environ.get("POLL_INTERVAL", DEFAULT_POLL_INTERVAL))
    task = load_task(
        os.environ.get("WORKER_TASK", "transit"), float(os.environ.get("SIMULATED_TASK_SECONDS", 1.0))
    )

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    with httpx.Client(base_url=coordinator_url, timeout=5.0) as client:
        timeout = register(client, worker_id)
        heartbeats = threading.Thread(
            target=send_heartbeats, args=(client, worker_id, timeout / 3, stop), name="heartbeats", daemon=True
        )
        heartbeats.start()
        run_tasks(client, worker_id, task, stop, poll_interval)
        heartbeats.join()
    log.info("worker %s stopped", worker_id)


if __name__ == "__main__":
    main()
