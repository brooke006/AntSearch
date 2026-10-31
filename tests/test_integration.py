"""Tests against a running compose stack (real Redis, real worker containers), over HTTP.

    make test-integration       # starts a throwaway stack, runs these, tears it down

Skipped unless ANTSEARCH_URL points at a coordinator. Use a short HEARTBEAT_TIMEOUT on the stack
(make test-integration sets 3 s) or the offline test waits a long time, and WORKER_TASK=simulated
so the compose workers don't download from MAST.

The compose workers pull from the same queue as these tests, so tasks a test queues may be run by
either of them.
"""

import os
import time
import uuid

import httpx
import pytest

pytestmark = pytest.mark.integration

URL = os.environ.get("ANTSEARCH_URL")
if not URL:
    pytest.skip("ANTSEARCH_URL not set; run make test-integration", allow_module_level=True)


@pytest.fixture
def api():
    with httpx.Client(base_url=URL, timeout=10) as client:
        yield client


def new_worker(api):
    worker_id = f"it-{uuid.uuid4().hex[:8]}"
    response = api.post("/workers/register", json={"worker_id": worker_id})
    response.raise_for_status()
    return worker_id, response.json()["heartbeat_timeout"]


def take_task(api, worker_id):
    """Queue a task and request one until this worker holds a task (the compose workers may win some)."""
    for _ in range(100):
        api.post("/tasks", json={"star_ids": ["WASP-19"]}).raise_for_status()
        response = api.post("/tasks/request", json={"worker_id": worker_id})
        assert response.status_code in (200, 204), response.text
        if response.status_code == 200:
            return response.json()["task_id"]
    pytest.fail(f"worker {worker_id} never received a task")


def wait_until_done(api, task_ids, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = [api.get(f"/tasks/{task_id}").json() for task_id in task_ids]
        if all(task["status"] == "done" for task in tasks):
            return tasks
        time.sleep(0.5)
    pytest.fail(f"tasks not done after {timeout} s: {[t['task_id'] for t in tasks if t['status'] != 'done']}")


def workers(api):
    return {w["worker_id"]: w for w in api.get("/workers").json()["workers"]}


def test_coordinator_is_healthy(api):
    assert api.get("/health").json() == {"status": "ok"}


def test_compose_workers_register_and_stay_online(api):
    found = workers(api)
    assert found["worker-1"]["status"] == "online"
    assert found["worker-2"]["status"] == "online"


def test_task_round_trip(api):
    worker_id, _ = new_worker(api)
    assert api.post(f"/workers/{worker_id}/heartbeat").json()["status"] == "online"
    task_id = take_task(api, worker_id)

    response = api.post(f"/tasks/{task_id}/result", json={"worker_id": worker_id, "result": {"period": 0.94136}})

    assert response.json() == {"task_id": task_id, "status": "done"}
    task = api.get(f"/tasks/{task_id}").json()
    assert (task["status"], task["worker_id"], task["result"]) == ("done", worker_id, {"period": 0.94136})


def test_compose_workers_process_50_stars(api):
    star_ids = [f"TOI-{n}" for n in range(1000, 1050)]
    task_ids = api.post("/tasks", json={"star_ids": star_ids}).json()["task_ids"]

    tasks = wait_until_done(api, task_ids)

    assert [task["result"]["star_id"] for task in tasks] == star_ids
    assert {task["worker_id"] for task in tasks} == {"worker-1", "worker-2"}


def test_silent_worker_goes_offline_and_another_worker_finishes_its_task(api):
    worker_id, timeout = new_worker(api)
    if timeout > 10:
        pytest.skip(f"stack heartbeat timeout is {timeout} s; start it with HEARTBEAT_TIMEOUT=3")
    task_id = take_task(api, worker_id)

    deadline = time.monotonic() + timeout * 3
    while workers(api)[worker_id]["status"] == "online" and time.monotonic() < deadline:
        time.sleep(0.5)

    assert workers(api)[worker_id]["status"] == "offline"
    # The task went back on the queue, and a compose worker (which heartbeats every timeout/3,
    # so outlives the silent one) picked it up.
    (task,) = wait_until_done(api, [task_id])
    assert task["worker_id"] in {"worker-1", "worker-2"}
    assert workers(api)["worker-1"]["status"] == "online"
