"""Tests against a running compose stack (real Redis, real worker containers), over HTTP.

    make test-integration       # starts a throwaway stack, runs these, tears it down

Skipped unless ANTSEARCH_URL points at a coordinator. Use a short HEARTBEAT_TIMEOUT on the stack
(make test-integration sets 3 s) or the offline test waits a long time.
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


def take_task(api, worker_id, star_id):
    """Queue a task for star_id and keep requesting until this worker gets it."""
    (task_id,) = api.post("/tasks", json={"star_ids": [star_id]}).json()["task_ids"]
    for _ in range(100):
        response = api.post("/tasks/request", json={"worker_id": worker_id})
        assert response.status_code == 200, response.text
        if response.json()["task_id"] == task_id:
            return task_id
    pytest.fail(f"never received task {task_id}")


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
    task_id = take_task(api, worker_id, "WASP-18")

    response = api.post(f"/tasks/{task_id}/result", json={"worker_id": worker_id, "result": {"period": 0.94136}})

    assert response.json() == {"task_id": task_id, "status": "done"}
    task = api.get(f"/tasks/{task_id}").json()
    assert (task["status"], task["worker_id"], task["result"]) == ("done", worker_id, {"period": 0.94136})


def test_silent_worker_goes_offline_and_its_task_is_requeued(api):
    worker_id, timeout = new_worker(api)
    if timeout > 10:
        pytest.skip(f"stack heartbeat timeout is {timeout} s; start it with HEARTBEAT_TIMEOUT=3")
    task_id = take_task(api, worker_id, "WASP-19")

    deadline = time.monotonic() + timeout * 3
    while workers(api)[worker_id]["status"] == "online" and time.monotonic() < deadline:
        time.sleep(0.5)

    assert workers(api)[worker_id]["status"] == "offline"
    assert api.get(f"/tasks/{task_id}").json()["status"] == "pending"
    # The compose workers heartbeat every timeout/3, so they outlive the silent one.
    assert workers(api)["worker-1"]["status"] == "online"
