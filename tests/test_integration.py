"""Tests against a running compose stack (real Redis, real worker containers), over HTTP.

    make test-integration       # starts a throwaway stack, runs these, tears it down

Skipped unless ANTSEARCH_URL points at a coordinator. Use a short HEARTBEAT_TIMEOUT on the stack
(make test-integration sets 3 s) or the offline test waits a long time, and WORKER_TASK=simulated
so the compose workers don't download from MAST.

The compose workers pull from the same queue as these tests, so tasks a test queues may be run by
any of them. The stack runs each task on REPLICAS workers (2 by default); a test worker that wants
its result accepted reports what the compose workers' simulated task does. The failure-scenario test also needs ANTSEARCH_COMPOSE_PROJECT (make test-integration
sets antsearch-it) and the docker CLI; it runs last because it kills worker-2 for a while.
"""

import os
import threading
import time
import uuid

import httpx
import pytest

pytestmark = pytest.mark.integration

URL = os.environ.get("ANTSEARCH_URL")
PROJECT = os.environ.get("ANTSEARCH_COMPOSE_PROJECT")
RUN_LOG_DIR = os.environ.get("ANTSEARCH_RUN_LOG_DIR")
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
        time.sleep(0.1)  # not this worker's turn; the worker chosen may poll slowly or have gone silent
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


COMPOSE_WORKERS = {"worker-1", "worker-2", "worker-3"}


def simulated(star_id):
    """What a compose worker running the simulated task reports for star_id, give or take its timing."""
    return {"star_id": star_id, "simulated": True, "compute_seconds": 0.0}


def test_coordinator_is_healthy(api):
    assert api.get("/health").json() == {"status": "ok"}


def test_compose_workers_register_and_stay_online(api):
    found = workers(api)
    assert all(found[worker_id]["status"] == "online" for worker_id in COMPOSE_WORKERS)


def test_task_round_trip(api):
    worker_id, _ = new_worker(api)
    assert api.post(f"/workers/{worker_id}/heartbeat").json()["status"] == "online"
    task_id = take_task(api, worker_id)

    response = api.post(f"/tasks/{task_id}/result", json={"worker_id": worker_id, "result": simulated("WASP-19")})

    assert response.status_code == 200
    (task,) = wait_until_done(api, [task_id])  # once a compose worker's run agrees
    assert (task["status"], task["outcome"]) == ("done", "accepted")
    mine = next(run for run in task["runs"] if run["worker_id"] == worker_id)
    assert mine["verdict"] == "accepted"
    assert workers(api)[worker_id]["flagged"] is False


def test_a_worker_that_returns_garbage_is_outvoted_and_flagged(api):
    worker_id, _ = new_worker(api)
    task_id = take_task(api, worker_id)
    garbage = {**simulated("WASP-19"), "period": 123.456}

    api.post(f"/tasks/{task_id}/result", json={"worker_id": worker_id, "result": garbage}).raise_for_status()

    # The compose worker running the other copy disagrees, so a third one breaks the tie.
    (task,) = wait_until_done(api, [task_id])
    assert task["outcome"] == "accepted"
    assert "period" not in task["result"] and task["worker_id"] in COMPOSE_WORKERS
    verdicts = {run["worker_id"]: run["verdict"] for run in task["runs"]}
    assert verdicts.pop(worker_id) == "disputed"
    assert len(verdicts) == 2 and set(verdicts.values()) == {"accepted"}
    flagged = workers(api)[worker_id]
    assert (flagged["flagged"], flagged["disputed"]) == (True, 1)


def test_compose_workers_process_50_stars(api):
    star_ids = [f"TOI-{n}" for n in range(1000, 1050)]
    task_ids = api.post("/tasks", json={"star_ids": star_ids}).json()["task_ids"]

    tasks = wait_until_done(api, task_ids)

    assert [task["result"]["star_id"] for task in tasks] == star_ids
    assert all(task["outcome"] == "accepted" for task in tasks)
    assert len({task["worker_id"] for task in tasks}) >= 2
    runs = [run for task in tasks for run in task["runs"]]
    assert {run["worker_id"] for run in runs} <= COMPOSE_WORKERS
    assert all(len({run["worker_id"] for run in task["runs"]}) == len(task["runs"]) >= 2 for task in tasks)


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
    assert task["worker_id"] in COMPOSE_WORKERS
    assert workers(api)["worker-1"]["status"] == "online"


@pytest.mark.skipif(not RUN_LOG_DIR, reason="ANTSEARCH_RUN_LOG_DIR not set; run make test-integration")
def test_run_log_records_a_lost_task_and_metrics_count_it_as_wasted(api):
    from antsearch.metrics import compute, newest_log
    from antsearch.runlog import read_events

    worker_id, timeout = new_worker(api)
    if timeout > 10:
        pytest.skip(f"stack heartbeat timeout is {timeout} s; start it with HEARTBEAT_TIMEOUT=3")
    task_id = take_task(api, worker_id)
    wait_for_status(api, worker_id, "offline", timeout=timeout * 3)
    late = api.post(f"/tasks/{task_id}/result", json={"worker_id": worker_id, "result": {"period": 1.0}})
    assert late.status_code == 409
    wait_until_done(api, [task_id])

    events = read_events(newest_log(RUN_LOG_DIR))

    assert events[0]["event"] == "coordinator_started"
    mine = [e for e in events if e.get("task_id") == task_id]
    names = [e["event"] for e in mine]
    for name in ("task_queued", "task_assigned", "task_requeued", "result_rejected", "task_done"):
        assert name in names, names
    assert [e["worker_id"] for e in mine if e["event"] == "task_assigned"][0] == worker_id
    assert {"worker_offline"} <= {e["event"] for e in events if e.get("worker_id") == worker_id}
    assert [e["time"] for e in mine] == sorted(e["time"] for e in mine)

    metrics = compute(events)
    assert metrics["late_results"] >= 1
    assigned = next(e["time"] for e in mine if e["event"] == "task_assigned")
    reported = next(e["time"] for e in mine if e["event"] == "result_rejected")
    assert metrics["lost_seconds"] >= reported - assigned  # it reported late, so all that time counts
    assert metrics["tasks_succeeded"] > 0
    assert metrics["completion_seconds"] > 0


def wait_for_status(api, worker_id, status, timeout):
    deadline = time.monotonic() + timeout
    while workers(api)[worker_id]["status"] != status:
        if time.monotonic() > deadline:
            pytest.fail(f"{worker_id} not {status} after {timeout} s")
        time.sleep(0.25)


@pytest.mark.skipif(not PROJECT, reason="ANTSEARCH_COMPOSE_PROJECT not set; run make test-integration")
def test_scenario_slows_kills_and_restarts_a_worker_on_schedule(api):
    from antsearch import scenario

    docker = scenario.Docker(PROJECT)
    container = docker.container("worker-2")
    plan = scenario.parse_scenario({"duration": 12, "events": [
        {"at": 0, "worker": "worker-2", "action": "cpu", "cpus": 0.5},
        {"at": 0, "worker": "worker-2", "action": "delay", "ms": 100},
        {"at": 1, "worker": "worker-2", "action": "kill"},
        {"at": 7, "worker": "worker-2", "action": "start"},
    ]})
    entries = []
    run = threading.Thread(target=scenario.run, args=(plan, scenario.Injector(docker), entries.append, threading.Event().wait))
    run.start()
    try:
        wait_for_status(api, "worker-2", "offline", timeout=7)  # killed at 1 s; 3 s heartbeat timeout
        assert docker.docker("inspect", "--format", "{{.HostConfig.NanoCpus}}", container) == "500000000"
        wait_for_status(api, "worker-2", "online", timeout=8)  # started again at 7 s
        # A kill throws the delay away with the network namespace, so the scenario set it again.
        assert "delay 100ms" in docker.docker("exec", container, "tc", "qdisc", "show", "dev", "eth0")
    finally:
        run.join()

    assert [(e["at"], e["action"]) for e in entries] == [(0, "cpu"), (0, "delay"), (1, "kill"), (7, "start")]
    assert all("error" not in e and e["elapsed"] - e["at"] < 1 for e in entries), entries
    # Reset at the end: no CPU limit, no delay, running and back online.
    assert docker.docker("inspect", "--format", "{{.HostConfig.NanoCpus}}", container) == f"{docker.host_cpus() * 1e9:.0f}"
    assert "netem" not in docker.docker("exec", container, "tc", "qdisc", "show", "dev", "eth0")
    assert workers(api)["worker-2"]["status"] == "online"
