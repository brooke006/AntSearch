import json
import logging
import threading
import time

import fakeredis
import httpx
import pytest
from fastapi.testclient import TestClient

from antsearch import submit
from antsearch.coordinator import create_app
from antsearch.worker import (
    execute,
    load_task,
    register,
    report_result,
    request_task,
    run_tasks,
    send_heartbeats,
    simulated_task,
)


def test_register_posts_worker_id_and_logs(caplog):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={})

    client = httpx.Client(base_url="http://coordinator", transport=httpx.MockTransport(handler))
    with caplog.at_level(logging.INFO, logger="antsearch.worker"):
        register(client, "w1")

    assert [r.url.path for r in requests] == ["/workers/register"]
    assert json.loads(requests[0].content) == {"worker_id": "w1"}
    assert "worker w1 registered" in caplog.text


def test_register_returns_coordinator_heartbeat_timeout():
    client = httpx.Client(
        base_url="http://coordinator",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"heartbeat_timeout": 12})),
    )

    assert register(client, "w1") == 12


class StopAfter(threading.Event):
    """An Event whose wait() returns False n times (so the loop runs n times), then True."""

    def __init__(self, n):
        super().__init__()
        self.n = n

    def wait(self, timeout=None):
        self.n -= 1
        return self.n < 0


def test_heartbeats_reregister_when_coordinator_forgets_worker_and_survive_errors(caplog):
    responses = [
        httpx.Response(200, json={}),  # heartbeat
        httpx.Response(404),  # heartbeat: coordinator has forgotten w1...
        httpx.Response(200, json={}),  # ...so it registers again
        httpx.ConnectError("down"),  # heartbeat: network error, logged
        httpx.Response(200, json={}),  # heartbeat
    ]
    paths = []

    def handler(request):
        paths.append(request.url.path)
        outcome = responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    client = httpx.Client(base_url="http://coordinator", transport=httpx.MockTransport(handler))
    with caplog.at_level(logging.WARNING, logger="antsearch.worker"):
        send_heartbeats(client, "w1", interval=10, stop=StopAfter(4))

    heartbeat = "/workers/w1/heartbeat"
    assert paths == [heartbeat, heartbeat, "/workers/register", heartbeat, heartbeat]
    assert "heartbeat from worker w1 failed" in caplog.text


def scripted_client(*responses):
    """A client whose requests get each of responses in turn (an exception is raised instead).

    Returns the client and the list of (method, path, JSON body) it was sent.
    """
    queue = list(responses)
    sent = []

    def handler(request):
        sent.append((request.method, request.url.path, json.loads(request.content) if request.content else None))
        outcome = queue.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return httpx.Client(base_url="http://coordinator", transport=httpx.MockTransport(handler)), sent


def test_request_task_returns_assignment_or_none_when_queue_is_empty():
    client, sent = scripted_client(
        httpx.Response(200, json={"task_id": "t1", "star_id": "TOI-102"}),
        httpx.Response(204),
    )

    assert request_task(client, "w1") == {"task_id": "t1", "star_id": "TOI-102"}
    assert request_task(client, "w1") is None
    assert sent == [("POST", "/tasks/request", {"worker_id": "w1"})] * 2


@pytest.mark.parametrize("status", [404, 409])
def test_request_task_registers_again_when_forgotten_or_offline(status):
    client, sent = scripted_client(
        httpx.Response(status),
        httpx.Response(200, json={}),
        httpx.Response(200, json={"task_id": "t1", "star_id": "TOI-102"}),
    )

    assert request_task(client, "w1") == {"task_id": "t1", "star_id": "TOI-102"}
    assert [path for _, path, _ in sent] == ["/tasks/request", "/workers/register", "/tasks/request"]


def test_execute_turns_a_failure_into_an_error_result(caplog):
    def fail(star_id):
        raise LookupError(f"no light curve for {star_id}")

    with caplog.at_level(logging.WARNING, logger="antsearch.worker"):
        result = execute(fail, "TOI-1")

    assert result == {"error": "LookupError", "message": "no light curve for TOI-1"}
    assert "task for star TOI-1 failed" in caplog.text


def test_report_result_retries_network_and_server_errors():
    client, sent = scripted_client(httpx.ConnectError("down"), httpx.Response(503), httpx.Response(200, json={}))

    assert report_result(client, "w1", "t1", {"period": 1.0}, StopAfter(2), retry_interval=1)
    assert sent == [("POST", "/tasks/t1/result", {"worker_id": "w1", "result": {"period": 1.0}})] * 3


def test_report_result_drops_result_the_coordinator_rejects(caplog):
    client, sent = scripted_client(httpx.Response(409, json={"detail": "task 't1' is not assigned to 'w1'"}))

    with caplog.at_level(logging.WARNING, logger="antsearch.worker"):
        assert not report_result(client, "w1", "t1", {}, StopAfter(5), retry_interval=1)
    assert len(sent) == 1
    assert "rejected the result for task t1" in caplog.text


def test_report_result_gives_up_when_stopped():
    client, sent = scripted_client(httpx.ConnectError("down"), httpx.ConnectError("down"))

    assert not report_result(client, "w1", "t1", {}, StopAfter(1), retry_interval=1)
    assert len(sent) == 2


def test_run_tasks_runs_reports_and_waits_when_queue_is_empty():
    client, sent = scripted_client(
        httpx.Response(200, json={"task_id": "t1", "star_id": "TOI-102"}),
        httpx.Response(200, json={}),  # result accepted
        httpx.Response(204),  # queue empty: wait, then ask again
        httpx.ConnectError("down"),  # coordinator unreachable: wait, then ask again
        httpx.Response(200, json={"task_id": "t2", "star_id": "TOI-103"}),
        httpx.Response(200, json={}),
    )
    stop = StopAfter(2)

    def task(star_id):
        if star_id == "TOI-103":
            stop.set()  # stopping mid-task still reports the result, then the loop ends
        return {"star": star_id}

    run_tasks(client, "w1", task, stop, poll_interval=1)

    assert sent == [
        ("POST", "/tasks/request", {"worker_id": "w1"}),
        ("POST", "/tasks/t1/result", {"worker_id": "w1", "result": {"star": "TOI-102"}}),
        ("POST", "/tasks/request", {"worker_id": "w1"}),
        ("POST", "/tasks/request", {"worker_id": "w1"}),
        ("POST", "/tasks/request", {"worker_id": "w1"}),
        ("POST", "/tasks/t2/result", {"worker_id": "w1", "result": {"star": "TOI-103"}}),
    ]


def test_load_task_picks_simulated_or_rejects_unknown():
    assert load_task("simulated", simulated_seconds=0)("TOI-1") == {
        "star_id": "TOI-1", "simulated": True, "compute_seconds": 0,
    }
    with pytest.raises(ValueError, match="unknown WORKER_TASK"):
        load_task("nope")


def test_two_workers_process_50_stars_through_the_coordinator():
    app = create_app(fakeredis.FakeRedis(decode_responses=True))
    stop = threading.Event()

    def worker(worker_id):
        with TestClient(app) as client:
            register(client, worker_id)
            # A short sleep in the task lets both workers get a share of the queue.
            run_tasks(client, worker_id, lambda star_id: simulated_task(star_id, 0.01), stop, poll_interval=0.01)

    threads = [threading.Thread(target=worker, args=(f"w{n}",)) for n in (1, 2)]
    with TestClient(app) as api:
        task_ids = submit.submit(api, submit.read_stars(limit=50))
        for thread in threads:
            thread.start()
        try:
            deadline = time.monotonic() + 30
            tasks = []
            while time.monotonic() < deadline:
                tasks = [api.get(f"/tasks/{task_id}").json() for task_id in task_ids]
                if all(task["status"] == "done" for task in tasks):
                    break
                time.sleep(0.05)
        finally:
            stop.set()
            for thread in threads:
                thread.join()

    assert len(tasks) == 50
    assert all(task["status"] == "done" for task in tasks)
    assert [task["result"]["star_id"] for task in tasks] == [task["star_id"] for task in tasks]
    assert {task["worker_id"] for task in tasks} == {"w1", "w2"}
