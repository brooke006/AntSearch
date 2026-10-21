import json
import logging
import threading

import httpx

from antsearch.worker import register, send_heartbeats


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
