import threading
import time

import fakeredis
import pytest
from fastapi.testclient import TestClient
from redis.commands.core import HashCommands, SetCommands

from antsearch.coordinator import (
    PENDING_KEY,
    WORKERS_KEY,
    create_app,
    mark_offline,
    requeue_tasks,
    worker_key,
    worker_tasks_key,
)

TIMEOUT = 30


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def client(redis_client, clock):
    return TestClient(create_app(redis_client, heartbeat_timeout=TIMEOUT, clock=clock))


def register(client, worker_id):
    assert client.post("/workers/register", json={"worker_id": worker_id}).status_code == 200


def add_tasks(client, *star_ids):
    response = client.post("/tasks", json={"star_ids": list(star_ids)})
    assert response.status_code == 201
    return response.json()["task_ids"]


def status_of(client, worker_id):
    return {w["worker_id"]: w["status"] for w in client.get("/workers").json()["workers"]}[worker_id]


def test_health_pings_redis(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_health_is_503_when_redis_is_down(clock):
    server = fakeredis.FakeServer()
    server.connected = False
    client = TestClient(create_app(fakeredis.FakeRedis(server=server, decode_responses=True), clock=clock))

    assert client.get("/health").status_code == 503


def test_get_unknown_task_is_404(client):
    assert client.get("/tasks/t99").status_code == 404


def test_register_adds_worker_to_redis_as_online(client, redis_client, clock):
    response = client.post("/workers/register", json={"worker_id": "w1"})

    assert response.status_code == 200
    assert response.json() == {"worker_id": "w1", "registered": True, "heartbeat_timeout": TIMEOUT}
    assert redis_client.smembers(WORKERS_KEY) == {"w1"}
    assert redis_client.hgetall(worker_key("w1")) == {
        "status": "online",
        "registered_at": str(clock.now),
        "last_heartbeat": str(clock.now),
    }


def test_heartbeat_updates_last_heartbeat(client, redis_client, clock):
    register(client, "w1")
    clock.now += 10

    response = client.post("/workers/w1/heartbeat")

    assert response.json() == {"worker_id": "w1", "status": "online"}
    assert redis_client.hget(worker_key("w1"), "last_heartbeat") == str(clock.now)


def test_heartbeat_from_unknown_worker_is_404(client):
    assert client.post("/workers/nobody/heartbeat").status_code == 404


def test_list_workers_shows_status_and_tasks(client, clock):
    register(client, "w1")
    register(client, "w2")
    (task_id,) = add_tasks(client, "TIC 1")
    client.post("/tasks/request", json={"worker_id": "w2"})

    assert client.get("/workers").json() == {
        "workers": [
            {"worker_id": "w1", "status": "online", "last_heartbeat": clock.now, "tasks": []},
            {"worker_id": "w2", "status": "online", "last_heartbeat": clock.now, "tasks": [task_id]},
        ]
    }


def test_add_tasks_queues_them_in_order(client, redis_client):
    task_ids = add_tasks(client, "TIC 1", "TIC 2")

    assert task_ids == ["t1", "t2"]
    assert redis_client.lrange(PENDING_KEY, 0, -1) == ["t1", "t2"]
    assert client.get("/tasks/t2").json() == {"task_id": "t2", "star_id": "TIC 2", "status": "pending"}


def test_request_task_assigns_next_pending_task(client, clock):
    register(client, "w1")
    add_tasks(client, "TIC 1", "TIC 2")

    response = client.post("/tasks/request", json={"worker_id": "w1"})

    assert response.json() == {"task_id": "t1", "star_id": "TIC 1"}
    assert client.get("/tasks/t1").json() == {
        "task_id": "t1",
        "star_id": "TIC 1",
        "status": "assigned",
        "worker_id": "w1",
        "assigned_at": str(clock.now),
    }


def test_request_task_with_empty_queue_is_204(client):
    register(client, "w1")

    assert client.post("/tasks/request", json={"worker_id": "w1"}).status_code == 204


def test_request_task_from_unknown_worker_is_404(client):
    add_tasks(client, "TIC 1")

    assert client.post("/tasks/request", json={"worker_id": "nobody"}).status_code == 404


def test_submit_result_marks_task_done(client):
    register(client, "w1")
    add_tasks(client, "TIC 1")
    client.post("/tasks/request", json={"worker_id": "w1"})

    response = client.post("/tasks/t1/result", json={"worker_id": "w1", "result": {"period": 0.94}})

    assert response.json() == {"task_id": "t1", "status": "done"}
    task = client.get("/tasks/t1").json()
    assert (task["status"], task["result"]) == ("done", {"period": 0.94})
    assert client.get("/workers").json()["workers"][0]["tasks"] == []


def test_submit_result_from_wrong_worker_is_409(client):
    register(client, "w1")
    register(client, "w2")
    add_tasks(client, "TIC 1")
    client.post("/tasks/request", json={"worker_id": "w1"})

    response = client.post("/tasks/t1/result", json={"worker_id": "w2", "result": {}})

    assert response.status_code == 409
    assert client.get("/tasks/t1").json()["status"] == "assigned"


def test_submit_result_for_unknown_task_is_404(client):
    register(client, "w1")

    assert client.post("/tasks/t99/result", json={"worker_id": "w1", "result": {}}).status_code == 404


def test_worker_with_missed_heartbeats_is_marked_offline_and_its_task_requeued(client, clock):
    register(client, "w1")
    register(client, "w2")
    add_tasks(client, "TIC 1", "TIC 2")
    client.post("/tasks/request", json={"worker_id": "w1"})  # w1 takes t1

    clock.now += TIMEOUT - 1
    client.post("/workers/w2/heartbeat")
    assert status_of(client, "w1") == "online"  # not late yet

    clock.now += 2
    assert status_of(client, "w1") == "offline"
    assert status_of(client, "w2") == "online"
    assert client.get("/tasks/t1").json()["status"] == "pending"
    # t1 goes back to the front of the queue, ahead of t2.
    assert client.post("/tasks/request", json={"worker_id": "w2"}).json() == {"task_id": "t1", "star_id": "TIC 1"}


def test_offline_worker_cannot_take_tasks_or_submit_until_it_returns(client, clock):
    register(client, "w1")
    add_tasks(client, "TIC 1")
    client.post("/tasks/request", json={"worker_id": "w1"})
    clock.now += TIMEOUT + 1

    assert client.post("/tasks/request", json={"worker_id": "w1"}).status_code == 409
    assert client.post("/tasks/t1/result", json={"worker_id": "w1", "result": {}}).status_code == 409

    client.post("/workers/w1/heartbeat")
    assert status_of(client, "w1") == "online"
    assert client.post("/tasks/request", json={"worker_id": "w1"}).json()["task_id"] == "t1"


def test_reregistering_worker_gives_up_its_old_tasks(client):
    register(client, "w1")
    add_tasks(client, "TIC 1")
    client.post("/tasks/request", json={"worker_id": "w1"})

    register(client, "w1")  # e.g. the worker crashed and restarted

    assert client.get("/tasks/t1").json()["status"] == "pending"


def test_mark_offline_returns_only_newly_offline_workers(client, redis_client, clock):
    register(client, "w1")

    assert mark_offline(redis_client, clock.now + TIMEOUT + 1, TIMEOUT) == ["w1"]
    assert mark_offline(redis_client, clock.now + TIMEOUT + 2, TIMEOUT) == []


def test_background_sweep_marks_workers_offline_without_requests(redis_client, clock):
    app = create_app(redis_client, heartbeat_timeout=TIMEOUT, sweep_interval=0.01, clock=clock)
    with TestClient(app) as client:
        register(client, "w1")
        clock.now += TIMEOUT + 1
        deadline = time.monotonic() + 2
        while redis_client.hget(worker_key("w1"), "status") != "offline" and time.monotonic() < deadline:
            time.sleep(0.01)

    assert redis_client.hget(worker_key("w1"), "status") == "offline"


def interrupt_once(monkeypatch, commands, method, key, action):
    """Run action() right after the first read of `key` with `method`, i.e. between a read and its write.

    Patches the redis-py command class, so it catches the read whether it goes through the client
    or a pipeline.
    """
    real = getattr(commands, method)
    fired = []

    def patched(self, name, *args, **kwargs):
        out = real(self, name, *args, **kwargs)
        if name == key and not fired:
            fired.append(True)
            action()
        return out

    monkeypatch.setattr(commands, method, patched)


def test_overlapping_checks_requeue_a_task_only_once(client, redis_client, clock, monkeypatch):
    register(client, "w1")
    add_tasks(client, "TIC 1", "TIC 2")
    client.post("/tasks/request", json={"worker_id": "w1"})
    # Two checks on two threads, lined up so both read w1's status and both read its tasks
    # before either writes: the worst-case interleaving.
    barrier = threading.Barrier(2, timeout=5)
    paused = threading.local()
    reads = [(HashCommands, "hmget", worker_key("w1")), (SetCommands, "smembers", worker_tasks_key("w1"))]
    for commands, method, key in reads:
        real = getattr(commands, method)

        def patched(self, name, *args, _real=real, _key=key, **kwargs):
            out = _real(self, name, *args, **kwargs)
            if name == _key and _key not in vars(paused):
                setattr(paused, _key, True)  # pause only on each thread's first read; retries run free
                barrier.wait()
            return out

        monkeypatch.setattr(commands, method, patched)

    late = clock.now + TIMEOUT + 1
    marked = []
    def check():
        marked.extend(mark_offline(redis_client, late, TIMEOUT))

    threads = [threading.Thread(target=check) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert redis_client.lrange(PENDING_KEY, 0, -1) == ["t1", "t2"]
    assert marked == ["w1"]


def test_check_overlapping_reregister_requeues_a_task_only_once(client, redis_client, clock, monkeypatch):
    register(client, "w1")
    add_tasks(client, "TIC 1")
    client.post("/tasks/request", json={"worker_id": "w1"})
    reregister = lambda: requeue_tasks(redis_client, "w1")  # noqa: E731
    interrupt_once(monkeypatch, SetCommands, "smembers", worker_tasks_key("w1"), reregister)

    mark_offline(redis_client, clock.now + TIMEOUT + 1, TIMEOUT)

    assert redis_client.lrange(PENDING_KEY, 0, -1) == ["t1"]


def test_heartbeat_during_check_keeps_worker_online(client, redis_client, clock, monkeypatch):
    register(client, "w1")
    add_tasks(client, "TIC 1")
    client.post("/tasks/request", json={"worker_id": "w1"})
    clock.now += TIMEOUT + 1
    # The heartbeat arrives after the check has seen w1 as late, before it marks w1 offline.
    heartbeat = lambda: client.post("/workers/w1/heartbeat")  # noqa: E731
    interrupt_once(monkeypatch, HashCommands, "hmget", worker_key("w1"), heartbeat)

    assert mark_offline(redis_client, clock.now, TIMEOUT) == []
    assert redis_client.hget(worker_key("w1"), "status") == "online"
    assert client.get("/tasks/t1").json()["status"] == "assigned"
