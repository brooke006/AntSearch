import threading
import time
from concurrent.futures import ThreadPoolExecutor

import fakeredis
import pytest
from fastapi.testclient import TestClient
from redis.commands.core import ListCommands

from antsearch import coordinator
from antsearch.coordinator import PENDING_KEY, create_app
from antsearch.scheduler import GreedyScheduler, RandomScheduler, RoundRobinScheduler
from antsearch.worker import register, run_tasks, simulated_task


@pytest.fixture
def cluster():
    database = fakeredis.FakeRedis(decode_responses=True)
    now = [1000.0]
    app = create_app(database, clock=lambda: now[0], scheduler=RoundRobinScheduler())
    with TestClient(app) as client:
        for worker in ["worker1", "worker2"]:
            client.post("/workers/register", json={"worker_id": worker})
        yield client, database, now


def test_worker_waits_for_its_turn(cluster):
    client, database, _ = cluster
    client.post("/tasks", json={"star_ids": ["star1", "star2"]})

    assert client.post("/tasks/request", json={"worker_id": "worker2"}).status_code == 204
    assert database.lrange(PENDING_KEY, 0, -1) == ["t1", "t2"]
    assert client.post("/tasks/request", json={"worker_id": "worker1"}).json()["task_id"] == "t1"
    assert client.post("/tasks/request", json={"worker_id": "worker2"}).json()["task_id"] == "t2"


def test_busy_worker_does_not_block_another_worker(cluster):
    client, _, _ = cluster
    client.post("/tasks", json={"star_ids": ["star1", "star2", "star3"]})
    client.post("/tasks/request", json={"worker_id": "worker1"})
    client.post("/tasks/request", json={"worker_id": "worker2"})
    assert client.post("/tasks/request", json={"worker_id": "worker1"}).status_code == 204
    client.post("/tasks/t2/result", json={"worker_id": "worker2", "result": {}})

    assert client.post("/tasks/request", json={"worker_id": "worker2"}).json()["task_id"] == "t3"


def test_joining_worker_can_take_existing_pending_work():
    database = fakeredis.FakeRedis(decode_responses=True)
    with TestClient(create_app(database, scheduler=RoundRobinScheduler())) as client:
        client.post("/workers/register", json={"worker_id": "worker1"})
        client.post("/tasks", json={"star_ids": ["star1", "star2", "star3"]})
        client.post("/tasks/request", json={"worker_id": "worker1"})
        client.post("/workers/register", json={"worker_id": "worker2"})

        assert client.post("/tasks/request", json={"worker_id": "worker2"}).json()["task_id"] == "t2"


def test_offline_worker_is_skipped_and_its_task_recovers(cluster):
    client, _, now = cluster
    client.post("/tasks", json={"star_ids": ["star1", "star2"]})
    client.post("/tasks/request", json={"worker_id": "worker1"})
    now[0] += 31
    client.post("/workers/worker2/heartbeat")

    assert client.post("/tasks/request", json={"worker_id": "worker2"}).json()["task_id"] == "t1"
    assert client.post("/tasks/t1/result", json={"worker_id": "worker1", "result": {}}).status_code == 409


def test_concurrent_requests_do_not_claim_the_same_task(cluster):
    client, database, _ = cluster
    client.post("/tasks", json={"star_ids": ["star1", "star2"]})

    def request(_):
        response = client.post("/tasks/request", json={"worker_id": "worker1"})
        if response.status_code == 200:
            return response.json()["task_id"]
        assert response.status_code == 204
        return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(request, range(8)))

    assert results.count("t1") == 1
    assert results.count(None) == 7
    assert database.lrange(PENDING_KEY, 0, -1) == ["t2"]


def test_queue_change_retries_without_skipping_a_turn(cluster, monkeypatch):
    client, database, _ = cluster
    client.post("/tasks", json={"star_ids": ["star1", "star2"]})
    original = ListCommands.lrange
    changed = False

    def interrupt(self, name, start, end):
        nonlocal changed
        result = original(self, name, start, end)
        if name == PENDING_KEY and not changed:
            changed = True
            database.lrem(PENDING_KEY, 1, "t2")
        return result

    monkeypatch.setattr(ListCommands, "lrange", interrupt)
    assert client.post("/tasks/request", json={"worker_id": "worker1"}).json()["task_id"] == "t1"
    client.post("/tasks", json={"star_ids": ["star3"]})
    assert client.post("/tasks/request", json={"worker_id": "worker2"}).json()["task_id"] == "t3"


def test_only_accepted_results_reach_scheduler():
    class RecordingScheduler(RoundRobinScheduler):
        results = []

        def on_result(self, worker_id, result):
            self.results.append((worker_id, result))

    database = fakeredis.FakeRedis(decode_responses=True)
    with TestClient(create_app(database, scheduler=RecordingScheduler())) as client:
        client.post("/workers/register", json={"worker_id": "worker1"})
        client.post("/tasks", json={"star_ids": ["star1"]})
        client.post("/tasks/request", json={"worker_id": "worker1"})
        assert client.post("/tasks/t1/result", json={"worker_id": "other", "result": {}}).status_code == 409
        assert RecordingScheduler.results == []
        assert client.post("/tasks/t1/result", json={"worker_id": "worker1", "result": {"period": 1}}).status_code == 200
        assert RecordingScheduler.results == [("worker1", {"period": 1})]


def test_main_enables_round_robin(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    database = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: database)

    def run(app, **kwargs):
        with TestClient(app) as client:
            for worker in ["worker1", "worker2"]:
                client.post("/workers/register", json={"worker_id": worker})
            client.post("/tasks", json={"star_ids": ["star1"]})
            assert client.post("/tasks/request", json={"worker_id": "worker2"}).status_code == 204
            assert client.post("/tasks/request", json={"worker_id": "worker1"}).status_code == 200

    monkeypatch.setattr(coordinator.uvicorn, "run", run)
    coordinator.main()


@pytest.mark.parametrize("name, expected", [("random", RandomScheduler), ("greedy", GreedyScheduler)])
def test_main_uses_the_scheduler_named_in_the_environment(monkeypatch, tmp_path, name, expected):
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: fakeredis.FakeRedis(decode_responses=True))
    monkeypatch.setattr(coordinator.uvicorn, "run", lambda app, **kwargs: None)
    used = []
    monkeypatch.setattr(coordinator, "create_app", lambda client, **kwargs: used.append(kwargs["scheduler"]))
    monkeypatch.setenv("SCHEDULER", name)
    monkeypatch.setenv("SCHEDULER_SEED", "4")

    coordinator.main()

    assert isinstance(used[0], expected)


def test_main_rejects_an_unknown_scheduler(monkeypatch):
    monkeypatch.setenv("SCHEDULER", "ant")

    with pytest.raises(ValueError, match="unknown SCHEDULER"):
        coordinator.main()


def baseline_cluster(scheduler):
    client = TestClient(create_app(fakeredis.FakeRedis(decode_responses=True), scheduler=scheduler))
    for worker in ["worker1", "worker2"]:
        client.post("/workers/register", json={"worker_id": worker})
    return client


def test_random_choice_survives_between_requests_through_the_coordinator():
    client = baseline_cluster(RandomScheduler(seed=1))
    client.post("/tasks", json={"star_ids": ["star1"]})

    # Whichever worker wasn't chosen keeps getting 204, however often it asks; the other gets the task.
    answers = {worker: [client.post("/tasks/request", json={"worker_id": worker}).status_code for _ in range(5)]
               for worker in ["worker1", "worker2"]}

    assert sorted(answers.values()) == sorted([[204] * 5, [200, 204, 204, 204, 204]])


def test_greedy_learns_speeds_from_results_through_the_coordinator():
    client = baseline_cluster(GreedyScheduler())
    client.post("/tasks", json={"star_ids": ["star1", "star2", "star3"]})
    assert client.post("/tasks/request", json={"worker_id": "worker1"}).json()["task_id"] == "t1"
    assert client.post("/tasks/request", json={"worker_id": "worker2"}).json()["task_id"] == "t2"
    client.post("/tasks/t1/result", json={"worker_id": "worker1", "result": {"compute_seconds": 9}})
    client.post("/tasks/t2/result", json={"worker_id": "worker2", "result": {"compute_seconds": 1}})

    assert client.post("/tasks/request", json={"worker_id": "worker1"}).status_code == 204
    assert client.post("/tasks/request", json={"worker_id": "worker2"}).json()["task_id"] == "t3"


def test_greedy_ignores_a_garbage_result_without_failing_the_request():
    client = baseline_cluster(GreedyScheduler())
    client.post("/tasks", json={"star_ids": ["star1"]})
    client.post("/tasks/request", json={"worker_id": "worker1"})

    response = client.post("/tasks/t1/result", json={"worker_id": "worker1", "result": {"compute_seconds": "fast"}})

    assert response.status_code == 200


@pytest.mark.parametrize("make", [RoundRobinScheduler, RandomScheduler, GreedyScheduler], ids=["round-robin", "random", "greedy"])
def test_two_worker_loops_finish_50_tasks(make):
    database = fakeredis.FakeRedis(decode_responses=True)
    app = create_app(database, scheduler=make())
    stop = threading.Event()

    def worker(worker_id):
        with TestClient(app) as client:
            run_tasks(client, worker_id, lambda star: simulated_task(star, 0.005), stop, 0.005)

    with TestClient(app) as client:
        for worker_id in ["worker1", "worker2"]:
            register(client, worker_id)
        task_ids = client.post("/tasks", json={"star_ids": [f"star{i}" for i in range(50)]}).json()["task_ids"]
        threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in ["worker1", "worker2"]]
        for thread in threads:
            thread.start()
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                tasks = [client.get(f"/tasks/{task_id}").json() for task_id in task_ids]
                if all(task["status"] == "done" for task in tasks):
                    break
                time.sleep(0.01)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert all(task["status"] == "done" for task in tasks)
    assert {task["worker_id"] for task in tasks} == {"worker1", "worker2"}
