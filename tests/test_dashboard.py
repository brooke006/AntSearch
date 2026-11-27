import fakeredis
import pytest

from antsearch.coordinator import (
    PENDING_KEY,
    WORKERS_KEY,
    worker_key,
    worker_tasks_key,
)

from antsearch.dashboard import (
    snapshot,
)

from antsearch.pheromone import PheromoneTable

@pytest.fixture
def server():
    return fakeredis.FakeServer()

@pytest.fixture
def redis_client(server):
    return fakeredis.FakeRedis(server=server, decode_responses=True)


def test_snapshot_reads_everything(redis_client):
    # Set up: two workers, one holding two tasks, two tasks waiting, one pheromon deposit
    redis_client.sadd(WORKERS_KEY, "worker-2", "worker-1")
    redis_client.hset(worker_key("worker-1"), mapping={"status": "online"})
    redis_client.hset(worker_key("worker-2"), mapping={"status": "offline"})
    redis_client.sadd(worker_tasks_key("worker-1"), "3", "1")
    redis_client.rpush(PENDING_KEY, "7", "8")
    PheromoneTable(redis_client).add("worker-1", "short", 0.5)

    # Act
    snap = snapshot(redis_client)

    # Check
    assert list(snap["workers"]) == ["worker-1", "worker-2"]
    assert snap["workers"]["worker-1"] == {"status": "online", "tasks": ["1", "3"]}
    assert snap["workers"]["worker-2"] == {"status": "offline", "tasks": []}
    assert snap["pending"] == ["7", "8"]
    assert snap["pheromones"]["worker-1"]["short"] == 1.5

def test_snapshot_with_empty_redis(redis_client):
    # Start with empty redis
    # Act
    snap = snapshot(redis_client)

    # Check
    assert snap["workers"] == {}
    assert snap["pending"] == []
    assert snap["pheromones"] == {}
