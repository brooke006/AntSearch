import math
import threading

import fakeredis
import pytest

from antsearch.pheromone import (
    DEFAULT_INITIAL_LEVEL,
    MULTI_SECTOR,
    SHORT,
    TASK_TYPES,
    PheromoneTable,
    pheromone_key,
    task_type,
)


@pytest.fixture
def server():
    return fakeredis.FakeServer()


@pytest.fixture
def redis_client(server):
    return fakeredis.FakeRedis(server=server, decode_responses=True)


@pytest.fixture
def table(redis_client):
    return PheromoneTable(redis_client)


@pytest.mark.parametrize("sectors, expected", [(0, SHORT), (1, SHORT), (2, MULTI_SECTOR), (44, MULTI_SECTOR)])
def test_task_type_splits_single_sector_from_multi_sector(sectors, expected):
    assert task_type(sectors) == expected


@pytest.mark.parametrize("sectors", [-1, 1.5, "3", None, True])
def test_task_type_rejects_bad_sector_counts(sectors):
    with pytest.raises(ValueError):
        task_type(sectors)


def test_unset_pairs_read_as_the_initial_level_without_writing(table, redis_client):
    assert table.get("w1", SHORT) == DEFAULT_INITIAL_LEVEL
    assert table.levels(MULTI_SECTOR, ["w1", "w2"]) == {"w1": DEFAULT_INITIAL_LEVEL, "w2": DEFAULT_INITIAL_LEVEL}
    assert redis_client.keys("antsearch:pheromone:*") == []


def test_initial_level_is_configurable(redis_client):
    table = PheromoneTable(redis_client, initial_level=0.1)
    assert table.get("w1", SHORT) == 0.1
    assert table.add("w1", SHORT, 0.5) == pytest.approx(0.6)


def test_levels_are_keyed_by_worker_and_task_type(table):
    table.set("w1", SHORT, 3.0)
    table.set("w1", MULTI_SECTOR, 0.5)
    table.set("w2", SHORT, 2.0)

    assert table.get("w1", SHORT) == 3.0
    assert table.get("w1", MULTI_SECTOR) == 0.5
    assert table.get("w2", SHORT) == 2.0
    assert table.get("w2", MULTI_SECTOR) == DEFAULT_INITIAL_LEVEL
    assert table.levels(SHORT, ["w1", "w2", "w3"]) == {"w1": 3.0, "w2": 2.0, "w3": DEFAULT_INITIAL_LEVEL}
    assert table.table() == {"w1": {SHORT: 3.0, MULTI_SECTOR: 0.5}, "w2": {SHORT: 2.0}}


def test_levels_are_stored_in_redis_hashes_per_task_type(table, redis_client):
    table.set("w1", SHORT, 3.0)
    table.set("w2", MULTI_SECTOR, 0.25)

    assert redis_client.hgetall(pheromone_key(SHORT)) == {"w1": "3.0"}
    assert redis_client.hgetall(pheromone_key(MULTI_SECTOR)) == {"w2": "0.25"}


def test_levels_with_no_workers_is_empty(table):
    assert table.levels(SHORT, []) == {}


def test_add_starts_from_the_initial_level_and_accumulates(table):
    assert table.add("w1", SHORT, 0.5) == pytest.approx(1.5)
    assert table.add("w1", SHORT, 0.25) == pytest.approx(1.75)
    assert table.get("w1", SHORT) == pytest.approx(1.75)
    assert table.get("w1", MULTI_SECTOR) == DEFAULT_INITIAL_LEVEL


def test_add_after_set_builds_on_the_set_level(table):
    table.set("w1", SHORT, 0.0)
    assert table.add("w1", SHORT, 2.0) == pytest.approx(2.0)


def test_concurrent_adds_all_count(server):
    def deposit():
        table = PheromoneTable(fakeredis.FakeRedis(server=server, decode_responses=True))
        for _ in range(50):
            table.add("w1", SHORT, 1.0)

    threads = [threading.Thread(target=deposit) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    reader = PheromoneTable(fakeredis.FakeRedis(server=server, decode_responses=True))
    assert reader.get("w1", SHORT) == pytest.approx(DEFAULT_INITIAL_LEVEL + 200)


@pytest.mark.parametrize("call", ["get", "set", "add", "levels"])
def test_unknown_task_type_is_rejected(table, call):
    args = {"get": ("w1", "long"), "set": ("w1", "long", 1.0), "add": ("w1", "long", 1.0), "levels": ("long", ["w1"])}
    with pytest.raises(ValueError, match="unknown task type"):
        getattr(table, call)(*args[call])


@pytest.mark.parametrize("bad", [-0.1, math.inf, math.nan, "1.0", None, True])
def test_bad_levels_and_amounts_are_rejected(table, redis_client, bad):
    with pytest.raises(ValueError):
        table.set("w1", SHORT, bad)
    with pytest.raises(ValueError):
        table.add("w1", SHORT, bad)
    with pytest.raises(ValueError):
        PheromoneTable(redis_client, initial_level=bad)
    assert table.table() == {}


def test_forget_worker_removes_only_that_worker(table):
    for kind in TASK_TYPES:
        table.set("w1", kind, 5.0)
        table.set("w2", kind, 4.0)

    table.forget_worker("w1")

    assert table.get("w1", SHORT) == DEFAULT_INITIAL_LEVEL
    assert table.table() == {"w2": {SHORT: 4.0, MULTI_SECTOR: 4.0}}


def test_clear_removes_every_level(table, redis_client):
    table.set("w1", SHORT, 5.0)
    table.set("w2", MULTI_SECTOR, 4.0)

    table.clear()

    assert table.table() == {}
    assert redis_client.keys("antsearch:pheromone:*") == []


def test_worker_ids_with_separator_like_characters_stay_distinct(table):
    table.set("a:b", SHORT, 2.0)
    table.set("a", SHORT, 3.0)
    assert table.levels(SHORT, ["a:b", "a"]) == {"a:b": 2.0, "a": 3.0}


def test_table_survives_a_coordinator_restart(server):
    # The levels live in Redis, not in the table object, so a restarted coordinator (a new process
    # with a new connection to the same Redis) reads them back and carries on.
    first_client = fakeredis.FakeRedis(server=server, decode_responses=True)
    first_table = PheromoneTable(first_client)
    first_table.add("w1", SHORT, 2.0)
    first_table.set("w1", MULTI_SECTOR, 0.3)
    first_client.close()

    second_table = PheromoneTable(fakeredis.FakeRedis(server=server, decode_responses=True))

    assert second_table.table() == {"w1": {SHORT: pytest.approx(3.0), MULTI_SECTOR: pytest.approx(0.3)}}
    assert second_table.add("w1", SHORT, 1.0) == pytest.approx(4.0)
