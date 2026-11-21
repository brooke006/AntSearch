import random
import threading
import time

import fakeredis
import pytest
from fastapi.testclient import TestClient
from redis.commands.core import HashCommands

from antsearch import coordinator
from antsearch.coordinator import PENDING_KEY, create_app, task_results_key, task_type_lookup, worker_key
from antsearch.metrics import compute
from antsearch.pheromone import SHORT, PheromoneTable
from antsearch.runlog import RunLog, read_events
from antsearch.scheduler import AntScheduler
from antsearch.worker import register, run_tasks

TIMEOUT = 30
ANSWER = {"star_id": "TOI-102", "period": 4.4119, "depth": 0.0011}
GARBAGE = {"star_id": "TOI-102", "period": 0.123, "depth": 0.9}


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
def path(tmp_path):
    return tmp_path / "run.jsonl"


@pytest.fixture
def client(redis_client, clock, path):
    app = create_app(redis_client, heartbeat_timeout=TIMEOUT, clock=clock, run_log=RunLog(path, clock), replicas=2)
    with TestClient(app) as client:
        for worker_id in ("w1", "w2", "w3"):
            client.post("/workers/register", json={"worker_id": worker_id})
        yield client


def take(client, worker_id):
    response = client.post("/tasks/request", json={"worker_id": worker_id})
    return None if response.status_code == 204 else response.json()["task_id"]


def report(client, worker_id, task_id, result):
    response = client.post(f"/tasks/{task_id}/result", json={"worker_id": worker_id, "result": result})
    assert response.status_code == 200, response.text
    return response.json()


def queue(client, *star_ids):
    return client.post("/tasks", json={"star_ids": list(star_ids)}).json()["task_ids"]


def task(client, task_id):
    return client.get(f"/tasks/{task_id}").json()


def flags(client):
    return {w["worker_id"]: (w["flagged"], w["disputed"]) for w in client.get("/workers").json()["workers"]}


def events(path, name=None):
    return [event for event in read_events(path) if name is None or event["event"] == name]


# Accepting, rerunning and flagging


def test_matching_results_are_accepted(client, path):
    (task_id,) = queue(client, "TOI-102")
    assert take(client, "w1") == task_id
    assert take(client, "w2") == task_id

    assert report(client, "w1", task_id, {**ANSWER, "compute_seconds": 8.0}) == {"task_id": task_id, "status": "assigned"}
    assert task(client, task_id)["status"] == "assigned"  # one result isn't enough
    assert report(client, "w2", task_id, {**ANSWER, "compute_seconds": 5.0}) == {"task_id": task_id, "status": "done"}

    done = task(client, task_id)
    assert (done["status"], done["outcome"], done["worker_id"]) == ("done", "accepted", "w1")
    assert done["result"] == {**ANSWER, "compute_seconds": 8.0}
    assert [(run["worker_id"], run["verdict"]) for run in done["runs"]] == [("w1", "accepted"), ("w2", "accepted")]
    (event,) = events(path, "task_done")
    assert (event["outcome"], event["agreed"]) == ("accepted", ["w1", "w2"])
    assert events(path, "task_rerun") == []
    assert take(client, "w3") is None  # nothing more to run


def test_a_mismatch_triggers_a_third_run(client, path):
    (task_id,) = queue(client, "TOI-102")
    take(client, "w1")
    take(client, "w2")
    report(client, "w1", task_id, ANSWER)

    assert report(client, "w2", task_id, GARBAGE) == {"task_id": task_id, "status": "pending"}

    (rerun,) = events(path, "task_rerun")
    assert (rerun["task_id"], rerun["runs"]) == (task_id, 1)
    # Neither worker that already ran it can take the third run; w3 can.
    assert take(client, "w1") is None
    assert take(client, "w2") is None
    assert take(client, "w3") == task_id
    report(client, "w3", task_id, ANSWER)

    done = task(client, task_id)
    assert (done["status"], done["result"], done["worker_id"]) == ("done", ANSWER, "w1")
    assert [(run["worker_id"], run["verdict"]) for run in done["runs"]] == [("w1", "accepted"), ("w2", "disputed"), ("w3", "accepted")]
    assert events(path, "task_done")[0]["agreed"] == ["w1", "w3"]


def test_a_worker_that_returns_garbage_gets_flagged(redis_client, path):
    """Three honest worker loops and one that makes up its answers, through 30 stars."""
    app = create_app(redis_client, run_log=RunLog(path), replicas=2)
    stop = threading.Event()
    answers = random.Random(0)

    def honest(star):
        return {"star_id": star, "period": len(star) * 1.5, "compute_seconds": 0.001}

    def garbage(star):
        return {"star_id": star, "period": answers.uniform(0.5, 15), "compute_seconds": 0.001}

    def worker(worker_id, job):
        with TestClient(app) as client:
            run_tasks(client, worker_id, job, stop, 0.002)

    jobs = {"honest-1": honest, "honest-2": honest, "honest-3": honest, "faulty": garbage}
    with TestClient(app) as client:
        for worker_id in jobs:
            register(client, worker_id)
        task_ids = queue(client, *[f"star-{n}" for n in range(30)])
        threads = [threading.Thread(target=worker, args=item) for item in jobs.items()]
        for thread in threads:
            thread.start()
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                tasks = [task(client, task_id) for task_id in task_ids]
                if all(each["status"] == "done" for each in tasks):
                    break
                time.sleep(0.01)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)
        found = flags(client)

    assert all(each["status"] == "done" for each in tasks)
    # Every accepted answer is the honest one, even for stars the faulty worker ran.
    assert all(each["result"] == honest(each["star_id"]) for each in tasks)
    faulty_runs = sum(run["worker_id"] == "faulty" for each in tasks for run in each["runs"])
    assert faulty_runs > 0
    assert found["faulty"] == (True, faulty_runs)
    assert all(found[worker_id] == (False, 0) for worker_id in ("honest-1", "honest-2", "honest-3"))
    assert {event["worker_id"] for event in events(path, "result_disputed")} == {"faulty"}


# Assigning runs


def test_each_run_goes_to_a_different_worker(client):
    t1, t2 = queue(client, "a", "b")

    assert take(client, "w1") == t1
    assert take(client, "w1") == t2  # its second request skips t1's other run
    assert take(client, "w2") == t1
    assert take(client, "w2") == t2
    assert take(client, "w3") is None


def test_a_lone_worker_waits_for_a_second_one(redis_client, clock):
    with TestClient(create_app(redis_client, clock=clock, replicas=2)) as client:
        client.post("/workers/register", json={"worker_id": "w1"})
        (task_id,) = queue(client, "a")
        take(client, "w1")
        report(client, "w1", task_id, ANSWER)

        assert take(client, "w1") is None
        assert task(client, task_id)["status"] == "pending"
        client.post("/workers/register", json={"worker_id": "w2"})
        assert take(client, "w2") == task_id
        report(client, "w2", task_id, ANSWER)
        assert task(client, task_id)["status"] == "done"


def test_a_lost_run_goes_back_on_the_queue_for_another_worker(client, clock, path):
    (task_id,) = queue(client, "a")
    take(client, "w1")
    take(client, "w2")
    report(client, "w1", task_id, ANSWER)
    clock.now += TIMEOUT + 1
    for worker_id in ("w1", "w3"):
        client.post(f"/workers/{worker_id}/heartbeat")

    client.get("/workers")  # w2 is marked offline, and its run requeued

    assert task(client, task_id)["status"] == "pending"
    assert take(client, "w1") is None
    assert take(client, "w3") == task_id
    late = client.post(f"/tasks/{task_id}/result", json={"worker_id": "w2", "result": ANSWER})
    assert late.status_code == 409
    report(client, "w3", task_id, ANSWER)
    assert task(client, task_id)["outcome"] == "accepted"
    assert events(path, "task_done")[0]["agreed"] == ["w1", "w3"]


def test_scheduler_chooses_among_workers_free_to_run_the_task(redis_client):
    # Ant would otherwise often choose w1, which already ran t1, and t1's second run would wait.
    app = create_app(redis_client, scheduler=AntScheduler(PheromoneTable(redis_client), task_type_lookup(redis_client), seed=3), replicas=2)
    with TestClient(app) as client:
        for worker_id in ("w1", "w2"):
            client.post("/workers/register", json={"worker_id": worker_id})
        (task_id,) = queue(client, "a")
        first = next(worker for _ in range(20) for worker in ("w1", "w2") if take(client, worker))
        report(client, first, task_id, ANSWER)
        other = "w2" if first == "w1" else "w1"

        assert take(client, first) is None
        assert take(client, other) == task_id


# Settling


def test_two_failures_of_the_same_kind_settle_the_task_as_failed(client):
    (task_id,) = queue(client, "a")
    take(client, "w1")
    take(client, "w2")
    report(client, "w1", task_id, {"error": "NoDataError", "message": "w1 found nothing"})
    report(client, "w2", task_id, {"error": "NoDataError", "message": "w2 found nothing"})

    done = task(client, task_id)
    assert (done["status"], done["outcome"], done["result"]["error"]) == ("done", "accepted", "NoDataError")
    assert flags(client)["w1"] == (False, 0)


def test_a_failed_run_gets_another_run_but_no_flag(client):
    (task_id,) = queue(client, "a")
    take(client, "w1")
    take(client, "w2")
    report(client, "w1", task_id, {"error": "DownloadError", "message": "MAST timed out"})
    report(client, "w2", task_id, ANSWER)
    assert take(client, "w3") == task_id
    report(client, "w3", task_id, ANSWER)

    done = task(client, task_id)
    assert done["result"] == ANSWER
    assert [run["verdict"] for run in done["runs"]] == ["failed", "accepted", "accepted"]
    assert flags(client)["w1"] == (False, 0)  # a failure isn't a wrong answer


def test_no_agreement_after_three_runs_is_unresolved_and_flags_nobody(client, redis_client, path):
    (task_id,) = queue(client, "a")
    take(client, "w1")
    take(client, "w2")
    report(client, "w1", task_id, ANSWER)
    report(client, "w2", task_id, GARBAGE)
    take(client, "w3")
    report(client, "w3", task_id, {**ANSWER, "period": 7.0})

    done = task(client, task_id)
    assert (done["status"], done["outcome"], done["result"]["error"]) == ("done", "unresolved", "Unresolved")
    assert "worker_id" not in done
    assert [run["verdict"] for run in done["runs"]] == ["unresolved"] * 3
    assert all(flag == (False, 0) for flag in flags(client).values())
    assert redis_client.lrange(PENDING_KEY, 0, -1) == []
    (event,) = events(path, "task_done")
    assert (event["outcome"], event["agreed"], event["worker_id"]) == ("unresolved", [], None)


def test_results_arriving_together_settle_the_task_once(client, redis_client, path, monkeypatch):
    (task_id,) = queue(client, "a")
    take(client, "w1")
    take(client, "w2")
    # w2's result lands while w1's is being judged, between its read of the results and its write.
    real = HashCommands.hgetall
    fired = []

    def patched(self, name, *args, **kwargs):
        out = real(self, name, *args, **kwargs)
        if name == task_results_key(task_id) and not fired:
            fired.append(True)
            report(client, "w2", task_id, ANSWER)
        return out

    monkeypatch.setattr(HashCommands, "hgetall", patched)
    report(client, "w1", task_id, ANSWER)
    monkeypatch.undo()

    assert len(events(path, "task_done")) == 1
    assert len(events(path, "result_received")) == 2
    assert {run["worker_id"] for run in task(client, task_id)["runs"]} == {"w1", "w2"}
    assert redis_client.lrange(PENDING_KEY, 0, -1) == []


def test_one_replica_accepts_the_first_result(redis_client):
    with TestClient(create_app(redis_client)) as client:
        client.post("/workers/register", json={"worker_id": "w1"})
        (task_id,) = queue(client, "a")
        take(client, "w1")
        report(client, "w1", task_id, GARBAGE)
        assert task(client, task_id)["result"] == GARBAGE


@pytest.mark.parametrize("replicas", [0, -1, 1.5, True])
def test_replicas_must_be_a_whole_number_of_at_least_one(redis_client, replicas):
    with pytest.raises(ValueError):
        create_app(redis_client, replicas=replicas)


# What the scheduler and the metrics see


def test_an_outvoted_worker_earns_no_pheromone(redis_client, clock, path):
    table = PheromoneTable(redis_client)
    scheduler = AntScheduler(table, task_type_lookup(redis_client), seed=1)
    app = create_app(redis_client, heartbeat_timeout=TIMEOUT, clock=clock, scheduler=scheduler, run_log=RunLog(path, clock), replicas=2)
    with TestClient(app) as client:
        for worker_id in ("w1", "w2", "w3"):
            client.post("/workers/register", json={"worker_id": worker_id})
        (task_id,) = queue(client, "a")
        holders = [worker for _ in range(20) for worker in ("w1", "w2", "w3") if take(client, worker)]
        clock.now += 1.0
        report(client, holders[0], task_id, ANSWER)
        report(client, holders[1], task_id, GARBAGE)
        third = next(worker for _ in range(20) for worker in ("w1", "w2", "w3") if take(client, worker))
        clock.now += 1.0
        report(client, third, task_id, ANSWER)

    (done,) = events(path, "task_done")
    assert done["pheromone"] == {holders[0]: "deposited", holders[1]: "disputed", third: "deposited"}
    # It started at the ceiling, then evaporated in its own round and the next, with nothing added.
    assert table.get(holders[1], SHORT) == pytest.approx(1.0 * 0.8 * 0.8)
    assert table.get(third, SHORT) == 1.0


def test_metrics_count_each_task_once_and_outvoted_runs_as_wasted(client, clock, path):
    t1, t2 = queue(client, "a", "b")
    for worker_id in ("w1", "w2"):
        take(client, worker_id)
    clock.now += 10
    report(client, "w1", t1, ANSWER)
    report(client, "w2", t1, GARBAGE)
    take(client, "w3")
    clock.now += 10
    report(client, "w3", t1, ANSWER)
    take(client, "w1")
    take(client, "w2")
    clock.now += 5
    report(client, "w1", t2, ANSWER)
    report(client, "w2", t2, ANSWER)

    metrics = compute(read_events(path))

    assert (metrics["tasks_queued"], metrics["tasks_succeeded"], metrics["tasks_failed"]) == (2, 2, 0)
    assert (metrics["disputed_runs"], metrics["disputed_seconds"], metrics["reruns"]) == (1, 10.0, 1)
    assert metrics["worker_seconds"] == 10 + 10 + 10 + 5 + 5
    assert metrics["wasted_seconds"] == 10.0


def test_main_replicates_twice_by_default(monkeypatch, tmp_path):
    used = run_main(monkeypatch, tmp_path)
    assert used["replicas"] == 2
    started = read_events(next(tmp_path.glob("*.jsonl")))[0]
    assert started["replicas"] == 2


def test_main_reads_replicas(monkeypatch, tmp_path):
    monkeypatch.setenv("REPLICAS", "3")
    assert run_main(monkeypatch, tmp_path)["replicas"] == 3


def run_main(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: fakeredis.FakeRedis(decode_responses=True))
    monkeypatch.setattr(coordinator.uvicorn, "run", lambda app, **kwargs: None)
    used = {}
    monkeypatch.setattr(coordinator, "create_app", lambda client, **kwargs: used.update(kwargs))
    coordinator.main()
    return used


def test_worker_hash_keeps_its_disputed_count_across_reregistering(client, redis_client):
    (task_id,) = queue(client, "a")
    take(client, "w1")
    take(client, "w2")
    report(client, "w1", task_id, GARBAGE)
    report(client, "w2", task_id, ANSWER)
    take(client, "w3")
    report(client, "w3", task_id, ANSWER)
    client.post("/workers/register", json={"worker_id": "w1"})

    assert redis_client.hget(worker_key("w1"), "disputed") == "1"
    assert flags(client)["w1"] == (True, 1)
