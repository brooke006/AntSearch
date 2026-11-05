import json
import threading

import fakeredis
import pytest
from fastapi.testclient import TestClient

from antsearch import coordinator
from antsearch.coordinator import create_app, open_run_log
from antsearch.runlog import RunLog, read_events

TIMEOUT = 30


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def path(tmp_path):
    return tmp_path / "runs" / "run.jsonl"


@pytest.fixture
def client(path, clock):
    run_log = RunLog(path, clock)
    return TestClient(create_app(fakeredis.FakeRedis(decode_responses=True), heartbeat_timeout=TIMEOUT, clock=clock, run_log=run_log))


def register(client, *worker_ids):
    for worker_id in worker_ids:
        client.post("/workers/register", json={"worker_id": worker_id})


def take_task(client, worker_id, star_id="WASP-18"):
    client.post("/tasks", json={"star_ids": [star_id]})
    return client.post("/tasks/request", json={"worker_id": worker_id}).json()["task_id"]


def events_named(path, *names):
    return [event for event in read_events(path) if event["event"] in names]


def test_record_writes_one_timestamped_json_line_per_event(path, clock):
    run_log = RunLog(path, clock)
    run_log.record("task_queued", task_id="t1")
    clock.now = 1001.5
    run_log.record("task_assigned", task_id="t1", worker_id="w1")

    assert [json.loads(line) for line in path.read_text().splitlines()] == [
        {"time": 1000.0, "event": "task_queued", "task_id": "t1"},
        {"time": 1001.5, "event": "task_assigned", "task_id": "t1", "worker_id": "w1"},
    ]


def test_events_reach_the_file_straight_away_so_a_kill_loses_nothing(path):
    RunLog(path).record("task_queued", task_id="t1")

    assert len(read_events(path)) == 1


def test_a_run_log_with_no_path_records_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    RunLog().record("task_queued", task_id="t1")

    assert list(tmp_path.iterdir()) == []


def test_a_failed_write_does_not_raise(path, caplog):
    run_log = RunLog(path)
    run_log.file = FailingFile()

    run_log.record("task_queued", task_id="t1")

    assert "could not write task_queued" in caplog.text


class FailingFile:
    def write(self, text):
        raise OSError("no space left on device")


def test_concurrent_records_stay_on_their_own_lines(path):
    run_log = RunLog(path)

    def record_many(thread):
        for n in range(200):
            run_log.record("task_queued", task_id=f"t{thread}-{n}", star_id="x" * 2000)

    threads = [threading.Thread(target=record_many, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len({event["task_id"] for event in read_events(path)}) == 1600


def test_read_events_ignores_a_line_cut_off_by_a_kill(path, caplog):
    path.parent.mkdir()
    path.write_text('{"time": 1.0, "event": "task_queued"}\n{"time": 2.0, "eve')

    assert read_events(path) == [{"time": 1.0, "event": "task_queued"}]
    assert "unfinished line" in caplog.text


def test_read_events_of_an_empty_log(path):
    path.parent.mkdir()
    path.write_text("")

    assert read_events(path) == []


def test_queueing_and_assigning_are_logged_with_the_star(client, path, clock):
    register(client, "w1")
    clock.now = 1005.0
    client.post("/tasks", json={"star_ids": ["WASP-18"]})
    clock.now = 1007.0
    client.post("/tasks/request", json={"worker_id": "w1"})

    assert events_named(path, "task_queued", "task_assigned") == [
        {"time": 1005.0, "event": "task_queued", "task_id": "t1", "star_id": "WASP-18"},
        {"time": 1007.0, "event": "task_assigned", "task_id": "t1", "star_id": "WASP-18", "worker_id": "w1"},
    ]


def test_a_request_with_no_task_logs_nothing(client, path):
    register(client, "w1")

    assert client.post("/tasks/request", json={"worker_id": "w1"}).status_code == 204

    assert events_named(path, "task_assigned") == []


def test_results_are_logged_in_full(client, path):
    register(client, "w1")
    task_id = take_task(client, "w1")
    result = {"period": 0.94, "compute_seconds": 2.0}

    client.post(f"/tasks/{task_id}/result", json={"worker_id": "w1", "result": result})

    (event,) = events_named(path, "task_done")
    assert (event["task_id"], event["worker_id"], event["result"]) == (task_id, "w1", result)


def test_an_error_result_is_logged_with_its_error(client, path):
    register(client, "w1")
    task_id = take_task(client, "w1")
    result = {"error": "NoDataError", "message": "no SPOC 2-minute light curve"}

    client.post(f"/tasks/{task_id}/result", json={"worker_id": "w1", "result": result})

    (event,) = events_named(path, "task_done")
    assert event["result"] == result


def test_a_silent_worker_logs_going_offline_and_each_task_it_lost(client, path, clock):
    register(client, "w1")
    task_id = take_task(client, "w1")
    clock.now = 1010.0
    client.post("/workers/w1/heartbeat")

    clock.now = 1010.0 + TIMEOUT + 1
    client.get("/workers")

    assert events_named(path, "worker_offline", "task_requeued") == [
        {"time": clock.now, "event": "worker_offline", "worker_id": "w1", "last_heartbeat": 1010.0},
        {
            "time": clock.now,
            "event": "task_requeued",
            "task_id": task_id,
            "worker_id": "w1",
            "reason": "offline",
            "last_heartbeat": 1010.0,
        },
    ]


def test_going_offline_is_logged_once(client, path, clock):
    register(client, "w1")
    clock.now += TIMEOUT + 1
    client.get("/workers")
    client.get("/workers")

    assert len(events_named(path, "worker_offline")) == 1


def test_a_worker_coming_back_is_logged(client, path, clock):
    register(client, "w1")
    clock.now += TIMEOUT + 1
    client.get("/workers")
    client.post("/workers/w1/heartbeat")
    client.post("/workers/w1/heartbeat")

    assert len(events_named(path, "worker_online")) == 1


def test_a_restarted_worker_logs_the_task_it_gave_up(client, path, clock):
    register(client, "w1")
    task_id = take_task(client, "w1")
    clock.now = 1004.0
    client.post("/workers/w1/heartbeat")

    clock.now = 1009.0
    register(client, "w1")

    (event,) = events_named(path, "task_requeued")
    assert (event["task_id"], event["reason"], event["last_heartbeat"]) == (task_id, "reregistered", 1004.0)
    assert [event["time"] for event in events_named(path, "worker_registered")] == [1000.0, 1009.0]


def test_a_late_result_is_logged_as_rejected(client, path, clock):
    register(client, "w1")
    task_id = take_task(client, "w1")
    clock.now += TIMEOUT + 1
    client.get("/workers")

    response = client.post(f"/tasks/{task_id}/result", json={"worker_id": "w1", "result": {"period": 1.0}})

    assert response.status_code == 409
    (event,) = events_named(path, "result_rejected")
    assert (event["task_id"], event["worker_id"], event["status"], event["result"]) == (task_id, "w1", "pending", {"period": 1.0})
    assert events_named(path, "task_done") == []


def test_a_coordinator_with_no_run_log_still_works():
    client = TestClient(create_app(fakeredis.FakeRedis(decode_responses=True)))
    register(client, "w1")

    assert take_task(client, "w1") == "t1"


def test_a_restarted_coordinator_keeps_writing_to_the_runs_file(tmp_path):
    database = fakeredis.FakeRedis(decode_responses=True)
    first = open_run_log(database, tmp_path, "greedy", clock=lambda: 0.0)
    second = open_run_log(database, tmp_path, "greedy", clock=lambda: 500.0)

    assert first.path == second.path == tmp_path / "19700101-000000-greedy.jsonl"


def test_a_fresh_redis_starts_a_new_file(tmp_path):
    first = open_run_log(fakeredis.FakeRedis(decode_responses=True), tmp_path, "greedy", clock=lambda: 0.0)
    second = open_run_log(fakeredis.FakeRedis(decode_responses=True), tmp_path, "greedy", clock=lambda: 500.0)

    assert first.path != second.path


def test_main_writes_the_run_log_where_the_environment_says(monkeypatch, tmp_path):
    database = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: database)
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SCHEDULER", "random")
    monkeypatch.setenv("SCHEDULER_SEED", "4")
    monkeypatch.setenv("HEARTBEAT_TIMEOUT", "12")

    def run(app, **kwargs):
        with TestClient(app) as client:
            client.post("/tasks", json={"star_ids": ["star1"]})

    monkeypatch.setattr(coordinator.uvicorn, "run", run)
    coordinator.main()

    (log_file,) = tmp_path.glob("*-random.jsonl")
    started, queued = read_events(log_file)
    assert (started["event"], started["scheduler"], started["seed"], started["heartbeat_timeout"]) == ("coordinator_started", "random", 4, 12.0)
    assert queued["event"] == "task_queued"
