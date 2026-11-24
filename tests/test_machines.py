import json
import threading

import fakeredis
import httpx
from fastapi.testclient import TestClient

from antsearch import metrics
from antsearch.coordinator import create_app
from antsearch.metrics import compute, describe, report
from antsearch.runlog import RunLog, read_events
from antsearch.worker import connect, container_limits, machine_info, register

LAPTOP = {"hostname": "laptop", "os": "Darwin 24.1.0", "arch": "arm64", "cpu": "Apple M2", "cpus": 8, "memory_gb": 16.0,
          "docker": False, "python": "3.12.4", "numpy": "2.1.0", "astropy": "6.1.0", "lightkurve": "2.5.0"}


def test_machine_info_describes_this_machine():
    info = machine_info()

    assert set(info) == {"hostname", "os", "arch", "cpu", "cpus", "memory_gb", "docker", "python", "numpy", "astropy", "lightkurve"}
    assert info["cpus"] >= 1
    assert json.loads(json.dumps(info)) == info


def test_container_limits_come_from_cgroup_files(tmp_path):
    assert container_limits(tmp_path) == (None, None)  # not in a container

    (tmp_path / "cpu.max").write_text("50000 100000\n")
    (tmp_path / "memory.max").write_text(f"{3 * 2**29}\n")
    assert container_limits(tmp_path) == (0.5, 1.5)

    (tmp_path / "cpu.max").write_text("max 100000\n")
    (tmp_path / "memory.max").write_text("max\n")
    assert container_limits(tmp_path) == (None, None)


def test_connect_retries_until_the_coordinator_answers(caplog):
    answers = iter([httpx.ConnectError("refused"), httpx.Response(503), httpx.Response(200, json={"heartbeat_timeout": 9})])

    def handler(request):
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    client = httpx.Client(base_url="http://coordinator", transport=httpx.MockTransport(handler))
    waits = []
    stop = threading.Event()
    stop.wait = lambda seconds: waits.append(seconds) or False

    assert connect(client, "w1", stop, 2.0) == 9
    assert waits == [2.0, 2.0]
    assert "can't register with coordinator" in caplog.text


def test_connect_gives_up_when_stopped():
    def refuse(request):
        raise httpx.ConnectError("refused")

    client = httpx.Client(base_url="http://coordinator", transport=httpx.MockTransport(refuse))
    stop = threading.Event()
    stop.set()

    assert connect(client, "w1", stop, 2.0) is None


def cluster(tmp_path, now):
    path = tmp_path / "run.jsonl"
    app = create_app(fakeredis.FakeRedis(decode_responses=True), heartbeat_timeout=600, clock=lambda: now[0], run_log=RunLog(path, lambda: now[0]))
    return TestClient(app), path


def machines(client):
    return {worker["worker_id"]: worker["machine"] for worker in client.get("/workers").json()["workers"]}


def test_coordinator_keeps_and_logs_each_workers_machine(tmp_path):
    client, path = cluster(tmp_path, [1000.0])

    register(client, "w1")
    client.post("/workers/register", json={"worker_id": "laptop", "machine": LAPTOP})
    client.post("/workers/register", json={"worker_id": "old"})

    assert machines(client) == {"w1": machine_info(), "laptop": LAPTOP, "old": None}
    logged = {event["worker_id"]: event.get("machine") for event in read_events(path) if event["event"] == "worker_registered"}
    assert logged == {"w1": machine_info(), "laptop": LAPTOP, "old": None}


def test_registering_again_without_a_machine_keeps_the_old_one(tmp_path):
    client, _ = cluster(tmp_path, [1000.0])
    client.post("/workers/register", json={"worker_id": "laptop", "machine": LAPTOP})
    client.post("/workers/register", json={"worker_id": "laptop"})

    assert machines(client) == {"laptop": LAPTOP}


def test_machine_must_be_flat(tmp_path):
    client, _ = cluster(tmp_path, [1000.0])

    response = client.post("/workers/register", json={"worker_id": "w1", "machine": {"cpu": {"cores": 8}}})

    assert response.status_code == 422


def test_metrics_break_the_run_down_by_worker_and_machine(tmp_path):
    now = [1000.0]
    client, path = cluster(tmp_path, now)
    client.post("/workers/register", json={"worker_id": "laptop", "machine": LAPTOP})
    client.post("/workers/register", json={"worker_id": "desktop"})
    client.post("/tasks", json={"star_ids": ["a", "b", "c", "d"]})
    for task, worker, seconds, result in [("t1", "laptop", 10, {"period": 1.0}), ("t2", "desktop", 4, {"period": 2.0}),
                                          ("t3", "laptop", 30, {"period": 3.0}), ("t4", "laptop", 1, {"error": "NoDataError"})]:
        assert client.post("/tasks/request", json={"worker_id": worker}).json()["task_id"] == task
        now[0] += seconds
        client.post(f"/tasks/{task}/result", json={"worker_id": worker, "result": result})

    result = compute(read_events(path))

    assert result["workers"] == {
        "desktop": {"machine": None, "runs": 1, "failed": 0, "outvoted": 0, "lost": 0, "mean_answer_seconds": 4.0},
        "laptop": {"machine": LAPTOP, "runs": 3, "failed": 1, "outvoted": 0, "lost": 0, "mean_answer_seconds": 20.0},
    }
    lines = report(result).splitlines()
    assert lines[-3].split() == ["worker", "runs", "failed", "outvoted", "lost", "mean", "answer", "machine"]
    assert lines[-2].split()[:7] == ["desktop", "1", "0", "0", "0", "4.0", "s"]
    assert lines[-1].endswith("Darwin 24.1.0 arm64, Apple M2, 8 CPUs, 16 GB")


def test_describe_a_machine():
    assert describe(None) == "-"
    assert describe({"os": "Linux 6.8", "arch": "x86_64", "cpus": 4, "docker": True}) == "Linux 6.8 x86_64, 4 CPUs, in Docker"
    assert describe({"os": "Linux 6.8", "cpus": 0.5, "memory_gb": 1.0}) == "Linux 6.8, 0.5 CPUs, 1 GB"


def test_save_copies_the_log_and_writes_its_report(tmp_path, capsys):
    client, path = cluster(tmp_path, [1000.0])
    client.post("/workers/register", json={"worker_id": "laptop", "machine": LAPTOP})
    saved = tmp_path / "docs" / "runs"

    assert metrics.main([str(path), "--save", str(saved)]) == 0
    assert metrics.main([str(saved / "run.jsonl"), "--save", str(saved)]) == 0  # saving a saved log again is fine

    assert (saved / "run.jsonl").read_text() == path.read_text()
    text = (saved / "run.txt").read_text()
    assert text.startswith(f"{saved / 'run.jsonl'}\n")
    assert "laptop" in text and "Apple M2" in text


def test_two_machines_with_the_same_worker_id_get_a_warning(tmp_path, caplog):
    client, _ = cluster(tmp_path, [1000.0])
    client.post("/workers/register", json={"worker_id": "mac", "machine": LAPTOP})
    client.post("/workers/register", json={"worker_id": "mac", "machine": LAPTOP})  # the same laptop restarting
    assert "WORKER_ID" not in caplog.text

    client.post("/workers/register", json={"worker_id": "mac", "machine": {**LAPTOP, "hostname": "other"}})

    assert "worker mac registered from other while still online from laptop" in caplog.text
