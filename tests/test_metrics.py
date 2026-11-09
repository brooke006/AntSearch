import os

import fakeredis
import pytest
from fastapi.testclient import TestClient

from antsearch import metrics
from antsearch.coordinator import create_app
from antsearch.metrics import compute, report
from antsearch.runlog import RunLog, read_events


def queued(time, task):
    return {"time": time, "event": "task_queued", "task_id": task, "star_id": "s"}


def assigned(time, task, worker):
    return {"time": time, "event": "task_assigned", "task_id": task, "star_id": "s", "worker_id": worker}


def done(time, task, worker, **result):
    return {"time": time, "event": "task_done", "task_id": task, "worker_id": worker, "result": result or {"period": 1.0}}


def requeued(time, task, worker, last_heartbeat):
    return {"time": time, "event": "task_requeued", "task_id": task, "worker_id": worker, "reason": "offline", "last_heartbeat": last_heartbeat}


def rejected(time, task, worker, status="pending"):
    return {"time": time, "event": "result_rejected", "task_id": task, "worker_id": worker, "status": status, "result": {}}


def test_clean_run():
    result = compute([
        {"time": 99, "event": "coordinator_started", "scheduler": "greedy", "seed": None, "heartbeat_timeout": 30},
        queued(100, "t1"),
        queued(110, "t2"),
        assigned(101, "t1", "w1"),
        assigned(111, "t2", "w2"),
        done(105, "t1", "w1"),
        done(130, "t2", "w2"),
    ])

    assert result["schedulers"] == ["greedy"]
    assert (result["tasks_queued"], result["tasks_succeeded"], result["tasks_failed"], result["tasks_unfinished"]) == (2, 2, 0, 0)
    assert result["completion_seconds"] == 30  # 100 to 130, not counting the time before the first task
    assert result["mean_turnaround_seconds"] == 12.5  # (5 + 20) / 2
    assert result["throughput_per_minute"] == 4
    assert result["worker_seconds"] == 23  # 4 + 19
    assert result["wasted_seconds"] == 0
    assert result["wasted_fraction"] == 0


def test_task_lost_with_a_worker_wastes_the_time_up_to_its_last_heartbeat():
    result = compute([
        queued(0, "t1"),
        assigned(10, "t1", "w1"),
        requeued(60, "t1", "w1", last_heartbeat=25),  # silent from 25 s, noticed at 60 s
        assigned(60, "t1", "w2"),
        done(70, "t1", "w2"),
    ])

    assert result["lost_runs"] == 1
    assert result["lost_seconds"] == 15
    assert result["wasted_seconds"] == 15
    assert result["worker_seconds"] == 25
    assert result["wasted_fraction"] == pytest.approx(15 / 25)
    assert result["completion_seconds"] == 70
    assert result["tasks_succeeded"] == 1  # the lost run doesn't count as a second task


def test_task_assigned_after_the_last_heartbeat_wastes_nothing_rather_than_negative_time():
    result = compute([queued(0, "t1"), assigned(10, "t1", "w1"), requeued(40, "t1", "w1", last_heartbeat=5)])

    assert result["lost_seconds"] == 0
    assert result["lost_runs"] == 1


def test_late_result_replaces_the_estimate_with_the_whole_time_the_worker_spent():
    result = compute([
        queued(0, "t1"),
        assigned(10, "t1", "w1"),
        requeued(60, "t1", "w1", last_heartbeat=25),
        assigned(60, "t1", "w2"),
        rejected(80, "t1", "w1"),
        done(90, "t1", "w2"),
    ])

    assert result["late_results"] == 1
    assert result["lost_seconds"] == 70  # 10 to 80, not 15 + 70
    assert result["wasted_seconds"] == 70


def test_only_the_first_rejection_of_a_lost_run_counts():
    result = compute([
        queued(0, "t1"),
        assigned(10, "t1", "w1"),
        requeued(60, "t1", "w1", last_heartbeat=25),
        rejected(80, "t1", "w1"),
        rejected(90, "t1", "w1"),
    ])

    assert result["late_results"] == 1
    assert result["lost_seconds"] == 70


def test_rejection_of_a_result_that_was_already_accepted_wastes_nothing():
    # A worker's report times out after the coordinator accepted it, so the retry gets a 409.
    result = compute([queued(0, "t1"), assigned(10, "t1", "w1"), done(20, "t1", "w1"), rejected(21, "t1", "w1", status="done")])

    assert result["late_results"] == 0
    assert result["wasted_seconds"] == 0
    assert result["tasks_succeeded"] == 1


def test_rejection_from_a_worker_that_never_held_the_task_wastes_nothing():
    result = compute([queued(0, "t1"), assigned(10, "t1", "w1"), rejected(20, "t1", "w2", status="assigned")])

    assert result["wasted_seconds"] == 0


def test_failed_runs_count_as_wasted_and_not_towards_throughput():
    result = compute([
        queued(0, "t1"),
        queued(0, "t2"),
        assigned(0, "t1", "w1"),
        assigned(0, "t2", "w2"),
        done(10, "t1", "w1", error="DownloadError", message="MAST down"),
        done(30, "t2", "w2"),
    ])

    assert (result["tasks_succeeded"], result["tasks_failed"], result["tasks_unfinished"]) == (1, 1, 0)
    assert result["failed_seconds"] == 10
    assert result["wasted_seconds"] == 10
    assert result["worker_seconds"] == 40
    assert result["throughput_per_minute"] == 2  # 1 success in 30 s
    assert result["completion_seconds"] == 30


def test_same_worker_losing_the_same_task_twice_adds_up():
    result = compute([
        queued(0, "t1"),
        assigned(0, "t1", "w1"),
        requeued(40, "t1", "w1", last_heartbeat=10),
        assigned(40, "t1", "w1"),
        requeued(80, "t1", "w1", last_heartbeat=50),
    ])

    assert result["lost_runs"] == 2
    assert result["lost_seconds"] == 20


def test_requeue_logged_after_the_next_assignment_still_counts_against_the_right_worker():
    # The offline check commits before it logs, so another worker can log its assignment first.
    result = compute([
        queued(0, "t1"),
        assigned(10, "t1", "w1"),
        assigned(60, "t1", "w2"),
        requeued(60, "t1", "w1", last_heartbeat=25),
        done(70, "t1", "w2"),
    ])

    assert result["lost_seconds"] == 15
    assert result["worker_seconds"] == 25


def test_unfinished_tasks_are_counted_and_left_out_of_the_times():
    result = compute([queued(0, "t1"), queued(0, "t2"), assigned(0, "t1", "w1"), assigned(0, "t2", "w2"), done(10, "t1", "w1")])

    assert result["tasks_unfinished"] == 1
    assert result["completion_seconds"] == 10
    assert result["worker_seconds"] == 10


def test_a_run_with_no_results_has_no_times():
    for events in ([], [queued(0, "t1")]):
        result = compute(events)
        assert result["completion_seconds"] is None
        assert result["mean_turnaround_seconds"] is None
        assert result["throughput_per_minute"] is None
        assert result["wasted_fraction"] is None
        assert "no task has finished yet" in report(result)


def test_a_run_where_every_task_failed_can_be_reported():
    result = compute([queued(0, "t1"), assigned(0, "t1", "w1"), done(5, "t1", "w1", error="NoDataError")])

    assert result["throughput_per_minute"] == 0
    assert "0.0 succeeded tasks per minute" in report(result)


def test_a_task_finished_in_the_same_instant_it_was_queued_can_be_reported():
    result = compute([queued(5, "t1"), assigned(5, "t1", "w1"), done(5, "t1", "w1")])

    assert result["completion_seconds"] == 0
    assert result["throughput_per_minute"] is None
    assert "completion time: 0.0 s" in report(result)


def test_report_shows_the_three_metrics():
    text = report(compute([
        {"time": 0, "event": "coordinator_started", "scheduler": "greedy", "seed": None, "heartbeat_timeout": 30},
        queued(0, "t1"),
        assigned(10, "t1", "w1"),
        requeued(60, "t1", "w1", last_heartbeat=25),
        assigned(60, "t1", "w2"),
        done(70, "t1", "w2"),
    ]))

    assert "scheduler:       greedy" in text
    assert "completion time: 70.0 s" in text
    assert "throughput:      0.9 succeeded tasks per minute" in text
    assert "wasted work:     15.0 s of 25.0 s worker time (60.0%)" in text


def test_report_warns_when_the_scheduler_changed_mid_run():
    text = report(compute([
        {"time": 0, "event": "coordinator_started", "scheduler": "greedy", "seed": None, "heartbeat_timeout": 30},
        {"time": 1, "event": "coordinator_started", "scheduler": "random", "seed": None, "heartbeat_timeout": 30},
        {"time": 2, "event": "coordinator_started", "scheduler": "random", "seed": None, "heartbeat_timeout": 30},
    ]))

    assert "greedy, random" in text
    assert "changed mid-run" in text


def test_metrics_from_a_log_the_coordinator_really_wrote(tmp_path):
    now = [1000.0]
    path = tmp_path / "run.jsonl"
    client = TestClient(create_app(
        fakeredis.FakeRedis(decode_responses=True), heartbeat_timeout=30, clock=lambda: now[0], run_log=RunLog(path, lambda: now[0])
    ))
    for worker in ("w1", "w2"):
        client.post("/workers/register", json={"worker_id": worker})
    client.post("/tasks", json={"star_ids": ["star1", "star2"]})
    now[0] = 1002.0
    client.post("/tasks/request", json={"worker_id": "w1"})  # t1
    now[0] = 1020.0
    client.post("/workers/w1/heartbeat")  # w1 goes silent after this, 18 s into t1
    client.post("/workers/w2/heartbeat")
    now[0] = 1040.0
    client.post("/workers/w2/heartbeat")
    now[0] = 1051.0
    client.get("/workers")  # 31 s since w1's heartbeat, so t1 goes back on the queue
    t1 = client.post("/tasks/request", json={"worker_id": "w2"}).json()["task_id"]
    now[0] = 1061.0
    client.post(f"/tasks/{t1}/result", json={"worker_id": "w2", "result": {"period": 1.0}})  # 10 s
    client.post("/workers/w2/heartbeat")
    t2 = client.post("/tasks/request", json={"worker_id": "w2"}).json()["task_id"]
    now[0] = 1064.0
    client.post(f"/tasks/{t2}/result", json={"worker_id": "w2", "result": {"error": "NoDataError"}})  # 3 s

    result = compute(read_events(path))

    assert (result["tasks_queued"], result["tasks_succeeded"], result["tasks_failed"], result["tasks_unfinished"]) == (2, 1, 1, 0)
    assert result["completion_seconds"] == 64
    assert result["throughput_per_minute"] == pytest.approx(60 / 64)
    assert result["mean_turnaround_seconds"] == pytest.approx((61 + 64) / 2)
    assert result["lost_seconds"] == 18
    assert result["failed_seconds"] == 3
    assert result["wasted_seconds"] == 21
    assert result["worker_seconds"] == 31


def test_main_reads_the_newest_log_by_default(tmp_path, monkeypatch, capsys):
    older, newer = tmp_path / "older.jsonl", tmp_path / "newer.jsonl"
    RunLog(older).record("task_queued", task_id="t1", star_id="s")
    RunLog(newer).record("task_queued", task_id="t1", star_id="s")
    RunLog(newer).record("task_queued", task_id="t2", star_id="s")
    os.utime(older, (1, 1))
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))

    assert metrics.main([]) == 0

    out = capsys.readouterr().out
    assert str(newer) in out
    assert "2 queued" in out


def test_main_reads_the_log_it_is_given(tmp_path, monkeypatch, capsys):
    path = tmp_path / "run.jsonl"
    RunLog(path).record("task_queued", task_id="t1", star_id="s")
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path / "elsewhere"))

    assert metrics.main([str(path)]) == 0

    assert "1 queued" in capsys.readouterr().out


def test_main_says_when_there_are_no_logs(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))

    with pytest.raises(SystemExit) as exit_info:
        metrics.main([])

    assert exit_info.value.code == 2
    assert "no run logs" in capsys.readouterr().err


@pytest.mark.parametrize("content", [None, "not json\n{}\n"])
def test_main_says_when_a_log_cannot_be_read(tmp_path, capsys, content):
    path = tmp_path / "run.jsonl"
    if content is not None:
        path.write_text(content)

    with pytest.raises(SystemExit) as exit_info:
        metrics.main([str(path)])

    assert exit_info.value.code == 2
    assert "can't read" in capsys.readouterr().err
