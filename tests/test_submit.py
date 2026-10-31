import fakeredis
from fastapi.testclient import TestClient

from antsearch import submit
from antsearch.coordinator import create_app


def test_read_stars_names_the_first_stars_as_tois(tmp_path):
    stars = tmp_path / "stars.csv"
    stars.write_text("star_id,has_planet,num_sectors,period\n102,True,38,4.41\n103,True,2,3.55\n1049,True,3,2.18\n")

    assert submit.read_stars(stars, limit=2) == ["TOI-102", "TOI-103"]
    assert submit.read_stars(stars) == ["TOI-102", "TOI-103", "TOI-1049"]


def test_bundled_star_list_has_enough_stars_for_a_50_star_run():
    stars = submit.read_stars()

    assert len(stars) >= 50
    assert len(set(stars)) == len(stars)


def test_submit_queues_stars_and_wait_for_returns_finished_tasks():
    with TestClient(create_app(fakeredis.FakeRedis(decode_responses=True))) as api:
        task_ids = submit.submit(api, ["TOI-102", "TOI-103"])
        api.post("/workers/register", json={"worker_id": "w1"})
        for _ in task_ids:
            task = api.post("/tasks/request", json={"worker_id": "w1"}).json()
            api.post(f"/tasks/{task['task_id']}/result", json={"worker_id": "w1", "result": {"star_id": task["star_id"]}})

        tasks = submit.wait_for(api, task_ids, poll_interval=0)

    assert [(t["task_id"], t["star_id"], t["status"]) for t in tasks] == [
        ("t1", "TOI-102", "done"), ("t2", "TOI-103", "done"),
    ]


def test_summarize_lists_each_task_and_counts_per_worker():
    tasks = [
        {"task_id": "t1", "star_id": "TOI-102", "worker_id": "w1", "result": {"period": 4.41305, "depth": 0.0131}},
        {"task_id": "t2", "star_id": "TOI-9999", "worker_id": "w2", "result": {"error": "NoDataError", "message": "none"}},
        {"task_id": "t3", "star_id": "TOI-103", "worker_id": "w1", "result": {"simulated": True}},
    ]

    summary = submit.summarize(tasks, elapsed=12.3).splitlines()

    assert "P = 4.41305 d, depth = 13100 ppm" in summary[0]
    assert "NoDataError: none" in summary[1]
    assert summary[2].endswith("done")
    assert summary[4:] == ["3 tasks in 12 s, 1 failed", "  w1: 2 tasks", "  w2: 1 tasks"]
