import copy
import itertools
import json
import math
from collections import Counter

import fakeredis
import pytest
from fastapi.testclient import TestClient

from antsearch import coordinator, submit
from antsearch.coordinator import create_app, task_key, task_type_lookup
from antsearch.pheromone import MULTI_SECTOR, SHORT, PheromoneTable
from antsearch.scheduler import AntScheduler, RandomScheduler, Scheduler, make_scheduler


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def pheromones(redis_client):
    return PheromoneTable(redis_client)


def measured(scheduler, **seconds):
    """Give each named worker one timed result."""
    for worker, value in seconds.items():
        scheduler.on_result(worker, {"compute_seconds": value})
    return scheduler


def run_one_at_a_time(scheduler, speeds, count):
    """Hand out count tasks one at a time, every worker free for every choice, and count who got them.

    The workers ask in turn until one is given the task, then report a result that took its speed in
    seconds. Every worker is free each time, so who gets more is the scheduler's choice alone: with
    workers pulling from a queue, a fast worker also wins tasks just by being free more often.
    """
    workers = sorted(speeds)
    counts = Counter()
    for n in range(count):
        task = f"t{n}"
        for worker in itertools.cycle(workers):
            if scheduler.assign(worker, [task], workers) == task:
                break
        counts[worker] += 1
        scheduler.on_result(worker, {"compute_seconds": speeds[worker]})
    return counts


# --- The Ant System rule -----------------------------------------------------------------------


def test_is_a_scheduler(pheromones):
    assert isinstance(AntScheduler(pheromones), Scheduler)


def test_probability_is_pheromone_to_the_alpha_times_speed_to_the_beta(pheromones):
    pheromones.set("w1", SHORT, 2.0)
    pheromones.set("w2", SHORT, 1.0)
    scheduler = measured(AntScheduler(pheromones, alpha=1, beta=2), w1=2.0, w2=1.0)

    # w1: 2 ** 1 * (1/2) ** 2 = 0.5;  w2: 1 ** 1 * (1/1) ** 2 = 1
    assert scheduler.probabilities(SHORT, ["w1", "w2"]) == pytest.approx({"w1": 1 / 3, "w2": 2 / 3})


@pytest.mark.parametrize(
    "alpha, beta, expected",
    [
        (0, 0, {"w1": 0.5, "w2": 0.5}),  # uniform
        (1, 0, {"w1": 0.8, "w2": 0.2}),  # pheromone only: 4 vs 1
        (2, 0, {"w1": 16 / 17, "w2": 1 / 17}),  # pheromone squared: 16 vs 1
        (0, 1, {"w1": 0.25, "w2": 0.75}),  # speed only: 1/3 vs 1
        (1, 1, {"w1": 4 / 7, "w2": 3 / 7}),  # 4/3 vs 1
    ],
)
def test_alpha_and_beta_weigh_pheromone_against_speed(pheromones, alpha, beta, expected):
    pheromones.set("w1", SHORT, 4.0)
    pheromones.set("w2", SHORT, 1.0)
    scheduler = measured(AntScheduler(pheromones, alpha=alpha, beta=beta), w1=3.0, w2=1.0)

    assert scheduler.probabilities(SHORT, ["w1", "w2"]) == pytest.approx(expected)


def test_uses_the_pheromone_for_the_tasks_type(pheromones):
    pheromones.set("w1", SHORT, 9.0)
    pheromones.set("w2", MULTI_SECTOR, 9.0)
    types = {"short-task": SHORT, "long-task": MULTI_SECTOR}
    scheduler = AntScheduler(pheromones, types.__getitem__, alpha=1, beta=0)

    assert scheduler.probabilities(SHORT, ["w1", "w2"]) == pytest.approx({"w1": 0.9, "w2": 0.1})
    assert scheduler.probabilities(MULTI_SECTOR, ["w1", "w2"]) == pytest.approx({"w1": 0.1, "w2": 0.9})

    # With zero pheromone off its own type, each worker can only win its own type of task.
    pheromones.set("w1", MULTI_SECTOR, 0.0)
    pheromones.set("w2", SHORT, 0.0)
    assert scheduler.assign("w1", ["long-task"], ["w1", "w2"]) is None
    assert scheduler.assign("w2", ["long-task"], ["w1", "w2"]) == "long-task"
    assert scheduler.assign("w2", ["short-task"], ["w1", "w2"]) is None
    assert scheduler.assign("w1", ["short-task"], ["w1", "w2"]) == "short-task"


def test_tasks_without_a_type_lookup_count_as_short(pheromones):
    pheromones.set("w1", SHORT, 0.0)
    scheduler = AntScheduler(pheromones, alpha=1, beta=0)

    assert all(scheduler.assign("w1", ["t1"], ["w1", "w2"]) is None for _ in range(20))
    assert scheduler.assign("w2", ["t1"], ["w1", "w2"]) == "t1"


def test_unmeasured_worker_counts_as_fast_as_the_fastest(pheromones):
    scheduler = measured(AntScheduler(pheromones, alpha=0, beta=1), w1=2.0, w2=4.0)

    assert scheduler.probabilities(SHORT, ["w1", "w2", "new"]) == pytest.approx({"w1": 0.4, "w2": 0.2, "new": 0.4})


def test_worker_that_has_only_failed_counts_as_slow_as_the_slowest(pheromones):
    scheduler = measured(AntScheduler(pheromones, alpha=0, beta=1), w1=2.0, w2=4.0)
    scheduler.on_result("broken", {"error": "NoDataError"})

    assert scheduler.probabilities(SHORT, ["w1", "w2", "broken"]) == pytest.approx({"w1": 0.5, "w2": 0.25, "broken": 0.25})


def test_with_nobody_measured_the_choice_follows_pheromone(pheromones):
    pheromones.set("w1", SHORT, 3.0)
    scheduler = AntScheduler(pheromones, alpha=1, beta=2)
    scheduler.on_result("w2", {"error": "boom"})

    assert scheduler.probabilities(SHORT, ["w1", "w2"]) == pytest.approx({"w1": 0.75, "w2": 0.25})


def test_speed_is_a_moving_average_of_recent_results(pheromones):
    scheduler = AntScheduler(pheromones, smoothing=0.5)
    for seconds in [1.0, 3.0, 5.0]:
        scheduler.on_result("w1", {"download_seconds": seconds, "compute_seconds": 0})

    assert scheduler.estimates["w1"] == pytest.approx(3.5)  # 1, then 2, then 3.5
    scheduler.on_result("w1", {"error": "boom"})
    assert scheduler.estimates["w1"] == pytest.approx(3.5)


def test_a_zero_second_result_does_not_divide_by_zero(pheromones):
    scheduler = measured(AntScheduler(pheromones), w1=0, w2=1)

    probabilities = scheduler.probabilities(SHORT, ["w1", "w2"])
    assert probabilities["w1"] > 0.99
    assert math.isclose(sum(probabilities.values()), 1)


def test_zero_pheromone_rules_a_worker_out_unless_everyone_has_zero(pheromones):
    pheromones.set("w1", SHORT, 0.0)
    scheduler = AntScheduler(pheromones)

    assert scheduler.probabilities(SHORT, ["w1", "w2"]) == {"w1": 0.0, "w2": 1.0}
    pheromones.set("w2", SHORT, 0.0)
    assert scheduler.probabilities(SHORT, ["w1", "w2"]) == {"w1": 0.5, "w2": 0.5}


def test_probabilities_of_no_workers(pheromones):
    assert AntScheduler(pheromones).probabilities(SHORT, []) == {}


@pytest.mark.parametrize("bad", [-1, math.inf, math.nan, "1", None, True])
def test_bad_alpha_or_beta_is_rejected(pheromones, bad):
    with pytest.raises(ValueError, match="alpha"):
        AntScheduler(pheromones, alpha=bad)
    with pytest.raises(ValueError, match="beta"):
        AntScheduler(pheromones, beta=bad)


def test_does_not_change_the_pheromone_table(pheromones, redis_client):
    pheromones.set("w1", SHORT, 2.0)
    before = redis_client.hgetall("antsearch:pheromone:short")
    run_one_at_a_time(AntScheduler(pheromones, seed=1), {"w1": 1, "w2": 2}, 30)

    assert redis_client.hgetall("antsearch:pheromone:short") == before
    assert redis_client.keys("antsearch:pheromone:multi_sector") == []


# --- Faster workers get more tasks -------------------------------------------------------------


@pytest.mark.parametrize("seed", range(5))
def test_faster_fake_worker_receives_more_tasks(pheromones, seed):
    speeds = {"fast": 1.0, "slow": 4.0}

    ant = run_one_at_a_time(AntScheduler(pheromones, seed=seed), speeds, 200)
    random = run_one_at_a_time(RandomScheduler(seed=seed), speeds, 200)

    # Ant System with the default beta of 2: once both are measured, 1 ** 2 vs (1/4) ** 2, so the
    # fast worker's chance is 16/17 (94%). Random gives each about half.
    assert ant["fast"] > 0.85 * 200
    assert 0 < ant["slow"] < 0.15 * 200  # the slow worker is still explored
    assert 0.35 * 200 < random["fast"] < 0.65 * 200


def test_tasks_are_shared_in_order_of_speed(pheromones):
    counts = run_one_at_a_time(AntScheduler(pheromones, seed=3), {"w1": 1.0, "w2": 2.0, "w3": 4.0}, 300)

    assert counts["w1"] > counts["w2"] > counts["w3"] > 0


def test_beta_zero_ignores_speed(pheromones):
    counts = run_one_at_a_time(AntScheduler(pheromones, beta=0, seed=3), {"fast": 1.0, "slow": 4.0}, 400)

    assert 0.4 * 400 < counts["fast"] < 0.6 * 400


def test_a_higher_beta_favours_the_faster_worker_more(pheromones):
    speeds = {"fast": 1.0, "slow": 2.0}
    shares = [run_one_at_a_time(AntScheduler(pheromones, beta=beta, seed=5), speeds, 400)["fast"] for beta in (0.5, 1, 3)]

    assert shares[0] < shares[1] < shares[2]


def test_pheromone_favours_a_worker_of_the_same_speed(pheromones):
    pheromones.set("trusted", SHORT, 4.0)
    counts = run_one_at_a_time(AntScheduler(pheromones, seed=3), {"trusted": 1.0, "other": 1.0}, 300)

    assert counts["trusted"] > 0.7 * 300  # 4 to 1
    assert counts["other"] > 0


def test_a_worker_that_slows_down_gets_fewer_tasks(pheromones):
    scheduler = AntScheduler(pheromones, seed=2)
    before = run_one_at_a_time(scheduler, {"w1": 1.0, "w2": 1.0}, 200)
    after = run_one_at_a_time(scheduler, {"w1": 5.0, "w2": 1.0}, 200)

    assert 0.4 * 200 < before["w1"] < 0.6 * 200
    assert after["w1"] < 0.2 * 200


# --- Choosing in a pull model ------------------------------------------------------------------


@pytest.mark.parametrize("seed", range(5))
def test_choice_is_kept_until_that_worker_asks(pheromones, seed):
    scheduler = AntScheduler(pheromones, seed=seed)
    workers = ["w1", "w2", "w3"]
    if scheduler.assign("w1", ["t1"], workers) == "t1":
        return  # w1 was chosen and took the task straight away
    chosen = scheduler.chosen
    others = [worker for worker in workers if worker != chosen]

    for _ in range(20):
        assert all(scheduler.assign(worker, ["t1"], workers) is None for worker in others)
        assert scheduler.chosen == chosen
    assert scheduler.assign(chosen, ["t1"], workers) == "t1"
    assert scheduler.chosen is None


def test_polling_more_often_does_not_win_more_tasks(pheromones):
    def share(polls):
        scheduler = AntScheduler(pheromones, seed=9)
        wins = Counter()
        for n in range(300):
            task = f"t{n}"
            while True:
                if any(scheduler.assign("eager", [task], ["eager", "calm"]) for _ in range(polls)):
                    wins["eager"] += 1
                    break
                if scheduler.assign("calm", [task], ["eager", "calm"]):
                    wins["calm"] += 1
                    break
        return wins["eager"]

    assert share(1) == share(5)
    assert 0.4 * 300 < share(5) < 0.6 * 300


def test_chooses_again_when_the_chosen_worker_stops_being_available(pheromones):
    pheromones.set("w1", SHORT, 0.0)
    scheduler = AntScheduler(pheromones)

    assert scheduler.assign("w1", ["t1"], ["w1", "w2"]) is None
    assert scheduler.chosen == "w2"
    # w2 goes busy or offline: w1 is the only one left, so it gets the task despite its zero pheromone.
    assert scheduler.assign("w1", ["t1"], ["w1"]) == "t1"


def test_same_seed_gives_the_same_choices(pheromones):
    speeds = {"w1": 1.0, "w2": 2.0, "w3": 3.0}

    first = run_one_at_a_time(AntScheduler(pheromones, seed=11), speeds, 100)
    assert run_one_at_a_time(AntScheduler(pheromones, seed=11), speeds, 100) == first


def test_nothing_to_assign(pheromones):
    assert AntScheduler(pheromones).assign("w1", [], ["w1"]) is None
    assert AntScheduler(pheromones).assign("w1", ["t1"], []) is None
    assert AntScheduler(pheromones).assign("w2", ["t1"], ["w1"]) is None


def test_copy_shares_the_pheromone_table_but_not_the_choice_state(pheromones):
    scheduler = measured(AntScheduler(pheromones, seed=1), w1=1.0)
    clone = copy.deepcopy(scheduler)

    assert clone.pheromones is scheduler.pheromones
    clone.on_result("w1", {"compute_seconds": 9.0})
    clone.assign("w1", ["t1"], ["w1", "w2"])
    assert scheduler.estimates == {"w1": 1.0}
    assert scheduler.chosen is None
    assert scheduler.random.getstate() != clone.random.getstate()


def test_make_scheduler_builds_the_ant_scheduler(pheromones):
    scheduler = make_scheduler("ant", seed=3, pheromones=pheromones, alpha=0.5, beta=3)

    assert isinstance(scheduler, AntScheduler)
    assert (scheduler.alpha, scheduler.beta, scheduler.pheromones) == (0.5, 3.0, pheromones)
    with pytest.raises(ValueError, match="pheromone table"):
        make_scheduler("ant")


# --- Through the coordinator -------------------------------------------------------------------


def test_faster_fake_worker_receives_more_tasks_through_the_coordinator(redis_client):
    speeds = {"fast": 1.0, "slow": 4.0}
    scheduler = AntScheduler(PheromoneTable(redis_client), task_type_lookup(redis_client), seed=7)
    with TestClient(create_app(redis_client, scheduler=scheduler)) as client:
        for worker in speeds:
            client.post("/workers/register", json={"worker_id": worker})
        task_ids = client.post("/tasks", json={"star_ids": [f"star{n}" for n in range(100)]}).json()["task_ids"]

        # Both workers are free for every choice: whoever is given a task finishes it before the next.
        for _ in task_ids:
            for worker in itertools.cycle(speeds):
                response = client.post("/tasks/request", json={"worker_id": worker})
                if response.status_code == 200:
                    break
            task_id = response.json()["task_id"]
            result = {"compute_seconds": speeds[worker]}
            assert client.post(f"/tasks/{task_id}/result", json={"worker_id": worker, "result": result}).status_code == 200

        done_by = Counter(client.get(f"/tasks/{task_id}").json()["worker_id"] for task_id in task_ids)

    assert done_by["fast"] > 80
    assert done_by["slow"] > 0


def test_queued_sector_counts_set_each_tasks_type(redis_client):
    with TestClient(create_app(redis_client)) as client:
        response = client.post("/tasks", json={"star_ids": ["a", "b", "c"], "num_sectors": [1, 0, 5]})
        client.post("/tasks", json={"star_ids": ["d"]})

        assert response.status_code == 201
        assert [client.get(f"/tasks/t{n}").json().get("task_type") for n in (1, 2, 3, 4)] == [SHORT, SHORT, MULTI_SECTOR, None]

    lookup = task_type_lookup(redis_client)
    assert [lookup(f"t{n}") for n in (1, 2, 3, 4)] == [SHORT, SHORT, MULTI_SECTOR, SHORT]
    assert redis_client.hget(task_key("t3"), "task_type") == MULTI_SECTOR


@pytest.mark.parametrize("sectors", [[1], [1, 2, 3], [1, -1]])
def test_sector_counts_must_match_the_stars(redis_client, sectors):
    with TestClient(create_app(redis_client)) as client:
        response = client.post("/tasks", json={"star_ids": ["a", "b"], "num_sectors": sectors})

    assert response.status_code == 422
    assert redis_client.keys("antsearch:task:*") == []


def test_ant_scheduler_uses_the_type_of_the_task_at_the_front(redis_client):
    pheromones = PheromoneTable(redis_client)
    pheromones.set("short-expert", SHORT, 1.0)
    pheromones.set("short-expert", MULTI_SECTOR, 0.0)
    pheromones.set("long-expert", SHORT, 0.0)
    pheromones.set("long-expert", MULTI_SECTOR, 1.0)
    scheduler = AntScheduler(pheromones, task_type_lookup(redis_client), seed=1)
    with TestClient(create_app(redis_client, scheduler=scheduler)) as client:
        for worker in ["short-expert", "long-expert"]:
            client.post("/workers/register", json={"worker_id": worker})
        client.post("/tasks", json={"star_ids": ["long", "short"], "num_sectors": [7, 1]})

        assert client.post("/tasks/request", json={"worker_id": "short-expert"}).status_code == 204
        assert client.post("/tasks/request", json={"worker_id": "long-expert"}).json()["star_id"] == "long"
        assert client.post("/tasks/request", json={"worker_id": "short-expert"}).json()["star_id"] == "short"


def test_main_builds_the_ant_scheduler_from_the_environment(monkeypatch, tmp_path):
    database = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SCHEDULER", "ant")
    monkeypatch.setenv("SCHEDULER_SEED", "4")
    monkeypatch.setenv("SCHEDULER_ALPHA", "0.5")
    monkeypatch.setenv("SCHEDULER_BETA", "3")
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: database)
    monkeypatch.setattr(coordinator.uvicorn, "run", lambda app, **kwargs: None)
    used = []
    monkeypatch.setattr(coordinator, "create_app", lambda client, **kwargs: used.append(kwargs["scheduler"]))

    coordinator.main()

    scheduler = used[0]
    assert isinstance(scheduler, AntScheduler)
    assert (scheduler.alpha, scheduler.beta) == (0.5, 3.0)
    assert scheduler.pheromones.redis is database
    database.hset(task_key("t9"), "task_type", MULTI_SECTOR)
    assert scheduler.task_type_of("t9") == MULTI_SECTOR
    started = json.loads(next(tmp_path.glob("*.jsonl")).read_text().splitlines()[0])
    assert (started["scheduler"], started["seed"], started["alpha"], started["beta"]) == ("ant", 4, 0.5, 3.0)


def test_main_defaults_alpha_and_beta(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SCHEDULER", "ant")
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: fakeredis.FakeRedis(decode_responses=True))
    monkeypatch.setattr(coordinator.uvicorn, "run", lambda app, **kwargs: None)
    used = []
    monkeypatch.setattr(coordinator, "create_app", lambda client, **kwargs: used.append(kwargs["scheduler"]))

    coordinator.main()

    assert (used[0].alpha, used[0].beta) == (1.0, 2.0)


# --- Submitting sector counts ------------------------------------------------------------------


def test_read_sectors(tmp_path):
    stars = tmp_path / "stars.csv"
    stars.write_text("star_id,has_planet,num_sectors,period\n102,True,38,4.41\n103,True,1,3.55\n1049,True,3,2.18\n")
    no_column = tmp_path / "bare.csv"
    no_column.write_text("star_id\n102\n")

    assert submit.read_sectors(stars, limit=2) == [38, 1]
    assert submit.read_sectors(stars) == [38, 1, 3]
    assert submit.read_sectors(no_column) is None


def test_submit_sends_sector_counts(redis_client):
    with TestClient(create_app(redis_client)) as api:
        task_ids = submit.submit(api, ["TOI-102", "TOI-103"], [38, 1])

        assert [api.get(f"/tasks/{task_id}").json()["task_type"] for task_id in task_ids] == [MULTI_SECTOR, SHORT]


def test_bundled_star_list_has_a_sector_count_for_every_star():
    sectors = submit.read_sectors()

    assert sectors is not None and len(sectors) == len(submit.read_stars())
