import copy
import itertools
import json

import fakeredis
import pytest
from fastapi.testclient import TestClient

from antsearch import coordinator
from antsearch.coordinator import create_app, task_type_lookup
from antsearch.pheromone import MULTI_SECTOR, SHORT, PheromoneTable
from antsearch.runlog import RunLog, read_events
from antsearch.scheduler import DEFAULT_EVAPORATION, DEFAULT_LATE_FACTOR, DEFAULT_MIN_LEVEL, AntScheduler


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def table(redis_client):
    return PheromoneTable(redis_client)


def ok(seconds=1.0):
    return {"compute_seconds": seconds}


# --- PheromoneTable.evaporate and capped add ---------------------------------------------------


def test_evaporate_scales_every_stored_level(table):
    table.set("w1", SHORT, 1.0)
    table.set("w2", SHORT, 0.5)
    table.set("w1", MULTI_SECTOR, 0.8)

    table.evaporate(0.25)

    assert table.table() == {"w1": {SHORT: 0.75, MULTI_SECTOR: pytest.approx(0.6)}, "w2": {SHORT: 0.375}}


def test_evaporate_keeps_levels_at_or_above_the_minimum(table):
    table.set("w1", SHORT, 1.0)
    table.set("w2", SHORT, 0.3)

    table.evaporate(0.5, minimum=0.2)

    assert table.table() == {"w1": {SHORT: 0.5}, "w2": {SHORT: 0.2}}
    table.evaporate(1.0, minimum=0.2)
    assert table.table() == {"w1": {SHORT: 0.2}, "w2": {SHORT: 0.2}}


def test_evaporate_leaves_unwritten_pairs_at_the_initial_level(table, redis_client):
    table.evaporate(0.5)

    assert table.get("w1", SHORT) == 1.0
    assert redis_client.keys("antsearch:pheromone:*") == []


def test_ensure_stores_a_missing_pair_at_the_initial_level_only(table):
    table.set("w1", SHORT, 0.3)
    table.ensure("w1", SHORT)
    table.ensure("w2", SHORT)

    assert table.table() == {"w1": {SHORT: 0.3}, "w2": {SHORT: 1.0}}


def test_a_worker_whose_first_result_fails_is_punished(table):
    scheduler = scheduler_with_types(table, evaporation=0.2)

    assert scheduler.on_result("new", {"error": "boom"}, task_id="s1", seconds=1.0) == "failed"
    assert table.get("new", SHORT) == pytest.approx(0.8)


def test_evaporate_at_rate_zero_changes_nothing(table):
    table.set("w1", SHORT, 0.7)
    table.evaporate(0)
    assert table.get("w1", SHORT) == 0.7


@pytest.mark.parametrize("rate", [-0.1, 1.5, None, "0.1", True])
def test_evaporate_rejects_bad_rates(table, rate):
    with pytest.raises(ValueError, match="rate"):
        table.evaporate(rate)


def test_capped_add_stops_at_the_maximum(table):
    table.set("w1", SHORT, 0.5)

    assert table.add("w1", SHORT, 0.3, maximum=1.0) == pytest.approx(0.8)
    assert table.add("w1", SHORT, 0.3, maximum=1.0) == 1.0
    assert table.add("w2", SHORT, 0.3, maximum=1.0) == 1.0  # unwritten pairs start at the initial level, 1
    assert table.get("w1", SHORT) == 1.0


# --- One round: evaporate, then deposit or not --------------------------------------------------


def scheduler_with_types(table, **kwargs):
    types = {"s1": SHORT, "s2": SHORT, "s3": SHORT, "m1": MULTI_SECTOR, "m2": MULTI_SECTOR}
    return AntScheduler(table, types.__getitem__, **kwargs)


def test_defaults():
    scheduler = AntScheduler(PheromoneTable(fakeredis.FakeRedis(decode_responses=True)))
    assert (scheduler.evaporation, scheduler.late_factor, scheduler.min_level, scheduler.max_level) == (
        DEFAULT_EVAPORATION, DEFAULT_LATE_FACTOR, DEFAULT_MIN_LEVEL, 1.0,
    )


def test_a_round_evaporates_every_level_then_deposits_for_the_tasks_type(table):
    table.set("w1", SHORT, 0.5)
    table.set("w2", SHORT, 0.5)
    table.set("w2", MULTI_SECTOR, 0.5)
    scheduler = scheduler_with_types(table, evaporation=0.2)

    assert scheduler.on_result("w1", ok(10), task_id="s1", seconds=10.0) == "deposited"

    # Everything evaporated by 20%; w1 then earned 0.2 * typical / seconds, with the first task's
    # own time as the typical one: 0.4 + 0.2.
    assert table.table() == {"w1": {SHORT: pytest.approx(0.6)}, "w2": {SHORT: 0.4, MULTI_SECTOR: 0.4}}


def test_the_deposit_grows_with_relative_speed(table):
    scheduler = scheduler_with_types(table, evaporation=0.2)
    table.set("w1", SHORT, 0.4)
    table.set("w2", SHORT, 0.4)
    scheduler.on_result("w0", ok(), task_id="s1", seconds=10.0)  # sets the typical short task at 10 s

    scheduler.on_result("w1", ok(), task_id="s2", seconds=5.0)   # twice as fast: earns 0.2 * 10 / 5
    after_fast = table.get("w1", SHORT)
    scheduler.on_result("w2", ok(), task_id="s3", seconds=20.0)  # typical is now 8.5 s

    assert after_fast == pytest.approx(0.4 * 0.8 * 0.8 + 0.4)
    assert table.get("w2", SHORT) == pytest.approx(0.4 * 0.8 ** 3 + 0.2 * 8.5 / 20)


def test_the_typical_time_is_kept_per_task_type(table):
    scheduler = scheduler_with_types(table, evaporation=0.2)
    scheduler.on_result("w1", ok(), task_id="s1", seconds=10.0)
    scheduler.on_result("w1", ok(), task_id="m1", seconds=100.0)

    assert scheduler.typical == {SHORT: 10.0, MULTI_SECTOR: 100.0}
    # 100 s is the usual time for a multi-sector task, so it isn't late and earns a full deposit.
    assert scheduler.on_result("w2", ok(), task_id="m2", seconds=100.0) == "deposited"
    assert table.get("w2", MULTI_SECTOR) == 1.0


@pytest.mark.parametrize(
    "result, kwargs, outcome",
    [
        ({"error": "NoDataError", "message": "none"}, {"seconds": 10.0}, "failed"),
        (ok(), {"seconds": 31.0}, "late"),  # more than 3 * the typical 10 s
        (ok(), {"seconds": 10.0, "disputed": True}, "disputed"),
    ],
)
def test_failed_late_and_disputed_results_add_nothing(table, result, kwargs, outcome):
    scheduler = scheduler_with_types(table, evaporation=0.2)
    scheduler.on_result("w0", ok(), task_id="s1", seconds=10.0)
    table.set("w1", SHORT, 0.5)

    assert scheduler.on_result("w1", result, task_id="s2", **kwargs) == outcome
    assert table.get("w1", SHORT) == pytest.approx(0.4)  # evaporated, and nothing added


def test_a_result_just_inside_the_deadline_still_counts(table):
    scheduler = scheduler_with_types(table, evaporation=0.2)
    scheduler.on_result("w0", ok(), task_id="s1", seconds=10.0)

    assert scheduler.on_result("w1", ok(), task_id="s2", seconds=30.0) == "deposited"


def test_late_factor_sets_the_deadline(table):
    scheduler = scheduler_with_types(table, late_factor=1.5)
    scheduler.on_result("w0", ok(), task_id="s1", seconds=10.0)

    assert scheduler.on_result("w1", ok(), task_id="s2", seconds=16.0) == "late"


def test_late_results_move_the_typical_time_but_failed_ones_do_not(table):
    scheduler = scheduler_with_types(table, smoothing=0.5)
    scheduler.on_result("w0", ok(), task_id="s1", seconds=10.0)
    scheduler.on_result("w1", {"error": "boom"}, task_id="s2", seconds=1.0)
    assert scheduler.typical[SHORT] == 10.0

    scheduler.on_result("w1", ok(), task_id="s3", seconds=50.0)
    assert scheduler.typical[SHORT] == 30.0


def test_a_worker_that_keeps_failing_sinks_to_the_floor_but_not_below(table):
    scheduler = scheduler_with_types(table, evaporation=0.2, min_level=0.2)
    for _ in range(30):
        scheduler.on_result("broken", {"error": "boom"}, task_id="s1", seconds=1.0)
        scheduler.on_result("good", ok(), task_id="s2", seconds=1.0)

    # good takes every other round, so it settles where 0.8 * (0.8 * level) + 0.2 = level: 5/9.
    assert table.get("broken", SHORT) == 0.2
    assert table.get("good", SHORT) == pytest.approx(5 / 9, abs=1e-3)
    assert 0 < scheduler.probabilities(SHORT, ["broken", "good"])["broken"] < 0.3  # punished, still explored


def test_levels_never_go_above_the_initial_level(table):
    scheduler = scheduler_with_types(table, evaporation=0.5)
    scheduler.on_result("w0", ok(), task_id="s1", seconds=10.0)
    for _ in range(5):
        scheduler.on_result("w1", ok(), task_id="s2", seconds=1.0)  # ten times faster than usual

    assert table.get("w1", SHORT) == 1.0


def test_evaporation_is_configurable(table):
    for rate, expected in [(0.0, 0.5), (0.1, 0.45), (0.5, 0.25)]:
        table.set("idle", SHORT, 0.5)
        scheduler_with_types(table, evaporation=rate, min_level=0.0).on_result("w1", ok(), task_id="s1", seconds=1.0)
        assert table.get("idle", SHORT) == pytest.approx(expected)


def test_with_no_evaporation_levels_never_change(table):
    scheduler = scheduler_with_types(table, evaporation=0)
    for task_id, seconds in [("s1", 1.0), ("s2", 9.0), ("m1", 4.0)]:
        scheduler.on_result("w1", ok(), task_id=task_id, seconds=seconds)
    scheduler.on_result("w1", {"error": "boom"}, task_id="s3", seconds=1.0)

    assert table.table() == {"w1": {SHORT: 1.0, MULTI_SECTOR: 1.0}}


def test_without_the_tasks_timing_there_is_no_round(table, redis_client):
    scheduler = AntScheduler(table)
    table.set("w1", SHORT, 0.5)

    assert scheduler.on_result("w1", ok(), task_id="s1") is None
    assert scheduler.on_result("w1", ok(), seconds=1.0) is None
    assert scheduler.on_result("w1", ok()) is None
    assert table.table() == {"w1": {SHORT: 0.5}}
    assert scheduler.estimates == {"w1": 1.0}  # the speed estimate still learns from the result


def test_a_zero_second_task_does_not_divide_by_zero(table):
    scheduler = scheduler_with_types(table)
    assert scheduler.on_result("w1", ok(), task_id="s1", seconds=0.0) == "deposited"
    assert scheduler.on_result("w1", ok(), task_id="s2", seconds=-1.0) == "deposited"


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"evaporation": -0.1}, "evaporation"),
        ({"evaporation": 1.1}, "evaporation"),
        ({"evaporation": True}, "evaporation"),
        ({"late_factor": 0.5}, "late_factor"),
        ({"late_factor": "3"}, "late_factor"),
        ({"min_level": -1}, "min_level"),
        ({"min_level": 2.0}, "above the pheromone table's initial level"),
    ],
)
def test_bad_settings_are_rejected(table, kwargs, match):
    with pytest.raises(ValueError, match=match):
        AntScheduler(table, **kwargs)


def test_copy_keeps_its_own_typical_times(table):
    scheduler = scheduler_with_types(table)
    scheduler.on_result("w1", ok(), task_id="s1", seconds=10.0)
    clone = copy.deepcopy(scheduler)
    clone.on_result("w1", ok(), task_id="s2", seconds=20.0)

    assert scheduler.typical == {SHORT: 10.0}


# --- A worker that slows down loses share ------------------------------------------------------


def play(scheduler, speeds, rounds, first_task=0):
    """Hand out tasks one at a time, with every worker free for every choice, and report each result
    with its time as the task's time on the coordinator's clock. Returns w1's chance of the next
    task after each round."""
    workers = sorted(speeds)
    chances = []
    for n in range(first_task, first_task + rounds):
        task = f"t{n}"
        for worker in itertools.cycle(workers):
            if scheduler.assign(worker, [task], workers) == task:
                break
        scheduler.on_result(worker, ok(speeds[worker]), task_id=task, seconds=speeds[worker])
        chances.append(scheduler.probabilities(SHORT, workers)["w1"])
    return chances


@pytest.mark.parametrize("seed", range(10))
def test_a_worker_that_slows_down_loses_share_within_a_few_rounds(table, seed):
    scheduler = AntScheduler(table, seed=seed)  # default alpha, beta and evaporation
    play(scheduler, {"w1": 1.0, "w2": 1.0}, 100)

    chances = play(scheduler, {"w1": 5.0, "w2": 1.0}, 5, first_task=100)

    assert chances[-1] < 0.25


def test_pheromone_alone_makes_a_slowed_worker_lose_share_and_evaporation_is_what_lets_it(redis_client):
    # beta = 0 switches the speed estimate off, so only the pheromone levels decide. The workers take
    # turns, so both always have fresh results; w1 slows down five-fold after 20 rounds.
    def chances(evaporation):
        table = PheromoneTable(redis_client)
        table.clear()
        scheduler = AntScheduler(table, beta=0, evaporation=evaporation)
        history = []
        for n in range(40):
            worker = ("w1", "w2")[n % 2]
            seconds = 5.0 if worker == "w1" and n >= 20 else 1.0
            scheduler.on_result(worker, ok(seconds), task_id=f"t{n}", seconds=seconds)
            history.append(scheduler.probabilities(SHORT, ["w1", "w2"])["w1"])
        return history

    evaporating, frozen = chances(0.2), chances(0.0)

    assert evaporating[19] == pytest.approx(0.5, abs=0.06)  # equal before the slowdown
    assert evaporating[29] < 0.3  # within ten rounds (five of w1's own tasks)
    assert evaporating[39] < 0.3  # and it stays down
    assert frozen[39] == 0.5  # no evaporation, no deposits: the stale reputation never fades


def test_a_fresh_good_run_outweighs_an_old_reputation(table):
    # w1 was fast for a long time, then turns slow; w2 joins later but is fast. Evaporation lets
    # w2's recent results count for more than w1's old ones.
    scheduler = AntScheduler(table, beta=0)
    for n in range(50):
        scheduler.on_result("w1", ok(), task_id=f"a{n}", seconds=1.0)
    for n in range(10):
        scheduler.on_result("w1", ok(5), task_id=f"b{n}", seconds=5.0)
        scheduler.on_result("w2", ok(), task_id=f"c{n}", seconds=1.0)

    assert scheduler.probabilities(SHORT, ["w1", "w2"])["w1"] < 0.3


# --- Through the coordinator -------------------------------------------------------------------


def test_coordinator_times_tasks_on_its_own_clock_and_logs_the_outcome(redis_client, tmp_path):
    now = [1000.0]
    table = PheromoneTable(redis_client)
    scheduler = AntScheduler(table, task_type_lookup(redis_client), seed=1)
    run_log = RunLog(tmp_path / "run.jsonl", clock=lambda: now[0])
    app = create_app(redis_client, heartbeat_timeout=1000, clock=lambda: now[0], scheduler=scheduler, run_log=run_log)
    with TestClient(app) as client:
        client.post("/workers/register", json={"worker_id": "w1"})
        client.post("/tasks", json={"star_ids": ["a", "b", "c"], "num_sectors": [1, 1, 1]})

        for task_id, seconds, result in [("t1", 10.0, ok(0.1)), ("t2", 40.0, ok(0.1)), ("t3", 1.0, {"error": "boom"})]:
            assert client.post("/tasks/request", json={"worker_id": "w1"}).json()["task_id"] == task_id
            now[0] += seconds
            client.post(f"/tasks/{task_id}/result", json={"worker_id": "w1", "result": result})

    # The worker claimed 0.1 s each time, but the coordinator timed 10 s and then 40 s: late.
    done = [event for event in read_events(tmp_path / "run.jsonl") if event["event"] == "task_done"]
    assert [event["pheromone"] for event in done] == ["deposited", "late", "failed"]
    # Deposited at the ceiling, then two rounds of evaporation with nothing added.
    assert table.get("w1", SHORT) == pytest.approx(1.0 * 0.8 * 0.8)


def test_other_schedulers_log_no_pheromone_outcome(redis_client, tmp_path):
    from antsearch.scheduler import GreedyScheduler

    run_log = RunLog(tmp_path / "run.jsonl")
    with TestClient(create_app(redis_client, scheduler=GreedyScheduler(), run_log=run_log)) as client:
        client.post("/workers/register", json={"worker_id": "w1"})
        client.post("/tasks", json={"star_ids": ["a"]})
        client.post("/tasks/request", json={"worker_id": "w1"})
        client.post("/tasks/t1/result", json={"worker_id": "w1", "result": ok()})

    done = [event for event in read_events(tmp_path / "run.jsonl") if event["event"] == "task_done"]
    assert "pheromone" not in done[0]


def test_levels_learned_before_a_restart_carry_on_after_it():
    server = fakeredis.FakeServer()
    first = fakeredis.FakeRedis(server=server, decode_responses=True)
    scheduler = AntScheduler(PheromoneTable(first), task_type_lookup(first))
    scheduler.on_result("w1", {"error": "boom"}, task_id="t1", seconds=1.0)
    scheduler.on_result("w2", ok(), task_id="t2", seconds=1.0)

    second = fakeredis.FakeRedis(server=server, decode_responses=True)
    restarted = AntScheduler(PheromoneTable(second), task_type_lookup(second))

    assert restarted.probabilities(SHORT, ["w1", "w2"]) == scheduler.probabilities(SHORT, ["w1", "w2"])
    assert PheromoneTable(second).table() == {"w1": {SHORT: pytest.approx(0.64)}, "w2": {SHORT: 1.0}}


def test_main_reads_the_evaporation_and_late_factor_settings(monkeypatch, tmp_path):
    database = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SCHEDULER", "ant")
    monkeypatch.setenv("SCHEDULER_EVAPORATION", "0.35")
    monkeypatch.setenv("SCHEDULER_LATE_FACTOR", "4")
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: database)
    monkeypatch.setattr(coordinator.uvicorn, "run", lambda app, **kwargs: None)
    used = []
    monkeypatch.setattr(coordinator, "create_app", lambda client, **kwargs: used.append(kwargs["scheduler"]))

    coordinator.main()

    assert (used[0].evaporation, used[0].late_factor) == (0.35, 4.0)
    started = json.loads(next(tmp_path.glob("*.jsonl")).read_text().splitlines()[0])
    assert (started["evaporation"], started["late_factor"]) == (0.35, 4.0)


def test_main_defaults_the_evaporation_and_late_factor(monkeypatch, tmp_path):
    monkeypatch.setenv("RUN_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SCHEDULER", "ant")
    monkeypatch.setattr(coordinator.redis.Redis, "from_url", lambda url, **kwargs: fakeredis.FakeRedis(decode_responses=True))
    monkeypatch.setattr(coordinator.uvicorn, "run", lambda app, **kwargs: None)
    used = []
    monkeypatch.setattr(coordinator, "create_app", lambda client, **kwargs: used.append(kwargs["scheduler"]))

    coordinator.main()

    assert (used[0].evaporation, used[0].late_factor) == (DEFAULT_EVAPORATION, DEFAULT_LATE_FACTOR)
