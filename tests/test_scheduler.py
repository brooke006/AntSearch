import copy
import itertools
import math
from collections import Counter

import pytest

from antsearch.scheduler import (
    GreedyScheduler,
    RandomScheduler,
    RoundRobinScheduler,
    Scheduler,
    make_scheduler,
    task_seconds,
)


class FakeWorker:
    def __init__(self, name):
        self.name = name


def test_worker_order():
    worker1 = FakeWorker("worker1")
    worker2 = FakeWorker("worker2")
    worker3 = FakeWorker("worker3")
    workers = [worker1.name, worker2.name, worker3.name]
    scheduler = RoundRobinScheduler()

    assert scheduler.assign(worker1.name, ["t1"], workers) == "t1"
    assert scheduler.assign(worker2.name, ["t2"], workers) == "t2"
    assert scheduler.assign(worker3.name, ["t3"], workers) == "t3"
    assert scheduler.assign(worker1.name, ["t4"], workers) == "t4"


def test_empty_list():
    scheduler = RoundRobinScheduler()

    assert scheduler.assign("worker1", ["t1"], []) is None
    scheduler.assign("worker1", ["t1"], ["worker1", "worker2"])
    scheduler.assign("worker2", ["t2"], [])
    assert scheduler.assign("worker2", ["t2"], ["worker1", "worker2"]) == "t2"


def test_empty_tasks():
    scheduler = RoundRobinScheduler()
    workers = ["worker1", "worker2"]

    assert scheduler.assign("worker1", [], workers) is None
    assert scheduler.assign("worker1", ["t1"], workers) == "t1"


def test_one_worker():
    scheduler = RoundRobinScheduler()

    assert scheduler.assign("worker1", ["t1"], ["worker1"]) == "t1"
    assert scheduler.assign("worker1", ["t2"], ["worker1"]) == "t2"


def test_removed_worker():
    scheduler = RoundRobinScheduler()
    scheduler.assign("worker1", ["t1"], ["worker1", "worker2", "worker3"])

    assert scheduler.assign("worker2", ["t2"], ["worker2", "worker3"]) == "t2"
    assert scheduler.assign("worker3", ["t3"], ["worker2", "worker3"]) == "t3"


def test_removed_middle_worker():
    scheduler = RoundRobinScheduler()
    workers = ["worker1", "worker2", "worker3"]
    scheduler.assign("worker1", ["t1"], workers)
    scheduler.assign("worker2", ["t2"], workers)

    assert scheduler.assign("worker3", ["t3"], ["worker1", "worker3"]) == "t3"


def test_waiting_request_does_not_skip_next_worker():
    scheduler = RoundRobinScheduler()
    workers = ["worker1", "worker2", "worker3"]
    scheduler.assign("worker1", ["t1"], workers)
    scheduler.assign("worker2", ["t2"], workers)

    assert scheduler.assign("worker1", ["t3"], ["worker1", "worker3"]) is None
    assert scheduler.assign("worker3", ["t3"], ["worker1", "worker3"]) == "t3"


def test_added_worker():
    scheduler = RoundRobinScheduler()
    scheduler.assign("worker1", ["t1"], ["worker1"])

    assert scheduler.assign("worker2", ["t2", "t3"], ["worker1", "worker2"]) == "t2"


def test_repeated_worker():
    scheduler = RoundRobinScheduler()
    workers = ["worker1", "worker1", "worker2"]

    assert scheduler.assign("worker1", ["t1"], workers) == "t1"
    assert scheduler.assign("worker2", ["t2"], workers) == "t2"
    assert scheduler.assign("worker1", ["t3"], workers) == "t3"


def test_worker_result():
    scheduler = RoundRobinScheduler()
    workers = ["worker1", "worker2"]
    scheduler.assign("worker1", ["t1"], workers)
    scheduler.on_result("worker1", {})

    assert scheduler.assign("worker2", ["t2"], workers) == "t2"


def test_unknown_worker():
    scheduler = RoundRobinScheduler()

    assert scheduler.assign("unknown", ["t1"], ["worker1"]) is None
    assert scheduler.assign("worker1", ["t1"], ["worker1"]) == "t1"


def test_inputs_are_not_changed():
    scheduler = RoundRobinScheduler()
    workers = ["worker1", "worker2"]
    tasks = ["t1", "t2"]
    scheduler.assign("worker1", tasks, workers)

    assert workers == ["worker1", "worker2"]
    assert tasks == ["t1", "t2"]


def test_all_workers_replaced():
    scheduler = RoundRobinScheduler()
    scheduler.assign("worker1", ["t1"], ["worker1", "worker2"])

    assert scheduler.assign("worker3", ["t2"], ["worker3"]) == "t2"


def test_scheduler_interface():
    scheduler = Scheduler()

    with pytest.raises(NotImplementedError):
        scheduler.assign("worker1", ["t1"], ["worker1"])
    with pytest.raises(NotImplementedError):
        scheduler.on_result("worker1", {})


@pytest.mark.parametrize("make", [RandomScheduler, GreedyScheduler], ids=["random", "greedy"])
class TestBaselines:
    def test_implements_the_scheduler_interface(self, make):
        assert isinstance(make(), Scheduler)

    def test_nothing_to_assign(self, make):
        assert make().assign("worker1", [], ["worker1"]) is None
        assert make().assign("worker1", ["t1"], []) is None

    def test_unavailable_worker_gets_nothing(self, make):
        assert make().assign("worker2", ["t1"], ["worker1"]) is None

    def test_only_available_worker_gets_the_next_task_every_time(self, make):
        scheduler = make()

        assert scheduler.assign("worker1", ["t1", "t2"], ["worker1"]) == "t1"
        assert scheduler.assign("worker1", ["t2"], ["worker1"]) == "t2"

    def test_inputs_are_not_changed(self, make):
        scheduler = make()
        workers = ["worker1", "worker2"]
        tasks = ["t1", "t2"]
        for worker in workers:
            scheduler.assign(worker, tasks, workers)
        scheduler.on_result("worker1", {"compute_seconds": 1})

        assert workers == ["worker1", "worker2"]
        assert tasks == ["t1", "t2"]

    def test_state_survives_the_copy_the_coordinator_makes(self, make):
        scheduler = copy.deepcopy(make())
        scheduler.on_result("worker1", {"compute_seconds": 1})

        assert scheduler.assign("worker1", ["t1"], ["worker1"]) == "t1"


def choices(scheduler, workers, count):
    """Which worker gets each of count tasks, with every worker asking in turn until one is given it."""
    chosen = []
    for task in range(count):
        for worker in itertools.cycle(workers):
            if scheduler.assign(worker, [f"t{task}"], workers) is not None:
                chosen.append(worker)
                break
    return chosen


WORKERS = ["worker1", "worker2", "worker3"]


def test_random_same_seed_gives_the_same_choices():
    assert choices(RandomScheduler(seed=7), WORKERS, 50) == choices(RandomScheduler(seed=7), WORKERS, 50)


def test_random_different_seeds_give_different_choices():
    assert choices(RandomScheduler(seed=7), WORKERS, 50) != choices(RandomScheduler(seed=8), WORKERS, 50)


def test_random_shares_tasks_evenly():
    counts = Counter(choices(RandomScheduler(seed=1), WORKERS, 3000))

    assert set(counts) == set(WORKERS)
    assert all(abs(count - 1000) < 100 for count in counts.values())


def test_random_choices_do_not_depend_on_who_polls_or_how_often():
    expected = choices(RandomScheduler(seed=3), WORKERS, 20)
    scheduler = RandomScheduler(seed=3)
    for _ in range(20):
        scheduler.assign("worker3", [], WORKERS)  # polling an empty queue doesn't use up a choice

    got = []
    for task in range(20):
        for worker in itertools.cycle(["worker3", "worker3", "worker3", "worker1", "worker1", "worker2"]):
            if scheduler.assign(worker, [f"t{task}"], WORKERS) is not None:
                got.append(worker)
                break

    assert got == expected


def holding_a_choice(seed=1):
    """A RandomScheduler that has chosen a worker other than worker1 and is waiting for it to ask."""
    scheduler = RandomScheduler(seed)
    while scheduler.assign("worker1", ["t0"], WORKERS) is not None:
        pass
    return scheduler


def test_random_keeps_its_choice_until_that_worker_asks():
    scheduler = holding_a_choice()
    chosen = scheduler.chosen
    others = [worker for worker in WORKERS if worker != chosen]

    for worker in others * 10:
        assert scheduler.assign(worker, ["t1"], WORKERS) is None
    assert scheduler.assign(chosen, ["t1"], WORKERS) == "t1"


def test_random_chooses_again_when_the_chosen_worker_stops_being_available():
    scheduler = holding_a_choice()
    chosen = scheduler.chosen
    remaining = [worker for worker in WORKERS if worker != chosen]

    answers = {worker: scheduler.assign(worker, ["t1"], remaining) for worker in remaining}

    assert "t1" in answers.values()  # not left waiting for the worker that went away


def test_random_seed_gives_the_same_choices_whatever_the_worker_order():
    assert choices(RandomScheduler(seed=5), WORKERS, 20) == choices(RandomScheduler(seed=5), WORKERS[::-1], 20)


def timed(seconds):
    return {"compute_seconds": seconds}


def test_greedy_measures_workers_with_no_result_first():
    scheduler = GreedyScheduler()
    scheduler.on_result("worker1", timed(1))

    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker2"]) is None
    assert scheduler.assign("worker2", ["t1"], ["worker1", "worker2"]) == "t1"


def test_greedy_chooses_the_fastest_available_worker():
    scheduler = GreedyScheduler()
    for worker, seconds in [("worker1", 5), ("worker2", 1), ("worker3", 3)]:
        scheduler.on_result(worker, timed(seconds))

    assert scheduler.assign("worker1", ["t1"], WORKERS) is None
    assert scheduler.assign("worker3", ["t1"], WORKERS) is None
    assert scheduler.assign("worker2", ["t1"], WORKERS) == "t1"


def test_greedy_uses_a_slower_worker_while_the_fastest_is_busy():
    scheduler = GreedyScheduler()
    for worker, seconds in [("worker1", 5), ("worker2", 1), ("worker3", 3)]:
        scheduler.on_result(worker, timed(seconds))

    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker3"]) is None
    assert scheduler.assign("worker3", ["t1"], ["worker1", "worker3"]) == "t1"
    assert scheduler.assign("worker1", ["t1"], ["worker1"]) == "t1"


def test_greedy_counts_download_and_compute_time():
    scheduler = GreedyScheduler()
    scheduler.on_result("worker1", {"download_seconds": 50, "compute_seconds": 5})
    scheduler.on_result("worker2", {"download_seconds": 0.1, "compute_seconds": 10})

    assert scheduler.assign("worker2", ["t1"], ["worker1", "worker2"]) == "t1"


def test_greedy_moving_average_follows_a_worker_that_slows_down():
    scheduler = GreedyScheduler(smoothing=0.5)
    scheduler.on_result("worker1", timed(1))
    scheduler.on_result("worker2", timed(2))
    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker2"]) == "t1"

    scheduler.on_result("worker1", timed(2.5))  # averages to 1.75: one slow task doesn't change the ranking
    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker2"]) == "t1"

    scheduler.on_result("worker1", timed(10))  # averages to 5.875: now it does
    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker2"]) is None
    assert scheduler.assign("worker2", ["t1"], ["worker1", "worker2"]) == "t1"


def test_greedy_breaks_ties_by_worker_id_whatever_the_order():
    scheduler = GreedyScheduler()
    for worker in WORKERS:
        scheduler.on_result(worker, timed(1))

    assert scheduler.assign("worker1", ["t1"], WORKERS[::-1]) == "t1"
    assert scheduler.assign("worker2", ["t1"], WORKERS[::-1]) is None


def test_greedy_puts_a_worker_that_has_only_failed_last():
    scheduler = GreedyScheduler()
    scheduler.on_result("worker1", {"error": "DownloadError", "message": "MAST down"})
    scheduler.on_result("worker2", timed(100))

    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker2"]) is None
    assert scheduler.assign("worker2", ["t1"], ["worker1", "worker2"]) == "t1"
    assert scheduler.assign("worker1", ["t1"], ["worker1"]) == "t1"  # still used if nobody else is free


def test_greedy_a_failure_does_not_change_a_measured_workers_estimate():
    scheduler = GreedyScheduler()
    scheduler.on_result("worker1", timed(1))
    scheduler.on_result("worker2", timed(2))
    scheduler.on_result("worker1", {"error": "NoDataError", "message": "not on MAST"})

    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker2"]) == "t1"


def test_greedy_a_success_after_failures_gives_a_worker_an_estimate():
    scheduler = GreedyScheduler()
    scheduler.on_result("worker1", {"error": "NoDataError", "message": "not on MAST"})
    scheduler.on_result("worker1", timed(1))
    scheduler.on_result("worker2", timed(2))

    assert scheduler.assign("worker1", ["t1"], ["worker1", "worker2"]) == "t1"


@pytest.mark.parametrize(
    "result",
    [
        {},
        {"compute_seconds": "fast"},
        {"compute_seconds": True},
        {"compute_seconds": None},
        {"compute_seconds": -1},
        {"compute_seconds": math.nan},
        {"compute_seconds": math.inf},
    ],
)
def test_task_seconds_ignores_unusable_timings(result):
    assert task_seconds(result) is None


def test_task_seconds_adds_download_and_compute():
    assert task_seconds({"download_seconds": 2, "compute_seconds": 0.5}) == 2.5
    assert task_seconds({"compute_seconds": 0}) == 0


def test_make_scheduler():
    assert isinstance(make_scheduler("round-robin"), RoundRobinScheduler)
    assert isinstance(make_scheduler("greedy"), GreedyScheduler)
    assert choices(make_scheduler("random", seed=2), WORKERS, 20) == choices(RandomScheduler(seed=2), WORKERS, 20)
    with pytest.raises(ValueError, match="unknown SCHEDULER 'ant'"):
        make_scheduler("ant")
