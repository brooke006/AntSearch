import pytest

from antsearch.scheduler import RoundRobinScheduler, Scheduler


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
