from antsearch.scheduler import RoundRobinScheduler

class FakeWorker:
    def __init__(self, name):
        self.name = name
def test_worker_order():
    worker1 = FakeWorker("worker1")
    worker2 = FakeWorker("worker2")
    worker3 = FakeWorker("worker3")
    workers = [worker1.name, worker2.name, worker3.name]
    scheduler = RoundRobinScheduler()
    # The fourth turn should go back to the first worker.
    result1 = scheduler.assign(workers)
    result2 = scheduler.assign(workers)
    result3 = scheduler.assign(workers)
    result4 = scheduler.assign(workers)

    assert result1 == worker1.name
    assert result2 == worker2.name
    assert result3 == worker3.name
    assert result4 == worker1.name

def test_empty_list():
    scheduler = RoundRobinScheduler()
    result = scheduler.assign([])
    assert result is None

    scheduler.assign(["worker1", "worker2"])
    scheduler.assign([])
    result = scheduler.assign(["worker1", "worker2"])
    assert result == "worker2"


def test_one_worker():
    scheduler = RoundRobinScheduler()
    result1 = scheduler.assign(["worker1"])
    result2 = scheduler.assign(["worker1"])

    assert result1 == "worker1"
    assert result2 == "worker1"

def test_removed_worker():
    scheduler = RoundRobinScheduler()
    scheduler.assign(["worker1", "worker2", "worker3"])

    result1 = scheduler.assign(["worker2", "worker3"])
    result2 = scheduler.assign(["worker2", "worker3"])

    assert result1 == "worker2"
    assert result2 == "worker3"


def test_added_worker():
    scheduler = RoundRobinScheduler()
    scheduler.assign(["worker1"])
    result = scheduler.assign(["worker1", "worker2"])
    assert result == "worker2"


def test_repeated_worker():
    scheduler = RoundRobinScheduler()
    workers = ["worker1", "worker1", "worker2"]
    result1 = scheduler.assign(workers)
    result2 = scheduler.assign(workers)
    result3 = scheduler.assign(workers)

    assert result1 == "worker1"
    assert result2 == "worker2"
    assert result3 == "worker1"

def test_worker_result():
    scheduler = RoundRobinScheduler()
    scheduler.assign(["worker1", "worker2"])
    scheduler.on_result("worker1", {})
    result = scheduler.assign(["worker1", "worker2"])
    assert result == "worker2"
