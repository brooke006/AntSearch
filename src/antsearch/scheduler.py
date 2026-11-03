import math
import random


class Scheduler:
    def assign(self, worker_id, tasks, workers):
        raise NotImplementedError

    def on_result(self, worker_id, result):
        raise NotImplementedError


class RoundRobinScheduler(Scheduler):
    def __init__(self):
        self.last_worker = None
        self.previous_workers = []

    def assign(self, worker_id, tasks, workers):
        available_workers = []
        for worker in workers:
            if worker not in available_workers:
                available_workers.append(worker)

        if len(tasks) == 0 or len(available_workers) == 0:
            return None

        if worker_id not in available_workers:
            return None

        next_worker = available_workers[0]

        if self.last_worker in available_workers:
            position = available_workers.index(self.last_worker) + 1
            if position == len(available_workers):
                position = 0
            next_worker = available_workers[position]

        elif self.last_worker in self.previous_workers:
            # Continue after the removed worker using the previous order.
            position = self.previous_workers.index(self.last_worker) + 1
            worker_order = self.previous_workers[position:] + self.previous_workers[:position]
            for worker in worker_order:
                if worker in available_workers:
                    next_worker = worker
                    break

        if worker_id != next_worker:
            return None

        self.last_worker = worker_id
        self.previous_workers = available_workers[:]
        return tasks[0]

    def on_result(self, worker_id, result):
        pass


class RandomScheduler(Scheduler):
    """Baseline: each task goes to an available worker chosen uniformly at random.

    The choice is kept until that worker asks or stops being available, like a round-robin turn, so
    a worker that polls more often doesn't win more tasks, and a seed gives the same choices however
    the polls happen to be timed.
    """

    def __init__(self, seed=None):
        self.random = random.Random(seed)
        self.chosen = None

    def assign(self, worker_id, tasks, workers):
        if len(tasks) == 0 or worker_id not in workers:
            return None

        if self.chosen not in workers:
            self.chosen = self.random.choice(sorted(set(workers)))

        if worker_id != self.chosen:
            return None

        self.chosen = None
        return tasks[0]

    def on_result(self, worker_id, result):
        pass


def task_seconds(result):
    """How long a result says its task took, or None if it has no usable timings (a failed task, say)."""
    total = None
    for key in ("download_seconds", "compute_seconds"):
        value = result.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
            total = (total or 0) + value
    return total


class GreedyScheduler(Scheduler):
    """Baseline: each task goes to the available worker with the shortest estimated task time.

    The estimate is a moving average of the times in a worker's results; smoothing is the weight of
    the newest one. Workers with no result yet go first, so each one gets measured. Workers whose
    results have all failed go last, so a broken worker doesn't look fastest and swallow every task.
    Ties go to the lowest worker ID. Failures don't otherwise count against a worker.
    """

    def __init__(self, smoothing=0.3):
        self.smoothing = smoothing
        self.estimates = {}
        self.failed = set()

    def assign(self, worker_id, tasks, workers):
        if len(tasks) == 0 or worker_id not in workers:
            return None

        fastest = min(workers, key=lambda worker: (self._estimate(worker), worker))
        return tasks[0] if worker_id == fastest else None

    def on_result(self, worker_id, result):
        seconds = task_seconds(result)
        if seconds is None:
            self.failed.add(worker_id)
        elif worker_id in self.estimates:
            self.estimates[worker_id] += self.smoothing * (seconds - self.estimates[worker_id])
        else:
            self.estimates[worker_id] = seconds

    def _estimate(self, worker):
        if worker in self.estimates:
            return self.estimates[worker]
        return math.inf if worker in self.failed else 0.0


def make_scheduler(name, seed=None):
    """The scheduler the SCHEDULER setting names: round-robin, random or greedy. Only random uses seed."""
    if name == "round-robin":
        return RoundRobinScheduler()
    if name == "random":
        return RandomScheduler(seed)
    if name == "greedy":
        return GreedyScheduler()
    raise ValueError(f"unknown SCHEDULER {name!r}; use round-robin, random or greedy")
