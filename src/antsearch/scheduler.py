import copy
import math
import random

from antsearch.pheromone import SHORT


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


DEFAULT_ALPHA = 1.0
DEFAULT_BETA = 2.0
MIN_TASK_SECONDS = 0.001  # floor on a speed estimate, so a task that reports 0 s doesn't divide by zero


def _check_exponent(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite number >= 0, not {value!r}")
    return float(value)


class AntScheduler(Scheduler):
    """Ant System: each task goes to an available worker chosen with probability

        p(worker) = pheromone ** alpha * speed ** beta / (the same summed over available workers)

    pheromone is the worker's level for the task's type in the pheromone table (Redis), and speed
    is 1 / the worker's estimated task time, a moving average of its results like the greedy
    scheduler's. alpha weighs what the colony has learned, beta how fast the worker is now; with
    both at 0 the choice is uniform. Slower workers keep a non-zero chance, so they're still
    explored.

    A worker with no timed result yet counts as fast as the fastest measured worker, so it gets
    tried. One whose results have all failed counts as slow as the slowest. If nobody is measured
    yet, every worker's speed is 1.

    Like the random baseline, the choice is kept until that worker asks or stops being available,
    so polling more often doesn't win more tasks. The choice is made for the task at the front of
    the queue, using its type. This scheduler only reads the pheromone table; raising and
    evaporating the levels is a separate step.

    pheromones is a PheromoneTable. task_type_of maps a task ID to its task type; without it,
    every task counts as short.
    """

    def __init__(self, pheromones, task_type_of=None, alpha=DEFAULT_ALPHA, beta=DEFAULT_BETA, seed=None, smoothing=0.3):
        self.pheromones = pheromones
        self.task_type_of = task_type_of or (lambda task_id: SHORT)
        self.alpha = _check_exponent(alpha, "alpha")
        self.beta = _check_exponent(beta, "beta")
        self.random = random.Random(seed)
        self.smoothing = smoothing
        self.estimates = {}
        self.failed = set()
        self.chosen = None

    def __deepcopy__(self, memo):
        # The coordinator copies the scheduler before each choice and keeps the copy only if the
        # assignment commits. The pheromone table and task lookup talk to Redis, so share them.
        clone = copy.copy(self)
        clone.random = copy.deepcopy(self.random, memo)
        clone.estimates = dict(self.estimates)
        clone.failed = set(self.failed)
        return clone

    def assign(self, worker_id, tasks, workers):
        if len(tasks) == 0 or worker_id not in workers:
            return None

        if self.chosen not in workers:
            weights = self.probabilities(self.task_type_of(tasks[0]), workers)
            candidates = sorted(weights)
            self.chosen = self.random.choices(candidates, [weights[worker] for worker in candidates])[0]

        if worker_id != self.chosen:
            return None

        self.chosen = None
        return tasks[0]

    def probabilities(self, task_type, workers):
        """The chance of each worker in workers being chosen for a task of task_type."""
        workers = sorted(set(workers))
        if not workers:
            return {}
        levels = self.pheromones.levels(task_type, workers)
        speeds = self.speeds(workers)
        weights = {worker: levels[worker] ** self.alpha * speeds[worker] ** self.beta for worker in workers}
        total = sum(weights.values())
        if total == 0 or not math.isfinite(total):
            # Every worker has zero pheromone (or the weights overflowed): fall back to uniform.
            return {worker: 1 / len(workers) for worker in workers}
        return {worker: weight / total for worker, weight in weights.items()}

    def speeds(self, workers):
        """1 / estimated task time for each worker, with the stand-ins for unmeasured workers."""
        measured = [1 / max(seconds, MIN_TASK_SECONDS) for seconds in self.estimates.values()]
        fastest = max(measured, default=1.0)
        slowest = min(measured, default=1.0)
        speeds = {}
        for worker in workers:
            if worker in self.estimates:
                speeds[worker] = 1 / max(self.estimates[worker], MIN_TASK_SECONDS)
            else:
                speeds[worker] = slowest if worker in self.failed else fastest
        return speeds

    def on_result(self, worker_id, result):
        seconds = task_seconds(result)
        if seconds is None:
            self.failed.add(worker_id)
        elif worker_id in self.estimates:
            self.estimates[worker_id] += self.smoothing * (seconds - self.estimates[worker_id])
        else:
            self.estimates[worker_id] = seconds


SCHEDULER_NAMES = ("round-robin", "random", "greedy", "ant")


def make_scheduler(name, seed=None, pheromones=None, task_type_of=None, alpha=DEFAULT_ALPHA, beta=DEFAULT_BETA):
    """The scheduler the SCHEDULER setting names: round-robin, random, greedy or ant.

    random and ant use seed. ant also needs pheromones (a PheromoneTable) and uses task_type_of,
    alpha and beta.
    """
    if name == "round-robin":
        return RoundRobinScheduler()
    if name == "random":
        return RandomScheduler(seed)
    if name == "greedy":
        return GreedyScheduler()
    if name == "ant":
        if pheromones is None:
            raise ValueError("the ant scheduler needs a pheromone table")
        return AntScheduler(pheromones, task_type_of, alpha=alpha, beta=beta, seed=seed)
    raise ValueError(f"unknown SCHEDULER {name!r}; use {', '.join(SCHEDULER_NAMES[:-1])} or {SCHEDULER_NAMES[-1]}")
