import copy
import math
import random

from antsearch.pheromone import SHORT


class Scheduler:
    def assign(self, worker_id, tasks, workers):
        raise NotImplementedError

    def on_result(self, worker_id, result, task_id=None, seconds=None, disputed=False):
        """Called for each run of a task once the coordinator has settled it. task_id is the task,
        seconds how long the run took on the coordinator's clock, from assignment to result, and
        disputed whether the result failed to match the accepted answer (or no answer was agreed)."""
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

    def on_result(self, worker_id, result, task_id=None, seconds=None, disputed=False):
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

    def on_result(self, worker_id, result, task_id=None, seconds=None, disputed=False):
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

    def on_result(self, worker_id, result, task_id=None, seconds=None, disputed=False):
        reported = task_seconds(result)
        if reported is None:
            self.failed.add(worker_id)
        elif worker_id in self.estimates:
            self.estimates[worker_id] += self.smoothing * (reported - self.estimates[worker_id])
        else:
            self.estimates[worker_id] = reported

    def _estimate(self, worker):
        if worker in self.estimates:
            return self.estimates[worker]
        return math.inf if worker in self.failed else 0.0


# alpha, beta and evaporation were chosen with the parameter sweep: see docs/parameter-sweep.md.
DEFAULT_ALPHA = 1.0
DEFAULT_BETA = 4.0
DEFAULT_EVAPORATION = 0.2
DEFAULT_LATE_FACTOR = 3.0
DEFAULT_MIN_LEVEL = 0.2
MIN_TASK_SECONDS = 0.001  # floor on a speed estimate, so a task that reports 0 s doesn't divide by zero


def _check_exponent(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite number >= 0, not {value!r}")
    return float(value)


class AntScheduler(Scheduler):
    """Ant System: picks an available worker with probability proportional to
    pheromone ** alpha * speed ** beta.

    pheromone is the worker's level for the task's type and speed is 1 / its estimated task time.
    Unmeasured workers count as the fastest so they get tried. Levels stay between min_level and
    the table's initial level, as in MAX-MIN Ant System (Stuetzle and Hoos, 2000).
    task_type_of maps a task ID to its type; without it every task is short.
    """

    def __init__(
        self,
        pheromones,
        task_type_of=None,
        alpha=DEFAULT_ALPHA,
        beta=DEFAULT_BETA,
        seed=None,
        smoothing=0.3,
        evaporation=DEFAULT_EVAPORATION,
        late_factor=DEFAULT_LATE_FACTOR,
        min_level=DEFAULT_MIN_LEVEL,
    ):
        self.pheromones = pheromones
        self.task_type_of = task_type_of or (lambda task_id: SHORT)
        self.alpha = _check_exponent(alpha, "alpha")
        self.beta = _check_exponent(beta, "beta")
        if isinstance(evaporation, bool) or not isinstance(evaporation, (int, float)) or not 0 <= evaporation <= 1:
            raise ValueError(f"evaporation must be between 0 and 1, not {evaporation!r}")
        self.evaporation = float(evaporation)
        if isinstance(late_factor, bool) or not isinstance(late_factor, (int, float)) or not late_factor >= 1:
            raise ValueError(f"late_factor must be a number >= 1, not {late_factor!r}")
        self.late_factor = float(late_factor)
        self.max_level = pheromones.initial_level
        self.min_level = _check_exponent(min_level, "min_level")
        if self.min_level > self.max_level:
            raise ValueError(f"min_level {min_level!r} is above the pheromone table's initial level {self.max_level!r}")
        self.random = random.Random(seed)
        self.smoothing = smoothing
        self.estimates = {}
        self.failed = set()
        self.typical = {}  # task type -> moving average of how long its tasks take, on the coordinator's clock
        self.chosen = None

    def __deepcopy__(self, memo):
        # The coordinator copies the scheduler before each choice and keeps the copy only if the
        # assignment commits. The pheromone table and task lookup talk to Redis, so share them.
        clone = copy.copy(self)
        clone.random = copy.deepcopy(self.random, memo)
        clone.estimates = dict(self.estimates)
        clone.failed = set(self.failed)
        clone.typical = dict(self.typical)
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

    def on_result(self, worker_id, result, task_id=None, seconds=None, disputed=False):
        """Update the speed estimate, then evaporate all levels and deposit for this worker.

        The deposit is evaporation * max_level * typical / seconds, where typical is the recent
        average time for the task's type. Failed, late and disputed results get nothing.
        Returns "deposited", "failed", "late" or "disputed", or None without task_id and seconds.
        """
        reported = task_seconds(result)
        if reported is None:
            self.failed.add(worker_id)
        elif worker_id in self.estimates:
            self.estimates[worker_id] += self.smoothing * (reported - self.estimates[worker_id])
        else:
            self.estimates[worker_id] = reported

        if task_id is None or seconds is None:
            return None
        kind = self.task_type_of(task_id)
        seconds = max(seconds, MIN_TASK_SECONDS)
        typical = self.typical.get(kind)

        # Store the worker's pair first: one that has never been written reads as the ceiling and
        # evaporation skips it, so a worker whose every result failed would otherwise stay on top.
        self.pheromones.ensure(worker_id, kind)
        self.pheromones.evaporate(self.evaporation, self.min_level)
        if "error" in result:
            return "failed"
        if disputed:
            outcome = "disputed"
        elif typical is not None and seconds > self.late_factor * typical:
            outcome = "late"
        else:
            amount = self.evaporation * self.max_level * (typical or seconds) / seconds
            self.pheromones.add(worker_id, kind, amount, maximum=self.max_level)
            outcome = "deposited"
        # Late results still count towards the typical time, so if every worker slows down
        # together the yardstick follows instead of calling everything late for good.
        self.typical[kind] = seconds if typical is None else typical + self.smoothing * (seconds - typical)
        return outcome


SCHEDULER_NAMES = ("round-robin", "random", "greedy", "ant")


def make_scheduler(
    name,
    seed=None,
    pheromones=None,
    task_type_of=None,
    alpha=DEFAULT_ALPHA,
    beta=DEFAULT_BETA,
    evaporation=DEFAULT_EVAPORATION,
    late_factor=DEFAULT_LATE_FACTOR,
):
    """The scheduler the SCHEDULER setting names: round-robin, random, greedy or ant.

    random and ant use seed. ant also needs pheromones (a PheromoneTable) and uses task_type_of,
    alpha, beta, evaporation and late_factor.
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
        return AntScheduler(
            pheromones, task_type_of, alpha=alpha, beta=beta, seed=seed, evaporation=evaporation, late_factor=late_factor
        )
    raise ValueError(f"unknown SCHEDULER {name!r}; use {', '.join(SCHEDULER_NAMES[:-1])} or {SCHEDULER_NAMES[-1]}")
