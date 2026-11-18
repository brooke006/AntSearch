"""Pheromone table: how well each worker handles each type of task, kept in Redis.

The ant colony scheduler remembers who handles what well by keeping a pheromone level for every
(worker, task type) pair. Task types split the stars by how much light curve they have:

    short           one TESS sector or none (about 27 days of data at most)
    multi_sector    two or more sectors

Redis keys, one hash per task type so the scheduler can read every worker's level for a task's
type in one call:

    antsearch:pheromone:<task type>     hash: worker ID -> pheromone level (float)

Levels live in Redis rather than in the coordinator's memory, so a coordinator that restarts
picks up where it left off. A pair with no entry yet has the initial level. This module stores
levels and can evaporate them all at once; when to evaporate and how much to deposit is up to the
scheduler (see AntScheduler.on_result).
"""

import math
from collections.abc import Iterable

import redis

SHORT = "short"
MULTI_SECTOR = "multi_sector"
TASK_TYPES = (SHORT, MULTI_SECTOR)
DEFAULT_INITIAL_LEVEL = 1.0


def pheromone_key(task_type: str) -> str:
    return f"antsearch:pheromone:{task_type}"


def task_type(num_sectors: int) -> str:
    """The task type of a star observed in num_sectors TESS sectors."""
    if isinstance(num_sectors, bool) or not isinstance(num_sectors, int) or num_sectors < 0:
        raise ValueError(f"num_sectors must be a non-negative integer, not {num_sectors!r}")
    return SHORT if num_sectors <= 1 else MULTI_SECTOR


def _check_task_type(value: str) -> None:
    if value not in TASK_TYPES:
        raise ValueError(f"unknown task type {value!r}; use {' or '.join(TASK_TYPES)}")


def _check_level(level: float, name: str = "level") -> float:
    if isinstance(level, bool) or not isinstance(level, (int, float)) or not math.isfinite(level) or level < 0:
        raise ValueError(f"{name} must be a finite number >= 0, not {level!r}")
    return float(level)


class PheromoneTable:
    """Pheromone levels per (worker, task type), stored in Redis.

    Reads never write: a pair that has never been set reads as initial_level until something
    sets or adds to it. Every write is a single Redis command, so writes from several threads
    or processes don't lose each other's updates. Pass a client made with decode_responses=True,
    as the coordinator's is, so worker IDs come back as strings.
    """

    def __init__(self, redis_client: redis.Redis, initial_level: float = DEFAULT_INITIAL_LEVEL):
        self.redis = redis_client
        self.initial_level = _check_level(initial_level, "initial_level")

    def get(self, worker_id: str, task_type: str) -> float:
        _check_task_type(task_type)
        value = self.redis.hget(pheromone_key(task_type), worker_id)
        return self.initial_level if value is None else float(value)

    def levels(self, task_type: str, worker_ids: Iterable[str]) -> dict[str, float]:
        """Each worker's level for task_type, in one round trip; what the scheduler reads to choose a worker."""
        _check_task_type(task_type)
        worker_ids = list(worker_ids)
        if not worker_ids:
            return {}
        values = self.redis.hmget(pheromone_key(task_type), worker_ids)
        return {
            worker_id: self.initial_level if value is None else float(value)
            for worker_id, value in zip(worker_ids, values)
        }

    def set(self, worker_id: str, task_type: str, level: float) -> None:
        _check_task_type(task_type)
        self.redis.hset(pheromone_key(task_type), worker_id, _check_level(level))

    def add(self, worker_id: str, task_type: str, amount: float, maximum: float | None = None) -> float:
        """Add amount (>= 0) to a level, starting from the initial level, and return the new level.

        With maximum, the level is capped there; that read-and-write is one transaction watching
        the hash, retried if anything else writes to it first.
        """
        _check_task_type(task_type)
        amount = _check_level(amount, "amount")
        key = pheromone_key(task_type)
        if maximum is not None:
            maximum = _check_level(maximum, "maximum")

            def capped(pipe: redis.client.Pipeline) -> float:
                current = pipe.hget(key, worker_id)
                level = min((self.initial_level if current is None else float(current)) + amount, maximum)
                pipe.multi()
                pipe.hset(key, worker_id, level)
                return level

            return self.redis.transaction(capped, key, value_from_callable=True)
        # One MULTI: start a missing pair at the initial level, then increment it in Redis, so two
        # deposits at once both count.
        with self.redis.pipeline() as pipe:
            pipe.hsetnx(key, worker_id, self.initial_level)
            pipe.hincrbyfloat(key, worker_id, amount)
            _, level = pipe.execute()
        return float(level)

    def ensure(self, worker_id: str, task_type: str) -> None:
        """Store a pair at the initial level if it has never been written, so evaporation reaches it."""
        _check_task_type(task_type)
        self.redis.hsetnx(pheromone_key(task_type), worker_id, self.initial_level)

    def evaporate(self, rate: float, minimum: float = 0.0) -> None:
        """Multiply every stored level by (1 - rate), keeping each at or above minimum.

        Pairs never written stay at the initial level. Each task type's hash is rewritten in one
        Redis transaction that watches it, so a deposit that lands mid-way is retried, not lost.
        """
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not 0 <= rate <= 1:
            raise ValueError(f"rate must be between 0 and 1, not {rate!r}")
        minimum = _check_level(minimum, "minimum")
        if rate == 0:
            return

        def scale(pipe: redis.client.Pipeline, key: str) -> None:
            levels = pipe.hgetall(key)
            pipe.multi()
            if levels:
                pipe.hset(key, mapping={worker: max(float(level) * (1 - rate), minimum) for worker, level in levels.items()})

        for kind in TASK_TYPES:
            key = pheromone_key(kind)
            self.redis.transaction(lambda pipe: scale(pipe, key), key)

    def table(self) -> dict[str, dict[str, float]]:
        """Every stored level, as {worker ID: {task type: level}}. Pairs never set are left out."""
        table: dict[str, dict[str, float]] = {}
        for kind in TASK_TYPES:
            for worker_id, value in self.redis.hgetall(pheromone_key(kind)).items():
                table.setdefault(worker_id, {})[kind] = float(value)
        return table

    def forget_worker(self, worker_id: str) -> None:
        """Remove a worker's levels, so it starts again from the initial level."""
        with self.redis.pipeline() as pipe:
            for kind in TASK_TYPES:
                pipe.hdel(pheromone_key(kind), worker_id)
            pipe.execute()

    def clear(self) -> None:
        """Remove every level."""
        self.redis.delete(*(pheromone_key(kind) for kind in TASK_TYPES))
