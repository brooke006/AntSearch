"""Common scheduler interface and a round-robin baseline.

The caller supplies available worker IDs in a stable order. The coordinator
still owns task storage, worker health, and sending tasks to workers.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any


class Scheduler(ABC):
    @abstractmethod
    def assign(self, worker_ids: Sequence[str]) -> str | None:
        """Choose an available worker, or return None if there are none."""

    @abstractmethod
    def on_result(self, worker_id: str, result: Mapping[str, Any]) -> None:
        """Receive a completed worker result for scheduler feedback."""


class RoundRobinScheduler(Scheduler):
    """Choose each available worker in turn, then repeat.

    This is an in-memory baseline intended for one serialized caller. It does
    not persist its position or coordinate multiple coordinator processes.
    """

    def __init__(self) -> None:
        self._last_worker: str | None = None

    def assign(self, worker_ids: Sequence[str]) -> str | None:
        # Ignore duplicate IDs so a worker does not get extra turns.
        workers = list(dict.fromkeys(worker_ids))
        if not workers:
            return None

        if self._last_worker in workers:
            index = (workers.index(self._last_worker) + 1) % len(workers)
        else:
            index = 0

        self._last_worker = workers[index]
        return self._last_worker

    def on_result(self, worker_id: str, result: Mapping[str, Any]) -> None:
        # Round robin does not use results to change worker selection.
        pass
