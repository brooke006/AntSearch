"""Scheduler class and a round-robin version.

Pass in the available worker IDs in the same order each time.
The coordinator handles the tasks and checks which workers are online.
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
        """Receive a worker's result after it finishes a task."""


class RoundRobinScheduler(Scheduler):
    """Give each worker a turn, then start over.

    This remembers the last worker until the scheduler restarts.
    Calls must happen one at a time. Separate processes do not share turns.
    """

    def __init__(self) -> None:
        self._last_worker: str | None = None

    def assign(self, worker_ids: Sequence[str]) -> str | None:
        # Remove duplicate IDs so each worker gets one turn.
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
        # Results do not change whose turn is next.
        pass
