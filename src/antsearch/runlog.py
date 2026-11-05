"""Run log: one JSON line for everything that happens in a run, so metrics can be computed afterwards.

Every line has a time (Unix seconds on the coordinator's clock) and an event, plus:

    coordinator_started   scheduler, seed, heartbeat_timeout
    worker_registered     worker_id
    worker_offline        worker_id, last_heartbeat
    worker_online         worker_id
    task_queued           task_id, star_id
    task_assigned         task_id, star_id, worker_id
    task_done             task_id, worker_id, result (a failed run's result has an "error" key)
    task_requeued         task_id, worker_id, reason (offline or reregistered), last_heartbeat
    result_rejected       task_id, worker_id, status, result

antsearch.metrics reads these files.
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

log = logging.getLogger("antsearch.runlog")


class RunLog:
    """Appends events to a JSON-lines file, flushing each one so a killed coordinator loses nothing.

    With no path it records nothing. A failed write is logged rather than raised: a full disk
    shouldn't stop the cluster, whose state has already changed by the time an event is recorded.
    """

    def __init__(self, path: str | Path | None = None, clock: Callable[[], float] = time.time):
        self.path = Path(path) if path else None
        self.clock = clock
        self.lock = threading.Lock()
        self.file = None
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.file = self.path.open("a", buffering=1)

    def record(self, event: str, **fields: Any) -> None:
        if self.file is None:
            return
        try:
            with self.lock:
                self.file.write(json.dumps({"time": self.clock(), "event": event, **fields}) + "\n")
        except OSError as exc:
            log.error("could not write %s to %s: %s", event, self.path, exc)


NO_RUN_LOG = RunLog()


def read_events(path: str | Path) -> list[dict[str, Any]]:
    """The events in a run log, in the order they were written.

    A last line with no newline is dropped with a warning: the coordinator was killed mid-write.
    """
    text = Path(path).read_text()
    lines = text.split("\n")
    if lines[-1]:
        log.warning("%s ends with an unfinished line; ignoring it", path)
    return [json.loads(line) for line in lines[:-1] if line]
