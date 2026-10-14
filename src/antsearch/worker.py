"""Worker stub: registers with the coordinator, then idles until stopped."""

import logging
import os
import signal
import socket
import threading

import httpx

log = logging.getLogger("antsearch.worker")


def register(client: httpx.Client, worker_id: str) -> None:
    client.post("/workers/register", json={"worker_id": worker_id}).raise_for_status()
    log.info("worker %s registered with coordinator at %s", worker_id, client.base_url)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    coordinator_url = os.environ.get("COORDINATOR_URL", "http://localhost:8000")
    worker_id = os.environ.get("WORKER_ID") or socket.gethostname()

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    with httpx.Client(base_url=coordinator_url, timeout=5.0) as client:
        register(client, worker_id)
    stop.wait()
    log.info("worker %s stopped", worker_id)


if __name__ == "__main__":
    main()
