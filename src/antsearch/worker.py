"""Worker stub: registers with the coordinator, then sends heartbeats until stopped."""

import logging
import os
import signal
import socket
import threading

import httpx

log = logging.getLogger("antsearch.worker")


DEFAULT_HEARTBEAT_TIMEOUT = 30.0  # seconds; the coordinator's answer to register overrides it


def register(client: httpx.Client, worker_id: str) -> float:
    """Register with the coordinator and return its heartbeat timeout in seconds."""
    response = client.post("/workers/register", json={"worker_id": worker_id})
    response.raise_for_status()
    log.info("worker %s registered with coordinator at %s", worker_id, client.base_url)
    return float(response.json().get("heartbeat_timeout", DEFAULT_HEARTBEAT_TIMEOUT))


def send_heartbeats(client: httpx.Client, worker_id: str, interval: float, stop: threading.Event) -> None:
    """Send a heartbeat every interval seconds until stop is set.

    If the coordinator no longer knows this worker, register again. Network errors are logged and
    the next heartbeat tries again.
    """
    while not stop.wait(interval):
        try:
            response = client.post(f"/workers/{worker_id}/heartbeat")
            if response.status_code == 404:
                register(client, worker_id)
            else:
                response.raise_for_status()
        except httpx.HTTPError as exc:
            log.warning("heartbeat from worker %s failed: %s", worker_id, exc)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    coordinator_url = os.environ.get("COORDINATOR_URL", "http://localhost:8000")
    worker_id = os.environ.get("WORKER_ID") or socket.gethostname()

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    with httpx.Client(base_url=coordinator_url, timeout=5.0) as client:
        timeout = register(client, worker_id)
        send_heartbeats(client, worker_id, timeout / 3, stop)
    log.info("worker %s stopped", worker_id)


if __name__ == "__main__":
    main()
