"""Failure scenarios: limit CPUs, add network delay, and kill or restart compose workers on a fixed timeline.

    python -m antsearch.scenario scenarios/crash-and-recover.yaml
    python -m antsearch.scenario scenarios/slow-worker.yaml --log output/slow-worker.jsonl
    python -m antsearch.scenario scenarios/slow-worker.yaml --dry-run     # print the timeline only

A scenario file lists events, each at a number of seconds after the scenario starts:

    duration: 90            # optional: reset and exit this many seconds after the start
    events:
      - {at: 0,  worker: worker-1, action: cpu, cpus: 0.25}    # Docker CPU limit; 0 lifts it
      - {at: 0,  worker: worker-2, action: delay, ms: 200}     # delay on every packet it sends; 0 removes it
      - {at: 30, worker: worker-2, action: kill}               # SIGKILL: no goodbye, the task is lost
      - {at: 60, worker: worker-2, action: start}              # start a killed worker again
      - {at: 75, worker: worker-1, action: restart}            # kill, then start straight away

Every run starts from the same state: before the first event, each worker the scenario names is
started if it isn't running, and its CPU limit and delay are removed. Events are timed from that
point, so a slow docker command makes its own event late but not the ones after it. Events at the
same time run in file order. After the last event the scenario holds until `duration` (or Ctrl-C /
SIGTERM if there's none), then resets the workers again.

A worker's delay lives in its container's network namespace, which a kill throws away, so it's set
again whenever the scenario starts that worker. The CPU limit is part of the container's config
and survives on its own.

Exit codes:
    0  every event applied
    1  some events failed (each is logged, with its error, and the scenario carries on)
    3  couldn't reach the workers (is the stack up?), so no events ran
(2 is argparse's code for bad command-line arguments or an invalid scenario file.)
"""

import argparse
import json
import logging
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger("antsearch.scenario")

EXIT_OK = 0
EXIT_EVENTS_FAILED = 1
EXIT_NO_STACK = 3

# Each action and the parameter it takes, if any.
ACTIONS = {"cpu": "cpus", "delay": "ms", "kill": None, "start": None, "restart": None}


class ScenarioError(ValueError):
    """The scenario file isn't valid."""


class DockerError(RuntimeError):
    """A docker command failed."""


@dataclass(frozen=True)
class Event:
    at: float  # seconds after the scenario starts
    worker: str  # compose service name
    action: str
    value: float | None = None  # cpus for cpu, ms for delay

    def describe(self) -> str:
        if self.action == "cpu":
            return f"{self.worker}: limit to {self.value:g} CPUs" if self.value else f"{self.worker}: no CPU limit"
        if self.action == "delay":
            return f"{self.worker}: {self.value:g} ms network delay" if self.value else f"{self.worker}: no network delay"
        return f"{self.worker}: {self.action}"

    def to_dict(self) -> dict[str, Any]:
        entry = {"at": self.at, "worker": self.worker, "action": self.action}
        if ACTIONS[self.action]:
            entry[ACTIONS[self.action]] = self.value
        return entry


@dataclass(frozen=True)
class Scenario:
    events: tuple[Event, ...]  # sorted by time; events at the same time keep their file order
    duration: float | None = None

    @property
    def workers(self) -> list[str]:
        return sorted({event.worker for event in self.events})


def load_scenario(path: str | Path) -> Scenario:
    with open(path) as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            raise ScenarioError(f"{path} isn't valid YAML: {exc}") from exc
    return parse_scenario(data)


def parse_scenario(data: Any) -> Scenario:
    """Check a scenario's structure and turn it into a Scenario, sorted by time."""
    if not isinstance(data, dict):
        raise ScenarioError("a scenario must be a mapping with an events list")
    unknown = set(data) - {"description", "duration", "events"}
    if unknown:
        raise ScenarioError(f"unknown keys {sorted(unknown)}; expected description, duration and events")
    raw_events = data.get("events")
    if not isinstance(raw_events, list) or not raw_events:
        raise ScenarioError("events must be a non-empty list")
    events = sorted((parse_event(raw, i) for i, raw in enumerate(raw_events, 1)), key=lambda e: e.at)
    duration = data.get("duration")
    if duration is not None:
        duration = number(duration, "duration")
        if duration < events[-1].at:
            raise ScenarioError(f"duration {duration:g} s ends before the last event at {events[-1].at:g} s")
    return Scenario(tuple(events), duration)


def parse_event(raw: Any, index: int) -> Event:
    where = f"event {index}"
    if not isinstance(raw, dict):
        raise ScenarioError(f"{where} must be a mapping with at, worker and action")
    action = raw.get("action")
    if action not in ACTIONS:
        raise ScenarioError(f"{where}: action must be one of {', '.join(ACTIONS)}, not {action!r}")
    param = ACTIONS[action]
    expected = {"at", "worker", "action"} | ({param} if param else set())
    if set(raw) != expected:
        raise ScenarioError(f"{where}: a {action} event takes exactly {', '.join(sorted(expected))}")
    worker = raw["worker"]
    if not isinstance(worker, str) or not worker:
        raise ScenarioError(f"{where}: worker must be a compose service name such as worker-1")
    return Event(number(raw["at"], f"{where}: at"), worker, action, number(raw[param], f"{where}: {param}") if param else None)


def number(value: Any, name: str) -> float:
    """value as a float, if it's a number that isn't negative."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ScenarioError(f"{name} must be a number of at least 0, not {value!r}")
    return float(value)


def run_command(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, timeout=60)


class Docker:
    """The docker commands a scenario needs, run against one compose project's containers."""

    def __init__(self, project: str | None = None, run: Callable[[list[str]], subprocess.CompletedProcess] | None = None):
        self.project = project
        self.run = run or run_command
        self.containers: dict[str, str] = {}
        self.cpus: float | None = None

    def docker(self, *args: str) -> str:
        result = self.run(["docker", *args])
        if result.returncode != 0:
            raise DockerError(f"docker {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def container(self, service: str) -> str:
        """The container ID for a compose service, running or not."""
        if service not in self.containers:
            project = ["-p", self.project] if self.project else []
            container = self.docker("compose", *project, "ps", "--all", "--quiet", service)
            if not container:
                raise DockerError(f"no container for {service!r}; is the stack up (make up)?")
            self.containers[service] = container
        return self.containers[service]

    def host_cpus(self) -> float:
        """Every CPU Docker has. docker update can't remove a CPU limit, so this stands in for none."""
        if self.cpus is None:
            self.cpus = float(self.docker("info", "--format", "{{.NCPU}}"))
        return self.cpus

    def limit_cpus(self, service: str, cpus: float) -> None:
        self.docker("update", "--cpus", f"{cpus or self.host_cpus():g}", self.container(service))

    def set_delay(self, service: str, ms: float) -> None:
        """Delay every packet the container sends by ms, or remove the delay if ms is 0."""
        tc = ["exec", "--user", "root", self.container(service), "tc", "qdisc"]
        if ms:
            self.docker(*tc, "replace", "dev", "eth0", "root", "netem", "delay", f"{ms:g}ms")
        else:
            self.run(["docker", *tc, "del", "dev", "eth0", "root"])  # fails if there's no delay, which is fine

    def kill(self, service: str) -> None:
        self.docker("kill", self.container(service))

    def start(self, service: str) -> None:
        self.docker("start", self.container(service))


class Injector:
    """Applies events to the workers, remembering each one's delay so a restart can set it again."""

    def __init__(self, docker: Docker):
        self.docker = docker
        self.delays: dict[str, float] = {}
        self.stopped: set[str] = set()

    def reset(self, workers: list[str]) -> None:
        """Start each worker if it isn't running, and remove its CPU limit and delay."""
        for worker in workers:
            self.docker.start(worker)
            self.docker.limit_cpus(worker, 0)
            self.docker.set_delay(worker, 0)
        self.delays.clear()
        self.stopped.clear()

    def apply(self, event: Event) -> None:
        worker = event.worker
        if event.action == "cpu":
            self.docker.limit_cpus(worker, event.value)
        elif event.action == "delay":
            self.delays[worker] = event.value
            if worker not in self.stopped:  # a killed worker gets its delay when it starts
                self.docker.set_delay(worker, event.value)
        elif event.action == "kill":
            self.docker.kill(worker)
            self.stopped.add(worker)
        else:  # start or restart
            if event.action == "restart" and worker not in self.stopped:
                self.docker.kill(worker)
            self.docker.start(worker)
            self.stopped.discard(worker)
            if self.delays.get(worker):
                self.docker.set_delay(worker, self.delays[worker])


def run(
    scenario: Scenario,
    injector: Injector,
    record: Callable[[dict[str, Any]], None],
    wait: Callable[[float | None], bool],
    now: Callable[[], float] = time.monotonic,
) -> int:
    """Reset the workers, apply each event at its time, then hold and reset again. Returns the number of failed events.

    record gets one entry per event: the event, plus elapsed (seconds since the start when it was
    applied) and error if it failed. wait(seconds) sleeps, returning True if the run should stop
    early (wait(None) sleeps until then).
    """
    injector.reset(scenario.workers)
    start = now()
    failed = 0
    try:
        for event in scenario.events:
            if wait(max(0.0, start + event.at - now())):
                log.info("stopped before the event at %g s", event.at)
                return failed
            entry = {**event.to_dict(), "elapsed": round(now() - start, 3)}
            try:
                injector.apply(event)
                log.info("%7.2f s  %s", entry["elapsed"], event.describe())
            except DockerError as exc:
                failed += 1
                entry["error"] = str(exc)
                log.error("%7.2f s  %s failed: %s", entry["elapsed"], event.describe(), exc)
            record(entry)
        if scenario.duration is None:
            log.info("all events applied; holding until Ctrl-C or SIGTERM")
            wait(None)
        else:
            wait(max(0.0, start + scenario.duration - now()))
        return failed
    finally:
        try:
            injector.reset(scenario.workers)
            log.info("workers reset")
        except DockerError as exc:
            log.error("could not reset the workers: %s", exc)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("scenario", type=Path, help="scenario YAML file")
    parser.add_argument("--project", "-p", help="compose project the workers belong to (default: compose's own default)")
    parser.add_argument("--log", type=Path, help="write one JSON line per applied event to this file")
    parser.add_argument("--dry-run", action="store_true", help="print the timeline and exit without touching Docker")
    args = parser.parse_args(argv)

    try:
        scenario = load_scenario(args.scenario)
    except (OSError, ScenarioError) as exc:
        parser.error(str(exc))
    if args.dry_run:
        for event in scenario.events:
            print(f"{event.at:7.2f} s  {event.describe()}")
        if scenario.duration is not None:
            print(f"{scenario.duration:7.2f} s  reset and exit")
        return EXIT_OK

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    entries = []
    if args.log:
        args.log.parent.mkdir(parents=True, exist_ok=True)
        args.log.write_text("")

    def record(entry):
        entries.append(entry)
        if args.log:
            with args.log.open("a") as f:
                f.write(json.dumps(entry) + "\n")

    try:
        failed = run(scenario, Injector(Docker(args.project)), record, stop.wait)
    except DockerError as exc:
        log.error("%s", exc)
        return EXIT_NO_STACK
    log.info("%d of %d events applied", len(entries) - failed, len(scenario.events))
    return EXIT_EVENTS_FAILED if failed else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
