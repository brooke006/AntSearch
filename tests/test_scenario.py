import json
import subprocess
from pathlib import Path

import pytest

from antsearch import scenario
from antsearch.scenario import Docker, Event, Injector, ScenarioError, parse_scenario

SCENARIOS = Path(__file__).parent.parent / "scenarios"


class FakeClock:
    """now() and wait() for run(), with time that only moves when something waits or runs."""

    def __init__(self):
        self.t = 100.0

    def now(self):
        return self.t

    def wait(self, seconds):
        if seconds is None:  # hold until stopped: stop straight away
            return True
        self.t += seconds
        return False


class FakeDocker(Docker):
    """Records each docker command instead of running it; each takes command_seconds on the clock."""

    def __init__(self, clock=None, command_seconds=0.0, fail=()):
        super().__init__(project="test", run=self.fake_run)
        self.clock = clock
        self.command_seconds = command_seconds
        self.fail = fail  # commands whose first word after "docker" is in here fail
        self.commands = []

    def fake_run(self, args):
        if self.clock:
            self.clock.t += self.command_seconds
        args = args[1:]
        if args[0] == "compose":  # compose ps --quiet <service>: the container ID
            return subprocess.CompletedProcess(args, 0, f"id-{args[-1]}\n", "")
        if args[0] == "info":
            return subprocess.CompletedProcess(args, 0, "8\n", "")
        self.commands.append(" ".join(args))
        if args[0] in self.fail:
            return subprocess.CompletedProcess(args, 1, "", f"{args[0]} broke")
        return subprocess.CompletedProcess(args, 0, "", "")


def reset_commands(*workers):
    commands = []
    for worker in workers:
        commands += [
            f"start id-{worker}",
            f"update --cpus 8 id-{worker}",
            f"exec --user root id-{worker} tc qdisc del dev eth0 root",
        ]
    return commands


def play(data, docker=None, clock=None):
    clock = clock or FakeClock()
    docker = docker or FakeDocker(clock)
    entries = []
    failed = scenario.run(parse_scenario(data), Injector(docker), entries.append, clock.wait, clock.now)
    return entries, docker.commands, failed


def test_events_are_sorted_by_time_and_ties_keep_file_order():
    parsed = parse_scenario({"events": [
        {"at": 30, "worker": "worker-2", "action": "kill"},
        {"at": 0, "worker": "worker-1", "action": "cpu", "cpus": 0.5},
        {"at": 0, "worker": "worker-1", "action": "delay", "ms": 100},
    ]})

    assert [(e.at, e.action) for e in parsed.events] == [(0, "cpu"), (0, "delay"), (30, "kill")]
    assert parsed.workers == ["worker-1", "worker-2"]
    assert parsed.duration is None


@pytest.mark.parametrize(
    "data, message",
    [
        ([], "must be a mapping"),
        ({"events": []}, "non-empty list"),
        ({"events": [{"at": 0, "worker": "w", "action": "kill"}], "seed": 1}, "unknown keys"),
        ({"events": [{"at": 0, "worker": "w", "action": "explode"}]}, "action must be one of"),
        ({"events": [{"at": 0, "worker": "w", "action": "cpu"}]}, "takes exactly action, at, cpus, worker"),
        ({"events": [{"at": 0, "worker": "w", "action": "kill", "ms": 5}]}, "takes exactly action, at, worker"),
        ({"events": [{"at": -1, "worker": "w", "action": "kill"}]}, "event 1: at must be a number"),
        ({"events": [{"at": True, "worker": "w", "action": "kill"}]}, "at must be a number"),
        ({"events": [{"at": 0, "worker": "w", "action": "delay", "ms": "200ms"}]}, "ms must be a number"),
        ({"events": [{"at": 0, "worker": "", "action": "kill"}]}, "compose service name"),
        ({"events": [{"at": 50, "worker": "w", "action": "kill"}], "duration": 40}, "ends before the last event"),
    ],
)
def test_invalid_scenarios_are_rejected(data, message):
    with pytest.raises(ScenarioError, match=message):
        parse_scenario(data)


@pytest.mark.parametrize("path", sorted(SCENARIOS.glob("*.yaml")), ids=lambda p: p.name)
def test_bundled_scenarios_are_valid(path):
    assert scenario.load_scenario(path).events


def test_run_resets_applies_each_event_and_resets_again():
    entries, commands, failed = play({"duration": 50, "events": [
        {"at": 0, "worker": "worker-1", "action": "cpu", "cpus": 0.25},
        {"at": 5, "worker": "worker-1", "action": "delay", "ms": 200},
        {"at": 10, "worker": "worker-2", "action": "kill"},
        {"at": 20, "worker": "worker-2", "action": "start"},
        {"at": 30, "worker": "worker-1", "action": "restart"},
        {"at": 40, "worker": "worker-1", "action": "cpu", "cpus": 0},
    ]})

    assert failed == 0
    assert commands == [
        *reset_commands("worker-1", "worker-2"),
        "update --cpus 0.25 id-worker-1",
        "exec --user root id-worker-1 tc qdisc replace dev eth0 root netem delay 200ms",
        "kill id-worker-2",
        "start id-worker-2",
        "kill id-worker-1",
        "start id-worker-1",
        "exec --user root id-worker-1 tc qdisc replace dev eth0 root netem delay 200ms",  # the restart lost it
        "update --cpus 8 id-worker-1",  # 0 means no limit: all of Docker's CPUs
        *reset_commands("worker-1", "worker-2"),
    ]
    assert [(e["at"], e["elapsed"]) for e in entries] == [(0, 0), (5, 5), (10, 10), (20, 20), (30, 30), (40, 40)]


def test_delay_set_while_a_worker_is_killed_is_applied_when_it_starts():
    _, commands, _ = play({"events": [
        {"at": 0, "worker": "worker-2", "action": "kill"},
        {"at": 1, "worker": "worker-2", "action": "delay", "ms": 50},
        {"at": 2, "worker": "worker-2", "action": "start"},
    ]})

    assert commands[3:6] == [
        "kill id-worker-2",
        "start id-worker-2",
        "exec --user root id-worker-2 tc qdisc replace dev eth0 root netem delay 50ms",
    ]


def test_slow_commands_make_their_own_event_late_but_not_later_ones():
    clock = FakeClock()
    entries, _, _ = play(
        {"events": [{"at": t, "worker": "worker-1", "action": "cpu", "cpus": 1} for t in (0, 0, 0, 10, 20)]},
        docker=FakeDocker(clock, command_seconds=2.0),
        clock=clock,
    )

    # The three events at 0 queue up behind each other; the later ones start on time again.
    assert [e["elapsed"] for e in entries] == [0, 2, 4, 10, 20]


def test_same_scenario_twice_gives_the_same_events_at_the_same_times():
    data = {"duration": 60, "events": [
        {"at": 0, "worker": "worker-1", "action": "cpu", "cpus": 0.5},
        {"at": 15, "worker": "worker-2", "action": "delay", "ms": 300},
        {"at": 30, "worker": "worker-2", "action": "kill"},
        {"at": 45, "worker": "worker-2", "action": "start"},
    ]}

    first, second = play(data), play(data)

    assert first == second


def test_failed_event_is_recorded_and_the_rest_still_run():
    entries, commands, failed = play(
        {"events": [
            {"at": 0, "worker": "worker-1", "action": "kill"},
            {"at": 1, "worker": "worker-1", "action": "start"},
        ]},
        docker=FakeDocker(fail={"kill"}),
    )

    assert failed == 1
    assert "kill broke" in entries[0]["error"]
    assert "error" not in entries[1]
    assert commands[-3:] == reset_commands("worker-1")


def test_stopping_early_skips_the_remaining_events_and_resets():
    clock = FakeClock()
    stops = iter([False, True])  # the wait for the first event ends normally; Ctrl-C during the second
    clock.wait = lambda seconds: next(stops)
    entries, commands, _ = play(
        {"events": [{"at": 1, "worker": "worker-1", "action": "kill"}, {"at": 2, "worker": "worker-1", "action": "start"}]},
        clock=clock,
    )

    assert [e["action"] for e in entries] == ["kill"]
    assert commands[-3:] == reset_commands("worker-1")


def test_describe():
    assert Event(0, "worker-1", "cpu", 0.25).describe() == "worker-1: limit to 0.25 CPUs"
    assert Event(0, "worker-1", "cpu", 0).describe() == "worker-1: no CPU limit"
    assert Event(0, "worker-2", "delay", 200).describe() == "worker-2: 200 ms network delay"
    assert Event(0, "worker-2", "delay", 0).describe() == "worker-2: no network delay"
    assert Event(0, "worker-2", "restart").describe() == "worker-2: restart"


def write(tmp_path, data):
    path = tmp_path / "s.yaml"
    path.write_text(json.dumps(data))  # JSON is YAML
    return path


def test_main_dry_run_prints_the_timeline_without_docker(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(scenario, "run_command", lambda args: pytest.fail("dry run ran docker"))
    path = write(tmp_path, {"duration": 30, "events": [{"at": 10, "worker": "worker-2", "action": "kill"}]})

    assert scenario.main([str(path), "--dry-run"]) == scenario.EXIT_OK
    assert capsys.readouterr().out.split("\n")[:2] == ["  10.00 s  worker-2: kill", "  30.00 s  reset and exit"]


def test_main_rejects_an_invalid_file(tmp_path, capsys):
    with pytest.raises(SystemExit) as exc:
        scenario.main([str(write(tmp_path, {"events": []}))])

    assert exc.value.code == 2
    assert "non-empty list" in capsys.readouterr().err


def test_main_writes_the_event_log(tmp_path, monkeypatch):
    docker = FakeDocker()
    monkeypatch.setattr(scenario, "run_command", docker.fake_run)
    path = write(tmp_path, {"duration": 0, "events": [{"at": 0, "worker": "worker-1", "action": "cpu", "cpus": 2}]})
    log = tmp_path / "out" / "run.jsonl"

    assert scenario.main([str(path), "--log", str(log)]) == scenario.EXIT_OK
    (entry,) = [json.loads(line) for line in log.read_text().splitlines()]
    assert entry["action"] == "cpu" and entry["cpus"] == 2 and entry["elapsed"] < 1


def test_main_reports_a_stack_that_isnt_up(tmp_path, monkeypatch):
    monkeypatch.setattr(scenario, "run_command", lambda args: subprocess.CompletedProcess(args, 0, "", ""))
    path = write(tmp_path, {"events": [{"at": 0, "worker": "worker-1", "action": "kill"}]})

    assert scenario.main([str(path)]) == scenario.EXIT_NO_STACK
