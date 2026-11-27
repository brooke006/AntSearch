import csv
import re
import statistics
from pathlib import Path

import pytest
import yaml

from antsearch import sweep
from antsearch.pheromone import MULTI_SECTOR, SHORT
from antsearch.scheduler import DEFAULT_ALPHA, DEFAULT_BETA, DEFAULT_EVAPORATION
from antsearch.submit import read_sectors, read_stars
from antsearch.sweep import Run, Setting, Worker, ant_settings, arrival_rate, simulate, summarize

STARS = list(zip(read_stars(limit=40), read_sectors(limit=40)))
ANT = Setting("ant", 1.0, 2.0, 0.2)


def test_same_inputs_give_the_same_run():
    assert simulate(ANT, "flaky", 0.5, 3, STARS) == simulate(ANT, "flaky", 0.5, 3, STARS)


@pytest.mark.parametrize("workload", sorted(sweep.WORKLOADS))
def test_every_star_finishes(workload):
    result = simulate(ANT, workload, 0.8, 0, STARS)

    assert result["mean_turnaround"] > 0
    assert sum(result["runs"].values()) == len(STARS) + result["failed_runs"]


def test_only_the_flaky_worker_fails_and_failed_stars_are_run_again():
    flaky = simulate(Setting("round-robin"), "flaky", 0.5, 0, STARS)
    steady = simulate(Setting("round-robin"), "mixed-speeds", 0.5, 0, STARS)

    assert flaky["failed_runs"] > 0
    assert steady["failed_runs"] == 0


def test_every_setting_gets_the_same_work():
    # With one worker every scheduler has to give it everything, so the runs match exactly.
    sweep.WORKLOADS["solo"] = {"worker-1": Worker()}
    try:
        results = [simulate(setting, "solo", 0.5, 1, STARS)["mean_turnaround"] for setting in (Setting("round-robin"), Setting("greedy"), ANT)]
    finally:
        del sweep.WORKLOADS["solo"]

    assert results[0] == pytest.approx(results[1]) == pytest.approx(results[2])


def test_scheduling_matters_when_workers_differ():
    stars = list(zip(read_stars(limit=100), read_sectors(limit=100)))
    ratios = [
        simulate(Setting("greedy"), "mixed-speeds", 0.5, seed, stars)["mean_turnaround"]
        / simulate(Setting("round-robin"), "mixed-speeds", 0.5, seed, stars)["mean_turnaround"]
        for seed in range(3)
    ]

    assert statistics.fmean(ratios) < 0.95


def test_arrival_rate_is_load_times_capacity():
    stars = [("a", 1), ("b", 3)]  # one short task, one multi-sector
    workers = {"w1": Worker(), "w2": Worker(slowness={SHORT: 1.0, MULTI_SECTOR: 3.0})}
    mean_seconds = [(10 + 20) / 2, (10 + 60) / 2]

    assert arrival_rate(workers, stars, 0.5) == pytest.approx(0.5 * sum(1 / seconds for seconds in mean_seconds))


def test_alpha_zero_appears_once_per_beta():
    settings = ant_settings(alphas=(0, 1), betas=(0, 2), evaporations=(0.1, 0.2))

    assert settings == [
        Setting("ant", 0.0, 0.0, None),
        Setting("ant", 0.0, 2.0, None),
        Setting("ant", 1.0, 0.0, 0.1),
        Setting("ant", 1.0, 0.0, 0.2),
        Setting("ant", 1.0, 2.0, 0.1),
        Setting("ant", 1.0, 2.0, 0.2),
    ]
    assert settings[0].label == "ant alpha=0 beta=0 evaporation=any"


def test_scores_are_relative_to_round_robin_and_sorted():
    def run(setting, seed, seconds):
        return Run(setting, "slowdown", 0.5, seed, seconds, 0)

    runs = [
        run(Setting("round-robin"), 0, 10.0),
        run(Setting("round-robin"), 1, 20.0),
        run(ANT, 0, 8.0),
        run(ANT, 1, 18.0),
        run(Setting("random"), 0, 11.0),
        run(Setting("random"), 1, 22.0),
    ]
    summaries = summarize(runs)

    assert [summary.setting for summary in summaries] == [ANT, Setting("round-robin"), Setting("random")]
    assert summaries[0].score == pytest.approx((0.8 + 0.9) / 2)
    assert summaries[1].score == 1.0
    assert summaries[2].score == pytest.approx(1.1)


def test_main_writes_the_table_and_the_runs(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sweep, "ALPHAS", (0.0, 1.0))
    monkeypatch.setattr(sweep, "BETAS", (2.0,))
    monkeypatch.setattr(sweep, "EVAPORATIONS", (0.2,))
    monkeypatch.setattr(sweep, "LOADS", (0.5,))

    assert sweep.main(["--seeds", "2", "--stars", "10", "--processes", "1", "--out-dir", str(tmp_path)]) == 0

    results = (tmp_path / sweep.RESULTS_FILE).read_text()
    assert "python -m antsearch.sweep --seeds 2 --stars 10" in results
    assert "| round-robin |" in results
    assert "| greedy |" in results
    with (tmp_path / sweep.RUNS_FILE).open() as f:
        rows = list(csv.DictReader(f))
    # (3 baselines + 2 ant settings) x 3 workloads x 1 load x 2 seeds
    assert len(rows) == 5 * 3 * 2
    assert "best ant setting" in capsys.readouterr().out


@pytest.mark.parametrize("args", [["--seeds", "1"], ["--stars", "0"]])
def test_main_rejects_bad_arguments(args):
    with pytest.raises(SystemExit) as exit:
        sweep.main(args)
    assert exit.value.code == 2


def test_compose_uses_the_chosen_settings():
    compose = yaml.safe_load((Path(__file__).parents[1] / "compose.yaml").read_text())
    environment = compose["services"]["coordinator"]["environment"]

    def default(name):
        return float(re.fullmatch(r"\$\{" + name + r":-(.+)\}", environment[name]).group(1))

    assert default("SCHEDULER_ALPHA") == DEFAULT_ALPHA
    assert default("SCHEDULER_BETA") == DEFAULT_BETA
    assert default("SCHEDULER_EVAPORATION") == DEFAULT_EVAPORATION
