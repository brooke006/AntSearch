import csv

import pytest

from antsearch import recall
from antsearch.recall import compare, compute, read_star_list, report
from antsearch.runlog import RunLog

STARS = {
    "TOI-1": {"has_planet": True, "period": 4.0},
    "TOI-2": {"has_planet": True, "period": 3.0},
    "TOI-3": {"has_planet": True, "period": 2.0},
    "TOI-4": {"has_planet": True, "period": 20.0},
    "TOI-5": {"has_planet": False, "period": 1.5},
    "TOI-6": {"has_planet": False, "period": 6.0},
    "TOI-7": {"has_planet": False, "period": 5.0},
    "TOI-8": {"has_planet": True, "period": 5.0},
    "TOI-9": {"has_planet": True, "period": 7.0},
}


def signal(period, snr=20.0, depth=0.001):
    return {"period": period, "depth": depth, "power": 0.1, "snr": snr}


def events(results):
    out = []
    for n, (star, result) in enumerate(results.items()):
        result, outcome = result if isinstance(result, tuple) else (result, "accepted")
        out.append({"event": "task_queued", "time": n, "task_id": f"t{n}", "star_id": star})
        out.append({"event": "task_done", "time": n, "task_id": f"t{n}", "result": result, "outcome": outcome})
    return out


@pytest.mark.parametrize("found, known, expected", [
    (4.02, 4.0, "same"),
    (4.05, 4.0, None),
    (8.0, 4.0, "harmonic"),
    (2.0, 4.0, "harmonic"),
    (12.05, 4.0, "harmonic"),
    (6.0, 4.0, None),
])
def test_compare_periods(found, known, expected):
    assert compare(found, known) == expected


def test_recall_counts_recovered_planets_and_lists_unvetted_signals():
    result = compute(events({
        "TOI-1": signal(4.01),
        "TOI-2": signal(6.0),
        "TOI-3": signal(5.3),
        "TOI-4": signal(13.0),
        "TOI-5": signal(1.5, snr=30),
        "TOI-6": signal(2.2, snr=9),
        "TOI-7": signal(3.0, snr=5),
        "TOI-8": {"error": "NoDataError", "message": "no light curve"},
        "TOI-9": (signal(7.0), "unresolved"),
        "TOI-404": signal(1.0),
    }), STARS)

    assert {row["star_id"]: row["verdict"] for row in result["stars"]} == {
        "TOI-1": "recovered", "TOI-2": "harmonic", "TOI-3": "missed", "TOI-4": "missed",
        "TOI-5": "unvetted", "TOI-6": "unvetted", "TOI-7": "quiet", "TOI-8": "failed", "TOI-9": "unresolved",
    }
    assert (result["recovered"], result["planets_searched"], result["recall"]) == (1, 4, 0.25)
    assert (result["recovered_in_range"], result["planets_in_range"]) == (1, 3)
    assert (result["harmonic"], result["missed"], result["failed"], result["unresolved"]) == (1, 2, 1, 1)
    assert (result["unvetted"], result["no_planet_searched"]) == (2, 3)
    notes = {row["star_id"]: row["note"] for row in result["stars"] if row["verdict"] == "unvetted"}
    assert notes == {"TOI-5": "the signal that was ruled a false positive", "TOI-6": "a new signal"}


def test_min_snr_and_tolerance_can_be_changed():
    log = events({"TOI-1": signal(4.1), "TOI-5": signal(9.0, snr=9)})

    assert compute(log, STARS)["recovered"] == 0
    assert compute(log, STARS, tolerance=0.05)["recovered"] == 1
    assert compute(log, STARS)["unvetted"] == 1
    assert compute(log, STARS, min_snr=10)["unvetted"] == 0


def test_results_without_snr_are_never_strong():
    old = {"period": 9.0, "depth": 0.01, "power": 0.4}
    result = compute(events({"TOI-5": old}), STARS)

    assert (result["unvetted"], result["no_snr"]) == (0, 1)
    assert "no SNR" in report(result)


def test_report_lists_unvetted_signals_and_misses():
    text = report(compute(events({"TOI-1": signal(4.0), "TOI-2": signal(6.0), "TOI-6": signal(2.2, snr=9)}), STARS))

    assert "recall:     1 of 2 known planets recovered (50.0%)" in text
    assert "TOI-6      found   2.20000 d    1000 ppm  SNR    9.0  a new signal" in text
    assert "TOI-2      known   3.00000 d  found   6.00000 d  SNR   20.0  harmonic" in text


def test_reads_the_bundled_star_list():
    stars = read_star_list()

    assert len(stars) == 200
    assert sum(star["has_planet"] for star in stars.values()) == 100
    assert stars["TOI-102"] == {"has_planet": True, "period": pytest.approx(4.4119372)}


def test_main_saves_the_report_and_a_csv_per_star(tmp_path, capsys):
    path = tmp_path / "run.jsonl"
    log = RunLog(path, lambda: 0.0)
    log.record("task_queued", task_id="t1", star_id="TOI-102")
    log.record("task_done", task_id="t1", result=signal(4.412), outcome="accepted")

    assert recall.main([str(path), "--save", str(tmp_path / "saved")]) == 0

    assert "1 of 1 known planets recovered (100.0%)" in (tmp_path / "saved" / "run-recall.txt").read_text()
    with open(tmp_path / "saved" / "run-recall.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [(row["star_id"], row["verdict"]) for row in rows] == [("TOI-102", "recovered")]


def test_main_on_a_simulated_run_says_so(tmp_path, capsys):
    path = tmp_path / "run.jsonl"
    path.write_text('{"event": "task_queued", "time": 0, "task_id": "t1", "star_id": "TOI-102"}\n')

    assert recall.main([str(path), "--save", str(tmp_path / "saved")]) == 0
    assert "no transit-search results" in capsys.readouterr().out
    assert not (tmp_path / "saved").exists()
