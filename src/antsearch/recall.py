"""Planet recall: which known planets a run found again, and strong signals on the other stars.

    python -m antsearch.recall [output/runs/<run>.jsonl] [--save docs/runs]
"""

import argparse
import csv
import json
import logging
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from antsearch.metrics import DEFAULT_LOG_DIR, newest_log
from antsearch.runlog import read_events
from antsearch.submit import DEFAULT_STARS

TOLERANCE = 0.01
MIN_SNR = 7.1  # Kepler's detection threshold
SEARCH_RANGE = (0.5, 15.0)  # days, as in pipeline.run_task
HARMONICS = (2, 3)


def read_star_list(path: str | Path = DEFAULT_STARS) -> dict[str, dict[str, Any]]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    return {
        f"TOI-{row['star_id']}": {
            "has_planet": row["has_planet"].strip().lower() == "true",
            "period": float(row["period"]) if row.get("period") else None,
        }
        for row in rows
    }


def compare(found: float, known: float, tolerance: float = TOLERANCE) -> str | None:
    if abs(found / known - 1) <= tolerance:
        return "same"
    for n in HARMONICS:
        if abs(found / (known * n) - 1) <= tolerance or abs(found * n / known - 1) <= tolerance:
            return "harmonic"
    return None


def classify(star: dict[str, Any], result: dict[str, Any], outcome: str, tolerance: float, min_snr: float) -> str:
    if outcome != "accepted":
        return "unresolved"
    if "error" in result:
        return "failed"
    if star["has_planet"]:
        match = compare(result["period"], star["period"], tolerance) if star["period"] else None
        return {"same": "recovered", "harmonic": "harmonic"}.get(match, "missed")
    snr = result.get("snr")
    return "unvetted" if snr is not None and snr >= min_snr else "quiet"


def compute(
    events: Iterable[dict[str, Any]],
    stars: dict[str, dict[str, Any]],
    tolerance: float = TOLERANCE,
    min_snr: float = MIN_SNR,
) -> dict[str, Any]:
    queued = {}
    done = {}
    for event in events:
        if event["event"] == "task_queued":
            queued[event["task_id"]] = event["star_id"]
        elif event["event"] == "task_done" and event["task_id"] in queued:
            done[queued[event["task_id"]]] = (event["result"], event.get("outcome", "accepted"))

    rows = []
    for star_id, (result, outcome) in done.items():
        if star_id not in stars:
            continue
        star = stars[star_id]
        verdict = classify(star, result, outcome, tolerance, min_snr)
        row = {
            "star_id": star_id,
            "has_planet": star["has_planet"],
            "known_period": star["period"],
            "period": result.get("period"),
            "depth_ppm": round(result["depth"] * 1e6) if "depth" in result else None,
            "snr": result.get("snr"),
            "verdict": verdict,
            "note": "",
        }
        if verdict == "unvetted":
            same = star["period"] and compare(result["period"], star["period"], tolerance)
            row["note"] = "the signal that was ruled a false positive" if same else "a new signal"
        elif verdict == "failed":
            row["note"] = result.get("error", "")
        rows.append(row)
    rows.sort(key=lambda row: (not row["has_planet"], row["star_id"]))

    verdicts = [row["verdict"] for row in rows]
    planets = [row for row in rows if row["verdict"] in ("recovered", "harmonic", "missed")]
    in_range = [row for row in planets if searchable(row["known_period"])]
    recovered = verdicts.count("recovered")
    recovered_in_range = sum(row["verdict"] == "recovered" for row in in_range)
    return {
        "tolerance": tolerance,
        "min_snr": min_snr,
        "stars_listed": len(stars),
        "stars_with_result": len(rows),
        "planets_searched": len(planets),
        "planets_in_range": len(in_range),
        "recovered": recovered,
        "recovered_in_range": recovered_in_range,
        "harmonic": verdicts.count("harmonic"),
        "missed": verdicts.count("missed"),
        "recall": recovered / len(planets) if planets else None,
        "recall_in_range": recovered_in_range / len(in_range) if in_range else None,
        "failed": verdicts.count("failed"),
        "unresolved": verdicts.count("unresolved"),
        "no_planet_searched": verdicts.count("unvetted") + verdicts.count("quiet"),
        "unvetted": verdicts.count("unvetted"),
        "no_snr": sum(row["verdict"] == "quiet" and row["snr"] is None for row in rows),
        "stars": rows,
    }


def searchable(period: float | None) -> bool:
    return period is not None and SEARCH_RANGE[0] <= period <= SEARCH_RANGE[1]


def percent(value: float | None) -> str:
    return "-" if value is None else f"{value:.1%}"


def fmt(value: float | None, spec: str) -> str:
    return "-" if value is None else format(value, spec)


def report(recall: dict[str, Any]) -> str:
    low, high = SEARCH_RANGE
    lines = [
        f"stars:      {recall['stars_with_result']} of {recall['stars_listed']} listed have a result "
        f"({recall['failed']} failed, {recall['unresolved']} unresolved)",
        f"recall:     {recall['recovered']} of {recall['planets_searched']} known planets recovered "
        f"({percent(recall['recall'])}), period within {recall['tolerance']:.0%}",
        f"            {recall['recovered_in_range']} of {recall['planets_in_range']} with a known period "
        f"of {low:g} to {high:g} d, the range searched ({percent(recall['recall_in_range'])})",
        f"            {recall['harmonic']} more found at 2 or 3 times the period or a fraction of it, "
        f"{recall['missed']} at another period",
        f"unvetted:   {recall['unvetted']} of {recall['no_planet_searched']} stars with no known planet "
        f"have a signal with SNR >= {recall['min_snr']:g}",
    ]
    if recall["no_snr"]:
        lines.append(f"            ({recall['no_snr']} results have no SNR; they come from workers older than the snr field)")

    unvetted = [row for row in recall["stars"] if row["verdict"] == "unvetted"]
    if unvetted:
        lines += ["", "unvetted signals (no known planet):"]
        for row in unvetted:
            lines.append(
                f"  {row['star_id']:<9}  found {row['period']:9.5f} d  {row['depth_ppm']:6d} ppm  "
                f"SNR {row['snr']:6.1f}  {row['note']}"
            )
    missed = [row for row in recall["stars"] if row["verdict"] in ("harmonic", "missed")]
    if missed:
        lines += ["", "known planets not recovered:"]
        for row in missed:
            lines.append(
                f"  {row['star_id']:<9}  known {row['known_period']:9.5f} d  found {row['period']:9.5f} d  "
                f"SNR {fmt(row['snr'], '6.1f')}  {row['verdict']}"
            )
    return "\n".join(lines)


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    columns = ["star_id", "has_planet", "known_period", "period", "depth_ppm", "snr", "verdict", "note"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, columns)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("log", nargs="?", type=Path, help="run log (default: the newest in RUN_LOG_DIR or output/runs)")
    parser.add_argument("--stars", type=Path, default=DEFAULT_STARS, help="star list CSV with has_planet and period")
    parser.add_argument("--tolerance", type=float, default=TOLERANCE, help="fractional period error that still counts")
    parser.add_argument("--min-snr", type=float, default=MIN_SNR, help="SNR of a strong signal")
    parser.add_argument("--save", type=Path, metavar="DIR", help="write <run>-recall.txt and <run>-recall.csv into DIR")
    args = parser.parse_args(argv)

    directory = os.environ.get("RUN_LOG_DIR") or DEFAULT_LOG_DIR
    path = args.log or newest_log(directory)
    if path is None:
        parser.error(f"no run logs in {directory}; start a cluster with make up, or name a log")
    try:
        events = read_events(path)
        stars = read_star_list(args.stars)
    except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
        parser.error(f"can't read the run log or star list: {exc}")
    recall = compute(events, stars, args.tolerance, args.min_snr)
    print(path)
    if not recall["stars_with_result"]:
        print(f"no transit-search results for stars in {args.stars.name} (a simulated run?)")
        return 0
    text = report(recall)
    print(text)
    if args.save:
        args.save.mkdir(parents=True, exist_ok=True)
        stem = args.save / f"{path.stem}-recall"
        stem.with_suffix(".txt").write_text(f"{path}\n{text}\n")
        write_csv(recall["stars"], stem.with_suffix(".csv"))
        print(f"saved {stem.with_suffix('.txt')} and {stem.with_suffix('.csv')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
