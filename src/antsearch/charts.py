"""
Generate charts comparing completion time, throughput and wasted work from a comma-separated
list of filepaths to logs. The charts are to be saved in "docs/figures/".
"""

import argparse
import json
import logging
import os
import shutil
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from antsearch.runlog import read_events
from antsearch.metrics import compute

import matplotlib.pyplot as plt

def create_charts(runs_to_compare: list[str]):
    computed_log_data = []
    for current_run in runs_to_compare:
        current_event = None
        if current_run.find(".jsonl") != -1:
            try:
                current_event = read_events(current_run)
            except (OSError, json.JSONDecodeError) as exc:
                parser.error(f"can't read {current_run}: {exc}")

            if current_event != None:
                computed_log_data.append(compute(current_event))
        else:
            print(f"invalid file extension of {current_run}: not a .jsonl file")

    metrics_to_graph = [
        {"metric_type": "completion_seconds", "title": "Scheduler Completion Times", "label": "Completion Time (Seconds)"},
        {"metric_type": "throughput_per_minute", "title": "Scheduler Throughput", "label": "Throughput (Succeeded Tasks Per Minute)"},
        {"metric_type": "wasted_fraction", "title": "Scheduler Wasted Work", "label": "Wasted Work (Percentage of Worker Processing Time)"}]

    colors_for_schedulers = {"first-come": "#F85555", "round-robin": "#F8F855", "random": "#55F855", "greedy": "#55F8F8", "ant": "#5555F8"}

    for current_metric in metrics_to_graph:
        schedulers = []
        colors = []
        values = []
        for computed_log in computed_log_data:
            current_log_scheduler = "\n".join(computed_log["schedulers"]) or "unknown"
            schedulers.append(current_log_scheduler)
            try:
                colors.append(colors_for_schedulers[current_log_scheduler])
            except KeyError:
                colors.append("#AAAAAA")
            values.append(computed_log[current_metric["metric_type"]])

        plt.bar(schedulers, values, color = colors, width = 0.5)
        plt.title(current_metric["title"])
        plt.xlabel("Scheduler")
        plt.ylabel(current_metric["label"])

        plt.savefig(f"docs/figures/{current_metric["metric_type"]}.png", dpi = 300, bbox_inches = "tight")
        plt.clf()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("chart_logs", nargs="?", type=str, help="comma-separated filepaths to run logs")
    args = parser.parse_args(argv)

    if args.chart_logs:
        run_file_names = args.chart_logs.split(",")

        if len(run_file_names) > 0:
            create_charts(run_file_names)
        else:
            print("could not create charts: no files specified")
    else:
        print("could not create charts: no arguments given")
    return 0

if __name__ == "__main__":
    sys.exit(main())