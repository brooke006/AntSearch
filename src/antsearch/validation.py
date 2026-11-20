"""Comparing results from different workers, and deciding when a task's answer is settled.

Two results match if they have the same keys and the same values, ignoring timings (any key
ending in _seconds) and allowing a small relative difference between numbers, since two machines
can round the same calculation differently. Two failed results match if they failed with the
same error type, whatever their messages say.
"""

import math
from dataclasses import dataclass
from typing import Any

TOLERANCE = 1e-6  # relative difference allowed between two numbers that should be the same


def _same(a: Any, b: Any, tolerance: float) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
            return True
        return math.isclose(a, b, rel_tol=tolerance, abs_tol=1e-12)
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_same(a[key], b[key], tolerance) for key in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_same(x, y, tolerance) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


def answer(result: dict[str, Any]) -> dict[str, Any]:
    """The part of a result that two workers should agree on."""
    if "error" in result:
        return {"error": result["error"]}
    return {key: value for key, value in result.items() if not key.endswith("_seconds")}


def matches(a: dict[str, Any], b: dict[str, Any], tolerance: float = TOLERANCE) -> bool:
    return _same(answer(a), answer(b), tolerance)


def groups(results: list[dict[str, Any]], tolerance: float = TOLERANCE) -> list[list[int]]:
    """Indexes of results that match, grouped, in order of each group's first result."""
    found: list[list[int]] = []
    for index, result in enumerate(results):
        for group in found:
            if matches(results[group[0]], result, tolerance):
                group.append(index)
                break
        else:
            found.append([index])
    return found


@dataclass(frozen=True)
class Decision:
    """What to do with a task after a result arrives.

    accepted    enough results agree: agreed holds their indexes, first one first
    waiting     runs still out could settle it
    rerun       queue `runs` more runs on other workers
    unresolved  out of runs with no agreement
    """

    kind: str
    agreed: tuple[int, ...] = ()
    runs: int = 0


def decide(results: list[dict[str, Any]], outstanding: int, quorum: int, max_runs: int, tolerance: float = TOLERANCE) -> Decision:
    """Settle a task once quorum of its results match.

    results are the runs that have finished, in the order they finished; outstanding is how many
    more are running or queued. A task gets at most max_runs runs in all.
    """
    best = max(groups(results, tolerance), key=len, default=[])
    if len(best) >= quorum:
        return Decision("accepted", agreed=tuple(best))
    if len(best) + outstanding >= quorum:
        return Decision("waiting")
    room = max_runs - len(results) - outstanding
    if room > 0:
        return Decision("rerun", runs=min(room, quorum - len(best) - outstanding))
    if outstanding:
        return Decision("waiting")
    return Decision("unresolved")
